"""Authenticated JSON client for the WorkBuddy upstream.

Error contract (shared by every API, including the future chat path):

* HTTP 200 with a non-zero business ``code`` is an error (never returned to the
  caller as data) and never triggers a refresh.
* HTTP 401 triggers *exactly one* refresh-and-resend; anything else (403, 429,
  5xx, business errors) is surfaced as-is.
* The upstream body is never embedded in the raised error.
"""

from __future__ import annotations

import asyncio
import hashlib
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiohttp

from .auth import common_headers, parse_retry_after
from .credits import (
    format_credits,
    is_finite_number,
    parse_credits,
    promotion_discount,
    promotion_label,
    promotion_note,
    select_promotion,
)
from .errors import UpstreamError

MODELS_PATH = "/console/enterprises/personal/models"
BALANCE_PATH = "/v2/billing/meter/get-user-resource"
CHECKIN_STATUS_PATH = "/v2/billing/meter/checkin-activity-status"
CHECKIN_CLAIM_PATH = "/v2/billing/meter/daily-checkin"
CHAT_PATH = "/v2/chat/completions"

# Pinned WorkBuddy desktop client version (``X-IDE-Version`` / UA).
_CHAT_CLIENT_VERSION = "5.5.4"

# ``/v3/config`` (IDE shape) supplements same-id credits + ``modelPromotions``.
V3_CONFIG_PATH = "/v3/config"
_V3_CONFIG_HEADERS = {
    "User-Agent": "CodeBuddyIDE/4.12.0 CodeBuddy/4.12.0",
    "X-Product": "SaaS",
}

# Balance (resource) query: fixed page/product/status and effective-time window.
_PACKAGE_PAGE_SIZE = 100
_PACKAGE_PRODUCT_CODE = "p_tcaca"
_PACKAGE_STATUS = (0, 3)
_PACKAGE_WINDOW_TZ = ZoneInfo("Asia/Shanghai")
_PACKAGE_WINDOW_DAYS = 101 * 365


def _stable_account_id(uid, purpose) -> str:
    """Derive the 36-hex per-account device/session id pinned in ``headers.go``."""
    digest = hashlib.sha256(
        ("wb2a:" + purpose + ":" + str(uid)).encode("utf-8")
    ).hexdigest()
    return digest[:36]


def _balance_window() -> tuple[str, str]:
    """The fixed, server-side effective window (Asia/Shanghai, +101 years)."""
    now = datetime.now(_PACKAGE_WINDOW_TZ)
    begin = now.strftime("%Y-%m-%d %H:%M:%S")
    end = (now + timedelta(days=_PACKAGE_WINDOW_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    return begin, end


def _unknown_balance() -> dict:
    """A safe "cannot aggregate" summary: never a fabricated zero."""
    return {
        "known": False,
        "remaining": None,
        "used": None,
        "total": None,
        "package_count": None,
    }


def _balance_page(data):
    """Extract one ``Response.Data`` page, or ``(None, None)`` if unrecognized."""
    if not isinstance(data, dict):
        return None, None
    response = data.get("Response")
    if not isinstance(response, dict):
        return None, None
    inner = response.get("Data")
    if not isinstance(inner, dict):
        return None, None
    accounts = inner.get("Accounts")
    total = inner.get("TotalCount")
    if not isinstance(accounts, list):
        return None, None
    if not isinstance(total, int) or isinstance(total, bool) or total < 0:
        return None, None
    return accounts, total


def _package_amounts(account):
    """Return ``(size, remain, used)`` for one package, or ``None`` if unusable.

    A cycle-quota package (valid ``CycleCapacitySize`` > 0) is read *only* from
    its ``Cycle*`` fields and never mixed with ``Capacity*``.  Otherwise the
    plain ``Capacity*`` fields apply.  Server-reported ``Used`` larger than
    ``size - remain`` is treated as truth (then clamped), and ``remain`` is
    re-derived from the adopted ``used``.
    """
    cycle_size = account.get("CycleCapacitySize")
    if is_finite_number(cycle_size) and cycle_size > 0:
        size = cycle_size
        remain_raw = account.get("CycleCapacityRemain")
        used_raw = account.get("CycleCapacityUsed")
    else:
        size = account.get("CapacitySize")
        remain_raw = account.get("CapacityRemain")
        used_raw = account.get("CapacityUsed")

    if not is_finite_number(size) or size < 0:
        return None
    if not is_finite_number(remain_raw):
        return None

    remain = 0.0 if remain_raw < 0 else float(remain_raw)
    if remain > size:
        remain = float(size)

    derived_used = float(size) - remain
    used = derived_used
    if is_finite_number(used_raw) and used_raw > derived_used:
        used = float(used_raw)
        if used > size:
            used = float(size)
    if used < 0:
        used = 0.0

    return float(size), float(size) - used, used


def _clean_number(value):
    """Collapse integral floats to ``int`` for stable summaries."""
    if value == int(value):
        return int(value)
    return value


# Catalog entries under these id prefixes / tags are never chat models.
_NON_CHAT_MODEL_PREFIXES = ("nes-", "completion-", "codewise-")
_NON_CHAT_MODEL_TAGS = ("text-to-image",)
# A catalog entry capped at/below this many output tokens is non-chat too.
_MIN_CHAT_MAX_OUTPUT_TOKENS = 256


class UpstreamClient:
    """Thin authenticated JSON client.  Owns no credentials of its own."""

    def __init__(self, session, manager, *, chat_base, billing_base=None, web_base=None):
        self._session = session
        self._manager = manager
        self.chat_base = chat_base.rstrip("/")
        self.billing_base = (billing_base or chat_base).rstrip("/")
        self.web_base = (web_base or chat_base).rstrip("/")

    @property
    def credentials(self) -> dict:
        return self._manager.credentials

    @property
    def manager(self):
        return self._manager

    def _base_for(self, path: str, base) -> str:
        if base:
            return base.rstrip("/")
        if path.startswith("/v2/billing"):
            return self.billing_base
        if path.startswith("/v2/plugin"):
            return self.web_base
        return self.chat_base

    def _headers(self, token, extra=None) -> dict:
        creds = self.credentials
        headers = common_headers(creds.get("realm") or "cn", token=token)
        # Identity headers come from the in-memory credential; ``domain`` is only
        # ever a header value here, never a URL router.  Ordinary API headers
        # never carry ``X-Refresh-Token``.
        identity = (
            ("X-User-Id", creds.get("uid")),
            ("X-Domain", creds.get("domain")),
            ("X-Enterprise-Id", creds.get("enterpriseId")),
        )
        for name, value in identity:
            if value:
                headers[name] = value
        if extra:
            headers.update(extra)
        return headers

    def _chat_headers(self, token, extra=None) -> dict:
        """Chat headers assembled from pinned ``internal/upstream/headers.go``.

        Carries the real desktop fingerprint (``X-No-Enterprise-Id``, ``X-IDE-*``,
        ``X-Agent-Purpose``) and a per-request trace family.  ``X-Refresh-Token``
        is a hard red line and is never emitted here.
        """
        creds = self.credentials
        realm = creds.get("realm") or "cn"
        headers = common_headers(realm, token=token)
        headers["Accept"] = "application/json, text/event-stream"
        if token:
            headers["Authorization"] = "Bearer " + token
        else:
            headers.pop("Authorization", None)
            headers["X-No-Authorization"] = "1"

        uid = creds.get("uid")
        if uid:
            headers["X-User-Id"] = uid
            headers["X-Machine-ID"] = _stable_account_id(uid, "machine")
            headers["X-Session-ID"] = _stable_account_id(uid, "session")
        else:
            headers["X-No-User-Id"] = "1"

        if realm == "global":
            # International client shape: declare "no enterprise" and the intl domain.
            headers["X-No-Enterprise-Id"] = "1"
            headers["X-Domain"] = "www.workbuddy.ai"
        else:
            enterprise_id = creds.get("enterpriseId")
            if enterprise_id:
                headers["X-Enterprise-Id"] = enterprise_id
            else:
                headers["X-No-Enterprise-Id"] = "1"
            domain = creds.get("domain")
            if domain:
                headers["X-Domain"] = domain
            else:
                headers["X-No-Department-Info"] = "1"

        # Usage attribution: impersonate the official desktop fingerprint.
        headers["X-Agent-Purpose"] = "conversation"
        headers["X-IDE-Name"] = "WorkBuddy"
        headers["X-IDE-Type"] = "WorkBuddy"
        headers["X-IDE-Version"] = _CHAT_CLIENT_VERSION
        headers["X-Product"] = "WorkBuddy"

        # Conversation / request trace family (one user send shares one request id).
        request_id = secrets.token_hex(16)
        message_id = secrets.token_hex(16)
        headers["X-Conversation-Request-ID"] = request_id
        headers["X-Conversation-Message-ID"] = message_id
        headers["X-Request-ID"] = message_id
        headers["X-Root-Request-ID"] = request_id
        headers["X-Trace-ID"] = request_id
        headers["X-B3-TraceId"] = request_id
        headers["X-B3-SpanId"] = message_id[:16]
        headers["X-B3-Sampled"] = "1"

        if extra:
            for name, value in extra.items():
                if value is not None:
                    headers[name] = value
        return headers

    async def request_json(self, method, path, *, base=None, json=None, headers=None, auth=True):
        """Perform one request, refresh-and-resend once on 401, return ``data``."""
        url = self._base_for(path, base) + path
        token = self.credentials.get("accessToken") if auth else None
        resp = await self._session.request(
            method,
            url,
            json=json,
            headers=self._headers(token, headers),
            allow_redirects=False,
        )
        try:
            if resp.status == 401 and auth:
                await resp.read()
                try:
                    await self._manager.refresh(stale_token=token)
                except UpstreamError:
                    # Keep the auth failure visible: refresh did not rescue us.
                    raise UpstreamError(401, None) from None
                token = self.credentials.get("accessToken")
                retry = await self._session.request(
                    method,
                    url,
                    json=json,
                    headers=self._headers(token, headers),
                    allow_redirects=False,
                )
                try:
                    return await self._parse(retry)
                finally:
                    retry.release()
            return await self._parse(resp)
        finally:
            resp.release()

    async def _parse(self, resp) -> dict:
        try:
            body = await resp.json(content_type=None)
        except Exception:
            await resp.read()
            raise UpstreamError(resp.status, None, parse_retry_after(resp))
        code = body.get("code") if isinstance(body, dict) else None
        if resp.status != 200:
            raise UpstreamError(resp.status, code, parse_retry_after(resp))
        if code != 0:
            raise UpstreamError(200, code)
        data = body.get("data") if isinstance(body, dict) else None
        return data if isinstance(data, dict) else {}

    def _select_chat_entries(self, data: dict) -> list[dict]:
        """Return the raw catalog entries the ``cli`` agent exposes.

        The enterprise ``personal/models`` document decides the callable set:
        the ids the ``cli`` agent lists, intersected with the ``models`` catalog
        and filtered for non-chat entries (disabled / id prefix / tag / capped
        ``maxOutputTokens``), order preserved and de-duplicated.  A missing
        ``cli`` agent or an empty result is an upstream contract violation --
        never a static fallback.
        """
        catalog = data.get("models")
        catalog = catalog if isinstance(catalog, list) else []
        agents = data.get("agents")
        agents = agents if isinstance(agents, list) else []

        cli_ids = None
        for agent in agents:
            if isinstance(agent, dict) and agent.get("name") == "cli":
                listed = agent.get("models")
                cli_ids = (
                    [m for m in listed if isinstance(m, str)]
                    if isinstance(listed, list)
                    else []
                )
                break
        if cli_ids is None:
            raise UpstreamError(200, None)

        allowed = set(cli_ids)
        result = []
        seen = set()
        for item in catalog:
            if not isinstance(item, dict):
                continue
            model_id = item.get("id")
            if not isinstance(model_id, str) or not model_id:
                continue
            if model_id in seen or model_id not in allowed:
                continue
            if item.get("disabled"):
                continue
            if model_id.startswith(_NON_CHAT_MODEL_PREFIXES):
                continue
            tags = item.get("tags")
            if isinstance(tags, list) and any(
                isinstance(tag, str) and tag in _NON_CHAT_MODEL_TAGS for tag in tags
            ):
                continue
            max_out = item.get("maxOutputTokens")
            if (
                isinstance(max_out, (int, float))
                and not isinstance(max_out, bool)
                and max_out <= _MIN_CHAT_MAX_OUTPUT_TOKENS
            ):
                continue
            seen.add(model_id)
            result.append(item)

        if not result:
            raise UpstreamError(200, None)
        return result

    async def models(self) -> list[str]:
        """Return the CLI-chat model ids advertised by the live catalog.

        Unchanged contract: only the enterprise ``personal/models`` document is
        read, and only the pure ids are returned (no ``/v3/config`` query).
        """
        data = await self.request_json("GET", MODELS_PATH)
        return [item["id"] for item in self._select_chat_entries(data)]

    async def _fetch_v3_config(self) -> dict:
        """Fetch ``/v3/config`` (IDE shape); a failure yields an empty supplement.

        ``UpstreamError`` (bad status / business code) and network/timeout
        failures leave the enterprise metadata usable.  Cancellation is a
        ``BaseException`` and is deliberately never caught here.
        """
        try:
            return await self.request_json(
                "GET",
                V3_CONFIG_PATH,
                base=self.chat_base,
                headers=dict(_V3_CONFIG_HEADERS),
            )
        except UpstreamError:
            return {}
        except (asyncio.TimeoutError, aiohttp.ClientError):
            return {}

    async def model_details(self, now=None) -> list[dict]:
        """Return per-model credit / promotion metadata for the CLI-callable set.

        The enterprise document decides the set; ``/v3/config`` only
        *supplements* same-id credits and ``modelPromotions`` and never widens
        it.  A failing ``/v3/config`` leaves the still-usable enterprise
        metadata in place.  Each entry carries the raw base ``credits`` (number
        or ``None``), its canonical ``effective_credits`` string, and the active
        promotion's ``promo_label`` / ``promo_note`` (``""`` when none applies).
        """
        data = await self.request_json("GET", MODELS_PATH)
        entries = self._select_chat_entries(data)

        v3 = await self._fetch_v3_config()
        v3_models = v3.get("models") if isinstance(v3, dict) else None
        v3_models = v3_models if isinstance(v3_models, list) else []
        v3_by_id = {}
        for item in v3_models:
            if not isinstance(item, dict):
                continue
            model_id = item.get("id")
            if isinstance(model_id, str) and model_id and model_id not in v3_by_id:
                v3_by_id[model_id] = item
        promotions = v3.get("modelPromotions") if isinstance(v3, dict) else None
        promotions = promotions if isinstance(promotions, list) else []

        if now is None:
            now = datetime.now(timezone.utc)
        elif now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        details = []
        for item in entries:
            model_id = item["id"]
            name = item.get("name")
            name = name if isinstance(name, str) else ""

            base = parse_credits(item.get("credits"))
            if base is None:
                supplement = v3_by_id.get(model_id)
                if supplement is not None:
                    base = parse_credits(supplement.get("credits"))

            promo = select_promotion(promotions, model_id, now)
            label = ""
            note = ""
            effective = format_credits(base)
            if promo is not None:
                label = promotion_label(promo)
                note = promotion_note(promo)
                discounted = promotion_discount(promo, base)
                if discounted is not None:
                    effective = format_credits(discounted)

            details.append(
                {
                    "id": model_id,
                    "name": name,
                    "credits": base,
                    "effective_credits": effective,
                    "promo_label": label,
                    "promo_note": note,
                }
            )
        return details

    async def balance(self) -> dict:
        """Aggregate the account resource/balance summary across all packages.

        Reads the billing meter with a fixed, server-side effective window and
        paginates until every advertised record is collected.  ``TotalDosage``
        is deliberately ignored (its semantics are unconfirmed).  A partial or
        unreadable document is reported as *unknown* (all-``None``), never as a
        fabricated zero; a page failure raises a safe ``UpstreamError`` rather
        than exposing a partial total.
        """
        begin, end = _balance_window()
        accounts = []
        total_count = None
        deadline_pages = None
        page_number = 1
        while True:
            payload = {
                "PageNumber": page_number,
                "PageSize": _PACKAGE_PAGE_SIZE,
                "ProductCode": _PACKAGE_PRODUCT_CODE,
                "Status": list(_PACKAGE_STATUS),
                "PackageEndTimeRangeBegin": begin,
                "PackageEndTimeRangeEnd": end,
            }
            data = await self.request_json(
                "POST", BALANCE_PATH, base=self.billing_base, json=payload
            )
            page_accounts, page_total = _balance_page(data)
            if page_accounts is None:
                return _unknown_balance()
            if total_count is None:
                total_count = page_total
                deadline_pages = (
                    (total_count + _PACKAGE_PAGE_SIZE - 1) // _PACKAGE_PAGE_SIZE + 1
                )
            accounts.extend(page_accounts)
            if len(accounts) >= total_count:
                break
            if not page_accounts or page_number >= deadline_pages:
                # The server claims more records but cannot deliver them.
                return _unknown_balance()
            page_number += 1

        remaining = 0.0
        used_total = 0.0
        total = 0.0
        for account in accounts:
            if not isinstance(account, dict):
                return _unknown_balance()
            amounts = _package_amounts(account)
            if amounts is None:
                return _unknown_balance()
            size, remain, used = amounts
            total += size
            remaining += remain
            used_total += used
        return {
            "known": True,
            "remaining": _clean_number(remaining),
            "used": _clean_number(used_total),
            "total": _clean_number(total),
            "package_count": len(accounts),
        }

    async def signin_status(self) -> dict:
        """Fetch the daily check-in status."""
        return await self.request_json(
            "POST", CHECKIN_STATUS_PATH, base=self.billing_base, json={}
        )

    async def claim_signin(self) -> dict:
        """Perform the daily check-in claim."""
        return await self.request_json(
            "POST", CHECKIN_CLAIM_PATH, base=self.billing_base, json={}
        )

    @asynccontextmanager
    async def chat_stream(self, payload=None, *, base=None, headers=None):
        """Open an upstream chat completion response, refreshing once on 401.

        The upstream speaks SSE; ``core.chat`` forces ``stream: true``.  A 401
        reuses :meth:`AuthManager.refresh` with the stale token (single-flight)
        and resends exactly once; every other status is yielded untouched for the
        caller to interpret.  The response is always released, including when
        the body is cancelled or the client disconnects.  ``X-Refresh-Token`` is
        never part of the chat headers.
        """
        url = self._base_for(CHAT_PATH, base) + CHAT_PATH
        token = self.credentials.get("accessToken")
        resp = await self._session.post(
            url,
            json=payload,
            headers=self._chat_headers(token, headers),
            allow_redirects=False,
        )
        try:
            if resp.status == 401:
                await resp.read()
                resp.release()
                try:
                    await self._manager.refresh(stale_token=token)
                except UpstreamError:
                    # Refresh did not rescue the call: keep the auth failure visible.
                    raise UpstreamError(401, None) from None
                token = self.credentials.get("accessToken")
                resp = await self._session.post(
                    url,
                    json=payload,
                    headers=self._chat_headers(token, headers),
                    allow_redirects=False,
                )
            yield resp
        finally:
            resp.release()
