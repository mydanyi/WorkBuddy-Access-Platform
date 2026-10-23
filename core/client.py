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
import inspect
import secrets
from contextlib import asynccontextmanager
from contextvars import ContextVar
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
from .errors import ChatStreamError, UpstreamError

# Network/timeout failures collapsed into one observation category.
_NETWORK_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError)


def _observe_category(status=None, exc=None) -> str:
    """Map a final failure to a fixed, leak-free observation category.

    Never inspects the response body, URL, token or business ``code``; only the
    HTTP status and the exception *class* matter.  A ``401`` that still failed
    after the one refresh is ``auth``; ``403`` is ``permission``; ``429`` is
    ``rate``; ``5xx`` / missing status / network / timeout are ``network``;
    every other ``UpstreamError`` or a ``ChatStreamError`` is ``upstream``.
    """
    if isinstance(exc, ChatStreamError):
        return "upstream"
    if isinstance(exc, _NETWORK_ERRORS):
        return "network"
    if isinstance(exc, UpstreamError):
        status = exc.status
    elif exc is not None:
        return "upstream"
    if status == 401:
        return "auth"
    if status == 403:
        return "permission"
    if status == 429:
        return "rate"
    if status is None or (isinstance(status, int) and status >= 500):
        return "network"
    return "upstream"

MODELS_PATH = "/console/enterprises/personal/models"
BALANCE_PATH = "/v2/billing/meter/get-user-resource"
CHECKIN_STATUS_PATH = "/v2/billing/meter/checkin-activity-status"
CHECKIN_CLAIM_PATH = "/v2/billing/meter/daily-checkin"
CHAT_PATH = "/v2/chat/completions"

# The documented request-source keys handed to the error observer.  They are
# fixed literals: a source never carries a URL, a payload, a token or any free
# exception text, so nothing sensitive can leak through the notification layer.
SOURCE_CHAT = "chat"
SOURCE_MODELS = "models"
SOURCE_BALANCE = "balance"
SOURCE_SIGNIN = "signin"
SOURCE_GROWTH = "growth"
SOURCE_API = "api"

# Fixed growth endpoints (kept as literals so this module never imports the
# growth service; only the mapping matters here).
_GROWTH_PATH_PREFIXES = ("/v2/activity/growth/", "/activity/growth/")
_GROWTH_PATHS = (
    "/v2/report",
    "/v2/user-asset/appearance/set",
    "/activity/growth/buddy/agreement",
    "/activity/growth/buddy/first",
)


def _source_for_path(path) -> str:
    """Map one fixed request path to its documented source key.

    Only the module's own pinned path constants are compared; an unknown path is
    the generic ``api`` source.  No URL, query or payload is ever inspected.
    """
    if not isinstance(path, str):
        return SOURCE_API
    if path in (MODELS_PATH, V3_CONFIG_PATH):
        return SOURCE_MODELS
    if path == BALANCE_PATH:
        return SOURCE_BALANCE
    if path in (CHECKIN_STATUS_PATH, CHECKIN_CLAIM_PATH):
        return SOURCE_SIGNIN
    if path in _GROWTH_PATHS or path.startswith(_GROWTH_PATH_PREFIXES):
        return SOURCE_GROWTH
    return SOURCE_API

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

    def __init__(
        self,
        session,
        manager,
        *,
        chat_base,
        billing_base=None,
        web_base=None,
        on_error=None,
        on_models=None,
    ):
        self._session = session
        self._manager = manager
        self.chat_base = chat_base.rstrip("/")
        self.billing_base = (billing_base or chat_base).rstrip("/")
        self.web_base = (web_base or chat_base).rstrip("/")
        # Optional observer: called with one fixed category string (sync/async).
        self._on_error = on_error
        # Optional observer: called once with the successful model details
        # (sync/async) so a consumer can publish the live catalog.  It never
        # runs for a failed enterprise read, is never given a second argument,
        # and never adds a request of its own.
        self._on_models = on_models
        # Per-instance, read-only request source.  It is set only while the
        # observer runs and restored immediately afterwards, so a queued task
        # can never read a stale value from another request.
        self._error_source = ContextVar("workbuddy_error_source", default=None)

    @property
    def credentials(self) -> dict:
        return self._manager.credentials

    @property
    def error_source(self):
        """The documented source key of the failure currently being observed.

        Read-only: ``chat`` / ``models`` / ``balance`` / ``signin`` / ``growth``
        / ``api`` while the ``on_error`` callback runs, ``None`` otherwise.
        """
        return self._error_source.get()

    @property
    def manager(self):
        return self._manager

    async def _invoke(self, callback, *args) -> None:
        """Run a sync/async observer without letting it affect the request.

        A regular exception from the observer is swallowed; a ``CancelledError``
        is always re-raised so cancellation keeps propagating.
        """
        try:
            outcome = callback(*args)
            if inspect.isawaitable(outcome):
                await outcome
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - the observer must never change the request
            pass

    async def _notify_error(self, exc, *, observed=True, status=None, source=None) -> None:
        """Report one fixed category for an authenticated request failure.

        The fixed ``source`` key is scoped to the observer call itself through a
        per-instance context variable: it is set right before the callback and
        restored in ``finally``, so the callback's single-argument ``category``
        contract is unchanged and no global mute / global state is involved.
        """
        if not observed or self._on_error is None:
            return
        token = self._error_source.set(source)
        try:
            await self._invoke(self._on_error, _observe_category(status=status, exc=exc))
        finally:
            self._error_source.reset(token)

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
        """Public wrapper: observe the final failure of an authenticated call.

        The request contract is entirely owned by :meth:`_request_json`; this
        wrapper only reports one fixed category for an authenticated request that
        carried a login ``accessToken`` and ultimately failed.  A successful
        refresh never reports, and an unauthenticated call never reports.  The
        fixed source key is derived from the pinned path constant only.
        """
        observed = bool(auth and self.credentials.get("accessToken"))
        try:
            return await self._request_json(
                method, path, base=base, json=json, headers=headers, auth=auth
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - re-raised unchanged after observing
            await self._notify_error(
                exc, observed=observed, source=_source_for_path(path)
            )
            raise

    async def _request_json(self, method, path, *, base=None, json=None, headers=None, auth=True):
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

        A successful call hands the finished list to the optional ``on_models``
        observer exactly once (through :meth:`_invoke`, so a regular observer
        error is isolated and cancellation still propagates).  No extra request
        is made for it, and a failed read never invokes it.
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
        # Publish the finished catalog once; the observer owns no request and
        # cannot change what this call returns.
        if self._on_models is not None:
            await self._invoke(self._on_models, details)
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
    async def chat_stream(
        self,
        payload=None,
        *,
        base=None,
        headers=None,
        before_post=None,
        observe_errors=True,
    ):
        """Public wrapper: observe the final, un-rescued chat failure.

        The response/refresh/release contract is owned by :meth:`_chat_stream`.
        Only an authenticated request that carried a login ``accessToken`` and
        finally failed is reported: an un-rescued ``HTTP >= 400`` response, an
        ``UpstreamError`` / ``ChatStreamError``, or a network/timeout failure.
        At most one report is emitted per call, and ``CancelledError`` always
        propagates without being observed.

        ``before_post`` is awaited right before every real chat POST (the first
        attempt and the one resend after a 401 refresh), so a shared budget can
        count an actual attempt; the token refresh itself is never counted.
        ``observe_errors=False`` silences the observation for *this* call only
        (a routed standby attempt that is expected to be absorbed) -- the shared
        ``on_error`` observer and every other request stay untouched.  The
        reported source is always the fixed ``chat`` key.
        """
        observed = bool(observe_errors and self.credentials.get("accessToken"))
        reported = False
        try:
            async with self._chat_stream(
                payload, base=base, headers=headers, before_post=before_post
            ) as resp:
                status = getattr(resp, "status", None)
                if observed and isinstance(status, int) and status >= 400:
                    reported = True
                    await self._notify_error(
                        None, observed=True, status=status, source=SOURCE_CHAT
                    )
                yield resp
        except asyncio.CancelledError:
            raise
        except (UpstreamError, ChatStreamError) + _NETWORK_ERRORS as exc:
            if not reported:
                await self._notify_error(exc, observed=observed, source=SOURCE_CHAT)
            raise

    @asynccontextmanager
    async def _chat_stream(
        self, payload=None, *, base=None, headers=None, before_post=None
    ):
        """Open an upstream chat completion response, refreshing once on 401.

        The upstream speaks SSE; ``core.chat`` forces ``stream: true``.  A 401
        reuses :meth:`AuthManager.refresh` with the stale token (single-flight)
        and resends exactly once; every other status is yielded untouched for the
        caller to interpret.  The response is always released, including when
        the body is cancelled or the client disconnects.  ``X-Refresh-Token`` is
        never part of the chat headers.

        ``before_post`` (optional, awaitable) runs immediately before each real
        chat POST -- the first attempt and the resend after the refresh -- and
        its failure aborts the call without sending anything; the refresh call
        itself is not a chat attempt and is never counted.
        """
        url = self._base_for(CHAT_PATH, base) + CHAT_PATH
        token = self.credentials.get("accessToken")
        if before_post is not None:
            await before_post()
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
                if before_post is not None:
                    await before_post()
                resp = await self._session.post(
                    url,
                    json=payload,
                    headers=self._chat_headers(token, headers),
                    allow_redirects=False,
                )
            yield resp
        finally:
            resp.release()
