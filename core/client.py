"""Authenticated JSON client for the WorkBuddy upstream.

Error contract (shared by every API, including the future chat path):

* HTTP 200 with a non-zero business ``code`` is an error (never returned to the
  caller as data) and never triggers a refresh.
* HTTP 401 triggers *exactly one* refresh-and-resend; anything else (403, 429,
  5xx, business errors) is surfaced as-is.
* The upstream body is never embedded in the raised error.
"""

from __future__ import annotations

import hashlib
import secrets
from contextlib import asynccontextmanager

from .auth import common_headers, parse_retry_after
from .errors import UpstreamError

MODELS_PATH = "/console/enterprises/personal/models"
BALANCE_PATH = "/v2/billing/meter/get-user-resource"
CHECKIN_STATUS_PATH = "/v2/billing/meter/checkin-activity-status"
CHECKIN_CLAIM_PATH = "/v2/billing/meter/daily-checkin"
CHAT_PATH = "/v2/chat/completions"

# Pinned WorkBuddy desktop client version (``X-IDE-Version`` / UA).
_CHAT_CLIENT_VERSION = "5.5.4"


def _stable_account_id(uid, purpose) -> str:
    """Derive the 36-hex per-account device/session id pinned in ``headers.go``."""
    digest = hashlib.sha256(
        ("wb2a:" + purpose + ":" + str(uid)).encode("utf-8")
    ).hexdigest()
    return digest[:36]


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

    async def models(self) -> list[str]:
        """Return the CLI-chat model ids advertised by the live catalog.

        Derived from the enterprise ``personal/models`` document: the ids the
        ``cli`` agent exposes, intersected with the ``models`` metadata list,
        with non-chat entries filtered out.  A missing ``cli`` agent or an empty
        result is an upstream contract violation -- never a static fallback.
        """
        data = await self.request_json("GET", MODELS_PATH)
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
            result.append(model_id)

        if not result:
            raise UpstreamError(200, None)
        return result

    async def balance(self) -> dict:
        """Fetch the account resource/balance summary."""
        return await self.request_json(
            "POST", BALANCE_PATH, base=self.billing_base, json={}
        )

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
