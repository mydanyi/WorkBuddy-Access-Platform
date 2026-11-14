"""签到用的带认证 JSON 客户端。

聊天已经改走 ACP（CLI 子进程），这个客户端现在只剩一件事：**把签到那两个请求发
出去**。所以它不再有聊天流、模型目录、余额聚合，也没有任何桌面端指纹头——那些都是
私有 HTTP 路径的一部分，随它们一起下线了。

错误契约（只剩签到这条链，语义与迁移前一致）：

* HTTP 200 但业务 ``code`` 非 0 是错误（绝不当数据返回），且不触发刷新；
* HTTP 401 触发**恰好一次**"叫 CLI 刷新 + 重发"，其余（403 / 429 / 5xx / 业务错）
  原样上抛；
* 上游响应体永远不会被塞进异常里。
"""

from __future__ import annotations

import asyncio
import inspect
from contextvars import ContextVar

import aiohttp

from .errors import UpstreamError
from .http_utils import common_headers, parse_retry_after

# Network/timeout failures collapsed into one observation category.
_NETWORK_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError)

CHECKIN_STATUS_PATH = "/v2/billing/meter/checkin-activity-status"
CHECKIN_CLAIM_PATH = "/v2/billing/meter/daily-checkin"

# The documented request-source keys handed to the error observer.  They are
# fixed literals: a source never carries a URL, a payload, a token or any free
# exception text, so nothing sensitive can leak through the notification layer.
SOURCE_SIGNIN = "signin"
SOURCE_API = "api"


def _observe_category(status=None, exc=None) -> str:
    """Map a final failure to a fixed, leak-free observation category.

    Never inspects the response body, URL, token or business ``code``; only the
    HTTP status and the exception *class* matter.  A ``401`` that still failed
    after the one refresh is ``auth``; ``403`` is ``permission``; ``429`` is
    ``rate``; ``5xx`` / missing status / network / timeout are ``network``;
    every other ``UpstreamError`` is ``upstream``.
    """
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


def _source_for_path(path) -> str:
    """Map one fixed request path to its documented source key.

    Only the module's own pinned path constants are compared; an unknown path is
    the generic ``api`` source.  No URL, query or payload is ever inspected.
    """
    if not isinstance(path, str):
        return SOURCE_API
    if path in (CHECKIN_STATUS_PATH, CHECKIN_CLAIM_PATH):
        return SOURCE_SIGNIN
    return SOURCE_API


class UpstreamClient:
    """Thin authenticated JSON client.  Owns no credentials of its own."""

    def __init__(self, session, manager, *, billing_base, on_error=None):
        self._session = session
        # 凭据来源（``CliCredentials``）：只读 + 一次"叫 CLI 刷新"。
        self._manager = manager
        self.billing_base = billing_base.rstrip("/")
        # Optional observer: called with one fixed category string (sync/async).
        self._on_error = on_error
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

        Read-only: ``signin`` / ``api`` while the ``on_error`` callback runs,
        ``None`` otherwise.
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
        url = (base.rstrip("/") if base else self.billing_base) + path
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


__all__ = [
    "UpstreamClient",
    "CHECKIN_STATUS_PATH",
    "CHECKIN_CLAIM_PATH",
    "SOURCE_SIGNIN",
    "SOURCE_API",
]
