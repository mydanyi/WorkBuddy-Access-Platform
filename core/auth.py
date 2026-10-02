"""OAuth login and access-token refresh for the single WorkBuddy account.

Protocol is pinned from the reverse-engineered references:

* ``cmd/login/main.go`` -- ``POST /v2/plugin/auth/state?platform=CLI`` returns
  ``{state, authUrl}``; ``GET /v2/plugin/auth/token?state=`` is the authoritative
  login-status endpoint (pending while the business code is non-zero, e.g.
  ``"login ing"``); ``GET /v2/plugin/login/account?state=`` is fetched with a
  ``Bearer`` access token to obtain ``uid``/``nickname``.
* ``internal/upstream/headers.go`` -- refresh headers.

Contract highlights implemented here:

* The pending OAuth ``state`` lives only in memory and is cleared on completion,
  timeout, cancellation or a new ``begin_login``; a login can only be completed
  with the state issued by the *current* ``begin_login`` (no mixing accounts).
* ``refresh`` is single-flight per account: callers pass the stale access token
  they observed; the lock is taken first and the stale token is re-compared
  inside it, so the same old token is refreshed at most once.
* Credentials are never written back after a ``logout``/re-login superseded the
  in-flight operation (epoch guard).
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from .errors import LoginStateError, LoginTimeoutError, UpstreamError

STATE_PATH = "/v2/plugin/auth/state"
TOKEN_PATH = "/v2/plugin/auth/token"
ACCOUNT_PATH = "/v2/plugin/login/account"
REFRESH_PATH = "/v2/plugin/auth/token/refresh"

_CLIENT_VERSION = "5.5.4"
_CLI_VERSION = "2.137.1"
_ORIGIN_CN = "https://www.codebuddy.cn"
_ORIGIN_GLOBAL = "https://www.workbuddy.ai"

# Server messages that mean "login still in progress" (never a terminal error).
# The business code alone is deliberately *not* trusted: an unexpected message
# (even under ``1001``) must surface as a terminal error instead of spinning
# until the timeout pretending the login is merely slow.
_PENDING_MARKERS = ("login ing", "login-ing", "waiting")


def _is_global(realm) -> bool:
    return realm == "global"


def _origin_for(realm) -> str:
    return _ORIGIN_GLOBAL if _is_global(realm) else _ORIGIN_CN


def _accept_language(realm) -> str:
    return "en-US" if _is_global(realm) else "zh-CN"


def common_headers(realm, token=None) -> dict:
    """Common outbound headers shared by every API call."""
    origin = _origin_for(realm)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": origin,
        "Referer": origin + "/",
        "User-Agent": "WorkBuddy/{} WorkBuddy/{} CLI/{}".format(
            _CLIENT_VERSION, _CLIENT_VERSION, _CLI_VERSION
        ),
        "X-CodeBuddy-Request": "1",
        "Accept-Language": _accept_language(realm),
    }
    if token:
        headers["Authorization"] = "Bearer " + token
    return headers


def refresh_headers(realm, refresh_token) -> dict:
    """Refresh-endpoint headers.  ``X-Refresh-Token`` only ever appears here."""
    headers = common_headers(realm)
    headers.pop("Authorization", None)
    headers["X-Refresh-Token"] = refresh_token
    headers["X-Auth-Refresh-Source"] = "plugin"
    return headers


def _envelope(body):
    """Return ``(code, data, msg)`` from a decoded envelope, defensively."""
    if not isinstance(body, dict):
        return None, {}, None
    data = body.get("data")
    data = data if isinstance(data, dict) else {}
    return body.get("code"), data, body.get("msg")


def _is_pending(code, msg) -> bool:
    """Only *known* waiting/login-ing messages count as pending.

    The business code is deliberately ignored: an unexpected message (even under
    ``1001``) is a terminal error and must never spin until timeout pretending
    the login is merely slow.
    """
    if code == 0:
        # Success envelope without a token yet: still finishing.
        return True
    if isinstance(msg, str):
        low = msg.lower()
        if any(marker in low for marker in _PENDING_MARKERS):
            return True
    return False


def _is_https_url(value) -> bool:
    """A usable authorization URL must be a non-empty ``https://`` string."""
    return isinstance(value, str) and value.startswith("https://")


class AuthManager:
    """Owns the single stored account: OAuth login and token refresh."""

    def __init__(
        self,
        session,
        store,
        *,
        chat_base,
        poll_interval=1.0,
        login_timeout=120.0,
    ):
        self._session = session
        self._store = store
        self._chat_base = chat_base.rstrip("/")
        self._poll_interval = poll_interval
        self._login_timeout = login_timeout
        self._pending_state = None
        self._refresh_lock = asyncio.Lock()
        # Bumped by begin_login/logout; in-flight work only commits if unchanged.
        self._epoch = 0

    @property
    def credentials(self) -> dict:
        return self._store.load() or {}

    def _realm(self) -> str:
        return self.credentials.get("realm") or "cn"

    # -- OAuth ---------------------------------------------------------------

    async def begin_login(self) -> dict:
        """Request a fresh authorization ``state`` (invalidates any previous)."""
        self._epoch += 1
        epoch = self._epoch
        self._pending_state = None
        url = self._chat_base + STATE_PATH
        resp = await self._session.post(
            url,
            params={"platform": "CLI"},
            json={},
            headers=common_headers("cn"),
            allow_redirects=False,
        )
        try:
            data = await self._read_envelope(resp)
            status = resp.status
        finally:
            resp.release()
        state = data.get("state")
        auth_url = data.get("authUrl")
        if not state or not _is_https_url(auth_url):
            raise UpstreamError(status, None)
        if epoch != self._epoch:
            # A newer begin_login superseded us: never clobber its pending state.
            raise LoginStateError("login state request was superseded")
        self._pending_state = state
        return {"state": state, "authUrl": auth_url}

    async def complete_login(self, state) -> dict:
        """Poll the token endpoint for ``state`` then persist the account.

        Returns an account *summary* that never contains credentials.
        """
        if not state or state != self._pending_state:
            raise LoginStateError(
                "login state was not issued by the active begin_login"
            )
        epoch = self._epoch
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._login_timeout
        realm = self._realm()
        token_url = self._chat_base + TOKEN_PATH
        data = None
        try:
            while True:
                if epoch != self._epoch:
                    raise LoginStateError("login was superseded")
                resp = await self._session.get(
                    token_url,
                    params={"state": state},
                    headers=common_headers(realm),
                    allow_redirects=False,
                )
                try:
                    try:
                        body = await resp.json(content_type=None)
                    except Exception:
                        await resp.read()
                        raise UpstreamError(resp.status, None)
                    status = resp.status
                finally:
                    resp.release()
                if epoch != self._epoch:
                    # The response belongs to a superseded login: it must never
                    # be turned into credentials nor touch the active pending state.
                    raise LoginStateError("login was superseded")
                code, rdata, msg = _envelope(body)
                if status in (401, 403) or status != 200:
                    raise UpstreamError(status, code)
                if code == 0 and rdata.get("accessToken"):
                    data = rdata
                    break
                if _is_pending(code, msg):
                    if loop.time() >= deadline:
                        raise LoginTimeoutError("login did not complete in time")
                    await asyncio.sleep(self._poll_interval)
                    continue
                raise UpstreamError(status, code)

            access_token = data.get("accessToken")
            account = {}
            try:
                aresp = await self._session.get(
                    self._chat_base + ACCOUNT_PATH,
                    params={"state": state},
                    headers=common_headers(realm, token=access_token),
                    allow_redirects=False,
                )
                try:
                    account = await self._read_envelope(aresp)
                finally:
                    aresp.release()
            except UpstreamError:
                account = {}

            if epoch != self._epoch:
                # Superseded before persisting: never write a stale account.
                raise LoginStateError("login was superseded")

            credentials = {
                "accessToken": access_token,
                "refreshToken": data.get("refreshToken"),
                "uid": account.get("uid") or "",
                "enterpriseId": account.get("enterpriseId") or "",
                "domain": data.get("domain") or self.credentials.get("domain") or "",
                "realm": realm,
            }
            expires_in = data.get("expiresIn")
            if isinstance(expires_in, (int, float)):
                credentials["expiresAt"] = int(time.time() * 1000) + int(expires_in) * 1000

            self._store.save(credentials)

            return {
                "uid": account.get("uid") or "",
                "nickname": account.get("nickname") or "",
                "enterpriseId": account.get("enterpriseId") or "",
                "domain": credentials["domain"],
                "realm": credentials["realm"],
            }
        finally:
            # Invalidate *this* login only; a newer begin_login owns its own state.
            if epoch == self._epoch and self._pending_state == state:
                self._pending_state = None

    def cancel_login(self) -> None:
        """Abort the active login so any in-flight poll can no longer commit."""
        self._epoch += 1
        self._pending_state = None

    # -- refresh -------------------------------------------------------------

    async def refresh(self, stale_token=None) -> dict:
        """Refresh the access token once per observed stale token.

        ``stale_token`` is the access token the caller used for the failed
        request.  The lock is acquired *first*, then the token is re-compared,
        so concurrent 401s sharing one old token trigger exactly one refresh.
        """
        async with self._refresh_lock:
            creds = self._store.load() or {}
            if stale_token is not None and creds.get("accessToken") != stale_token:
                # Another caller already replaced the token we were using.
                return creds
            refresh_token = creds.get("refreshToken")
            if not refresh_token:
                raise UpstreamError(401, None)
            realm = creds.get("realm") or "cn"
            epoch = self._epoch
            resp = await self._session.post(
                self._chat_base + REFRESH_PATH,
                json={},
                headers=refresh_headers(realm, refresh_token),
                allow_redirects=False,
            )
            try:
                data = await self._read_envelope(resp)
            finally:
                resp.release()
            new_access = data.get("accessToken")
            if not new_access:
                raise UpstreamError(resp.status, None)

            updated = dict(creds)
            updated["accessToken"] = new_access
            if data.get("refreshToken"):
                updated["refreshToken"] = data["refreshToken"]
            if data.get("domain"):
                updated["domain"] = data["domain"]
            expires_in = data.get("expiresIn")
            if isinstance(expires_in, (int, float)):
                updated["expiresAt"] = int(time.time() * 1000) + int(expires_in) * 1000

            if epoch != self._epoch:
                # Superseded by logout/re-login: never persist nor report success.
                return self._store.load() or {}
            self._store.save(updated)
            return updated

    async def logout(self) -> None:
        """Forget the account; supersedes any in-flight refresh/login write."""
        self._epoch += 1
        self._pending_state = None
        self._store.delete()

    # -- helpers -------------------------------------------------------------

    async def _read_envelope(self, resp) -> dict:
        try:
            body = await resp.json(content_type=None)
        except Exception:
            await resp.read()
            raise UpstreamError(resp.status, None)
        code, data, _ = _envelope(body)
        if resp.status != 200:
            raise UpstreamError(resp.status, code, parse_retry_after(resp))
        if code != 0:
            raise UpstreamError(200, code)
        return data


def parse_retry_after(resp):
    """Parse ``Retry-After`` as delay-seconds or as an HTTP-date.

    Returns a non-negative number of seconds, or ``None`` when the header is
    absent or cannot be parsed.
    """
    value = resp.headers.get("Retry-After")
    if not value:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        pass
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())
