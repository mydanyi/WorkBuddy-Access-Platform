"""Behavior tests for the WorkBuddy OAuth / refresh / signin HTTP contract.

Scope: only the documented public contract of ``core.errors``, ``core.auth``,
``core.client`` and ``core.signin``.  The production modules do not exist yet on
purpose: the top-level imports below are expected to fail until those modules
are implemented and verified remotely in the Docker host.  Do not "fix" that
here.

The fake upstream is a real ``aiohttp.web`` server bound to 127.0.0.1:0 (no
AstrBot, no mock of our own code).  Every fake credential is a FICTIONAL FIXED
SAMPLE and is never read from any live source.

Temporary files use the same NAS-hosted ``tmp`` root as ``test_storage.py``;
the system temp dir is never used as a fallback.
"""

import asyncio
import base64
import collections
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

import aiohttp
from aiohttp import web

# --- Make ``astrbot_plugin_workbuddy`` importable when discovered from anywhere.
_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]  # directory that contains astrbot_plugin_workbuddy/
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.auth import AuthManager  # noqa: E402
from astrbot_plugin_workbuddy.core.client import UpstreamClient  # noqa: E402
from astrbot_plugin_workbuddy.core.errors import UpstreamError  # noqa: E402
from astrbot_plugin_workbuddy.core.signin import SigninService  # noqa: E402
from astrbot_plugin_workbuddy.core.storage import CredentialStore  # noqa: E402


def _resolve_base_tmp() -> Path:
    """Temp root: env override, else the current task's NAS tmp dir."""
    env = os.environ.get("WORKBUDDY_TEST_TMP")
    if env:
        return Path(env)
    task_root = _HERE.parents[3]
    return task_root / "tmp"


BASE_TMP = _resolve_base_tmp()

# --- FICTIONAL FIXED SAMPLE KEY (standard base64 of exactly 32 bytes). ---
_KEY_BYTES = b"workbuddy-http-master-key-000001"
if len(_KEY_BYTES) != 32:
    raise AssertionError("sample master key must be exactly 32 bytes")
MASTER_KEY = base64.b64encode(_KEY_BYTES).decode("ascii")

# --- FICTIONAL FIXED SAMPLE TOKENS / ACCOUNTS (never real). ---
STATE_A = "state-aaaa-000000000001"
STATE_B = "state-bbbb-000000000002"
AUTH_URL = "https://auth.example.test.invalid/authorize?state=" + STATE_A

OLD_AT = "FICTIONAL-OLD-ACCESS-TOKEN"
OLD_RT = "FICTIONAL-OLD-REFRESH-TOKEN"

AT_A = "FICTIONAL-ACCESS-TOKEN-ACCOUNT-A"
RT_A = "FICTIONAL-REFRESH-TOKEN-ACCOUNT-A"
UID_A = "test-uid-account-a"
AT_B = "FICTIONAL-ACCESS-TOKEN-ACCOUNT-B"
RT_B = "FICTIONAL-REFRESH-TOKEN-ACCOUNT-B"
UID_B = "test-uid-account-b"

NEW_AT = "FICTIONAL-NEW-ACCESS-TOKEN"
NEW_RT = "FICTIONAL-NEW-REFRESH-TOKEN"
DOMAIN = "example.test.invalid"

# Refresh endpoint path from pinned internal/upstream/client.go:919.
REFRESH_PATH = "/v2/plugin/auth/token/refresh"
STATE_PATH = "/v2/plugin/auth/state"
TOKEN_PATH = "/v2/plugin/auth/token"
ACCOUNT_PATH = "/v2/plugin/login/account"
CHECKIN_STATUS_PATH = "/v2/billing/meter/checkin-activity-status"
CHECKIN_CLAIM_PATH = "/v2/billing/meter/daily-checkin"

SEED_CREDENTIALS = {
    "accessToken": OLD_AT,
    "refreshToken": OLD_RT,
    "uid": UID_A,
    "expiresAt": 4102444800000,  # 2100-01-01 in ms, fictional
    "domain": DOMAIN,
    "realm": "cn",
}


def _ok(data=None):
    """Envelope success response: HTTP 200 with business code 0."""
    return web.json_response({"code": 0, "data": {} if data is None else data})


def _biz(code, msg, status=200):
    """Business-code response (code may be non-zero) at an arbitrary HTTP status."""
    return web.json_response({"code": code, "msg": msg}, status=status)


class _FakeUpstream:
    """A real aiohttp upstream on 127.0.0.1:0 that records every request."""

    def __init__(self) -> None:
        self.app = web.Application()
        self.handlers = {}
        self.calls = []
        self.hits = collections.Counter()
        self.base = None
        self._runner = None
        self._site = None
        self.app.router.add_route("*", "/{tail:.*}", self._dispatch)

    def on(self, method: str, path: str, handler) -> None:
        self.handlers[(method.upper(), path)] = handler

    async def _dispatch(self, request):
        body = await request.read()
        self.calls.append(
            {
                "method": request.method,
                "path": request.path,
                "query": dict(request.query),
                "headers": {k.lower(): v for k, v in request.headers.items()},
                "body": body,
            }
        )
        self.hits[(request.method, request.path)] += 1
        handler = self.handlers.get((request.method, request.path))
        if handler is None:
            return web.json_response({"code": 5999, "msg": "unmapped route"}, status=404)
        return await handler(request)

    async def start(self) -> None:
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await self._site.start()
        host, port = self._runner.addresses[0][:2]
        self.base = "http://{}:{}".format(host, port)

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()

    def calls_for(self, method: str, path: str):
        return [c for c in self.calls if c["method"] == method.upper() and c["path"] == path]

    def json_body(self, call):
        return json.loads(call["body"].decode("utf-8")) if call["body"] else None


class _HttpContractTestCase(unittest.IsolatedAsyncioTestCase):
    """Spins up the fake upstream plus a NAS-hosted data dir per test."""

    async def asyncSetUp(self) -> None:
        BASE_TMP.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(tempfile.mkdtemp(prefix="wb-http-", dir=str(BASE_TMP)))
        self.up = _FakeUpstream()
        await self.up.start()
        self.session = aiohttp.ClientSession()

    async def asyncTearDown(self) -> None:
        await self.session.close()
        await self.up.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)

    # -- helpers ---------------------------------------------------------

    def _seed_store(self):
        store = CredentialStore(self.data_dir, MASTER_KEY)
        store.save(dict(SEED_CREDENTIALS))
        return store

    def _make_manager(self, store, **kwargs):
        params = {
            "chat_base": self.up.base,
            "poll_interval": 0.05,
            "login_timeout": 1.0,
        }
        params.update(kwargs)
        return AuthManager(self.session, store, **params)

    def _make_client(self, manager=None, store=None):
        if manager is None:
            manager = self._make_manager(store or self._seed_store())
        return UpstreamClient(
            self.session,
            manager,
            chat_base=self.up.base,
            billing_base=self.up.base,
            web_base=self.up.base,
        )

    def _credential_file(self) -> Path:
        return self.data_dir / "credentials.enc"

    # -- OAuth -----------------------------------------------------------

    async def test_begin_login_requests_state_and_returns_urls(self):
        async def state_handler(request):
            return _ok({"state": STATE_A, "authUrl": AUTH_URL})

        self.up.on("POST", STATE_PATH, state_handler)

        manager = self._make_manager(CredentialStore(self.data_dir, MASTER_KEY))
        result = await manager.begin_login()

        self.assertEqual(result.get("state"), STATE_A)
        self.assertEqual(result.get("authUrl"), AUTH_URL)

        calls = self.up.calls_for("POST", STATE_PATH)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["query"].get("platform"), "CLI")
        self.assertEqual(calls[0]["headers"].get("content-type"), "application/json")
        self.assertIn("origin", calls[0]["headers"])
        self.assertIn("referer", calls[0]["headers"])
        self.assertIn("user-agent", calls[0]["headers"])

    async def test_complete_login_polls_pending_then_saves_encrypted_credentials(self):
        token_calls = {"n": 0}

        async def token_handler(request):
            self.assertEqual(request.query.get("state"), STATE_A)
            token_calls["n"] += 1
            if token_calls["n"] == 1:
                return _biz(1001, "login ing")  # pending, source login ing semantics
            return _ok(
                {
                    "accessToken": AT_A,
                    "refreshToken": RT_A,
                    "expiresIn": 3600,
                    "domain": DOMAIN,
                }
            )

        async def account_handler(request):
            self.assertEqual(request.query.get("state"), STATE_A)
            self.assertEqual(request.headers.get("Authorization"), "Bearer " + AT_A)
            return _ok({"uid": UID_A, "enterpriseId": "", "nickname": "Tester"})

        async def state_handler(request):
            return _ok({"state": STATE_A, "authUrl": AUTH_URL})

        self.up.on("POST", STATE_PATH, state_handler)
        self.up.on("GET", TOKEN_PATH, token_handler)
        self.up.on("GET", ACCOUNT_PATH, account_handler)

        store = CredentialStore(self.data_dir, MASTER_KEY)
        manager = self._make_manager(store)
        # The state must be the one issued by begin_login, never an arbitrary value.
        state = (await manager.begin_login())["state"]
        summary = await manager.complete_login(state)

        self.assertIsInstance(summary, dict)
        self.assertEqual(summary.get("uid"), UID_A)
        self.assertNotIn("accessToken", summary)
        self.assertNotIn("refreshToken", summary)
        text = json.dumps(summary, ensure_ascii=False) + repr(summary)
        self.assertNotIn(AT_A, text)
        self.assertNotIn(RT_A, text)

        saved = store.load()
        self.assertEqual(saved["accessToken"], AT_A)
        self.assertEqual(saved["refreshToken"], RT_A)
        self.assertEqual(saved["uid"], UID_A)
        self.assertGreaterEqual(token_calls["n"], 2)

        raw = self._credential_file().read_bytes()
        self.assertNotIn(AT_A.encode("utf-8"), raw)
        self.assertNotIn(RT_A.encode("utf-8"), raw)

    async def test_complete_login_times_out_without_writing_credentials(self):
        async def token_handler(request):
            return _biz(1001, "login ing")

        async def state_handler(request):
            return _ok({"state": STATE_A, "authUrl": AUTH_URL})

        self.up.on("POST", STATE_PATH, state_handler)
        self.up.on("GET", TOKEN_PATH, token_handler)

        store = CredentialStore(self.data_dir, MASTER_KEY)
        manager = self._make_manager(store, poll_interval=0.05, login_timeout=0.3)
        state = (await manager.begin_login())["state"]

        raised = False
        try:
            await manager.complete_login(state)
        except Exception:  # noqa: BLE001 - error type is not part of the contract
            raised = True
        self.assertTrue(raised, "complete_login must not succeed while pending")
        self.assertIsNone(store.load())
        self.assertGreaterEqual(self.up.hits[("GET", TOKEN_PATH)], 2)

    async def test_complete_login_rejects_superseded_state_and_accepts_active_one(self):
        bundles = {
            STATE_A: {
                "accessToken": AT_A,
                "refreshToken": RT_A,
                "expiresIn": 3600,
                "domain": DOMAIN,
            },
            STATE_B: {
                "accessToken": AT_B,
                "refreshToken": RT_B,
                "expiresIn": 3600,
                "domain": DOMAIN,
            },
        }
        uid_by_token = {AT_A: UID_A, AT_B: UID_B}
        issued = {"n": 0}

        async def state_handler(request):
            # A second begin_login issues STATE_B, which supersedes STATE_A.
            issued["n"] += 1
            state = STATE_A if issued["n"] == 1 else STATE_B
            return _ok({"state": state, "authUrl": AUTH_URL + state})

        async def token_handler(request):
            state = request.query.get("state")
            if state in bundles:
                return _ok(bundles[state])
            return _biz(1001, "login ing")

        async def account_handler(request):
            bearer = request.headers.get("Authorization") or ""
            token = bearer[len("Bearer "):] if bearer.startswith("Bearer ") else ""
            return _ok({"uid": uid_by_token.get(token, ""), "nickname": "T"})

        self.up.on("POST", STATE_PATH, state_handler)
        self.up.on("GET", TOKEN_PATH, token_handler)
        self.up.on("GET", ACCOUNT_PATH, account_handler)

        store = CredentialStore(self.data_dir, MASTER_KEY)
        manager = self._make_manager(store)
        state_a = (await manager.begin_login())["state"]
        state_b = (await manager.begin_login())["state"]
        self.assertEqual((state_a, state_b), (STATE_A, STATE_B))

        # The superseded state must be rejected -- no token poll, no credentials.
        with self.assertRaises(Exception):
            await manager.complete_login(state_a)
        self.assertIsNone(store.load())
        self.assertEqual(self.up.hits[("GET", TOKEN_PATH)], 0)

        summary = await manager.complete_login(state_b)

        self.assertEqual(summary.get("uid"), UID_B)
        saved = store.load()
        self.assertEqual(saved["uid"], UID_B)
        self.assertEqual(saved["accessToken"], AT_B)
        account_calls = self.up.calls_for("GET", ACCOUNT_PATH)
        self.assertTrue(account_calls)
        self.assertEqual(account_calls[-1]["query"].get("state"), STATE_B)

    async def test_complete_login_rejects_foreign_state_without_begin_login(self):
        async def token_handler(request):
            return _ok(
                {
                    "accessToken": AT_A,
                    "refreshToken": RT_A,
                    "expiresIn": 3600,
                    "domain": DOMAIN,
                }
            )

        self.up.on("GET", TOKEN_PATH, token_handler)

        store = CredentialStore(self.data_dir, MASTER_KEY)
        manager = self._make_manager(store)

        with self.assertRaises(Exception):
            await manager.complete_login(STATE_B)

        self.assertIsNone(store.load())
        self.assertEqual(self.up.hits[("GET", TOKEN_PATH)], 0)

    async def test_logout_during_refresh_does_not_write_back_credentials(self):
        started = asyncio.Event()
        release = asyncio.Event()

        async def refresh_handler(request):
            started.set()
            await release.wait()
            return _ok(
                {
                    "accessToken": NEW_AT,
                    "refreshToken": NEW_RT,
                    "expiresIn": 3600,
                    "domain": DOMAIN,
                }
            )

        self.up.on("POST", REFRESH_PATH, refresh_handler)

        store = self._seed_store()
        manager = self._make_manager(store)

        task = asyncio.create_task(manager.refresh())
        await started.wait()
        await manager.logout()
        release.set()
        await task

        # The late refresh response must not resurrect the logged-out account.
        self.assertIsNone(store.load())
        self.assertEqual(manager.credentials, {})
        self.assertEqual(self.up.hits[("POST", REFRESH_PATH)], 1)

    # -- refresh ---------------------------------------------------------

    async def test_refresh_posts_pinned_headers_and_persists(self):
        async def refresh_handler(request):
            return _ok(
                {
                    "accessToken": NEW_AT,
                    "refreshToken": NEW_RT,
                    "expiresIn": 3600,
                    "domain": DOMAIN,
                }
            )

        self.up.on("POST", REFRESH_PATH, refresh_handler)

        store = self._seed_store()
        manager = self._make_manager(store)
        self.assertEqual(manager.credentials.get("refreshToken"), OLD_RT)

        await manager.refresh()

        calls = self.up.calls_for("POST", REFRESH_PATH)
        self.assertEqual(len(calls), 1)
        headers = calls[0]["headers"]
        self.assertEqual(headers.get("x-refresh-token"), OLD_RT)
        self.assertEqual(headers.get("x-auth-refresh-source"), "plugin")
        self.assertEqual(headers.get("x-codebuddy-request"), "1")
        self.assertEqual(headers.get("accept-language"), "zh-CN")
        self.assertEqual(headers.get("content-type"), "application/json")
        self.assertEqual(headers.get("accept"), "application/json")
        self.assertEqual(headers.get("origin"), "https://www.codebuddy.cn")
        self.assertEqual(headers.get("referer"), "https://www.codebuddy.cn/")
        self.assertNotIn("authorization", headers)
        self.assertIn("WorkBuddy", headers.get("user-agent", ""))

        self.assertEqual(manager.credentials.get("accessToken"), NEW_AT)
        reloaded = store.load()
        self.assertEqual(reloaded["accessToken"], NEW_AT)
        self.assertEqual(reloaded["refreshToken"], NEW_RT)

    async def test_concurrent_401_triggers_single_refresh_and_at_most_one_retry(self):
        store = self._seed_store()

        refresh_hits = {"n": 0}

        async def refresh_handler(request):
            refresh_hits["n"] += 1
            await asyncio.sleep(0.05)  # widen the concurrency window
            return _ok(
                {
                    "accessToken": NEW_AT,
                    "refreshToken": NEW_RT,
                    "expiresIn": 3600,
                    "domain": DOMAIN,
                }
            )

        def protected(tag):
            async def handler(request):
                if request.headers.get("Authorization") == "Bearer " + NEW_AT:
                    return _ok({"ok": tag})
                return _biz(1001, "token expired", status=401)

            return handler

        self.up.on("POST", REFRESH_PATH, refresh_handler)
        self.up.on("GET", "/v2/protected/a", protected("a"))
        self.up.on("GET", "/v2/protected/b", protected("b"))

        client = self._make_client(manager=self._make_manager(store))

        results = await asyncio.gather(
            client.request_json("GET", "/v2/protected/a"),
            client.request_json("GET", "/v2/protected/b"),
        )
        self.assertEqual([r.get("ok") for r in results], ["a", "b"])

        self.assertEqual(refresh_hits["n"], 1)
        self.assertEqual(self.up.hits[("GET", "/v2/protected/a")], 2)
        self.assertEqual(self.up.hits[("GET", "/v2/protected/b")], 2)

    async def test_refresh_failure_does_not_loop(self):
        store = self._seed_store()

        async def refresh_handler(request):
            return _biz(5000, "refresh broken", status=500)

        async def protected(request):
            return _biz(1001, "token expired", status=401)

        self.up.on("POST", REFRESH_PATH, refresh_handler)
        self.up.on("GET", "/v2/protected/x", protected)

        client = self._make_client(manager=self._make_manager(store))

        with self.assertRaises(UpstreamError):
            await client.request_json("GET", "/v2/protected/x")

        self.assertEqual(self.up.hits[("POST", REFRESH_PATH)], 1)
        self.assertEqual(self.up.hits[("GET", "/v2/protected/x")], 1)

    # -- error mapping ---------------------------------------------------

    async def test_http200_business_code_nonzero_is_error_without_body_leak(self):
        secret = "UPSTREAM-SECRET-BODY-xyz"

        async def handler(request):
            return web.json_response({"code": 123, "msg": secret}, status=200)

        self.up.on("GET", "/v2/plugin/business", handler)

        client = self._make_client()

        with self.assertRaises(UpstreamError) as ctx:
            await client.request_json("GET", "/v2/plugin/business")

        self.assertEqual(ctx.exception.status, 200)
        self.assertEqual(ctx.exception.code, 123)
        text = "{}\n{}".format(ctx.exception, repr(ctx.exception))
        self.assertNotIn(secret, text)
        self.assertEqual(self.up.hits[("POST", REFRESH_PATH)], 0)

    async def test_403_is_error_and_does_not_refresh(self):
        async def handler(request):
            return _biz(4030, "forbidden", status=403)

        self.up.on("GET", "/v2/plugin/forbidden", handler)
        client = self._make_client()

        with self.assertRaises(UpstreamError) as ctx:
            await client.request_json("GET", "/v2/plugin/forbidden")

        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(self.up.hits[("POST", REFRESH_PATH)], 0)
        self.assertEqual(self.up.hits[("GET", "/v2/plugin/forbidden")], 1)

    async def test_429_parses_retry_after_without_refresh(self):
        async def handler(request):
            return web.json_response(
                {"code": 4290, "msg": "slow down"},
                status=429,
                headers={"Retry-After": "7"},
            )

        self.up.on("GET", "/v2/plugin/throttled", handler)
        client = self._make_client()

        with self.assertRaises(UpstreamError) as ctx:
            await client.request_json("GET", "/v2/plugin/throttled")

        self.assertEqual(ctx.exception.status, 429)
        self.assertEqual(float(ctx.exception.retry_after), 7.0)
        self.assertEqual(self.up.hits[("POST", REFRESH_PATH)], 0)

    # -- signin ----------------------------------------------------------

    async def test_signin_already_signed_skips_claim(self):
        async def status_handler(request):
            return _ok({"active": True, "today_checked_in": True, "streak_days": 5})

        self.up.on("POST", CHECKIN_STATUS_PATH, status_handler)

        result = await SigninService(self._make_client()).run()

        self.assertEqual(result.get("outcome"), "already_signed")
        self.assertTrue(result.get("verified"))
        status_calls = self.up.calls_for("POST", CHECKIN_STATUS_PATH)
        self.assertEqual(len(status_calls), 1)
        self.assertEqual(self.up.json_body(status_calls[0]), {})
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 0)

    async def test_signin_not_signed_claims_then_rechecks_status(self):
        statuses = [
            {"active": True, "today_checked_in": False},
            {"active": True, "today_checked_in": True, "streak_days": 6},
        ]
        counter = {"n": 0}

        async def status_handler(request):
            index = min(counter["n"], len(statuses) - 1)
            counter["n"] += 1
            return _ok(dict(statuses[index]))

        async def claim_handler(request):
            return _ok({"credit": 10})

        self.up.on("POST", CHECKIN_STATUS_PATH, status_handler)
        self.up.on("POST", CHECKIN_CLAIM_PATH, claim_handler)

        result = await SigninService(self._make_client()).run()

        self.assertEqual(result.get("outcome"), "signed")
        self.assertTrue(result.get("verified"))
        self.assertEqual(self.up.hits[("POST", CHECKIN_STATUS_PATH)], 2)
        claim_calls = self.up.calls_for("POST", CHECKIN_CLAIM_PATH)
        self.assertEqual(len(claim_calls), 1)
        self.assertEqual(self.up.json_body(claim_calls[0]), {})

    async def test_signin_claim_network_interruption_rechecks_without_duplicate(self):
        statuses = [
            {"active": True, "today_checked_in": False},
            {"active": True, "today_checked_in": True, "streak_days": 6},
        ]
        counter = {"n": 0}

        async def status_handler(request):
            index = min(counter["n"], len(statuses) - 1)
            counter["n"] += 1
            return _ok(dict(statuses[index]))

        async def claim_handler(request):
            return web.json_response({"code": 9001, "msg": "upstream down"}, status=503)

        self.up.on("POST", CHECKIN_STATUS_PATH, status_handler)
        self.up.on("POST", CHECKIN_CLAIM_PATH, claim_handler)

        result = await SigninService(self._make_client()).run()

        self.assertTrue(result.get("verified"))
        self.assertIn(result.get("outcome"), ("signed", "already_signed"))
        self.assertEqual(self.up.hits[("POST", CHECKIN_STATUS_PATH)], 2)
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 1)

    async def test_signin_claim_business_error_is_unconfirmed_not_success(self):
        async def status_handler(request):
            return _ok({"active": True, "today_checked_in": False})

        async def claim_handler(request):
            return _biz(10000, "system busy")

        self.up.on("POST", CHECKIN_STATUS_PATH, status_handler)
        self.up.on("POST", CHECKIN_CLAIM_PATH, claim_handler)

        result = await SigninService(self._make_client()).run()

        self.assertEqual(result.get("outcome"), "unconfirmed")
        self.assertFalse(result.get("verified"))
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 1)
        self.assertGreaterEqual(self.up.hits[("POST", CHECKIN_STATUS_PATH)], 1)


if __name__ == "__main__":
    unittest.main()
