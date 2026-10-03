"""Defect regression tests for the WorkBuddy OAuth / signin / models contract.

Companion to ``tests/test_http.py``.  It reuses that module's real ``aiohttp``
fake upstream and envelope helpers but defines its *own* test case, so none of
the existing contract tests are executed twice.

Every scenario here pins behavior the current production code
(``core.auth`` / ``core.signin`` / ``core.client``) is known to get wrong.  The
assertions describe the intended contract and are expected to fail until the
modules are fixed and re-verified remotely in the Docker host.  Nothing in this
file mocks our own production functions: the fake upstream is a real
``aiohttp.web`` server and concurrency is driven with ``asyncio`` events.

Every token / state / model id below is a FICTIONAL FIXED SAMPLE.

Temporary files use the NAS-hosted ``tmp`` root and honour
``WORKBUDDY_TEST_TMP``; the system temp dir is never used as a fallback.
"""

import asyncio
import base64
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

# Reuse the contract suite's real fake-upstream fixtures, but NOT its test case
# (subclassing ``_HttpContractTestCase`` would re-run every contract test).
try:  # package discovery: ``python -m unittest discover`` from the plugin dir.
    from tests.test_http import (  # noqa: E402
        _FakeUpstream,
        _ok,
        _biz,
        MASTER_KEY,
        STATE_A,
        STATE_B,
        AUTH_URL,
        AT_A,
        RT_A,
        UID_A,
        AT_B,
        RT_B,
        UID_B,
        DOMAIN,
        STATE_PATH,
        TOKEN_PATH,
        ACCOUNT_PATH,
        CHECKIN_STATUS_PATH,
        CHECKIN_CLAIM_PATH,
    )
except ImportError:  # top-level discovery: ``python -m unittest discover -s tests``.
    from test_http import (  # noqa: E402
        _FakeUpstream,
        _ok,
        _biz,
        MASTER_KEY,
        STATE_A,
        STATE_B,
        AUTH_URL,
        AT_A,
        RT_A,
        UID_A,
        AT_B,
        RT_B,
        UID_B,
        DOMAIN,
        STATE_PATH,
        TOKEN_PATH,
        ACCOUNT_PATH,
        CHECKIN_STATUS_PATH,
        CHECKIN_CLAIM_PATH,
    )


def _resolve_base_tmp() -> Path:
    """Temp root: env override, else the current task's NAS tmp dir."""
    env = os.environ.get("WORKBUDDY_TEST_TMP")
    if env:
        return Path(env)
    task_root = _HERE.parents[3]
    return task_root / "tmp"


BASE_TMP = _resolve_base_tmp()

# Pinned real upstream endpoints (see references/workbuddy2api-panel).
MODELS_CATALOG_PATH = "/console/enterprises/personal/models"
BALANCE_PATH = "/v2/billing/meter/get-user-resource"

_SUCCESS_BUNDLE_A = {
    "accessToken": AT_A,
    "refreshToken": RT_A,
    "expiresIn": 3600,
    "domain": DOMAIN,
}
_SUCCESS_BUNDLE_B = {
    "accessToken": AT_B,
    "refreshToken": RT_B,
    "expiresIn": 3600,
    "domain": DOMAIN,
}

# ``MASTER_KEY`` is imported to keep the sample key identical to the contract
# suite; assert the invariant locally so a bad sample can never slip through.
if len(base64.b64decode(MASTER_KEY)) != 32:  # pragma: no cover - guard
    raise AssertionError("sample master key must decode to 32 bytes")


class _RegressionTestCase(unittest.IsolatedAsyncioTestCase):
    """Fake upstream + NAS-hosted data dir, mirroring the contract suite."""

    async def asyncSetUp(self) -> None:
        BASE_TMP.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(tempfile.mkdtemp(prefix="wb-regress-", dir=str(BASE_TMP)))
        self.up = _FakeUpstream()
        await self.up.start()
        self.session = aiohttp.ClientSession()
        self._extra_sessions = []

    async def asyncTearDown(self) -> None:
        for session in self._extra_sessions:
            await session.close()
        await self.session.close()
        await self.up.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)

    # -- helpers ---------------------------------------------------------

    def _store(self):
        return CredentialStore(self.data_dir, MASTER_KEY)

    def _make_manager(self, store, *, session=None, **kwargs):
        params = {"chat_base": self.up.base, "poll_interval": 0.02, "login_timeout": 1.0}
        params.update(kwargs)
        return AuthManager(session or self.session, store, **params)

    def _make_client(self, manager=None, *, session=None, store=None):
        session = session or self.session
        if manager is None:
            manager = self._make_manager(store or self._store(), session=session)
        return UpstreamClient(
            session,
            manager,
            chat_base=self.up.base,
            billing_base=self.up.base,
            web_base=self.up.base,
        )

    async def _run_signin_with_statuses(self, statuses):
        counter = {"n": 0}

        async def status_handler(request):
            index = min(counter["n"], len(statuses) - 1)
            counter["n"] += 1
            return _ok(dict(statuses[index]))

        self.up.on("POST", CHECKIN_STATUS_PATH, status_handler)
        return counter

    # -- 1. late complete_login success must not win / must not clobber -----

    async def test_complete_login_success_after_new_begin_is_rejected_and_keeps_new_state(self):
        started = asyncio.Event()
        release = asyncio.Event()
        issued = {"n": 0}
        uid_by_token = {AT_A: UID_A, AT_B: UID_B}

        async def state_handler(request):
            n = issued["n"]
            issued["n"] += 1
            state = STATE_A if n == 0 else STATE_B
            return _ok({"state": state, "authUrl": AUTH_URL + state})

        async def token_handler(request):
            state = request.query.get("state")
            if state == STATE_A:
                # Block A's success response until B has begun a new login.
                started.set()
                await release.wait()
                return _ok(dict(_SUCCESS_BUNDLE_A))
            if state == STATE_B:
                return _ok(dict(_SUCCESS_BUNDLE_B))
            return _biz(1001, "login ing")

        async def account_handler(request):
            bearer = request.headers.get("Authorization") or ""
            token = bearer[len("Bearer "):] if bearer.startswith("Bearer ") else ""
            return _ok({"uid": uid_by_token.get(token, ""), "nickname": "T"})

        self.up.on("POST", STATE_PATH, state_handler)
        self.up.on("GET", TOKEN_PATH, token_handler)
        self.up.on("GET", ACCOUNT_PATH, account_handler)

        store = self._store()
        manager = self._make_manager(store)
        state_a = (await manager.begin_login())["state"]
        self.assertEqual(state_a, STATE_A)

        task_a = asyncio.create_task(manager.complete_login(state_a))
        await asyncio.wait_for(started.wait(), 5)

        # A brand new authorization supersedes A while its response is in flight.
        state_b = (await manager.begin_login())["state"]
        self.assertEqual(state_b, STATE_B)

        release.set()
        rejected = False
        try:
            await asyncio.wait_for(task_a, 5)
        except Exception:  # noqa: BLE001 - a superseded login must not report success
            rejected = True
        self.assertTrue(
            rejected,
            "a complete_login superseded by a later begin_login must not succeed",
        )

        # B's pending state must have survived A's late response.
        summary_b = await manager.complete_login(state_b)
        self.assertEqual(summary_b.get("uid"), UID_B)

        saved = store.load()
        self.assertIsNotNone(saved)
        self.assertEqual(saved.get("accessToken"), AT_B)
        self.assertEqual(saved.get("uid"), UID_B)
        self.assertNotEqual(saved.get("accessToken"), AT_A)

    # -- 2. slow begin_login response must not clobber a newer state --------

    async def test_slow_begin_login_response_cannot_clobber_newer_state(self):
        first_started = asyncio.Event()
        first_release = asyncio.Event()
        state_calls = {"n": 0}
        uid_by_token = {AT_A: UID_A, AT_B: UID_B}

        async def state_handler(request):
            n = state_calls["n"]
            state_calls["n"] += 1
            if n == 0:
                # A's state response is held back until after B has answered.
                first_started.set()
                await first_release.wait()
                return _ok({"state": STATE_A, "authUrl": AUTH_URL + "A"})
            return _ok({"state": STATE_B, "authUrl": AUTH_URL + "B"})

        async def token_handler(request):
            state = request.query.get("state")
            if state == STATE_A:
                return _ok(dict(_SUCCESS_BUNDLE_A))
            if state == STATE_B:
                return _ok(dict(_SUCCESS_BUNDLE_B))
            return _biz(1001, "login ing")

        async def account_handler(request):
            bearer = request.headers.get("Authorization") or ""
            token = bearer[len("Bearer "):] if bearer.startswith("Bearer ") else ""
            return _ok({"uid": uid_by_token.get(token, ""), "nickname": "T"})

        self.up.on("POST", STATE_PATH, state_handler)
        self.up.on("GET", TOKEN_PATH, token_handler)
        self.up.on("GET", ACCOUNT_PATH, account_handler)

        store = self._store()
        manager = self._make_manager(store)

        task_a = asyncio.create_task(manager.begin_login())
        await asyncio.wait_for(first_started.wait(), 5)

        result_b = await manager.begin_login()
        self.assertEqual(result_b["state"], STATE_B)

        first_release.set()
        try:
            await asyncio.wait_for(task_a, 5)
        except Exception:  # noqa: BLE001 - a late begin_login may report failure
            pass

        # The later begin_login (B) must remain the active state.
        with self.assertRaises(Exception):
            await manager.complete_login(STATE_A)
        self.assertIsNone(store.load())

        summary_b = await manager.complete_login(STATE_B)
        self.assertEqual(summary_b.get("uid"), UID_B)
        saved = store.load()
        self.assertIsNotNone(saved)
        self.assertEqual(saved.get("accessToken"), AT_B)

    # -- 3. cancelling complete_login must invalidate its state --------------

    async def test_cancelled_complete_login_invalidates_its_state(self):
        started = asyncio.Event()
        release = asyncio.Event()
        token_calls = {"n": 0}

        async def state_handler(request):
            return _ok({"state": STATE_A, "authUrl": AUTH_URL})

        async def token_handler(request):
            token_calls["n"] += 1
            if token_calls["n"] == 1:
                started.set()
                await release.wait()
                return _ok(dict(_SUCCESS_BUNDLE_A))
            return _biz(1001, "login ing")

        self.up.on("POST", STATE_PATH, state_handler)
        self.up.on("GET", TOKEN_PATH, token_handler)

        store = self._store()
        manager = self._make_manager(store, poll_interval=0.02, login_timeout=0.3)
        state = (await manager.begin_login())["state"]

        task = asyncio.create_task(manager.complete_login(state))
        await asyncio.wait_for(started.wait(), 5)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        release.set()

        # The cancelled state must be dead: a second attempt is rejected with no
        # further token polling and no credentials written.
        with self.assertRaises(Exception):
            await asyncio.wait_for(manager.complete_login(state), 5)
        self.assertEqual(self.up.hits[("GET", TOKEN_PATH)], 1)
        self.assertIsNone(store.load())

    # -- 4. signin 429 must keep the server cooldown ------------------------

    async def test_signin_claim_429_preserves_retry_after_when_recheck_still_unsigned(self):
        async def status_handler(request):
            return _ok({"active": True, "today_checked_in": False})

        async def claim_handler(request):
            return web.json_response(
                {"code": 4290, "msg": "slow down"},
                status=429,
                headers={"Retry-After": "7"},
            )

        self.up.on("POST", CHECKIN_STATUS_PATH, status_handler)
        self.up.on("POST", CHECKIN_CLAIM_PATH, claim_handler)

        with self.assertRaises(UpstreamError) as ctx:
            await SigninService(self._make_client()).run()

        self.assertEqual(ctx.exception.status, 429)
        self.assertEqual(float(ctx.exception.retry_after), 7.0)
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 1)

    async def test_signin_claim_429_then_signed_recheck_is_verified(self):
        await self._run_signin_with_statuses(
            [
                {"active": True, "today_checked_in": False},
                {"active": True, "today_checked_in": True, "streak_days": 3},
            ]
        )

        async def claim_handler(request):
            return web.json_response(
                {"code": 4290, "msg": "slow down"},
                status=429,
                headers={"Retry-After": "7"},
            )

        self.up.on("POST", CHECKIN_CLAIM_PATH, claim_handler)

        result = await SigninService(self._make_client()).run()

        self.assertTrue(result.get("verified"))
        self.assertIn(result.get("outcome"), ("signed", "already_signed"))
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 1)
        self.assertEqual(self.up.hits[("POST", CHECKIN_STATUS_PATH)], 2)

    # -- 5. a real HTTP timeout must be re-verified, not surfaced -----------

    async def test_signin_claim_real_http_timeout_rechecks_and_verifies(self):
        await self._run_signin_with_statuses(
            [
                {"active": True, "today_checked_in": False},
                {"active": True, "today_checked_in": True, "streak_days": 3},
            ]
        )

        release = asyncio.Event()
        self.addCleanup(release.set)

        async def claim_handler(request):
            # Hold the claim far past the client's timeout.
            try:
                await asyncio.wait_for(release.wait(), timeout=10)
            except asyncio.TimeoutError:
                pass
            return _ok({"credit": 1})

        self.up.on("POST", CHECKIN_CLAIM_PATH, claim_handler)

        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0.5))
        self._extra_sessions.append(session)
        store = self._store()
        manager = self._make_manager(store, session=session)
        client = self._make_client(manager=manager, session=session, store=store)

        result = await asyncio.wait_for(SigninService(client).run(), 10)
        release.set()

        self.assertTrue(result.get("verified"))
        self.assertIn(result.get("outcome"), ("signed", "already_signed"))
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 1)
        self.assertEqual(self.up.hits[("POST", CHECKIN_STATUS_PATH)], 2)

    # -- 6. non-zero claim business codes must be re-verified ---------------

    async def _assert_business_code_rechecks_and_verifies(self, code):
        await self._run_signin_with_statuses(
            [
                {"active": True, "today_checked_in": False},
                {"active": True, "today_checked_in": True, "streak_days": 3},
            ]
        )

        async def claim_handler(request):
            return _biz(code, "system busy")

        self.up.on("POST", CHECKIN_CLAIM_PATH, claim_handler)

        result = await SigninService(self._make_client()).run()

        self.assertTrue(result.get("verified"))
        self.assertIn(result.get("outcome"), ("signed", "already_signed"))
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 1)
        self.assertEqual(self.up.hits[("POST", CHECKIN_STATUS_PATH)], 2)

    async def test_signin_claim_code_10001_rechecks_and_verifies(self):
        await self._assert_business_code_rechecks_and_verifies(10001)

    async def test_signin_claim_code_14001_rechecks_and_verifies(self):
        await self._assert_business_code_rechecks_and_verifies(14001)

    # -- 7. upstream models catalog + balance -------------------------------

    @staticmethod
    def _model_ids(result):
        items = result
        if isinstance(result, dict):
            items = result.get("models") or result.get("data") or []
        ids = []
        for item in items or []:
            if isinstance(item, dict):
                ids.append(item.get("id"))
            elif isinstance(item, str):
                ids.append(item)
        return ids

    async def test_models_uses_real_catalog_and_filters_cli_chat_models(self):
        cli_order = [
            "wb-1",
            "cli-2",
            "wb-disabled",
            "nes-embed",
            "completion-x",
            "codewise-y",
            "text-to-image",
            "ghost-id",
        ]

        async def models_handler(request):
            return _ok(
                {
                    "models": [
                        {"id": "wb-1", "name": "WB One", "maxOutputTokens": 8192},
                        {"id": "cli-2", "name": "CLI Two", "maxOutputTokens": 8192},
                        {"id": "wb-disabled", "name": "Off", "disabled": True},
                        {"id": "nes-embed", "name": "Embed", "maxOutputTokens": 8192},
                        {"id": "completion-x", "name": "Complete", "maxOutputTokens": 8192},
                        {"id": "codewise-y", "name": "Code", "maxOutputTokens": 8192},
                        {"id": "text-to-image", "name": "Image", "tags": ["text-to-image"]},
                        {"id": "other-1", "name": "Other", "maxOutputTokens": 8192},
                    ],
                    "agents": [
                        {"name": "cli", "models": list(cli_order)},
                        {"name": "other", "models": ["other-1"]},
                    ],
                }
            )

        self.up.on("GET", MODELS_CATALOG_PATH, models_handler)

        client = self._make_client()
        result = await client.models()

        # The real enterprise catalog must be requested (GET), never fabricated.
        self.assertIn(
            ("GET", MODELS_CATALOG_PATH),
            [(call["method"], call["path"]) for call in self.up.calls],
        )
        ids = self._model_ids(result)
        self.assertEqual(ids, ["wb-1", "cli-2"])
        for excluded in (
            "wb-disabled",
            "nes-embed",
            "completion-x",
            "codewise-y",
            "text-to-image",
            "ghost-id",
            "other-1",
        ):
            self.assertNotIn(excluded, ids)

    async def test_balance_posts_resource_filters(self):
        async def balance_handler(request):
            return _ok(
                {
                    "Response": {
                        "Data": {
                            "Accounts": [{"CapacitySize": 2, "CapacityRemain": 1}],
                            "TotalCount": 1,
                        }
                    }
                }
            )

        self.up.on("POST", BALANCE_PATH, balance_handler)

        client = self._make_client()
        summary = await client.balance()

        self.assertTrue(summary["known"])
        self.assertEqual(summary["remaining"], 1)
        self.assertEqual(summary["used"], 1)
        self.assertEqual(summary["total"], 2)

        calls = self.up.calls_for("POST", BALANCE_PATH)
        self.assertEqual(len(calls), 1)
        body = self.up.json_body(calls[0])
        self.assertEqual(body["PageNumber"], 1)
        self.assertEqual(body["PageSize"], 100)
        self.assertEqual(body["ProductCode"], "p_tcaca")
        self.assertEqual(body["Status"], [0, 3])
        self.assertRegex(
            body["PackageEndTimeRangeBegin"],
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
        )
        self.assertRegex(
            body["PackageEndTimeRangeEnd"],
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
        )


if __name__ == "__main__":
    unittest.main()
