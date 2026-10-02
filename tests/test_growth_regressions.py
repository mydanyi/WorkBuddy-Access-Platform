"""Defect regression tests for the growth task runner.

Companion to ``tests/test_growth.py``.  It reuses that module's real
``_GrowthUpstream`` (and the shared sample credentials / frame helpers) but NOT
its test case, so none of the existing contract tests are executed twice.

Every scenario here pins behaviour the current production code
(``core.growth.GrowthService``) is known to get wrong.  The assertions describe
the intended contract and are expected to fail until the module is fixed and
re-verified remotely in the Docker host.  Nothing in this file mocks our own
production code: the fake *WorkBuddy upstream* is a real ``aiohttp.web`` server
and the runner sits behind a real ``CredentialStore`` / ``AuthManager`` /
``UpstreamClient`` / ``ChatService`` / ``StateStore``.  External server handlers
are overridden with small subclasses; the request components themselves are
never mocked.

Pinned defects:

1. a ``chat_5`` 0/5 task needs five *successful* chats -- ``max_retries`` bounds
   only failure retries, it never caps the number of real progress steps;
2. a single successful chat whose read-back progress never moves is
   ``progress_unconfirmed`` after one attempt, not replayed ``max_retries`` more
   times;
3. a task lacking ``target`` / ``current`` (and not ``complete``) is never
   claimed as ``0 >= 0``;
4. ``resume()`` clears only the pause, never the server-side 429 cooldown; a
   restart or a day boundary cannot bypass it;
5. a real 403/429 at list / models / accept / report is never swallowed into
   ``model_unavailable`` / ``None`` and never continues the run;
6. a claim whose 5xx / disconnect may already have credited is read back before
   any reward is (not) reported -- unknown credit/energy is never fabricated;
7. ``allowed_tasks=[]`` means "all whitelisted", an unknown-but-complete task is
   still claimable in ``claim_only``, and an explicit ``claim_only`` is a manual
   user authorization that ``auto_claim=False`` must not block;
8. a cancelled slow background run settles cleanly to ``running=False`` with a
   fixed interrupted/failed description, counting only its own task;
9. a failing pre-claim read-back (list 503) is never replaced by stale
   ``target`` data: an unverifiable completion stays ``claim_unconfirmed`` and no
   claim is sent;
10. a claim answered with HTTP 200 but a non-zero *business* code is definitive
   -- it is never retried like a 5xx and never fabricates a reward.

Every credential / token below is a FICTIONAL FIXED SAMPLE.

Temporary files use the NAS-hosted ``tmp`` root and honour
``WORKBUDDY_TEST_TMP``; the system temp dir is never used as a fallback.
"""

import asyncio
import contextlib
import json
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
from astrbot_plugin_workbuddy.core.chat import ChatService  # noqa: E402
from astrbot_plugin_workbuddy.core.client import UpstreamClient  # noqa: E402
from astrbot_plugin_workbuddy.core.growth import GrowthService  # noqa: E402
from astrbot_plugin_workbuddy.core.limits import ConcurrencyGate  # noqa: E402
from astrbot_plugin_workbuddy.core.storage import CredentialStore, StateStore  # noqa: E402

# Reuse the contract suite's real fixtures, but NOT its test case.
try:  # package discovery
    from tests.test_growth import (  # noqa: E402
        _GrowthUpstream,
        _Clock,
        _chat_sse,
        _flat,
        _nested,
        at,
        GROWTH_LIST_PATH,
        ACCEPT_PATH,
        REPORT_PATH,
        MODELS_CATALOG_PATH,
        CHAT_PATH,
        MASTER_KEY,
        UID,
        MODEL,
        BASE_TMP,
    )
except ImportError:  # top-level discovery
    from test_growth import (  # noqa: E402
        _GrowthUpstream,
        _Clock,
        _chat_sse,
        _flat,
        _nested,
        at,
        GROWTH_LIST_PATH,
        ACCEPT_PATH,
        REPORT_PATH,
        MODELS_CATALOG_PATH,
        CHAT_PATH,
        MASTER_KEY,
        UID,
        MODEL,
        BASE_TMP,
    )

try:  # sample credentials live with the chat gateway fixtures
    from tests.test_chat_gateway import SEED_CREDENTIALS  # noqa: E402
except ImportError:  # top-level discovery
    from test_chat_gateway import SEED_CREDENTIALS  # noqa: E402


class _ScriptedGrowthUpstream(_GrowthUpstream):
    """``_GrowthUpstream`` whose real handlers can be forced to fail.

    ``force`` maps a handler name (``"list"`` / ``"models"`` / ``"accept"`` /
    ``"report"``) to a ``(status, body, headers)`` triple.  Only the external
    server side is replaced; the runner and its request components are used
    unchanged.
    """

    def __init__(self) -> None:
        super().__init__()
        self.force = {}
        # ``list_fail_after=N``: the first N ``GET tasks`` calls answer normally,
        # every later one answers 503.  ``None`` disables the fault (the default),
        # so ``force["list"]`` and this knob are independent.
        self.list_fail_after = None
        self._list_ok = 0

    def _forced(self, name):
        return self.force.get(name)

    async def _list(self, request):
        forced = self._forced("list")
        if forced is not None:
            status, body, headers = forced
            self.hits[("GET", GROWTH_LIST_PATH)] += 1
            return web.json_response(body, status=status, headers=headers)
        if self.list_fail_after is not None:
            if self._list_ok >= self.list_fail_after:
                self.hits[("GET", GROWTH_LIST_PATH)] += 1
                return web.json_response(
                    {"code": 5030, "msg": "SECRET-BODY-TOKEN"}, status=503
                )
            self._list_ok += 1
        return await super()._list(request)

    async def _models(self, request):
        forced = self._forced("models")
        if forced is not None:
            status, body, headers = forced
            self.hits[("GET", MODELS_CATALOG_PATH)] += 1
            return web.json_response(body, status=status, headers=headers)
        return await super()._models(request)

    async def _accept(self, request):
        forced = self._forced("accept")
        if forced is not None:
            status, body, headers = forced
            self.hits[("POST", ACCEPT_PATH)] += 1
            return web.json_response(body, status=status, headers=headers)
        return await super()._accept(request)

    async def _report(self, request):
        forced = self._forced("report")
        if forced is not None:
            status, body, headers = forced
            self.hits[("POST", REPORT_PATH)] += 1
            return web.json_response(body, status=status, headers=headers)
        return await super()._report(request)


class _GrowthRegressionTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        BASE_TMP.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(
            tempfile.mkdtemp(prefix="wb-growth-reg-", dir=str(BASE_TMP))
        )
        self._extra_dirs = []
        self.up = _ScriptedGrowthUpstream()
        self.up.models = [MODEL]
        await self.up.start()
        self.session = aiohttp.ClientSession()
        self.store = CredentialStore(self.data_dir, MASTER_KEY)
        self.store.save(dict(SEED_CREDENTIALS))
        self.manager = AuthManager(
            self.session, self.store, chat_base=self.up.base, poll_interval=0.01
        )
        self.client = UpstreamClient(
            self.session,
            self.manager,
            chat_base=self.up.base,
            billing_base=self.up.base,
            web_base=self.up.base,
        )
        self.chat = ChatService(self.client)
        # Keep the shared gate out of the way: these tests are about growth task
        # semantics, not admission control (the default gate is pinned elsewhere).
        self.chat.limits = ConcurrencyGate(
            concurrency=8, queue_size=16, queue_timeout=5, rpm=600
        )
        self.clock = _Clock(at(2026, 10, 4, 10, 0))
        self._growths = []

    async def asyncTearDown(self) -> None:
        for growth in self._growths:
            with contextlib.suppress(Exception):
                await growth.cancel()
        await self.session.close()
        await self.up.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)
        for extra in self._extra_dirs:
            shutil.rmtree(extra, ignore_errors=True)

    # -- factories -----------------------------------------------------------
    def _fresh_state_dir(self) -> Path:
        directory = Path(
            tempfile.mkdtemp(prefix="wb-growth-sub-", dir=str(BASE_TMP))
        )
        self._extra_dirs.append(directory)
        return directory

    def _make(self, *, state_dir=None, **overrides) -> GrowthService:
        params = {
            "task_interval": 0.02,
            "poll_interval": 0.02,
            "progress_timeout": 0.3,
            "max_retries": 2,
            "max_chat_requests": 20,
            "auto_claim": True,
            "clock": self.clock,
        }
        params.update(overrides)
        growth = GrowthService(
            self.client, self.chat, StateStore(state_dir or self.data_dir), **params
        )
        self._growths.append(growth)
        return growth

    @staticmethod
    def _pending_codes(result) -> set:
        return {item.get("task_code") for item in result.get("pending", [])}

    @staticmethod
    def _action_outcome(result, code):
        for item in result.get("actions", []):
            if item.get("task_code") == code:
                return item.get("outcome")
        return None

    @staticmethod
    async def _wait_until(predicate, timeout):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if predicate():
                return True
            await asyncio.sleep(0.02)
        return predicate()

    # -- 1. five successful chats are needed; max_retries is not a step cap ---

    async def test_chat5_needs_five_successful_chats_not_limited_by_max_retries(self):
        self.up.tasks = [_nested("chat_5", 0, 5, accept_status="accepted")]

        def on_report(events):
            self.up.set_progress("chat_5", min(5, self.up.current("chat_5") + 1))

        self.up.on_report = on_report
        self.up.on_claim = lambda code: self.up.set_accept(code, "claimed")
        for index in range(6):
            self.up.chat_scripts.append(_chat_sse("x", "s{}".format(index)))

        growth = self._make(max_retries=0, max_chat_requests=20)
        result = await growth.run()

        # A normal success step is not a failure retry: even with max_retries=0
        # the runner must perform all five progress steps.
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 5)
        self.assertEqual(result["progress"]["chat_5"], {"current": 5, "target": 5})
        self.assertEqual(growth.status()["max_chat_requests"], 20)
        self.assertIn("chat_5", [c["task_code"] for c in self.up.claim_calls])

    # -- 2. a single success whose progress never moves is unconfirmed once ---

    async def test_single_success_without_progress_is_unconfirmed_once(self):
        self.up.tasks = [_nested("chat_5", 0, 1, accept_status="accepted")]
        # The report is accepted but the read-back progress never advances.
        self.up.on_report = lambda events: None
        self.up.chat_scripts.extend(
            [_chat_sse("a", "s1"), _chat_sse("b", "s2"), _chat_sse("c", "s3")]
        )

        growth = self._make(max_retries=2, progress_timeout=0.2, poll_interval=0.02)
        result = await growth.run()

        # One real chat + one report: no replays and no fabricated progress.
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 1)
        self.assertEqual(len(self.up.report_calls), 1)
        self.assertIn("chat_5", result["unconfirmed"])
        self.assertEqual(self._action_outcome(result, "chat_5"), "progress_unconfirmed")
        self.assertEqual(self.up.claim_calls, [])
        self.assertEqual(result["rewards"], [])

    # -- 3. missing progress is never claimed as 0 >= 0 -----------------------

    async def test_missing_progress_task_is_never_claimed(self):
        self.up.tasks = [{"task_code": "chat_5", "accept_status": "accepted"}]
        self.up.chat_scripts.append(_chat_sse("x", "s1"))

        growth = self._make(state_dir=self._fresh_state_dir())
        result = await growth.run()

        self.assertEqual(self.up.claim_calls, [])
        self.assertEqual(result["rewards"], [])
        # Missing target/current is an unknown-progress task, never a success.
        self.assertIn("progress_unknown", json.dumps(result, default=str))

        # The same holds for an explicit claim_only round.
        self.up.claim_calls = []
        claim_growth = self._make(state_dir=self._fresh_state_dir())
        claim_result = await claim_growth.run(claim_only=True)
        self.assertEqual(self.up.claim_calls, [])
        self.assertEqual(claim_result["rewards"], [])

    # -- 4. resume clears the pause, never the server 429 cooldown ------------

    async def test_429_resume_keeps_server_cooldown_across_restart_and_day(self):
        self.up.tasks = [_nested("chat_5", 0, 1, accept_status="accepted")]
        self.up.chat_scripts.append(
            {
                "kind": "status",
                "status": 429,
                "body": {"code": 4290, "msg": "slow"},
                "headers": {"Retry-After": "100000"},
            }
        )

        growth = self._make()
        await growth.run()
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 1)

        # resume() must only lift the pause; the hard cooldown is server-side.
        growth.resume()
        self.assertFalse(growth.status()["paused"])
        persisted = StateStore(self.data_dir).load().get("growth", {})
        self.assertIsNotNone(persisted.get("cooldown_until"))

        # An immediate re-run (same instance) sends no new action.
        self.up.chat_scripts.append(_chat_sse("ok", "s2"))
        await growth.run()
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 1)

        # A restart on the same state directory cannot bypass the cooldown.
        restarted = self._make()
        await restarted.run()
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 1)

        # Nor can crossing the day boundary while the cooldown is still live.
        self.clock.advance(24 * 3600)
        await restarted.run()
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 1)

        # Only after the cooldown elapses may a new action be sent.
        self.clock.advance(100001)
        await restarted.run()
        self.assertGreaterEqual(self.up.hits[("POST", CHAT_PATH)], 2)

    # -- 5. real 403 / 429 are never swallowed into a soft "continue" ----------

    async def test_real_403_429_are_never_swallowed(self):
        # (a) list 403 -- an auth failure must surface as a paused, fixed result.
        self.up.force["list"] = (403, {"code": 4030, "msg": "nope"}, {})
        self.up.tasks = [_nested("chat_5", 0, 1, accept_status="accepted")]
        self.up.chat_scripts.append(_chat_sse("x", "s1"))
        growth = self._make(state_dir=self._fresh_state_dir())
        try:
            result = await growth.run()
        except Exception as exc:  # noqa: BLE001 - a 403 must not escape run()
            self.fail("a 403 from the task list escaped run(): {!r}".format(exc))
        self.assertIsInstance(result, dict)
        self.assertTrue(growth.status()["paused"])
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 0)

        # (b) models 403 -- never downgraded to "model_unavailable".
        self.up.force.clear()
        self.up.force["models"] = (403, {"code": 4030, "msg": "nope"}, {})
        self.up.hits.clear()
        self.up.chat_scripts.append(_chat_sse("x", "s1"))
        growth = self._make(state_dir=self._fresh_state_dir())
        result = await growth.run()
        self.assertTrue(growth.status()["paused"])
        self.assertNotIn(
            "model_unavailable",
            json.dumps(result.get("pending", []), default=str),
        )

        # (c) accept 429 -- must install the rate-limit pause, not "accept_unconfirmed".
        self.up.force.clear()
        self.up.hits.clear()
        self.up.tasks = [_nested("chat_5", 0, 1, accept_status="not_accepted")]
        self.up.force["accept"] = (
            429,
            {"code": 4290, "msg": "slow"},
            {"Retry-After": "30"},
        )
        growth = self._make(state_dir=self._fresh_state_dir())
        result = await growth.run()
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 0)
        self.assertNotIn("accept_unconfirmed", json.dumps(result, default=str))
        self.assertTrue(growth.status()["paused"])

        # (d) report 429 -- surfaced as a fixed failure; the background entry
        # point settles with no unretrieved task exception and running=False.
        self.up.force.clear()
        self.up.hits.clear()
        self.up.tasks = [_nested("chat_5", 0, 1, accept_status="accepted")]
        self.up.on_report = None
        self.up.force["report"] = (
            429,
            {"code": 4290, "msg": "SECRET-BODY-TOKEN"},
            {"Retry-After": "30"},
        )
        self.up.chat_scripts.append(_chat_sse("x", "s1"))
        growth = self._make(state_dir=self._fresh_state_dir())
        try:
            result = await growth.run()
        except Exception as exc:  # noqa: BLE001 - a report 429 must not escape
            self.fail("a 429 from report escaped run(): {!r}".format(exc))
        self.assertIsInstance(result, dict)
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 1)
        self.assertEqual(self.up.claim_calls, [])
        self.assertNotIn("SECRET-BODY-TOKEN", json.dumps(result, default=str))

        self.up.hits.clear()
        self.up.chat_scripts.append(_chat_sse("x", "s2"))
        bg = self._make(state_dir=self._fresh_state_dir())
        started = bg.start()
        self.assertTrue(started.get("started"))
        await self._wait_until(lambda: bg.status()["running"] is False, 5.0)
        self.assertFalse(bg.status()["running"])
        if bg._task is not None:
            self.assertTrue(bg._task.done())
            self.assertIsNone(bg._task.exception())

    # -- 6. a possibly-credited 5xx claim is read back, never re-claimed ------

    async def test_5xx_claim_readback_verifies_without_fabricating_credit(self):
        self.up.tasks = [
            _nested("chat_5", 5, 5, accept_status="accepted", status="complete")
        ]

        def on_claim(code):
            # The server credits, then the connection breaks with a 503.
            self.up.set_accept(code, "claimed")
            self.up.claim_responses[code] = (
                503,
                {"code": 9001, "msg": "SECRET-BODY-TOKEN"},
                {},
            )

        self.up.on_claim = on_claim
        growth = self._make()
        result = await growth.run()

        self.assertEqual(len(self.up.claim_calls), 1)  # never re-claimed
        rewards = [r for r in result["rewards"] if r["task_code"] == "chat_5"]
        self.assertTrue(rewards, "a read-back-confirmed claim must report a reward")
        # The unknown credit / energy is never fabricated into a number.
        self.assertIsNone(rewards[0]["credit"])
        self.assertIsNone(rewards[0]["energy"])
        self.assertEqual(self._action_outcome(result, "chat_5"), "verified")
        self.assertNotIn("SECRET-BODY-TOKEN", json.dumps(result, default=str))

    # -- 6b. a failing pre-claim read-back must never claim on stale data ------

    async def test_list_failure_before_claim_never_claims(self):
        # The run's first list succeeds (the task looks complete); the fresh
        # read-back right before the claim then fails with a 503.
        self.up.tasks = [
            _nested("chat_5", 5, 5, accept_status="accepted", status="complete")
        ]
        self.up.list_fail_after = 1

        growth = self._make(state_dir=self._fresh_state_dir())
        result = await growth.run()

        # A read-back that cannot confirm the completion must not be replaced by
        # stale data: no claim is sent and the task stays unconfirmed.
        self.assertEqual(self.up.claim_calls, [])
        self.assertEqual(result["rewards"], [])
        self.assertIn("chat_5", result["unconfirmed"])

    # -- 6c. a 200 business-code claim error is definitive, not retriable ------

    async def test_business_code_claim_error_claims_once_without_retry(self):
        # The server keeps the task complete + accepted and answers the claim with
        # HTTP 200 but a non-zero *business* code: a definitive answer, not a 5xx.
        self.up.tasks = [
            _nested("chat_5", 5, 5, accept_status="accepted", status="complete")
        ]
        self.up.claim_responses["chat_5"] = (
            200,
            {"code": 90001, "msg": "SECRET-BODY-TOKEN"},
        )

        growth = self._make(state_dir=self._fresh_state_dir())
        result = await growth.run()

        # Exactly one claim; a business error is never retried like a 5xx and no
        # reward or raw upstream body is ever fabricated or leaked.
        self.assertEqual(len(self.up.claim_calls), 1)
        self.assertEqual(result["rewards"], [])
        self.assertIn("chat_5", result["unconfirmed"])
        self.assertNotIn("SECRET-BODY-TOKEN", json.dumps(result, default=str))

    # -- 7. allowed_tasks=[] + claim_only semantics ---------------------------

    async def test_allowed_tasks_empty_is_all_and_claim_only_semantics(self):
        # (a) an empty allowed_tasks list is "all whitelisted", not "all disabled".
        self.up.tasks = [_nested("chat_5", 0, 1, accept_status="accepted")]
        self.up.on_report = lambda events: self.up.set_progress(
            "chat_5", min(1, self.up.current("chat_5") + 1)
        )
        self.up.chat_scripts.append(_chat_sse("x", "s1"))
        growth = self._make(state_dir=self._fresh_state_dir(), allowed_tasks=[])
        await growth.run()
        self.assertGreaterEqual(self.up.hits[("POST", CHAT_PATH)], 1)

        # (b) an unknown-but-complete task is claimable in claim_only (no action).
        self.up.hits.clear()
        self.up.tasks = [
            _nested("done_task", 3, 3, accept_status="accepted", status="complete")
        ]
        self.up.on_claim = lambda code: self.up.set_accept(code, "claimed")
        self.up.claim_calls = []
        growth = self._make(state_dir=self._fresh_state_dir(), allowed_tasks=None)
        await growth.run(claim_only=True)
        self.assertEqual(
            [c["task_code"] for c in self.up.claim_calls], ["done_task"]
        )
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 0)

        # (c) an explicit claim_only is a manual user authorization:
        # auto_claim=False must not block it.
        self.up.tasks = [
            _nested("done_task", 3, 3, accept_status="accepted", status="complete")
        ]
        self.up.claim_calls = []
        growth = self._make(
            state_dir=self._fresh_state_dir(), auto_claim=False
        )
        await growth.run(claim_only=True)
        self.assertIn(
            "done_task", [c["task_code"] for c in self.up.claim_calls]
        )

        # (d) an unknown / manual *incomplete* task is always skipped.
        self.up.tasks = [_flat("mystery_task", 0, 1, accept_status="not_accepted")]
        self.up.accept_calls = []
        self.up.claim_calls = []
        self.up.hits.clear()
        growth = self._make(state_dir=self._fresh_state_dir())
        result = await growth.run()
        self.assertIn("mystery_task", self._pending_codes(result))
        self.assertEqual(self.up.accept_calls, [])
        self.assertEqual(self.up.claim_calls, [])
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 0)

    # -- 8. a cancelled slow background run settles cleanly -------------------

    async def test_cancel_slow_background_run_settles_clean(self):
        self.up.tasks = [_nested("chat_5", 0, 1, accept_status="accepted")]
        self.up.chat_scripts.append(_chat_sse("slow", "s1", delay=5.0))

        growth = self._make(progress_timeout=10.0)
        started = growth.start()
        self.assertTrue(started.get("started"))

        # Wait until the in-flight chat request is genuinely underway.
        await self._wait_until(
            lambda: self.up.hits[("POST", CHAT_PATH)] >= 1, 5.0
        )
        await growth.cancel()

        status = growth.status()
        self.assertFalse(status["running"])
        blob = json.dumps(status, default=str).lower()
        self.assertTrue(
            status.get("last_result") is not None
            or "interrupted" in blob
            or "failed" in blob,
            "a cancelled run must leave a fixed interrupted/failed description",
        )
        # Only the runner's own task is tracked; it must be done after cancel.
        self.assertTrue(growth._task is None or growth._task.done())


if __name__ == "__main__":
    unittest.main()
