"""Defect regression tests for the daily check-in scheduler.

Companion to ``tests/test_scheduler.py``.  It reuses that module's real
``FakeClock`` / ``ScriptedService`` / ``BlockingService`` fixtures but defines its
*own* test case, so none of the existing contract tests are executed twice.

Every scenario here pins behaviour the current production code
(``core.scheduler.SigninScheduler``) is known to get wrong.  The assertions
describe the intended contract and are expected to fail until the module is fixed
and re-verified remotely in the Docker host.  Nothing in this file mocks the
scheduler under test: the service is the external collaborator and ``state`` is a
real :class:`StateStore` on the NAS-hosted ``tmp`` root.

The two discovered behaviour gaps pinned here:

1. ``enabled=False`` must only stop the *automatic* tick.  A manual round still
   drives the same service and consumes the same persisted per-day attempt
   budget; a manual round may never bypass a pause or a hard 429 cooldown (this
   is the "auto check-in switch" setting, not "disable the manual command").
2. ``run_once`` must return a safe result dict, and ``status()`` must expose the
   last outcome so ``/wb signin`` never reports an unconfirmed round as done.  A
   round may not stamp *today* as ``last_success_date`` when its response only
   arrived after the midnight boundary of the business date it started under.

Every date / token below is a FICTIONAL FIXED SAMPLE.

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

# --- Make ``astrbot_plugin_workbuddy`` importable when discovered from anywhere.
_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]  # directory that contains astrbot_plugin_workbuddy/
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.errors import UpstreamError  # noqa: E402
from astrbot_plugin_workbuddy.core.scheduler import SigninScheduler  # noqa: E402
from astrbot_plugin_workbuddy.core.storage import StateStore  # noqa: E402

# Reuse the contract suite's real fixtures, but NOT its test case (subclassing
# ``_SchedulerTestCase`` would re-run every contract test).
try:  # package discovery: ``python -m unittest discover`` from the plugin dir.
    from tests.test_scheduler import (  # noqa: E402
        BlockingService,
        FakeClock,
        ScriptedService,
        BASE_TMP,
        at,
    )
except ImportError:  # top-level discovery: ``python -m unittest discover -s tests``.
    from test_scheduler import (  # noqa: E402
        BlockingService,
        FakeClock,
        ScriptedService,
        BASE_TMP,
        at,
    )


class _SchedulerRegressionTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        BASE_TMP.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(
            tempfile.mkdtemp(prefix="wb-sched-reg-", dir=str(BASE_TMP))
        )
        self._extra_dirs = []

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.data_dir, ignore_errors=True)
        for extra in self._extra_dirs:
            shutil.rmtree(extra, ignore_errors=True)

    def _state(self) -> StateStore:
        return StateStore(self.data_dir)

    def _fresh_dir(self) -> Path:
        """A second, independent real StateStore directory (own persisted state)."""
        directory = Path(
            tempfile.mkdtemp(prefix="wb-sched-sub-", dir=str(BASE_TMP))
        )
        self._extra_dirs.append(directory)
        return directory

    def _make(self, service, *, clock, state_dir=None, **overrides) -> SigninScheduler:
        params = {
            "timezone": "Asia/Shanghai",
            "primary": "00:10",
            "recovery": "12:10",
            "jitter": 0,
            "max_attempts": 5,
            "enabled": True,
            "clock": clock,
        }
        params.update(overrides)
        return SigninScheduler(
            service, StateStore(state_dir or self.data_dir), **params
        )

    # -- 1. enabled=False disables *automatic* rounds, never the manual one ---

    async def test_enabled_false_still_allows_manual_rounds_against_same_budget(self):
        service = ScriptedService(
            [{"outcome": "unconfirmed", "verified": False}] * 8
        )
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock, enabled=False, max_attempts=5)

        # The "auto check-in switch" is off, but a manual round must still run.
        await scheduler.run_once(manual=True)
        self.assertEqual(service.calls, 1)
        self.assertEqual(scheduler.status()["attempts_today"], 1)

        # ...while the automatic tick stays disabled.
        await scheduler.tick()
        self.assertEqual(service.calls, 1)

        # Manual rounds share the very same persisted 5-round budget.
        for _ in range(4):
            await scheduler.run_once(manual=True)
        self.assertEqual(service.calls, 5)
        await scheduler.run_once(manual=True)
        self.assertEqual(service.calls, 5)  # budget exhausted for the day

    async def test_manual_round_never_bypasses_pause_or_429_cooldown(self):
        # A 403 pauses every round (auto and manual) until resume().
        pause_service = ScriptedService(
            [UpstreamError(403, 4030), {"outcome": "signed", "verified": True}]
        )
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        paused = self._make(pause_service, clock=clock)
        await paused.run_once(manual=True)
        self.assertEqual(pause_service.calls, 1)
        self.assertTrue(paused.status()["paused"])
        await paused.run_once(manual=True)
        self.assertEqual(pause_service.calls, 1)  # a pause blocks manual rounds too

        # A hard 429 cooldown survives and blocks manual rounds until it elapses.
        cool_service = ScriptedService(
            [
                UpstreamError(429, 4290, retry_after=3600),
                {"outcome": "signed", "verified": True},
            ]
        )
        cool_clock = FakeClock(at(2026, 10, 4, 0, 15))
        cooling = self._make(cool_service, clock=cool_clock, state_dir=self._fresh_dir())
        await cooling.run_once(manual=True)
        self.assertEqual(cool_service.calls, 1)
        await cooling.run_once(manual=True)
        self.assertEqual(cool_service.calls, 1)  # still cooling down

        cool_clock.advance(3601)
        await cooling.run_once(manual=True)
        self.assertEqual(cool_service.calls, 2)

    # -- 2. safe result dict + status outcome --------------------------------

    async def test_run_once_returns_a_safe_summary_and_status_exposes_outcome(self):
        secret = "FICTIONAL-SECRET-ACCESS-TOKEN"
        service = ScriptedService(
            [{"outcome": "unconfirmed", "verified": False, "accessToken": secret}]
        )
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock)

        result = await scheduler.run_once(manual=True)
        self.assertIsInstance(result, dict)
        # An unconfirmed round must never be reported as a completed sign-in.
        self.assertFalse(bool(result.get("verified")))
        self.assertEqual(result.get("outcome"), "unconfirmed")
        # The raw collaborator dict (which may carry a token) is never echoed.
        blob = json.dumps(result, default=str)
        self.assertNotIn(secret, blob)

        status = scheduler.status()
        self.assertTrue(
            "last_outcome" in status or "last_result" in status,
            "status() must expose the last outcome for /wb signin",
        )
        self.assertNotIn(secret, json.dumps(status, default=str))

        # Network / business failures keep a fixed, desensitized summary.
        failing = ScriptedService([UpstreamError(429, 4290, retry_after=7)])
        failing_scheduler = self._make(
            failing, clock=FakeClock(at(2026, 10, 4, 0, 16))
        )
        failed_result = await failing_scheduler.run_once(manual=True)
        self.assertIsInstance(failed_result, dict)
        self.assertFalse(bool(failed_result.get("verified")))
        self.assertNotIn(secret, json.dumps(failed_result, default=str))

    async def test_success_is_verified_then_already_signed_and_callback_runs_once(self):
        callbacks = []

        def on_success():
            callbacks.append(1)

        service = ScriptedService(
            [
                {"outcome": "signed", "verified": True},
                {"outcome": "signed", "verified": True},
            ]
        )
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock, on_success=on_success)

        first = await scheduler.run_once(manual=True)
        self.assertTrue(first.get("verified"))
        self.assertEqual(service.calls, 1)
        self.assertEqual(scheduler.status()["last_success_date"], "2026-10-04")
        self.assertEqual(len(callbacks), 1)

        # A later round the same day is a no-op that still reports a safe,
        # verified summary -- never a fresh unconfirmed claim.
        second = await scheduler.run_once(manual=True)
        self.assertEqual(service.calls, 1)
        self.assertIsInstance(second, dict)
        self.assertTrue(
            second.get("verified") is True or second.get("outcome") == "already_signed"
        )
        self.assertEqual(len(callbacks), 1)  # on_success fires once per day

        # A failing callback never reverts a confirmed sign-in.
        async def boom():
            raise RuntimeError("callback boom")

        resilient = ScriptedService([{"outcome": "signed", "verified": True}])
        resilient_scheduler = self._make(
            resilient,
            clock=FakeClock(at(2026, 10, 4, 0, 20)),
            on_success=boom,
            state_dir=self._fresh_dir(),
        )
        result = await resilient_scheduler.run_once(manual=True)
        # The service really ran and produced a verified success despite the
        # exploding callback -- this is not a replay of the previous signed day.
        self.assertEqual(resilient.calls, 1)
        self.assertTrue(result.get("verified"))
        self.assertEqual(
            resilient_scheduler.status()["last_success_date"], "2026-10-04"
        )

    # -- 3. cross-midnight round records the business date it started under ---

    async def test_cross_midnight_round_records_start_date_not_new_day(self):
        gate = asyncio.Event()
        service = BlockingService(
            gate, outcome={"outcome": "signed", "verified": True}
        )
        clock = FakeClock(at(2026, 10, 4, 23, 59, 50))
        scheduler = self._make(service, clock=clock, max_attempts=5)

        round_task = asyncio.ensure_future(scheduler.run_once(manual=True))
        await asyncio.wait_for(service.started.wait(), 5)
        # The network response only arrives after the local midnight boundary.
        clock.set(at(2026, 10, 5, 0, 0, 30))
        gate.set()
        with contextlib.suppress(BaseException):
            await round_task

        # Success belongs to the business date the round *started* under.
        self.assertEqual(scheduler.status()["last_success_date"], "2026-10-04")

        # The next day is still queryable / executable -- a round that started
        # on the previous day must not mark the new day as already done.
        before = service.calls
        clock.set(at(2026, 10, 5, 0, 15))
        await scheduler.run_once(manual=True)
        self.assertEqual(service.calls, before + 1)
        self.assertEqual(scheduler.status()["date"], "2026-10-05")
        # The second (fresh) round really succeeded on 2026-10-05, so its own
        # start date -- not the previous day -- is what gets recorded.
        self.assertEqual(scheduler.status()["last_success_date"], "2026-10-05")


if __name__ == "__main__":
    unittest.main()
