"""Executable contract tests for the daily check-in scheduler.

Scope: only the documented public contract of ``core.scheduler.SigninScheduler``.
The production module does not exist yet on purpose: the top-level import below
is expected to fail until ``core/scheduler.py`` is implemented and verified
remotely in the Docker host.  Do not "fix" that here.

Pinned interface (from the approved small design)::

    SigninScheduler(service, state, timezone="Asia/Shanghai",
                    primary="00:10", recovery="12:10", jitter=120,
                    max_attempts=5, enabled=True, clock=None, on_success=None)

    await run_once(manual=False)   # one check-in round, serialized
    await tick()                   # one scheduler decision point
    start()                        # non-async: ticks once, then bounded sleeps
    await stop()                   # cancels the loop and any in-flight round
    resume()                       # clear a pause / awaiting-authorization state
    status() -> dict               # {"enabled","paused","date",
                                   #  "attempts_today","last_success_date"}

Boundary substitutions only:

* ``service`` is the *external* check-in collaborator; the tests script its
  return values / raised errors and count how often ``run()`` was invoked.  No
  scheduler code is mocked.
* ``clock`` is a zero-argument callable returning POSIX seconds (UTC).  Time is
  only ever moved at this boundary.
* ``state`` is a real :class:`StateStore` on the NAS-hosted ``tmp`` root; a
  "restart" is a second scheduler reading the same directory.

Everything below is a FICTIONAL FIXED SAMPLE (dates, account, tokens).
"""

import asyncio
import collections
import contextlib
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# --- Make ``astrbot_plugin_workbuddy`` importable when discovered from anywhere.
_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]  # directory that contains astrbot_plugin_workbuddy/
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.errors import UpstreamError  # noqa: E402
from astrbot_plugin_workbuddy.core.scheduler import SigninScheduler  # noqa: E402
from astrbot_plugin_workbuddy.core.storage import StateStore  # noqa: E402


def _resolve_base_tmp() -> Path:
    """Temp root: env override, else the current task's NAS tmp dir."""
    env = os.environ.get("WORKBUDDY_TEST_TMP")
    if env:
        return Path(env)
    task_root = _HERE.parents[3]
    return task_root / "tmp"


BASE_TMP = _resolve_base_tmp()
TZ = ZoneInfo("Asia/Shanghai")
UTC = timezone.utc


def at(year, month, day, hour, minute, second=0) -> datetime:
    """A Shanghai-local wall-clock instant."""
    return datetime(year, month, day, hour, minute, second, tzinfo=TZ)


class FakeClock:
    """Controllable clock boundary returning POSIX seconds (UTC)."""

    def __init__(self, moment_utc: datetime):
        self.set(moment_utc)

    def set(self, moment_utc: datetime) -> None:
        self._moment = moment_utc.astimezone(UTC)

    def advance(self, seconds: float = 0, **kwargs) -> None:
        self._moment = self._moment + timedelta(seconds=seconds, **kwargs)

    def __call__(self) -> float:
        return self._moment.timestamp()


class ScriptedService:
    """External check-in collaborator returning a scripted series of results."""

    def __init__(self, script=None):
        self._script = collections.deque(script or [])
        self.calls = 0

    async def run(self):
        self.calls += 1
        item = (
            self._script.popleft()
            if self._script
            else {"outcome": "unconfirmed", "verified": False}
        )
        if isinstance(item, BaseException):
            raise item
        return dict(item)


class BlockingService:
    """Service whose single in-flight ``run`` waits on a gate (concurrency probe)."""

    def __init__(self, gate: asyncio.Event, outcome=None):
        self._gate = gate
        self._outcome = outcome or {"outcome": "unconfirmed", "verified": False}
        self.calls = 0
        self.active = 0
        self.max_active = 0
        self.started = asyncio.Event()

    async def run(self):
        self.calls += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.started.set()
        try:
            await self._gate.wait()
            return dict(self._outcome)
        finally:
            self.active -= 1


class _SchedulerTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        BASE_TMP.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(tempfile.mkdtemp(prefix="wb-sched-", dir=str(BASE_TMP)))

    async def asyncTearDown(self) -> None:
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def _state(self) -> StateStore:
        return StateStore(self.data_dir)

    def _make(self, service, *, clock, **overrides) -> SigninScheduler:
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
        return SigninScheduler(service, self._state(), **params)

    # -- timezone / date -------------------------------------------------

    async def test_status_date_follows_configured_timezone_across_midnight(self):
        clock = FakeClock(datetime(2026, 10, 3, 16, 30, tzinfo=UTC))  # 10-04 00:30 +08
        scheduler = self._make(ScriptedService(), clock=clock)
        self.assertEqual(scheduler.status()["date"], "2026-10-04")

        clock.set(datetime(2026, 10, 3, 15, 0, tzinfo=UTC))  # 10-03 23:00 +08
        self.assertEqual(scheduler.status()["date"], "2026-10-03")

    async def test_jitter_delays_the_primary_window_within_bound(self):
        service = ScriptedService([{"outcome": "signed", "verified": True}])
        clock = FakeClock(at(2026, 10, 4, 0, 9))
        scheduler = self._make(service, clock=clock, jitter=120)

        await scheduler.tick()  # before the 00:10 primary instant
        self.assertEqual(service.calls, 0)

        clock.set(at(2026, 10, 4, 0, 13))  # past 00:10 + max 120s jitter
        await scheduler.tick()
        self.assertEqual(service.calls, 1)

    async def test_late_start_catches_up_only_for_the_current_local_day(self):
        service = ScriptedService([{"outcome": "signed", "verified": True}])
        clock = FakeClock(at(2026, 10, 4, 8, 0))
        scheduler = self._make(service, clock=clock)

        await scheduler.tick()
        self.assertEqual(service.calls, 1)
        self.assertEqual(scheduler.status()["last_success_date"], "2026-10-04")
        self.assertTrue((self.data_dir / "state.json").is_file())

    # -- success / inactive ---------------------------------------------

    async def test_success_stops_further_rounds_for_the_day(self):
        service = ScriptedService([{"outcome": "signed", "verified": True}])
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock)

        await scheduler.run_once(manual=True)
        self.assertEqual(service.calls, 1)

        await scheduler.tick()
        await scheduler.run_once(manual=True)
        self.assertEqual(service.calls, 1)  # success for today: no re-claim
        self.assertEqual(scheduler.status()["last_success_date"], "2026-10-04")

    async def test_no_second_claim_at_recovery_after_primary_success(self):
        service = ScriptedService([{"outcome": "signed", "verified": True}])
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock)

        await scheduler.tick()
        self.assertEqual(service.calls, 1)

        clock.set(at(2026, 10, 4, 12, 15))
        await scheduler.tick()
        self.assertEqual(service.calls, 1)

    async def test_inactive_stops_today_without_a_success_reward(self):
        service = ScriptedService(
            [{"outcome": "inactive", "verified": False}, {"outcome": "signed", "verified": True}]
        )
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock)

        await scheduler.run_once(manual=True)
        self.assertEqual(service.calls, 1)

        await scheduler.tick()
        await scheduler.run_once(manual=True)
        self.assertEqual(service.calls, 1)  # inactive today: no further claim
        self.assertIsNone(scheduler.status()["last_success_date"])

    # -- attempt budget --------------------------------------------------

    async def test_attempt_budget_is_shared_and_survives_a_restart(self):
        service = ScriptedService([{"outcome": "unconfirmed", "verified": False}] * 8)
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock, max_attempts=5)

        for _ in range(3):
            await scheduler.run_once(manual=True)  # auto-counted
        for _ in range(2):
            await scheduler.run_once(manual=True)  # manual shares the same budget
        self.assertEqual(service.calls, 5)

        await scheduler.run_once(manual=True)
        await scheduler.tick()
        self.assertEqual(service.calls, 5)  # budget exhausted for the day

        rebuilt_service = ScriptedService([{"outcome": "signed", "verified": True}])
        rebuilt = self._make(rebuilt_service, clock=clock, max_attempts=5)
        await rebuilt.run_once(manual=True)
        self.assertEqual(rebuilt_service.calls, 0)  # budget persisted in StateStore

    # -- pause / authorization ------------------------------------------

    async def test_403_pauses_across_a_restart_until_resume(self):
        service = ScriptedService(
            [UpstreamError(403, 4030), {"outcome": "signed", "verified": True}]
        )
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock)

        await scheduler.run_once(manual=True)
        self.assertEqual(service.calls, 1)
        self.assertTrue(scheduler.status()["paused"])

        await scheduler.tick()
        await scheduler.run_once(manual=True)
        self.assertEqual(service.calls, 1)  # paused blocks further rounds

        rebuilt = self._make(service, clock=clock)
        self.assertTrue(rebuilt.status()["paused"])
        await rebuilt.run_once(manual=True)
        self.assertEqual(service.calls, 1)  # pause persisted across the restart

        rebuilt.resume()
        self.assertFalse(rebuilt.status()["paused"])
        await rebuilt.run_once(manual=True)
        self.assertEqual(service.calls, 2)

    async def test_notlogin_waits_for_authorization_until_resume(self):
        service = ScriptedService(
            [{"outcome": "notlogin", "verified": False}, {"outcome": "signed", "verified": True}]
        )
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock)

        await scheduler.run_once(manual=True)
        self.assertEqual(service.calls, 1)

        await scheduler.tick()
        self.assertEqual(service.calls, 1)  # waits for authorization

        scheduler.resume()
        await scheduler.run_once(manual=True)
        self.assertEqual(service.calls, 2)

    async def test_unknown_outcome_is_not_retried_blindly(self):
        service = ScriptedService([{"outcome": "unknown", "verified": False}])
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock)

        await scheduler.tick()
        self.assertEqual(service.calls, 1)

        await scheduler.tick()  # one tick == one round; never a spin loop
        self.assertEqual(service.calls, 1)

    # -- rate limit / backoff -------------------------------------------

    async def test_429_honours_retry_after_across_a_day_boundary(self):
        service = ScriptedService(
            [UpstreamError(429, 4290, retry_after=3600), {"outcome": "signed", "verified": True}]
        )
        clock = FakeClock(at(2026, 10, 3, 23, 50))
        scheduler = self._make(service, clock=clock)

        await scheduler.run_once(manual=True)
        self.assertEqual(service.calls, 1)

        clock.set(at(2026, 10, 4, 0, 15))  # next day, past primary, still cooling down
        await scheduler.tick()
        self.assertEqual(service.calls, 1)

        clock.set(at(2026, 10, 4, 0, 55))  # 23:50 + 3600s = 00:50 elapsed
        await scheduler.tick()
        self.assertEqual(service.calls, 2)

    async def test_network_backoff_is_bounded_to_60_300_1800(self):
        service = ScriptedService([UpstreamError(503, 9001)] * 8)
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock, max_attempts=10)

        await scheduler.run_once(manual=True)  # failure #1 -> next in 60s
        self.assertEqual(service.calls, 1)

        clock.advance(59)
        await scheduler.tick()
        self.assertEqual(service.calls, 1)
        clock.advance(2)
        await scheduler.tick()
        self.assertEqual(service.calls, 2)  # failure #2 -> next in 300s

        clock.advance(299)
        await scheduler.tick()
        self.assertEqual(service.calls, 2)
        clock.advance(2)
        await scheduler.tick()
        self.assertEqual(service.calls, 3)  # failure #3 -> next in 1800s

        clock.advance(1799)
        await scheduler.tick()
        self.assertEqual(service.calls, 3)
        clock.advance(2)
        await scheduler.tick()
        self.assertEqual(service.calls, 4)  # failure #4 -> capped at 1800s

        clock.advance(1799)
        await scheduler.tick()
        self.assertEqual(service.calls, 4)
        clock.advance(2)
        await scheduler.tick()
        self.assertEqual(service.calls, 5)

    # -- serialization / shutdown ---------------------------------------

    async def test_concurrent_rounds_never_overlap(self):
        gate = asyncio.Event()
        service = BlockingService(gate, outcome={"outcome": "signed", "verified": True})
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock)

        first = asyncio.ensure_future(scheduler.run_once(manual=True))
        await asyncio.wait_for(service.started.wait(), 2)
        ticker = asyncio.ensure_future(scheduler.tick())
        await asyncio.sleep(0.05)
        self.assertEqual(service.max_active, 1)  # never two rounds at once

        gate.set()
        await asyncio.gather(first, ticker)
        self.assertEqual(service.max_active, 1)
        self.assertEqual(service.calls, 1)  # the overlap was deferred, not doubled

    async def test_stop_cancels_inflight_and_keeps_the_started_attempt(self):
        gate = asyncio.Event()
        service = BlockingService(gate)
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock)

        task = asyncio.ensure_future(scheduler.run_once(manual=True))
        await asyncio.wait_for(service.started.wait(), 2)
        await scheduler.stop()
        with contextlib.suppress(BaseException):
            await task

        self.assertEqual(service.calls, 1)
        self.assertGreaterEqual(scheduler.status()["attempts_today"], 1)
        gate.set()

    # -- start / config --------------------------------------------------

    async def test_start_is_synchronous_and_ticks_once(self):
        service = ScriptedService([{"outcome": "signed", "verified": True}])
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock)

        self.assertFalse(asyncio.iscoroutinefunction(scheduler.start))
        self.assertIsNone(scheduler.start())

        for _ in range(50):
            if service.calls:
                break
            await asyncio.sleep(0.02)
        self.assertEqual(service.calls, 1)
        await scheduler.stop()

    def test_invalid_time_and_timezone_are_rejected(self):
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        with self.assertRaises(Exception):
            self._make(ScriptedService(), clock=clock, primary="25:99")
        with self.assertRaises(Exception):
            self._make(ScriptedService(), clock=clock, timezone="Not/AZone")


if __name__ == "__main__":
    unittest.main()
