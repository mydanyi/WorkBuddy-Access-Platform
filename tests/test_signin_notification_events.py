"""Executable contract tests for the approved check-in reward + scheduler events.

Scope: the documented *new* contract only.

* ``core.signin.SigninService.run`` attaches ``reward_credits`` **only** when a
  fresh claim returned a valid ``credit`` and a follow-up status read confirmed a
  signature.  The credit is a finite, non-negative ``int``/``float`` (``bool``
  excluded); missing / invalid / unconfirmed values are omitted, never guessed.
  Only the ``credit`` field is trusted -- ``reward``/``points``/``token`` and any
  other raw upstream field must never be echoed into the result.
* ``core.scheduler.SigninScheduler`` gains an optional ``on_result(summary)``
  callback fired once per *genuinely executed* round, carrying a safe summary
  (``outcome``, ``verified``, ``date``; ``reward_credits`` when signed;
  ``http_status`` for an upstream HTTP error; ``retry_after`` for
  rate-limit/backoff).  Blocked ticks and same-day duplicates do not fire it.
  The legacy ``on_success()`` (no-arg, at most once per business date) is kept.

These tests are the executable contract for the reward and result-event
behaviour described above.  They drive the real production code through the
boundary substitutions below and must pass unchanged.

Boundary substitutions only (no production method is mocked):

* the *external* billing client is a tiny scripted fake returning already
  unwrapped envelope payloads / raising transport errors;
* ``ScriptedService`` and ``FakeClock`` are small local fixtures (mirroring
  ``tests/test_scheduler.py`` but not subclassing its test classes);
* ``state`` is the real :class:`StateStore` on a ``WORKBUDDY_TEST_TMP`` (or
  project-local ``tmp``) root; a "restart" is a second scheduler reading the
  same directory.

Everything below is a FICTIONAL FIXED SAMPLE (account, tokens, credits).
"""

import asyncio
import collections
import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp

# --- Make ``astrbot_plugin_workbuddy`` importable when discovered from anywhere.
_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]  # directory that contains astrbot_plugin_workbuddy/
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.errors import UpstreamError  # noqa: E402
from astrbot_plugin_workbuddy.core.scheduler import SigninScheduler  # noqa: E402
from astrbot_plugin_workbuddy.core.signin import (  # noqa: E402
    CHECKIN_CLAIM_PATH,
    CHECKIN_STATUS_PATH,
    SigninService,
)
from astrbot_plugin_workbuddy.core.storage import StateStore  # noqa: E402


def _resolve_base_tmp() -> Path:
    """Temp root: ``WORKBUDDY_TEST_TMP`` env override, else a project-local tmp."""
    env = os.environ.get("WORKBUDDY_TEST_TMP")
    if env:
        return Path(env)
    task_root = _HERE.parents[3]
    return task_root / "tmp"


BASE_TMP = _resolve_base_tmp()
TZ = ZoneInfo("Asia/Shanghai")
UTC = timezone.utc

# FICTIONAL FIXED SAMPLE -- never a real credential.
SECRET = "FICTIONAL-RAW-UPSTREAM-TOKEN-0001"


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


class ScriptedClient:
    """External billing client returning a scripted series of payloads.

    Each item is either an already-unwrapped envelope ``dict`` (success) or a
    ``BaseException`` instance to raise (transport / business error).  Every call
    is recorded so the tests can assert *which* endpoint was hit.
    """

    def __init__(self, script=()):
        self.billing_base = "https://billing.invalid.test"
        self._script = collections.deque(script)
        self.calls = []

    async def request_json(self, method, path, base=None, json=None):
        self.calls.append({"method": method, "path": path, "base": base, "json": json})
        item = self._script.popleft() if self._script else None
        if isinstance(item, BaseException):
            raise item
        return dict(item) if isinstance(item, dict) else item

    def hits(self, path) -> int:
        return sum(1 for c in self.calls if c["path"] == path)


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


class _SigninEventsTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        BASE_TMP.mkdir(parents=True, exist_ok=True)
        self._dirs = []
        self.data_dir = self._new_dir()

    async def asyncTearDown(self) -> None:
        for d in self._dirs:
            shutil.rmtree(d, ignore_errors=True)

    def _new_dir(self) -> Path:
        d = Path(tempfile.mkdtemp(prefix="wb-signin-notify-", dir=str(BASE_TMP)))
        self._dirs.append(d)
        return d

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

    def _fresh(self, service, *, clock, **overrides) -> SigninScheduler:
        """A scheduler on its own fresh data dir (isolated error scenarios)."""
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
        return SigninScheduler(service, StateStore(self._new_dir()), **params)

    @staticmethod
    def _signed_round(credit=10):
        return [
            {"active": True, "today_checked_in": False},
            {"credit": credit},
            {"active": True, "today_checked_in": True},
        ]

    # -- SigninService: reward_credits contract (1, 2) -------------------

    async def test_credit_is_attached_after_verified_signed(self):
        for good in (0, 10, 12.5, 3.0):
            with self.subTest(credit=good):
                client = ScriptedClient(self._signed_round(good))
                result = await SigninService(client).run()
                self.assertEqual(result.get("outcome"), "signed")
                self.assertTrue(result.get("verified"))
                # The 0 credit must be *kept*, not treated as "missing".
                self.assertIn("reward_credits", result)
                self.assertEqual(result.get("reward_credits"), good)
                self.assertEqual(client.hits(CHECKIN_CLAIM_PATH), 1)
                self.assertEqual(client.hits(CHECKIN_STATUS_PATH), 2)

    async def test_non_finite_or_non_numeric_credit_is_omitted(self):
        bad_values = [
            True,
            False,
            -1,
            -0.5,
            float("nan"),
            float("inf"),
            float("-inf"),
            "10",
            None,
            [1],
            {"value": 1},
        ]
        for bad in bad_values:
            with self.subTest(credit=bad):
                client = ScriptedClient(self._signed_round(bad))
                result = await SigninService(client).run()
                # The claim itself is still confirmed as a signature...
                self.assertEqual(result.get("outcome"), "signed")
                self.assertTrue(result.get("verified"))
                # ...but an unusable credit is never surfaced.
                self.assertNotIn("reward_credits", result)

    async def test_missing_credit_and_unconfirmed_fields_are_omitted(self):
        for claim_data in ({}, {"reward": 5}, {"points": 3}, {"reward": 5, "points": 3}):
            with self.subTest(claim=claim_data):
                client = ScriptedClient(
                    [
                        {"active": True, "today_checked_in": False},
                        claim_data,
                        {"active": True, "today_checked_in": True},
                    ]
                )
                result = await SigninService(client).run()
                self.assertEqual(result.get("outcome"), "signed")
                self.assertNotIn("reward_credits", result)

    async def test_already_signed_initially_has_no_reward_credits(self):
        # Even if the status payload itself carried a credit, it is not a claim.
        client = ScriptedClient(
            [{"active": True, "today_checked_in": True, "credit": 99}]
        )
        result = await SigninService(client).run()

        self.assertEqual(result.get("outcome"), "already_signed")
        self.assertTrue(result.get("verified"))
        self.assertNotIn("reward_credits", result)
        self.assertEqual(client.hits(CHECKIN_STATUS_PATH), 1)
        self.assertEqual(client.hits(CHECKIN_CLAIM_PATH), 0)

    async def test_failed_claim_recheck_signed_does_not_invent_reward(self):
        client = ScriptedClient(
            [
                {"active": True, "today_checked_in": False},
                aiohttp.ClientError("network down"),
                {"active": True, "today_checked_in": True},
            ]
        )
        result = await SigninService(client).run()

        self.assertTrue(result.get("verified"))
        self.assertIn(result.get("outcome"), ("signed", "already_signed"))
        # A signature confirmed only by the re-check has no attributable credit.
        self.assertNotIn("reward_credits", result)
        self.assertEqual(client.hits(CHECKIN_CLAIM_PATH), 1)
        self.assertEqual(client.hits(CHECKIN_STATUS_PATH), 2)

    async def test_credit_without_verified_signature_is_omitted(self):
        client = ScriptedClient(
            [
                {"active": True, "today_checked_in": False},
                {"credit": 10},
                {"active": True, "today_checked_in": False},
            ]
        )
        result = await SigninService(client).run()

        self.assertEqual(result.get("outcome"), "unconfirmed")
        self.assertFalse(result.get("verified"))
        self.assertNotIn("reward_credits", result)

    async def test_raw_claim_fields_do_not_leak_into_result(self):
        client = ScriptedClient(
            [
                {"active": True, "today_checked_in": False},
                {"credit": 5, "token": SECRET, "accessToken": SECRET, "extra": SECRET},
                {"active": True, "today_checked_in": True},
            ]
        )
        result = await SigninService(client).run()

        self.assertEqual(result.get("reward_credits"), 5)
        text = json.dumps(result, ensure_ascii=False, default=str) + repr(result)
        self.assertNotIn(SECRET, text)
        for leaked in ("token", "accessToken", "extra"):
            self.assertNotIn(leaked, result)

    # -- SigninScheduler: on_result contract (3, 4, 5) -------------------

    async def test_on_result_fires_once_with_safe_summary(self):
        events = []

        async def on_result(summary):  # async callbacks are allowed
            events.append(summary)

        service = ScriptedService(
            [
                {
                    "outcome": "signed",
                    "verified": True,
                    "reward_credits": 12,
                    "raw": {"token": SECRET},
                }
            ]
        )
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock, on_result=on_result)

        result = await scheduler.run_once(manual=True)

        self.assertEqual(service.calls, 1)
        self.assertEqual(len(events), 1)
        summary = events[0]
        self.assertEqual(summary.get("outcome"), "signed")
        self.assertTrue(summary.get("verified"))
        self.assertEqual(summary.get("date"), "2026-10-04")
        self.assertEqual(summary.get("reward_credits"), 12)
        # No arbitrary/raw field or secret survives into the safe summary.
        self.assertNotIn("raw", summary)
        text = json.dumps(summary, ensure_ascii=False, default=str) + repr(summary)
        self.assertNotIn(SECRET, text)
        self.assertEqual(result.get("reward_credits"), 12)

    async def test_on_result_not_fired_for_blocked_tick_or_same_day_repeat(self):
        events = []
        service = ScriptedService([{"outcome": "signed", "verified": True, "reward_credits": 5}])
        clock = FakeClock(at(2026, 10, 4, 0, 5))  # before the primary window
        scheduler = self._make(service, clock=clock, on_result=events.append)

        await scheduler.tick()  # not due yet: blocked, not executed
        self.assertEqual(service.calls, 0)
        self.assertEqual(events, [])

        clock.set(at(2026, 10, 4, 0, 15))
        await scheduler.tick()  # genuinely executed once
        self.assertEqual(service.calls, 1)
        self.assertEqual(len(events), 1)

        await scheduler.tick()  # same-day duplicate: blocked
        await scheduler.run_once(manual=True)  # duplicate manual: blocked
        self.assertEqual(service.calls, 1)
        self.assertEqual(len(events), 1)  # never fired for a blocked round

    async def test_on_result_reports_retry_after_and_http_status(self):
        clock = FakeClock(at(2026, 10, 4, 0, 15))

        # 429 -> hard rate limit, Retry-After surfaced.
        events = []
        service = ScriptedService([UpstreamError(429, 4290, retry_after=3600)])
        scheduler = self._fresh(service, clock=clock, on_result=events.append)
        await scheduler.run_once(manual=True)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].get("outcome"), "rate_limited")
        self.assertEqual(events[0].get("retry_after"), 3600.0)
        self.assertEqual(events[0].get("http_status"), 429)

        # 503 -> bounded network backoff, retry hint + status surfaced.
        events = []
        service = ScriptedService([UpstreamError(503, 9001)])
        scheduler = self._fresh(service, clock=clock, on_result=events.append)
        await scheduler.run_once(manual=True)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].get("outcome"), "network")
        self.assertEqual(events[0].get("retry_after"), 60.0)
        self.assertEqual(events[0].get("http_status"), 503)

        # Unknown business error -> terminal error with its HTTP status.
        events = []
        service = ScriptedService([UpstreamError(400, 123)])
        scheduler = self._fresh(service, clock=clock, on_result=events.append)
        await scheduler.run_once(manual=True)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].get("outcome"), "error")
        self.assertEqual(events[0].get("http_status"), 400)

    async def test_on_result_failure_does_not_change_result_and_cancellation_propagates(self):
        clock = FakeClock(at(2026, 10, 4, 0, 15))

        # A raising synchronous callback must not change the business result.
        def boom(_summary):
            raise ValueError("callback boom")

        service = ScriptedService([{"outcome": "signed", "verified": True, "reward_credits": 7}])
        scheduler = self._make(service, clock=clock, on_result=boom)
        result = await scheduler.run_once(manual=True)
        self.assertEqual(result.get("outcome"), "signed")
        self.assertTrue(result.get("verified"))
        self.assertEqual(result.get("reward_credits"), 7)

        # A cancelled async callback must propagate, never be swallowed.
        async def cancelled(_summary):
            raise asyncio.CancelledError

        service = ScriptedService([{"outcome": "signed", "verified": True}])
        scheduler = self._fresh(service, clock=clock, on_result=cancelled)
        with self.assertRaises(asyncio.CancelledError):
            await scheduler.run_once(manual=True)
        self.assertGreaterEqual(scheduler.status()["attempts_today"], 1)

    async def test_on_success_still_fires_once_per_day_with_no_args(self):
        success_calls = []
        events = []

        def on_success():  # legacy: no-arg, once per business date
            success_calls.append("ok")

        service = ScriptedService(
            [
                {"outcome": "signed", "verified": True, "reward_credits": 4},
                {"outcome": "signed", "verified": True},
            ]
        )
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock, on_success=on_success, on_result=events.append)

        await scheduler.run_once(manual=True)
        self.assertEqual(len(success_calls), 1)
        self.assertEqual(len(events), 1)

        clock.set(at(2026, 10, 4, 12, 15))
        await scheduler.tick()  # already signed today
        await scheduler.run_once(manual=True)  # duplicate manual
        self.assertEqual(service.calls, 1)
        self.assertEqual(len(success_calls), 1)  # never a second success callback

    async def test_last_result_retains_reward_and_restart_does_not_refire_success(self):
        success_calls = []
        service = ScriptedService([{"outcome": "signed", "verified": True, "reward_credits": 9}])
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        scheduler = self._make(service, clock=clock, on_success=lambda: success_calls.append("ok"))

        await scheduler.run_once(manual=True)
        self.assertEqual(scheduler.status()["last_result"].get("reward_credits"), 9)

        # "Restart": a new scheduler over the same StateStore directory.
        rebuilt = self._make(service, clock=clock, on_success=lambda: success_calls.append("ok"))
        self.assertEqual(rebuilt.status()["last_result"].get("reward_credits"), 9)

        result = await rebuilt.run_once(manual=True)
        self.assertEqual(result.get("outcome"), "already_signed")
        self.assertEqual(service.calls, 1)  # no re-claim after restart
        self.assertEqual(len(success_calls), 1)  # no duplicate success callback

    async def test_summary_reward_credits_is_validated(self):
        clock = FakeClock(at(2026, 10, 4, 0, 15))
        for bad in (True, -1, float("nan"), float("inf"), "12", None):
            with self.subTest(credit=bad):
                events = []
                service = ScriptedService(
                    [{"outcome": "signed", "verified": True, "reward_credits": bad}]
                )
                scheduler = self._fresh(service, clock=clock, on_result=events.append)
                result = await scheduler.run_once(manual=True)

                self.assertEqual(result.get("outcome"), "signed")
                self.assertTrue(result.get("verified"))
                # An unusable reward is never passed through the safe summary.
                self.assertNotIn("reward_credits", result)
                self.assertEqual(len(events), 1)
                self.assertNotIn("reward_credits", events[0])

    async def test_error_category_is_validated_and_never_leaks(self):
        clock = FakeClock(at(2026, 10, 4, 0, 15))

        # (a) A known failure category on an unconfirmed round is carried
        #     unchanged into the returned summary, the on_result callback and
        #     the persisted last_result.
        events = []
        service = ScriptedService(
            [{"outcome": "unconfirmed", "verified": False, "error_category": "upstream"}]
        )
        scheduler = self._fresh(service, clock=clock, on_result=events.append)
        result = await scheduler.run_once(manual=True)

        with self.subTest(error_category="upstream"):
            self.assertEqual(result.get("outcome"), "unconfirmed")
            self.assertFalse(result.get("verified"))
            self.assertEqual(result.get("error_category"), "upstream")
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].get("error_category"), "upstream")
            self.assertEqual(
                scheduler.status()["last_result"].get("error_category"), "upstream"
            )

        # (b) An unknown category is dropped everywhere, and no raw collaborator
        #     field (including a raw token) survives into any of the three.
        events = []
        service = ScriptedService(
            [
                {
                    "outcome": "unconfirmed",
                    "verified": False,
                    "error_category": SECRET,
                    "raw": {"token": SECRET},
                }
            ]
        )
        scheduler = self._fresh(service, clock=clock, on_result=events.append)
        result = await scheduler.run_once(manual=True)
        last = scheduler.status()["last_result"]

        with self.subTest(error_category="invalid"):
            self.assertEqual(result.get("outcome"), "unconfirmed")
            self.assertFalse(result.get("verified"))
            self.assertNotIn("error_category", result)
            self.assertEqual(len(events), 1)
            self.assertNotIn("error_category", events[0])
            self.assertNotIn("error_category", last)
            for payload in (result, events[0], last, scheduler.status()):
                text = json.dumps(payload, ensure_ascii=False, default=str) + repr(payload)
                self.assertNotIn(SECRET, text)


if __name__ == "__main__":
    unittest.main()
