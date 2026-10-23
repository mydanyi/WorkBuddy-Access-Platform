"""Boundary regressions for the ordered model route (T1-boundaries-red).

Five behaviours the current ``core.routing.ModelRouter`` gets wrong, pinned as
failing tests before the fix.  Nothing of ours is doubled: every test drives the
real ``ModelRouter`` over the real ``core.storage.StateStore``; only the upstream
``send`` boundary and the clock are small fakes, taken from
``tests.test_routing`` (imported as ``support`` -- its ``TestCase`` is *not*
inherited and no router is faked here).

What is pinned:

1. two 429s with different ``Retry-After`` on one round: the round stops instead
   of walking on, and the surfaced hint is the round-wide (longest) cooldown, not
   the short one of the last candidate;
2. a round that is already talking to the primary must stop when *another*
   round's account-scope 429 cools the whole route -- it may not open the standby;
3. a short model-scope 429 hint from one round may not shorten another round's
   still-unexpired long cooldown;
4. an upstream stream that simply ends with no frame is a failure, never a
   fabricated success and never a standby attempt;
5. a 429 that already happened must be persisted before the round ends, so a
   cancelled round leaves a rebuilt router still skipping the primary --
   the cancellation itself must not announce a result.

The two-round races are ordered with ``asyncio.Event`` gates placed in front of
the first upstream frame, so they are deterministic and no fixed sleep, internal
lock or private attribute is inspected.  Model calls are observed through the
payloads the real router handed to the external boundary.

Scratch data lives in the task's NAS directory (``WORKBUDDY_TEST_TMP``, else the
route tests' own root); the system temp dir is never used.

Remote verification (manager): ``python -B -m unittest
tests.test_routing_boundaries -v``.  Local: NOT RUN.
"""

from __future__ import annotations

import asyncio
import copy
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

_PLUGIN_ROOT = Path(__file__).resolve().parents[1]
if str(_PLUGIN_ROOT) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT))

from core.errors import ChatStreamError, UpstreamError  # noqa: E402
from core.storage import StateStore  # noqa: E402

try:  # package discovery: ``python -m unittest tests.test_routing_boundaries``
    from tests import test_routing as support  # noqa: E402
except ImportError:  # top-level discovery: ``python -m unittest discover -s tests``
    import test_routing as support  # noqa: E402


# --------------------------------------------------------------------------- #
# local scratch space (task/NAS directory only -- never the system temp dir)
# --------------------------------------------------------------------------- #

def _tmp_root() -> str:
    """Directory that holds every ``StateStore`` scratch dir of this module."""
    chosen = os.environ.get("WORKBUDDY_TEST_TMP") or os.environ.get(
        "WB_ROUTING_TEST_TMPDIR"
    )
    root = Path(chosen) if chosen else _PLUGIN_ROOT / ".tmp-routing-tests"
    root.mkdir(parents=True, exist_ok=True)
    return str(root)


def _make_data_dir(case):
    path = tempfile.mkdtemp(prefix="routing-boundaries-", dir=_tmp_root())
    case.addCleanup(shutil.rmtree, path, True)
    return path


# --------------------------------------------------------------------------- #
# gated upstream: scripted attempts held open *before* their first frame
# --------------------------------------------------------------------------- #

class _Attempt:
    """One scripted upstream attempt behind an ``asyncio.Event`` gate.

    ``entered`` is set as soon as the router really starts reading this attempt,
    ``release()`` lets the script run.  The interleaving of two rounds is decided
    by the test through these events, never by a fixed sleep.
    """

    def __init__(self, script, *, gated=True):
        self.script = list(script or [])
        self.entered = asyncio.Event()
        self._released = asyncio.Event()
        if not gated:
            self._released.set()

    def release(self) -> None:
        self._released.set()

    async def wait_until_reading(self) -> None:
        await asyncio.wait_for(self.entered.wait(), 5.0)

    async def wait_release(self) -> None:
        await self._released.wait()


class _GatedStream:
    """One upstream stream: async-iterable and closeable, gated on first frame."""

    def __init__(self, model, attempt, closed_log):
        self.model = model
        self._attempt = attempt
        self._closed_log = closed_log
        self._pos = 0
        self._closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        attempt = self._attempt
        attempt.entered.set()
        if self._pos == 0:
            # Held open until the test releases this attempt.  A cancellation
            # here propagates untouched, exactly like a real stalled socket.
            await attempt.wait_release()
        items = attempt.script
        while self._pos < len(items):
            item = items[self._pos]
            self._pos += 1
            if isinstance(item, BaseException):
                self._close()
                raise item
            return item
        self._close()
        raise StopAsyncIteration

    async def aclose(self) -> None:
        self._close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._close()
        return False

    def _close(self) -> None:
        if not self._closed:
            self._closed = True
            self._closed_log.append(self.model)


class _GatedSend:
    """The external send boundary: per-model attempts, consumed in call order.

    ``models`` is the important record -- one entry per real upstream attempt,
    which is how these tests see what the route actually asked for.  A model
    without a scripted attempt answers a healthy stream at once, so a router that
    wrongly walks on fails on the assertions instead of hanging the suite.
    """

    def __init__(self):
        self.attempts = {}
        self.calls = []
        self.models = []
        self.closed = []

    def script(self, model, *attempts) -> None:
        self.attempts.setdefault(model, []).extend(attempts)

    def __call__(self, payload=None):
        model = payload.get("model") if isinstance(payload, dict) else None
        self.calls.append(copy.deepcopy(payload))
        self.models.append(model)
        queue = self.attempts.get(model)
        if queue:
            attempt = queue.pop(0)
        else:
            attempt = _Attempt(
                support._success_script(model), gated=False
            )
        return _GatedStream(model, attempt, self.closed)


# --------------------------------------------------------------------------- #
# the five boundary regressions
# --------------------------------------------------------------------------- #

class RoutingBoundaryTests(unittest.IsolatedAsyncioTestCase):
    """Boundary behaviour of the real router across rounds and cancellation."""

    def setUp(self):
        self.assertIsNotNone(
            support.routing,
            "core.routing is missing: the ordered routing layer is not implemented",
        )
        self.assertIsNotNone(
            getattr(support.routing, "ModelRouter", None),
            "core.routing.ModelRouter is missing",
        )
        self.data_dir = _make_data_dir(self)
        self.state = StateStore(self.data_dir)
        self.clock = support._FakeClock()

    # -- helpers ---------------------------------------------------------- #

    def make_router(self, models, *, on_result=None, **overrides):
        """A real router over a real ``StateStore`` sharing this test's clock."""
        raw = {"enabled": True, "models": list(models)}
        raw.update(overrides)
        return support.routing.ModelRouter(
            support._load_routing(raw),
            StateStore(self.data_dir),
            clock=self.clock,
            on_result=on_result,
        )

    def retry_hint(self, error) -> float:
        """The numeric ``retry_after`` a refusal surfaced to the caller."""
        value = getattr(error, "retry_after", None)
        self.assertIsNotNone(
            value, "the refusal must carry a retry hint, got {!r}".format(error)
        )
        return float(value)

    # -- 1 ---------------------------------------------------------------- #

    async def test_second_429_stops_the_round_and_keeps_the_longest_hint(self):
        """A 120s 429 then a 10s 429: stop, and never surface the short hint."""
        models = ["wb-alpha", "wb-beta", "wb-gamma"]
        router = self.make_router(models, cooldown_seconds=60)
        send = _GatedSend()
        send.script(
            "wb-alpha", _Attempt([support._error(429, retry_after=120)], gated=False)
        )
        send.script(
            "wb-beta", _Attempt([support._error(429, retry_after=10)], gated=False)
        )

        chunks, error = await support._drain(router, support._payload(), send)

        self.assertEqual(chunks, [], "a refused candidate may not emit a frame")
        self.assertIsNotNone(error, "an exhausted route must surface an error")
        self.assertEqual(
            getattr(error, "status", None),
            429,
            "an exhausted route is the local cooling 429",
        )
        self.assertEqual(
            send.models,
            ["wb-alpha", "wb-beta"],
            "the second real 429 must stop the whole round: wb-gamma is never asked",
        )
        self.assertGreaterEqual(
            self.retry_hint(error),
            120.0,
            "the round-wide cooldown is 120s; the 10s hint of the second 429 may "
            "not shorten the hint given to the caller",
        )

        before = len(send.calls)
        again_chunks, again_error = await support._drain(
            router, support._payload(), send
        )

        self.assertEqual(
            len(send.calls), before, "a cooling route must not reach the upstream"
        )
        self.assertEqual(again_chunks, [])
        self.assertIsNotNone(again_error, "the next round must report the refusal")
        self.assertGreaterEqual(
            self.retry_hint(again_error),
            120.0,
            "the immediately following round must be told the whole route is "
            "cooling for 120s, not for the 10s of the last candidate",
        )

    # -- 2 ---------------------------------------------------------------- #

    async def test_route_wide_cooldown_from_another_round_stops_the_live_round(self):
        """A round already inside the primary must stop when the route cools."""
        models = ["wb-alpha", "wb-beta"]
        router = self.make_router(models, cooldown_seconds=60)
        send = _GatedSend()
        slow_503 = _Attempt([support._error(503)])
        account_429 = _Attempt(
            [support._error(429, retry_after=120, limit_scope="account")]
        )
        send.script("wb-alpha", slow_503, account_429)
        # wb-beta is unscripted: it answers at once, so a router that keeps
        # walking after the route cooled fails on an assertion, not by hanging.

        round_one = asyncio.create_task(
            support._drain(router, support._payload(), send)
        )
        await slow_503.wait_until_reading()
        round_two = asyncio.create_task(
            support._drain(router, support._payload(), send)
        )
        await account_429.wait_until_reading()

        # Round two reports an account-scope limit first: the whole route cools.
        account_429.release()
        two_chunks, two_error = await asyncio.wait_for(round_two, 5.0)
        self.assertEqual(two_chunks, [])
        self.assertIsNotNone(
            two_error, "an account-scope 429 must refuse the round that saw it"
        )
        self.assertEqual(
            send.models,
            ["wb-alpha", "wb-alpha"],
            "the round that saw the account limit may not open the standby",
        )

        # Now the older round's 503 arrives: it must terminate, not fail over.
        slow_503.release()
        one_chunks, one_error = await asyncio.wait_for(round_one, 5.0)

        self.assertNotIn(
            "wb-beta",
            send.models,
            "a round that is already talking to wb-alpha must stop once another "
            "round cooled the whole route: wb-beta may not be called at all",
        )
        self.assertEqual(
            one_chunks, [], "the stopped round may not replay or fabricate frames"
        )
        self.assertIsNotNone(
            one_error,
            "the stopped round must surface the route-wide cooldown instead of "
            "continuing on the standby",
        )

    # -- 3 ---------------------------------------------------------------- #

    async def test_short_429_hint_cannot_shorten_an_unexpired_long_cooldown(self):
        """Two concurrent rounds, 120s then 10s hints: the 120s window survives."""
        models = ["wb-alpha", "wb-beta"]
        router = self.make_router(models, cooldown_seconds=60)
        send = _GatedSend()
        long_429 = _Attempt(
            [support._error(429, retry_after=120, limit_scope="model")]
        )
        short_429 = _Attempt(
            [support._error(429, retry_after=10, limit_scope="model")]
        )
        send.script("wb-alpha", long_429, short_429)

        round_one = asyncio.create_task(
            support._drain(router, support._payload(), send)
        )
        await long_429.wait_until_reading()
        round_two = asyncio.create_task(
            support._drain(router, support._payload(), send)
        )
        await short_429.wait_until_reading()

        long_429.release()
        one_chunks, one_error = await asyncio.wait_for(round_one, 5.0)
        short_429.release()
        two_chunks, two_error = await asyncio.wait_for(round_two, 5.0)

        self.assertIsNone(one_error, "a model-scope 429 must fail over to wb-beta")
        self.assertIsNone(two_error, "both rounds must be served by the standby")
        self.assertEqual(
            send.models,
            ["wb-alpha", "wb-alpha", "wb-beta", "wb-beta"],
            "both rounds are the primary, then the standby",
        )
        expected = support._shape(support._success_script("wb-beta"))
        self.assertEqual(support._shape(one_chunks), expected)
        self.assertEqual(support._shape(two_chunks), expected)

        self.clock.advance(20)
        before = len(send.calls)
        third_chunks, third_error = await support._drain(
            router, support._payload(), send
        )

        self.assertIsNone(third_error, "the standby still serves the third round")
        self.assertEqual(
            send.models[before:],
            ["wb-beta"],
            "20s into a 120s cooldown the primary must still be skipped: the "
            "second round's 10s hint may not overwrite the unexpired 120s window",
        )
        self.assertEqual(
            support._shape(third_chunks), expected, "the third round answers standby"
        )

    # -- 4 ---------------------------------------------------------------- #

    async def test_an_immediately_empty_upstream_stream_is_a_failure(self):
        """No frame, no error, no terminator: a failure, not a success."""
        models = ["wb-alpha", "wb-beta"]
        events = []
        router = self.make_router(models, cooldown_seconds=60, on_result=events.append)
        sent = []

        def send(payload=None):
            sent.append(payload.get("model") if isinstance(payload, dict) else None)

            async def _empty_stream():
                for chunk in ():  # an immediately exhausted async generator
                    yield chunk

            return _empty_stream()

        chunks, error = await support._drain(router, support._payload(), send)

        self.assertEqual(chunks, [], "an empty stream must not fabricate a chunk")
        self.assertIsNotNone(
            error, "a stream that ends before any frame must be a reported failure"
        )
        self.assertIsInstance(
            error,
            (UpstreamError, ChatStreamError),
            "the failure must be one of the fixed plugin errors, got {!r}".format(
                error
            ),
        )
        self.assertEqual(
            sent, ["wb-alpha"], "an empty stream must not be retried on the standby"
        )
        self.assertEqual(
            [event.get("outcome") for event in events],
            ["failed"],
            "exactly one failed round may be announced: an empty stream is never "
            "a success",
        )
        self.assertIsNone(
            router.status().get("last_success_model"),
            "an empty stream must not update last_success_model",
        )

    # -- 5 ---------------------------------------------------------------- #

    async def test_cancelling_before_the_first_standby_frame_keeps_the_cooldown(self):
        """The 429 that already happened must be persisted before the round ends."""
        models = ["wb-alpha", "wb-beta"]
        events = []
        router = self.make_router(models, cooldown_seconds=60, on_result=events.append)
        send = _GatedSend()
        send.script(
            "wb-alpha", _Attempt([support._error(429, retry_after=120)], gated=False)
        )
        hanging = _Attempt([support._frame("wb-beta", role="assistant")])
        send.script("wb-beta", hanging)

        stream = router.stream(support._payload(), send)
        pending = asyncio.create_task(stream.__anext__())
        await hanging.wait_until_reading()

        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending

        self.assertIn(
            "wb-beta", send.closed, "cancelling must close the live standby stream"
        )
        self.assertEqual(
            events, [], "a cancelled round must not announce a result of its own"
        )

        rebuilt = self.make_router(models, cooldown_seconds=60)
        rebuilt_send = _GatedSend()
        rebuilt_chunks, rebuilt_error = await support._drain(
            rebuilt, support._payload(), rebuilt_send
        )

        self.assertEqual(
            rebuilt_send.models,
            ["wb-beta"],
            "the 429 that really happened must be persisted: a rebuilt router may "
            "only ask the standby",
        )
        self.assertIsNone(rebuilt_error, "the standby still serves the next round")
        self.assertEqual(
            support._shape(rebuilt_chunks),
            support._shape(support._success_script("wb-beta")),
        )
        self.assertIn(
            "wb-alpha",
            rebuilt.status().get("cooldowns") or {},
            "the observed 429 cooldown must survive the cancelled round",
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
