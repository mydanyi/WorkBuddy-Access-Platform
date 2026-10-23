"""Behaviour-pinning tests for the ordered model routing layer.

``core.routing`` is *not* implemented yet: this module only records the contract
the manager already decided on (an ordered primary / standby list driven by the
plugin's ``routing`` settings group).  Every test is expected to fail until the
implementation ticket lands.

What is real here: ``core.config.load_settings``, ``core.errors``,
``core.storage.StateStore`` (on a temporary directory inside the project tree)
and the router itself.  What is a small fake: only the upstream ``send``
boundary and the clock -- nothing business-level is doubled, so a missing or
mis-branching router fails for the right reason.

The recorded ``send`` calls are how these tests observe which model the *real*
router handed to the external boundary; assertions on the fake's own mechanics
are deliberately absent.
"""

from __future__ import annotations

import asyncio
import copy
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from core.config import ConfigError, load_settings
from core.errors import UpstreamError
from core.storage import StateStore

try:  # added by the implementation ticket; absent means "feature missing"
    from core import routing
except ImportError:  # pragma: no cover - the failing-first phase
    routing = None


ORDERED_MODEL_ID_TEXT = "按顺序调用"
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_UNSET = object()


# --------------------------------------------------------------------------- #
# local scratch space (NAS only -- never the system drive)
# --------------------------------------------------------------------------- #

def _tmp_root():
    """Directory that holds every scratch data dir created by these tests."""
    override = os.environ.get("WB_ROUTING_TEST_TMPDIR")
    root = Path(override) if override else _PROJECT_ROOT / ".tmp-routing-tests"
    root.mkdir(parents=True, exist_ok=True)
    return str(root)


def _make_data_dir(case):
    path = tempfile.mkdtemp(prefix="routing-", dir=_tmp_root())
    case.addCleanup(shutil.rmtree, path, True)
    return path


# --------------------------------------------------------------------------- #
# settings helpers
# --------------------------------------------------------------------------- #

def _load_routing(raw):
    """Return the validated ``routing`` section for a raw routing dict."""
    settings = load_settings({"routing": raw})
    if not isinstance(settings, dict) or "routing" not in settings:
        raise AssertionError("load_settings() did not return a 'routing' section")
    section = settings["routing"]
    if not isinstance(section, dict):
        raise AssertionError("settings['routing'] is not a dict")
    return section


# --------------------------------------------------------------------------- #
# tiny fakes: the upstream boundary and the clock
# --------------------------------------------------------------------------- #

class _FakeClock:
    """A controllable monotonic clock handed to ``ModelRouter``."""

    def __init__(self, now=1000.0):
        self.now = float(now)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += float(seconds)
        return self.now


class _Hang:
    """Script marker: the upstream stalls and never yields a first frame."""


_HANG = _Hang()


class _FakeStream:
    """One upstream stream: async-iterable, awaitable and closeable.

    The shape is intentionally liberal so the test does not dictate whether the
    router awaits ``send(payload)``, iterates it directly or opens it as a
    context manager.  Only the sequencing and the closing matter.
    """

    def __init__(self, model, script, closed_log):
        self.model = model
        self._script = list(script)
        self._pos = 0
        self._closed_log = closed_log
        self._closed = False

    def __await__(self):
        async def _identity():
            return self

        return _identity().__await__()

    def __aiter__(self):
        return self

    async def __anext__(self):
        while self._pos < len(self._script):
            item = self._script[self._pos]
            self._pos += 1
            if item is _HANG:
                while True:
                    await asyncio.sleep(0.2)
            if isinstance(item, BaseException):
                self._finish()
                raise item
            return item
        self._finish()
        raise StopAsyncIteration

    async def aclose(self):
        self._finish()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._finish()
        return False

    def _finish(self):
        if not self._closed:
            self._closed = True
            self._closed_log.append(self.model)


class _FakeSend:
    """The external send boundary: records the model the router actually used."""

    def __init__(self, scripts=None):
        self.scripts = dict(scripts or {})
        self.calls = []   # deep copies of the payloads handed to the upstream
        self.models = []  # one entry per upstream attempt, in call order
        self.opened = []
        self.closed = []

    def __call__(self, payload=None):
        model = payload.get("model") if isinstance(payload, dict) else None
        self.calls.append(copy.deepcopy(payload))
        self.models.append(model)
        self.opened.append(model)
        script = self.scripts.get(model)
        if script is None:
            script = _success_script(model)
        return _FakeStream(model, script, self.closed)


# --------------------------------------------------------------------------- #
# frame + payload helpers
# --------------------------------------------------------------------------- #

def _frame(model, *, role=None, content=None, tool_calls=None, finish_reason=None):
    delta = {}
    if role is not None:
        delta["role"] = role
    if content is not None:
        delta["content"] = content
    if tool_calls is not None:
        delta["tool_calls"] = tool_calls
    return {
        "id": "chatcmpl-" + str(model),
        "object": "chat.completion.chunk",
        "created": 1700000000,
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    }


def _success_script(model):
    return [
        _frame(model, role="assistant"),
        _frame(model, content="answer from " + str(model)),
        _frame(model, finish_reason="stop"),
    ]


def _shape(chunks):
    """A model-name-agnostic view of emitted chunks (naming is not pinned)."""
    shape = []
    for chunk in chunks:
        choices = chunk.get("choices") if isinstance(chunk, dict) else None
        if not choices:
            shape.append(("meta", chunk.get("usage") if isinstance(chunk, dict) else None))
            continue
        choice = choices[0]
        shape.append(
            ("frame", dict(choice.get("delta") or {}), choice.get("finish_reason"))
        )
    return shape


def _error(status, *, retry_after=None, code=None, limit_scope=None):
    """Build an ``UpstreamError``; ``limit_scope`` is the new optional field."""
    kwargs = {}
    if retry_after is not None:
        kwargs["retry_after"] = retry_after
    if limit_scope is not None:
        kwargs["limit_scope"] = limit_scope
    return UpstreamError(status, code=code, **kwargs)


def _payload(model=None):
    return {
        "model": routing.ORDERED_MODEL_ID if model is None else model,
        "messages": [{"role": "user", "content": "你好"}],
        "tools": [
            {"type": "function", "function": {"name": "ping", "arguments": "{}"}}
        ],
        "tool_choice": "auto",
        "temperature": 0.2,
        "max_tokens": 256,
        "stream": True,
    }


async def _drain(router, payload, send):
    """Run one routing request; return ``(chunks, error)`` without asserting."""
    chunks = []
    error = None
    try:
        async for chunk in router.stream(payload, send):
            chunks.append(chunk)
    except Exception as exc:  # the concrete error type is the manager's call
        error = exc
    return chunks, error


# --------------------------------------------------------------------------- #
# configuration + error contracts
# --------------------------------------------------------------------------- #

class RoutingConfigTests(unittest.TestCase):
    """The ``routing`` settings group and the new ``limit_scope`` field."""

    def setUp(self):
        self.assertIsNotNone(
            routing,
            "core.routing is missing: the ordered routing layer is not implemented",
        )

    def test_default_routing_settings(self):
        settings = load_settings({})
        self.assertIn(
            "routing", settings, "load_settings({}) must expose routing defaults"
        )
        section = settings["routing"]
        self.assertIs(section["enabled"], False, "routing must default to disabled")
        self.assertEqual(section["models"], [])
        self.assertEqual(section["cooldown_seconds"], 60)
        self.assertEqual(section["first_frame_timeout"], 90)

    def test_ordered_model_identifier(self):
        self.assertEqual(routing.ORDERED_MODEL_ID, ORDERED_MODEL_ID_TEXT)

    def test_enabled_requires_a_non_empty_model_list(self):
        bad = (
            {"enabled": True},
            {"enabled": True, "models": []},
            {"enabled": True, "models": None},
        )
        for raw in bad:
            with self.subTest(raw=raw):
                with self.assertRaises(ConfigError):
                    load_settings({"routing": raw})

    def test_invalid_model_entries_are_rejected_without_echo(self):
        marker = "wb-secret-model"
        cases = {
            "empty entry": [marker, ""],
            "blank entry": [marker, "   "],
            "duplicate entry": [marker, marker],
            "non string entry": [marker, 7],
            "nan entry": [marker, float("nan")],
            "self reference": [routing.ORDERED_MODEL_ID],
            "list is a bare string": marker,
        }
        for label, models in cases.items():
            with self.subTest(case=label):
                with self.assertRaises(ConfigError) as caught:
                    load_settings({"routing": {"enabled": True, "models": models}})
                message = str(caught.exception)
                self.assertTrue(
                    message, "the fixed ConfigError message must not be empty"
                )
                self.assertNotIn(
                    marker, message, "the raw model value leaked into the error"
                )

    def test_ordered_model_list_keeps_config_order(self):
        models = [
            "wb-alpha",
            "wb-beta",
            "wb-gamma",
            "wb-delta",
            "wb-epsilon",
            "wb-zeta",
        ]
        section = _load_routing(
            {"enabled": True, "models": models, "cooldown_seconds": 30}
        )
        self.assertEqual(
            section["models"], models, "the validated list must keep its order"
        )

    def test_upstream_error_exposes_optional_limit_scope(self):
        for scope in ("account", "ip", "model", None):
            with self.subTest(scope=scope):
                error = UpstreamError(429, limit_scope=scope)
                self.assertEqual(error.limit_scope, scope)
                self.assertEqual(error.status, 429)
        self.assertIsNone(
            getattr(UpstreamError(503), "limit_scope", None),
            "an unset limit_scope must default to None",
        )


# --------------------------------------------------------------------------- #
# router behaviour
# --------------------------------------------------------------------------- #

class RoutingBehaviourTests(unittest.IsolatedAsyncioTestCase):
    """Ordered failover, cooldown and stream-integrity behaviour."""

    def setUp(self):
        self.assertIsNotNone(
            routing,
            "core.routing is missing: the ordered routing layer is not implemented",
        )
        self.assertIsNotNone(
            getattr(routing, "ModelRouter", None),
            "core.routing.ModelRouter is missing",
        )
        self.data_dir = _make_data_dir(self)
        self.state = StateStore(self.data_dir)
        self.clock = _FakeClock()
        self.settings = self.settings_for(["wb-alpha", "wb-beta", "wb-gamma"])
        self.payload = _payload()

    # -- helpers ---------------------------------------------------------- #

    def settings_for(self, models, **overrides):
        raw = {"enabled": True, "models": list(models)}
        raw.update(overrides)
        return _load_routing(raw)

    def make_router(self, settings=None, *, clock=_UNSET):
        """A real router over a real StateStore (a fresh one per call)."""
        chosen_clock = self.clock if clock is _UNSET else clock
        return routing.ModelRouter(
            self.settings if settings is None else settings,
            StateStore(self.data_dir),
            clock=chosen_clock,
        )

    # -- 4/5: primary-only path ------------------------------------------- #

    async def test_primary_success_sends_once_and_preserves_payload(self):
        router = self.make_router()
        send = _FakeSend()
        original = copy.deepcopy(self.payload)

        chunks, error = await _drain(router, self.payload, send)

        self.assertIsNone(error, "a healthy primary must not fail the request")
        self.assertEqual(
            send.models, ["wb-alpha"], "the primary must be asked exactly once"
        )
        self.assertEqual(
            send.calls[0],
            dict(original, model="wb-alpha"),
            "only the model field may change on the upstream payload",
        )
        self.assertEqual(
            self.payload, original, "the caller payload must not be mutated"
        )
        self.assertEqual(_shape(chunks), _shape(_success_script("wb-alpha")))

    async def test_ordered_list_is_used_in_full_without_a_hard_cap(self):
        models = ["wb-m1", "wb-m2", "wb-m3", "wb-m4", "wb-m5", "wb-m6"]
        settings = self.settings_for(models, cooldown_seconds=60)
        router = self.make_router(settings)
        send = _FakeSend({model: [_error(503)] for model in models[:5]})

        chunks, error = await _drain(router, _payload(), send)

        self.assertIsNone(error, "the last configured model still serves the request")
        self.assertEqual(
            send.models, models, "no hard-coded standby cap may cut the ordered list"
        )
        self.assertEqual(_shape(chunks), _shape(_success_script(models[5])))

    # -- 6/7: failover on retryable statuses ------------------------------ #

    async def test_429_falls_over_to_the_next_model(self):
        router = self.make_router()
        send = _FakeSend({"wb-alpha": [_error(429)]})

        chunks, error = await _drain(router, self.payload, send)

        self.assertIsNone(error, "the standby must absorb a 429")
        self.assertEqual(send.models, ["wb-alpha", "wb-beta"])
        self.assertEqual(_shape(chunks), _shape(_success_script("wb-beta")))

    async def test_503_then_502_then_success(self):
        router = self.make_router()
        send = _FakeSend(
            {"wb-alpha": [_error(503)], "wb-beta": [_error(502)]}
        )

        chunks, error = await _drain(router, self.payload, send)

        self.assertIsNone(error)
        self.assertEqual(send.models, ["wb-alpha", "wb-beta", "wb-gamma"])
        self.assertEqual(_shape(chunks), _shape(_success_script("wb-gamma")))

    # -- 8/9: where the ordered route must stop --------------------------- #

    async def test_two_exhausted_429s_stop_and_cool_the_whole_route(self):
        router = self.make_router()
        send = _FakeSend({"wb-alpha": [_error(429)], "wb-beta": [_error(429)]})

        chunks, error = await _drain(router, self.payload, send)

        self.assertIsNotNone(error, "an exhausted route must surface an error")
        self.assertEqual(chunks, [], "no frame from a refused model may be emitted")
        self.assertEqual(
            send.models,
            ["wb-alpha", "wb-beta"],
            "a second 429 must stop the route instead of walking on",
        )
        self.assertNotIn("wb-gamma", send.opened)
        self.assertIsNotNone(router.status(), "status() must report local state")

        before = len(send.calls)
        cooling_chunks, cooling_error = await _drain(router, _payload(), send)
        self.assertEqual(
            len(send.calls), before, "a cooling route must not reach the upstream"
        )
        self.assertEqual(cooling_chunks, [])
        self.assertIsNotNone(
            cooling_error, "a cooling route must report the refusal to the caller"
        )

    async def test_account_scope_429_stops_immediately(self):
        router = self.make_router()
        send = _FakeSend({"wb-alpha": [_error(429, limit_scope="account")]})

        chunks, error = await _drain(router, self.payload, send)

        self.assertIsNotNone(error)
        self.assertEqual(
            send.models,
            ["wb-alpha"],
            "an account-scope 429 must not try the standby model",
        )
        self.assertEqual(chunks, [])

    # -- 10: client errors never fail over -------------------------------- #

    async def test_client_errors_do_not_fail_over(self):
        for status in (400, 401, 403):
            with self.subTest(status=status):
                router = self.make_router()
                send = _FakeSend({"wb-alpha": [_error(status)]})

                chunks, error = await _drain(router, _payload(), send)

                self.assertIsNotNone(error)
                self.assertEqual(
                    send.models,
                    ["wb-alpha"],
                    "a {} must be reported, not routed around".format(status),
                )
                self.assertEqual(chunks, [])

    # -- 11: per-model cooldown ------------------------------------------- #

    async def test_cooled_model_is_skipped_then_recovers(self):
        settings = self.settings_for(["wb-alpha", "wb-beta", "wb-gamma"],
                                     cooldown_seconds=60)
        router = self.make_router(settings)
        send = _FakeSend({"wb-alpha": [_error(503)]})

        await _drain(router, _payload(), send)
        self.assertEqual(send.models, ["wb-alpha", "wb-beta"])

        self.clock.advance(30)
        await _drain(router, _payload(), send)
        self.assertEqual(
            send.models,
            ["wb-alpha", "wb-beta", "wb-beta"],
            "a cooling model must be skipped on the next request",
        )

        self.clock.advance(31)  # now 61s after the first failure
        await _drain(router, _payload(), send)
        self.assertEqual(
            send.models[-2:],
            ["wb-alpha", "wb-beta"],
            "the primary must be retried once its cooldown expired",
        )

    # -- 12: Retry-After wins over the default cooldown ------------------- #

    async def test_retry_after_longer_than_the_default_cooldown(self):
        settings = self.settings_for(["wb-alpha", "wb-beta"], cooldown_seconds=60)
        router = self.make_router(settings)
        send = _FakeSend({"wb-alpha": [_error(429, retry_after=300)]})

        await _drain(router, _payload(), send)
        self.assertEqual(send.models, ["wb-alpha", "wb-beta"])

        self.clock.advance(120)
        await _drain(router, _payload(), send)
        self.assertEqual(
            send.models,
            ["wb-alpha", "wb-beta", "wb-beta"],
            "a Retry-After beyond the default must keep the model cooling",
        )

        self.clock.advance(181)  # 301s after the 429
        await _drain(router, _payload(), send)
        self.assertEqual(
            send.models[-2:],
            ["wb-alpha", "wb-beta"],
            "the model must return once its Retry-After elapsed",
        )

    async def test_retry_after_shorter_than_the_default_cooldown(self):
        settings = self.settings_for(["wb-alpha", "wb-beta"], cooldown_seconds=60)
        router = self.make_router(settings)
        send = _FakeSend({"wb-alpha": [_error(429, retry_after=5)]})

        await _drain(router, _payload(), send)
        self.assertEqual(send.models, ["wb-alpha", "wb-beta"])

        self.clock.advance(10)
        await _drain(router, _payload(), send)
        self.assertEqual(
            send.models[-2:],
            ["wb-alpha", "wb-beta"],
            "a short Retry-After must release the model before the default cooldown",
        )

    # -- 13: cooldown is persisted, other state sections untouched -------- #

    async def test_rebuilt_router_keeps_cooldown_and_other_state_sections(self):
        settings = self.settings_for(["wb-alpha", "wb-beta"], cooldown_seconds=60)
        self.state.update("signin", {"last_success": "2026-10-19"})
        self.state.update("limits", {"rpm_window": [1, 2]})

        router = self.make_router(settings)
        send = _FakeSend({"wb-alpha": [_error(503)]})
        await _drain(router, _payload(), send)
        self.assertEqual(send.models, ["wb-alpha", "wb-beta"])

        rebuilt = self.make_router(settings)
        rebuilt_send = _FakeSend()
        await _drain(rebuilt, _payload(), rebuilt_send)

        self.assertEqual(
            rebuilt_send.models,
            ["wb-beta"],
            "a rebuilt router must still skip the cooling model",
        )
        state = self.state.load()
        self.assertEqual(state.get("signin"), {"last_success": "2026-10-19"})
        self.assertEqual(state.get("limits"), {"rpm_window": [1, 2]})

    # -- 14: nothing left to try ------------------------------------------ #

    async def test_fully_cooling_route_never_calls_the_upstream(self):
        settings = self.settings_for(["wb-alpha"], cooldown_seconds=60)
        router = self.make_router(settings)
        send = _FakeSend({"wb-alpha": [_error(429)]})

        chunks, error = await _drain(router, _payload(), send)
        self.assertIsNotNone(error)
        self.assertEqual(chunks, [])
        self.assertEqual(send.models, ["wb-alpha"])

        second_chunks, second_error = await _drain(router, _payload(), send)
        self.assertEqual(
            len(send.calls), 1, "a fully cooling route must not reach the upstream"
        )
        self.assertEqual(second_chunks, [])
        self.assertIsNotNone(second_error)

    # -- 15: partial frames pin the request to one model ------------------ #

    async def test_error_after_frames_does_not_fail_over(self):
        router = self.make_router()
        alpha_script = [
            _frame("wb-alpha", role="assistant"),
            _frame(
                "wb-alpha",
                tool_calls=[
                    {
                        "index": 0,
                        "id": "call-1",
                        "type": "function",
                        "function": {"name": "ping", "arguments": ""},
                    }
                ],
            ),
            _error(503),
        ]
        send = _FakeSend({"wb-alpha": alpha_script})

        chunks, error = await _drain(router, self.payload, send)

        self.assertIsNotNone(error, "an upstream failure after frames must surface")
        self.assertEqual(
            send.models,
            ["wb-alpha"],
            "once frames were emitted no standby may be tried",
        )
        self.assertEqual(
            _shape(chunks),
            _shape(alpha_script[:2]),
            "no second model's output may be spliced into the emitted stream",
        )

    # -- 16: cancellation closes the live upstream ------------------------ #

    async def test_cancelled_stream_closes_upstream_without_failover(self):
        router = self.make_router(self.settings_for(["wb-alpha", "wb-beta"]))
        send = _FakeSend(
            {
                "wb-alpha": [
                    _frame("wb-alpha", role="assistant"),
                    _frame("wb-alpha", content="first part"),
                    _frame("wb-alpha", finish_reason="stop"),
                ]
            }
        )

        stream = router.stream(_payload(), send)
        first = await stream.__anext__()
        self.assertEqual(
            _shape([first]), _shape([_frame("wb-alpha", role="assistant")])
        )

        await stream.aclose()

        self.assertEqual(
            send.models, ["wb-alpha"], "a cancelled stream must not try a standby"
        )
        self.assertIn(
            "wb-alpha", send.closed, "the cancelled stream must close the live upstream"
        )
        self.assertNotIn("wb-beta", send.opened)

    # -- 17: the first-frame timeout terminates --------------------------- #

    async def test_first_frame_timeout_terminates_the_route(self):
        settings = self.settings_for(
            ["wb-alpha", "wb-beta"], first_frame_timeout=1
        )
        router = self.make_router(settings, clock=None)
        send = _FakeSend({"wb-alpha": [_HANG]})

        loop = asyncio.get_running_loop()
        started = loop.time()
        chunks, error = await asyncio.wait_for(
            _drain(router, _payload(), send), 5.0
        )
        elapsed = loop.time() - started

        self.assertIsNotNone(error, "a stalled first frame must fail the request")
        self.assertEqual(chunks, [])
        self.assertLess(
            elapsed, 3.0, "the global first-frame timeout must not loop forever"
        )
        self.assertLessEqual(
            len(send.calls), 2, "the route must stay bounded by its model list"
        )


# --------------------------------------------------------------------------- #
# manager-review gaps (append-only; the 21 tests above are untouched)
# --------------------------------------------------------------------------- #

class RoutingGapRegressionTests(unittest.IsolatedAsyncioTestCase):
    """Two concrete gaps found in review, pinned before the fix.

    1. A persisted cooldown is stored as "remaining seconds" at failure time.  A
       router rebuilt *later* must not restart that 60s window from the rebuild
       moment: after 61s of a 60s cooldown the primary has to be tried again at
       once.
    2. When every candidate really answers 503 the round must keep the last real
       503 status; only a genuinely cooling next round is the local 429.  A real
       failure may never be rewritten into a fabricated rate limit.
    """

    def setUp(self):
        self.assertIsNotNone(
            routing,
            "core.routing is missing: the ordered routing layer is not implemented",
        )
        self.data_dir = _make_data_dir(self)
        self.state = StateStore(self.data_dir)
        self.clock = _FakeClock()

    def make_router(self, models, **overrides):
        raw = {"enabled": True, "models": list(models)}
        raw.update(overrides)
        return routing.ModelRouter(
            _load_routing(raw), StateStore(self.data_dir), clock=self.clock
        )

    async def test_rebuilt_router_retries_the_primary_once_the_cooldown_elapsed(self):
        models = ["wb-alpha", "wb-beta"]
        router = self.make_router(models, cooldown_seconds=60)
        send = _FakeSend({"wb-alpha": [_error(503)]})

        await _drain(router, _payload(), send)
        self.assertEqual(send.models, ["wb-alpha", "wb-beta"])

        # 61s after a 60s cooldown: the persisted state still says "60s left",
        # but 60 of those seconds are already in the past.
        self.clock.advance(61)
        rebuilt = self.make_router(models, cooldown_seconds=60)
        rebuilt_send = _FakeSend()

        await _drain(rebuilt, _payload(), rebuilt_send)

        self.assertEqual(
            rebuilt_send.models,
            ["wb-alpha"],
            "a rebuilt router must measure the persisted cooldown from the real "
            "failure time: after 61s of a 60s cooldown the primary has to be "
            "tried immediately instead of restarting the 60s window",
        )

    async def test_all_candidates_real_503_keeps_503_and_only_the_next_round_is_429(self):
        models = ["wb-alpha", "wb-beta"]
        router = self.make_router(models, cooldown_seconds=60)
        send = _FakeSend(
            {"wb-alpha": [_error(503)], "wb-beta": [_error(503)]}
        )

        chunks, error = await _drain(router, _payload(), send)

        self.assertEqual(chunks, [], "no frame from a refused model may be emitted")
        self.assertIsInstance(error, UpstreamError)
        self.assertEqual(
            getattr(error, "status", None),
            503,
            "an exhausted route must keep the last real upstream status (503) "
            "instead of being rewritten into a fabricated 429",
        )
        self.assertEqual(send.models, ["wb-alpha", "wb-beta"])

        cooling_chunks, cooling_error = await _drain(router, _payload(), send)
        self.assertEqual(
            len(send.calls),
            2,
            "the next round is fully cooling and must not reach the upstream",
        )
        self.assertEqual(cooling_chunks, [])
        self.assertEqual(
            getattr(cooling_error, "status", None),
            429,
            "only a genuinely cooling round answers 429",
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
