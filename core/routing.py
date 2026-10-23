"""Ordered primary / standby model routing in front of the chat stream.

The plugin exposes one virtual model id (:data:`ORDERED_MODEL_ID`).  A request
for it is walked down a configured, ordered list of real upstream model ids;
every other model id is forwarded verbatim and never becomes a candidate.
Only ``model`` on a *shallow copy* of the payload is rewritten, so messages,
tools, tool choice, temperature and every other client field reach the upstream
untouched.

Contract pinned by ``tests/test_routing.py``:

* a candidate is abandoned only for an :class:`UpstreamError` whose real
  ``status`` is 429/502/503/504 *and* only while no chunk was emitted yet; once
  one chunk (role / tool / reasoning / metadata) is committed the stream
  belongs to that model and a later error is propagated, never replayed;
* an abandoned stream is closed before the next candidate is opened;
* a 429 whose ``limit_scope`` is ``account``/``ip`` cools the whole route at
  once, and two consecutive real 429 attempts do the same, using the largest
  ``Retry-After`` seen in that streak;
* with no usable candidate the router raises a plain ``429``
  :class:`UpstreamError` carrying the earliest retry hint and never touches the
  upstream for that round;
* one absolute first-frame deadline covers the whole round (it is *not* reset
  per candidate) and only guards the pre-commit window; a timeout raises a
  fixed :class:`ChatStreamError` and stops the route;
* cooldowns are persisted in the ``routing`` section of the real
  :class:`core.storage.StateStore` as absolute wall-clock deadlines (only the
  visible ``status()`` view reports remaining seconds), so a rebuilt router
  keeps them, an already elapsed window is not restarted, and no other section
  (signin / limits / notifications) is disturbed;
* a stream that ends normally before its first frame is an empty-stream failure,
  never a success and never a standby attempt;
* cancellation / ``GeneratorExit`` propagate untouched, close the live
  upstream and never fire the ``on_result`` hook.
"""

from __future__ import annotations

import asyncio
import contextlib
import time

from .credits import is_finite_number
from .errors import ChatStreamError, UpstreamError
from .limits import LimitExceeded

ORDERED_MODEL_ID = "按顺序调用"

# Only these real upstream statuses are allowed to move on to a standby model.
RETRYABLE_STATUSES = (429, 502, 503, 504)
# A rate limit reported against one of these scopes covers every candidate.
ROUTE_WIDE_SCOPES = ("account", "ip")

_DEFAULT_COOLDOWN = 60.0
_DEFAULT_FIRST_FRAME_TIMEOUT = 90.0

_DISABLED_CODE = "routing_disabled"
_COOLING_CODE = "routing_cooling"

_TIMEOUT_MESSAGE = "upstream chat stream did not deliver a first frame in time"
_EMPTY_MESSAGE = "upstream chat stream ended before its first frame"

_CATEGORY_OF_REASON = {
    "ok": "ok",
    "cooling": "routing",
    "rate_limited": "upstream",
    "upstream": "upstream",
    "stream": "upstream",
    "timeout": "timeout",
    "error": "error",
    # The *local* admission budget ran out: a fixed local marker, never a fake
    # upstream rate limit, so a later notification layer can skip it.
    "local_limit": "routing",
}


async def _aclose(agen) -> None:
    """Close an upstream generator, never masking the reason we got here."""
    closer = getattr(agen, "aclose", None)
    if closer is None:
        return
    with contextlib.suppress(Exception):
        await closer()


def _seconds(value, default):
    """A finite, strictly positive seconds value or ``default``."""
    if is_finite_number(value) and value > 0:
        return float(value)
    return default


def _reason_for(exc) -> str:
    """One fixed label describing why a round failed (never raw upstream text)."""
    if isinstance(exc, LimitExceeded):
        # The local request budget refused an attempt: a fixed local marker, not
        # an upstream rate limit and never a standby hint.
        return "local_limit"
    if isinstance(exc, UpstreamError):
        if getattr(exc, "code", None) == _COOLING_CODE:
            return "cooling"
        return "rate_limited" if exc.status == 429 else "upstream"
    if isinstance(exc, ChatStreamError):
        return "timeout" if str(exc) == _TIMEOUT_MESSAGE else "stream"
    return "error"


class ModelRouter:
    """Walks the ordered model list for the virtual :data:`ORDERED_MODEL_ID`.

    ``settings`` is the validated ``routing`` settings group, ``state`` is the
    real :class:`core.storage.StateStore`.  ``clock`` defaults to ``time.time``
    so a persisted cooldown survives a restart; ``on_result`` is an optional
    synchronous hook called once per finished round with a small, safe summary.
    """

    def __init__(self, settings, state, clock=None, on_result=None):
        section = settings if isinstance(settings, dict) else {}
        self._state = state
        self._clock = clock if clock is not None else time.time
        self._on_result = on_result
        self._enabled = section.get("enabled") is True
        raw_models = section.get("models")
        if isinstance(raw_models, list):
            self._models = [
                model
                for model in raw_models
                if isinstance(model, str) and model.strip()
            ]
        else:
            self._models = []
        self._cooldown = _seconds(
            section.get("cooldown_seconds"), _DEFAULT_COOLDOWN
        )
        self._timeout = _seconds(
            section.get("first_frame_timeout"), _DEFAULT_FIRST_FRAME_TIMEOUT
        )
        self._deadlines = {}
        self._global_until = 0.0
        self._last_success = None
        self._last_result = None
        self._hydrated = False

    # -- local state ---------------------------------------------------------

    def _now(self) -> float:
        return float(self._clock())

    def _hydrate(self, now) -> None:
        """Restore persisted *absolute* deadlines into memory once.

        The stored values are wall-clock instants, so a rebuilt router does not
        restart an already partly elapsed window.  A deadline that is already
        in the past is simply not restored.
        """
        if self._hydrated:
            return
        self._hydrated = True
        try:
            stored = self._state.load()
        except Exception:
            return
        section = stored.get("routing") if isinstance(stored, dict) else None
        if not isinstance(section, dict):
            return
        cooldowns = section.get("cooldowns")
        if isinstance(cooldowns, dict):
            for model, deadline in cooldowns.items():
                if not isinstance(model, str):
                    continue
                if is_finite_number(deadline) and float(deadline) > now:
                    self._deadlines[model] = max(
                        self._deadlines.get(model, 0.0), float(deadline)
                    )
        global_until = section.get("global_until")
        if is_finite_number(global_until) and float(global_until) > now:
            self._global_until = max(self._global_until, float(global_until))
        last_success = section.get("last_success_model")
        if isinstance(last_success, str):
            self._last_success = last_success
        last_result = section.get("last_result")
        if isinstance(last_result, dict):
            self._last_result = last_result

    def _stored(self, now) -> dict:
        """The persisted ``routing`` section: absolute wall-clock deadlines."""
        return {
            "enabled": self._enabled,
            "models": list(self._models),
            "cooldowns": {
                model: round(deadline, 3)
                for model, deadline in self._deadlines.items()
                if deadline > now
            },
            "global_until": (
                round(self._global_until, 3) if self._global_until > now else 0.0
            ),
            "last_success_model": self._last_success,
            "last_result": self._last_result,
        }

    def _snapshot(self, now) -> dict:
        """The visible status view: remaining durations, never raw payloads."""
        return {
            "enabled": self._enabled,
            "models": list(self._models),
            "cooldowns": {
                model: round(deadline - now, 3)
                for model, deadline in self._deadlines.items()
                if deadline > now
            },
            "global_cooldown_remaining": (
                round(self._global_until - now, 3)
                if self._global_until > now
                else 0.0
            ),
            "last_success_model": self._last_success,
            "last_result": self._last_result,
        }

    def status(self) -> dict:
        """Local-only view of the route; it never probes the upstream."""
        now = self._now()
        self._hydrate(now)
        return self._snapshot(now)

    def _persist(self, now) -> None:
        """Write only the ``routing`` section; a failed write never fails a chat."""
        try:
            self._state.update("routing", self._stored(now))
        except Exception:
            pass

    def _notify(self, event) -> None:
        """Fire the optional round hook; its failure cannot change the outcome."""
        callback = self._on_result
        if callback is None:
            return
        try:
            callback(dict(event))
        except Exception:
            pass

    # -- cooldown bookkeeping ------------------------------------------------

    def _cooling(self, model, now) -> bool:
        deadline = self._deadlines.get(model)
        return deadline is not None and deadline > now

    def _cool(self, model, now, delay) -> None:
        """Cool one model, keeping an already longer window intact.

        A concurrent round's shorter ``Retry-After`` may never overwrite the
        still-unexpired deadline this model already has.
        """
        deadline = now + delay
        current = self._deadlines.get(model)
        if current is not None and current > deadline:
            deadline = current
        self._deadlines[model] = deadline

    def _cool_everything(self, now, delay) -> None:
        self._global_until = max(self._global_until, now + delay)

    def _delay(self, error) -> float:
        """``Retry-After`` when it is a finite non-negative number, else default."""
        retry_after = getattr(error, "retry_after", None)
        if is_finite_number(retry_after) and retry_after >= 0:
            return float(retry_after)
        return self._cooldown

    def _earliest(self, now, models) -> float:
        """Seconds until the soonest candidate is *really* usable again.

        A candidate recovers at ``max(route-wide deadline, its own deadline)``,
        so a route-wide 120s window is never shortened by a 10s model hint; the
        round gets the earliest of those, and at least one second.
        """
        earliest = None
        for model in models:
            deadline = self._deadlines.get(model)
            base = deadline if deadline is not None and deadline > now else now
            recovery = max(self._global_until, base)
            earliest = recovery if earliest is None else min(earliest, recovery)
        if earliest is None:
            return max(1.0, self._cooldown)
        return max(1.0, earliest - now)

    def _refusal(self, now, models) -> UpstreamError:
        """The fixed ``429`` used when no candidate may be asked this round."""
        return UpstreamError(
            429, code=_COOLING_CODE, retry_after=self._earliest(now, models)
        )

    # -- round outcomes ------------------------------------------------------

    def _finish_success(self, model, attempts) -> None:
        now = self._now()
        self._last_success = model
        self._last_result = {
            "outcome": "success",
            "attempts": list(attempts),
            "reason": "ok",
            "model": model,
            "status": 200,
            "retry_after": None,
        }
        self._persist(now)
        self._notify(
            {
                "outcome": "success",
                "category": _CATEGORY_OF_REASON["ok"],
                "reason": "ok",
                "model": model,
                "attempts": list(attempts),
                "attempt_count": len(attempts),
                "status": 200,
                "retry_after": None,
            }
        )

    def _finish_failed(self, attempts, exc) -> None:
        reason = _reason_for(exc)
        status = getattr(exc, "status", None)
        retry_after = getattr(exc, "retry_after", None)
        status = status if isinstance(status, int) and not isinstance(status, bool) else None
        retry_after = retry_after if is_finite_number(retry_after) else None
        model = attempts[-1] if attempts else None
        now = self._now()
        self._last_result = {
            "outcome": "failed",
            "attempts": list(attempts),
            "reason": reason,
            "model": model,
            "status": status,
            "retry_after": retry_after,
        }
        self._persist(now)
        self._notify(
            {
                "outcome": "failed",
                "category": _CATEGORY_OF_REASON.get(reason, "error"),
                "reason": reason,
                "model": model,
                "attempts": list(attempts),
                "attempt_count": len(attempts),
                "status": status,
                "retry_after": retry_after,
            }
        )

    # -- streaming -----------------------------------------------------------

    async def _next(self, iterator, committed, deadline, loop):
        """Next frame; only the *pre-first-frame* window is bounded."""
        if committed:
            return await iterator.__anext__()
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise ChatStreamError(_TIMEOUT_MESSAGE)
        try:
            return await asyncio.wait_for(iterator.__anext__(), remaining)
        except (asyncio.TimeoutError, TimeoutError):
            raise ChatStreamError(_TIMEOUT_MESSAGE) from None

    async def stream(self, payload, send):
        """Yield the upstream chunks for ``payload``, failing over in order.

        ``send(payload)`` opens exactly one upstream stream (an async
        generator).  Only the ``model`` field of a shallow copy is changed.
        """
        if not isinstance(payload, dict):
            raise ChatStreamError("chat payload must be a JSON object")
        if payload.get("model") != ORDERED_MODEL_ID:
            # A concrete model id is forwarded verbatim: never a candidate.
            agen = send(dict(payload))
            try:
                async for chunk in agen:
                    yield chunk
            finally:
                await _aclose(agen)
            return

        if not self._enabled or not self._models:
            # The virtual id only exists with a configured, enabled list; a
            # disabled route is a fixed local error and never reaches upstream.
            raise UpstreamError(400, code=_DISABLED_CODE)

        models = list(self._models)
        loop = asyncio.get_running_loop()
        # One absolute deadline for the whole round -- never reset per candidate.
        deadline = loop.time() + self._timeout

        now = self._now()
        self._hydrate(now)
        attempts = []
        committed = False
        streak = 0
        streak_delay = 0.0
        last_failure = None
        try:
            if self._global_until > now:
                candidates = []
            else:
                candidates = [
                    model for model in models if not self._cooling(model, now)
                ]
            if not candidates:
                raise self._refusal(now, models)

            while candidates:
                model = candidates.pop(0)
                now = self._now()
                # Re-check right before the real send: another round may have
                # cooled the whole route -- then no standby is opened at all --
                # or just this model, which is skipped in configured order.
                if self._global_until > now:
                    raise self._refusal(now, models)
                if self._cooling(model, now):
                    continue
                attempts.append(model)
                body = dict(payload)
                body["model"] = model
                agen = send(body)
                iterator = agen.__aiter__()
                failure = None
                switched = False
                try:
                    while True:
                        try:
                            chunk = await self._next(
                                iterator, committed, deadline, loop
                            )
                        except StopAsyncIteration:
                            break
                        except UpstreamError as exc:
                            if committed or exc.status not in RETRYABLE_STATUSES:
                                raise
                            failure = exc
                            switched = True
                            break
                        if not committed:
                            # Any first frame (role / tool / reasoning / usage)
                            # commits the stream to this model.
                            committed = True
                        yield chunk
                finally:
                    await _aclose(agen)

                if not switched:
                    if not committed:
                        # A stream that ends before its first frame is an empty
                        # stream: a fixed failure, never a silent success and
                        # never a standby attempt.
                        raise ChatStreamError(_EMPTY_MESSAGE)
                    self._finish_success(model, attempts)
                    return

                now = self._now()
                delay = self._delay(failure)
                self._cool(model, now, delay)
                if failure.status == 429:
                    streak += 1
                    streak_delay = max(streak_delay, delay)
                else:
                    streak = 0
                    streak_delay = 0.0
                route_wide = (
                    getattr(failure, "limit_scope", None) in ROUTE_WIDE_SCOPES
                )
                if route_wide:
                    # An account/IP limit is not about this one model.
                    self._cool_everything(now, delay)
                elif streak >= 2:
                    # Two consecutive real 429s cool the whole route too, using
                    # the largest hint seen in the streak.
                    self._cool_everything(now, streak_delay)
                # Persist at once, before any standby waits for its first frame:
                # a cancellation there must not lose a failure that happened.
                self._persist(now)
                if route_wide or streak >= 2:
                    raise self._refusal(now, models)
                last_failure = failure

            # Every configured candidate really failed: keep the last real
            # upstream error, unless the route cooled while we were trying.
            now = self._now()
            if self._global_until > now:
                raise self._refusal(now, models)
            if last_failure is not None:
                raise last_failure
            raise self._refusal(now, models)
        except (asyncio.CancelledError, GeneratorExit):
            # A cancelled round is not a failure: no hook, no state change.
            raise
        except Exception as exc:
            self._finish_failed(attempts, exc)
            raise
