"""Daily check-in scheduler around :class:`core.signin.SigninService`.

The scheduler is a *policy* layer: :class:`SigninService` performs one network
round, the scheduler decides **when** a round may run and remembers the outcome
across restarts.  It reuses the real service and the real :class:`StateStore`
and adds no network retry of its own.

Pinned contract (see ``tests/test_scheduler.py``)::

    SigninScheduler(service, state, timezone="Asia/Shanghai",
                    primary="00:10", recovery="12:10", jitter=120,
                    max_attempts=5, enabled=True, clock=None, on_success=None,
                    on_result=None)

    await run_once(manual=False) -> dict   # one round, serialized
    await tick() -> dict                    # one scheduler decision point
    start()                                 # non-async: ticks once, then sleeps
    await stop()                            # cancels the loop / in-flight round
    resume()                                # clear a pause / notlogin state
    status() -> {"enabled", "paused", "date", "attempts_today",
                 "last_success_date", "last_result", "last_outcome"}

Semantics:

* ``enabled`` only gates the *automatic* tick.  A manual round still runs when
  the automatic check-in switch is off, but it shares the same persisted per-day
  attempt budget and can never bypass a pause or a hard 429 cooldown.
* The business date follows the configured timezone; the daily attempt budget
  (``max_attempts``) is shared by automatic and manual rounds, counted and
  persisted *before* the round runs, and never refunded on cancellation.
* A verified success is attributed to the business date the round **started**
  under (``state["date"]``), never to the wall-clock day the response arrived.
  ``on_success`` fires once for that day.
* ``on_result`` is an optional callback invoked *once per genuinely executed*
  round (blocked ticks and same-day duplicates never call it), just before the
  round's safe summary is returned.  It may be sync or async; a failing callback
  never changes the business result while a ``CancelledError`` always propagates.
  The summary carries ``outcome``/``verified``/``date``, a validated
  ``reward_credits`` when signed, ``http_status`` for an upstream HTTP error and
  ``retry_after`` for a rate-limit/backoff; no raw collaborator field leaks.
* ``401``/``403`` pause all rounds until :meth:`resume`; ``notlogin`` waits for
  authorization the same way.
* ``429`` installs a hard cooldown of at least ``Retry-After`` seconds which
  also survives a day boundary.  Network/``5xx`` failures back off with a
  bounded 60/300/1800s schedule.  A manual round may override the soft backoff
  but never the hard cooldown, a pause or the daily budget.
* Every round returns a small, desensitized result dict; the raw collaborator
  payload (which may carry a token) is never echoed.
"""

from __future__ import annotations

import asyncio
import inspect
import random
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiohttp

from .credits import is_finite_number
from .errors import UpstreamError

# Bounded automatic backoff after a network / 5xx failure (seconds).
_BACKOFF_SECONDS = (60.0, 300.0, 1800.0)
# Fallback cooldown for a 429 that carried no usable Retry-After.
_DEFAULT_COOLDOWN = 60.0
# Length of one bounded sleep in the background loop.
_TICK_INTERVAL = 60.0
_STATE_SECTION = "scheduler"
# The only failure categories the scheduler may carry from a handled service
# result into its safe summary; every other value is dropped.
_SAFE_ERROR_CATEGORIES = ("network", "upstream", "rate", "auth", "permission")


def _parse_hhmm(value: str) -> tuple[int, int]:
    """Parse ``HH:MM``; raise ``ValueError`` on anything out of range."""
    if not isinstance(value, str) or value.count(":") != 1:
        raise ValueError("time must be HH:MM")
    head, tail = value.split(":")
    try:
        hour = int(head)
        minute = int(tail)
    except (TypeError, ValueError):
        raise ValueError("time must be HH:MM") from None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError("time must be HH:MM")
    return hour, minute


class SigninScheduler:
    """Decides when the check-in service may run and remembers the outcome."""

    def __init__(
        self,
        service,
        state,
        *,
        timezone="Asia/Shanghai",
        primary="00:10",
        recovery="12:10",
        jitter=120,
        max_attempts=5,
        enabled=True,
        clock=None,
        on_success=None,
        on_result=None,
    ):
        self._service = service
        self._state = state
        self._tz = ZoneInfo(timezone)  # invalid zone raises immediately
        self._ph, self._pm = _parse_hhmm(primary)
        self._rh, self._rm = _parse_hhmm(recovery)
        self._jitter = max(0, int(jitter))
        self._max_attempts = max(0, int(max_attempts))
        self._enabled = bool(enabled)
        self._clock = clock or time.time
        self._on_success = on_success
        self._on_result = on_result

        self._lock = asyncio.Lock()
        self._loop_task = None
        self._inflight_task = None

    # -- time helpers --------------------------------------------------------

    def _now(self) -> float:
        return float(self._clock())

    def _local_date_str(self) -> str:
        return datetime.fromtimestamp(self._now(), self._tz).strftime("%Y-%m-%d")

    def _window_open(self, state: dict, now: float) -> bool:
        """True once the day's primary (or recovery) window has opened."""
        local = datetime.fromtimestamp(now, self._tz)
        slot = state.get("primary_slot") or 0
        base = (local.year, local.month, local.day)
        primary = datetime(base[0], base[1], base[2], self._ph, self._pm, tzinfo=self._tz)
        recovery = datetime(base[0], base[1], base[2], self._rh, self._rm, tzinfo=self._tz)
        offset = timedelta(seconds=slot)
        return now >= (primary + offset).timestamp() or now >= (recovery + offset).timestamp()

    # -- persistence ---------------------------------------------------------

    def _load(self) -> dict:
        stored = self._state.load().get(_STATE_SECTION)
        return dict(stored) if isinstance(stored, dict) else {}

    def _save(self, state: dict) -> None:
        self._state.update(_STATE_SECTION, dict(state))

    def _ensure_day(self) -> dict:
        """Return state for the current business day, rolling over if needed."""
        state = self._load()
        today = self._local_date_str()
        if state.get("date") != today:
            slot = random.randint(0, self._jitter) if self._jitter > 0 else 0
            state = {
                "date": today,
                "attempts": 0,
                "verified": False,
                "retry_at": None,
                "backoff_index": 0,
                "stopped": False,
                "stop_reason": None,
                "primary_slot": slot,
                # A pause and a 429 cooldown deliberately survive the day boundary.
                "paused": bool(state.get("paused", False)),
                "cooldown_until": state.get("cooldown_until"),
                "last_success_date": state.get("last_success_date"),
                "last_result": state.get("last_result"),
            }
            self._save(state)
        return state

    # -- decision ------------------------------------------------------------

    def _block_reason(self, state: dict, now: float, manual: bool):
        """Return a fixed reason string when the round may not run, else ``None``.

        ``enabled`` only gates automatic rounds; a manual round is still bound by
        a pause, a hard cooldown, the daily budget and a same-day success.
        """
        if not self._enabled and not manual:
            return "disabled"
        if state.get("paused"):
            return "paused"
        if state.get("stopped"):
            if state.get("verified"):
                return "already_signed"
            return state.get("stop_reason") or "inactive"
        if int(state.get("attempts", 0)) >= self._max_attempts:
            return "budget"
        cooldown = state.get("cooldown_until")
        if cooldown is not None and now < cooldown:
            return "cooldown"
        if not manual:
            retry_at = state.get("retry_at")
            if retry_at is not None and now < retry_at:
                return "backoff"
            if not self._window_open(state, now):
                return "not_due"
        return None

    async def run_once(self, manual=False) -> dict:
        """Run a single round; returns a safe, desensitized summary dict."""
        return await self._round(bool(manual))

    async def tick(self) -> dict:
        """One scheduler decision point; runs at most one bounded round."""
        return await self._round(False)

    async def _round(self, manual: bool) -> dict:
        async with self._lock:
            self._inflight_task = asyncio.current_task()
            try:
                state = self._ensure_day()
                now = self._now()
                reason = self._block_reason(state, now, manual)
                if reason is not None:
                    if reason == "already_signed":
                        return self._summary(state, "already_signed", True)
                    return self._summary(state, "blocked", False, reason=reason)
                # Count the attempt *before* it runs: a cancellation is not a refund.
                state["attempts"] = int(state.get("attempts", 0)) + 1
                self._save(state)
                summary = await self._execute(state, now)
                # A genuinely executed round announces its safe summary exactly
                # once, just before returning (never for a blocked round).
                await self._fire_on_result(summary)
                return summary
            finally:
                self._inflight_task = None

    async def _execute(self, state: dict, now: float) -> dict:
        try:
            result = await self._service.run()
        except UpstreamError as exc:
            return self._on_upstream_error(state, now, exc)
        except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError):
            return self._on_backoff(state, now, "network")
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - any unexpected failure backs off, never spins
            return self._on_backoff(state, now, "error")

        outcome = result.get("outcome") if isinstance(result, dict) else None
        verified = bool(result.get("verified")) if isinstance(result, dict) else False

        if verified:
            return await self._record_success(state, outcome or "signed", result)
        if outcome in ("inactive", "unknown"):
            # A terminal, well-defined non-success: stop the day, do not spin.
            state["stopped"] = True
            state["stop_reason"] = outcome
            self._save(state)
            return self._summary(state, outcome, False)
        if outcome == "notlogin":
            state["paused"] = True
            self._save(state)
            return self._summary(state, "notlogin", False)
        # ``unconfirmed`` (and anything unrecognized) retries on a bounded backoff.
        error_category = None
        if isinstance(result, dict):
            candidate = result.get("error_category")
            if candidate in _SAFE_ERROR_CATEGORIES:
                error_category = candidate
        return self._on_backoff(
            state, now, outcome or "unconfirmed", error_category=error_category
        )

    async def _record_success(self, state: dict, outcome: str, result=None) -> dict:
        # Success belongs to the business date the round *started* under, never
        # to the wall-clock day the (possibly post-midnight) response arrived.
        day = state.get("date") or self._local_date_str()
        first_today = state.get("last_success_date") != day
        state["verified"] = True
        state["stopped"] = True
        state["stop_reason"] = "already_signed"
        state["retry_at"] = None
        state["last_success_date"] = day
        self._save(state)
        if first_today and self._on_success is not None:
            try:
                outcome_obj = self._on_success()
                if inspect.isawaitable(outcome_obj):
                    await outcome_obj
            except Exception:  # noqa: BLE001 - a callback failure never reverts the sign-in
                pass
        if outcome not in ("signed", "already_signed"):
            outcome = "signed"
        # Only a validated reward is surfaced; an unusable one is never guessed.
        reward = self._safe_reward_credits(result)
        return self._summary(state, outcome, True, reward_credits=reward)

    @staticmethod
    def _safe_reward_credits(result):
        """Return a finite non-negative numeric ``reward_credits`` or ``None``.

        Mirrors :func:`core.credits.is_finite_number` plus a ``>= 0`` bound;
        ``bool``/``NaN``/``inf``/negatives/strings are rejected and ``0`` is kept.
        """
        if not isinstance(result, dict):
            return None
        value = result.get("reward_credits")
        if is_finite_number(value) and value >= 0:
            return value
        return None

    async def _fire_on_result(self, summary: dict) -> None:
        """Invoke ``on_result`` once, safely; ``CancelledError`` always propagates."""
        if self._on_result is None:
            return
        try:
            outcome_obj = self._on_result(dict(summary))
            if inspect.isawaitable(outcome_obj):
                await outcome_obj
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a callback failure never changes the round
            pass

    def _on_upstream_error(self, state: dict, now: float, exc) -> dict:
        status = getattr(exc, "status", None)
        http_status = status if isinstance(status, int) and not isinstance(status, bool) else None
        if status in (401, 403):
            state["paused"] = True
            self._save(state)
            return self._summary(state, "paused", False, http_status=http_status)
        if status == 429:
            retry_after = getattr(exc, "retry_after", None)
            if (
                isinstance(retry_after, (int, float))
                and not isinstance(retry_after, bool)
                and retry_after > 0
            ):
                delay = float(retry_after)
            else:
                delay = _DEFAULT_COOLDOWN
            state["cooldown_until"] = now + delay
            self._save(state)
            return self._summary(
                state, "rate_limited", False, http_status=http_status, retry_after=delay
            )
        if isinstance(status, int) and status >= 500:
            return self._on_backoff(state, now, "network", http_status=http_status)
        # Unknown business error (including an HTTP 200 body code): no blind retry.
        state["stopped"] = True
        state["stop_reason"] = "error"
        self._save(state)
        return self._summary(state, "error", False, http_status=http_status)

    def _on_backoff(
        self, state: dict, now: float, outcome: str, http_status=None, error_category=None
    ) -> dict:
        index = int(state.get("backoff_index", 0))
        delay = _BACKOFF_SECONDS[index] if index < len(_BACKOFF_SECONDS) else _BACKOFF_SECONDS[-1]
        state["retry_at"] = now + delay
        state["backoff_index"] = index + 1
        self._save(state)
        return self._summary(
            state,
            outcome,
            False,
            http_status=http_status,
            retry_after=delay,
            error_category=error_category,
        )

    def _summary(
        self,
        state: dict,
        outcome: str,
        verified: bool,
        reason=None,
        reward_credits=None,
        http_status=None,
        retry_after=None,
        error_category=None,
    ) -> dict:
        result = {
            "outcome": outcome,
            "verified": bool(verified),
            "date": state.get("date"),
        }
        if reason is not None:
            result["reason"] = reason
        if is_finite_number(reward_credits) and reward_credits >= 0:
            result["reward_credits"] = reward_credits
        if isinstance(http_status, int) and not isinstance(http_status, bool):
            result["http_status"] = http_status
        if is_finite_number(retry_after) and retry_after > 0:
            result["retry_after"] = retry_after
        if error_category in _SAFE_ERROR_CATEGORIES:
            result["error_category"] = error_category
        state["last_result"] = result
        self._save(state)
        return dict(result)

    # -- pause / status ------------------------------------------------------

    def resume(self) -> None:
        """Clear a pause / awaiting-authorization state (persisted)."""
        state = self._load()
        state["paused"] = False
        self._save(state)

    def status(self) -> dict:
        state = self._load()
        today = self._local_date_str()
        current = state.get("date") == today
        last = state.get("last_result")
        return {
            "enabled": bool(self._enabled),
            "paused": bool(state.get("paused", False)),
            "date": today,
            "attempts_today": int(state.get("attempts", 0)) if current else 0,
            "last_success_date": state.get("last_success_date"),
            "last_result": last if isinstance(last, dict) else None,
            "last_outcome": last.get("outcome") if isinstance(last, dict) else None,
        }

    # -- background loop -----------------------------------------------------

    def start(self) -> None:
        """Start the background loop (non-async: one round, then bounded sleeps)."""
        if self._loop_task is not None and not self._loop_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._loop_task = loop.create_task(self._run_loop(), name="wb-signin-scheduler")

    async def _run_loop(self) -> None:
        while True:
            await self.tick()
            await asyncio.sleep(_TICK_INTERVAL)

    async def stop(self) -> None:
        """Cancel the loop and any in-flight round, then persist necessary state."""
        current = asyncio.current_task()
        tasks = []
        for task in (self._loop_task, self._inflight_task):
            if task is not None and task is not current and task not in tasks:
                tasks.append(task)
        self._loop_task = None
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        # Belt-and-braces: ensure the current state is flushed to disk.
        self._save(self._ensure_day())
