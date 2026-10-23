"""Shared admission gate for chat traffic.

One gate enforces both an in-flight concurrency cap (plus a bounded wait queue)
and an optional per-minute request budget.  The gateway holds a single
``ConcurrencyGate`` so the real bot chat and any future growth chat share the
same budget instead of each opening its own door.

Contract:

* The active + queued counts are exact: a request is admitted, queued or
  rejected, never silently dropped.
* The RPM check runs *before* a request is queued, and only for requests that
  already passed authorization in the caller.  Admission may also *pre-check*
  the budget without consuming it (``count_rpm=False``); the chat layer then
  spends exactly one permit per real upstream POST (``consume_rpm``), so a
  standby attempt or a 401 refresh resend is counted once, never twice, and a
  refused attempt consumes nothing.
* A full queue or an expired queue wait raises :class:`LimitExceeded`; the
  caller maps it to HTTP 429 with a ``Retry-After`` hint.
* ``close`` releases every waiter so a shutdown does not leave tasks parked.
"""

from __future__ import annotations

import asyncio
import collections
import time
from contextlib import asynccontextmanager

_WINDOW_SECONDS = 60.0


class LimitExceeded(Exception):
    """A request was refused by the admission gate."""

    def __init__(self, reason, retry_after=None):
        self.reason = reason
        self.retry_after = retry_after
        super().__init__(reason)


class ConcurrencyGate:
    """Admission control shared by every chat entry point."""

    def __init__(
        self,
        *,
        concurrency=1,
        queue_size=0,
        queue_timeout=None,
        rpm=None,
        clock=None,
    ):
        self._concurrency = max(1, int(concurrency))
        self._queue_size = max(0, int(queue_size))
        self._queue_timeout = queue_timeout
        self._rpm = int(rpm) if rpm else None
        self._clock = clock or time.monotonic
        self._lock = asyncio.Lock()
        self._active = 0
        self._waiters = collections.deque()
        self._rpm_events = collections.deque()
        self._closed = False

    @property
    def active(self) -> int:
        return self._active

    @property
    def queued(self) -> int:
        return len(self._waiters)

    def _trim_rpm_locked(self, now) -> None:
        while self._rpm_events and now - self._rpm_events[0] >= _WINDOW_SECONDS:
            self._rpm_events.popleft()

    def _refuse_rpm_locked(self, now):
        retry = _WINDOW_SECONDS - (now - self._rpm_events[0])
        raise LimitExceeded("rpm", retry_after=max(1.0, retry))

    def _precheck_rpm_locked(self) -> None:
        """Judge the budget *without* consuming a permit (admission time)."""
        if self._rpm is None:
            return
        now = self._clock()
        self._trim_rpm_locked(now)
        if len(self._rpm_events) >= self._rpm:
            self._refuse_rpm_locked(now)

    def _check_rpm_locked(self) -> None:
        """Judge the budget and consume one permit for a real upstream POST."""
        if self._rpm is None:
            return
        now = self._clock()
        self._trim_rpm_locked(now)
        if len(self._rpm_events) >= self._rpm:
            self._refuse_rpm_locked(now)
        self._rpm_events.append(now)

    async def acquire(self, *, count_rpm=True) -> None:
        """Reserve a slot, waiting in the queue if necessary.

        ``count_rpm=True`` keeps the historic behaviour: the permit is consumed
        before enqueueing and a refused request never consumes one.
        ``count_rpm=False`` only *judges* the budget here and leaves the permit
        to :meth:`consume_rpm`, which the chat layer calls right before each real
        upstream POST.
        """
        if self._closed:
            raise LimitExceeded("stopped", retry_after=1.0)
        async with self._lock:
            if count_rpm:
                # RPM is judged before enqueue; a refused request never consumes it.
                self._check_rpm_locked()
            else:
                self._precheck_rpm_locked()
            if self._closed:
                raise LimitExceeded("stopped", retry_after=1.0)
            if self._active < self._concurrency:
                self._active += 1
                return
            if len(self._waiters) >= self._queue_size:
                raise LimitExceeded("queue_full", retry_after=1.0)
            fut = asyncio.get_running_loop().create_future()
            self._waiters.append(fut)

        timeout = self._queue_timeout
        try:
            if timeout is not None:
                await asyncio.wait_for(fut, timeout)
            else:
                await fut
        except asyncio.CancelledError:
            await self._discard_waiter(fut)
            raise
        except (asyncio.TimeoutError, TimeoutError):
            await self._discard_waiter(fut)
            raise LimitExceeded("queue_timeout", retry_after=1.0) from None

    async def _discard_waiter(self, fut) -> None:
        async with self._lock:
            try:
                self._waiters.remove(fut)
            except ValueError:
                # A concurrent release already handed the slot to us; the caller
                # aborted anyway, so pass the slot on rather than leaking it.
                if fut.done() and not fut.cancelled():
                    self._handoff_locked()

    def _handoff_locked(self) -> None:
        while self._waiters:
            nxt = self._waiters.popleft()
            if not nxt.done():
                nxt.set_result(None)
                return
        self._active = max(0, self._active - 1)

    async def release(self) -> None:
        """Return a slot, handing it directly to the next waiter if any."""
        async with self._lock:
            self._handoff_locked()

    async def consume_rpm(self) -> None:
        """Consume one request permit right before a real upstream chat POST."""
        async with self._lock:
            if self._closed:
                raise LimitExceeded("stopped", retry_after=1.0)
            self._check_rpm_locked()

    @asynccontextmanager
    async def slot(self, *, count_rpm=True):
        await self.acquire(count_rpm=count_rpm)
        try:
            yield
        finally:
            await self.release()

    async def close(self) -> None:
        """Refuse new slots and wake every queued waiter with an error."""
        async with self._lock:
            self._closed = True
            waiters = list(self._waiters)
            self._waiters.clear()
        for fut in waiters:
            if not fut.done():
                fut.set_exception(LimitExceeded("stopped", retry_after=1.0))
