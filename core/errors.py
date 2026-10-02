"""Error types raised by the WorkBuddy HTTP layer.

Only the fields the public contract needs are exposed.  The raw upstream
response body (which may carry tokens or account data) is deliberately *never*
stored on an exception, so ``str()``/``repr()`` cannot leak it.
"""

from __future__ import annotations


class WorkBuddyError(Exception):
    """Base class for every error raised by this plugin."""


class UpstreamError(WorkBuddyError):
    """An upstream call failed.

    ``status`` is the HTTP status (``200`` when the envelope carried a non-zero
    business ``code``).  ``code`` is the business code when the body supplied
    one, else ``None``.  ``retry_after`` is the parsed ``Retry-After`` value in
    seconds for a 429 (else ``None``).  The body itself is not retained.
    """

    def __init__(self, status, code=None, retry_after=None):
        self.status = status
        self.code = code
        self.retry_after = retry_after
        parts = ["upstream error (status={}".format(status)]
        if code is not None:
            parts.append(", code={}".format(code))
        parts.append(")")
        super().__init__("".join(parts))


class ChatStreamError(WorkBuddyError):
    """The upstream chat stream was malformed or ended without a terminator.

    Raised for an empty stream, a ``[DONE]``-only stream, malformed JSON, an
    upstream ``error`` event and an EOF that never carried ``[DONE]`` or a
    ``finish_reason``.  ``status`` is the gateway HTTP status to report (``502``
    unless a caller overrides it) and ``retry_after`` mirrors ``UpstreamError``.
    """

    def __init__(self, message, status=502, retry_after=None):
        self.status = status
        self.retry_after = retry_after
        super().__init__(message)


class LoginError(WorkBuddyError):
    """The interactive OAuth login could not be completed."""


class LoginTimeoutError(LoginError):
    """``complete_login`` never observed a finished login."""


class LoginStateError(LoginError):
    """The state passed to ``complete_login`` was not issued by the active
    ``begin_login`` (stale or foreign state)."""
