"""Daily check-in workflow.

Semantics pinned from ``references/workbuddy-auto-signin/signin.py``
(``run_auto``): query status, only claim when the account is *confirmed*
not-yet-signed, then re-verify against a fresh status before reporting success.

Guarantees:

* An unknown status is never treated as "not signed" and claimed blindly.
* ``active == false`` reports ``inactive`` and never claims.
* A claim that fails on the network re-checks the status first; if the status
  still does not confirm a signature the result is ``unconfirmed``.
* A 429 re-checks first and, when still unsigned, re-raises the original error
  so its ``Retry-After`` hint survives; ``10001``/``14001`` business errors are
  re-checked the same way before any outcome is reported.
* 401/403 from the claim are re-raised, never swallowed by the re-check.
* A handled claim failure that a re-check still cannot confirm keeps its source
  as a fixed, leak-free ``error_category`` (``network`` for a 5xx/transport
  failure, ``upstream`` for a business error) so the same fault can be
  de-duplicated downstream; a signature that a re-check confirms never carries a
  failure category.
* ``reward_credits`` is attached **only** when a fresh claim returned a usable
  ``credit`` (a finite, non-negative ``int``/``float``; ``bool`` excluded, ``0``
  kept) *and* the follow-up status read confirmed a signature.  Only the
  ``credit`` field is trusted -- ``reward``/``points``/``token`` and any other
  raw upstream field are never echoed.  ``already_signed``, a signature that was
  only confirmed by a re-check after a failed claim, and an unconfirmed round
  never report a reward.
* No network retries happen here -- per-day cross-round retry is a higher-layer
  scheduling concern, not a client concern.
"""

from __future__ import annotations

import asyncio

import aiohttp

from .credits import is_finite_number
from .errors import UpstreamError

CHECKIN_STATUS_PATH = "/v2/billing/meter/checkin-activity-status"
CHECKIN_CLAIM_PATH = "/v2/billing/meter/daily-checkin"

_SIGNED = (True, 1)
# Non-zero claim business codes that still require a fresh status re-check
# before the outcome can be reported (the claim may have actually applied).
_RECHECK_CODES = (10001, 14001)


class SigninService:
    """Runs one check-in round against the billing API."""

    def __init__(self, client):
        self._client = client

    async def run(self) -> dict:
        status = await self._status()
        if status.get("active") is False:
            return {"outcome": "inactive", "verified": False}
        if self._is_signed(status):
            return {"outcome": "already_signed", "verified": True}
        if status.get("today_checked_in") is not False:
            # Missing/ambiguous flag: do not assume "not signed".
            return {"outcome": "unknown", "verified": False}

        try:
            claim = await self._client.request_json(
                "POST", CHECKIN_CLAIM_PATH, base=self._client.billing_base, json={}
            )
        except UpstreamError as exc:
            if exc.status in (401, 403):
                raise
            if exc.status == 429:
                # Keep the server cooldown visible unless a re-check confirms a
                # signature; never swallow the retry hint.
                return await self._recheck_after_failed_claim(exc)
            if exc.status == 200 and exc.code not in _RECHECK_CODES:
                # Envelope business error: claim did not succeed, do not
                # double-claim.  Preserve the source as a fixed category so the
                # same failure merges with the client's own observation.
                return {
                    "outcome": "unconfirmed",
                    "verified": False,
                    "error_category": "upstream",
                }
            # Any other handled failure keeps its fixed, safe category so a
            # re-check that still cannot confirm stays attributable.
            category = (
                "network"
                if isinstance(exc.status, int) and exc.status >= 500
                else "upstream"
            )
            return await self._recheck_after_failed_claim(error_category=category)
        except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError):
            return await self._recheck_after_failed_claim(error_category="network")

        # The claim's own payload is the only source of an attributable reward.
        return await self._verify_after_claim(claim)

    async def _status(self) -> dict:
        return await self._client.request_json(
            "POST", CHECKIN_STATUS_PATH, base=self._client.billing_base, json={}
        )

    async def _recheck_after_failed_claim(self, original=None, error_category=None) -> dict:
        status = await self._status()
        if self._is_signed(status):
            return {"outcome": "already_signed", "verified": True}
        if original is not None:
            raise original
        result = {"outcome": "unconfirmed", "verified": False}
        if error_category is not None:
            result["error_category"] = error_category
        return result

    async def _verify_after_claim(self, claim=None) -> dict:
        status = await self._status()
        if self._is_signed(status):
            result = {"outcome": "signed", "verified": True}
            credit = self._claim_reward(claim)
            if credit is not None:
                result["reward_credits"] = credit
            return result
        return {"outcome": "unconfirmed", "verified": False}

    @staticmethod
    def _claim_reward(claim):
        """Return the claim's usable non-negative finite ``credit`` or ``None``.

        Only the ``credit`` field is ever trusted; ``reward``/``points``/``token``
        and every other raw upstream field are deliberately ignored.  A whole
        ``0`` is a real (free) reward and is kept, never treated as "missing".
        """
        if not isinstance(claim, dict):
            return None
        value = claim.get("credit")
        if is_finite_number(value) and value >= 0:
            return value
        return None

    @staticmethod
    def _is_signed(status) -> bool:
        return status.get("today_checked_in") in _SIGNED
