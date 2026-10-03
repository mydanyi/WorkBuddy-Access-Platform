"""Pure credit-multiplier and promotion helpers (no I/O, no static prices).

Every value is parsed from the live upstream document; nothing here is written
to disk or hard-coded as a price table.  Shared by the client and the gateway.
"""

from __future__ import annotations

import math
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

__all__ = [
    "is_finite_number",
    "parse_credits",
    "format_credits",
    "select_promotion",
    "promotion_discount",
    "promotion_label",
    "promotion_note",
    "display_model_id",
    "upstream_model_id",
]

# ``x0.11 credits`` / ``x0.79`` / ``0.50x`` -- a leading/trailing ``x`` and an
# optional ``credits`` suffix around a plain decimal.  Anything wordier is not a
# multiplier.
_CREDITS_RE = re.compile(
    r"^\s*(?:x\s*)?(?P<num>[0-9]+(?:\.[0-9]+)?)\s*(?:x|credits?)?\s*$",
    re.IGNORECASE,
)

_HHMM_RE = re.compile(r"^\s*(\d{1,2}):(\d{2})(?::\d{2})?\s*$")

# The display alias carries the pure model id plus one *standard* bracket suffix:
# ``<id> [<multiplier>x]`` or ``<id> [<multiplier>x · <single-line label>]`` and
# the unknown form ``<id> [倍率未知]``.  Only this exact, end-anchored shape may
# be stripped back to the upstream id; free-form brackets are never touched.
_ALIAS_MULTIPLIER = r"[0-9]+(?:\.[0-9]+)?x|倍率未知"
_ALIAS_MULTIPLIER_RE = re.compile(r"^(?:" + _ALIAS_MULTIPLIER + r")$")
_ALIAS_SUFFIX_RE = re.compile(
    r" \[(?:" + _ALIAS_MULTIPLIER + r")(?: · [^\[\]\r\n]+)?\]$"
)


def _sanitize_alias_label(label) -> str:
    """Clean a promo badge so it can never break the standard alias suffix."""
    if not isinstance(label, str):
        return ""
    cleaned = label.replace("[", "").replace("]", "")
    cleaned = cleaned.replace("\r", " ").replace("\n", " ")
    return cleaned.strip()


def display_model_id(detail) -> str:
    """Build the AstrBot-facing alias ``"<id> [<credits>x · <label>]"``.

    The *id* is the upstream model id (never the JSON ``name``) so AstrBot can
    resolve it; the bracket suffix is limited to the standard forms so
    :func:`upstream_model_id` can reverse it.  A missing / unusable multiplier
    becomes ``倍率未知``.
    """
    model_id = ""
    if isinstance(detail, dict):
        raw_id = detail.get("id")
        if isinstance(raw_id, str):
            model_id = raw_id
    effective = detail.get("effective_credits") if isinstance(detail, dict) else None
    if isinstance(effective, str) and _ALIAS_MULTIPLIER_RE.match(effective):
        credits = effective
    else:
        credits = "倍率未知"
    label = _sanitize_alias_label(
        detail.get("promo_label") if isinstance(detail, dict) else None
    )
    if label:
        return "{} [{} · {}]".format(model_id, credits, label)
    return "{} [{}]".format(model_id, credits)


def upstream_model_id(value):
    """Strip only this plugin's standard alias suffix; anything else is kept.

    ``"wb-1 [0.11x · 夜间折扣]"`` -> ``"wb-1"`` while a plain id or a
    non-standard ``" [..]"`` tail (even a mid-name bracket) is returned as-is.
    A non-``str`` is returned unchanged and never fed to the regex.
    """
    if not isinstance(value, str):
        return value
    return _ALIAS_SUFFIX_RE.sub("", value)


def is_finite_number(value) -> bool:
    """True for a real, finite ``int``/``float`` (never ``bool``)."""
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def parse_credits(value):
    """Return the non-negative multiplier encoded by ``value`` or ``None``.

    Accepts a numeric multiplier or the upstream string shapes
    (``"x0.11 credits"``, ``"x0.79"``, ``"0.50x"``).  ``bool`` / ``NaN`` /
    ``inf`` / negatives / arbitrary prose are *not* multipliers.  Whole-number
    ``0`` is a legal (free) multiplier and is never conflated with "missing"
    (no truthiness shortcut is used).
    """
    if isinstance(value, bool):
        return None
    if is_finite_number(value):
        return value if value >= 0 else None
    if isinstance(value, str):
        match = _CREDITS_RE.match(value)
        if match is None:
            return None
        try:
            number = Decimal(match.group("num"))
        except InvalidOperation:
            return None
        if not number.is_finite():
            return None
        return float(number)
    return None


def format_credits(value):
    """Return the canonical ``"0.11x"`` / ``"0x"`` display string or ``None``.

    Strict decimal formatting with trailing zeros stripped, so ``0.50`` becomes
    ``"0.5x"`` and ``0`` becomes ``"0x"``.
    """
    if not is_finite_number(value) or value < 0:
        return None
    text = format(Decimal(str(value)), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if not text:
        text = "0"
    return text + "x"


def _rfc3339(value):
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    if raw[-1] in ("Z", "z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def _hhmm(value):
    if not isinstance(value, str):
        return None
    match = _HHMM_RE.match(value)
    if match is None:
        return None
    hours, minutes = int(match.group(1)), int(match.group(2))
    if hours > 23 or minutes > 59:
        return None
    return hours * 60 + minutes


def _schedule_active(schedule, now) -> bool:
    """Whether ``schedule`` admits ``now``; an unusable schedule is inactive."""
    if schedule is None:
        return True
    if not isinstance(schedule, dict):
        return False

    tz = None
    tz_name = schedule.get("timezone")
    if tz_name is not None:
        if not isinstance(tz_name, str):
            return False
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            return False

    valid_from = schedule.get("validFrom")
    if valid_from is not None:
        start = _rfc3339(valid_from)
        if start is None or now < start:
            return False
    valid_until = schedule.get("validUntil")
    if valid_until is not None:
        end = _rfc3339(valid_until)
        if end is None or now >= end:
            return False

    daily = schedule.get("daily")
    if daily is None:
        return True
    if isinstance(daily, dict):
        # Tolerate a legacy single-window mapping; the live shape is a list.
        daily = [daily]
    if not isinstance(daily, list):
        # An explicitly malformed shape (e.g. "broken" / False / 123) must never
        # be read as "all day free"; only a real list is a valid window set.
        return False
    if not daily:
        return True

    local = now.astimezone(tz) if tz is not None else now
    current = local.hour * 60 + local.minute
    for window in daily:
        if not isinstance(window, dict):
            return False
        start = _hhmm(window.get("start"))
        end = _hhmm(window.get("end"))
        if start is None or end is None:
            return False
        if start == end:
            return True
        if start < end:
            if start <= current < end:
                return True
        elif current >= start or current < end:
            # Cross-midnight window: [start, 24:00) U [00:00, end).
            return True
    return False


def select_promotion(promotions, model_id, now):
    """Return the highest-priority *active* promotion for ``model_id`` or None."""
    if not isinstance(promotions, list):
        return None
    best = None
    best_priority = None
    for promo in promotions:
        if not isinstance(promo, dict) or promo.get("enabled") is not True:
            continue
        ids = promo.get("modelIds")
        if not isinstance(ids, list) or model_id not in ids:
            continue
        if not _schedule_active(promo.get("schedule"), now):
            continue
        priority = promo.get("priority")
        priority = priority if is_finite_number(priority) else 0
        if best is None or priority > best_priority:
            best = promo
            best_priority = priority
    return best


def promotion_discount(promo, base):
    """Return the active promo's effective multiplier, or ``None`` to keep base.

    ``discount.discountedCredits`` wins over ``factor * base``.  A factor of
    exactly ``0`` is a real free discount (not "missing") and needs no known
    base.  A bare ``hover`` figure is never parsed into a discount.
    """
    if not isinstance(promo, dict):
        return None
    discount = promo.get("discount")
    if not isinstance(discount, dict):
        return None
    fixed = discount.get("discountedCredits")
    if fixed is not None:
        parsed = parse_credits(fixed)
        if parsed is not None:
            return parsed
    factor = discount.get("factor")
    if is_finite_number(factor) and factor >= 0:
        if factor == 0:
            return 0
        if base is not None:
            return factor * base
    return None


def promotion_label(promo) -> str:
    badge = promo.get("badge") if isinstance(promo, dict) else None
    label = badge.get("label") if isinstance(badge, dict) else None
    return label if isinstance(label, str) else ""


def promotion_note(promo) -> str:
    hover = promo.get("hover") if isinstance(promo, dict) else None
    note = hover.get("textZh") if isinstance(hover, dict) else None
    return note if isinstance(note, str) else ""
