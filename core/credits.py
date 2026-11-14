"""积分倍率与促销的纯函数（无 I/O、无写死的价目表）。

这里只做两件事：把上游给的倍率字符串读成数字并规范化成 ``"0.79x"`` 这种稳定写法，
以及在这串数字外面拼一个能被精确还原的方括号后缀。

**为什么拼在模型 ID 后面**：AstrBot 的模型下拉框只认 ``/v1/models`` 里的 ``id``，
没有第二个字段能显示价格。所以显示名就是 ID 本身，倍率挂在 ID 尾部；请求回来时
（:func:`core.models.strip_model_alias`）再原样剥掉。拼与剥必须是同一种形状，
``tests/test_credits.py`` 里那条 round-trip 用例负责盯着两边别走散。
"""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from .numbers import is_finite_number

__all__ = [
    "parse_credits",
    "format_credits",
    "select_promotion",
    "promotion_discount",
    "promotion_label",
    "promotion_note",
    "display_model_id",
]

# ``x0.11 credits`` / ``x0.79`` / ``0.50x`` —— 前后各一个可选的 ``x``，中间是
# 十进制数。再花哨的写法就不当倍率了，宁可显示成"没有倍率"。
_CREDITS_RE = re.compile(
    r"^\s*(?:x\s*)?(?P<num>[0-9]+(?:\.[0-9]+)?)\s*(?:x|credits?)?\s*$",
    re.IGNORECASE,
)

_HHMM_RE = re.compile(r"^\s*(\d{1,2}):(\d{2})(?::\d{2})?\s*$")

# 后缀只有这一种形状：``[<倍数>x]`` 或 ``[<倍数>x · <单行标签>]``。拼的时候按它
# 拼，剥的时候（core.models）按同一条正则剥，两边必须一致。
_ALIAS_MULTIPLIER_RE = re.compile(r"^(?:[0-9]+(?:\.[0-9]+)?x|倍率未知)$")


def _sanitize_alias_label(label) -> str:
    """把促销徽章洗一遍，让它不可能破坏后缀的形状。"""
    if not isinstance(label, str):
        return ""
    cleaned = label.replace("[", "").replace("]", "")
    cleaned = cleaned.replace("\r", " ").replace("\n", " ")
    return cleaned.strip()


def display_model_id(detail) -> str:
    """拼出给 AstrBot 看的显示名 ``"<id> [<倍数>x · <标签>]"``。

    只拼有倍率的条目：没有倍率的一律返回裸 ID，绝不写"倍率未知"——那会让一个
    单纯没报价的模型看起来像"价格读失败了"。调用方（网关）只在真拿到倍率时才
    进来，所以这里的 ``倍率未知`` 分支只是对脏数据的兜底。
    """
    model_id = ""
    if isinstance(detail, dict):
        raw_id = detail.get("id")
        if isinstance(raw_id, str):
            model_id = raw_id
    effective = detail.get("effective_credits") if isinstance(detail, dict) else None
    if not (isinstance(effective, str) and _ALIAS_MULTIPLIER_RE.match(effective)):
        return model_id
    label = _sanitize_alias_label(
        detail.get("promo_label") if isinstance(detail, dict) else None
    )
    if label:
        return "{} [{} · {}]".format(model_id, effective, label)
    return "{} [{}]".format(model_id, effective)


def parse_credits(value):
    """把 ``value`` 解析成非负倍率，解析不出来返回 ``None``。

    接受数字，以及上游那几种字符串写法（``"x0.11 credits"``、``"x0.79"``、
    ``"0.50x"``）。``bool`` / ``NaN`` / ``inf`` / 负数 / 任意散文都**不是**倍率。
    整数 ``0`` 是合法的（免费），绝不与"缺失"混为一谈——所以全程不走真值判断。
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
    """规范成 ``"0.11x"`` / ``"0x"``；非法值返回 ``None``。

    严格十进制、去掉末尾的零：``0.50`` → ``"0.5x"``，``0`` → ``"0x"``。
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
    """``schedule`` 是否允许 ``now``；配置坏掉一律当"不生效"。"""
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
        # 兼容只有一段窗口的旧形状；线上现在给的是列表。
        daily = [daily]
    if not isinstance(daily, list):
        # 形状不对（``"broken"`` / ``False`` / ``123``）绝不能被读成"全天免费"。
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
            # 跨零点的窗口：[start, 24:00) ∪ [00:00, end)。
            return True
    return False


def select_promotion(promotions, model_id, now):
    """返回 ``model_id`` 当前生效、优先级最高的促销；没有就 ``None``。"""
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
    """促销后的实际倍率；``None`` 表示维持原价。

    ``discount.discountedCredits`` 优先于 ``factor * base``。``factor`` 恰好为
    ``0`` 是真的免费（不是"缺失"），也不需要有已知原价。``hover`` 里的数字永远
    不参与计算。
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
