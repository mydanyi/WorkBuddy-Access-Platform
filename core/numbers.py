"""数值判断的公共小工具。

原先这些函数住在 ``core.credits`` 里，而 ``credits`` 是倍率与促销的解析器——整套
倍率逻辑随私有模型目录一起下线之后，还有几处在用的只剩"这是不是一个真数字"。
把它们单独放一层，免得为了一个 ``is_finite_number`` 去 import 一个已经不存在的模块。
"""

from __future__ import annotations

import math

__all__ = ["is_finite_number"]


def is_finite_number(value) -> bool:
    """True for a real, finite ``int``/``float`` (never ``bool``)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        # ``10**400`` 这种整数：类型对、也不是 ``bool``，但 ``math.isfinite`` 要先把它
        # 转成 double，转不动就抛。它本来就不是能拿来算的数值，按"不是"处理——
        # 抛出去的话，调用方那句 ``if not is_finite_number(...)`` 的兜底反而看不见，
        # 报错的是奖励文案或冷却计算这些更远的地方。
        return False
