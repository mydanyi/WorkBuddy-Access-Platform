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
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )
