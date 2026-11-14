"""``is_finite_number`` 的边界：类型对、也真存在，但**算不了**的那些值。

这个判断是"这个数能不能拿来用"的唯一入口——奖励文案、冷却时长、路由截止时间都
先过它。它自己抛异常，兜底就形同虚设：报错的会是更远的调用点，而且报的是一句跟
真实原因无关的话。

运行::

    python -m unittest tests.test_numbers -v
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.numbers import is_finite_number  # noqa: E402


class FiniteNumberTests(unittest.TestCase):
    def test_ordinary_numbers_pass(self):
        self.assertTrue(is_finite_number(0))
        self.assertTrue(is_finite_number(-3))
        self.assertTrue(is_finite_number(12.5))

    def test_non_numbers_and_bools_do_not(self):
        # ``bool`` 是 ``int`` 的子类：不单列出来的话 ``True`` 会被当成 1 混过去。
        self.assertFalse(is_finite_number(True))
        self.assertFalse(is_finite_number(False))
        self.assertFalse(is_finite_number("12"))
        self.assertFalse(is_finite_number(None))

    def test_infinities_and_nan_do_not(self):
        self.assertFalse(is_finite_number(float("inf")))
        self.assertFalse(is_finite_number(float("nan")))

    def test_an_integer_too_big_for_a_double_is_rejected_not_raised(self):
        # ``10**400`` 类型对、也不是 ``bool``，但 ``math.isfinite`` 要先把整数转成
        # double，转不动就抛 ``OverflowError``。JSON 里一个超长整数就能造出它——
        # 签到结果带这种奖励时，抛的话 ``_format_reward`` 会在发消息前倒下，而不是
        # 退回它自己的兜底文案。
        self.assertFalse(is_finite_number(10**400))
        self.assertFalse(is_finite_number(-(10**400)))


if __name__ == "__main__":
    unittest.main(verbosity=2)
