"""倍率与促销的纯函数：解析、拼显示名、拼回去还能剥干净。

拼（:func:`core.credits.display_model_id`）与剥（:func:`core.models.strip_model_alias`）
分住在两个模块里，但它们必须严格互逆——拼出来的东西剥不回去，就会变成
"列表里能选、发出去 400"。这里那条 round-trip 用例就是盯这个的。

运行::

    python -m unittest tests.test_credits -v
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.credits import (  # noqa: E402
    display_model_id,
    format_credits,
    parse_credits,
    promotion_discount,
    promotion_label,
    promotion_note,
    select_promotion,
)
from astrbot_plugin_workbuddy.core.models import strip_model_alias  # noqa: E402

_NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)


class ParseCreditsTests(unittest.TestCase):
    def test_the_upstream_string_shapes_are_all_understood(self):
        self.assertEqual(parse_credits("x0.79 credits"), 0.79)
        self.assertEqual(parse_credits("x0.11"), 0.11)
        self.assertEqual(parse_credits("0.50x"), 0.5)
        self.assertEqual(parse_credits("2"), 2.0)

    def test_numbers_pass_and_negative_numbers_do_not(self):
        self.assertEqual(parse_credits(0.79), 0.79)
        self.assertEqual(parse_credits(0), 0, "0 是合法倍率（免费），不是缺失")
        self.assertIsNone(parse_credits(-1))
        self.assertIsNone(parse_credits(-0.5))

    def test_bools_nan_and_prose_are_not_multipliers(self):
        # ``bool`` 是 ``int`` 的子类；不挡的话 ``True`` 会变成 1 倍。
        self.assertIsNone(parse_credits(True))
        self.assertIsNone(parse_credits(False))
        self.assertIsNone(parse_credits(float("nan")))
        self.assertIsNone(parse_credits(float("inf")))
        self.assertIsNone(parse_credits("按量计费"))
        self.assertIsNone(parse_credits(""))
        self.assertIsNone(parse_credits(None))


class FormatCreditsTests(unittest.TestCase):
    def test_trailing_zeros_are_stripped(self):
        self.assertEqual(format_credits(0.5), "0.5x")
        self.assertEqual(format_credits(0.50), "0.5x")
        self.assertEqual(format_credits(0.79), "0.79x")
        self.assertEqual(format_credits(1), "1x")
        self.assertEqual(format_credits(0), "0x")

    def test_unusable_values_have_no_string_form(self):
        self.assertIsNone(format_credits(-1))
        self.assertIsNone(format_credits(True))
        self.assertIsNone(format_credits(None))
        self.assertIsNone(format_credits(float("nan")))


class DisplayModelIdTests(unittest.TestCase):
    def test_a_priced_model_gets_the_standard_suffix(self):
        self.assertEqual(
            display_model_id({"id": "glm-5.2", "effective_credits": "0.79x"}),
            "glm-5.2 [0.79x]",
        )

    def test_an_active_promotion_adds_its_badge(self):
        self.assertEqual(
            display_model_id(
                {
                    "id": "deepseek-v4-pro",
                    "effective_credits": "0.255x",
                    "promo_label": "错峰使用",
                }
            ),
            "deepseek-v4-pro [0.255x · 错峰使用]",
        )

    def test_an_unpriced_model_stays_bare(self):
        # 没报价就是没报价，不写"倍率未知"——那会把一句"上游没给价"说成
        # "价格读失败了"。
        self.assertEqual(display_model_id({"id": "minimax-m3-pay"}), "minimax-m3-pay")
        self.assertEqual(display_model_id({"id": "x", "effective_credits": None}), "x")
        self.assertEqual(display_model_id({"id": "x", "effective_credits": "免费"}), "x")

    def test_a_badge_can_never_break_the_suffix_shape(self):
        # 徽章里带方括号或换行会把后缀切成两截，剥的时候剥不干净。
        self.assertEqual(
            display_model_id(
                {
                    "id": "m",
                    "effective_credits": "0.5x",
                    "promo_label": "[夜间]\n折扣",
                }
            ),
            "m [0.5x · 夜间 折扣]",
        )

    def test_the_suffix_always_survives_a_round_trip(self):
        cases = [
            {"id": "wb-1", "effective_credits": "0.11x"},
            {"id": "wb-2", "effective_credits": "1x", "promo_label": "夜间折扣"},
            {"id": "wb-3", "effective_credits": "0x"},
            {"id": "wb-4"},
        ]
        for detail in cases:
            shown = display_model_id(detail)
            self.assertEqual(strip_model_alias(shown), detail["id"], shown)


class SelectPromotionTests(unittest.TestCase):
    def _promo(self, **overrides):
        promo = {
            "enabled": True,
            "modelIds": ["m"],
            "priority": 0,
            "badge": {"label": "夜间折扣"},
            "hover": {"textZh": "23:00-06:00 五折"},
            "discount": {"factor": 0.5},
        }
        promo.update(overrides)
        return promo

    def test_a_disabled_promotion_never_applies(self):
        self.assertIsNone(select_promotion([self._promo(enabled=False)], "m", _NOW))

    def test_it_must_name_the_model(self):
        self.assertIsNone(
            select_promotion([self._promo(modelIds=["other"])], "m", _NOW)
        )

    def test_the_highest_priority_wins(self):
        low = self._promo(priority=1, badge={"label": "低"})
        high = self._promo(priority=9, badge={"label": "高"})
        chosen = select_promotion([low, high], "m", _NOW)
        self.assertEqual(promotion_label(chosen), "高")

    def test_a_daily_window_gates_the_promotion(self):
        promo = self._promo(schedule={"daily": [{"start": "23:00", "end": "06:00"}]})
        self.assertIsNone(select_promotion([promo], "m", _NOW), "中午不在窗口里")
        midnight = _NOW.replace(hour=23, minute=30)
        self.assertIsNotNone(select_promotion([promo], "m", midnight))

    def test_a_cross_midnight_window_covers_the_early_hours(self):
        promo = self._promo(schedule={"daily": [{"start": "23:00", "end": "06:00"}]})
        early = _NOW.replace(hour=2, minute=0)
        self.assertIsNotNone(select_promotion([promo], "m", early))

    def test_a_malformed_schedule_is_inactive_not_always_on(self):
        for broken in ("broken", False, 123, {"daily": "all"}):
            promo = self._promo(schedule=broken)
            self.assertIsNone(select_promotion([promo], "m", _NOW), repr(broken))

    def test_valid_from_and_until_are_respected(self):
        promo = self._promo(
            schedule={
                "validFrom": (_NOW - timedelta(days=1)).isoformat(),
                "validUntil": (_NOW + timedelta(days=1)).isoformat(),
            }
        )
        self.assertIsNotNone(select_promotion([promo], "m", _NOW))
        promo = self._promo(
            schedule={"validUntil": (_NOW - timedelta(minutes=1)).isoformat()}
        )
        self.assertIsNone(select_promotion([promo], "m", _NOW))

    def test_the_notes_come_from_the_hover_text(self):
        promo = self._promo()
        self.assertEqual(promotion_label(promo), "夜间折扣")
        self.assertEqual(promotion_note(promo), "23:00-06:00 五折")


class PromotionDiscountTests(unittest.TestCase):
    def test_a_fixed_discount_wins_over_the_factor(self):
        promo = {"discount": {"factor": 0.9, "discountedCredits": "x0.2"}}
        self.assertEqual(promotion_discount(promo, 1.0), 0.2)

    def test_the_factor_multiplies_the_base(self):
        self.assertEqual(promotion_discount({"discount": {"factor": 0.5}}, 0.79), 0.395)

    def test_a_zero_factor_is_free_not_missing(self):
        self.assertEqual(promotion_discount({"discount": {"factor": 0}}, None), 0)

    def test_a_missing_discount_keeps_the_base_price(self):
        self.assertIsNone(promotion_discount({}, 0.79))
        self.assertIsNone(promotion_discount({"discount": {}}, 0.79))
        self.assertIsNone(promotion_discount({"discount": {"factor": -1}}, 0.79))

    def test_a_bare_hover_number_is_never_a_discount(self):
        promo = {"hover": {"textZh": "五折"}, "discount": {"factor": "0.5"}}
        self.assertIsNone(promotion_discount(promo, 1.0))


if __name__ == "__main__":
    unittest.main()
