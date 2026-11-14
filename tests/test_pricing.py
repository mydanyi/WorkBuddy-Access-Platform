"""倍率表：合并两份文档、保鲜期、失败退避，以及"读不到就退回裸 ID"。

这条链路是**显示层**的数据，所以这里的重点全在"坏掉的时候不许闹"：读失败沿用上一
份、没有登录态干脆不问、失败不进管理员通知。任何一条破了，运维会开始收到"模型倍率
读不到"这种跟聊天毫无关系的告警。

运行::

    python -m unittest tests.test_pricing -v
"""

from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path

_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.pricing import (  # noqa: E402
    ENTERPRISE_MODELS_PATH,
    IDE_HEADERS,
    V3_CONFIG_PATH,
    ModelPricing,
    build_price_map,
)


class _FakeClient:
    """按路径回答的客户端；``credentials`` 就是那份内存凭据。"""

    def __init__(self, responses=None, *, creds=None) -> None:
        self.responses = dict(responses or {})
        self.calls = []
        self.credentials = {"accessToken": "tok"} if creds is None else creds

    async def request_json(self, method, path, *, headers=None, observe=True, **kwargs):
        self.calls.append(
            {"method": method, "path": path, "headers": dict(headers or {}),
             "observe": observe}
        )
        value = self.responses.get(path, {})
        if isinstance(value, BaseException):
            raise value
        return value

    def paths(self):
        return [call["path"] for call in self.calls]


# ``UpstreamClient._parse`` 已经把信封拆掉了：真实形状就是 ``data`` 那一层，
# 不是 ``{"code": 0, "data": {...}}``。替身必须照这个形状写——照完整信封写的话，
# 线上空表、用例全绿，上一版就是这么翻车的。
def _enterprise(*items):
    return {"models": list(items)}


def _v3(*items, promotions=None):
    data = {"models": list(items)}
    if promotions is not None:
        data["modelPromotions"] = promotions
    return data


class BuildPriceMapTests(unittest.TestCase):
    def test_the_enterprise_document_wins_over_v3(self):
        price = build_price_map(
            [{"id": "m", "credits": "x0.79 credits"}],
            [{"id": "m", "credits": "x0.11"}],
        )
        self.assertEqual(price["m"], 0.79)

    def test_v3_fills_in_what_the_enterprise_document_leaves_empty(self):
        price = build_price_map(
            [{"id": "m", "credits": None}, {"id": "n", "credits": "x0.5"}],
            [{"id": "m", "credits": "x0.19"}],
        )
        self.assertEqual(price["m"], 0.19)
        self.assertEqual(price["n"], 0.5)

    def test_an_id_without_any_price_stays_priceless(self):
        price = build_price_map([{"id": "m"}], [{"id": "other", "credits": "x1"}])
        self.assertIsNone(price["m"])

    def test_junk_entries_are_ignored(self):
        price = build_price_map([None, "x", {"id": ""}, {"id": 3}], None)
        self.assertEqual(price, {})


class EntriesTests(unittest.TestCase):
    def test_it_reads_the_shape_the_client_actually_returns(self):
        from astrbot_plugin_workbuddy.core.pricing import _entries

        # 线上真实形状：``_parse`` 只回 ``data`` 那一层。
        self.assertEqual(_entries({"models": [{"id": "m"}]}), [{"id": "m"}])
        # 裸数组与再嵌一层也照样认，少猜一层总比线上空表强。
        self.assertEqual(_entries([{"id": "m"}]), [{"id": "m"}])
        self.assertEqual(_entries({"data": {"models": [{"id": "m"}]}}), [{"id": "m"}])
        self.assertEqual(_entries({"code": 0}), [])
        self.assertEqual(_entries(None), [])


class ModelPricingTests(unittest.IsolatedAsyncioTestCase):
    def _pricing(self, responses, *, creds=None, ttl=300.0):
        self.now = 1000.0
        client = _FakeClient(responses, creds=creds)
        pricing = ModelPricing(client, ttl=ttl, clock=lambda: self.now)
        return client, pricing

    async def test_a_reading_carries_the_multiplier_and_never_notifies(self):
        client, pricing = self._pricing(
            {
                ENTERPRISE_MODELS_PATH: _enterprise(
                    {"id": "glm-5.2", "credits": "x0.79 credits"},
                    {"id": "minimax-m3-pay"},
                ),
                V3_CONFIG_PATH: _v3(),
            }
        )
        details = await pricing.ensure()
        self.assertEqual(details["glm-5.2"]["effective_credits"], "0.79x")
        self.assertEqual(details["glm-5.2"]["credits"], 0.79)
        self.assertNotIn("minimax-m3-pay", details, "没价就不进表，显示时退回裸 ID")
        self.assertTrue(all(call["observe"] is False for call in client.calls),
                        "读倍率失败不该惊动管理员")
        self.assertEqual(pricing.detail_for("glm-5.2")["id"], "glm-5.2")

    async def test_v3_is_asked_with_the_ide_headers(self):
        client, pricing = self._pricing(
            {
                ENTERPRISE_MODELS_PATH: _enterprise({"id": "m", "credits": "x1"}),
                V3_CONFIG_PATH: _v3(),
            }
        )
        await pricing.ensure()
        v3_calls = [c for c in client.calls if c["path"] == V3_CONFIG_PATH]
        self.assertEqual(len(v3_calls), 1)
        self.assertEqual(v3_calls[0]["headers"], IDE_HEADERS)

    async def test_the_result_is_cached_for_the_ttl(self):
        client, pricing = self._pricing(
            {
                ENTERPRISE_MODELS_PATH: _enterprise({"id": "m", "credits": "x1"}),
                V3_CONFIG_PATH: _v3(),
            }
        )
        await pricing.ensure()
        await pricing.ensure()
        self.assertEqual(client.paths().count(ENTERPRISE_MODELS_PATH), 1)
        self.now += 301.0
        await pricing.ensure()
        self.assertEqual(client.paths().count(ENTERPRISE_MODELS_PATH), 2)

    async def test_refresh_ignores_the_cache(self):
        client, pricing = self._pricing(
            {
                ENTERPRISE_MODELS_PATH: _enterprise({"id": "m", "credits": "x1"}),
                V3_CONFIG_PATH: _v3(),
            }
        )
        await pricing.ensure()
        await pricing.refresh()
        self.assertEqual(client.paths().count(ENTERPRISE_MODELS_PATH), 2)

    async def test_without_credentials_no_request_is_made(self):
        client, pricing = self._pricing({}, creds={})
        details = await pricing.ensure()
        self.assertEqual(details, {})
        self.assertEqual(client.calls, [], "没有登录态就别去讨一个注定 401 的请求")

    async def test_a_failed_read_keeps_the_previous_table(self):
        client, pricing = self._pricing(
            {
                ENTERPRISE_MODELS_PATH: _enterprise({"id": "m", "credits": "x1"}),
                V3_CONFIG_PATH: _v3(),
            }
        )
        await pricing.ensure()
        client.responses[ENTERPRISE_MODELS_PATH] = RuntimeError("boom")
        self.now += 400.0
        details = await pricing.ensure()
        self.assertEqual(
            details["m"]["effective_credits"], "1x", "读失败沿用上一份，不是清空"
        )

    async def test_a_failure_backs_off_instead_of_hammering(self):
        client, pricing = self._pricing(
            {ENTERPRISE_MODELS_PATH: RuntimeError("boom")}
        )
        await pricing.ensure()
        self.assertEqual(client.paths().count(ENTERPRISE_MODELS_PATH), 1)
        # 退避还没到点：一次都不许再打。
        self.now += 10.0
        await pricing.ensure()
        self.assertEqual(client.paths().count(ENTERPRISE_MODELS_PATH), 1)
        # 第一次退避是 30 秒，到点才重试；紧接着又失败，窗口翻倍成 60 秒。
        self.now += 25.0
        await pricing.ensure()
        self.assertEqual(client.paths().count(ENTERPRISE_MODELS_PATH), 2)
        self.now += 30.0
        await pricing.ensure()
        self.assertEqual(
            client.paths().count(ENTERPRISE_MODELS_PATH),
            2,
            "连败的窗口要变长，不能每 30 秒准点再打一次",
        )

    async def test_a_broken_v3_still_leaves_the_enterprise_prices(self):
        client, pricing = self._pricing(
            {
                ENTERPRISE_MODELS_PATH: _enterprise({"id": "m", "credits": "x0.5"}),
                V3_CONFIG_PATH: RuntimeError("boom"),
            }
        )
        details = await pricing.ensure()
        self.assertEqual(details["m"]["effective_credits"], "0.5x")

    async def test_an_active_promotion_discounts_the_shown_multiplier(self):
        promo = {
            "enabled": True,
            "modelIds": ["m"],
            "priority": 1,
            "badge": {"label": "错峰使用"},
            "hover": {"textZh": "非高峰期五折"},
            "discount": {"factor": 0.5},
        }
        client, pricing = self._pricing(
            {
                ENTERPRISE_MODELS_PATH: _enterprise({"id": "m", "credits": "x0.51"}),
                V3_CONFIG_PATH: _v3(promotions=[promo]),
            }
        )
        details = await pricing.ensure()
        self.assertEqual(details["m"]["effective_credits"], "0.255x")
        self.assertEqual(details["m"]["promo_label"], "错峰使用")
        self.assertEqual(details["m"]["promo_note"], "非高峰期五折")

    async def test_cancellation_is_never_swallowed(self):
        client, pricing = self._pricing(
            {ENTERPRISE_MODELS_PATH: asyncio.CancelledError()}
        )
        with self.assertRaises(asyncio.CancelledError):
            await pricing.ensure()


if __name__ == "__main__":
    unittest.main()
