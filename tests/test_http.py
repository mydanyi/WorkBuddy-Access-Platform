"""签到这条链的 HTTP 契约（真的上游、真的客户端、假的凭据来源）。

聊天已经改走 ACP，``UpstreamClient`` 只剩签到那两个请求。所以这里只验还能发生的
事：信封错误、403 不刷新、429 的 ``Retry-After``、401 恰好一次刷新加一次重发、以及
签到状态机在真实 HTTP 往返下的四种结局。旧版里针对 OAuth 登录、模型目录、余额聚合
的用例随那些代码一起删掉了——留着它们只会测试不存在的东西。

上游是一个真的 ``aiohttp`` 服务端（127.0.0.1:0），不是 mock：要验的正是"请求真的
发出去、带的头对不对、失败之后到底重发了几次"。

凭据来源是假的（``_FakeManager``）：真实来源 ``CliCredentials`` 有自己的 22 项测试，
这里只关心客户端对它的使用方式。
"""

import asyncio
import shutil
import sys
import unittest
from pathlib import Path

import aiohttp
from aiohttp import web

_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.client import (  # noqa: E402
    CHECKIN_CLAIM_PATH,
    CHECKIN_STATUS_PATH,
    UpstreamClient,
)
from astrbot_plugin_workbuddy.core.errors import UpstreamError  # noqa: E402
from astrbot_plugin_workbuddy.core.http_utils import (  # noqa: E402
    common_headers,
    parse_retry_after,
)
from astrbot_plugin_workbuddy.core.signin import SigninService  # noqa: E402

try:  # 包内发现：python -m unittest tests.test_http
    from tests import support
except ImportError:  # 顶层发现：python -m unittest discover -s tests
    import support  # type: ignore

AT_OLD = "FICTIONAL-OLD-ACCESS-TOKEN"
AT_NEW = "FICTIONAL-NEW-ACCESS-TOKEN"

REFRESH_PATH = "/v2/plugin/auth/token/refresh"


class _FakeManager:
    """只做两件事的凭据来源：读当前 token、叫一次刷新。"""

    def __init__(self, token=AT_OLD, *, new_token=AT_NEW, fail=False) -> None:
        self._token = token
        self._new_token = new_token
        self._fail = fail
        self.refresh_calls = []

    @property
    def credentials(self) -> dict:
        if not self._token:
            return {}
        return {"accessToken": self._token, "realm": "cn"}

    async def refresh(self, stale_token=None) -> dict:
        self.refresh_calls.append(stale_token)
        if self._fail:
            raise UpstreamError(401, None)
        self._token = self._new_token
        return self.credentials


class _ClientCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.data_dir = support.make_temp_dir("wb-http-")
        self.up = support.FakeUpstream()
        await self.up.start()
        self.session = aiohttp.ClientSession()

    async def asyncTearDown(self) -> None:
        await self.session.close()
        await self.up.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def _client(self, manager=None, **kwargs) -> UpstreamClient:
        return UpstreamClient(
            self.session,
            manager or _FakeManager(),
            billing_base=kwargs.pop("billing_base", self.up.base),
            **kwargs,
        )

    def _protected(self, tag):
        async def handler(request):
            if request.headers.get("Authorization") == "Bearer " + AT_NEW:
                return support.ok({"ok": tag})
            return support.biz(1001, "token expired", status=401)

        return handler


class ClientEnvelopeTests(_ClientCase):
    async def test_http200_business_code_nonzero_is_error_without_body_leak(self):
        secret = "UPSTREAM-SECRET-BODY-xyz"

        async def handler(request):
            return support.biz(123, secret)

        self.up.on("GET", "/v2/billing/probe", handler)
        client = self._client()

        with self.assertRaises(UpstreamError) as ctx:
            await client.request_json("GET", "/v2/billing/probe")

        self.assertEqual(ctx.exception.status, 200)
        self.assertEqual(ctx.exception.code, 123)
        text = "{}\n{}".format(ctx.exception, repr(ctx.exception))
        self.assertNotIn(secret, text)

    async def test_403_is_error_and_does_not_refresh(self):
        manager = _FakeManager()

        async def handler(request):
            return support.biz(4030, "forbidden", status=403)

        self.up.on("GET", "/v2/billing/probe", handler)
        client = self._client(manager)

        with self.assertRaises(UpstreamError) as ctx:
            await client.request_json("GET", "/v2/billing/probe")

        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(manager.refresh_calls, [])
        self.assertEqual(self.up.hits[("GET", "/v2/billing/probe")], 1)

    async def test_429_parses_retry_after_without_refresh(self):
        manager = _FakeManager()

        async def handler(request):
            return web.json_response(
                {"code": 4290, "msg": "slow down"},
                status=429,
                headers={"Retry-After": "7"},
            )

        self.up.on("GET", "/v2/billing/probe", handler)
        client = self._client(manager)

        with self.assertRaises(UpstreamError) as ctx:
            await client.request_json("GET", "/v2/billing/probe")

        self.assertEqual(ctx.exception.status, 429)
        self.assertEqual(float(ctx.exception.retry_after), 7.0)
        self.assertEqual(manager.refresh_calls, [])

    async def test_401_refreshes_once_then_resends_exactly_once(self):
        manager = _FakeManager()
        self.up.on("GET", "/v2/billing/probe", self._protected("a"))
        client = self._client(manager)

        result = await client.request_json("GET", "/v2/billing/probe")

        self.assertEqual(result.get("ok"), "a")
        # 刷新只被问一次，且问的是"调用方用失败的那个 token"。
        self.assertEqual(manager.refresh_calls, [AT_OLD])
        self.assertEqual(self.up.hits[("GET", "/v2/billing/probe")], 2)

    async def test_two_concurrent_401s_each_refresh_once_and_each_resend_once(self):
        manager = _FakeManager()
        self.up.on("GET", "/v2/billing/a", self._protected("a"))
        self.up.on("GET", "/v2/billing/b", self._protected("b"))
        client = self._client(manager)

        results = await asyncio.gather(
            client.request_json("GET", "/v2/billing/a"),
            client.request_json("GET", "/v2/billing/b"),
        )

        self.assertEqual([r.get("ok") for r in results], ["a", "b"])
        # 单飞（并发 401 只刷一次）是 ``CliCredentials`` 的职责，它有自己的测试；
        # 客户端这一层的契约是"每个失败请求只问一次、只重发一次"，绝不循环。
        self.assertEqual(len(manager.refresh_calls), 2)
        self.assertEqual(self.up.hits[("GET", "/v2/billing/a")], 2)
        self.assertEqual(self.up.hits[("GET", "/v2/billing/b")], 2)

    async def test_refresh_failure_does_not_loop(self):
        manager = _FakeManager(fail=True)

        async def handler(request):
            return support.biz(1001, "token expired", status=401)

        self.up.on("GET", "/v2/billing/probe", handler)
        client = self._client(manager)

        with self.assertRaises(UpstreamError) as ctx:
            await client.request_json("GET", "/v2/billing/probe")

        self.assertEqual(ctx.exception.status, 401)
        self.assertEqual(len(manager.refresh_calls), 1)
        self.assertEqual(self.up.hits[("GET", "/v2/billing/probe")], 1)

    async def test_unauthenticated_call_never_asks_for_a_refresh(self):
        manager = _FakeManager(token="")

        async def handler(request):
            return support.biz(1001, "token expired", status=401)

        self.up.on("GET", "/v2/billing/probe", handler)
        client = self._client(manager)

        with self.assertRaises(UpstreamError):
            await client.request_json("GET", "/v2/billing/probe", auth=False)

        self.assertEqual(manager.refresh_calls, [])


class SigninHttpTests(_ClientCase):
    async def test_already_signed_skips_claim(self):
        async def status_handler(request):
            return support.ok({"active": True, "today_checked_in": True, "streak_days": 5})

        self.up.on("POST", CHECKIN_STATUS_PATH, status_handler)

        result = await SigninService(self._client()).run()

        self.assertEqual(result.get("outcome"), "already_signed")
        self.assertTrue(result.get("verified"))
        status_calls = self.up.calls_for("POST", CHECKIN_STATUS_PATH)
        self.assertEqual(len(status_calls), 1)
        self.assertEqual(self.up.json_body(status_calls[0]), {})
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 0)

    async def test_not_signed_claims_then_rechecks_status(self):
        statuses = [
            {"active": True, "today_checked_in": False},
            {"active": True, "today_checked_in": True, "streak_days": 6},
        ]
        counter = {"n": 0}

        async def status_handler(request):
            index = min(counter["n"], len(statuses) - 1)
            counter["n"] += 1
            return support.ok(dict(statuses[index]))

        async def claim_handler(request):
            return support.ok({"credit": 10})

        self.up.on("POST", CHECKIN_STATUS_PATH, status_handler)
        self.up.on("POST", CHECKIN_CLAIM_PATH, claim_handler)

        result = await SigninService(self._client()).run()

        self.assertEqual(result.get("outcome"), "signed")
        self.assertTrue(result.get("verified"))
        self.assertEqual(result.get("reward_credits"), 10)
        self.assertEqual(self.up.hits[("POST", CHECKIN_STATUS_PATH)], 2)
        claim_calls = self.up.calls_for("POST", CHECKIN_CLAIM_PATH)
        self.assertEqual(len(claim_calls), 1)
        self.assertEqual(self.up.json_body(claim_calls[0]), {})

    async def test_claim_network_interruption_rechecks_without_duplicate(self):
        statuses = [
            {"active": True, "today_checked_in": False},
            {"active": True, "today_checked_in": True, "streak_days": 6},
        ]
        counter = {"n": 0}

        async def status_handler(request):
            index = min(counter["n"], len(statuses) - 1)
            counter["n"] += 1
            return support.ok(dict(statuses[index]))

        async def claim_handler(request):
            return web.json_response(
                {"code": 9001, "msg": "upstream down"}, status=503
            )

        self.up.on("POST", CHECKIN_STATUS_PATH, status_handler)
        self.up.on("POST", CHECKIN_CLAIM_PATH, claim_handler)

        result = await SigninService(self._client()).run()

        self.assertTrue(result.get("verified"))
        self.assertIn(result.get("outcome"), ("signed", "already_signed"))
        self.assertEqual(self.up.hits[("POST", CHECKIN_STATUS_PATH)], 2)
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 1)

    async def test_claim_business_error_is_unconfirmed_not_success(self):
        async def status_handler(request):
            return support.ok({"active": True, "today_checked_in": False})

        async def claim_handler(request):
            return support.biz(10000, "system busy")

        self.up.on("POST", CHECKIN_STATUS_PATH, status_handler)
        self.up.on("POST", CHECKIN_CLAIM_PATH, claim_handler)

        result = await SigninService(self._client()).run()

        self.assertEqual(result.get("outcome"), "unconfirmed")
        self.assertFalse(result.get("verified"))
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 1)
        self.assertGreaterEqual(self.up.hits[("POST", CHECKIN_STATUS_PATH)], 1)


class CommonHeaderTests(unittest.TestCase):
    @staticmethod
    def _lower(headers):
        """aiohttp 会把头名按小写发出去，断言也统一按小写比，免得靠大小写巧合。"""
        return {name.lower(): value for name, value in headers.items()}

    def test_common_headers_carry_the_documented_shape(self):
        headers = self._lower(common_headers("cn", token=AT_OLD))
        self.assertEqual(headers.get("authorization"), "Bearer " + AT_OLD)
        self.assertEqual(headers.get("origin"), "https://www.codebuddy.cn")
        self.assertEqual(headers.get("referer"), "https://www.codebuddy.cn/")
        self.assertEqual(headers.get("accept-language"), "zh-CN")
        self.assertEqual(headers.get("x-codebuddy-request"), "1")
        self.assertIn("WorkBuddy", headers.get("user-agent", ""))
        # 长期凭据绝不能出现在普通请求上。
        self.assertNotIn("x-refresh-token", headers)

    def test_global_realm_switches_origin_and_language(self):
        headers = self._lower(common_headers("global"))
        self.assertEqual(headers.get("origin"), "https://www.workbuddy.ai")
        self.assertEqual(headers.get("accept-language"), "en-US")
        self.assertNotIn("authorization", headers)


class RetryAfterTests(unittest.TestCase):
    class _Resp:
        def __init__(self, value):
            self.headers = {} if value is None else {"Retry-After": value}

    def test_seconds_and_dates_are_parsed_and_garbage_is_none(self):
        self.assertEqual(parse_retry_after(self._Resp("12")), 12.0)
        self.assertEqual(parse_retry_after(self._Resp("-3")), 0.0)
        self.assertIsNone(parse_retry_after(self._Resp(None)))
        self.assertIsNone(parse_retry_after(self._Resp("not-a-date")))
        self.assertGreater(parse_retry_after(self._Resp("Wed, 21 Oct 2099 07:28:00 GMT")), 0)
        # ``inf`` / ``nan`` 也归"认不出来"。``float("Infinity")`` 不报错，放过去的话
        # 冷却时长就成了无穷大：``_block_reason`` 从此永远返回"冷却中"，``resume()``
        # 只清暂停标记、清不掉它，这个值还会以 ``Infinity`` 的形式落进状态文件。
        # ``nan`` 反过来——跟任何数比较都是 False，冷却等于零。
        self.assertIsNone(parse_retry_after(self._Resp("Infinity")))
        self.assertIsNone(parse_retry_after(self._Resp("-Infinity")))
        self.assertIsNone(parse_retry_after(self._Resp("nan")))


if __name__ == "__main__":
    unittest.main()
