"""本地网关的对外契约：模型清单、虚拟入口、失败不拼接、请求体上限。

分工：``core/routing.py`` 的决策细节（冷却、作用域、备用顺序）由 ``test_routing.py``
在单元层覆盖；这里只验**网关这一层**能不能把路由的结果如实交出去——清单里有没有虚拟
入口、一次虚拟请求到底问了几次后端、流断在中途会不会拼两个模型、请求体超限会不会
去碰下游。

聊天后端是脚本化的（``support.ScriptedChat``），因为它扮演的是一个"已经按契约出错"
的后端；ACP 适配层自己的错误映射在 ``test_acp_backend`` 里用假 CLI 验。
"""

import contextlib
import json
import sys
import unittest
from pathlib import Path

import aiohttp

_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.config import ConfigError, load_settings  # noqa: E402
from astrbot_plugin_workbuddy.core.errors import (  # noqa: E402
    ChatStreamError,
    UpstreamError,
)
from astrbot_plugin_workbuddy.core.gateway import (  # noqa: E402
    COMPLETIONS_PATH,
    MODELS_PATH,
    Gateway,
)
from astrbot_plugin_workbuddy.core.routing import (  # noqa: E402
    ORDERED_MODEL_ID,
    ModelRouter,
)
from astrbot_plugin_workbuddy.core.storage import StateStore  # noqa: E402

try:  # 包内发现
    from tests import support
except ImportError:  # 顶层发现
    import support  # type: ignore

_UNSET = object()
_MIB = 1024 * 1024

MODELS = ("wb-alpha", "wb-beta", "wb-gamma")


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

def _routing_section(models, **overrides) -> dict:
    """The validated ``routing`` settings group for an enabled route."""
    raw = {"enabled": True, "models": list(models)}
    raw.update(overrides)
    settings = load_settings({"routing": raw})
    return settings["routing"]


async def _read_sse(resp):
    """Collect the gateway's SSE frames: ``(events, saw_done, raw_text)``."""
    events = []
    done = False
    raw_lines = []
    async for raw in resp.content:
        line = raw.decode("utf-8", "replace").rstrip("\r\n")
        raw_lines.append(line)
        if not line or line.startswith(":"):
            continue
        if line.startswith("data:"):
            payload = line[5:].lstrip(" ")
            if payload == "[DONE]":
                done = True
                continue
            events.append(json.loads(payload))
    return events, done, "\n".join(raw_lines)


def _merged_content(events) -> str:
    """Join every streamed ``delta.content`` fragment (gateway side only)."""
    parts = []
    for event in events:
        if not isinstance(event, dict):
            continue
        for choice in event.get("choices") or []:
            if not isinstance(choice, dict):
                continue
            delta = choice.get("delta") or {}
            if isinstance(delta, dict) and delta.get("content"):
                parts.append(delta["content"])
    return "".join(parts)


def _body_of_exact_bytes(total, *, model=None, prefix="") -> bytes:
    """A valid chat JSON body whose UTF-8 encoding is exactly ``total`` bytes.

    ``model`` 默认取清单里的第一个真模型：请求体会先过"未知模型 → 固定 400"
    这道闸，用清单外的 id 会让请求体上限的用例全部变成 400。
    """
    payload = {
        "model": MODELS[0] if model is None else model,
        "messages": [{"role": "user", "content": prefix}],
        "stream": False,
    }
    head = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    pad = total - head
    if pad < 0:
        raise AssertionError("total is too small for the JSON envelope")
    payload["messages"][0]["content"] = prefix + "a" * pad
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    if len(body) != total:  # paranoia: the size arithmetic must be exact
        raise AssertionError("body size helper produced {} bytes".format(len(body)))
    return body


async def _iter_slices(body, size=65536):
    """Yield ``body`` in slices so aiohttp must use chunked encoding."""
    import asyncio

    for start in range(0, len(body), size):
        yield body[start:start + size]
        await asyncio.sleep(0)


# --------------------------------------------------------------------------- #
# shared fixture
# --------------------------------------------------------------------------- #

class _GatewayCase(unittest.IsolatedAsyncioTestCase):
    """Real gateway + real router in front of a scripted chat backend."""

    async def asyncSetUp(self) -> None:
        self.data_dir = support.make_temp_dir("wb-rtgw-")
        self.session = aiohttp.ClientSession()
        self.chat = support.ScriptedChat(on_error=self._observe)
        self.catalog = support.StubCatalog(MODELS)
        self.state = StateStore(self.data_dir)
        self.router = ModelRouter(_routing_section(MODELS), self.state)
        self.gateway = None
        await self.start_gateway()

    async def asyncTearDown(self) -> None:
        if self.gateway is not None:
            with contextlib.suppress(Exception):
                await self.gateway.stop()
        await self.session.close()
        import shutil

        shutil.rmtree(self.data_dir, ignore_errors=True)

    # -- fixtures --------------------------------------------------------

    def _observe(self, category) -> None:
        self.observed.append(category)

    async def start_gateway(self, *, router=_UNSET, catalog=_UNSET, **overrides) -> None:
        params = {
            "api_key": support.API_KEY,
            "host": "127.0.0.1",
            "port": 0,
            "concurrency": 4,
            "rpm": 600,
            "queue_size": 8,
            "queue_timeout": 5,
        }
        params.update(overrides)
        if router is _UNSET:
            router = self.router
        if catalog is _UNSET:
            catalog = self.catalog
        self.gateway = Gateway(self.chat, router=router, catalog=catalog, **params)
        await self.gateway.start()
        self.base_url = "http://127.0.0.1:{}".format(self.gateway.bound_port)

    async def restart_gateway(self, **overrides) -> None:
        self.observed = []
        await self.gateway.stop()
        await self.start_gateway(**overrides)

    def headers(self, key=support.API_KEY) -> dict:
        return {"Authorization": "Bearer " + key} if key else {}

    def chat_payload(self, *, model=None, stream=False, **extra) -> dict:
        body = {
            "model": ORDERED_MODEL_ID if model is None else model,
            "messages": [{"role": "user", "content": "hi"}],
            "stream": stream,
        }
        body.update(extra)
        return body

    async def post_chat(self, payload):
        return await self.session.post(
            self.base_url + COMPLETIONS_PATH,
            json=payload,
            headers=self.headers(),
        )

    async def post_raw(self, body, *, key=support.API_KEY, chunked=False):
        head = self.headers(key)
        head["Content-Type"] = "application/json"
        if chunked:
            return await self.session.post(
                self.base_url + COMPLETIONS_PATH,
                data=_iter_slices(body),
                headers=head,
                chunked=True,
            )
        return await self.session.post(
            self.base_url + COMPLETIONS_PATH, data=body, headers=head
        )

    async def get_models(self):
        async with self.session.get(
            self.base_url + MODELS_PATH, headers=self.headers()
        ) as resp:
            self.assertEqual(resp.status, 200)
            return await resp.json()

    async def _setUp(self):  # pragma: no cover - IsolatedAsyncioTestCase hook
        self.observed = []
        await super()._setUp()


class RoutingThroughGatewayTests(_GatewayCase):
    """The virtual entry end to end: catalog, failover, no splicing, local 4xx."""

    def setUp(self) -> None:
        self.observed = []

    # -- catalog ---------------------------------------------------------

    async def test_models_listing_is_exactly_the_local_catalog_plus_the_virtual_entry(self):
        body = await self.get_models()
        data = body.get("data")
        self.assertIsInstance(data, list)
        ids = [item.get("id") for item in data if isinstance(item, dict)]
        self.assertEqual(ids, list(MODELS) + [ORDERED_MODEL_ID])
        for item in data:
            self.assertEqual(item.get("object"), "model")
            self.assertEqual(item.get("owned_by"), "workbuddy")
            self.assertEqual(item.get("created"), 0)
        self.assertEqual(ORDERED_MODEL_ID, "按顺序调用")

        await self.restart_gateway(router=None)
        body = await self.get_models()
        ids = [item.get("id") for item in body.get("data") or []]
        self.assertEqual(
            ids, list(MODELS), "a disabled route must not advertise the virtual model"
        )

    async def test_an_empty_catalog_serves_no_real_model_and_still_hides_no_request(self):
        await self.restart_gateway(catalog=support.StubCatalog([]))
        body = await self.get_models()
        ids = [item.get("id") for item in body.get("data") or []]
        self.assertEqual(ids, [ORDERED_MODEL_ID])

    # -- failover --------------------------------------------------------

    async def test_json_virtual_request_fails_over_and_keeps_the_payload_verbatim(self):
        self.chat.queue_error("wb-alpha", support.upstream_error(502))
        self.chat.queue_text("wb-beta", "beta answer")
        messages = [
            {"role": "system", "content": "persona"},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "看图"},
                    {
                        "type": "image_url",
                        "image_url": {"url": "data:image/png;base64,QUJD"},
                    },
                ],
            },
        ]
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "ping",
                    "description": "d",
                    "parameters": {"type": "object", "properties": {}},
                },
            }
        ]
        tool_choice = {"type": "function", "function": {"name": "ping"}}
        payload = self.chat_payload(
            messages=messages, tools=tools, tool_choice=tool_choice, temperature=0.25
        )

        resp = await self.post_chat(payload)
        try:
            self.assertEqual(resp.status, 200)
            body = await resp.json()
        finally:
            await resp.release()

        self.assertEqual(
            self.chat.sent_models,
            ["wb-alpha", "wb-beta"],
            "the backend may only see the pure candidate ids",
        )
        self.assertEqual(body.get("model"), "wb-beta", "the answer belongs to the standby")
        self.assertEqual(body["choices"][0]["message"]["content"], "beta answer")

        sent = self.chat.chat_bodies()
        self.assertEqual(len(sent), 2)
        expected_keys = {
            "model",
            "messages",
            "tools",
            "tool_choice",
            "temperature",
            "stream",
        }
        for index, call in enumerate(sent):
            self.assertEqual(
                set(call), expected_keys, "no extra metadata may be added for the backend"
            )
            self.assertEqual(call["messages"], messages)
            self.assertEqual(call["tools"], tools)
            self.assertEqual(call["tool_choice"], tool_choice)
            self.assertEqual(call["temperature"], 0.25)
            self.assertEqual(call["model"], self.chat.sent_models[index])
            self.assertNotIn(ORDERED_MODEL_ID, json.dumps(call, ensure_ascii=False))

    async def test_sse_virtual_request_fails_over_and_ends_with_done(self):
        self.chat.queue_error("wb-alpha", support.upstream_error(503))
        self.chat.queue_text("wb-beta", "beta stream")

        resp = await self.post_chat(self.chat_payload(stream=True))
        self.assertEqual(resp.status, 200)
        try:
            events, done, _raw = await _read_sse(resp)
        finally:
            await resp.release()

        self.assertTrue(done, "a successful route must end with data: [DONE]")
        self.assertEqual(_merged_content(events), "beta stream")
        self.assertNotIn(
            "wb-alpha",
            json.dumps(events, ensure_ascii=False),
            "the refused candidate may not appear in the client output",
        )
        self.assertEqual(self.chat.sent_models, ["wb-alpha", "wb-beta"])

    async def test_stream_break_after_a_partial_frame_never_splices_two_models(self):
        self.chat.queue_abort("wb-alpha", "partial from alpha")
        self.chat.queue_text("wb-beta", "beta answer")

        resp = await self.post_chat(self.chat_payload(stream=True))
        self.assertEqual(resp.status, 200)
        events, done = [], False
        try:
            with contextlib.suppress(aiohttp.ClientError):
                events, done, _raw = await _read_sse(resp)
        finally:
            await resp.release()

        self.assertEqual(
            self.chat.sent_models,
            ["wb-alpha"],
            "once a frame was emitted the stream belongs to that model",
        )
        self.assertFalse(done, "a broken stream must never get a fabricated [DONE]")
        text = json.dumps(events, ensure_ascii=False)
        self.assertNotIn("beta answer", text, "two models' output must not be spliced")
        self.assertTrue(
            any(isinstance(event, dict) and event.get("error") for event in events),
            events,
        )

    # -- a concrete model never borrows a standby ------------------------

    async def test_concrete_model_never_borrows_a_standby(self):
        self.chat.queue_error("wb-alpha", support.upstream_error(502))
        self.chat.queue_text("wb-beta", "beta answer")

        resp = await self.post_chat(self.chat_payload(model="wb-alpha"))
        try:
            self.assertEqual(resp.status, 502)
            await resp.read()
        finally:
            await resp.release()

        self.assertEqual(self.chat.sent_models, ["wb-alpha"])

    # -- local 4xx -------------------------------------------------------

    async def test_virtual_model_with_routing_disabled_is_a_fixed_local_4xx(self):
        await self.restart_gateway(router=None)
        resp = await self.post_chat(self.chat_payload())
        try:
            self.assertEqual(resp.status, 400)
            body = await resp.json()
        finally:
            await resp.release()
        self.assertEqual(body["error"]["type"], "invalid_request_error")
        self.assertIn(ORDERED_MODEL_ID, body["error"]["message"])
        self.assertEqual(self.chat.sent_models, [], "a local 400 never asks the backend")

    async def test_unknown_model_is_a_fixed_local_4xx(self):
        resp = await self.post_chat(self.chat_payload(model="wb-not-in-catalog"))
        try:
            self.assertEqual(resp.status, 400)
            body = await resp.json()
        finally:
            await resp.release()
        self.assertEqual(body["error"]["type"], "invalid_request_error")
        self.assertIn("wb-not-in-catalog", body["error"]["message"])
        self.assertIn("/v1/models", body["error"]["message"])
        self.assertEqual(self.chat.sent_models, [])

    async def test_an_oversized_model_id_is_rejected_without_echoing_it(self):
        # ``model`` 能顶满整个请求体上限（默认 16 MB），而这道检查排在限流之前：
        # 原样拼进错误文本就等于把网关当放大器；不掐长度它还会进 argv 和日志。
        huge = "wb-" + "x" * 4096
        resp = await self.post_chat(self.chat_payload(model=huge))
        try:
            self.assertEqual(resp.status, 400)
            body = await resp.json()
        finally:
            await resp.release()
        self.assertEqual(body["error"]["type"], "invalid_request_error")
        self.assertNotIn("x" * 64, body["error"]["message"], "错误文本不许回声请求体")
        self.assertEqual(self.chat.sent_models, [], "本地 400 不碰后端")

    async def test_an_empty_catalog_does_not_reject_anything(self):
        # 清单还没读成过（CLI 没装 / 装晚了一步）：此时拒绝一切请求只会把"还没准备好"
        # 变成"永远不能用"。
        await self.restart_gateway(catalog=support.StubCatalog([]))
        self.chat.queue_text("wb-anything", "ok")
        resp = await self.post_chat(self.chat_payload(model="wb-anything"))
        try:
            self.assertEqual(resp.status, 200)
            await resp.read()
        finally:
            await resp.release()
        self.assertEqual(self.chat.sent_models, ["wb-anything"])

    async def test_a_missing_catalog_skips_the_check_entirely(self):
        await self.restart_gateway(catalog=None)
        self.chat.queue_text("wb-anything", "ok")
        resp = await self.post_chat(self.chat_payload(model="wb-anything"))
        try:
            self.assertEqual(resp.status, 200)
            await resp.read()
        finally:
            await resp.release()

    # -- observer --------------------------------------------------------

    async def test_candidate_failures_are_not_announced_but_a_direct_failure_is(self):
        self.observed = []
        self.chat.queue_error("wb-alpha", support.upstream_error(502))
        self.chat.queue_text("wb-beta", "beta answer")

        resp = await self.post_chat(self.chat_payload())
        try:
            await resp.read()
        finally:
            await resp.release()
        self.assertEqual(
            self.observed, [], "a standby attempt that succeeded is not an incident"
        )

        self.observed = []
        self.chat.queue_error("wb-beta", support.upstream_error(502))
        resp = await self.post_chat(self.chat_payload(model="wb-beta"))
        try:
            await resp.read()
        finally:
            await resp.release()
        self.assertEqual(self.observed, ["upstream"], "a real failure is announced once")

    # -- budget ----------------------------------------------------------

    async def test_every_real_attempt_spends_one_permit(self):
        """额度按"真的问了一次上游"计：两个候选的一轮花两个额度。

        迁移到 ACP 之后每一次候选尝试都是一次真实的 CLI prompt，所以
        ``AcpChatBackend`` 是按次扣的（见 ``core/acp/backend.py`` 里
        ``consume_rpm`` 的位置）。网关自己只判不扣，于是这里要验的是"判"：
        额度花光后同一分钟内再来一个请求必须是本地 429。
        """
        self.chat.queue_error("wb-alpha", support.upstream_error(502))
        self.chat.queue_text("wb-beta", "beta answer")
        await self.restart_gateway(rpm=2)

        resp = await self.post_chat(self.chat_payload())
        try:
            self.assertEqual(resp.status, 200)
            await resp.read()
        finally:
            await resp.release()

        # 备用尝试不额外占**并发**名额，但两个额度确实是这一轮花掉的。
        resp = await self.post_chat(self.chat_payload(model="wb-beta"))
        try:
            self.assertEqual(resp.status, 429)
            await resp.read()
        finally:
            await resp.release()

    async def test_single_concurrency_round_fails_over_without_deadlock(self):
        self.chat.queue_error("wb-alpha", support.upstream_error(502))
        self.chat.queue_text("wb-beta", "beta answer")
        await self.restart_gateway(concurrency=1, queue_size=0)

        resp = await self.post_chat(self.chat_payload())
        try:
            self.assertEqual(resp.status, 200)
            await resp.read()
        finally:
            await resp.release()
        self.assertEqual(self.chat.sent_models, ["wb-alpha", "wb-beta"])

    async def test_a_local_limit_error_is_never_reported_as_an_upstream_failure(self):
        self.observed = []
        await self.restart_gateway(rpm=1, concurrency=1, queue_size=0)

        self.chat.queue_text(MODELS[0], "first")
        resp = await self.post_chat(self.chat_payload(model=MODELS[0]))
        try:
            await resp.read()
        finally:
            await resp.release()

        resp = await self.post_chat(self.chat_payload(model=MODELS[0]))
        try:
            self.assertEqual(resp.status, 429)
            body = await resp.json()
        finally:
            await resp.release()
        self.assertEqual(body["error"]["type"], "rate_limit_error")
        self.assertEqual(self.observed, [], "a local limit is not an upstream incident")


# --------------------------------------------------------------------------- #
# approved request-body limit (default 16 MiB, configurable)
# --------------------------------------------------------------------------- #

class RequestBodyLimitTests(_GatewayCase):
    """The gateway itself enforces the configured body limit, byte-exactly."""

    def setUp(self) -> None:
        self.observed = []

    async def test_default_limit_accepts_a_body_above_the_aiohttp_one_mib_default(self):
        body = _body_of_exact_bytes(int(1.12 * _MIB))
        self.assertGreater(len(body), _MIB)

        resp = await self.post_raw(body)
        try:
            self.assertEqual(
                resp.status,
                200,
                "the approved 16 MiB default must not be blocked by a 1 MiB limit",
            )
            await resp.read()
        finally:
            await resp.release()

        sent = self.chat.chat_bodies()
        self.assertEqual(len(sent), 1)
        self.assertGreater(
            len(sent[0]["messages"][0]["content"].encode("utf-8")),
            _MIB,
            "the whole body must reach the backend",
        )

    async def test_one_mib_limit_accepts_just_below_and_rejects_above(self):
        await self.restart_gateway(max_request_body_mb=1)

        for total, expected in ((_MIB - 1, 200), (_MIB + 1, 413)):
            with self.subTest(total=total):
                resp = await self.post_raw(_body_of_exact_bytes(total))
                try:
                    self.assertEqual(
                        resp.status,
                        expected,
                        "a byte-exact limit check for {} bytes".format(total),
                    )
                    with contextlib.suppress(Exception):
                        await resp.read()
                finally:
                    await resp.release()

    async def test_multibyte_and_chunked_bodies_are_counted_in_bytes(self):
        await self.restart_gateway(max_request_body_mb=1)

        over = _body_of_exact_bytes(_MIB + 6, prefix="中文内容")
        self.assertGreater(len(over), _MIB)
        self.assertLess(
            len(over.decode("utf-8")),
            _MIB,
            "the character count stays below the limit; only the byte count is over",
        )

        under = _body_of_exact_bytes(_MIB - 4, prefix="中文内容")
        self.assertLess(len(under), _MIB)

        resp = await self.post_raw(over)
        try:
            self.assertEqual(resp.status, 413)
            with contextlib.suppress(Exception):
                await resp.read()
        finally:
            await resp.release()

        resp = await self.post_raw(under)
        try:
            self.assertEqual(resp.status, 200)
            await resp.read()
        finally:
            await resp.release()

        resp = await self.post_raw(over, chunked=True)
        try:
            self.assertEqual(
                resp.status, 413, "a chunked oversized body must be refused too"
            )
            with contextlib.suppress(Exception):
                await resp.read()
        finally:
            await resp.release()

        resp = await self.post_raw(under, chunked=True)
        try:
            self.assertEqual(resp.status, 200, "a chunked in-limit body must be accepted")
            await resp.read()
        finally:
            await resp.release()

    async def test_413_is_json_with_a_fixed_type_and_an_actionable_message(self):
        await self.restart_gateway(max_request_body_mb=2)
        marker = "TOP-SECRET-BODY-MARKER"
        resp = await self.post_raw(_body_of_exact_bytes(3 * _MIB, prefix=marker))
        try:
            self.assertEqual(resp.status, 413)
            self.assertIn("application/json", resp.headers.get("Content-Type", ""))
            text = await resp.text()
        finally:
            await resp.release()

        error = (json.loads(text) or {}).get("error") or {}
        self.assertEqual(error.get("type"), "request_too_large")
        self.assertEqual(error.get("code"), "request_too_large")
        message = error.get("message") or ""
        self.assertIn("2", message, "the message must state the current limit")
        self.assertIn("MiB", message, "the message must state the unit of the limit")
        self.assertIn(
            "max_request_body_mb",
            message,
            "the message must say which setting to change",
        )
        for leak in (marker, support.API_KEY, support.SEED_CREDENTIALS["accessToken"]):
            self.assertNotIn(leak, text, "the 413 body must not echo request data")

    async def test_malformed_json_is_still_400_and_distinct_from_413(self):
        await self.restart_gateway(max_request_body_mb=1)

        resp = await self.post_raw(b'{"model": "wb-1", "messages": [')
        try:
            self.assertEqual(resp.status, 400)
            text = await resp.text()
        finally:
            await resp.release()

        error = (json.loads(text) or {}).get("error") or {}
        self.assertNotEqual(
            error.get("type"),
            "request_too_large",
            "a format error is a 400, never a 413",
        )

    async def test_unauthorized_oversized_body_is_401_first(self):
        await self.restart_gateway(max_request_body_mb=1)

        resp = await self.post_raw(_body_of_exact_bytes(_MIB + 128), key="")
        try:
            self.assertEqual(
                resp.status, 401, "authorization is judged before the body limit"
            )
            with contextlib.suppress(Exception):
                await resp.read()
        finally:
            await resp.release()

    async def test_oversized_virtual_request_never_reaches_the_route(self):
        await self.restart_gateway(max_request_body_mb=1)
        self.chat.queue_text("wb-alpha", "alpha answer")

        resp = await self.post_raw(
            _body_of_exact_bytes(_MIB + 64, model=ORDERED_MODEL_ID)
        )
        try:
            self.assertEqual(resp.status, 413)
            with contextlib.suppress(Exception):
                await resp.read()
        finally:
            await resp.release()

        self.assertEqual(
            self.chat.sent_models, [], "a 413 must never touch the backend"
        )
        self.assertIsNone(
            self.router.status().get("last_result"),
            "a 413 must not consume a candidate or record a round",
        )


class GatewayBodyLimitConfigTests(unittest.TestCase):
    """The approved setting: ``gateway.max_request_body_mb``, default 16."""

    def test_gateway_settings_expose_the_sixteen_mib_default(self):
        gateway = load_settings({}).get("gateway") or {}
        self.assertIn(
            "max_request_body_mb",
            gateway,
            "load_settings()['gateway'] must carry the body-limit setting",
        )
        self.assertEqual(
            gateway["max_request_body_mb"], 16, "the approved default is 16 MiB"
        )

        custom = load_settings({"gateway": {"max_request_body_mb": 3}}).get("gateway") or {}
        self.assertEqual(custom.get("max_request_body_mb"), 3)

    def test_invalid_body_limits_are_rejected_without_echo(self):
        marker = "wb-secret-limit"
        for bad in (0, -1, "16", True, 1.5, marker):
            with self.subTest(value=bad):
                with self.assertRaises(ConfigError) as caught:
                    load_settings({"gateway": {"max_request_body_mb": bad}})
                message = str(caught.exception)
                self.assertTrue(message, "the fixed ConfigError message must not be empty")
                self.assertNotIn(
                    marker, message, "the raw setting value leaked into the error"
                )


class GatewayInterfaceTests(unittest.TestCase):
    def test_gateway_has_no_upstream_client_and_no_alias_mapping(self):
        """网关不再认识"上游客户端"，也不再做显示别名到模型 ID 的翻译。"""
        import inspect

        params = set(inspect.signature(Gateway.__init__).parameters)
        self.assertIn("catalog", params)
        self.assertIn("router", params)
        self.assertNotIn("client", params)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
