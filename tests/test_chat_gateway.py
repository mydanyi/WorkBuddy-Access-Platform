"""Executable contract tests for the local OpenAI-compatible chat gateway.

Scope: only the documented public contract of ``core.chat.ChatService`` and
``core.gateway.Gateway``.  The production modules do not exist yet on purpose:
the top-level imports below are expected to fail until those modules are
implemented and verified remotely in the Docker host.  Do not "fix" that here.

The fake *WorkBuddy upstream* is a real ``aiohttp.web`` server bound to
127.0.0.1:0 that streams SSE on ``/v2/chat/completions``.  A real
``ChatService`` and a real ``Gateway`` (no mock of our own code) sit in front of
it, and the assertions drive the Gateway with a real ``aiohttp`` client.  Every
credential/token below is a FICTIONAL FIXED SAMPLE and is never read from a live
source.

Contract pinned from ``internal/upstream/sse.go``:

* upstream is always asked with ``stream: true`` (it speaks SSE);
* half lines / split UTF-8 / CRLF are reassembled; ``:``-comments and ``data:``
  without a space are handled; multiple ``data:`` lines join with ``\\n``;
* chunk framing carries ``id``/``object``/``created``/``model`` and per-choice
  ``index``/``delta``/``finish_reason``; ``reasoning_content`` and streamed
  ``tool_calls`` are merged by ``index``; ``usage`` is preserved;
* an empty stream, a ``[DONE]``-only stream, bad JSON, an error event or an EOF
  truncation is a *failure* (502), never a fake success;
* errors after the stream already started must not trigger a second upstream
  request.

Temporary files use the same NAS-hosted ``tmp`` root as the other tests; the
system temp dir is never used as a fallback.
"""

import asyncio
import base64
import collections
import contextlib
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

import aiohttp
from aiohttp import web

# --- Make ``astrbot_plugin_workbuddy`` importable when discovered from anywhere.
_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]  # directory that contains astrbot_plugin_workbuddy/
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.auth import AuthManager  # noqa: E402
from astrbot_plugin_workbuddy.core.chat import ChatService  # noqa: E402
from astrbot_plugin_workbuddy.core.client import UpstreamClient  # noqa: E402
from astrbot_plugin_workbuddy.core.gateway import Gateway  # noqa: E402
from astrbot_plugin_workbuddy.core.storage import CredentialStore  # noqa: E402


def _resolve_base_tmp() -> Path:
    env = os.environ.get("WORKBUDDY_TEST_TMP")
    if env:
        return Path(env)
    task_root = _HERE.parents[3]
    return task_root / "tmp"


BASE_TMP = _resolve_base_tmp()

# --- FICTIONAL FIXED SAMPLE KEY (standard base64 of exactly 32 bytes). ---
_KEY_BYTES = b"workbuddy-chatgw-master-key-0001"
if len(_KEY_BYTES) != 32:
    raise AssertionError("sample master key must be exactly 32 bytes")
MASTER_KEY = base64.b64encode(_KEY_BYTES).decode("ascii")

API_KEY = "FICTIONAL-LOCAL-GATEWAY-KEY"
AT_OLD = "FICTIONAL-OLD-ACCESS-TOKEN"
RT_OLD = "FICTIONAL-OLD-REFRESH-TOKEN"
AT_NEW = "FICTIONAL-NEW-ACCESS-TOKEN"
RT_NEW = "FICTIONAL-NEW-REFRESH-TOKEN"
UID = "test-uid-chatgw"
DOMAIN = "example.test.invalid"

CHAT_PATH = "/v2/chat/completions"
REFRESH_PATH = "/v2/plugin/auth/token/refresh"
STATE_PATH = "/v2/plugin/auth/state"
ACCOUNT_PATH = "/v2/plugin/login/account"
MODELS_PATH = "/v1/models"
COMPLETIONS_PATH = "/v1/chat/completions"
HEALTH_PATH = "/health"
# The real upstream enterprise model catalog the gateway must fetch its model
# list from (GET).  The gateway must never serve a fabricated static list.
UPSTREAM_MODELS_PATH = "/console/enterprises/personal/models"

MODEL = "wb-1"
SEED_CREDENTIALS = {
    "accessToken": AT_OLD,
    "refreshToken": RT_OLD,
    "uid": UID,
    "expiresAt": 4102444800000,
    "domain": DOMAIN,
    "realm": "cn",
}


def _frame(obj, ending="\n\n") -> bytes:
    return b"data: " + json.dumps(obj, ensure_ascii=False).encode("utf-8") + ending.encode()


_DONE = b"data: [DONE]\n\n"


def _chunk(delta, *, index=0, finish=None, cid="chatcmpl-1", model=MODEL, created=111,
           obj="chat.completion.chunk"):
    return {
        "id": cid,
        "object": obj,
        "created": created,
        "model": model,
        "choices": [{"index": index, "delta": delta, "finish_reason": finish}],
    }


def _basic_sse(text="hi", *, crlf=False):
    ending = "\r\n\r\n" if crlf else "\n\n"
    frames = [
        _frame(_chunk({"role": "assistant"}), ending=ending),
        _frame(_chunk({"content": text}), ending=ending),
        _frame(_chunk({}, finish="stop"), ending=ending),
        b"data: [DONE]" + ending.encode(),
    ]
    return frames


def _json_completion(text="json-hi"):
    return {
        "id": "chatcmpl-json",
        "object": "chat.completion",
        "created": 222,
        "model": MODEL,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
    }


def _merge_chunks(events):
    """Best-effort OpenAI chunk aggregation used only by the assertions."""
    content, reasoning = [], []
    tools = {}
    usage = None
    finish = None
    meta = {}
    for ev in events:
        for key in ("id", "model", "created", "object"):
            if ev.get(key) not in (None, ""):
                meta.setdefault(key, ev[key])
        if ev.get("usage"):
            usage = ev["usage"]
        for choice in ev.get("choices") or []:
            if choice.get("finish_reason"):
                finish = choice["finish_reason"]
            delta = choice.get("delta") or {}
            if delta.get("content"):
                content.append(delta["content"])
            if delta.get("reasoning_content"):
                reasoning.append(delta["reasoning_content"])
            for call in delta.get("tool_calls") or []:
                slot = tools.setdefault(
                    call.get("index", 0), {"id": "", "name": "", "arguments": ""}
                )
                if call.get("id"):
                    slot["id"] = call["id"]
                fn = call.get("function") or {}
                if fn.get("name"):
                    slot["name"] += fn["name"]
                if fn.get("arguments"):
                    slot["arguments"] += fn["arguments"]
    return {
        "content": "".join(content),
        "reasoning": "".join(reasoning),
        "tools": tools,
        "usage": usage,
        "finish": finish,
        "meta": meta,
    }


class _FakeUpstream:
    """Real aiohttp upstream: SSE chat endpoint + refresh + call recorder."""

    def __init__(self) -> None:
        self.app = web.Application()
        self.calls = []
        self.hits = collections.Counter()
        self.programs = collections.deque()
        self.refresh_hits = 0
        self.base = None
        self._runner = None
        self._site = None
        self.app.router.add_route("*", "/{tail:.*}", self._dispatch)

    # -- program helpers -------------------------------------------------
    def queue(self, program) -> None:
        self.programs.append(program)

    def queue_sse(self, chunks) -> None:
        self.queue({"kind": "sse", "chunks": list(chunks)})

    def queue_json(self, body, status=200) -> None:
        self.queue({"kind": "json", "body": body, "status": status})

    def queue_status(self, status, body=None, headers=None) -> None:
        self.queue({"kind": "status", "status": status, "body": body or {},
                    "headers": headers or {}})

    # -- routing ---------------------------------------------------------
    async def _dispatch(self, request):
        body = await request.read()
        self.calls.append(
            {
                "method": request.method,
                "path": request.path,
                "query": dict(request.query),
                "headers": {k.lower(): v for k, v in request.headers.items()},
                "body": body,
            }
        )
        self.hits[(request.method, request.path)] += 1
        if request.method == "POST" and request.path == REFRESH_PATH:
            return await self._refresh(request)
        if request.method == "POST" and request.path == CHAT_PATH:
            return await self._chat(request)
        if request.method == "POST" and request.path == STATE_PATH:
            return web.json_response({"code": 0, "data": {"state": "s-1", "authUrl": "https://x.invalid"}})
        if request.method == "GET" and request.path == ACCOUNT_PATH:
            return web.json_response({"code": 0, "data": {"uid": UID, "nickname": "T"}})
        if request.method == "GET" and request.path == UPSTREAM_MODELS_PATH:
            # Real dynamic catalog shape: data.models + data.agents[name=cli].
            # The gateway's /v1/models must be derived from this, not stubbed.
            return web.json_response(
                {
                    "code": 0,
                    "data": {
                        "models": [
                            {
                                "id": MODEL,
                                "name": MODEL,
                                "maxInputTokens": 1000000,
                                "maxOutputTokens": 128000,
                            }
                        ],
                        "agents": [{"name": "cli", "models": [MODEL]}],
                    },
                }
            )
        return web.json_response({"code": 5999, "msg": "unmapped"}, status=404)

    async def _refresh(self, request):
        self.refresh_hits += 1
        return web.json_response(
            {
                "code": 0,
                "data": {
                    "accessToken": AT_NEW,
                    "refreshToken": RT_NEW,
                    "expiresIn": 3600,
                    "domain": DOMAIN,
                },
            }
        )

    async def _chat(self, request):
        program = self.programs.popleft() if self.programs else {"kind": "sse", "chunks": _basic_sse()}
        kind = program.get("kind")
        if kind == "json":
            return web.json_response(program["body"], status=program.get("status", 200))
        if kind == "status":
            return web.json_response(
                program.get("body", {}),
                status=program["status"],
                headers=program.get("headers", {}),
            )
        resp = web.StreamResponse(
            status=200, headers={"Content-Type": "text/event-stream"}
        )
        await resp.prepare(request)
        if program.get("sleep_before"):
            await asyncio.sleep(program["sleep_before"])
        for chunk in program["chunks"]:
            await resp.write(chunk)
            await asyncio.sleep(0)
            if program.get("sleep_between"):
                await asyncio.sleep(program["sleep_between"])
        with contextlib.suppress(Exception):
            await resp.write_eof()
        return resp

    async def start(self) -> None:
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await self._site.start()
        host, port = self._runner.addresses[0][:2]
        self.base = "http://{}:{}".format(host, port)

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()

    def chat_bodies(self):
        return [self.json_body(c) for c in self.calls if c["path"] == CHAT_PATH]

    @staticmethod
    def json_body(call):
        return json.loads(call["body"].decode("utf-8")) if call["body"] else None


class _GatewayTestCase(unittest.IsolatedAsyncioTestCase):
    def _seed_store(self):
        store = CredentialStore(self.data_dir, MASTER_KEY)
        store.save(dict(SEED_CREDENTIALS))
        return store

    async def asyncSetUp(self) -> None:
        BASE_TMP.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(tempfile.mkdtemp(prefix="wb-chatgw-", dir=str(BASE_TMP)))
        self.up = _FakeUpstream()
        await self.up.start()
        self.session = aiohttp.ClientSession()
        self.store = self._seed_store()
        self.manager = AuthManager(
            self.session, self.store, chat_base=self.up.base, poll_interval=0.01
        )
        self.client = UpstreamClient(
            self.session,
            self.manager,
            chat_base=self.up.base,
            billing_base=self.up.base,
            web_base=self.up.base,
        )
        self.chat = ChatService(self.client)
        self.gateway = None
        await self._start_gateway()

    async def asyncTearDown(self) -> None:
        if self.gateway is not None:
            with contextlib.suppress(Exception):
                await self.gateway.stop()
        await self.session.close()
        await self.up.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)

    async def _start_gateway(self, **overrides):
        params = {
            "api_key": API_KEY,
            "host": "127.0.0.1",
            "port": 0,
            "concurrency": 4,
            "rpm": 600,
            "queue_size": 8,
            "queue_timeout": 5,
        }
        params.update(overrides)
        self.gateway = Gateway(self.chat, self.client, **params)
        await self.gateway.start()
        self.base_url = "http://127.0.0.1:{}".format(self.gateway.bound_port)

    async def _restart_gateway(self, **overrides):
        await self.gateway.stop()
        await self._start_gateway(**overrides)

    def _headers(self, key=API_KEY):
        return {"Authorization": "Bearer " + key} if key else {}

    async def _post_chat(self, payload, *, stream, key=API_KEY, session=None):
        body = dict(payload)
        body.setdefault("model", MODEL)
        body.setdefault("messages", [{"role": "user", "content": "hi"}])
        body["stream"] = stream
        sess = session or self.session
        return await sess.post(
            self.base_url + COMPLETIONS_PATH,
            json=body,
            headers=self._headers(key),
        )

    @staticmethod
    async def _read_sse(resp):
        events = []
        done = False
        async for raw in resp.content:
            line = raw.decode("utf-8").rstrip("\r\n")
            if not line or line.startswith(":"):
                continue
            if line.startswith("data:"):
                payload = line[5:].lstrip(" ")
                if payload == "[DONE]":
                    done = True
                    continue
                events.append(json.loads(payload))
        return events, done

    # -- streaming -------------------------------------------------------

    async def test_streaming_reassembles_utf8_half_lines_and_crlf(self):
        text = "你好世界"
        full = b"".join(_basic_sse(text, crlf=True))
        marker = "你".encode("utf-8")
        start = full.index(marker)
        chunks = [
            full[: start + 1],      # splits the first CJK codepoint
            full[start + 1: start + 3],
            full[start + 3: start + 9],
            full[start + 9:],
        ]
        self.up.queue_sse(chunks)

        async with await self._post_chat({}, stream=True) as resp:
            self.assertEqual(resp.status, 200)
            events, done = await self._read_sse(resp)

        merged = _merge_chunks(events)
        self.assertEqual(merged["content"], text)
        self.assertTrue(done)

    async def test_streaming_ignores_comments_and_accepts_data_without_space(self):
        first = json.dumps(_chunk({"role": "assistant"}), ensure_ascii=False)
        second = json.dumps(_chunk({"content": "ns"}), ensure_ascii=False)
        self.up.queue_sse(
            [
                b": keepalive\n\n",
                b"data:" + first.encode("utf-8") + b"\n\n",  # no space after colon
                b"data: " + second.encode("utf-8") + b"\n\n",
                _DONE,
            ]
        )

        async with await self._post_chat({}, stream=True) as resp:
            self.assertEqual(resp.status, 200)
            events, done = await self._read_sse(resp)

        merged = _merge_chunks(events)
        self.assertEqual(merged["content"], "ns")
        self.assertTrue(done)

    async def test_streaming_joins_multiline_data_fields(self):
        joined = (
            b'data: {"id":"m1","object":"chat.completion.chunk","model":"'
            + MODEL.encode()
            + b'","choices":[{"index":0,"delta":{"content":"ml"}\n'
            b'data: }],"created":5}\n\n'
        )
        self.up.queue_sse([joined, _DONE])

        async with await self._post_chat({}, stream=True) as resp:
            self.assertEqual(resp.status, 200)
            events, done = await self._read_sse(resp)

        merged = _merge_chunks(events)
        self.assertEqual(merged["content"], "ml")
        self.assertTrue(done)

    async def test_streaming_preserves_index_reasoning_tools_and_usage(self):
        cid, model, created = "chatcmpl-x", "wb-9", 42
        frames = [
            _frame(_chunk({"role": "assistant"}, cid=cid, model=model, created=created)),
            _frame(_chunk({"reasoning_content": "think "}, cid=cid, model=model, created=created)),
            _frame(_chunk({"content": "beta"}, index=1, cid=cid, model=model, created=created)),
            _frame(
                _chunk(
                    {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call_1",
                                "type": "function",
                                "function": {"name": "do_it", "arguments": '{"a":'},
                            }
                        ]
                    },
                    cid=cid,
                    model=model,
                    created=created,
                )
            ),
            _frame(
                _chunk(
                    {"tool_calls": [{"index": 0, "function": {"arguments": "1}"}}]},
                    cid=cid,
                    model=model,
                    created=created,
                )
            ),
            _frame(
                {
                    "id": cid,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model,
                    "choices": [],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
                }
            ),
            _frame(_chunk({}, finish="tool_calls", cid=cid, model=model, created=created)),
            _frame(_chunk({}, index=1, finish="stop", cid=cid, model=model, created=created)),
            _DONE,
        ]
        self.up.queue_sse(frames)

        async with await self._post_chat({}, stream=True) as resp:
            self.assertEqual(resp.status, 200)
            events, done = await self._read_sse(resp)

        merged = _merge_chunks(events)
        self.assertEqual(merged["reasoning"], "think ")
        self.assertEqual(merged["content"], "beta")
        self.assertEqual(merged["tools"][0]["name"], "do_it")
        self.assertEqual(merged["tools"][0]["arguments"], '{"a":1}')
        self.assertEqual(merged["usage"]["total_tokens"], 3)
        self.assertEqual(merged["meta"].get("model"), model)
        self.assertEqual(merged["meta"].get("created"), created)
        self.assertTrue(done)
        indices = {
            ch["index"]
            for ev in events
            for ch in (ev.get("choices") or [])
        }
        self.assertIn(0, indices)
        self.assertIn(1, indices)

    # -- non-stream ------------------------------------------------------

    async def test_nonstream_aggregates_forced_upstream_sse(self):
        self.up.queue_sse(_basic_sse("agg"))

        resp = await self._post_chat({}, stream=False)
        self.assertEqual(resp.status, 200)
        self.assertIn("application/json", resp.headers.get("Content-Type", ""))
        body = await resp.json()
        await resp.release()

        self.assertEqual(body["choices"][0]["message"]["content"], "agg")
        self.assertEqual(body["choices"][0]["finish_reason"], "stop")
        # The upstream is always spoken to in streaming mode.
        self.assertTrue(self.up.chat_bodies()[0]["stream"] is True)

    async def test_nonstream_accepts_plain_json_completion(self):
        self.up.queue_json(_json_completion("json-hi"))

        resp = await self._post_chat({}, stream=False)
        body = await resp.json()
        await resp.release()

        self.assertEqual(resp.status, 200)
        self.assertEqual(body["choices"][0]["message"]["content"], "json-hi")

    # -- failures are never fake successes -------------------------------

    async def test_empty_and_done_only_streams_return_502(self):
        self.up.queue_sse([])
        self.up.queue_sse([_DONE])

        for _ in range(2):
            async with await self._post_chat({}, stream=True) as resp:
                self.assertEqual(resp.status, 502)
                await resp.read()

    async def test_bad_json_and_error_event_return_502(self):
        self.up.queue_sse([b"data: {not json}\n\n", _DONE])
        self.up.queue_sse([_frame({"error": {"message": "upstream boom"}})])

        for _ in range(2):
            async with await self._post_chat({}, stream=True) as resp:
                self.assertEqual(resp.status, 502)
                await resp.read()

    async def test_eof_truncation_without_done_returns_502(self):
        # A complete-looking frame but the stream ends without data: [DONE].
        self.up.queue_sse([_frame(_chunk({"content": "partial"}))])

        resp = await self._post_chat({}, stream=False)
        try:
            self.assertEqual(resp.status, 502)
        finally:
            await resp.release()

    async def test_error_after_stream_started_does_not_refire_upstream(self):
        self.up.queue_sse(
            [
                _frame(_chunk({"role": "assistant"})),
                _frame(_chunk({"content": "began"})),
                _frame({"error": {"message": "boom mid-stream"}}),
            ]
        )

        async with await self._post_chat({}, stream=True) as resp:
            self.assertEqual(resp.status, 200)
            with contextlib.suppress(Exception):
                await self._read_sse(resp)

        # Streamed once, never re-requested after the mid-stream error.
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 1)

    # -- passthrough / validation ----------------------------------------

    async def test_messages_and_tools_pass_through_unmodified(self):
        self.up.queue_sse(_basic_sse("ok"))
        messages = [
            {"role": "system", "content": "You are persona X."},
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": None, "tool_calls": [
                {"id": "c0", "type": "function", "function": {"name": "f", "arguments": "{}"}}
            ]},
            {"role": "tool", "tool_call_id": "c0", "content": "result"},
            {"role": "user", "content": "again"},
        ]
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "f",
                    "description": "d",
                    "parameters": {"type": "object", "properties": {}},
                },
            }
        ]
        tool_choice = {"type": "function", "function": {"name": "f"}}

        resp = await self._post_chat(
            {"messages": messages, "tools": tools, "tool_choice": tool_choice},
            stream=False,
        )
        await resp.read()
        await resp.release()

        sent = self.up.chat_bodies()[0]
        self.assertEqual(sent["messages"], messages)
        self.assertEqual(sent["tools"], tools)
        self.assertEqual(sent["tool_choice"], tool_choice)

    async def test_missing_model_or_messages_returns_400(self):
        cases = [
            {},                                   # neither
            {"model": MODEL},                      # no messages
            {"messages": [{"role": "user", "content": "hi"}]},  # no model
        ]
        for payload in cases:
            body = dict(payload)
            body["stream"] = False
            async with self.session.post(
                self.base_url + COMPLETIONS_PATH, json=body, headers=self._headers()
            ) as resp:
                self.assertEqual(resp.status, 400)
                await resp.read()

        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 0)

    async def test_n_greater_than_one_never_silently_drops_choices(self):
        # Upstream offers two choices; a supporting gateway keeps both by index,
        # otherwise it must answer 400 -- never silently return only one.
        self.up.queue_sse(
            [
                _frame(_chunk({"content": "a"}, index=0)),
                _frame(_chunk({"content": "b"}, index=1)),
                _frame(_chunk({}, index=0, finish="stop")),
                _frame(_chunk({}, index=1, finish="stop")),
                _DONE,
            ]
        )

        resp = await self._post_chat({"n": 2}, stream=False)
        try:
            if resp.status == 200:
                body = await resp.json()
                indices = sorted(c["index"] for c in body["choices"])
                self.assertEqual(indices, [0, 1])
            else:
                self.assertEqual(resp.status, 400)
        finally:
            await resp.release()

    # -- gateway auth / health / models ----------------------------------

    async def test_gateway_auth_and_health_expose_no_account_data(self):
        async with self.session.post(
            self.base_url + COMPLETIONS_PATH,
            json={"model": MODEL, "messages": [{"role": "user", "content": "hi"}]},
        ) as resp:
            self.assertEqual(resp.status, 401)
            await resp.read()

        async with self.session.post(
            self.base_url + COMPLETIONS_PATH,
            json={"model": MODEL, "messages": [{"role": "user", "content": "hi"}]},
            headers=self._headers("WRONG-KEY"),
        ) as resp:
            self.assertEqual(resp.status, 401)
            await resp.read()

        async with self.session.get(
            self.base_url + MODELS_PATH, headers=self._headers()
        ) as resp:
            self.assertEqual(resp.status, 200)
            body = await resp.json()
            data = body.get("data")
            self.assertIsInstance(data, list)
            ids = [item.get("id") if isinstance(item, dict) else item for item in data]
            # The advertised model must come from the real upstream catalog.
            self.assertIn(MODEL, ids)
            self.assertNotIn(UID, json.dumps(body))
            self.assertNotIn(AT_OLD, json.dumps(body))

        # The gateway derived the list from the real catalog endpoint.
        self.assertGreaterEqual(self.up.hits[("GET", UPSTREAM_MODELS_PATH)], 1)

        async with self.session.get(self.base_url + HEALTH_PATH) as resp:
            self.assertEqual(resp.status, 200)
            body = await resp.json()
            text = json.dumps(body)
            for secret in (UID, AT_OLD, RT_OLD, API_KEY):
                self.assertNotIn(secret, text)

    async def test_empty_api_key_does_not_open_chat_listener(self):
        # With no api_key the gateway must not expose the chat port at all; any
        # health probing (with any status) is allowed, but chat must never open.
        await self.gateway.stop()
        gw = Gateway(
            self.chat,
            self.client,
            api_key="",
            host="127.0.0.1",
            port=0,
            concurrency=4,
            rpm=600,
            queue_size=8,
            queue_timeout=5,
        )
        self.gateway = gw
        try:
            await gw.start()
        except Exception:  # noqa: BLE001 - refusing to start is "not listening"
            return

        port = getattr(gw, "bound_port", None)
        if not port:
            # Refusing to bind at all is the strongest form of "not listening".
            self.assertFalse(port)
            return

        # If a port was bound it may serve health (any status), but the chat
        # endpoint must not be reachable: connecting to it has to fail.
        with contextlib.suppress(aiohttp.ClientError):
            async with self.session.get(
                "http://127.0.0.1:{}{}".format(port, HEALTH_PATH)
            ) as resp:
                await resp.read()

        chat_url = "http://127.0.0.1:{}{}".format(port, COMPLETIONS_PATH)
        body = {
            "model": MODEL,
            "messages": [{"role": "user", "content": "hi"}],
            "stream": False,
        }
        with self.assertRaises(aiohttp.ClientError):
            async with self.session.post(chat_url, json=body) as resp:
                await resp.read()

    # -- upstream auth semantics -----------------------------------------

    async def test_upstream_401_refreshes_once_while_403_429_preserved(self):
        # 401 then success -> exactly one refresh, exactly one resend.
        self.up.queue_status(401, {"code": 1001, "msg": "expired"})
        self.up.queue_sse(_basic_sse("after-refresh"))
        async with await self._post_chat({}, stream=True) as resp:
            self.assertEqual(resp.status, 200)
            await resp.read()
        self.assertEqual(self.up.refresh_hits, 1)
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 2)

        # 403 must not refresh and must be preserved.
        self.up.queue_status(403, {"code": 4030, "msg": "nope"})
        async with await self._post_chat({}, stream=False) as resp:
            self.assertEqual(resp.status, 403)
            await resp.read()
        self.assertEqual(self.up.refresh_hits, 1)

        # 429 must not refresh and must keep Retry-After.
        self.up.queue_status(429, {"code": 4290, "msg": "slow"}, {"Retry-After": "7"})
        async with await self._post_chat({}, stream=False) as resp:
            self.assertEqual(resp.status, 429)
            await resp.read()
        self.assertEqual(self.up.refresh_hits, 1)

    # -- concurrency / rate limiting -------------------------------------

    async def test_queue_full_and_rate_limit_reject(self):
        async def one():
            async with await self._post_chat({}, stream=False) as resp:
                await resp.read()
                return resp.status

        # A slow upstream keeps the single worker busy so the queue overflows.
        for _ in range(4):
            self.up.queue({"kind": "sse", "chunks": _basic_sse("slow"), "sleep_between": 0.2})
        await self._restart_gateway(concurrency=1, queue_size=1, rpm=1000, queue_timeout=1.0)

        statuses = await asyncio.gather(one(), one(), one())
        self.assertEqual(statuses.count(200), 2)
        self.assertEqual(len([s for s in statuses if s in (429, 503)]), 1)

        # Per-minute rate limit rejects the excess burst regardless of capacity.
        self.up.programs.clear()
        for _ in range(3):
            self.up.queue_sse(_basic_sse("fast"))
        await self._restart_gateway(concurrency=4, queue_size=4, rpm=2, queue_timeout=5)
        statuses = await asyncio.gather(one(), one(), one())
        self.assertGreaterEqual(statuses.count(200), 1)
        self.assertGreaterEqual(len([s for s in statuses if s == 429]), 1)

    # -- shutdown --------------------------------------------------------

    async def test_stop_closes_streams_without_leaked_tasks(self):
        self.up.queue({"kind": "sse", "chunks": _basic_sse("slow"), "sleep_between": 0.5})

        cancel_session = aiohttp.ClientSession()
        resp = None
        try:
            resp = await self._post_chat({}, stream=True, session=cancel_session)
            # Read the first frame so the gateway handler is genuinely
            # mid-stream (and registered as in-flight) before we stop.
            first = await asyncio.wait_for(resp.content.readline(), 1.0)
            self.assertTrue(first)
            inflight = set(self.gateway._inflight)
            self.assertTrue(inflight, "gateway never registered an in-flight handler")

            await self.gateway.stop()

            # Every real handler task captured before stop must have finished,
            # and no slot / waiter may be left behind.  The fake upstream's own
            # service task is deliberately NOT counted as a plugin leak.
            for task in inflight:
                self.assertTrue(task.done(), "gateway handler survived stop()")
            self.assertFalse(self.gateway._inflight)
            self.assertEqual(self.gateway.limits.active, 0)
            self.assertEqual(self.gateway.limits.queued, 0)
        finally:
            # The response is always released, even when an assertion failed, so
            # no half-read ClientResponse is left unclosed.
            if resp is not None:
                resp.close()
            await cancel_session.close()


if __name__ == "__main__":
    unittest.main()
