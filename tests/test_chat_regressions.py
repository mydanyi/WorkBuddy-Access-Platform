"""Defect regression tests for the chat gateway / ``core.chat`` SSE contract.

Companion to ``tests/test_chat_gateway.py``.  It reuses that module's real
``aiohttp`` fake upstream and frame helpers but defines its *own* test case, so
none of the existing contract tests are executed twice.

Every scenario here pins behaviour the current production code
(``core.chat`` / ``core.gateway`` / ``core.limits``) is known to get wrong.
The assertions describe the intended contract and are expected to fail until
the modules are fixed and re-verified remotely in the Docker host.  Nothing in
this file mocks our own production functions: the fake upstream is a real
``aiohttp.web`` server and the gateway is driven over a real socket.

Every credential / token below is a FICTIONAL FIXED SAMPLE.

Temporary files use the NAS-hosted ``tmp`` root and honour
``WORKBUDDY_TEST_TMP``; the system temp dir is never used as a fallback.
"""

import asyncio
import contextlib
import json
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
from astrbot_plugin_workbuddy.core.limits import LimitExceeded  # noqa: E402
from astrbot_plugin_workbuddy.core.storage import CredentialStore  # noqa: E402

# Reuse the contract suite's real fake-upstream fixtures, but NOT its test case
# (subclassing ``_GatewayTestCase`` would re-run every contract test).
try:  # package discovery: ``python -m unittest discover`` from the plugin dir.
    from tests.test_chat_gateway import (  # noqa: E402
        _FakeUpstream,
        _frame,
        _chunk,
        _basic_sse,
        _DONE,
        MASTER_KEY,
        API_KEY,
        AT_OLD,
        RT_OLD,
        UID,
        DOMAIN,
        MODEL,
        SEED_CREDENTIALS,
        CHAT_PATH,
        COMPLETIONS_PATH,
        HEALTH_PATH,
        BASE_TMP,
    )
except ImportError:  # top-level discovery: ``python -m unittest discover -s tests``.
    from test_chat_gateway import (  # noqa: E402
        _FakeUpstream,
        _frame,
        _chunk,
        _basic_sse,
        _DONE,
        MASTER_KEY,
        API_KEY,
        AT_OLD,
        RT_OLD,
        UID,
        DOMAIN,
        MODEL,
        SEED_CREDENTIALS,
        CHAT_PATH,
        COMPLETIONS_PATH,
        HEALTH_PATH,
        BASE_TMP,
    )

# The approved design aliases the unauthenticated liveness probe; it must expose
# no account data just like ``/health``.
HEALTHZ_PATH = "/healthz"


def _payload(**extra) -> dict:
    body = {
        "model": MODEL,
        "messages": [{"role": "user", "content": "hi"}],
        "stream": False,
    }
    body.update(extra)
    return body


class _HoldUpstream(_FakeUpstream):
    """Fake upstream that can hold a connection open or drop it mid-response.

    ``queue_hold`` writes its frames then keeps the connection alive until the
    transport closes (client hung up) or :attr:`release` is set.  ``queue_abort``
    writes its frames then tears the connection down abruptly without any SSE
    terminator -- exactly what an upstream disconnect / read timeout looks like.
    """

    def __init__(self) -> None:
        super().__init__()
        self.hold_started = asyncio.Event()
        self.release = asyncio.Event()
        self.upstream_closed = asyncio.Event()

    def queue_hold(self, chunks) -> None:
        self.queue({"kind": "hold", "chunks": list(chunks)})

    def queue_abort(self, chunks=()) -> None:
        self.queue({"kind": "abort", "chunks": list(chunks)})

    async def _chat(self, request):
        program = self.programs[0] if self.programs else None
        kind = program.get("kind") if isinstance(program, dict) else None
        if kind == "hold":
            self.programs.popleft()
            return await self._hold(request, program)
        if kind == "abort":
            self.programs.popleft()
            return await self._abort(request, program)
        return await super()._chat(request)

    async def _hold(self, request, program):
        resp = web.StreamResponse(
            status=200, headers={"Content-Type": "text/event-stream"}
        )
        await resp.prepare(request)
        for chunk in program["chunks"]:
            await resp.write(chunk)
        self.hold_started.set()
        # Capture the live transport once: once aiohttp observes the peer hanging
        # up, ``base_protocol.connection_lost`` sets ``request.transport`` to
        # ``None`` (``web_request.transport`` returns ``protocol.transport``), so
        # the closed state must be recognised from a ``None`` transport too -- not
        # only from ``is_closing()`` on a still-present transport.
        transport = request.transport
        try:
            while not self.release.is_set():
                if transport is None or transport.is_closing() or request.transport is None:
                    self.upstream_closed.set()
                    return resp
                await asyncio.sleep(0.01)
        finally:
            with contextlib.suppress(Exception):
                await resp.write_eof()
        return resp

    async def _abort(self, request, program):
        resp = web.StreamResponse(
            status=200, headers={"Content-Type": "text/event-stream"}
        )
        await resp.prepare(request)
        for chunk in program.get("chunks", []):
            await resp.write(chunk)
        if program.get("chunks"):
            await asyncio.sleep(0.1)  # let the frame reach the gateway
        # No terminator: an abrupt drop, never a clean end.
        raise RuntimeError("simulated upstream disconnect")


class _ChatRegressionTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        BASE_TMP.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(tempfile.mkdtemp(prefix="wb-chatreg-", dir=str(BASE_TMP)))
        self.up = _HoldUpstream()
        await self.up.start()
        self.session = aiohttp.ClientSession()
        self.store = CredentialStore(self.data_dir, MASTER_KEY)
        self.store.save(dict(SEED_CREDENTIALS))
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
        self.up.release.set()  # never leave a held upstream connection parked
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

    @staticmethod
    async def _wait(event, timeout):
        try:
            await asyncio.wait_for(event.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    @staticmethod
    async def _wait_until(predicate, timeout):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if predicate():
                return True
            await asyncio.sleep(0.02)
        return predicate()

    # -- 1. [DONE] terminates reading ------------------------------------

    async def test_done_terminates_reading_and_ignores_trailing_data(self):
        # Non-stream request: a valid stream ended by [DONE] must complete even
        # though the upstream then keeps the connection open (no EOF).  The
        # gateway must stop reading the upstream right after [DONE].
        self.up.queue_hold(
            [
                _frame(_chunk({"role": "assistant"})),
                _frame(_chunk({"content": "done-content"})),
                _frame(_chunk({}, finish="stop")),
                _DONE,
            ]
        )
        loop = asyncio.get_running_loop()
        started = loop.time()
        resp = await asyncio.wait_for(self._post_chat({}, stream=False), timeout=2.0)
        elapsed = loop.time() - started
        try:
            self.assertLess(elapsed, 1.0)
            body = await asyncio.wait_for(resp.json(), timeout=1.0)
        finally:
            await resp.release()
        self.assertEqual(resp.status, 200)
        self.assertEqual(body["choices"][0]["message"]["content"], "done-content")
        # The upstream read loop was torn down, not left parked on the open link.
        self.assertTrue(await self._wait(self.up.upstream_closed, 1.0))

        # [DONE] followed, in the same network chunk, by a malformed frame and
        # stray body text: the tail must be ignored (only pre-[DONE] content).
        self.up.queue_sse(
            [
                _frame(_chunk({"content": "kept"})),
                _DONE + b'data: {not json}\n\n' + b"stray trailing body",
            ]
        )
        resp = await self._post_chat({}, stream=False)
        try:
            self.assertEqual(resp.status, 200)
            body = await resp.json()
        finally:
            await resp.release()
        self.assertEqual(body["choices"][0]["message"]["content"], "kept")

    # -- 2. client disconnect releases the gate + upstream ---------------

    async def test_client_disconnect_releases_gate_and_upstream(self):
        self.up.queue_hold([_frame(_chunk({"role": "assistant"}))])
        session = aiohttp.ClientSession()
        try:
            resp = await session.post(
                self.base_url + COMPLETIONS_PATH,
                json=_payload(stream=True),
                headers=self._headers(),
            )
            first = await asyncio.wait_for(resp.content.readline(), 1.0)
            self.assertTrue(first)
            resp.close()  # abrupt client disconnect while upstream holds
        finally:
            await session.close()

        # Without gateway.stop(), the cancelled handler must release its slot
        # and close the upstream connection within ~1s.
        released = await self._wait_until(
            lambda: self.gateway.limits.active == 0 and not self.gateway._inflight,
            1.0,
        )
        self.assertTrue(released, "gateway kept the slot/inflight after disconnect")
        self.assertTrue(await self._wait(self.up.upstream_closed, 1.0))

    # -- 3. shared gate covers direct ChatService growth calls -----------

    async def test_chat_complete_shares_gateway_gate_and_default_rpm(self):
        # Gateway and a direct ChatService call must share one gate: one gateway
        # request consumes the single rpm, so a direct call is limited.
        await self._restart_gateway(
            concurrency=1, queue_size=2, queue_timeout=1.0, rpm=1
        )
        self.up.queue_sse(_basic_sse("via-gateway"))
        resp = await self._post_chat({}, stream=False)
        try:
            self.assertEqual(resp.status, 200)  # no double gate acquisition
            body = await resp.json()
        finally:
            await resp.release()
        self.assertEqual(body["choices"][0]["message"]["content"], "via-gateway")

        with self.assertRaises(LimitExceeded):
            await self.chat.complete(_payload())
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 1)

        # The default service carries its own conservative budget (1 concurrent,
        # 6 RPM, queue 2): the 7th request is refused without waiting a minute.
        bare = ChatService(self.client)
        for _ in range(6):
            self.up.queue_sse(_basic_sse("ok"))
        for _ in range(6):
            result = await bare.complete(_payload())
            self.assertIn("choices", result)
        with self.assertRaises(LimitExceeded):
            await bare.complete(_payload())
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 7)

    # -- 4. empty / envelope / usage-only frames are never success -------

    async def test_empty_envelope_and_usage_only_frames_fail(self):
        cases = [
            [_frame({}), _DONE],  # an empty object is not a chunk
            [_frame({"code": 123, "msg": "FAKE_SECRET"}), _DONE],  # business envelope
            [
                _frame(
                    {
                        "id": "c",
                        "object": "chat.completion.chunk",
                        "choices": [],
                        "usage": {
                            "prompt_tokens": 1,
                            "completion_tokens": 1,
                            "total_tokens": 2,
                        },
                    }
                ),
                _DONE,
            ],  # usage alone cannot fake success
        ]
        for frames in cases:
            self.up.queue_sse(frames)
            resp = await self._post_chat({}, stream=False)
            try:
                self.assertEqual(resp.status, 502)
                text = await resp.text()
            finally:
                await resp.release()
            self.assertNotIn("FAKE_SECRET", text)

    # -- 5. no [DONE]: every choice must finish --------------------------

    async def test_partial_choice_finish_without_done_fails(self):
        # Choice 0 finishes, choice 1 never does, and there is no [DONE].
        self.up.queue_sse(
            [
                _frame(_chunk({"content": "a"}, index=0)),
                _frame(_chunk({"content": "b"}, index=1)),
                _frame(_chunk({}, index=0, finish="stop")),
            ]
        )
        resp = await self._post_chat({}, stream=False)
        try:
            self.assertEqual(resp.status, 502)
        finally:
            await resp.release()

    # -- 6. tool fragment merging + incomplete arguments -----------------

    async def test_tool_fragments_merge_and_incomplete_json_fails(self):
        # name fragments concatenate, argument fragments are never dropped.
        self.up.queue_sse(
            [
                _frame(_chunk({"tool_calls": [
                    {"index": 0, "id": "c0", "type": "function",
                     "function": {"name": "do_"}}]})),
                _frame(_chunk({"tool_calls": [
                    {"index": 0, "function": {"name": "it"}}]})),
                _frame(_chunk({"tool_calls": [
                    {"index": 0, "function": {"arguments": '{"a":'}}]})),
                _frame(_chunk({"tool_calls": [
                    {"index": 0, "function": {"arguments": "1}"}}]})),
                _frame(_chunk({}, finish="tool_calls")),
                _DONE,
            ]
        )
        resp = await self._post_chat({}, stream=False)
        try:
            body = await resp.json()
        finally:
            await resp.release()
        self.assertEqual(resp.status, 200)
        call = body["choices"][0]["message"]["tool_calls"][0]
        self.assertEqual(call["id"], "c0")
        self.assertEqual(call["function"]["name"], "do_it")
        self.assertEqual(call["function"]["arguments"], '{"a":1}')

        # A single tool addressed by id may keep streaming without an index.
        self.up.queue_sse(
            [
                _frame(_chunk({"tool_calls": [
                    {"id": "c0", "type": "function", "function": {"name": "do"}}]})),
                _frame(_chunk({"tool_calls": [
                    {"function": {"name": "_it"}}]})),
                _frame(_chunk({"tool_calls": [
                    {"function": {"arguments": '{"x":1}'}}]})),
                _frame(_chunk({}, finish="tool_calls")),
                _DONE,
            ]
        )
        resp = await self._post_chat({}, stream=False)
        try:
            body = await resp.json()
        finally:
            await resp.release()
        self.assertEqual(resp.status, 200)
        fn = body["choices"][0]["message"]["tool_calls"][0]["function"]
        self.assertEqual(fn["name"], "do_it")
        self.assertEqual(fn["arguments"], '{"x":1}')

        # Truncated tool arguments must never become a fake complete tool_calls.
        self.up.queue_sse(
            [
                _frame(_chunk({"tool_calls": [
                    {"index": 0, "id": "c1",
                     "function": {"name": "f", "arguments": '{"a":'}}]})),
                _frame(_chunk({}, finish="tool_calls")),
                _DONE,
            ]
        )
        resp = await self._post_chat({}, stream=False)
        try:
            self.assertEqual(resp.status, 502)
        finally:
            await resp.release()

    async def test_ambiguous_tool_fragment_without_index_or_id_errors(self):
        # Two tools are known; a fragment with neither index nor id cannot be
        # placed and must fail instead of being appended to the nearest tool.
        self.up.queue_sse(
            [
                _frame(_chunk({"tool_calls": [
                    {"index": 0, "id": "c0", "function": {"name": "a"}}]})),
                _frame(_chunk({"tool_calls": [
                    {"index": 1, "id": "c1", "function": {"name": "b"}}]})),
                _frame(_chunk({"tool_calls": [
                    {"function": {"arguments": '{"z":1}'}}]})),
                _frame(_chunk({}, finish="tool_calls")),
                _DONE,
            ]
        )
        resp = await self._post_chat({}, stream=False)
        try:
            self.assertEqual(resp.status, 502)
        finally:
            await resp.release()

    # -- 7. upstream disconnect / timeout / bad UTF-8 --------------------

    async def test_upstream_disconnect_maps_to_json_502_and_error_event(self):
        # Before any frame: a real OpenAI error JSON with HTTP 502.
        self.up.queue_abort()
        resp = await self._post_chat({}, stream=False)
        try:
            self.assertEqual(resp.status, 502)
            body = await resp.json()
        finally:
            await resp.release()
        self.assertIn("error", body)

        # After the first frame: a fixed error event, never a fake [DONE], and
        # no automatic upstream replay.
        self.up.queue_abort([_frame(_chunk({"role": "assistant"}))])
        resp = await self._post_chat({}, stream=True)
        self.assertEqual(resp.status, 200)
        events, done = [], False
        with contextlib.suppress(aiohttp.ClientError):
            events, done = await self._read_sse(resp)
        await resp.release()
        self.assertFalse(done)
        self.assertTrue(any(isinstance(e, dict) and e.get("error") for e in events), events)
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 2)

    async def test_client_timeout_and_bad_utf8_return_502(self):
        # A genuine ClientTimeout before the first frame is an OpenAI JSON 502.
        tiny = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0.3))
        gw = None
        try:
            client = UpstreamClient(
                tiny, self.manager, chat_base=self.up.base,
                billing_base=self.up.base, web_base=self.up.base,
            )
            gw = Gateway(
                ChatService(client), client, api_key=API_KEY, host="127.0.0.1",
                port=0, concurrency=4, rpm=600, queue_size=8, queue_timeout=5,
            )
            await gw.start()
            self.up.queue({"kind": "sse", "chunks": _basic_sse("slow"),
                           "sleep_before": 1.5})
            url = "http://127.0.0.1:{}{}".format(gw.bound_port, COMPLETIONS_PATH)
            resp = await self.session.post(url, json=_payload(), headers=self._headers())
            try:
                self.assertEqual(resp.status, 502)
                text = await resp.text()
            finally:
                await resp.release()
            self.assertIn("error", text)
        finally:
            if gw is not None:
                with contextlib.suppress(Exception):
                    await gw.stop()
            await tiny.close()

        # Invalid UTF-8 anywhere in the stream is never a fake success.
        self.up.queue_sse([b"data: \xff\xfe\n\n", _DONE])
        resp = await self._post_chat({}, stream=False)
        try:
            self.assertEqual(resp.status, 502)
        finally:
            await resp.release()

    # -- 8. non-ASCII key + healthz --------------------------------------

    async def test_non_ascii_wrong_key_is_401_and_healthz_hides_account(self):
        # A wrong bearer containing a sendable Latin-1 char must be a clean 401,
        # not a 500 from hmac.compare_digest rejecting a non-ASCII str.
        async with self.session.post(
            self.base_url + COMPLETIONS_PATH,
            json=_payload(),
            headers={"Authorization": "Bearer \u00e9"},
        ) as resp:
            self.assertEqual(resp.status, 401)
            await resp.read()

        for path in (HEALTHZ_PATH, HEALTH_PATH):
            async with self.session.get(self.base_url + path) as resp:
                self.assertEqual(resp.status, 200)
                body = await resp.json()
                text = json.dumps(body)
                for secret in (UID, AT_OLD, RT_OLD, API_KEY):
                    self.assertNotIn(secret, text)


if __name__ == "__main__":
    unittest.main()
