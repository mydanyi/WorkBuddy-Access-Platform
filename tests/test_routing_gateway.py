"""Real-socket behaviour tests: the ordered model route **inside** the gateway.

Scope of this file (T2-red, write-failing-tests-first):

* the virtual model (:data:`core.routing.ORDERED_MODEL_ID`) only exists through
  the real ``Gateway`` -> ``ChatService`` -> ``UpstreamClient`` chain;
* the shared admission budget (``ConcurrencyGate``) is consumed once per *real*
  chat POST -- before the first attempt, again before a refresh resend and again
  before every standby attempt -- while a refused attempt consumes nothing and
  never becomes a 502;
* a routed candidate failure is silent for the ``on_error`` observer, while an
  ordinary chat failure still reports its fixed category (no global mute);
* the approved configurable request-body limit (default 16 MiB) is enforced by
  the gateway itself: byte-exact, chunk-aware, JSON 413 with no content leak.

Nothing of ours is doubled: the fake WorkBuddy upstream is a real ``aiohttp.web``
server bound to ``127.0.0.1:0``, and the assertions drive a real
``AuthManager`` / ``UpstreamClient`` / ``ChatService`` / ``Gateway`` /
``ModelRouter`` / ``ConcurrencyGate`` / ``StateStore`` over a real socket.

Portions of the interface are still missing on purpose (see the ticket): the
capability guard in :meth:`_GatewayCase._require_new_interfaces` turns such a
gap into one explicit ``AssertionError`` per test instead of a ``TypeError``
buried in a fixture.  Do not "fix" that here.

Every credential / token below is a FICTIONAL FIXED SAMPLE copied from the
contract suite.  Scratch data uses ``WORKBUDDY_TEST_TMP`` or the NAS-hosted
``tmp`` root; the system temp dir is never used.

Remote verification (manager): ``python -B -m unittest
tests.test_routing_gateway tests.test_routing -v``.  Local: NOT RUN.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import inspect
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
from astrbot_plugin_workbuddy.core.config import ConfigError, load_settings  # noqa: E402
from astrbot_plugin_workbuddy.core.credits import upstream_model_id  # noqa: E402
from astrbot_plugin_workbuddy.core.gateway import Gateway  # noqa: E402
from astrbot_plugin_workbuddy.core.limits import ConcurrencyGate, LimitExceeded  # noqa: E402
from astrbot_plugin_workbuddy.core.routing import ModelRouter, ORDERED_MODEL_ID  # noqa: E402
from astrbot_plugin_workbuddy.core.storage import CredentialStore, StateStore  # noqa: E402

# Reuse the contract suite's real fake-upstream fixtures -- but NOT its test case
# (subclassing ``_GatewayTestCase`` would re-run every contract test).
try:  # package discovery: ``python -m unittest tests.test_routing_gateway``.
    from tests.test_chat_gateway import (  # noqa: E402
        _FakeUpstream,
        _chunk,
        _frame,
        _DONE,
        API_KEY,
        AT_OLD,
        BASE_TMP,
        CHAT_PATH,
        COMPLETIONS_PATH,
        MASTER_KEY,
        MODEL,
        MODELS_PATH,
        SEED_CREDENTIALS,
    )
except ImportError:  # top-level discovery: ``python -m unittest discover -s tests``.
    from test_chat_gateway import (  # noqa: E402
        _FakeUpstream,
        _chunk,
        _frame,
        _DONE,
        API_KEY,
        AT_OLD,
        BASE_TMP,
        CHAT_PATH,
        COMPLETIONS_PATH,
        MASTER_KEY,
        MODEL,
        MODELS_PATH,
        SEED_CREDENTIALS,
    )

_UNSET = object()

_MIB = 1024 * 1024


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

def _signature_params(target) -> set:
    """Parameter names of ``target`` (``asynccontextmanager`` wrappers unwrap)."""
    try:
        return set(inspect.signature(target).parameters)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return set()


def _routing_section(models, **overrides) -> dict:
    """The validated ``routing`` settings group for an enabled route."""
    raw = {"enabled": True, "models": list(models)}
    raw.update(overrides)
    settings = load_settings({"routing": raw})
    if not isinstance(settings, dict) or "routing" not in settings:
        raise AssertionError("load_settings() did not return a 'routing' section")
    return settings["routing"]


def _sse_frames(model, text):
    """One complete SSE answer, tagged with the model that produced it."""
    return [
        _frame(_chunk({"role": "assistant"}, model=model)),
        _frame(_chunk({"content": text}, model=model)),
        _frame(_chunk({}, finish="stop", model=model)),
        _DONE,
    ]


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


def _body_of_exact_bytes(total, *, model=MODEL, prefix="") -> bytes:
    """A valid chat JSON body whose UTF-8 encoding is exactly ``total`` bytes."""
    payload = {
        "model": model,
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
    for start in range(0, len(body), size):
        yield body[start:start + size]
        await asyncio.sleep(0)


# --------------------------------------------------------------------------- #
# the fake upstream, scripted per model id
# --------------------------------------------------------------------------- #

class _ModelUpstream(_FakeUpstream):
    """Real aiohttp upstream that answers each *model* from its own program.

    ``sent_models`` records the pure model id of every real chat POST in order,
    which is how these tests observe what the real route actually asked for.
    """

    def __init__(self) -> None:
        super().__init__()
        # The inherited ``_FakeUpstream`` app keeps aiohttp's 1 MiB request-body
        # default, which would refuse the >1 MiB bodies the gateway's own limit
        # tests legitimately send.  Raise it for *this* fake upstream only, so
        # the 1.12 MiB body reaches the upstream instead of dying in the fixture;
        # the shared ``test_chat_gateway`` fixture, the test payloads and every
        # assertion stay untouched.
        self.app._client_max_size = 32 * _MIB
        self.by_model = {}
        self.sent_models = []

    # -- scripting -------------------------------------------------------

    def _queue(self, model, program) -> None:
        self.by_model.setdefault(model, collections.deque()).append(program)

    def queue_model_sse(self, model, text) -> None:
        self._queue(model, {"kind": "sse", "chunks": _sse_frames(model, text)})

    def queue_model_frames(self, model, chunks) -> None:
        self._queue(model, {"kind": "sse", "chunks": list(chunks)})

    def queue_model_status(self, model, status, body=None, headers=None) -> None:
        self._queue(
            model,
            {
                "kind": "status",
                "status": status,
                "body": body if body is not None else {},
                "headers": headers or {},
            },
        )

    def queue_model_abort(self, model, chunks=()) -> None:
        self._queue(model, {"kind": "abort", "chunks": list(chunks)})

    # -- routing ---------------------------------------------------------

    async def _chat(self, request):
        # ``_dispatch`` already read the body; aiohttp caches it, so this is cheap.
        raw = await request.read()
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            body = {}
        model = body.get("model") if isinstance(body, dict) else None
        self.sent_models.append(model)
        queue = self.by_model.get(model)
        program = queue.popleft() if queue else None
        if program is None:
            program = {
                "kind": "sse",
                "chunks": _sse_frames(model, "answer from {}".format(model)),
            }
        kind = program.get("kind")
        if kind == "status":
            return web.json_response(
                program.get("body") or {},
                status=program["status"],
                headers=program.get("headers") or {},
            )
        if kind == "abort":
            # Frames first, then an abrupt drop: no SSE terminator, ever.
            resp = web.StreamResponse(
                status=200, headers={"Content-Type": "text/event-stream"}
            )
            await resp.prepare(request)
            for chunk in program.get("chunks") or []:
                await resp.write(chunk)
            await asyncio.sleep(0.05)
            raise RuntimeError("simulated upstream disconnect")
        resp = web.StreamResponse(
            status=200, headers={"Content-Type": "text/event-stream"}
        )
        await resp.prepare(request)
        for chunk in program.get("chunks") or []:
            await resp.write(chunk)
            await asyncio.sleep(0)
        with contextlib.suppress(Exception):
            await resp.write_eof()
        return resp


# --------------------------------------------------------------------------- #
# shared fixture
# --------------------------------------------------------------------------- #

class _GatewayCase(unittest.IsolatedAsyncioTestCase):
    """Real gateway + real service stack in front of the scripted upstream."""

    MODELS = ("wb-alpha", "wb-beta", "wb-gamma")

    # -- capability guard ------------------------------------------------

    def _require_new_interfaces(self) -> None:
        """Fail once, clearly, when a decided interface is still missing."""
        gateway_params = _signature_params(Gateway.__init__)
        self.assertIn(
            "router",
            gateway_params,
            "Gateway(...) must accept an optional router= keyword",
        )
        self.assertIn(
            "max_request_body_mb",
            gateway_params,
            "Gateway(...) must accept max_request_body_mb= (approved default 16)",
        )
        self.assertIn(
            "observe_errors",
            _signature_params(ChatService.stream),
            "ChatService.stream must accept observe_errors=",
        )
        chat_stream_params = _signature_params(UpstreamClient.chat_stream)
        for name in ("before_post", "observe_errors"):
            self.assertIn(
                name,
                chat_stream_params,
                "UpstreamClient.chat_stream must accept {}=".format(name),
            )
        for name in ("acquire", "slot"):
            self.assertIn(
                "count_rpm",
                _signature_params(getattr(ConcurrencyGate, name)),
                "ConcurrencyGate.{} must accept count_rpm=".format(name),
            )
        self.assertTrue(
            asyncio.iscoroutinefunction(getattr(ConcurrencyGate, "consume_rpm", None)),
            "ConcurrencyGate.consume_rpm must be an async method",
        )

    # -- lifecycle -------------------------------------------------------

    async def asyncSetUp(self) -> None:
        self._require_new_interfaces()
        BASE_TMP.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(tempfile.mkdtemp(prefix="wb-rtgw-", dir=str(BASE_TMP)))
        self.up = _ModelUpstream()
        await self.up.start()
        self.session = aiohttp.ClientSession()
        self.store = CredentialStore(self.data_dir, MASTER_KEY)
        self.store.save(dict(SEED_CREDENTIALS))
        self.manager = AuthManager(
            self.session, self.store, chat_base=self.up.base, poll_interval=0.01
        )
        self.observed = []
        self.client = UpstreamClient(
            self.session,
            self.manager,
            chat_base=self.up.base,
            billing_base=self.up.base,
            web_base=self.up.base,
            on_error=self._observe,
        )
        self.chat = ChatService(self.client)
        self.state = StateStore(self.data_dir)
        self.router = ModelRouter(_routing_section(self.MODELS), self.state)
        self.gateway = None
        await self.start_gateway()

    async def asyncTearDown(self) -> None:
        if self.gateway is not None:
            with contextlib.suppress(Exception):
                await self.gateway.stop()
        await self.session.close()
        await self.up.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)

    # -- fixtures --------------------------------------------------------

    def _observe(self, category) -> None:
        """The real ``on_error`` string observer (sync callable)."""
        self.observed.append(category)

    async def start_gateway(self, *, router=_UNSET, **overrides) -> None:
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
        if router is _UNSET:
            router = self.router
        self.gateway = Gateway(self.chat, self.client, router=router, **params)
        await self.gateway.start()
        self.base_url = "http://127.0.0.1:{}".format(self.gateway.bound_port)

    async def restart_gateway(self, **overrides) -> None:
        await self.gateway.stop()
        await self.start_gateway(**overrides)

    def headers(self, key=API_KEY) -> dict:
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

    async def post_raw(self, body, *, key=API_KEY, chunked=False):
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


# --------------------------------------------------------------------------- #
# 1..12 -- routing behaviour through the gateway
# --------------------------------------------------------------------------- #

class RoutingGatewayTests(_GatewayCase):
    """The virtual model end to end: models list, failover, budget, observer."""

    # -- 1. catalog ------------------------------------------------------

    async def test_models_listing_appends_the_virtual_entry_only_when_enabled(self):
        async with self.session.get(
            self.base_url + MODELS_PATH, headers=self.headers()
        ) as resp:
            self.assertEqual(resp.status, 200)
            body = await resp.json()

        data = body.get("data")
        self.assertIsInstance(data, list)
        self.assertTrue(data, "the live upstream catalog must still be served")

        real = [
            item
            for item in data
            if isinstance(item, dict) and item.get("upstream_id") == MODEL
        ]
        self.assertEqual(len(real), 1, "the real entry must keep its upstream_id")
        entry = real[0]
        self.assertEqual(
            entry.get("id"),
            "{} [倍率未知]".format(MODEL),
            "an existing model keeps its multiplier suffix",
        )
        for key in ("credits", "effective_credits", "object", "owned_by"):
            self.assertIn(key, entry, "the existing /v1/models fields must survive")

        ids = [item.get("id") for item in data if isinstance(item, dict)]
        self.assertEqual(ids.count(ORDERED_MODEL_ID), 1, "exactly one virtual entry")
        self.assertEqual(
            ids[-1],
            ORDERED_MODEL_ID,
            "the virtual entry is appended after the real models",
        )
        self.assertEqual(ORDERED_MODEL_ID, "按顺序调用")

        await self.restart_gateway(router=None)
        async with self.session.get(
            self.base_url + MODELS_PATH, headers=self.headers()
        ) as resp:
            self.assertEqual(resp.status, 200)
            body = await resp.json()
        ids = [item.get("id") for item in body.get("data") or []]
        self.assertNotIn(
            ORDERED_MODEL_ID, ids, "a disabled route must not advertise the virtual model"
        )
        self.assertIn("{} [倍率未知]".format(MODEL), ids)

    # -- 2. non-stream failover ------------------------------------------

    async def test_json_virtual_request_fails_over_and_keeps_the_payload_verbatim(self):
        self.up.queue_model_status("wb-alpha", 429, {"code": 4290, "msg": "slow"})
        self.up.queue_model_sse("wb-beta", "beta answer")
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
            self.up.sent_models,
            ["wb-alpha", "wb-beta"],
            "the upstream may only see the pure candidate ids",
        )
        self.assertEqual(body.get("model"), "wb-beta", "the answer belongs to the standby")
        self.assertEqual(body["choices"][0]["message"]["content"], "beta answer")

        sent = self.up.chat_bodies()
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
                set(call),
                expected_keys,
                "no extra metadata may be added for the upstream",
            )
            self.assertEqual(call["messages"], messages)
            self.assertEqual(call["tools"], tools)
            self.assertEqual(call["tool_choice"], tool_choice)
            self.assertEqual(call["temperature"], 0.25)
            self.assertIs(call["stream"], True)
            self.assertEqual(call["model"], self.up.sent_models[index])
            self.assertNotIn(ORDERED_MODEL_ID, json.dumps(call, ensure_ascii=False))

    # -- 3. stream failover ----------------------------------------------

    async def test_sse_virtual_request_fails_over_and_ends_with_done(self):
        self.up.queue_model_status("wb-alpha", 503, {"code": 5030})
        self.up.queue_model_sse("wb-beta", "beta stream")

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
        self.assertEqual(self.up.sent_models, ["wb-alpha", "wb-beta"])

    # -- 4. a committed stream is never replayed -------------------------

    async def test_stream_break_after_a_partial_frame_never_splices_two_models(self):
        self.up.queue_model_abort(
            "wb-alpha",
            [
                _frame(_chunk({"role": "assistant"}, model="wb-alpha")),
                _frame(
                    _chunk(
                        {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {"name": "ping", "arguments": "{}"},
                                }
                            ]
                        },
                        model="wb-alpha",
                    )
                ),
            ],
        )
        self.up.queue_model_sse("wb-beta", "beta answer")

        resp = await self.post_chat(self.chat_payload(stream=True))
        self.assertEqual(resp.status, 200)
        events, done = [], False
        try:
            with contextlib.suppress(aiohttp.ClientError):
                events, done, _raw = await _read_sse(resp)
        finally:
            await resp.release()

        self.assertEqual(
            self.up.sent_models,
            ["wb-alpha"],
            "once a frame was emitted the stream belongs to that model",
        )
        self.assertNotIn("wb-beta", self.up.sent_models)
        self.assertFalse(done, "a broken stream must never get a fabricated [DONE]")
        text = json.dumps(events, ensure_ascii=False)
        self.assertNotIn("beta answer", text, "two models' output must not be spliced")
        self.assertTrue(
            any(isinstance(event, dict) and event.get("error") for event in events),
            events,
        )

    # -- 5. real 429 handling --------------------------------------------

    async def test_two_real_429s_stop_before_the_third_candidate(self):
        for model in ("wb-alpha", "wb-beta"):
            self.up.queue_model_status(model, 429, {"code": 4290}, {"Retry-After": "9"})

        resp = await self.post_chat(self.chat_payload())
        try:
            self.assertEqual(
                resp.status, 429, "an exhausted route is a local 429, never a 502"
            )
            retry_after = resp.headers.get("Retry-After")
            body = await resp.json()
        finally:
            await resp.release()

        self.assertIsNotNone(retry_after, "the route must keep the upstream Retry-After")
        self.assertEqual(body["error"]["type"], "rate_limit_error")
        self.assertEqual(self.up.sent_models, ["wb-alpha", "wb-beta"])
        self.assertNotIn(
            "wb-gamma",
            self.up.sent_models,
            "the second real 429 cools the whole route instead of walking on",
        )

    async def test_account_scope_429_tries_only_the_primary(self):
        self.up.queue_model_status(
            "wb-alpha", 429, {"scope": "account"}, {"Retry-After": "30"}
        )
        self.up.queue_model_sse("wb-beta", "beta answer")

        resp = await self.post_chat(self.chat_payload())
        try:
            self.assertEqual(resp.status, 429)
            await resp.read()
        finally:
            await resp.release()

        self.assertEqual(
            self.up.sent_models,
            ["wb-alpha"],
            "an account-scoped limit must not be handed to the standby model",
        )

    async def test_retry_after_is_kept_as_the_standby_cooldown(self):
        self.up.queue_model_status("wb-alpha", 429, {"code": 4290}, {"Retry-After": "7"})
        self.up.queue_model_sse("wb-beta", "beta answer")

        resp = await self.post_chat(self.chat_payload())
        try:
            self.assertEqual(resp.status, 200)
            body = await resp.json()
        finally:
            await resp.release()
        self.assertEqual(body["choices"][0]["message"]["content"], "beta answer")

        resp = await self.post_chat(self.chat_payload())
        try:
            self.assertEqual(resp.status, 200)
            await resp.read()
        finally:
            await resp.release()
        self.assertEqual(
            self.up.sent_models,
            ["wb-alpha", "wb-beta", "wb-beta"],
            "the 7s Retry-After must keep the primary cooling on the next round",
        )

    # -- 6. a concrete model is never routed -----------------------------

    async def test_concrete_model_never_borrows_a_standby(self):
        self.up.queue_model_status("wb-alpha", 429, {"code": 4290})
        self.up.queue_model_sse("wb-beta", "beta answer")

        resp = await self.post_chat(self.chat_payload(model="wb-alpha"))
        try:
            self.assertEqual(resp.status, 429)
            await resp.read()
        finally:
            await resp.release()

        self.assertEqual(
            self.up.sent_models,
            ["wb-alpha"],
            "a request for a concrete model is forwarded verbatim and never routed",
        )

    # -- 7. the virtual id without a route -------------------------------

    async def test_virtual_model_with_routing_disabled_is_a_fixed_local_4xx(self):
        await self.restart_gateway(router=None)

        resp = await self.post_chat(self.chat_payload())
        try:
            self.assertEqual(
                resp.status,
                400,
                "a virtual model with routing disabled must be a fixed local 400",
            )
            await resp.read()
        finally:
            await resp.release()

        self.assertEqual(
            self.up.hits[("POST", CHAT_PATH)], 0, "no upstream request may be made"
        )
        encoded = ORDERED_MODEL_ID.encode("utf-8")
        for call in self.up.calls:
            self.assertNotIn(
                encoded, call["body"], "the Chinese virtual id must never reach the upstream"
            )

    # -- 7b. the virtual id's own display alias, still without a route ---

    async def test_virtual_display_alias_with_routing_disabled_is_the_same_fixed_local_4xx(
        self,
    ):
        """``"按顺序调用 [倍率未知]"`` is the virtual id, not a concrete model.

        The alias is this plugin's own standard suffix (``credits.upstream_model_id``
        strips exactly that shape), so it normalizes to the pure virtual id and
        must land on the *same* fixed local ``400 invalid_request_error``: no
        upstream chat POST, no 5xx from a missing/disabled router, and no routing
        round recorded.  Both route states are covered by one real HTTP request
        each -- ``router=None`` and a real router whose route is ``enabled=False``.
        """
        alias = "{} [倍率未知]".format(ORDERED_MODEL_ID)
        self.assertNotEqual(alias, ORDERED_MODEL_ID, "the alias must differ textually")
        self.assertEqual(
            upstream_model_id(alias),
            ORDERED_MODEL_ID,
            "the '[倍率未知]' suffix is the standard alias, so the gateway's own "
            "normalization reaches the pure virtual id",
        )

        disabled_router = ModelRouter(
            _routing_section(self.MODELS, enabled=False), self.state
        )
        cases = (
            ("router=None", None, self.router),
            ("router enabled=False", disabled_router, disabled_router),
        )
        for label, router, watched in cases:
            with self.subTest(routing=label):
                await self.restart_gateway(router=router)
                before = self.up.hits[("POST", CHAT_PATH)]

                resp = await self.post_chat(self.chat_payload(model=alias))
                try:
                    self.assertEqual(
                        resp.status,
                        400,
                        "the virtual id's own alias is the same fixed local 400 as "
                        "the pure id, never a 5xx",
                    )
                    self.assertIn(
                        "application/json",
                        resp.headers.get("Content-Type", ""),
                        "the refusal stays an OpenAI-compatible JSON error",
                    )
                    body = await resp.json()
                finally:
                    await resp.release()

                error = (body or {}).get("error") or {}
                self.assertEqual(
                    error.get("type"),
                    "invalid_request_error",
                    "a disabled-route refusal is a request-validation error",
                )
                self.assertEqual(
                    self.up.hits[("POST", CHAT_PATH)],
                    before,
                    "no upstream chat request may be made for the alias",
                )
                self.assertIsNone(
                    watched.status().get("last_result"),
                    "a refused alias must not record a routing round",
                )

    # -- 8. the rpm budget bounds the failover ---------------------------

    async def test_single_rpm_budget_stops_the_standby_attempt(self):
        await self.restart_gateway(concurrency=1, queue_size=2, rpm=1)
        self.up.queue_model_status("wb-alpha", 429, {"code": 4290})
        self.up.queue_model_sse("wb-beta", "beta answer")
        self.up.queue_model_sse("wb-gamma", "gamma answer")

        resp = await self.post_chat(self.chat_payload())
        try:
            self.assertEqual(
                resp.status,
                429,
                "an exhausted budget is a local 429 for the client, never a 502",
            )
            body = await resp.json()
        finally:
            await resp.release()

        self.assertEqual(body["error"]["type"], "rate_limit_error")
        self.assertEqual(
            self.up.sent_models,
            ["wb-alpha"],
            "the standby attempt must not exceed the chat rpm budget",
        )
        self.assertNotIn("wb-gamma", self.up.sent_models)

    # -- 9/10. the budget also bounds a refresh resend -------------------

    async def test_refresh_resend_is_refused_when_the_budget_is_spent(self):
        await self.restart_gateway(concurrency=1, queue_size=2, rpm=1)
        self.up.queue_model_status("wb-alpha", 401, {"code": 1001, "msg": "expired"})
        self.up.queue_model_sse("wb-alpha", "never streamed")
        self.up.queue_model_sse("wb-beta", "beta answer")

        resp = await self.post_chat(self.chat_payload())
        try:
            self.assertEqual(
                resp.status,
                429,
                "the refused resend is a local 429, not an upstream transport error",
            )
            await resp.read()
        finally:
            await resp.release()

        self.assertEqual(
            self.up.hits[("POST", CHAT_PATH)],
            1,
            "only the first chat POST may be sent when the budget is 1",
        )
        self.assertEqual(
            self.up.refresh_hits, 1, "the token refresh itself is not a chat rpm"
        )
        self.assertEqual(self.up.sent_models, ["wb-alpha"])

    async def test_two_rpm_allows_the_refresh_resend_then_blocks_the_next_request(self):
        await self.restart_gateway(concurrency=1, queue_size=2, rpm=2)
        self.up.queue_model_status("wb-alpha", 401, {"code": 1001, "msg": "expired"})
        self.up.queue_model_sse("wb-alpha", "after refresh")

        resp = await self.post_chat(self.chat_payload())
        try:
            self.assertEqual(resp.status, 200)
            body = await resp.json()
        finally:
            await resp.release()
        self.assertEqual(body["choices"][0]["message"]["content"], "after refresh")
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 2)
        self.assertEqual(self.up.refresh_hits, 1)

        before = self.up.hits[("POST", CHAT_PATH)]
        resp = await self.post_chat(self.chat_payload(model=MODEL))
        try:
            self.assertEqual(
                resp.status, 429, "both permits are spent, so the next request is refused"
            )
            await resp.read()
        finally:
            await resp.release()
        self.assertEqual(
            self.up.hits[("POST", CHAT_PATH)],
            before,
            "a refused request must never reach the upstream",
        )

    # -- 11. one slot serves the whole round; growth shares the budget ---

    async def test_single_concurrency_round_fails_over_without_deadlock(self):
        await self.restart_gateway(concurrency=1, queue_size=1, rpm=2)
        self.up.queue_model_status("wb-alpha", 429, {"code": 4290})
        self.up.queue_model_sse("wb-beta", "beta answer")

        resp = await asyncio.wait_for(self.post_chat(self.chat_payload()), 10.0)
        try:
            self.assertEqual(
                resp.status, 200, "the standby attempt runs inside the already held slot"
            )
            body = await resp.json()
        finally:
            await resp.release()

        self.assertEqual(body["choices"][0]["message"]["content"], "beta answer")
        self.assertEqual(self.up.sent_models, ["wb-alpha", "wb-beta"])
        self.assertEqual(self.gateway.limits.active, 0, "the round must release its slot")
        self.assertEqual(self.gateway.limits.queued, 0)

        # Both permits were consumed by the round: a direct growth call is now
        # refused on the *same* gate, so gateway and growth chat share a budget.
        with self.assertRaises(LimitExceeded):
            await self.chat.complete(
                {"model": MODEL, "messages": [{"role": "user", "content": "hi"}]}
            )

    # -- 12. candidate failures are silent, ordinary ones are not --------

    async def test_candidate_failures_are_not_announced_but_normal_429s_are(self):
        self.up.queue_model_status("wb-alpha", 429, {"code": 4290})
        self.up.queue_model_sse("wb-beta", "beta answer")

        resp = await self.post_chat(self.chat_payload())
        try:
            self.assertEqual(resp.status, 200)
            await resp.read()
        finally:
            await resp.release()
        self.assertEqual(
            self.observed,
            [],
            "an absorbed candidate failure must not be reported as an error",
        )

        self.up.queue_model_status(MODEL, 429, {"code": 4290})
        resp = await self.post_chat(self.chat_payload(model=MODEL))
        try:
            self.assertEqual(resp.status, 429)
            await resp.read()
        finally:
            await resp.release()
        self.assertIn(
            "rate",
            self.observed,
            "an ordinary chat failure must still reach the observer (no global mute)",
        )


# --------------------------------------------------------------------------- #
# approved request-body limit (default 16 MiB, configurable)
# --------------------------------------------------------------------------- #

class RequestBodyLimitTests(_GatewayCase):
    """The gateway itself enforces the configured body limit, byte-exactly."""

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

        sent = self.up.chat_bodies()
        self.assertEqual(len(sent), 1)
        self.assertGreater(
            len(sent[0]["messages"][0]["content"].encode("utf-8")),
            _MIB,
            "the whole body must reach the upstream",
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

        # Multibyte, with Content-Length: bytes are what count.
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

        # Chunked (no Content-Length at all): the streamed byte count decides.
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
        for leak in (marker, API_KEY, AT_OLD):
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
        self.up.queue_model_sse("wb-alpha", "alpha answer")

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
            self.up.hits[("POST", CHAT_PATH)],
            0,
            "a 413 must never touch the upstream",
        )
        self.assertEqual(self.up.sent_models, [])
        self.assertIsNone(
            self.router.status().get("last_result"),
            "a 413 must not consume a candidate or record a round",
        )


class GatewayBodyLimitConfigTests(unittest.TestCase):
    """The approved setting: ``gateway.max_request_body_mb``, default 16."""

    def test_gateway_settings_expose_the_sixteen_mib_default(self):
        gateway = (load_settings({}).get("gateway") or {})
        self.assertIn(
            "max_request_body_mb",
            gateway,
            "load_settings()['gateway'] must carry the body-limit setting",
        )
        self.assertEqual(
            gateway["max_request_body_mb"], 16, "the approved default is 16 MiB"
        )

        custom = (load_settings({"gateway": {"max_request_body_mb": 3}}).get("gateway") or {})
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


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
