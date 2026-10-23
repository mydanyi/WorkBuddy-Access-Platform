"""OpenAI-compatible chat handling in front of the WorkBuddy upstream.

``ChatService`` opens one upstream ``/v2/chat/completions`` call through
:meth:`UpstreamClient.chat_stream` (which owns the exactly-once 401 refresh) and
turns the response into a stream of OpenAI chunk dicts.

Pinned from ``internal/upstream/sse.go``:

* the upstream is always asked with ``stream: true``;
* half lines, split UTF-8, CRLF and mixed ``\\r``/``\\n`` endings are reassembled;
  ``:``-comments are ignored, ``data:`` without a space is accepted, and several
  ``data:`` lines join with ``\\n``;
* ``id``/``object``/``created``/``model``/``usage`` and per-choice
  ``index``/``delta``/``finish_reason`` (with ``reasoning_content`` and streamed
  ``tool_calls``) survive untouched;
* an empty stream, a ``[DONE]``-only stream, malformed JSON, an ``error`` event
  or an EOF without a terminator is a failure, never a synthesized success;
* a plain JSON ``chat.completion`` is accepted and converted to chunks.

No persona surgery, no tool execution, no extra network tools: the payload is
forwarded with only ``stream`` forced and the transport headers assembled from
``internal/upstream/headers.go``.
"""

from __future__ import annotations

import asyncio
import codecs
import contextlib
import json

import aiohttp

from .auth import parse_retry_after
from .errors import ChatStreamError, UpstreamError
from .limits import ConcurrencyGate


# --------------------------------------------------------------------------- #
# SSE decoding
# --------------------------------------------------------------------------- #

class _SSEReader:
    """Incremental SSE line decoder.

    Bytes are decoded with a stateful UTF-8 decoder so a multibyte codepoint
    split across network chunks is reassembled.  Complete lines are split on
    ``\\n``, ``\\r`` or ``\\r\\n``; a trailing ``\\r`` is held back until the next
    byte (it may start a CRLF).
    """

    def __init__(self) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")("strict")
        self._pending = ""
        self._data_lines = []

    def _decode(self, chunk, final=False):
        try:
            return self._decoder.decode(chunk, final)
        except UnicodeDecodeError:
            raise ChatStreamError("upstream stream was not valid UTF-8") from None

    def feed(self, chunk: bytes):
        self._pending += self._decode(chunk)
        return self._drain()

    def flush(self):
        self._pending += self._decode(b"", final=True)
        events = self._drain()
        if self._pending:
            event = self._consume_line(self._pending)
            self._pending = ""
            if event is not None:
                events.append(event)
        if self._data_lines:
            events.append("\n".join(self._data_lines))
            self._data_lines = []
        return events

    def _drain(self):
        events = []
        while True:
            line, rest = _split_line(self._pending)
            if line is None:
                break
            self._pending = rest
            event = self._consume_line(line)
            if event is not None:
                events.append(event)
        return events

    def _consume_line(self, line):
        if line == "":
            # Blank line flushes the event assembled from the ``data:`` lines.
            if self._data_lines:
                event = "\n".join(self._data_lines)
                self._data_lines = []
                return event
            return None
        if line.startswith(":"):
            return None  # comment / keepalive
        if line.startswith("data:"):
            value = line[5:]
            if value.startswith(" "):
                value = value[1:]
            self._data_lines.append(value)
        # ``event:``/``id:``/``retry:`` fields are not part of this contract.
        return None


def _split_line(text):
    """Split off the first complete line; return ``(None, text)`` to wait."""
    n = len(text)
    i = 0
    while True:
        r = text.find("\r", i)
        l = text.find("\n", i)
        if r == -1 and l == -1:
            return None, text
        if r == -1 or (l != -1 and l < r):
            return text[:l], text[l + 1:]
        if r + 1 >= n:
            # Trailing CR: could be the first half of a CRLF pair.
            return None, text
        if text[r + 1] == "\n":
            return text[:r], text[r + 2:]
        return text[:r], text[r + 1:]


# --------------------------------------------------------------------------- #
# payload handling
# --------------------------------------------------------------------------- #

def _build_upstream_body(payload) -> dict:
    """Copy the client payload and force ``stream: true``.

    Every other field (``messages``/``tools``/``tool_choice``/``n``/...) is kept
    verbatim: no persona splitting and no content rewriting.
    """
    body = dict(payload) if isinstance(payload, dict) else {}
    body["stream"] = True
    return body


def _choice_index(choice) -> int:
    index = choice.get("index")
    if not isinstance(index, int) or isinstance(index, bool):
        return 0
    return index


def _track_choices(obj, seen, finished) -> bool:
    """Record seen / finished choice indices.

    Returns ``True`` when the frame carried at least one *choice* object; a
    frame whose ``choices`` is empty (metadata / usage) is not a real choice and
    must never be enough to fake a completed stream.
    """
    choices = obj.get("choices")
    if not isinstance(choices, list) or not choices:
        return False
    present = False
    for choice in choices:
        if not isinstance(choice, dict):
            continue
        index = _choice_index(choice)
        seen.add(index)
        present = True
        if choice.get("finish_reason"):
            finished.add(index)
    return present


def _decode_event(payload: str):
    """Return ``(obj, is_done)``; raise on malformed / error frames.

    A frame that is not a JSON object, carries an ``error`` key or has no
    ``choices`` list (an empty ``{}`` or a ``{"code": .., "msg": ..}`` business
    envelope) is never a valid chat chunk.  The upstream ``msg`` is never
    surfaced, so the raised error stays desensitized.
    """
    if payload == "[DONE]":
        return None, True
    try:
        obj = json.loads(payload)
    except Exception:
        raise ChatStreamError("upstream sent malformed SSE JSON") from None
    if not isinstance(obj, dict):
        raise ChatStreamError("upstream sent a non-object SSE frame")
    if "error" in obj:
        raise ChatStreamError("upstream reported a stream error")
    if not isinstance(obj.get("choices"), list):
        raise ChatStreamError("upstream sent an unrecognized SSE frame")
    return obj, False


async def _iter_sse(resp):
    """Yield chunk dicts from an SSE response, enforcing the terminator contract.

    ``[DONE]`` ends the stream *immediately* -- the upstream is closed by the
    caller's context manager and any trailing bytes are ignored (never read).
    A ``[DONE]`` that carried no real choice is an empty stream.  An EOF without
    ``[DONE]`` is only valid once *every* seen choice index reported an explicit
    ``finish_reason``.
    """
    reader = _SSEReader()
    saw_data = False
    seen = set()
    finished = set()
    tools = {}
    source = resp.content.iter_any()
    try:
        async for raw in source:
            for payload in reader.feed(raw):
                obj, is_done = _decode_event(payload)
                if is_done:
                    if not saw_data:
                        raise ChatStreamError("upstream returned an empty stream")
                    _validate_all_tool_arguments(tools)
                    return
                _track_tools(obj, tools)
                if _track_choices(obj, seen, finished):
                    saw_data = True
                yield obj
        for payload in reader.flush():
            obj, is_done = _decode_event(payload)
            if is_done:
                if not saw_data:
                    raise ChatStreamError("upstream returned an empty stream")
                _validate_all_tool_arguments(tools)
                return
            _track_tools(obj, tools)
            if _track_choices(obj, seen, finished):
                saw_data = True
            yield obj
    finally:
        with contextlib.suppress(Exception):
            await source.aclose()
    if not saw_data:
        raise ChatStreamError("upstream returned an empty stream")
    if not seen or not seen <= finished:
        raise ChatStreamError("upstream stream ended without a terminator")


async def _read_error_details(resp):
    """Read a non-200 chat error body *once*: ``(code, limit_scope)``.

    The business ``code`` is passed through untouched (no guessing at its
    semantics).  ``limit_scope`` is only taken from an explicit ``root.scope`` /
    ``error.scope`` string; anything else is ``None`` and is normalised again by
    :class:`UpstreamError`.  The body itself is never stored on the exception.
    """
    try:
        body = await resp.json(content_type=None)
    except Exception:
        return None, None
    if not isinstance(body, dict):
        return None, None
    scope = body.get("scope")
    if not isinstance(scope, str):
        error = body.get("error")
        scope = error.get("scope") if isinstance(error, dict) else None
    if not isinstance(scope, str):
        scope = None
    return body.get("code"), scope


async def _read_json_body(resp):
    try:
        return await resp.json(content_type=None)
    except Exception:
        raise ChatStreamError("upstream returned a malformed JSON completion") from None


def _completion_to_chunks(obj):
    """Turn a plain ``chat.completion`` into OpenAI stream chunks."""
    if not isinstance(obj, dict):
        raise ChatStreamError("upstream returned a non-object completion")
    choices = obj.get("choices") or []
    if not choices:
        raise ChatStreamError("upstream completion carried no choices")
    base = {
        "id": obj.get("id") or "chatcmpl-wb",
        "object": "chat.completion.chunk",
        "created": obj.get("created"),
        "model": obj.get("model"),
    }
    for choice in choices:
        if not isinstance(choice, dict):
            continue
        message = choice.get("message") or {}
        delta = {}
        for src, dst in (
            ("role", "role"),
            ("content", "content"),
            ("reasoning_content", "reasoning_content"),
            ("tool_calls", "tool_calls"),
        ):
            value = message.get(src)
            if value not in (None, "", []):
                delta[dst] = value
        chunk = dict(base)
        chunk["choices"] = [
            {
                "index": choice.get("index", 0),
                "delta": delta,
                "finish_reason": choice.get("finish_reason"),
            }
        ]
        yield chunk
    if obj.get("usage"):
        usage_chunk = dict(base)
        usage_chunk["choices"] = []
        usage_chunk["usage"] = obj["usage"]
        yield usage_chunk


# --------------------------------------------------------------------------- #
# non-stream aggregation
# --------------------------------------------------------------------------- #

def _new_choice(index):
    return {
        "index": index,
        "role": "assistant",
        "content": "",
        "reasoning": "",
        "tools": {},
        "tool_order": [],
        "id_index": {},
        "finish": None,
    }


def _new_tool_state():
    return {"tools": {}, "tool_order": [], "id_index": {}}


def _tool_slot(index):
    return {
        "index": index,
        "id": "",
        "type": "function",
        "function": {"name": "", "arguments": ""},
    }


def _next_tool_index(state) -> int:
    candidate = 0
    while candidate in state["tools"]:
        candidate += 1
    return candidate


def _allocate_tool_slot(state):
    index = _next_tool_index(state)
    slot = _tool_slot(index)
    state["tools"][index] = slot
    state["tool_order"].append(index)
    return slot


def _merge_tool_calls(state, calls) -> None:
    """Merge streamed ``tool_calls`` fragments.

    Fragments are placed by ``index`` first, then by ``id``; a fragment with
    neither is only placeable while a single tool is known (the common
    single-tool stream).  Once two tools exist an unplaceable fragment is
    ambiguous and raises :class:`ChatStreamError` rather than guessing.
    ``name`` and ``arguments`` fragments concatenate.
    """
    for call in calls:
        if not isinstance(call, dict):
            continue
        index = call.get("index")
        call_id = call.get("id")
        slot = None
        if isinstance(index, int) and not isinstance(index, bool):
            slot = state["tools"].get(index)
            if slot is None:
                slot = _tool_slot(index)
                state["tools"][index] = slot
                state["tool_order"].append(index)
        elif call_id:
            known = state["id_index"].get(call_id)
            if known is not None:
                slot = state["tools"][known]
            else:
                slot = _allocate_tool_slot(state)
        elif len(state["tool_order"]) == 1:
            slot = state["tools"][state["tool_order"][0]]
        elif not state["tool_order"]:
            slot = _allocate_tool_slot(state)
        else:
            raise ChatStreamError(
                "upstream tool call fragment has neither index nor id"
            )

        if call_id:
            slot["id"] = call_id
            state["id_index"][call_id] = slot["index"]
        if call.get("type"):
            slot["type"] = call["type"]
        function = call.get("function")
        if isinstance(function, dict):
            name = function.get("name")
            if name:
                slot["function"]["name"] += name
            arguments = function.get("arguments")
            if arguments:
                slot["function"]["arguments"] += arguments


def _validate_tool_arguments(state) -> None:
    """A finished choice must not expose truncated (fake complete) tool args."""
    for tool_index in state["tool_order"]:
        arguments = state["tools"][tool_index]["function"]["arguments"]
        if not arguments:
            continue
        try:
            json.loads(arguments)
        except Exception:
            raise ChatStreamError(
                "upstream tool call arguments were not complete JSON"
            ) from None


def _validate_all_tool_arguments(tools) -> None:
    for state in tools.values():
        _validate_tool_arguments(state)


def _track_tools(obj, tools) -> None:
    """Merge this frame's tool_call fragments and validate a just-finished choice."""
    choices = obj.get("choices")
    if not isinstance(choices, list):
        return
    for choice in choices:
        if not isinstance(choice, dict):
            continue
        index = _choice_index(choice)
        delta = choice.get("delta")
        if not isinstance(delta, dict):
            delta = choice.get("message")
        state = tools.get(index)
        if isinstance(delta, dict) and isinstance(delta.get("tool_calls"), list):
            if state is None:
                state = _new_tool_state()
                tools[index] = state
            _merge_tool_calls(state, delta["tool_calls"])
        if choice.get("finish_reason") and state is not None:
            _validate_tool_arguments(state)


def _merge_message(state, message) -> None:
    if not isinstance(message, dict):
        return
    if message.get("role"):
        state["role"] = message["role"]
    if message.get("content"):
        state["content"] += message["content"]
    if message.get("reasoning_content"):
        state["reasoning"] += message["reasoning_content"]
    if isinstance(message.get("tool_calls"), list):
        _merge_tool_calls(state, message["tool_calls"])


def aggregate_chunks(chunks, payload) -> dict:
    """Aggregate streamed chunks into one ``chat.completion``, by choice index."""
    meta = {"id": None, "created": None, "model": None}
    choices = {}
    order = []
    usage = None
    for chunk in chunks:
        if not isinstance(chunk, dict):
            continue
        for key in ("id", "created", "model"):
            if meta.get(key) in (None, "") and chunk.get(key) not in (None, ""):
                meta[key] = chunk[key]
        if chunk.get("usage"):
            usage = chunk["usage"]
        for choice in chunk.get("choices") or []:
            if not isinstance(choice, dict):
                continue
            index = choice.get("index", 0)
            if not isinstance(index, int) or isinstance(index, bool):
                index = 0
            state = choices.get(index)
            if state is None:
                state = _new_choice(index)
                choices[index] = state
                order.append(index)
            if isinstance(choice.get("delta"), dict):
                _merge_message(state, choice["delta"])
            elif isinstance(choice.get("message"), dict):
                _merge_message(state, choice["message"])
            if choice.get("finish_reason"):
                state["finish"] = choice["finish_reason"]

    result_choices = []
    for index in sorted(order):
        state = choices[index]
        if state["tool_order"]:
            _validate_tool_arguments(state)
        message = {"role": state["role"] or "assistant", "content": state["content"]}
        if state["reasoning"]:
            message["reasoning_content"] = state["reasoning"]
        if state["tool_order"]:
            calls = []
            for tool_index in sorted(state["tool_order"]):
                slot = state["tools"][tool_index]
                calls.append(
                    {
                        "index": slot["index"],
                        "id": slot["id"],
                        "type": slot["type"],
                        "function": dict(slot["function"]),
                    }
                )
            message["tool_calls"] = calls
            if not message["content"]:
                message["content"] = None
        result_choices.append(
            {
                "index": state["index"],
                "message": message,
                "finish_reason": state["finish"] or "stop",
            }
        )

    model = meta["model"] or (payload.get("model") if isinstance(payload, dict) else None)
    completion = {
        "id": meta["id"] or "chatcmpl-wb",
        "object": "chat.completion",
        "created": meta["created"] if meta["created"] is not None else 0,
        "model": model,
        "choices": result_choices,
    }
    if usage:
        completion["usage"] = usage
    return completion


# --------------------------------------------------------------------------- #
# public service
# --------------------------------------------------------------------------- #

class ChatService:
    """Forwards one chat request to the upstream and streams it back.

    The service owns the shared admission gate (default: 1 concurrent, 6 RPM,
    queue 2, queue timeout 30s) so a direct :meth:`complete` call and a gateway
    request draw on *one* budget.  :meth:`complete` takes a concurrency slot but
    deliberately does *not* spend a request permit at admission: :meth:`stream`
    consumes exactly one permit per real upstream POST through the client's
    ``before_post`` hook, so failover and refresh resends are counted once each.
    :meth:`stream` performs no admission of its own -- the caller owns the slot.
    """

    def __init__(self, client, limits=None):
        self._client = client
        self.limits = limits or ConcurrencyGate(
            concurrency=1, queue_size=2, queue_timeout=30.0, rpm=6
        )

    async def complete(self, payload):
        """Aggregate the streamed answer into one ``chat.completion``."""
        async with self.limits.slot(count_rpm=False):
            chunks = []
            async for chunk in self.stream(payload):
                chunks.append(chunk)
            return aggregate_chunks(chunks, payload)

    async def stream(self, payload, *, observe_errors=True):
        """Yield OpenAI chunk dicts; raise on any malformed / failed upstream.

        The upstream response is closed (via the ``chat_stream`` context
        manager) when this generator is exhausted, closed early or cancelled,
        so a client disconnect tears the upstream read loop down too.  Transport
        failures are folded into a fixed, desensitized :class:`ChatStreamError`;
        ``CancelledError`` / ``GeneratorExit`` are never swallowed, and a local
        :class:`core.limits.LimitExceeded` (budget spent) is passed through
        untouched so the caller can answer 429 instead of a fake 502.

        ``observe_errors=False`` is used for a routed candidate attempt whose
        failure may be absorbed by the standby model: only *this* call is muted.
        """
        body = _build_upstream_body(payload)
        try:
            async with self._client.chat_stream(
                body,
                before_post=self.limits.consume_rpm,
                observe_errors=observe_errors,
            ) as resp:
                if resp.status != 200:
                    code, scope = await _read_error_details(resp)
                    raise UpstreamError(
                        resp.status, code, parse_retry_after(resp), limit_scope=scope
                    )
                content_type = resp.headers.get("Content-Type", "")
                if "text/event-stream" not in content_type:
                    data = await _read_json_body(resp)
                    for chunk in _completion_to_chunks(data):
                        yield chunk
                    return
                async for chunk in _iter_sse(resp):
                    yield chunk
        except (aiohttp.ClientError, asyncio.TimeoutError, UnicodeError):
            raise ChatStreamError("upstream chat request failed") from None
