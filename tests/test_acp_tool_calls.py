"""Contract tests for tool-call bridging in :mod:`core.acp.backend`.

Re-derived marker contract (independent of production):
``WB_TOOL_CALLS:<16 lowercase hex>\\n`` + JSON array of ``{name, arguments}``.
"""

from __future__ import annotations

import json
import re
import sys
import unittest
from pathlib import Path

_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.acp.backend import AcpChatBackend  # noqa: E402
from astrbot_plugin_workbuddy.core.chat import aggregate_chunks  # noqa: E402
from astrbot_plugin_workbuddy.core.errors import ChatStreamError  # noqa: E402
from astrbot_plugin_workbuddy.core.limits import ConcurrencyGate  # noqa: E402

_MARKER_RE = re.compile(r"WB_TOOL_CALLS:([0-9a-f]{16})\n")

_SEARCH_MEMES_SCHEMA = {
    "type": "function",
    "function": {
        "name": "search_memes",
        "description": "Search the meme pool for an image matching the query.",
        "parameters": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    },
}

_SEND_MEME_SCHEMA = {
    "type": "function",
    "function": {
        "name": "send_meme",
        "description": "Send a previously found meme to the current chat.",
        "parameters": {
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "required": ["id"],
        },
    },
}


def _tool_call_deltas(chunks):
    out = []
    for chunk in chunks:
        for choice in chunk.get("choices") or []:
            delta = choice.get("delta") or {}
            for call in delta.get("tool_calls") or []:
                out.append(call)
    return out


def _text_of(chunks):
    return "".join(
        (choice.get("delta") or {}).get("content") or ""
        for chunk in chunks
        for choice in chunk.get("choices") or []
    )


def _finish_of(chunks):
    finishes = [
        choice.get("finish_reason")
        for chunk in chunks
        for choice in chunk.get("choices") or []
        if choice.get("finish_reason")
    ]
    return finishes[-1] if finishes else None


class _FakeAcpClient:
    """Yields normalised ``prompt_once()`` updates (text/thought/done)."""

    def __init__(self, responder):
        self._responder = responder
        self.prompts: list[str] = []

    def prompt_once(self, text, *, timeout=None):
        self.prompts.append(text)

        async def _gen():
            for update in self._responder(text):
                yield update

        return _gen()


def _split_marker_responder(tool_name, arguments):
    """Emit a marker-derived tool call split across deltas.

    Re-derives the nonce from the *actual* prompt; if absent, falls back to
    a plain reply (baseline red path, no setup errors).
    """

    def _responder(prompt):
        match = _MARKER_RE.search(prompt)
        if match is None:
            return [
                {"kind": "text", "text": "plain reply"},
                {"kind": "done", "stop_reason": "end_turn"},
            ]
        nonce = match.group(1)
        body = json.dumps(
            [{"name": tool_name, "arguments": arguments}],
            separators=(",", ":"),
            ensure_ascii=False,
        )
        full = "WB_TOOL_CALLS:" + nonce + "\n" + body
        cut1, cut2 = 6, 16
        return [
            {"kind": "text", "text": full[:cut1]},
            {"kind": "text", "text": full[cut1:cut2]},
            {"kind": "text", "text": full[cut2:]},
            {"kind": "done", "stop_reason": "end_turn"},
        ]

    return _responder


def _plain_responder(text_value):
    def _responder(_prompt):
        return [
            {"kind": "text", "text": text_value},
            {"kind": "done", "stop_reason": "end_turn"},
        ]

    return _responder


class _Base(unittest.IsolatedAsyncioTestCase):
    def _make_backend(self, responder, *, model="glm-5.2"):
        self.fake = _FakeAcpClient(responder)
        return AcpChatBackend(
            self.fake,
            model=model,
            limits=ConcurrencyGate(concurrency=4, rpm=10),
            prompt_timeout=3.0,
        )

    async def _drain(self, backend, payload):
        return [chunk async for chunk in backend.stream(payload)]

    def _payload(self, *, messages=None, tools=None, tool_choice=None):
        payload = {
            "model": "glm-5.2",
            "messages": messages
            if messages is not None
            else [{"role": "user", "content": "find a cat meme"}],
        }
        if tools is not None:
            payload["tools"] = tools
        if tool_choice is not None:
            payload["tool_choice"] = tool_choice
        return payload


class RequiredToolTests(_Base):
    async def test_required_streams_structured_call_and_aggregates(self):
        args = {"query": "sad cat"}
        backend = self._make_backend(
            _split_marker_responder("search_memes", args)
        )
        payload = self._payload(
            tools=[_SEARCH_MEMES_SCHEMA], tool_choice="required"
        )

        # Schema and marker instruction must reach the model.
        prompt_seen = {}

        calls = None
        chunks = await self._drain(backend, payload)
        prompt_seen["p"] = self.fake.prompts[0]

        calls = _tool_call_deltas(chunks)
        self.assertTrue(calls, "expected tool_calls deltas")
        call = calls[0]
        self.assertIsInstance(call["index"], int)
        self.assertRegex(call["id"], r"^call_[0-9a-f]+$")
        self.assertEqual(call["type"], "function")
        self.assertEqual(call["function"]["name"], "search_memes")
        self.assertEqual(json.loads(call["function"]["arguments"]), args)
        self.assertEqual(_finish_of(chunks), "tool_calls")
        self.assertEqual(_text_of(chunks), "")

        prompt = prompt_seen["p"]
        self.assertIn("search_memes", prompt)
        self.assertIn("query", prompt)
        self.assertRegex(prompt, r"WB_TOOL_CALLS:[0-9a-f]{16}")

        completion = aggregate_chunks(chunks, payload)
        self.assertEqual(completion["choices"][0]["finish_reason"], "tool_calls")
        msg = completion["choices"][0]["message"]
        self.assertIn(msg.get("content"), (None, ""))
        out = msg["tool_calls"][0]
        self.assertEqual(out["function"]["name"], "search_memes")
        self.assertEqual(json.loads(out["function"]["arguments"]), args)
        self.assertRegex(out["id"], r"^call_[0-9a-f]+$")

    async def test_unknown_tool_under_required_is_chat_stream_error(self):
        backend = self._make_backend(
            _split_marker_responder("send_meme", {"id": "42"})
        )
        payload = self._payload(
            tools=[_SEARCH_MEMES_SCHEMA], tool_choice="required"
        )
        collected = []
        with self.assertRaises(ChatStreamError):
            async for chunk in backend.stream(payload):
                collected.append(chunk)
        for chunk in collected:
            for choice in chunk.get("choices") or []:
                delta = choice.get("delta") or {}
                text = delta.get("content") or ""
                self.assertNotIn("WB_TOOL_CALLS", text)
                self.assertNotIn('"send_meme"', text)
                self.assertNotIn("tool_calls", delta)

    async def test_required_plain_reply_is_chat_stream_error(self):
        backend = self._make_backend(_plain_responder("no"))
        payload = self._payload(
            tools=[_SEARCH_MEMES_SCHEMA], tool_choice="required"
        )
        with self.assertRaises(ChatStreamError):
            await self._drain(backend, payload)

    async def test_auto_streams_text_without_executing_tool(self):
        backend = self._make_backend(_plain_responder("here is a cat meme list"))
        payload = self._payload(
            tools=[_SEARCH_MEMES_SCHEMA], tool_choice="auto"
        )
        chunks = await self._drain(backend, payload)
        self.assertEqual(_text_of(chunks), "here is a cat meme list")
        self.assertFalse(_tool_call_deltas(chunks))
        self.assertEqual(_finish_of(chunks), "stop")

    async def test_named_tool_choice_only_permits_named_function(self):
        # Model tries to call send_meme while tool_choice names search_memes.
        backend = self._make_backend(
            _split_marker_responder("send_meme", {"id": "x"})
        )
        payload = self._payload(
            tools=[_SEARCH_MEMES_SCHEMA, _SEND_MEME_SCHEMA],
            tool_choice={"type": "function", "function": {"name": "search_memes"}},
        )
        with self.assertRaises(ChatStreamError):
            await self._drain(backend, payload)


    async def test_auto_leaks_prose_before_protocol_envelope(self):
        # Screenshot reproduction: prose first, then a protocol envelope.
        # Old implementation permanently entered text state on prose and leaked
        # the marker/JSON body verbatim.
        args = {"query": "sad cat"}
        payload = self._payload(
            tools=[_SEARCH_MEMES_SCHEMA], tool_choice="auto"
        )

        # Interleave: prose, then marker envelope split across chunks.

        # Replace fake with a prose+marker responder in one stream.
        def _prose_then_marker(prompt):
            match = _MARKER_RE.search(prompt)
            if match is None:
                return [
                    {"kind": "text", "text": "plain reply"},
                    {"kind": "done", "stop_reason": "end_turn"},
                ]
            nonce = match.group(1)
            body = json.dumps(
                [{"name": "search_memes", "arguments": args}],
                separators=(",", ":"),
                ensure_ascii=False,
            )
            full = (
                "here is a cat meme list\n\n"
                + '{"meme_id":""}\n'
                + "WB_TOOL_CALLS:"
                + nonce
                + "\n"
                + body
            )
            return [
                {"kind": "text", "text": full[:15]},
                {"kind": "text", "text": full[15:40]},
                {"kind": "text", "text": full[40:70]},
                {"kind": "text", "text": full[70:]},
                {"kind": "done", "stop_reason": "end_turn"},
            ]

        backend = self._make_backend(_prose_then_marker)
        chunks = await self._drain(backend, payload)
        text = _text_of(chunks)
        calls = _tool_call_deltas(chunks)

        self.assertNotIn("WB_TOOL_CALLS", text)
        self.assertNotIn('{"meme_id"', text)
        self.assertIn("here is a cat meme list", text)
        self.assertTrue(calls, "expected structured tool_calls in auto mode")
        self.assertEqual(calls[0]["function"]["name"], "search_memes")
        self.assertEqual(json.loads(calls[0]["function"]["arguments"]), args)

    async def test_auto_discards_malformed_marker_after_prose(self):
        def _responder(prompt):
            return [
                {"kind": "text", "text": "hi there\n\nWB_TOOL_CALLS:deadbeefdeadbeef\nnot json"},
                {"kind": "done", "stop_reason": "end_turn"},
            ]

        backend = self._make_backend(_responder)
        payload = self._payload(tools=[_SEARCH_MEMES_SCHEMA], tool_choice="auto")
        chunks = await self._drain(backend, payload)
        text = _text_of(chunks)
        self.assertNotIn("WB_TOOL_CALLS", text)
        self.assertNotIn("deadbeef", text)
        self.assertNotIn("not json", text)
        self.assertIn("hi there", text)
        self.assertFalse(_tool_call_deltas(chunks))

    async def test_auto_empty_array_envelope_is_no_call(self):
        def _responder(prompt):
            match = _MARKER_RE.search(prompt)
            if match is None:
                return [
                    {"kind": "text", "text": "plain reply"},
                    {"kind": "done", "stop_reason": "end_turn"},
                ]
            nonce = match.group(1)
            return [
                {"kind": "text", "text": "done thinking\n\nWB_TOOL_CALLS:" + nonce + "\n[]"},
                {"kind": "done", "stop_reason": "end_turn"},
            ]

        backend = self._make_backend(_responder)
        payload = self._payload(tools=[_SEARCH_MEMES_SCHEMA], tool_choice="auto")
        chunks = await self._drain(backend, payload)
        text = _text_of(chunks)
        self.assertIn("done thinking", text)
        self.assertNotIn("WB_TOOL_CALLS", text)
        self.assertNotIn("[]", text)
        self.assertFalse(_tool_call_deltas(chunks))

    async def test_auto_preserves_standalone_user_json(self):
        obj = '{"meme_id":""}'
        backend = self._make_backend(_plain_responder("see:\n" + obj + "\nend"))
        payload = self._payload(tools=[_SEARCH_MEMES_SCHEMA], tool_choice="auto")
        chunks = await self._drain(backend, payload)
        self.assertEqual(_text_of(chunks), "see:\n" + obj + "\nend")
        self.assertFalse(_tool_call_deltas(chunks))

    async def test_auto_detects_marker_across_arbitrary_splits_crlf(self):
        def _responder(prompt):
            match = _MARKER_RE.search(prompt)
            if match is None:
                return [
                    {"kind": "text", "text": "plain reply"},
                    {"kind": "done", "stop_reason": "end_turn"},
                ]
            nonce = match.group(1)
            body = '[{"name":"search_memes","arguments":{"query":"x"}}]'
            full = "intro\r\n\r\nWB_TOOL_CALLS:" + nonce + "\r\n" + body
            # one-char-at-a-time split
            parts = [{"kind": "text", "text": ch} for ch in full]
            parts.append({"kind": "done", "stop_reason": "end_turn"})
            return parts

        backend = self._make_backend(_responder)
        payload = self._payload(tools=[_SEARCH_MEMES_SCHEMA], tool_choice="auto")
        chunks = await self._drain(backend, payload)
        text = _text_of(chunks)
        calls = _tool_call_deltas(chunks)
        self.assertNotIn("WB_TOOL_CALLS", text)
        self.assertIn("intro", text)
        self.assertTrue(calls)
        self.assertEqual(calls[0]["function"]["name"], "search_memes")


    async def test_auto_inline_marker_after_prose_is_not_executed(self):
        # Real providers sometimes append the exact marker + JSON array
        # immediately after ordinary prose on the SAME line.
        args = {"query": "sad cat"}
        payload = self._payload(tools=[_SEARCH_MEMES_SCHEMA], tool_choice="auto")
        prose = "这是一只猫 here is a cat meme list"

        def _responder(prompt):
            match = _MARKER_RE.search(prompt)
            if match is None:
                return [{"kind": "text", "text": "plain reply"},
                        {"kind": "done", "stop_reason": "end_turn"}]
            nonce = match.group(1)
            body = json.dumps([{"name": "search_memes", "arguments": args}],
                              separators=(",", ":"), ensure_ascii=False)
            full = prose + "WB_TOOL_CALLS:" + nonce + "\n" + body
            parts = [{"kind": "text", "text": ch} for ch in full]
            parts.append({"kind": "done", "stop_reason": "end_turn"})
            return parts

        backend = self._make_backend(_responder)
        chunks = await self._drain(backend, payload)
        text = _text_of(chunks)
        self.assertEqual(text, prose)
        self.assertNotIn("WB_TOOL_CALLS", text)
        self.assertNotIn('"search_memes"', text)
        self.assertFalse(_tool_call_deltas(chunks))
        self.assertEqual(_finish_of(chunks), "stop")

    async def test_auto_multiline_json_before_marker_discarded(self):
        # Pretty-printed argument-shaped JSON (literal newlines) precedes a
        # standalone marker emitting []; only intro text survives.
        payload = self._payload(tools=[_SEARCH_MEMES_SCHEMA], tool_choice="auto")
        intro = "intro"
        obj = '{\n  "meme_id": ""\n}'

        def _responder(prompt):
            match = _MARKER_RE.search(prompt)
            if match is None:
                return [{"kind": "text", "text": "plain reply"},
                        {"kind": "done", "stop_reason": "end_turn"}]
            nonce = match.group(1)
            full = intro + "\n" + obj + "\nWB_TOOL_CALLS:" + nonce + "\n[]"
            parts = [{"kind": "text", "text": full[i:i + 7]}
                     for i in range(0, len(full), 7)]
            parts.append({"kind": "done", "stop_reason": "end_turn"})
            return parts

        backend = self._make_backend(_responder)
        chunks = await self._drain(backend, payload)
        text = _text_of(chunks)
        self.assertIn(intro, text)
        self.assertNotIn('"meme_id"', text)
        self.assertNotIn("[]", text)
        self.assertFalse(_tool_call_deltas(chunks))

    async def test_auto_multiline_object_without_marker_preserved(self):
        # A real multiline object without a marker must survive EXACTLY,
        # including whitespace and following prose.
        payload = self._payload(tools=[_SEARCH_MEMES_SCHEMA], tool_choice="auto")
        full = 'note\n{\n  "meme_id": "x",\n  "n": 1\n}\ntail text'
        backend = self._make_backend(_plain_responder(full))
        chunks = await self._drain(backend, payload)
        self.assertEqual(_text_of(chunks), full)
        self.assertFalse(_tool_call_deltas(chunks))

    async def test_auto_trailing_wb_prefix_preserved_in_final_chunk(self):
        # Ordinary prose ending with literal "WB_" in a separate final chunk
        # must keep those exact bytes (nonempty prose already emitted).
        payload = self._payload(tools=[_SEARCH_MEMES_SCHEMA], tool_choice="auto")

        def _responder(_prompt):
            return [{"kind": "text", "text": "ordinary line without the marker yet "},
                    {"kind": "text", "text": "WB_"},
                    {"kind": "done", "stop_reason": "end_turn"}]

        backend = self._make_backend(_responder)
        chunks = await self._drain(backend, payload)
        text = _text_of(chunks)
        self.assertTrue(text.endswith("WB_"))
        self.assertEqual(text, "ordinary line without the marker yet WB_")
        self.assertNotIn("WB_TOOL_CALLS", text)
        self.assertFalse(_tool_call_deltas(chunks))
        self.assertEqual(_finish_of(chunks), "stop")


class ToolHistoryTests(_Base):
    CALL_ID = "call_ab12cd34"

    def _assistant_call(self):
        return {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": self.CALL_ID,
                    "type": "function",
                    "function": {
                        "name": "search_memes",
                        "arguments": json.dumps({"query": "cat"}),
                    },
                }
            ],
        }

    async def test_matched_tool_result_continues_with_associated_details(self):
        backend = self._make_backend(_plain_responder("found: cat-001"))
        payload = {
            "model": "glm-5.2",
            "messages": [
                {"role": "user", "content": "find a cat meme"},
                self._assistant_call(),
                {
                    "role": "tool",
                    "tool_call_id": self.CALL_ID,
                    "content": json.dumps({"id": "cat-001"}),
                },
            ],
            "tools": [_SEARCH_MEMES_SCHEMA, _SEND_MEME_SCHEMA],
            "tool_choice": "none",
        }
        chunks = await self._drain(backend, payload)
        self.assertEqual(_text_of(chunks), "found: cat-001")
        self.assertEqual(_finish_of(chunks), "stop")

        prompt = self.fake.prompts[0]
        self.assertIn("cat-001", prompt)
        self.assertIn("search_memes", prompt)
        self.assertIn('"query"', prompt)
        self.assertEqual(prompt.count("find a cat meme"), 1)

    async def test_mismatched_tool_result_is_not_final_valid_turn(self):
        backend = self._make_backend(_plain_responder("ok"))
        payload = {
            "model": "glm-5.2",
            "messages": [
                {"role": "user", "content": "find a cat meme"},
                self._assistant_call(),
                {
                    "role": "tool",
                    "tool_call_id": "call_ffffffff",
                    "content": "orphan",
                },
                {"role": "user", "content": "hi again"},
            ],
            "tools": [_SEARCH_MEMES_SCHEMA],
            "tool_choice": "none",
        }
        try:
            await self._drain(backend, payload)
        except ChatStreamError:
            pass
        prompt = self.fake.prompts[0] if self.fake.prompts else ""
        # Orphan tool result must not be treated as a matched final turn.
        self.assertNotEqual(
            prompt.split("\n")[-1].strip(), "orphan", "orphan leaked as final turn"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
