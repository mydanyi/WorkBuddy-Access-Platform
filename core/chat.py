"""把一串 OpenAI chunk 聚合成一个 ``chat.completion``。

聊天本体已经改走 ACP（``core.acp.backend`` 直接产出 chunk），所以这个模块不再有
SSE 解码、不再发上游请求、不再管 401 重试——它只剩非流式请求最后那一步：网关收了
一堆 chunk，得拼成一个普通 JSON 回给调用方。调用方要流式时网关直接转发 chunk，
根本走不到这里。

聚合规则（与 OpenAI 的语义一致，也和迁移前逐字相同）：

* 按 ``choices[].index`` 分组，最后按 index 升序输出；
* ``delta`` 与 ``message`` 都接受——上游可能给任何一种形状；
* 流式 ``tool_calls`` 按 ``index`` 优先、其次 ``id`` 合并，``name``/``arguments``
  片段拼接；拼完的 ``arguments`` 必须是完整 JSON，否则报错而不是交出一份假的完整；
* 只有 ``usage`` 没有 ``choices`` 的帧不算一个选项，但它的 ``usage`` 会被带上。
"""

from __future__ import annotations

import json

from .errors import ChatStreamError


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


__all__ = ["aggregate_chunks"]
