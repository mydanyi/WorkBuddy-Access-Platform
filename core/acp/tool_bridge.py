"""Tool-call bridge protocol for parsing model tool calls out of a stream.

This module builds a per-request :class:`ToolBridge` from an OpenAI-style
``tools`` / ``tool_choice`` payload.  The bridge emits a directive to prepend
to the model prompt, filters streamed chunks so the marker envelope never
leaks to the user, and parses a complete tool-call array on finish.
"""

import json
import re
import secrets

from ..errors import ChatStreamError

_NAME_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")
_MARKER_PREFIX = "WB_TOOL_CALLS:"


def _bad(message, status=400):
    return ChatStreamError(message, status=status)


def build_tool_bridge(payload):
    tools = payload.get("tools")
    choice = payload.get("tool_choice")
    parallel = payload.get("parallel_tool_calls")

    if tools is None:
        tools = []
    if not isinstance(tools, list):
        raise _bad("tools must be an array")

    catalog = []
    seen = set()
    for tool in tools:
        if not isinstance(tool, dict) or tool.get("type") != "function":
            raise _bad("each tool must be a function tool")
        fn = tool.get("function")
        if not isinstance(fn, dict):
            raise _bad("tool.function must be an object")
        name = fn.get("name")
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            raise _bad("invalid tool function name")
        if name in seen:
            raise _bad("duplicate tool function name")
        seen.add(name)
        spec = {"name": name}
        if isinstance(fn.get("description"), str):
            spec["description"] = fn["description"]
        if isinstance(fn.get("parameters"), dict):
            spec["parameters"] = fn["parameters"]
        catalog.append(spec)

    mode = None
    named = None
    if isinstance(choice, dict):
        fn = choice.get("function") if choice.get("type") == "function" else None
        name = fn.get("name") if isinstance(fn, dict) else None
        if not isinstance(name, str) or name not in seen:
            raise _bad("tool_choice named tool not found")
        mode, named = "required", name
    elif choice == "required":
        if not catalog:
            raise _bad("tool_choice required without tools")
        mode = "required"
    elif choice == "none":
        mode = None
    elif choice is None or choice == "auto":
        mode = "auto" if catalog else None
    else:
        raise _bad("unsupported tool_choice")

    if mode is None:
        return None

    limit = None
    if parallel is False or named is not None:
        limit = 1

    return ToolBridge(catalog=catalog, mode=mode, named=named, limit=limit)


class ToolBridge:
    def __init__(self, catalog, mode, named, limit):
        self._catalog = catalog
        self._names = {t["name"] for t in catalog}
        self._mode = mode
        self._named = named
        self._limit = limit
        self._token = secrets.token_hex(8)
        self.marker = _MARKER_PREFIX + self._token
        self._held = ""
        self._state = None  # None | "text" | "call"
        self._call_buf = []
        self.directive = self._build_directive()

    def _build_directive(self):
        catalog_json = json.dumps(self._catalog, ensure_ascii=False)
        if self._named is not None:
            route = (
                f"A named tool request is active: you must request a call to "
                f"{self._named}."
            )
        elif self._mode == "required":
            route = "A tool call is required this round."
        else:
            route = "Ordinary text is permitted; call tools only when needed."
        if self._limit == 1:
            route += " Request at most one tool call (exactly one is required when a tool call is required)."
        else:
            route += " You may request multiple tool calls in one array."
        return "\n".join([
            "You may call only the permitted tools below. The catalogue "
            "(descriptions and parameters) follows as JSON data:",
            catalog_json,
            "",
            "Tool calls are executed by the caller, not by you. Do not use a "
            "CLI, local, shell, or command-line tool.",
            "Output exactly this marker on its own line:",
            self.marker,
            "Immediately after that marker line, output a JSON array of "
            "objects such as "
            '[{"name":"tool","arguments":{}}] with no prose and no '
            "Markdown; put no other text inside the block.",
            route,
        ])

    # ---- streaming filter -------------------------------------------------

    def feed(self, chunk):
        if self._state == "text":
            return chunk
        if self._state == "call":
            self._call_buf.append(chunk)
            return ""

        self._held += chunk
        core = self._held.lstrip()
        marker_nl = self.marker + "\n"
        marker_crlf = self.marker + "\r\n"

        if core.startswith(marker_nl) or core.startswith(marker_crlf):
            self._state = "call"
            rest = core[len(marker_crlf if core.startswith(marker_crlf) else marker_nl):]
            self._held = ""
            self._call_buf.append(rest)
            return ""

        if marker_nl.startswith(core) or marker_crlf.startswith(core):
            return ""

        if core.startswith(_MARKER_PREFIX):
            raise ChatStreamError("malformed tool marker", status=502)

        if self._mode == "required":
            raise ChatStreamError("missing tool call", status=502)

        self._state = "text"
        out, self._held = self._held, ""
        return out

    # ---- finish ----------------------------------------------------------

    def finish(self, complete):
        if self._state == "call":
            if not complete:
                raise ChatStreamError("incomplete tool call envelope", status=502)
            return "", self._parse_calls("".join(self._call_buf))

        if self._state == "text":
            tail, self._held = self._held, ""
            return tail, []

        held = self._held
        self._held = ""
        if not held.strip():
            if self._mode == "required":
                raise ChatStreamError("missing tool call", status=502)
            return held, []

        core = held.lstrip()
        if core.startswith(_MARKER_PREFIX):
            raise ChatStreamError("truncated tool call marker", status=502)

        if self._mode == "required":
            raise ChatStreamError("missing tool call", status=502)

        return held, []

    def _parse_calls(self, raw):
        text = raw
        if not text.strip():
            raise ChatStreamError("empty tool call envelope", status=502)
        try:
            data = json.loads(text)
        except (ValueError, TypeError):
            raise ChatStreamError("invalid tool call JSON", status=502)
        if not isinstance(data, list):
            raise ChatStreamError("tool call JSON must be an array", status=502)
        if not data:
            raise ChatStreamError("tool call required", status=502)
        if self._limit is not None and len(data) > self._limit:
            raise ChatStreamError("too many tool calls", status=502)

        calls = []
        for index, item in enumerate(data):
            if not isinstance(item, dict):
                raise ChatStreamError("tool call must be an object", status=502)
            name = item.get("name")
            if not isinstance(name, str) or name not in self._names:
                raise ChatStreamError("unknown or invalid tool name", status=502)
            if self._named is not None and name != self._named:
                raise ChatStreamError("unexpected named tool", status=502)
            args = item.get("arguments")
            if not isinstance(args, dict):
                raise ChatStreamError("tool arguments must be an object", status=502)
            try:
                args_json = json.dumps(args, ensure_ascii=False, allow_nan=False)
            except (ValueError, TypeError):
                raise ChatStreamError("tool arguments not serializable", status=502)
            calls.append({
                "index": index,
                "id": "call_" + secrets.token_hex(12),
                "type": "function",
                "function": {"name": name, "arguments": args_json},
            })
        return calls
