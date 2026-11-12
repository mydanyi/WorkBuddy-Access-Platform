"""ACP (Agent Client Protocol) 线协议常量、报文校验与更新归一化。

本模块只做纯数据变换：不碰进程、不碰 socket、不做 I/O。所有对外暴露的错误
信息都是固定文案，绝不回显原始报文——上游 ``params`` 里可能带账号信息。

协议事实（2026-11-10 在 CLI 2.182.0 上实测，见任务目录 evidence/P0-acp-probe.md）：

* ``initialize`` 返回 ``protocolVersion`` / ``agentCapabilities`` / ``authMethods``；
* 中国版的认证方法 id 是 ``internal``（微信登录）；
* 未登录时 ``session/new`` 返回 ``-32000 Authentication required`` 且
  ``data.category == "auth"``；
* ``authenticate`` 请求不会立刻返回，而是先推一条
  ``_codebuddy.ai/authUrl`` 通知携带登录链接，请求本身挂起到扫码完成。
"""

from __future__ import annotations

import json

JSONRPC_VERSION = "2.0"

# 我们只声明支持 v1；高于此值的响应视为不兼容。
PROTOCOL_VERSION = 1

# -- 方法名 -----------------------------------------------------------------
M_INITIALIZE = "initialize"
M_AUTHENTICATE = "authenticate"
M_SESSION_NEW = "session/new"
M_SESSION_LOAD = "session/load"
M_SESSION_PROMPT = "session/prompt"
M_SESSION_CANCEL = "session/cancel"
M_SESSION_UPDATE = "session/update"
M_SESSION_REQUEST_PERMISSION = "session/request_permission"

# CodeBuddy 方言：登录链接以通知形式下发，不带 id。
N_AUTH_URL = "_codebuddy.ai/authUrl"

# -- session/update 变体 -----------------------------------------------------
U_USER_MESSAGE_CHUNK = "user_message_chunk"
U_AGENT_MESSAGE_CHUNK = "agent_message_chunk"
U_AGENT_THOUGHT_CHUNK = "agent_thought_chunk"
U_TOOL_CALL = "tool_call"
U_TOOL_CALL_UPDATE = "tool_call_update"
U_PLAN = "plan"
U_AVAILABLE_COMMANDS = "available_commands_update"
U_CURRENT_MODE = "current_mode_update"
U_CONFIG_OPTION = "config_option_update"
U_SESSION_INFO = "session_info_update"
U_USAGE = "usage_update"
U_NOTICE = "notice"

# -- 终态 -------------------------------------------------------------------
STOP_END_TURN = "end_turn"
STOP_MAX_TOKENS = "max_tokens"
STOP_MAX_TURN_REQUESTS = "max_turn_requests"
STOP_REFUSAL = "refusal"
STOP_CANCELLED = "cancelled"

STOP_REASONS = frozenset(
    {
        STOP_END_TURN,
        STOP_MAX_TOKENS,
        STOP_MAX_TURN_REQUESTS,
        STOP_REFUSAL,
        STOP_CANCELLED,
    }
)

# -- 错误 -------------------------------------------------------------------
# 未登录：实测 -32000，附 data.category = "auth"。
ERROR_AUTH_REQUIRED = -32000
CATEGORY_AUTH = "auth"

# 服务端主动发给客户端的请求，我们一律不实现，用这个固定错误码拒绝。
ERROR_METHOD_NOT_FOUND = -32601
ERROR_INVALID_PARAMS = -32602

# 归一化后的更新种类（对上层只有这几种，工具/计划/统计一律不透传）。
KIND_TEXT = "text"
KIND_THOUGHT = "thought"
KIND_TOOL = "tool"
KIND_OTHER = "other"


class AcpError(Exception):
    """ACP 层失败的基类。``str()`` 永远是固定文案，不含原始报文。"""


class AcpProtocolError(AcpError):
    """线协议被破坏（非 JSON 行、帧过大、结构不符）。不可恢复。"""


class AcpRpcError(AcpError):
    """JSON-RPC 错误响应。

    ``code`` 是协议错误码，``category`` 是 CodeBuddy 在 ``error.data.category``
    里给出的固定分类（目前只认 ``auth``），``message`` 只用于本地日志与判断，
    绝不拼进返回给用户或上游的文本里。
    """

    def __init__(self, code, category=None, message=None):
        self.code = code
        self.category = category if isinstance(category, str) else None
        self.message = message if isinstance(message, str) else ""
        parts = ["acp error (code={}".format(code)]
        if self.category:
            parts.append(", category={}".format(self.category))
        parts.append(")")
        super().__init__("".join(parts))

    @property
    def is_auth_required(self) -> bool:
        # ``category`` 是 CLI 自己给的分类，比错误码更能说明问题。``-32000`` 只是它的
        # 通用业务错误码：拿这个数字单独判鉴权，会把"会话不存在"之类也认成没登录，
        # 于是回 401、把 ``auth_state`` 压成 False，还跳过了备用模型那一步。有分类就
        # 听分类的，没有分类才退回看码——实测鉴权失败带的是 ``data.category="auth"``
        # （见 evidence/P0-acp-probe.md）。
        if self.category is not None:
            return self.category == CATEGORY_AUTH
        return self.code == ERROR_AUTH_REQUIRED


class AcpProcessError(AcpError):
    """CLI 子进程退出、崩溃或从未启动成功。"""


class AcpTimeoutError(AcpError):
    """等待响应超时。超时不代表请求没有被执行，调用方不得据此重放。"""


def encode_message(payload) -> bytes:
    """把一条 JSON-RPC 报文编码成一行 UTF-8（NDJSON 帧）。"""
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return text.encode("utf-8") + b"\n"


def decode_message(raw):
    """解析一行报文；不是 JSON 对象时抛 :class:`AcpProtocolError`。"""
    try:
        obj = json.loads(raw)
    except Exception:
        raise AcpProtocolError("acp frame was not valid JSON") from None
    if not isinstance(obj, dict):
        raise AcpProtocolError("acp frame was not a JSON object")
    return obj


def classify_message(obj):
    """把一个已解析的报文分成 ``response`` / ``notification`` / ``request``。

    * 带 ``result`` 或 ``error`` 且有 ``id`` —— 响应；
    * 带 ``method`` 但没有 ``id`` —— 通知；
    * 带 ``method`` 且有 ``id`` —— 服务端发来的请求（需要我们回应）；
    * 其余 —— 协议错误。
    """
    if not isinstance(obj, dict):
        raise AcpProtocolError("acp frame was not a JSON object")
    has_id = "id" in obj and obj.get("id") is not None
    has_method = isinstance(obj.get("method"), str) and obj.get("method")
    if has_id and ("result" in obj or "error" in obj):
        return "response"
    if has_method and not has_id:
        return "notification"
    if has_method and has_id:
        return "request"
    raise AcpProtocolError("acp frame had no recognizable shape")


def rpc_error_from(obj) -> AcpRpcError:
    """从一条错误响应里取出 ``(code, category, message)``，绝不回显 data。"""
    error = obj.get("error")
    if not isinstance(error, dict):
        return AcpRpcError(None)
    code = error.get("code")
    code = code if isinstance(code, int) and not isinstance(code, bool) else None
    data = error.get("data")
    category = data.get("category") if isinstance(data, dict) else None
    message = error.get("message")
    return AcpRpcError(code, category, message)


def session_id_of(params):
    """从 ``params`` 里取 ``sessionId``；取不到返回 ``None``。"""
    if not isinstance(params, dict):
        return None
    value = params.get("sessionId")
    return value if isinstance(value, str) and value else None


def text_of_content(content):
    """取一个 ContentBlock 的纯文本；非文本块返回 ``None``。"""
    if not isinstance(content, dict):
        return None
    if content.get("type") != "text":
        return None
    text = content.get("text")
    return text if isinstance(text, str) else None


def normalize_update(update):
    """把一个 ``session/update`` 的 ``update`` 归一化成固定的小 dict。

    只有文本与思考块会被上层渲染；工具调用单独标记但**不**映射成 OpenAI
    ``tools``（群聊里绝不能让消息驱动服务器执行命令）；计划、命令表、用量、
    通知一律折叠成 ``other``，不携带任何自由文本。
    """
    if not isinstance(update, dict):
        return None
    name = update.get("sessionUpdate")
    if not isinstance(name, str):
        return None

    if name in (U_AGENT_MESSAGE_CHUNK, U_AGENT_THOUGHT_CHUNK, U_USER_MESSAGE_CHUNK):
        text = text_of_content(update.get("content"))
        if text is None:
            return None
        kind = KIND_THOUGHT if name == U_AGENT_THOUGHT_CHUNK else KIND_TEXT
        message_id = update.get("messageId")
        return {
            "kind": kind,
            "text": text,
            "message_id": message_id if isinstance(message_id, str) else None,
            "from_user": name == U_USER_MESSAGE_CHUNK,
        }

    if name in (U_TOOL_CALL, U_TOOL_CALL_UPDATE):
        tool_call_id = update.get("toolCallId")
        status = update.get("status")
        return {
            "kind": KIND_TOOL,
            "tool_call_id": tool_call_id if isinstance(tool_call_id, str) else None,
            "status": status if isinstance(status, str) else None,
        }

    return {"kind": KIND_OTHER, "session_update": name}


def build_prompt_blocks(text) -> list:
    """把一段纯文本包成 ``session/prompt`` 的 ``ContentBlock`` 列表。"""
    return [{"type": "text", "text": text if isinstance(text, str) else ""}]
