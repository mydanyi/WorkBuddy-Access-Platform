"""把 ACP 的更新流适配成 OpenAI chunk 流，接到现有 Gateway 下面。

为什么要有这一层：``core.gateway`` 和 ``core.chat.aggregate_chunks`` 都只认
OpenAI 的 chunk 形状。只要这一层产出同样的形状，网关、流式写出、非流式聚合
全部不用改一行——ACP 是「换上游」，不是「换协议出口」。

三条语义决定（都是刻意的，不是省事）：

1. **无状态：每轮新建一个 ACP 会话。** AstrBot 的 OpenAI 请求体只有
   ``messages`` 和 ``model``，没有任何会话标识（见
   ``astrbot/core/provider/sources/openai_source.py`` 里的
   ``payloads = {"messages": ..., "model": ...}``）。拿不到群标识就没法按群
   复用会话，硬复用只会让不同群的历史互相污染。既然 AstrBot 每轮都重发完整
   上下文，插件就照旧做无状态转发——这与迁移前 ``/v2/chat/completions`` 的行为
   完全一致。
2. **system 提示每轮都带。** 会话是一次性的，没有「首轮」可言。
3. **工具调用不映射成 OpenAI ``tools``。** 群聊消息永远不能驱动服务器执行命令。
   ``tool_call`` / ``tool_call_update`` 在这一层直接丢弃。
4. **一轮一个模型，模型决定进程。** ``payload["model"]`` 直接决定用哪条 CLI
   进程（``--model`` 是进程启动参数，见 :mod:`core.acp.pool`）。本层不再"回显
   一个模型、实际用另一个"——那正是迁移前最容易骗过调用方的地方。

错误映射（上层依赖这些状态码**和类**）：

* ACP ``-32000 / category=auth`` → ``UpstreamError(401, "acp_auth")``；
* 请求超时 → ``UpstreamError(504)``；
* 进程中途不可用 → ``UpstreamError(502)``；
* 其余 JSON-RPC 错误 → ``UpstreamError(502)``；
* CLI 根本起不来（路径错 / 没装）→ ``ChatStreamError(status=502)``；
* 坏帧 → ``ChatStreamError``；一帧正文都没有 → ``ChatStreamError``。

**为什么「进程不可用」是 ``UpstreamError`` 而「起不来」是 ``ChatStreamError``**：
``core.routing`` 只在 ``UpstreamError`` 且状态可重试、且还没吐出任何正文时换下一个
候选。进程中途死掉、卡住、或这一轮被上游拒掉，换一条进程换个模型确实可能成功；而
``acp.cli_path`` 配错时每个候选都会以同样的方式起不来，那属于配置错误，不该让路由
把整条主备顺序挨个试一遍、再给每个模型扣一次 60 秒冷却。

**接线状态：已接进生产链路。** ``main.py`` 的 ``_build_gateway`` 用
``AcpChatBackend(None, pool=self.acp)`` 装配网关的聊天后端，CLI 由
``AcpRuntimePool`` 按模型懒启动；401 与正常收尾都会回灌到承载该模型的那条运行时，
所以 ``/wb status`` 的登录态跟真实请求同步。
"""

from __future__ import annotations

import contextlib
import inspect
import logging
import time
import uuid
from contextlib import asynccontextmanager

from ..errors import ChatStreamError, UpstreamError
from ..limits import ConcurrencyGate
from ..routing import ORDERED_MODEL_ID
from .client import KIND_DONE
from .protocol import (
    KIND_TEXT,
    KIND_THOUGHT,
    STOP_END_TURN,
    STOP_MAX_TOKENS,
    AcpError,
    AcpFrameTooLarge,
    AcpProcessError,
    AcpProtocolError,
    AcpRpcError,
    AcpTimeoutError,
)

_log = logging.getLogger(__name__)

# 历史段与当前提问段的分隔语。必须自己写死，不能让上游的消息内容冒充分隔。
_HISTORY_HEADER = "以下是本次对话的历史记录："
_CURRENT_HEADER = "以上是历史。下面是当前提问："

# 正文里若出现这两个分隔语，模型会把它当成本轮边界；发言者无需任何权限就能
# 在群里打出来。这里把它们替换成不可混淆的形态。
_HEADER_GUARDS = {
    _HISTORY_HEADER: "[以下是本次对话的历史记录]",
    _CURRENT_HEADER: "[以上是历史。下面是当前提问]",
}

# 已知终态到 OpenAI finish_reason 的映射。其余终态是失败，不能伪装成正常结束。
_FINISH_REASONS = {
    STOP_END_TURN: "stop",
    STOP_MAX_TOKENS: "length",
}

# 单轮 prompt 的兜底上限（秒）。ACP 没有内建超时，进程卡住时必须有东西收口。
_DEFAULT_PROMPT_TIMEOUT = 600.0

# 失败状态码 → 通知分类。这张表只描述本层**真会产出**的状态码，不是上游的通用
# 映射：本层的出口只有 400（没有可回答的用户消息）、401（ACP 要求认证）、
# 502（进程不可用 / JSON-RPC 失败 / 没有可渲染正文）、504（prompt 超时）。
# 403 与 429 在这条链上不会出现——限流由 ``limits`` 先拦下、权限问题由 CLI 表现
# 成 ``-32000``——写进来只会让后来者把它当契约。
_ERROR_CATEGORIES = {
    401: "auth",
    # 504 是 CLI 卡住不回（prompt 超时），不是真的断网；沿用既有的"网络连接
    # 异常"文案只是为了与路由层口径一致，别按字面理解。
    504: "network",
}
# 调用方自己的错误不提醒：那是网关该回的 4xx，不是上游事故。本层只产出 400。
_CALLER_ERROR_STATUSES = frozenset({400})


def _error_category(exc):
    """把一次聊天失败映射成固定的通知分类；不该提醒的返回 ``None``。"""
    status = getattr(exc, "status", None)
    if isinstance(status, int) and not isinstance(status, bool):
        if status in _CALLER_ERROR_STATUSES:
            return None
        category = _ERROR_CATEGORIES.get(status)
        if category is not None:
            return category
    return "upstream"


def _content_text(content):
    """把一条消息的 ``content`` 取成纯文本；非文本部分忽略。"""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for part in content:
            if not isinstance(part, dict):
                continue
            if part.get("type") == "text" and isinstance(part.get("text"), str):
                parts.append(part["text"])
        return "".join(parts) if parts else None
    return None


def _neutralize(text):
    """把消息正文里可能出现的历史分隔语换掉，避免伪造对话边界。"""
    for header, replacement in _HEADER_GUARDS.items():
        if header in text:
            text = text.replace(header, replacement)
    return text


def build_prompt(payload):
    """把 AstrBot 的 ``messages`` 摊平成一段自包含的 prompt 文本。

    返回 ``str``；没有可发的用户消息时返回 ``None``（调用方转成 400）。

    规则：
    * ``system`` / ``developer`` 只认第一条；
    * 只保留 ``user`` / ``assistant``，``tool`` 等一律丢弃——CLI 侧工具已被
      ``--tools ""`` 关掉，历史里的工具痕迹对它没有意义，反而占预算；
    * **最后一条必须是 user 且非空白**，否则视为没有可回答的问题。末条 user
      若内容是纯图片/空数组（取不到文本）也算没有——绝不能退回去把上一条老
      问题当成本轮问题再答一遍。
    """
    messages = payload.get("messages") if isinstance(payload, dict) else None
    if not isinstance(messages, list):
        return None

    system_text = None
    turns = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        role = message.get("role")
        text = _content_text(message.get("content"))
        if role in ("system", "developer"):
            if system_text is None and text is not None:
                system_text = text
            continue
        if text is None:
            # 取不到文本的消息（纯图片、空数组、非文本块）也要参与「末条」判定，
            # 否则它会从列表里消失，让更早的一条 user 顶上来。
            if role in ("user", "assistant"):
                turns.append((role, None))
            continue
        if role == "user":
            turns.append(("用户", text))
        elif role == "assistant":
            turns.append(("助手", text))

    if not turns:
        return None
    last_role, last_text = turns[-1]
    if last_role != "用户" or last_text is None or not last_text.strip():
        return None

    history = [
        (label, text)
        for label, text in turns[:-1]
        if text is not None and text.strip()
    ]

    parts = []
    if system_text is not None and system_text.strip():
        parts.append(_neutralize(system_text.strip()))
    if history:
        parts.append(_HISTORY_HEADER)
        for label, text in history:
            parts.append(_neutralize("{}：{}".format(label, text.strip())))
        parts.append(_CURRENT_HEADER)
    parts.append(_neutralize(last_text.strip()))
    return "\n\n".join(parts)


class AcpChatBackend:
    """对上层暴露与 ``ChatService`` 同形的 ``stream()``。"""

    def __init__(
        self,
        client,
        *,
        model=None,
        limits=None,
        clock=None,
        prompt_timeout=_DEFAULT_PROMPT_TIMEOUT,
        runtime=None,
        pool=None,
        on_error=None,
    ):
        if client is None and runtime is None and pool is None:
            raise ValueError("either client, runtime or pool is required")
        if runtime is not None and pool is not None:
            # 两者都指"去哪要进程"，同时给出来只说明装配方没想清楚该用哪个。
            raise ValueError("runtime and pool are mutually exclusive")
        self._client = client
        # 给了运行时就用它：CLI 是懒启动的，构造期还没有 client；顺带由它收下
        # 401 与正常收尾，让 ``/wb status`` 跟真实请求同步。
        self._runtime = runtime
        # 池是生产路径：一个模型一条进程。注入 ``client=`` 时（测试里常见）它不参与。
        self._pool = pool
        self._model = model
        self._clock = clock if clock is not None else time.time
        self._prompt_timeout = prompt_timeout
        # 只观测、不改变失败本身：网关照旧按异常回状态码，这里只为管理员提醒
        # 提供入口。回调必须是同步的；它自己抛什么都不能影响这次请求。
        self._on_error = on_error
        # Gateway 会把自己的门赋给 ``chat.limits``，所以这里必须是可写属性。
        self.limits = limits or ConcurrencyGate(
            concurrency=1, queue_size=2, queue_timeout=30.0, rpm=6
        )

    @property
    def model(self):
        """调用方没指定模型时用的那个（可能是 ``None``）。"""
        return self._model

    @property
    def runtime(self):
        """装配时交给本后端的单条运行时（用池或注入客户端时为 ``None``）。

        只读属性存在的理由跟 ``model`` 一样：装配是否正确必须能被外部验证，
        否则 ``_build_gateway`` 里少传一个 ``runtime=`` 就只能等线上 502 才发现。
        """
        return self._runtime

    @property
    def pool(self):
        """装配时交给本后端的进程池（未接线时为 ``None``）。理由同 :attr:`runtime`。"""
        return self._pool

    @property
    def on_error(self):
        """失败观察者（未接线时为 ``None``）。理由同 :attr:`runtime`。"""
        return self._on_error

    async def stream(self, payload, *, observe_errors=True):
        """产出一串 OpenAI chunk，最后一个带 ``finish_reason``。

        ``observe_errors`` 控制失败要不要报给 ``on_error`` 观察者：路由层重试
        单个候选时传 ``False``，因为那一轮失败由路由自己汇总提醒，重复报一次
        只会让管理员收到两条。
        """
        agen = self._stream(payload)
        try:
            async for chunk in agen:
                yield chunk
        except (UpstreamError, ChatStreamError) as exc:
            if observe_errors:
                category = _error_category(exc)
                if category is not None:
                    self._report(category)
            raise
        finally:
            # 外层被放弃时 ``async for`` 不会替我们关掉内层生成器；不显式
            # ``aclose``，退订与 ``session/cancel`` 会拖到 GC，锁也晚放一拍。
            # 收尾失败不能顶掉正在外传的异常：那段 502 / CancelledError 才是网关
            # 和调用方真正要看的，而生成器关不干净没有补救动作可做。
            with contextlib.suppress(Exception):
                await agen.aclose()

    def _report(self, category) -> None:
        """把一次失败报给观察者；观察者自己的异常绝不能影响请求。"""
        callback = self._on_error
        if callback is None:
            return
        try:
            outcome = callback(category)
            if inspect.isawaitable(outcome):
                # 约定是同步回调，这里没有地方 await。真返回了协程就 close 掉，
                # 否则 Python 会为"从未 await 的协程"发 RuntimeWarning；Task /
                # Future 没有 close()，取消失败就说明它已经在后台跑着了。
                with contextlib.suppress(Exception):
                    outcome.close()
        except Exception:  # noqa: BLE001 - 观察者失败不影响请求
            pass

    async def _stream(self, payload):
        prompt_text = build_prompt(payload)
        if prompt_text is None:
            raise ChatStreamError("no user message was provided", status=400)

        # 模型必须在花掉配额之前定下来：定不出来是调用方的问题，不该扣一次额度。
        model = self._requested_model(payload)
        if model is None:
            raise ChatStreamError("no model was requested", status=400)

        # 每次尝试消耗一个配额。注意它排在拉起 CLI 之前：CLI 起不来时同样计数，
        # 所以路径配错的表现是"先 502、六次之后变 429"，而不是一直 502。
        await self.limits.consume_rpm()

        stream_id = "chatcmpl-" + uuid.uuid4().hex
        created = int(self._clock())
        emitted_role = False
        emitted_text = False

        async with self._lease(model) as runtime:
            client = self._client
            if client is None:
                # 懒启动：第一轮聊天才拉起 CLI。起不来就是"进程不可用"，不是登录问题。
                try:
                    client = await runtime.ensure_started()
                except AcpError as exc:
                    # CLI 没装、路径配错、argv 非法，在这一层之后长得跟"上游挂了"
                    # 一模一样：网关回 502，管理员收到"上游服务异常"，没有任何东西
                    # 说得出是装错了。这条日志就是那个唯一的分辨点。
                    _log.warning(
                        "acp: failed to start the CLI for model %r: %s", model, exc
                    )
                    raise ChatStreamError(
                        "upstream chat process is unavailable", status=502
                    ) from exc

            # 无状态：每次新建会话，上下文全靠 prompt_text 自带（见模块 docstring 第 1 条）。
            updates = client.prompt_once(prompt_text, timeout=self._prompt_timeout)
            try:
                async for update in updates:
                    kind = update.get("kind")

                    if kind == KIND_TEXT:
                        if update.get("from_user"):
                            # session/update 可能把用户自己那条消息回吐一份；那不是
                            # 助手正文，混进来会让群里重复一遍提问。
                            continue
                        text = update.get("text") or ""
                        if not text:
                            continue
                        delta = {"content": text}
                        emitted_text = True
                        if not emitted_role:
                            delta["role"] = "assistant"
                            emitted_role = True
                        yield self._chunk(stream_id, created, delta, model=model)

                    elif kind == KIND_THOUGHT:
                        thought = update.get("text") or ""
                        if not thought:
                            continue
                        delta = {"reasoning_content": thought}
                        if not emitted_role:
                            # 首帧可能是思考块：role 必须挂在真正发出去的第一帧上，
                            # 否则客户端拿到的第一个 delta 没有身份。
                            delta["role"] = "assistant"
                            emitted_role = True
                        yield self._chunk(stream_id, created, delta, model=model)

                    elif kind == KIND_DONE:
                        finish = self._finish_reason(update.get("stop_reason"))
                        if not emitted_text:
                            # 一帧正文都没有（例如上游只给了思考块、图片或资源块）：
                            # 宁可报错，也不能把空正文配 stop 当成功交出去。
                            raise ChatStreamError("upstream returned no renderable text")
                        if runtime is not None:
                            # 真跑通一轮才算"登录有效"，比额外探测一次可信。
                            runtime.note_session_ok()
                        yield self._chunk(
                            stream_id, created, {}, model=model, finish=finish
                        )
                        return
            except AcpRpcError as exc:
                if exc.is_auth_required:
                    # 无状态路径没有会话映射要丢；下一次请求自然会重新认证。
                    if runtime is not None:
                        runtime.note_auth_failure()
                    raise UpstreamError(401, "acp_auth") from None
                # 上游明确拒了这一轮（限流、模型不可用、模型名不认等）：给路由一个
                # 机会换候选。但**必须留一行日志**：`UpstreamError` 到网关只剩一句
                # 固定的 "upstream request failed"，如果这里也静默，那么"模型名填错"
                # 和"上游真的挂了"对管理员就是同一件事。ACP 的错误文本只含 code 与
                # category，是固定投影，不含上游报文。
                _log.warning("acp: upstream rejected the turn: %s", exc)
                raise UpstreamError(502) from None
            except AcpTimeoutError:
                raise UpstreamError(504) from None
            except AcpFrameTooLarge:
                # 这一帧**没发出去**：不是上游坏了，是调用方这段 prompt 编码后超过了
                # 单帧上限（网关的请求体上限比它宽松，所以放得进来）。回 413 让它把
                # 请求改小，而不是丢一句 "stream was malformed" 让人去查 CLI。
                raise UpstreamError(413) from None
            except AcpProcessError:
                raise UpstreamError(502) from None
            except AcpProtocolError:
                # 坏帧、超长帧：网关只认 ChatStreamError/UpstreamError，原生异常
                # 逃出去会变成无 error event、无 [DONE] 的硬断连。
                raise ChatStreamError("upstream chat stream was malformed") from None
            finally:
                # 外层被放弃时，async for 不会替我们关掉内层生成器；不显式 aclose，
                # 退订与 session/cancel 会拖到 GC，锁也早放了一拍。
                await updates.aclose()

    def _requested_model(self, payload):
        """本轮该用哪个模型；定不出来返回 ``None``。

        ``ORDERED_MODEL_ID`` 在这里等于"没指定"：那是网关层的主备路由占位符，
        不是 CLI 认识的模型 ID，直接塞进 ``--model`` 只会起一条注定失败的进程。
        走到本层还带着它，说明路由没接上——退回默认模型比伪造一个模型名安全。
        """
        requested = payload.get("model") if isinstance(payload, dict) else None
        if isinstance(requested, str):
            requested = requested.strip()
            if requested and requested != ORDERED_MODEL_ID:
                return requested
        return self._model or None

    @asynccontextmanager
    async def _lease(self, model):
        """占用承载 ``model`` 的运行时直到本轮结束。

        三条装配形态共用这一个出口：注入客户端（测试）不需要运行时；单运行时是
        单模型部署；进程池是生产默认。占用期间池不会把这条进程淘汰掉——正跑到
        一半被杀，用户看到的就是"上游没理由地断流"。

        ``lease()`` 现在也会在两种情况下拒绝：池被永久关掉（插件卸载中）、或者
        正在登出。两者都是"这台机器此刻不接活"，不是"上游坏了"，所以统一翻成
        503 的 :class:`ChatStreamError`——裸 :class:`AcpError` 会一路逃到网关的
        ``except Exception``，被当成"上游请求失败"（502）报给用户、还顺带让路由
        把每个候选都试一遍，把一次登出放大成一串 502。
        """
        if self._client is not None:
            yield None
            return
        if self._pool is not None:
            try:
                async with self._pool.lease(model) as runtime:
                    yield runtime
            except AcpError as exc:
                raise ChatStreamError(
                    "acp runtime is unavailable: {}. Retry in a moment.".format(exc),
                    status=503,
                ) from None
            return
        yield self._runtime

    @staticmethod
    def _finish_reason(stop_reason):
        """终态映射；未知或失败的终态一律报错，绝不伪装成正常结束。

        ``cancelled`` / ``refusal`` / ``max_turn_requests`` 都落到这条报错上，
        于是被归进 ``upstream`` 并提醒管理员一次。``refusal``（模型按内容策略
        拒答）走这条路是刻意的：群里突然答不出来，管理员该知道；同一分类的
        去重窗口保证它不会刷屏。
        """
        finish = _FINISH_REASONS.get(stop_reason)
        if finish is None:
            raise ChatStreamError("upstream ended the turn unexpectedly")
        return finish

    def _chunk(self, stream_id, created, delta, *, model=None, finish=None):
        return {
            "id": stream_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model or self._model,
            "choices": [
                {"index": 0, "delta": dict(delta), "finish_reason": finish}
            ],
        }


__all__ = [
    "AcpChatBackend",
    "build_prompt",
]
