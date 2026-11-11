"""ACP 客户端：握手、登录、会话生命周期、prompt 流。

分工：:class:`core.acp.transport.AcpTransport` 只管一条管道的收发，
本模块只负责协议时序——什么时候 initialize、什么时候新建会话、一轮 prompt
怎么收尾、登录链接怎么转达。

会话模型（重要）：

* **一次请求一个会话，用完即弃。** AstrBot 的 OpenAI 请求体里没有任何会话标识
  （只有 ``messages`` 和 ``model``），拿不到群标识就没法按群复用会话；硬复用
  只会让不同群的历史互相污染。既然 AstrBot 每轮都重发完整上下文，插件就照旧
  做无状态转发——上下文全部由 prompt 自带。
* 因此本模块**没有**会话映射表：不存在"同一会话键串行"这回事，并发请求天然
  各走各的会话。
* ACP **没有** ``session/close``：一次性会话只能弃之不理，CLI 侧的记录靠运维清理。
* 会话失效（``-32000`` auth）时直接上抛，由运行时的登录态收口，不拿旧 id 硬试。
"""

from __future__ import annotations

import asyncio
import contextlib

from .protocol import (
    M_AUTHENTICATE,
    M_INITIALIZE,
    M_SESSION_CANCEL,
    M_SESSION_NEW,
    M_SESSION_PROMPT,
    N_AUTH_URL,
    PROTOCOL_VERSION,
    STOP_REASONS,
    AcpProcessError,
    AcpRpcError,
    build_prompt_blocks,
    normalize_update,
)
from .transport import SESSION_CLOSED

# 一轮 prompt 的终态标记：作为生成器的最后一项产出。
KIND_DONE = "done"

# 上报给 CLI 的客户端版本。必须与 metadata.yaml 的 version 一致；版本号统一
# 收敛是既有技术债，这里先不引入第五处来源，只在 P3 一并处理时改这一行。
CLIENT_VERSION = "0.1.3"


class AcpClient:
    """在一条已启动的传输上，跑完 ACP 的握手与会话时序。"""

    def __init__(self, transport, *, cwd, client_name="astrbot-plugin-workbuddy"):
        self._transport = transport
        self._cwd = cwd
        self._client_name = client_name
        self._capabilities = {}
        self._auth_methods = []
        self._initialized = False

    # -- 只读视图 -----------------------------------------------------------

    @property
    def capabilities(self) -> dict:
        return dict(self._capabilities)

    @property
    def auth_methods(self) -> list:
        return [dict(item) for item in self._auth_methods]

    @property
    def initialized(self) -> bool:
        return self._initialized

    def auth_method_ids(self) -> list:
        return [
            item["id"]
            for item in self._auth_methods
            if isinstance(item, dict) and isinstance(item.get("id"), str)
        ]

    def pick_auth_method(self, preferred="internal"):
        """按偏好挑一个认证方法 id；``internal`` 是中国版微信登录。"""
        ids = self.auth_method_ids()
        if preferred in ids:
            return preferred
        return ids[0] if ids else None

    # -- 握手 ---------------------------------------------------------------

    async def initialize(self) -> dict:
        """发 ``initialize`` 并记录能力位；版本不兼容直接失败。"""
        result = await self._transport.request(
            M_INITIALIZE,
            {
                "protocolVersion": PROTOCOL_VERSION,
                "clientCapabilities": {
                    # 不声明 fs / terminal：插件不给 CLI 任何文件或命令能力。
                    "promptCapabilities": {},
                },
                "clientInfo": {"name": self._client_name, "version": CLIENT_VERSION},
            },
        )
        version = result.get("protocolVersion")
        if isinstance(version, int) and version > PROTOCOL_VERSION:
            raise AcpRpcError(None, None, "unsupported acp protocol version")
        capabilities = result.get("agentCapabilities")
        self._capabilities = capabilities if isinstance(capabilities, dict) else {}
        methods = result.get("authMethods")
        self._auth_methods = [m for m in methods if isinstance(m, dict)] if isinstance(methods, list) else []
        self._initialized = True
        return {
            "protocolVersion": version if isinstance(version, int) else PROTOCOL_VERSION,
            "capabilities": dict(self._capabilities),
            "authMethods": self.auth_methods,
        }

    async def authenticate(self, method_id, *, on_auth_url=None, timeout=None) -> dict:
        """发起登录。

        CLI 的行为是：先推一条 ``_codebuddy.ai/authUrl`` 通知，然后请求本身一直
        挂到用户完成扫码。``on_auth_url`` 会在链接到达时被调用（同步回调，绝不
        把链接写进日志）。``timeout=None`` 表示一直等。
        """
        captured = {"url": None}

        def _listener(method, params):
            if method != N_AUTH_URL:
                return
            url = params.get("authUrl")
            if not isinstance(url, str) or not url.startswith("https://"):
                return
            captured["url"] = url
            if on_auth_url is not None:
                try:
                    on_auth_url(url)
                except Exception:  # noqa: BLE001 - 回调失败不能打断登录
                    pass

        self._transport.add_notification_listener(_listener)
        try:
            result = await self._transport.request(
                M_AUTHENTICATE, {"methodId": method_id}, timeout=timeout
            )
        finally:
            self._transport.remove_notification_listener(_listener)
        return {"result": result, "authUrl": captured["url"]}

    # -- 会话 ---------------------------------------------------------------

    async def _create_session(self, cwd=None) -> str:
        """建一个 ACP 会话并返回 id；不落进任何映射。"""
        result = await self._transport.request(
            M_SESSION_NEW,
            {"cwd": cwd or self._cwd, "mcpServers": []},
        )
        session_id = result.get("sessionId")
        if not isinstance(session_id, str) or not session_id:
            raise AcpProcessError("acp session/new returned no sessionId")
        return session_id

    async def prompt_once(self, text, *, cwd=None, timeout=None):
        """跑一轮用完即弃的 prompt：新建会话 → 发一次 → 丢弃会话。

        这是插件唯一的聊天路径。AstrBot 每轮都把完整上下文重发一遍，且请求体里
        没有任何会话标识；复用 ACP 会话只会让不同群的历史互相污染。``text`` 因此
        必须自带全部上下文（system 提示、历史、本轮问题），由
        :func:`core.acp.backend.build_prompt` 拼装。

        会话 id 不被任何地方记住，所以同键串行、并发单飞那些机制在这里都不适用
        ——每个请求天然独占一个会话，也就不需要锁。
        """
        session_id = await self._create_session(cwd=cwd)
        stream = self._prompt_once(session_id, text, timeout=timeout)
        try:
            async for item in stream:
                yield item
        finally:
            await stream.aclose()

    async def _prompt_once(self, session_id, text, *, timeout=None):
        queue = self._transport.subscribe(session_id)
        loop = asyncio.get_running_loop()
        done_marker = object()
        outcome = loop.create_future()

        async def runner():
            try:
                result = await self._transport.request(
                    M_SESSION_PROMPT,
                    {
                        "sessionId": session_id,
                        "prompt": build_prompt_blocks(text),
                    },
                    timeout=timeout,
                )
                if not outcome.done():
                    outcome.set_result(result)
            except asyncio.CancelledError:
                # 消费者提前退出时的主动取消。绝不把 CancelledError 塞进 outcome：
                # 那个 future 已经没人 await，带异常回收会打出
                # "Future exception was never retrieved"。
                if not outcome.done():
                    outcome.cancel()
                raise
            except (SystemExit, KeyboardInterrupt):
                # 进程退出信号不能被降级成一次普通的"上游失败"。
                raise
            except BaseException as exc:  # noqa: BLE001 - 交给消费者统一处理
                if not outcome.done():
                    outcome.set_exception(exc)
            finally:
                queue.put_nowait(done_marker)

        task = loop.create_task(runner(), name="wb-acp-prompt")
        completed = False
        try:
            while True:
                item = await queue.get()
                if item is done_marker:
                    break
                if item is SESSION_CLOSED:
                    # 响应可能已经到手，只是 done_marker 还没轮到入队：读循环在
                    # EOF 分支里是同步关队列的，会抢在 runner 被唤醒之前。先看
                    # outcome，已经完成就按正常收尾走，别把成功的一轮判成 502。
                    if outcome.done() and not outcome.cancelled():
                        break
                    raise AcpProcessError("acp process exited during prompt")
                params = item.get("params")
                update = normalize_update(
                    params.get("update") if isinstance(params, dict) else None
                )
                if update is not None:
                    yield update
            result = await outcome
            completed = True
            stop_reason = result.get("stopReason")
            if stop_reason not in STOP_REASONS:
                stop_reason = "unknown"
            yield {"kind": KIND_DONE, "stop_reason": stop_reason}
        finally:
            self._transport.unsubscribe(session_id, queue)
            try:
                # 只要这一轮没有正常收尾，上游就还在跑：超时、RPC 错误、消费者
                # 提前退出都算。不发 cancel，CLI 会把这一轮生成到底——额度白烧。
                if not completed:
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(
                            self._transport.notify(
                                M_SESSION_CANCEL, {"sessionId": session_id}
                            ),
                            2.0,
                        )
            finally:
                # 收尾本身也要 finally：上面最多会等 2 秒，这期间若外层被取消，
                # 后面的回收就全被跳过，留下一个跑到天荒地老的幽灵任务。
                try:
                    if not task.done():
                        task.cancel()
                    await task
                except asyncio.CancelledError:
                    # 只吞"被我们取消的那个 task"的取消；外层取消要继续往上走。
                    if not task.cancelled():
                        raise
                finally:
                    if not completed:
                        if outcome.done() and not outcome.cancelled():
                            # runner 已经把异常挂在 outcome 上，但没人 await 过它。
                            # 不取走会在 future 回收时打出 never-retrieved 告警。
                            outcome.exception()
                        elif not outcome.done():
                            outcome.cancel()


__all__ = ["AcpClient", "KIND_DONE", "AcpRpcError", "AcpProcessError"]
