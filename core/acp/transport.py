"""ACP over stdio 传输层：子进程、NDJSON 分帧、请求配对、通知路由、回收。

设计要点（全部由 ``tests/test_acp_transport.py`` 的假 CLI 夹具钉住）：

* 一行一条 JSON，``\\n`` 分隔。读用 ``StreamReader.readline`` 并设置较大的
  ``limit``，单帧超限是协议错误，不做截断重试。
* 请求 id 单调递增；响应按 id 唤醒对应 future；**未知 id 的响应直接丢弃**，
  绝不误配给别的调用。
* 无 id 的 ``session/update`` 按 ``params.sessionId`` 路由到该会话的订阅队列；
  无 ``sessionId`` 的通知（如 ``_codebuddy.ai/authUrl``）交给全局监听者。
* 服务端发来的请求（``session/request_permission`` 等）必须**当场回应**，
  否则 CLI 会一直挂着。我们一律拒绝/取消——插件不给任何工具授权。
* 进程退出或协议破坏时，所有待决 future 立刻以固定错误失败，所有会话队列收到
  结束哨兵，调用方不会无限等待。
* ``close()`` 先失败待决、再关 stdin、再等退出，超时 terminate、再超时 kill。
  杀不掉的进程会留着引用，``alive`` 照实说 True——上层据此拒绝删凭据。
"""

from __future__ import annotations

import asyncio
import contextlib

from .protocol import (
    ERROR_METHOD_NOT_FOUND,
    JSONRPC_VERSION,
    M_SESSION_REQUEST_PERMISSION,
    AcpProcessError,
    AcpProtocolError,
    AcpRpcError,
    AcpTimeoutError,
    classify_message,
    decode_message,
    encode_message,
    rpc_error_from,
    session_id_of,
)

# 单个 NDJSON 帧的读取上限。ACP 的正文块可能很大（长代码、大工具输出），
# 给到 8 MiB；超限是协议错误，不静默截断。
MAX_FRAME_BYTES = 8 * 1024 * 1024

# stderr 只保留尾部若干字节用于诊断，绝不进日志正文或上报。
_STDERR_TAIL_BYTES = 8 * 1024

# 会话队列的结束哨兵：进程没了，消费者必须知道。
SESSION_CLOSED = object()

_UNSET = object()

# 服务端请求的默认回应：拒绝，且绝不暴露我们自己的能力。
_DENY_PERMISSION_RESULT = {"outcome": {"outcome": "cancelled"}}


class AcpTransport:
    """一条 CLI 子进程 + 一条 stdio 管道上的 JSON-RPC 通道。"""

    def __init__(
        self,
        argv,
        *,
        env=None,
        cwd=None,
        request_timeout=60.0,
        read_limit=MAX_FRAME_BYTES,
        start_probe=0.15,
    ):
        if not argv:
            raise ValueError("argv must not be empty")
        self._argv = list(argv)
        self._env = dict(env) if env else None
        self._cwd = cwd
        self._request_timeout = request_timeout
        self._read_limit = read_limit
        # 启动后等多久来判断"进程是不是立刻死了"。慢机器上（冷启动解释器、
        # 网络盘上的解释器）150ms 可能不够，所以留成可调：用例需要确定性时
        # 给一个大窗口，生产环境用默认值，代价只是首次启动多等这么久。
        self._start_probe = start_probe

        self._proc = None
        self._reader_task = None
        self._stderr_task = None
        self._stderr_tail = b""
        self._pending = {}
        self._next_id = 0
        self._session_queues = {}
        self._notification_listeners = []
        # 读循环一旦退出，这条管道就再也没人读了：必须让新请求立刻失败，
        # 而不是分配 id、写 stdin、然后等满超时。
        self._dead = False

    # -- 生命周期 -----------------------------------------------------------

    @property
    def running(self) -> bool:
        return not self._dead and self._proc is not None and self._proc.returncode is None

    @property
    def alive(self) -> bool:
        """OS 层面这个子进程是否还在跑。

        与 :attr:`running` 的区别：``running`` 说的是"这条管道还能用"，``close()``
        之后恒为 False；``alive`` 说的是"进程还没退"，杀不掉时仍为 True。
        退出登录前要判的是后者——活着的 CLI 手里还攥着 token，会把凭据写回磁盘。
        """
        proc = self._proc
        return proc is not None and proc.returncode is None

    @property
    def returncode(self):
        return None if self._proc is None else self._proc.returncode

    @property
    def stderr_tail(self) -> str:
        """进程 stderr 的尾部（仅用于本地诊断，不进用户可见文本）。"""
        return self._stderr_tail.decode("utf-8", "replace")

    @property
    def pid(self):
        return None if self._proc is None else self._proc.pid

    async def start(self) -> None:
        """拉起子进程并开始读循环。子进程立刻退出时抛 :class:`AcpProcessError`。"""
        if self._proc is not None:
            raise AcpProcessError("acp transport already started")
        self._dead = False
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *self._argv,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=self._env,
                cwd=self._cwd,
                limit=self._read_limit,
            )
        except FileNotFoundError:
            raise AcpProcessError("acp cli executable was not found") from None
        except OSError:
            raise AcpProcessError("acp cli could not be started") from None

        # 给进程一点时间真正退出：能立刻退出的 CLI（缺依赖、参数被拒）必须报错，
        # 而不是让第一次请求等满超时。只 sleep 0 不够——子进程的退出状态要等
        # 内核回收后才写进 returncode。按小步轮询，退出就立刻收手。
        deadline = asyncio.get_running_loop().time() + self._start_probe
        while self._proc.returncode is None:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            await asyncio.sleep(min(0.02, remaining))
        if self._proc.returncode is not None:
            code = self._proc.returncode
            await self._drain_stderr()
            self._proc = None
            raise AcpProcessError("acp cli exited immediately (code={})".format(code))

        self._reader_task = asyncio.get_running_loop().create_task(
            self._read_loop(), name="wb-acp-reader"
        )
        self._stderr_task = asyncio.get_running_loop().create_task(
            self._stderr_loop(), name="wb-acp-stderr"
        )

    async def close(self, *, timeout=5.0) -> None:
        """关 stdin、等退出，超时 terminate 再超时 kill；待决请求全部失败。

        **取消不能把子进程丢下。** 等待退出这一步一旦被 cancel 打断，terminate/kill
        整段都会被跳过，而调用方那头往往已经把所有权摘掉了（``_discard()`` 先置空再
        关闭、``trim()`` 先移出映射再关）：进程就成了没人认领的孤儿，反复几次就超出
        池子上限。所以收尾放进独立任务，取消时先等它做完，再把取消往外传。
        """
        self._dead = True
        proc = self._proc
        if proc is None:
            await self._cancel_tasks()
            return

        self._fail_pending(AcpProcessError("acp transport closed"))
        self._close_session_queues()

        stdin = proc.stdin
        if stdin is not None and not stdin.is_closing():
            with contextlib.suppress(Exception):
                stdin.close()

        reaper = asyncio.ensure_future(self._reap(proc, timeout))
        try:
            await asyncio.shield(reaper)
        except asyncio.CancelledError:
            # shield 只是把取消挡在 reaper 外面，我们这边照样立刻收到 CancelledError。
            # 此刻返回等于宣布"关完了"，而进程还活着，所以先等它做完再抛。
            with contextlib.suppress(BaseException):
                await asyncio.shield(reaper)
            raise
        finally:
            # 无论如何都要收干净读协程与实例状态：否则 _dead 已是 True，却还留着一个
            # 挂着读循环的实例。
            with contextlib.suppress(BaseException):
                await self._cancel_tasks()
            # 杀不掉就**留着引用**：调用方要能看出"进程还在"，而 _dead 已是 True，
            # 没人会再拿它当活管道用。丢掉引用等于把"没杀干净"改写成"杀干净了"，
            # 于是"活着的 CLI 会把凭据写回去"这个前提就再也没人检查。
            self._proc = None if proc.returncode is not None else proc

    async def _reap(self, proc, timeout) -> None:
        """等进程退出；超时 terminate，再超时 kill。"""
        try:
            await asyncio.wait_for(proc.wait(), timeout)
        except (asyncio.TimeoutError, TimeoutError):
            with contextlib.suppress(Exception):
                proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), 3.0)
            except (asyncio.TimeoutError, TimeoutError):
                with contextlib.suppress(Exception):
                    proc.kill()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(proc.wait(), 3.0)

    async def _cancel_tasks(self) -> None:
        """取消并**等**读循环真正退出。

        只 cancel 不 await 会让读循环的 finally 拖到下一轮事件循环才跑：那时
        ``close()`` 已经返回、``_proc`` 已被置空，清理动作（失败待决、关队列）
        就会落到重启后的新进程上。
        """
        tasks = [t for t in (self._reader_task, self._stderr_task) if t is not None]
        self._reader_task = None
        self._stderr_task = None
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # -- 发送 ---------------------------------------------------------------

    def _send_sync(self, payload) -> None:
        """同步写一帧。整帧一次 ``write``，多协程交错也不会撕裂行。"""
        proc = self._proc
        if proc is None or proc.returncode is not None or self._dead:
            raise AcpProcessError("acp process is not running")
        stdin = proc.stdin
        if stdin is None or stdin.is_closing():
            raise AcpProcessError("acp stdin is closed")
        data = encode_message(payload)
        if len(data) > MAX_FRAME_BYTES:
            raise AcpProtocolError("outgoing acp frame was too large")
        stdin.write(data)

    async def _send(self, payload, *, strict=False) -> None:
        """写一帧；``strict`` 时把 ``drain`` 失败当成传输层错误抛出。

        ``notify()`` 是尽力而为，写不出去无所谓。``request()`` 不行：CLI 关掉 stdin
        读端、stdout 却还开着时，读循环不会失败，这个请求就会一直挂到超时——prompt
        用的是 600 秒那一档，并发名额被白白占满十分钟。直接失败，立刻把名额放出来。
        """
        self._send_sync(payload)
        proc = self._proc
        stdin = None if proc is None else proc.stdin
        if stdin is not None:
            try:
                await stdin.drain()
            except OSError as exc:
                if strict:
                    raise AcpProcessError("acp stdin write failed") from exc

    def _allocate_id(self) -> int:
        self._next_id += 1
        return self._next_id

    async def notify(self, method, params=None) -> None:
        """发一条通知（无 id，不等响应）。"""
        payload = {"jsonrpc": JSONRPC_VERSION, "method": method}
        if params is not None:
            payload["params"] = params
        await self._send(payload)

    async def request(self, method, params=None, *, timeout=_UNSET):
        """发一条请求并等响应；错误响应抛 :class:`AcpRpcError`。

        ``timeout=None`` 表示不设上限（``authenticate`` 会一直挂到用户扫码完成）。
        """
        if self._proc is None or self._proc.returncode is not None or self._dead:
            raise AcpProcessError("acp process is not running")
        if timeout is _UNSET:
            timeout = self._request_timeout

        loop = asyncio.get_running_loop()
        request_id = self._allocate_id()
        future = loop.create_future()
        self._pending[request_id] = future

        payload = {"jsonrpc": JSONRPC_VERSION, "id": request_id, "method": method}
        if params is not None:
            payload["params"] = params

        async def round_trip():
            await self._send(payload, strict=True)
            return await future

        try:
            if timeout is None:
                return await round_trip()
            # 发送与等待共用一个期限。只给响应 future 设超时是不够的：CLI 不读
            # stdin 时 ``drain()`` 会一直挂着——大 prompt 一旦超过管道缓冲，请求就
            # 永远卡在发送这一步，连后端那个 600 秒的 prompt 超时都够不着它，并发
            # 名额被长期占死。两段各自限时的叠法会凭空放大总预算，所以用一个
            # ``wait_for`` 把整趟往返包住。
            return await asyncio.wait_for(round_trip(), timeout)
        except (asyncio.TimeoutError, TimeoutError):
            raise AcpTimeoutError("acp request timed out") from None
        finally:
            self._pending.pop(request_id, None)
            # 等待方被取消、或 _fail_pending 恰好同时挂上异常时，future 可能带着
            # 没人取的异常被回收；主动取一次，避免 "exception was never retrieved"。
            if future.done() and not future.cancelled():
                with contextlib.suppress(BaseException):
                    future.exception()

    # -- 接收 ---------------------------------------------------------------

    async def _read_loop(self) -> None:
        proc = self._proc
        stream = None if proc is None else proc.stdout
        if stream is None:
            self._dead = True
            self._fail_pending(AcpProcessError("acp stdout is unavailable"))
            return
        try:
            while True:
                try:
                    line = await stream.readline()
                except (asyncio.LimitOverrunError, ValueError):
                    raise AcpProtocolError("acp frame exceeded the read limit") from None
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                self._dispatch(decode_message(line))
        except asyncio.CancelledError:
            raise
        except AcpProtocolError as exc:
            self._fail_pending(exc)
        except Exception:
            self._fail_pending(AcpProtocolError("acp reader failed unexpectedly"))
        finally:
            # 无论正常 EOF 还是协议破坏，等待者都必须被唤醒。管道已死，后续请求
            # 一律快速失败——读循环不在了，没人能再兑现它们的 future。
            # 守卫必须连 _dead 一起包住：否则重启后旧世代读循环退出时，会把新
            # 管道的 _dead 置 True，整条通道就再也起不来了。
            if self._proc is proc:
                self._dead = True
                self._fail_pending(AcpProcessError("acp process exited"))
                self._close_session_queues()

    def _dispatch(self, obj) -> None:
        kind = classify_message(obj)
        if kind == "response":
            self._resolve(obj)
            return
        if kind == "notification":
            self._route_notification(obj)
            return
        self._answer_server_request(obj)

    def _resolve(self, obj) -> None:
        request_id = obj.get("id")
        future = self._pending.get(request_id)
        if future is None or future.done():
            # 未知或已超时的 id：丢弃，绝不误配。
            return
        if "error" in obj:
            future.set_exception(rpc_error_from(obj))
        else:
            result = obj.get("result")
            future.set_result(result if isinstance(result, dict) else {})

    def _route_notification(self, obj) -> None:
        method = obj.get("method")
        params = obj.get("params")
        if not isinstance(params, dict):
            params = {}
        session_id = session_id_of(params)
        if session_id is not None:
            for queue in tuple(self._session_queues.get(session_id, ())):
                queue.put_nowait(obj)
            return
        # 无 sessionId 的通知（登录链接、全局事件）交给监听者，逐个隔离失败。
        for listener in tuple(self._notification_listeners):
            try:
                listener(method, params)
            except Exception:  # noqa: BLE001 - 监听者失败不能破坏读循环
                pass

    def _answer_server_request(self, obj) -> None:
        """回应服务端发来的请求；我们一概不授权。"""
        request_id = obj.get("id")
        method = obj.get("method")
        if method == M_SESSION_REQUEST_PERMISSION:
            # 契约要求客户端必须回应，否则 CLI 会一直等。
            self._reply(request_id, result=_DENY_PERMISSION_RESULT)
            return
        self._reply(
            request_id,
            error={"code": ERROR_METHOD_NOT_FOUND, "message": "Method not found"},
        )

    def _reply(self, request_id, *, result=None, error=None) -> None:
        payload = {"jsonrpc": JSONRPC_VERSION, "id": request_id}
        if error is not None:
            payload["error"] = error
        else:
            payload["result"] = result if result is not None else {}
        try:
            self._send_sync(payload)
        except Exception:  # noqa: BLE001 - 进程可能已经死了，回应失败不影响读循环
            pass

    async def _stderr_loop(self) -> None:
        proc = self._proc
        stream = None if proc is None else proc.stderr
        if stream is None:
            return
        try:
            while True:
                chunk = await stream.read(4096)
                if not chunk:
                    return
                self._stderr_tail = (self._stderr_tail + chunk)[-_STDERR_TAIL_BYTES:]
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - stderr 只用于诊断
            return

    async def _drain_stderr(self) -> None:
        proc = self._proc
        stream = None if proc is None else proc.stderr
        if stream is None:
            return
        with contextlib.suppress(Exception):
            chunk = await asyncio.wait_for(stream.read(4096), 0.5)
            if chunk:
                self._stderr_tail = (self._stderr_tail + chunk)[-_STDERR_TAIL_BYTES:]

    # -- 会话订阅 -----------------------------------------------------------

    def subscribe(self, session_id) -> asyncio.Queue:
        """为一个 ACP 会话建订阅队列（同一会话可多方订阅）。"""
        queue = asyncio.Queue()
        self._session_queues.setdefault(session_id, set()).add(queue)
        return queue

    def unsubscribe(self, session_id, queue) -> None:
        queues = self._session_queues.get(session_id)
        if not queues:
            return
        queues.discard(queue)
        if not queues:
            self._session_queues.pop(session_id, None)

    def add_notification_listener(self, listener) -> None:
        """注册全局通知监听（无 ``sessionId`` 的通知，例如登录链接）。"""
        self._notification_listeners.append(listener)

    def remove_notification_listener(self, listener) -> None:
        with contextlib.suppress(ValueError):
            self._notification_listeners.remove(listener)

    # -- 失败收敛 -----------------------------------------------------------

    def _fail_pending(self, exc) -> None:
        """把所有待决请求以固定错误失败。

        每个 future 拿一个**独立**的异常实例：同一个异常对象挂到多个 future 上
        会让 traceback 互相污染，排查时会误导。
        """
        protocol = isinstance(exc, AcpProtocolError)
        for future in list(self._pending.values()):
            if future.done():
                continue
            # 保留原始文案：主动 close 与进程崩溃要能分得清。
            future.set_exception(type(exc)(str(exc)) if protocol else AcpProcessError(str(exc)))

    def _close_session_queues(self) -> None:
        for queues in tuple(self._session_queues.values()):
            for queue in tuple(queues):
                queue.put_nowait(SESSION_CLOSED)


__all__ = [
    "AcpTransport",
    "AcpRpcError",
    "AcpTimeoutError",
    "AcpProcessError",
    "AcpProtocolError",
    "SESSION_CLOSED",
    "MAX_FRAME_BYTES",
]
