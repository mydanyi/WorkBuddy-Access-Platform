"""ACP 传输层与客户端的契约测试。

这一组**不依赖 AstrBot**：只需要标准库 + 一个假 CLI 脚本，因此可以在任何
Python 3.12 环境里独立跑。真 AstrBot 集成另有 P4 阶段。

运行::

    python -m unittest tests.test_acp_transport -v
"""

from __future__ import annotations

import asyncio
import json
import sys
import unittest
from pathlib import Path

_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.acp import (  # noqa: E402
    KIND_DONE,
    KIND_TEXT,
    KIND_THOUGHT,
    AcpClient,
    AcpFrameTooLarge,
    AcpProcessError,
    AcpProtocolError,
    AcpRpcError,
    AcpTimeoutError,
    AcpTransport,
    SESSION_CLOSED,
)
from astrbot_plugin_workbuddy.core.acp import protocol  # noqa: E402
from astrbot_plugin_workbuddy.core.acp.client import CLIENT_VERSION  # noqa: E402
from astrbot_plugin_workbuddy.core.acp.transport import MAX_FRAME_BYTES  # noqa: E402

_FAKE = str(_HERE.parent / "fake_acp_cli.py")


def _argv(scenario):
    return [sys.executable, _FAKE, "--scenario", scenario]


async def _drain(client, text="hi", *, timeout=5.0):
    """跑完一轮 prompt，返回 ``(updates, done)``。"""
    updates = []
    done = None
    async for item in client.prompt_once(text, timeout=timeout):
        if item.get("kind") == KIND_DONE:
            done = item
        else:
            updates.append(item)
    return updates, done


class ProtocolTests(unittest.TestCase):
    """纯函数：报文分类、错误识别、更新归一化。"""

    def test_classify_response_notification_request(self):
        self.assertEqual(
            protocol.classify_message({"jsonrpc": "2.0", "id": 1, "result": {}}),
            "response",
        )
        self.assertEqual(
            protocol.classify_message({"jsonrpc": "2.0", "method": "x/y", "params": {}}),
            "notification",
        )
        self.assertEqual(
            protocol.classify_message({"jsonrpc": "2.0", "id": 9, "method": "x/y"}),
            "request",
        )

    def test_classify_rejects_garbage(self):
        with self.assertRaises(AcpProtocolError):
            protocol.classify_message({"jsonrpc": "2.0"})
        with self.assertRaises(AcpProtocolError):
            protocol.decode_message(b"not json")
        with self.assertRaises(AcpProtocolError):
            protocol.decode_message(b"[1,2,3]")

    def test_auth_required_detection(self):
        err = protocol.rpc_error_from(
            {
                "id": 2,
                "error": {
                    "code": -32000,
                    "message": "Authentication required",
                    "data": {"category": "auth"},
                },
            }
        )
        self.assertTrue(err.is_auth_required)
        self.assertEqual(err.category, "auth")
        self.assertEqual(err.code, -32000)

    def test_category_decides_auth_and_the_code_is_only_a_fallback(self):
        # ``-32000`` 是 CLI 的通用业务错误码。只按它判鉴权的话，一次"会话不存在"
        # 就会被当成没登录：回 401、把登录态压成 False，还跳过备用模型。
        other = protocol.rpc_error_from(
            {"id": 3, "error": {"code": -32000, "data": {"category": "session"}}}
        )
        self.assertFalse(other.is_auth_required)
        # 没有分类时才退回看码——老版本 CLI 不带 ``data.category``。
        bare = protocol.rpc_error_from({"id": 4, "error": {"code": -32000}})
        self.assertTrue(bare.is_auth_required)

    def test_normalize_text_thought_tool_other(self):
        text = protocol.normalize_update(
            {
                "sessionUpdate": "agent_message_chunk",
                "messageId": "m1",
                "content": {"type": "text", "text": "hi"},
            }
        )
        self.assertEqual(text["kind"], KIND_TEXT)
        self.assertEqual(text["text"], "hi")
        self.assertEqual(text["message_id"], "m1")
        self.assertFalse(text["from_user"])

        thought = protocol.normalize_update(
            {
                "sessionUpdate": "agent_thought_chunk",
                "content": {"type": "text", "text": "hmm"},
            }
        )
        self.assertEqual(thought["kind"], KIND_THOUGHT)

        tool = protocol.normalize_update(
            {"sessionUpdate": "tool_call", "toolCallId": "c1", "status": "pending"}
        )
        self.assertEqual(tool["kind"], "tool")
        self.assertEqual(tool["tool_call_id"], "c1")

        other = protocol.normalize_update({"sessionUpdate": "usage_update"})
        self.assertEqual(other["kind"], "other")
        self.assertNotIn("text", other)

    def test_normalize_ignores_non_text_content(self):
        self.assertIsNone(
            protocol.normalize_update(
                {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "image", "data": "..."},
                }
            )
        )
        self.assertIsNone(protocol.normalize_update({"nope": 1}))


class TransportTestCase(unittest.IsolatedAsyncioTestCase):
    """每个用例起一个假 CLI；用例结束一定回收。"""

    scenario = "happy"

    async def asyncSetUp(self):
        self.transport = AcpTransport(
            _argv(self.scenario), request_timeout=5.0
        )
        await self.transport.start()

    async def asyncTearDown(self):
        await self.transport.close(timeout=3.0)


class _StubbornProcess:
    """杀不掉的子进程替身：``wait()`` 立刻超时，``returncode`` 永远为 None。

    真进程只要肯死就测不到这条分支（SIGKILL 之下没有进程能赖着），所以直接替掉。
    """

    returncode = None
    stdin = None
    pid = 4242

    def terminate(self):
        pass

    def kill(self):
        pass

    async def wait(self):
        raise asyncio.TimeoutError


class CloseTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_keeps_a_reference_when_the_process_survives(self):
        # 丢掉引用等于把"没杀干净"改写成"杀干净了"：上层要据此拒绝删凭据，
        # 因为活着的 CLI 会把 token 写回磁盘。
        transport = AcpTransport(_argv("happy"), request_timeout=5.0)
        transport._proc = _StubbornProcess()
        await transport.close(timeout=0.01)
        self.assertFalse(transport.running)
        self.assertTrue(transport.alive)
        # 引用还在：下次 close 才能再试一次 terminate/kill。
        self.assertEqual(transport.pid, 4242)

    async def test_close_clears_the_reference_when_the_process_is_gone(self):
        transport = AcpTransport(_argv("happy"), request_timeout=5.0)
        await transport.start()
        await transport.close(timeout=3.0)
        self.assertFalse(transport.running)
        self.assertFalse(transport.alive)


class CloseCancellationTests(unittest.IsolatedAsyncioTestCase):
    """关闭过程被打断时，子进程不能变成没人认领的孤儿。

    淘汰空闲进程正好撞上请求的截止时间就走这条路：调用方那头已经把所有权摘掉了，
    这边要是把 terminate/kill 一起跳过，进程就永远留着。
    """

    async def test_a_cancelled_close_still_reaps_the_child(self):
        transport = AcpTransport(_argv("linger_after_eof"), request_timeout=5.0)
        await transport.start()
        proc = transport._proc
        closer = asyncio.ensure_future(transport.close(timeout=0.2))
        await asyncio.sleep(0.05)  # 让它停在"等进程自己退出"上
        closer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closer
        self.assertIsNotNone(
            proc.returncode, "取消把 terminate/kill 一起跳过了：进程还活着"
        )
        self.assertFalse(transport.alive)


class _DeafStdin:
    """写得进去、drain 报管道已断：CLI 关掉了读端，stdout 却还开着。"""

    def __init__(self, error=BrokenPipeError):
        self._error = error

    def is_closing(self):
        return False

    def write(self, data):
        pass

    async def drain(self):
        raise self._error


class _DeafProcess:
    """只提供 ``request()`` 会碰到的那几个属性的子进程替身。"""

    returncode = None
    pid = 4243

    def __init__(self, error=BrokenPipeError):
        self.stdin = _DeafStdin(error)


class SendFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_broken_stdin_fails_the_request_instead_of_waiting(self):
        # stdout 还开着，读循环不会失败，没人来兑现这个请求的 future。要是把 drain
        # 的异常吞掉，调用方就得一直等到超时——prompt 那一档是 600 秒。
        transport = AcpTransport(_argv("happy"), request_timeout=5.0)
        transport._proc = _DeafProcess()
        loop = asyncio.get_running_loop()
        started = loop.time()
        with self.assertRaises(AcpProcessError):
            await transport.request("test/echo", {}, timeout=5.0)
        self.assertLess(loop.time() - started, 1.0, "不该傻等到超时")

    async def test_notify_still_shrugs_off_a_broken_pipe(self):
        # 取消通知、心跳这类东西写不出去就算了：它们没有等待方，抛出去只会让
        # 收尾路径半路炸掉。``request()`` 才是必须知道"没发出去"的那个。
        transport = AcpTransport(_argv("happy"), request_timeout=5.0)
        transport._proc = _DeafProcess(RuntimeError)
        await transport.notify("session/cancel", {"sessionId": "s1"})

    async def test_an_oversized_frame_blames_the_caller_not_the_upstream(self):
        # 网关的请求体上限（默认 16 MB）比单帧上限（8 MiB）宽松，所以"体量大到发不
        # 出去"的请求能走进来。这一帧根本没发出去，报成"上游流坏了"是甩锅，还会让
        # 人去查 CLI；它该是一个能让调用方改小请求的 413。
        transport = AcpTransport(_argv("happy"), request_timeout=5.0)
        transport._proc = _DeafProcess()
        with self.assertRaises(AcpFrameTooLarge):
            await transport.request(
                "session/prompt", {"text": "x" * (MAX_FRAME_BYTES + 1024)}
            )


class _SilentStdin:
    """``drain()`` 永远不回来：CLI 不读 stdin，管道缓冲区已经满了。"""

    def is_closing(self):
        return False

    def write(self, data):
        pass

    async def drain(self):
        await asyncio.Event().wait()  # 只有取消叫得醒它


class _SilentProcess:
    """只提供 ``request()`` 会碰到的那几个属性的子进程替身。"""

    returncode = None
    pid = 4245

    def __init__(self):
        self.stdin = _SilentStdin()


class BlockedSendTests(unittest.IsolatedAsyncioTestCase):
    """发不出去、而对面已经判死：这一轮必须立刻返回，不许陪着 drain 一起耗。"""

    async def test_a_dead_reader_interrupts_a_blocked_send(self):
        # CLI 不读 stdin（``drain()`` 挂着），stdout 又坏了。只按"先发完再等"串行走，
        # 这个失败要等 drain 自己愿意回来才看得见——prompt 那一档是 600 秒，并发名额
        # 就这么白占十分钟，而进程其实已经废了。
        transport = AcpTransport(_argv("happy"), request_timeout=600.0)
        transport._proc = _SilentProcess()
        loop = asyncio.get_running_loop()
        started = loop.time()

        async def _reader_dies():
            # 有界轮询：``_pending`` 万一一直没注册上（``request()`` 在它之前就退出
            # 了），这里也得自己收工。收不了就让用例按"没在 1 秒内返回"去红，绝不
            # 把一个核烧到天荒地老。
            for _ in range(200):
                if transport._pending:
                    transport._fail_pending(AcpProcessError("FICTIONAL-READER-DIED"))
                    return
                await asyncio.sleep(0.01)

        killer = asyncio.ensure_future(_reader_dies())
        try:
            with self.assertRaises(AcpProcessError):
                # 外面再套一个 2 秒上限：没修之前这里会一直挂着，用例不该跟着挂死。
                await asyncio.wait_for(
                    transport.request("session/prompt", {}, timeout=600.0), 2.0
                )
        finally:
            # ``return_exceptions`` 只兜"抛异常的死法"，不兜"根本不终止"。
            killer.cancel()
            await asyncio.gather(killer, return_exceptions=True)
        self.assertLess(loop.time() - started, 1.0, "读循环已经判死了，不该等到超时")

    async def test_a_cancelled_caller_is_not_turned_into_a_success(self):
        # 响应先到、发送还挂在 ``drain()`` 上（对端已经把整帧读完，只是缓冲区没排空）。
        # 这时候调用方被取消，取消必须真的抛出去：吞掉它等于把一次取消变成一次"成功
        # 返回"，请求方还以为下游收到了。写端真出错照样吞——响应都到手了，这次往返
        # 本身是成功的。
        transport = AcpTransport(_argv("happy"), request_timeout=600.0)
        transport._proc = _SilentProcess()

        task = asyncio.ensure_future(
            transport.request("session/prompt", {}, timeout=600.0)
        )
        try:
            for _ in range(200):  # 等它把 future 注册上
                if transport._pending:
                    break
                await asyncio.sleep(0.01)
            self.assertTrue(transport._pending, "请求没注册上，这条用例没验到东西")
            for request_id, future in list(transport._pending.items()):
                future.set_result(
                    {"jsonrpc": "2.0", "id": request_id, "result": {}}
                )
            for _ in range(50):  # 让它真的走到"响应到手、接着等发送"那一步
                if task.done():
                    break
                await asyncio.sleep(0.01)
            self.assertFalse(task.done(), "它该还卡在发送上，否则没验到那个窗口")
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2.0)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


class InitializeTests(TransportTestCase):
    scenario = "happy"

    def test_the_reported_client_version_matches_the_plugin_metadata(self):
        # 这一串会进 ``initialize`` 的 ``clientInfo.version``，是 CLI 那边唯一能知道
        # "谁在跟我说话"的东西。报个陈旧版本，它就会按旧行为对待我们，而没人看得出来。
        # 三处各写一遍（metadata.yaml / @register / 这里）是既有技术债，这条用例负责
        # 让"忘记一起改"变红。
        import re

        metadata = (_HERE.parents[1] / "metadata.yaml").read_text(encoding="utf-8")
        declared = re.search(r"^version:\s*v?([\d.]+)", metadata, re.MULTILINE)
        self.assertIsNotNone(declared, "metadata.yaml 里没找到 version")
        self.assertEqual(CLIENT_VERSION, declared.group(1))

    async def test_initialize_reports_capabilities_and_auth_methods(self):
        client = AcpClient(self.transport, cwd="/tmp")
        info = await client.initialize()
        self.assertEqual(info["protocolVersion"], 1)
        self.assertEqual(info["capabilities"]["loadSession"], True)
        self.assertIn("internal", client.auth_method_ids())
        self.assertEqual(client.pick_auth_method(), "internal")

    async def test_stray_response_id_is_ignored(self):
        await self.transport.close(timeout=3.0)
        self.transport = AcpTransport(_argv("stray"), request_timeout=5.0)
        await self.transport.start()
        # 走原始 request：一旦按到达顺序误配，这里拿到的是 {"junk": True}。
        # 用 initialize 的话，两种结果都会被规范化成 protocolVersion=1，测不出来。
        result = await self.transport.request(
            "initialize",
            {
                "protocolVersion": 1,
                "clientCapabilities": {"promptCapabilities": {}},
                "clientInfo": {"name": "t", "version": "0"},
            },
        )
        self.assertEqual(result.get("protocolVersion"), 1)
        self.assertIn("authMethods", result)


class PairingTests(TransportTestCase):
    scenario = "happy"

    async def test_concurrent_requests_pair_by_id(self):
        results = await asyncio.gather(
            *[
                self.transport.request("test/echo", {"n": n})
                for n in range(12)
            ]
        )
        self.assertEqual([r["echo"] for r in results], list(range(12)))


class FailureTests(TransportTestCase):
    scenario = "happy"

    async def test_malformed_frame_fails_pending(self):
        await self.transport.close(timeout=3.0)
        self.transport = AcpTransport(_argv("malformed"), request_timeout=5.0)
        await self.transport.start()
        with self.assertRaises(AcpProtocolError):
            await self.transport.request("session/new", {"cwd": "/tmp", "mcpServers": []})

    async def test_crash_on_start(self):
        await self.transport.close(timeout=3.0)
        # 给足探测窗口：慢机器上解释器冷启动可能超过默认的 150ms，给太小的
        # 窗口会让这条用例在快机器上绿、慢机器上红。
        transport = AcpTransport(
            _argv("crash"), request_timeout=2.0, start_probe=3.0
        )
        with self.assertRaises(AcpProcessError):
            await transport.start()
        await transport.close(timeout=1.0)

    async def test_request_timeout(self):
        await self.transport.close(timeout=3.0)
        self.transport = AcpTransport(_argv("silent"), request_timeout=5.0)
        await self.transport.start()
        # 未知方法：夹具不回应，只能靠超时收敛。
        with self.assertRaises(AcpTimeoutError):
            await self.transport.request("test/never", {"n": 1}, timeout=0.25)

    async def test_big_frame_is_accepted(self):
        await self.transport.close(timeout=3.0)
        self.transport = AcpTransport(_argv("big"), request_timeout=10.0)
        await self.transport.start()
        client = AcpClient(self.transport, cwd="/tmp")
        updates, done = await _drain(client, "k", timeout=15.0)
        self.assertEqual(len(updates), 1)
        self.assertEqual(len(updates[0]["text"]), 2 * 1024 * 1024)
        self.assertEqual(done["stop_reason"], "end_turn")

    async def test_late_response_after_timeout_cannot_leak(self):
        # 第一条超时后它的迟到响应才回来，绝不能顶替第二条请求的结果。
        with self.assertRaises(AcpTimeoutError):
            await self.transport.request(
                "test/echo", {"n": 1, "delay": 0.6}, timeout=0.1
            )
        result = await self.transport.request("test/echo", {"n": 2, "delay": 0.5})
        self.assertEqual(result["echo"], 2)

    async def test_oversized_frame_is_a_protocol_error(self):
        await self.transport.close(timeout=3.0)
        self.transport = AcpTransport(
            _argv("overflow"), request_timeout=5.0, read_limit=4096
        )
        await self.transport.start()
        with self.assertRaises(AcpProtocolError):
            await self.transport.request("session/new", {"cwd": "/tmp", "mcpServers": []})
        # 读循环已经死了：后续请求必须立刻失败，而不是白等满超时。
        self.assertFalse(self.transport.running)
        with self.assertRaises(AcpProcessError):
            await self.transport.request("test/echo", {"n": 1})


class AuthTests(TransportTestCase):
    scenario = "auth_required"

    async def test_session_new_auth_error_is_detectable(self):
        client = AcpClient(self.transport, cwd="/tmp")
        await client.initialize()
        with self.assertRaises(AcpRpcError) as ctx:
            async for _ in client.prompt_once("hi", timeout=5.0):
                pass
        self.assertTrue(ctx.exception.is_auth_required)
        self.assertEqual(ctx.exception.category, "auth")


class AuthFlowTests(TransportTestCase):
    scenario = "happy"

    async def test_authenticate_relays_auth_url(self):
        seen = []
        result = await AcpClient(self.transport, cwd="/tmp").authenticate(
            "internal", on_auth_url=seen.append, timeout=5.0
        )
        self.assertEqual(len(seen), 1)
        self.assertTrue(seen[0].startswith("https://"))
        self.assertEqual(result["authUrl"], seen[0])

    async def test_auth_url_listener_is_removed(self):
        client = AcpClient(self.transport, cwd="/tmp")
        await client.authenticate("internal", timeout=5.0)
        self.assertEqual(self.transport._notification_listeners, [])


class PromptTests(TransportTestCase):
    scenario = "happy"

    async def test_prompt_streams_text_then_done(self):
        client = AcpClient(self.transport, cwd="/tmp")
        updates, done = await _drain(client)
        texts = [u for u in updates if u["kind"] == KIND_TEXT]
        self.assertEqual([u["text"] for u in texts], ["Hello", " world", "!"])
        self.assertEqual(done["kind"], KIND_DONE)
        self.assertEqual(done["stop_reason"], "end_turn")

    async def test_each_turn_opens_its_own_session(self):
        client = AcpClient(self.transport, cwd="/tmp")
        await _drain(client)
        await _drain(client)
        stats = await self.transport.request("test/stats")
        self.assertEqual(stats["session_new"], 2)
        # 光数 session_new 不够：建了两个会话、却把两轮的 prompt 都发进第一个，
        # 一样能过。请求隔离坏掉时，上一轮的上下文会漏进下一轮。
        self.assertEqual(
            len(set(stats["prompt_sessions"])), 2, "两轮必须落在两个不同的会话上"
        )

    async def test_concurrent_turns_do_not_share_a_session(self):
        client = AcpClient(self.transport, cwd="/tmp")
        await asyncio.gather(_drain(client), _drain(client))
        stats = await self.transport.request("test/stats")
        self.assertEqual(stats["session_new"], 2)
        self.assertEqual(
            len(set(stats["prompt_sessions"])), 2, "并发两轮更不许挤进同一个会话"
        )

    async def test_mid_stream_error_propagates(self):
        await self.transport.close(timeout=3.0)
        self.transport = AcpTransport(_argv("mid_error"), request_timeout=5.0)
        await self.transport.start()
        client = AcpClient(self.transport, cwd="/tmp")
        updates = []
        with self.assertRaises(AcpRpcError):
            async for item in client.prompt_once("hi", timeout=5.0):
                updates.append(item)
        self.assertEqual([u["text"] for u in updates], ["partial"])

    async def test_thought_chunks_are_separated_from_content(self):
        client = AcpClient(self.transport, cwd="/tmp")
        updates, _done = await _drain(client)
        self.assertEqual(updates[0]["kind"], KIND_THOUGHT)
        self.assertEqual(updates[0]["text"], "pondering")
        self.assertTrue(any(u["kind"] == KIND_TEXT for u in updates[1:]))


class PermissionTests(TransportTestCase):
    scenario = "permission"

    async def test_server_permission_request_is_answered_not_hung(self):
        client = AcpClient(self.transport, cwd="/tmp")
        updates, done = await _drain(client, timeout=8.0)
        self.assertEqual([u["text"] for u in updates], ["proceeded"])
        self.assertEqual(done["stop_reason"], "end_turn")


class CancelTests(TransportTestCase):
    scenario = "cancel"

    async def test_early_close_sends_cancel_and_keeps_transport_alive(self):
        client = AcpClient(self.transport, cwd="/tmp")
        agen = client.prompt_once("hi", timeout=8.0)
        first = await agen.__anext__()
        self.assertEqual(first["text"], "start")
        await agen.aclose()
        # 取消后管道必须仍然可用：不是靠杀死进程来"结束"一轮。
        echo = await self.transport.request("test/echo", {"n": 7}, timeout=5.0)
        self.assertEqual(echo["echo"], 7)


class LifecycleTests(TransportTestCase):
    scenario = "happy"

    async def test_close_terminates_process(self):
        proc = self.transport._proc
        self.assertIsNotNone(proc)
        await self.transport.close(timeout=2.0)
        self.assertIsNotNone(proc.returncode)
        self.assertFalse(self.transport.running)

    async def test_request_after_close_fails_fast(self):
        await self.transport.close(timeout=2.0)
        with self.assertRaises(AcpProcessError):
            await self.transport.request("test/echo", {"n": 1}, timeout=1.0)

    async def test_session_queue_gets_closed_sentinel(self):
        from astrbot_plugin_workbuddy.core.acp import SESSION_CLOSED

        queue = self.transport.subscribe("sess-x")
        self.transport._close_session_queues()
        self.assertIs(await asyncio.wait_for(queue.get(), 1.0), SESSION_CLOSED)
        self.transport.unsubscribe("sess-x", queue)


class _ResultRacingEofTransport:
    """假传输层：把「EOF 排在结果前面被消费」这个时序钉死。

    真实管道上这靠运气——CLI 写完最后一帧和成功响应就退出时，读循环有可能
    先读到 EOF、先往会话队列里塞 ``SESSION_CLOSED``，轮到等待方把结果写进
    ``outcome`` 已经是下一轮事件循环的事。这里按最坏顺序投喂，确定性复现。
    """

    def __init__(self, result):
        self.queue = asyncio.Queue()
        self.result = result
        self.methods = []
        self.unsubscribed = False

    def subscribe(self, session_id):
        return self.queue

    def unsubscribe(self, session_id, queue):
        self.unsubscribed = True

    async def request(self, method, params, timeout=None):
        self.methods.append(method)
        if len(self.methods) == 1:  # 第一次是 session/new
            return {"sessionId": "sess-1"}
        # 先把 SESSION_CLOSED 塞进队列，再让结果落地。
        self.queue.put_nowait(SESSION_CLOSED)
        await asyncio.sleep(0)
        return self.result

    async def notify(self, method, params, timeout=None):
        return None


class EofBeforeResultTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_closed_session_does_not_discard_a_finished_turn(self):
        transport = _ResultRacingEofTransport({"stopReason": "end_turn"})
        client = AcpClient(transport, cwd=str(_HERE.parent))
        transport.queue.put_nowait(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": "sess-1",
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "messageId": "msg-1",
                        "content": {"type": "text", "text": "partial"},
                    },
                },
            }
        )
        # 旧实现会在看到 SESSION_CLOSED 时当场抛 AcpProcessError，把一条已经
        # 拿到成功响应的回答判成 502。
        done = None
        text = []
        async for item in client.prompt_once("hi", timeout=1.0):
            if item.get("kind") == KIND_DONE:
                done = item
            else:
                text.append(item.get("text"))
        self.assertEqual("".join(t for t in text if t), "partial")
        self.assertIsNotNone(done, "完整的一轮必须收尾成 done，而不是抛错")
        self.assertEqual(done["stop_reason"], "end_turn")
        self.assertTrue(transport.unsubscribed)


class _CapturingTransport:
    """只把报文记下来的假传输层，不发任何东西。"""

    def __init__(self):
        self.sent = []

    async def request(self, method, params, timeout=None):
        self.sent.append((method, params))
        return {"sessionId": "sess-1"}


class PathCwdTests(unittest.IsolatedAsyncioTestCase):
    """``cwd`` 可能是 ``pathlib.Path``（AstrBot 的 ``get_data_dir()`` 就是）。

    真管道上 Path 进 JSON 会炸 ``TypeError: Object of type PosixPath is not JSON
    serializable``，而网关只会把它压成一句 "upstream request failed"，从外部
    完全看不出根因。用例只要求一件事：发出去的报文原样能序列化。
    """

    async def test_a_path_cwd_is_serialised_as_a_string(self):
        transport = _CapturingTransport()
        client = AcpClient(transport, cwd=Path("/tmp/wb-cwd"))
        self.assertEqual(await client._create_session(), "sess-1")
        params = transport.sent[-1][1]
        self.assertIsInstance(params["cwd"], str)
        self.assertEqual(params["cwd"], "/tmp/wb-cwd")
        json.dumps(params)  # 不抛，才算真能在管道上过

    async def test_an_explicit_path_cwd_wins(self):
        transport = _CapturingTransport()
        client = AcpClient(transport, cwd="/tmp/default")
        await client._create_session(cwd=Path("/tmp/override"))
        params = transport.sent[-1][1]
        self.assertEqual(params["cwd"], "/tmp/override")
        json.dumps(params)


if __name__ == "__main__":
    unittest.main(verbosity=2)
