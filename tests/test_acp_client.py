"""``AcpClient`` 的一轮收尾时序：**先停下发送，再喊取消**。

两件事反过来的窗口很小但真实存在：``session/cancel`` 已经写出去，而那条发送任务
手里还攥着一个没落地的 ``session/prompt``；它随后落地，CLI 就开始生成一轮没人
取消的回答。ACP 的 cancel 打的是"正在进行的那一轮"，追不上后面才来的 prompt。

运行::

    python -m unittest tests.test_acp_client -v
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import unittest
from pathlib import Path

_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.acp.client import AcpClient  # noqa: E402
from astrbot_plugin_workbuddy.core.acp.protocol import (  # noqa: E402
    M_SESSION_CANCEL,
    M_SESSION_PROMPT,
)


class _TransportThatRecordsWrites:
    """记下"真写出去"的先后；``prompt`` 那一次先卡在闸门上。

    日志式地记在 ``request`` 入口没有用——入口处还没写出去。要的是"落到管道上"，
    所以记在闸门之后。
    """

    def __init__(self):
        self.writes = []
        self.prompt_started = asyncio.Event()
        self.gate = asyncio.Event()

    def subscribe(self, session_id):
        return asyncio.Queue()

    def unsubscribe(self, session_id, queue):
        pass

    async def request(self, method, params, timeout=None):
        if method == M_SESSION_PROMPT:
            self.prompt_started.set()
            await self.gate.wait()
            self.writes.append(method)
            return {"stopReason": "end_turn"}
        return {}

    async def notify(self, method, params):
        self.writes.append(method)
        if method == M_SESSION_CANCEL:
            # 取消通知一写出去，那条被挡住的 prompt 就有机会落地了——这正是要防的。
            self.gate.set()
            await asyncio.sleep(0)


class _TransportWhosePromptNeverFinishes:
    """prompt 一直挂着；**收到取消也不立刻死**，留给"外层取消落在收尸那一等上"的窗口。

    收尸那一步最要命的不是"取消喊没喊出去"，而是"等它收完"这一等会不会把调用方的
    取消一起吞掉。要检验这件事，就得让那条任务在收到取消之后还活上一小会儿：只有
    这样，调用方的取消才会正好落在"等它"的那个 await 上。
    """

    def __init__(self):
        self.queue = asyncio.Queue()
        self.prompt_started = asyncio.Event()
        self.cancel_seen = asyncio.Event()
        self.let_it_die = asyncio.Event()
        self.never = asyncio.Event()
        self.writes = []

    def subscribe(self, session_id):
        return self.queue

    def unsubscribe(self, session_id, queue):
        pass

    async def request(self, method, params, timeout=None):
        if method != M_SESSION_PROMPT:
            return {}
        self.prompt_started.set()
        try:
            await self.never.wait()
        except asyncio.CancelledError:
            # 先把"喊过取消了"报出去，再等测试放行。这段窗口里那条任务确实还没死。
            self.cancel_seen.set()
            await self.let_it_die.wait()
            raise
        return {"stopReason": "end_turn"}

    async def notify(self, method, params):
        self.writes.append(method)


class CancelOrderingTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_cancel_notification_never_lands_before_the_prompt_it_cancels(self):
        transport = _TransportThatRecordsWrites()
        client = AcpClient(transport, cwd=None)

        stream = client._prompt_once("s1", "hi")
        consumer = asyncio.create_task(stream.__anext__())
        await asyncio.wait_for(transport.prompt_started.wait(), 5.0)

        # 消费者在"prompt 还没写出去"的这一刻退出。
        consumer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await consumer

        self.assertNotIn(
            M_SESSION_PROMPT,
            transport.writes,
            "取消通知之后不许再有一条 prompt 落地：那一轮从此没人取消了",
        )
        self.assertIn(M_SESSION_CANCEL, transport.writes, "该发的取消还是要发")

    async def test_cancelling_the_caller_while_it_collects_the_corpse_is_not_swallowed(
        self,
    ):
        """调用方在"等那条任务收尸"的时候被取消，这个取消必须照常往外抛。

        ``await task`` 的写法在这里会哑掉：直接 await 一条任务时，取消会顺着这次
        await 传到那条任务上；等它真的收完，``task.cancelled()`` 也已经变成 True，
        于是"是调用方被取消了"和"是我们自己取消了它"再也分不出来——一个
        ``except CancelledError: if not task.cancelled(): raise`` 就把调用方的
        取消一起咽了，``aclose()`` 正常返回。用 ``gather`` 收尸不会：任务自己的
        结局被收成返回值，调用方的取消原样往上走。
        """
        transport = _TransportWhosePromptNeverFinishes()
        client = AcpClient(transport, cwd=None)
        stream = client._prompt_once("s1", "hi")

        first = asyncio.create_task(stream.__anext__())
        await asyncio.wait_for(transport.prompt_started.wait(), 5.0)
        # 随便一条更新都行：要的只是让生成器停在 yield 上，好从外面关它。
        transport.queue.put_nowait({"params": {"update": {"sessionUpdate": "plan"}}})
        await asyncio.wait_for(first, 5.0)

        # 关它 —— 走进 finally，卡在"等那条任务收尸"上。
        closing = asyncio.create_task(stream.aclose())
        await asyncio.wait_for(transport.cancel_seen.wait(), 5.0)
        for _ in range(3):
            await asyncio.sleep(0)

        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(closing, 5.0)

    async def test_the_corpse_is_still_collected_when_the_caller_goes_away(self):
        """上一条的另一半：调用方被取消之后，那条任务不许留在那里跑。"""
        transport = _TransportWhosePromptNeverFinishes()
        client = AcpClient(transport, cwd=None)
        stream = client._prompt_once("s1", "hi")

        first = asyncio.create_task(stream.__anext__())
        await asyncio.wait_for(transport.prompt_started.wait(), 5.0)
        transport.queue.put_nowait({"params": {"update": {"sessionUpdate": "plan"}}})
        await asyncio.wait_for(first, 5.0)

        closing = asyncio.create_task(stream.aclose())
        await asyncio.wait_for(transport.cancel_seen.wait(), 5.0)
        for _ in range(3):
            await asyncio.sleep(0)

        closing.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(closing, 5.0)

        prompts = [t for t in asyncio.all_tasks() if t.get_name() == "wb-acp-prompt"]
        self.assertEqual(prompts, [], "收尸没做完：那条 session/prompt 还在跑")


if __name__ == "__main__":
    unittest.main(verbosity=2)
