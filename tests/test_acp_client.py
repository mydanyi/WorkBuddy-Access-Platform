"""``AcpClient`` 的一轮收尾时序：**先停下发送，再喊取消**。

两件事反过来的窗口很小但真实存在：``session/cancel`` 已经写出去，而那条发送任务
手里还攥着一个没落地的 ``session/prompt``；它随后落地，CLI 就开始生成一轮没人
取消的回答。ACP 的 cancel 打的是"正在进行的那一轮"，追不上后面才来的 prompt。

运行::

    python -m unittest tests.test_acp_client -v
"""

from __future__ import annotations

import asyncio
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
