"""ACP 运行时（进程生命周期、登录、配置目录）的契约测试。

这些用例不需要真实账号：登录流程用假 CLI 的 ``auth_flow`` 场景模拟「先推链接、
再等扫码」的时序。真实 CLI 的行为见 evidence/P0-acp-probe.md。

运行::

    python -m unittest tests.test_acp_runtime -v
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.acp import (  # noqa: E402
    AUTH_SUBPATH,
    CONFIG_DIR_NAME,
    AcpError,
    AcpProcessError,
    AcpProtocolError,
    AcpRuntime,
    AcpTimeoutError,
    AcpTransport,
    default_argv,
)
from astrbot_plugin_workbuddy.core.acp import runtime as runtime_module  # noqa: E402

_FAKE = str(_HERE.parent / "fake_acp_cli.py")


def _argv(scenario="happy"):
    return [sys.executable, _FAKE, "--scenario", scenario]


class _SurvivingTransport:
    """``close()`` 之后进程依然活着的假传输层：专门测 logout 的守卫。

    真进程只要还活着就一定会退出，这条分支用夹具跑不到，所以直接替换掉。
    """

    def __init__(self):
        self.close_calls = 0
        self.start_calls = 0

    async def start(self):
        self.start_calls += 1

    async def close(self, *, timeout=5.0):
        self.close_calls += 1

    @property
    def alive(self):
        return True


class DefaultArgvTests(unittest.TestCase):
    def test_acp_mode_disables_tools_by_default(self):
        argv = default_argv("/usr/bin/codebuddy")
        self.assertEqual(argv[0], "/usr/bin/codebuddy")
        self.assertIn("--acp", argv)
        self.assertIn("--acp-transport", argv)
        self.assertEqual(argv[argv.index("--tools") + 1], "")

    def test_tools_can_be_omitted(self):
        self.assertNotIn("--tools", default_argv("codebuddy", tools=None))


class RuntimeTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.config_dir = self.root / CONFIG_DIR_NAME
        self.home_dir = self.root / "cli-home"
        self.runtime = None

    async def asyncTearDown(self):
        if self.runtime is not None:
            await self.runtime.close()

    def make_runtime(self, scenario="happy", **kwargs):
        self.runtime = AcpRuntime(
            argv=_argv(scenario),
            cwd=str(self.root),
            config_dir=str(self.config_dir),
            home_dir=str(self.home_dir),
            request_timeout=5.0,
            **kwargs
        )
        return self.runtime


class _ImpatientTransport(AcpTransport):
    """把优雅退出的窗口压到 0.2 秒：走的还是真的 terminate 路径，只是不用等 3 秒。"""

    async def close(self, *, timeout=5.0):
        await super().close(timeout=0.2)


class ShutdownCancellationTests(RuntimeTestCase):
    """淘汰空闲进程时正好被取消：不能留下一条没人认领的 CLI。"""

    async def test_a_cancelled_shutdown_leaves_no_live_process(self):
        runtime = self.make_runtime("linger_after_eof")
        with mock.patch.object(runtime_module, "AcpTransport", _ImpatientTransport):
            await runtime.ensure_started()
        transport = runtime.transport
        proc = transport._proc

        closer = asyncio.ensure_future(runtime.shutdown())
        await asyncio.sleep(0.05)  # 停在"等进程退出"上
        # 关闭还没结束，引用就必须还在身上：先摘再关的话，取消之后没有任何人
        # 拿得到这条进程，它就成了孤儿。
        self.assertIs(runtime.transport, transport)

        closer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closer
        self.assertIsNotNone(proc.returncode, "取消之后进程还活着")
        self.assertIsNone(runtime.transport, "关干净了就该摘掉引用")


class WedgedProcessTests(RuntimeTestCase):
    """管道坏了但进程还活着：它仍然占着一个名额，不能被当成"没启动"。"""

    async def test_a_wedged_child_still_counts_as_occupied(self):
        runtime = self.make_runtime("malformed")
        await runtime.ensure_started()
        with self.assertRaises(AcpProtocolError):
            await runtime.transport.request(
                "session/new", {"cwd": str(self.root), "mcpServers": []}
            )
        self.assertFalse(runtime.transport.running, "这条管道已经废了")
        self.assertTrue(runtime.transport.alive, "但子进程还在跑")
        self.assertTrue(
            runtime.started, "不计入上限的话，几轮坏帧就能把进程数撑到上限之外"
        )


class StartTests(RuntimeTestCase):
    async def test_config_dir_is_exported_to_child(self):
        runtime = self.make_runtime()
        await runtime.ensure_started()
        stats = await runtime.transport.request("test/stats", {})
        self.assertEqual(stats["config_dir"], str(self.config_dir))

    async def test_home_is_exported_to_child(self):
        # 凭据由 os.homedir() 解析（XDG_DATA_HOME 实测无效），所以必须改 HOME，
        # 否则凭据跑到镜像层，容器重建就丢。
        runtime = self.make_runtime()
        await runtime.ensure_started()
        stats = await runtime.transport.request("test/stats", {})
        self.assertEqual(stats["home"], str(self.home_dir))

    async def test_every_path_variable_points_into_home_dir(self):
        # 变量名拼错、路径拼错（.local/data 之类）不会有别的用例发现。
        runtime = self.make_runtime()
        await runtime.ensure_started()
        stats = await runtime.transport.request("test/stats", {})
        home = str(self.home_dir)
        self.assertEqual(stats["userprofile"], home)
        self.assertEqual(stats["xdg_config"], str(self.home_dir / ".config"))
        self.assertEqual(stats["xdg_cache"], str(self.home_dir / ".cache"))
        self.assertEqual(stats["xdg_data"], str(self.home_dir / ".local" / "share"))

    async def test_child_gets_a_usable_path(self):
        # 子进程 env 是整份替换，默认 cli_path 又是相对名；没有 PATH 就只能靠
        # os.defpath 碰运气。
        runtime = self.make_runtime()
        await runtime.ensure_started()
        stats = await runtime.transport.request("test/stats", {})
        self.assertEqual(stats["path"], os.environ.get("PATH", os.defpath))

    async def test_explicit_env_cannot_redirect_home(self):
        # 调用方塞一份 HOME 进来时，凭据会写到那儿，而 logout 仍按 home_dir 删：
        # 写的是一处、删的是另一处。构造函数必须覆盖掉。
        self.runtime = AcpRuntime(
            argv=_argv(),
            cwd=str(self.root),
            config_dir=str(self.config_dir),
            home_dir=str(self.home_dir),
            env={"HOME": str(self.root / "elsewhere"), "EXTRA": "1"},
            request_timeout=5.0,
        )
        await self.runtime.ensure_started()
        stats = await self.runtime.transport.request("test/stats", {})
        self.assertEqual(stats["home"], str(self.home_dir))

    async def test_relative_home_dir_is_rejected(self):
        # 相对路径会让 realpath 按父进程 cwd 解析，与子进程实际写入的位置分叉。
        with self.assertRaises(ValueError):
            AcpRuntime(argv=_argv(), cwd=str(self.root), home_dir="cli-home")

    async def test_ensure_started_is_single_flight(self):
        runtime = self.make_runtime()
        clients = await asyncio.gather(
            *(runtime.ensure_started() for _ in range(4))
        )
        self.assertTrue(all(c is clients[0] for c in clients))
        self.assertTrue(runtime.started)

    async def test_close_then_start_rebuilds_the_connection(self):
        runtime = self.make_runtime()
        first = await runtime.ensure_started()
        await runtime.close()
        self.assertFalse(runtime.started)
        second = await runtime.ensure_started()
        self.assertIsNot(first, second)
        self.assertTrue(runtime.started)

    async def test_status_starts_unknown_and_lazy(self):
        runtime = self.make_runtime()
        self.assertIsNone(runtime.status()["auth"])
        self.assertFalse(runtime.status()["started"])
        await runtime.ensure_started()
        self.assertTrue(runtime.status()["started"])

    async def test_start_failure_leaves_no_transport(self):
        self.runtime = AcpRuntime(
            argv=["/nonexistent-codebuddy-binary"],
            cwd=str(self.root),
            config_dir=str(self.config_dir),
            request_timeout=2.0,
        )
        with self.assertRaises(AcpProcessError):
            await self.runtime.ensure_started()
        self.assertIsNone(self.runtime.transport)
        self.assertFalse(self.runtime.started)

    async def test_shutdown_blocks_a_late_restart(self):
        runtime = self.make_runtime()
        await runtime.ensure_started()
        await runtime.shutdown()
        with self.assertRaises(AcpError):
            await runtime.ensure_started()

    async def test_a_shutdown_during_discard_blocks_the_restart(self):
        # ``ensure_started`` 已经进门（拿着 ``_start_lock``）、正卡在 ``_discard`` 里
        # 等旧进程退出，``shutdown()`` 这时进来置了 ``_closed`` 再去等锁。醒来之后
        # 不复查就直接 spawn，等于在关机的路上又拉起一条 CLI——shutdown 得陪着它
        # 把握手走完才关得掉。
        runtime = self.make_runtime()
        entered = asyncio.Event()
        gate = asyncio.Event()
        real_discard = runtime._discard

        async def _slow_discard():
            entered.set()
            await gate.wait()
            await real_discard()

        with mock.patch.object(runtime, "_discard", _slow_discard):
            starter = asyncio.ensure_future(runtime.ensure_started())
            await asyncio.wait_for(entered.wait(), 5.0)
            closer = asyncio.ensure_future(runtime.shutdown())
            await asyncio.sleep(0)  # 让 shutdown 先置 ``_closed``，再来等那把锁
            gate.set()

            with self.assertRaises(AcpError):
                await asyncio.wait_for(starter, 10.0)
            await asyncio.wait_for(closer, 10.0)
        self.assertIsNone(runtime.transport, "关机路上不许再拉起一条")

    async def test_a_restart_is_refused_while_the_previous_child_is_alive(self):
        # ``_discard`` 关不掉时会把引用留在身上（那条进程还握着 token）。若这里照旧
        # 拉一条新的，引用一被顶掉，旧的那条就再也没人认领：池子数不到、关不掉。
        runtime = self.make_runtime()
        await runtime.ensure_started()
        await runtime.close()
        runtime._transport = _SurvivingTransport()

        with self.assertRaises(AcpError):
            await runtime.ensure_started()
        self.assertIsInstance(
            runtime.transport,
            _SurvivingTransport,
            "拒绝重启之后引用必须还在，下一轮才收得掉",
        )

    async def test_a_failed_initialize_keeps_a_surviving_transport(self):
        # ``start()`` 成功、``initialize()`` 失败：收尾那一步照例 ``close()``，可它
        # 关不掉时进程还活着。此时若把 ``new_transport`` 当局部变量丢掉，那条 CLI
        # 就再也没人认领：``started`` 报 false、``shutdown`` 找不到它，下一轮还会
        # 再 spawn 一条，两条一起握着 token。
        runtime = self.make_runtime()
        surviving = _SurvivingTransport()

        class _ClientThatCannotInitialize:
            def __init__(self, transport, cwd=None):
                self.transport = transport

            async def initialize(self):
                raise AcpProtocolError("FICTIONAL-INIT-FAILURE")

        with mock.patch.object(
            runtime_module, "AcpTransport", lambda *a, **k: surviving
        ), mock.patch.object(
            runtime_module, "AcpClient", _ClientThatCannotInitialize
        ):
            with self.assertRaises(AcpProtocolError):
                await runtime.ensure_started()

        self.assertIs(runtime.transport, surviving, "进程还活着就不能撒手")
        self.assertTrue(runtime.started, "它还占着一个进程位")
        # 下一轮先收再拉；收不掉就按"上一条还活着"拒绝，绝不顶掉引用另起一条。
        with self.assertRaises(AcpError):
            await runtime.ensure_started()
        self.assertIs(runtime.transport, surviving)


class LoginTests(RuntimeTestCase):
    async def test_login_relays_url_and_marks_state(self):
        runtime = self.make_runtime("auth_flow")
        seen = []
        result = await runtime.login(on_auth_url=seen.append, timeout=5.0)
        self.assertEqual(len(seen), 1)
        self.assertTrue(seen[0].startswith("https://"))
        self.assertEqual(result["authUrl"], seen[0])
        self.assertIs(runtime.auth_state, True)

    async def test_login_timeout_marks_state_as_not_logged_in(self):
        runtime = self.make_runtime("auth_flow")
        with self.assertRaises(AcpTimeoutError):
            await runtime.login(timeout=0.05)
        # 超时了就确实还没登上，不能停在"未知"让状态栏退回旧凭据分支。
        self.assertIs(runtime.auth_state, False)
        self.assertTrue(runtime.started)

    async def test_login_without_any_method_fails_fast(self):
        runtime = self.make_runtime("no_auth_methods")
        with self.assertRaises(AcpError):
            await runtime.login(timeout=2.0)
        self.assertIsNone(runtime.auth_state)


class LogoutTests(RuntimeTestCase):
    def _seed_credentials(self):
        """按真实 CLI 的落盘位置造一份凭据。"""
        cred_dir = self.home_dir.joinpath(".local", "share", *AUTH_SUBPATH)
        cred_dir.mkdir(parents=True, exist_ok=True)
        real = cred_dir / "Tencent-Cloud.coding-copilot.info"
        real.write_text('{"auth": {"accessToken": "x"}}', encoding="utf-8")
        (cred_dir / "Tencent-Cloud.coding-copilot.info.logged-out").write_text(
            "", encoding="utf-8"
        )
        return cred_dir

    async def test_logout_purges_plugin_owned_config_dir(self):
        runtime = self.make_runtime()
        self.config_dir.mkdir(parents=True)
        (self.config_dir / "token.bin").write_bytes(b"secret")
        await runtime.logout(purge_config=True)
        self.assertFalse(self.config_dir.exists())
        self.assertFalse(runtime.started)

    async def test_logout_purges_the_real_credential_dir(self):
        # 凭据不在 config_dir 里。只删配置就让"已断开连接"变成谎话，这条钉住它。
        runtime = self.make_runtime()
        cred_dir = self._seed_credentials()
        self.assertEqual(Path(runtime.credential_dir), cred_dir)
        await runtime.logout(purge_config=True)
        self.assertFalse(cred_dir.exists())

    async def test_logout_stops_the_process_and_purges(self):
        runtime = self.make_runtime()
        await runtime.ensure_started()
        transport = runtime.transport
        self.config_dir.mkdir(parents=True, exist_ok=True)
        await runtime.logout(purge_config=True)
        self.assertFalse(transport.running)
        self.assertFalse(transport.alive)
        self.assertFalse(self.config_dir.exists())

    async def test_purge_refuses_a_foreign_directory(self):
        # 名字不对就不删——但必须**报错**。静默返回等于告诉命令层"已退出登录"，
        # 而目录还在，正是这次要根除的那类谎话。
        stranger = self.root / "not-codebuddy"
        stranger.mkdir()
        (stranger / "keep.txt").write_text("keep", encoding="utf-8")
        self.runtime = AcpRuntime(
            argv=_argv(),
            cwd=str(self.root),
            config_dir=str(stranger),
            home_dir=str(self.home_dir),
        )
        with self.assertRaises(AcpError):
            await self.runtime.logout(purge_config=True)
        self.assertTrue((stranger / "keep.txt").exists())

    async def test_purge_refuses_a_symlinked_credential_dir(self):
        # 指向别处的同名目录可能落在共享卷上，顺着删就是删别人的东西。
        real = self.root / "shared-credentials"
        real.mkdir()
        (real / "keep.txt").write_text("keep", encoding="utf-8")
        cred_dir = self.home_dir / ".local" / "share" / Path(*AUTH_SUBPATH)
        cred_dir.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.symlink(str(real), str(cred_dir))
        except (OSError, NotImplementedError):
            self.skipTest("symlinks are not available on this platform")
        runtime = self.make_runtime()
        with self.assertRaises(AcpError):
            await runtime.logout(purge_config=True)
        self.assertTrue((real / "keep.txt").exists())

    async def test_purge_without_home_dir_fails_loudly(self):
        # 拿不准凭据在哪就不能回复"已断开连接"。宁可报错让命令层说"凭据未能清除"。
        self.config_dir.mkdir(parents=True)
        (self.config_dir / "token.bin").write_bytes(b"secret")
        self.runtime = AcpRuntime(
            argv=_argv(), cwd=str(self.root), config_dir=str(self.config_dir)
        )
        with self.assertRaises(AcpError):
            await self.runtime.logout(purge_config=True)
        # 凭据先删、配置后删：凭据删不了就该停在原地，别把配置也搭进去。
        self.assertTrue((self.config_dir / "token.bin").exists())
        # 状态字段记的是结果不是意图；没退成就不许显示未登录。
        self.assertIsNot(self.runtime.auth_state, False)

    async def test_purge_is_refused_while_the_process_survives(self):
        # 活着的 CLI 手里还有 token，会把凭据写回来。close() 杀不掉时必须拒绝，
        # 而不是先删再说。
        cred_dir = self._seed_credentials()
        runtime = self.make_runtime()
        runtime._transport = _SurvivingTransport()
        with self.assertRaises(AcpError):
            await runtime.logout(purge_config=True)
        self.assertTrue(cred_dir.exists())
        self.assertIsNot(runtime.auth_state, False)

    async def test_logout_without_purge_keeps_the_directory(self):
        runtime = self.make_runtime()
        self.config_dir.mkdir(parents=True)
        cred_dir = self._seed_credentials()
        await runtime.logout()
        self.assertTrue(self.config_dir.exists())
        self.assertTrue(cred_dir.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
