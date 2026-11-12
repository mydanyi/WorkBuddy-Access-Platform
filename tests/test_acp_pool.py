"""按模型分进程的池：谁在哪条进程上、进程多了淘汰谁、关的时候用哪把刀。

为什么这里用假运行时而不是真 CLI：这层要验的是**池的记账**——上限、占用、淘汰顺序、
以及"登录成功后重启全部进程"这件事到底调了哪个方法。真进程生命周期、登录时序、目录
清理由 ``tests/test_acp_runtime.py`` 用假 CLI 覆盖；ACP 协议本身由
``tests/test_acp_transport.py`` 覆盖。三边不重复。

有一条特别值得盯：淘汰必须调 ``shutdown`` 而不是 ``close``。``close`` 之后
``ensure_started`` 还能把进程再拉起来，那条进程就活过了插件生命周期，再也没人回收。
这个区别被 ``test_eviction_shuts_down_instead_of_closing`` 钉住。

模型 ID 全是虚构样本。
"""

import asyncio
import sys
import unittest
from pathlib import Path
from unittest import mock

_HERE = Path(__file__).resolve()
_PLUGIN_ROOT = _HERE.parents[1]
_SOURCE_ROOT = _HERE.parents[2]
for _path in (str(_PLUGIN_ROOT), str(_SOURCE_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import astrbot_plugin_workbuddy.core.acp.pool as pool_module  # noqa: E402
from astrbot_plugin_workbuddy.core.acp import AcpError  # noqa: E402
from astrbot_plugin_workbuddy.core.acp.pool import AcpRuntimePool  # noqa: E402

ALPHA = "wb-alpha"
BETA = "wb-beta"
GAMMA = "wb-gamma"


class _FakeRuntime:
    """只记账的运行时：记录自己被怎么关的、被谁问过状态。"""

    def __init__(self, model, *, started=False, auth=None):
        self.model = model
        self.started = started
        self.auth_state = auth
        self.starts = 0
        self.closes = 0
        self.shutdowns = 0
        self.logouts = []

    async def ensure_started(self):
        self.starts += 1
        self.started = True
        return self

    async def close(self):
        self.closes += 1
        # 真运行时的 ``close()`` 会把 transport 丢掉，于是 ``started`` 变假、下次
        # 要用必须先 ``ensure_started()``。假运行时也得照这个来，否则"关掉之后还能
        # 直接 stream"这种误用在这里会看起来毫无问题。
        self.started = False

    async def shutdown(self):
        self.shutdowns += 1
        self.started = False

    async def logout(self, *, purge_config=False):
        self.logouts.append(purge_config)

    def note_session_ok(self):
        self.auth_state = True

    def note_auth_failure(self):
        self.auth_state = False

    def note_logged_out(self):
        """真运行时靠它把"凭据已删"和"这一轮撞了 401"分开记。"""
        self.logged_out = getattr(self, "logged_out", 0) + 1
        self.auth_state = False


def _start(*runtimes):
    """把这几条标成"进程真的起来了"——池的 running 视图只看这个。"""
    for runtime in runtimes:
        runtime.started = True


class _PoolCase(unittest.IsolatedAsyncioTestCase):
    def make_pool(self, *, max_processes=2, primary_model=None):
        """造一个池；``self.created`` 按创建顺序留着每一条，供断言引用。"""
        self.created = []

        def factory(model):
            runtime = _FakeRuntime(model)
            self.created.append(runtime)
            return runtime

        self.pool = AcpRuntimePool(
            factory, max_processes=max_processes, primary_model=primary_model
        )
        return self.pool


class ConstructionTests(unittest.TestCase):
    """构造参数是编程错误就当场抛，绝不静默降级。"""

    def test_max_processes_must_be_a_positive_integer(self):
        for bad in (0, -1, True, False, 1.5, "2", None):
            with self.subTest(value=bad):
                with self.assertRaises(ValueError):
                    AcpRuntimePool(lambda model: None, max_processes=bad)

    def test_factory_must_be_callable(self):
        with self.assertRaises(ValueError):
            AcpRuntimePool(None)
        with self.assertRaises(ValueError):
            AcpRuntimePool("not-callable")

    def test_primary_model_only_accepts_a_string(self):
        # 一个非字符串的 primary_model 会被当成"没指定"，而不是变成一个奇怪的键。
        pool = AcpRuntimePool(lambda model: _FakeRuntime(model), primary_model=123)
        self.assertIsNone(pool.status()["primary_model"])


class GetAndStatusTests(_PoolCase):
    """同一模型永远同一条进程；状态页是只读快照。"""

    def test_one_runtime_per_model_and_the_default_is_its_own(self):
        pool = self.make_pool()
        first = pool.get(ALPHA)
        self.assertIs(pool.get(ALPHA), first, "同一模型必须复用同一条进程")
        self.assertEqual(len(self.created), 1, "复用不该再造一条")
        other = pool.get(BETA)
        self.assertIsNot(other, first)
        default = pool.get(None)
        self.assertIsNot(default, first)
        self.assertEqual(default.model, None)

        self.assertEqual(pool.status()["models"], [None, ALPHA, BETA])
        self.assertEqual(pool.status()["max_processes"], 2)

    def test_blank_and_non_string_models_normalize_to_the_default(self):
        pool = self.make_pool()
        default = pool.get(None)
        for blank in ("", "   ", 0, False, [], {}):
            with self.subTest(value=blank):
                self.assertIs(pool.get(blank), default)

    def test_get_only_creates_the_object_and_never_spawns(self):
        pool = self.make_pool()
        runtime = pool.get(ALPHA)
        self.assertEqual(runtime.starts, 0, "get() 不许拉起进程")
        self.assertEqual(pool.status()["running"], [])

        # 拉起是运行时自己的事，池只如实转述它的 started。
        _start(runtime)
        self.assertEqual(pool.status()["running"], [ALPHA])

    def test_running_lists_only_started_runtimes_default_first(self):
        pool = self.make_pool()
        pool.get(GAMMA)
        default = pool.get(None)
        alpha = pool.get(ALPHA)
        _start(default, alpha)
        status = pool.status()
        self.assertEqual(status["models"], [None, ALPHA, GAMMA])
        self.assertEqual(status["running"], [None, ALPHA])

    def test_status_never_mutates_the_pool(self):
        pool = self.make_pool()
        pool.get(ALPHA)
        pool.get(None)
        before = pool.status()
        for _ in range(3):
            pool.status()
        self.assertEqual(pool.status(), before)
        self.assertEqual(len(self.created), 2, "读状态不许造出新运行时")

    def test_primary_is_the_configured_model_and_defaults_to_the_cli_default(self):
        pool = self.make_pool(primary_model=ALPHA)
        self.assertIs(pool.primary, pool.get(ALPHA))
        self.assertEqual(pool.status()["primary_model"], ALPHA)

        plain = self.make_pool()
        self.assertIs(plain.primary, plain.get(None))
        self.assertIsNone(plain.status()["primary_model"])


class AuthStateTests(_PoolCase):
    """整池登录态：一条真跑通过就算登录；全都明确失败才算没登录。"""

    def test_an_empty_pool_has_no_opinion(self):
        pool = self.make_pool()
        self.assertIsNone(pool.auth_state)

    def test_one_confirmed_runtime_makes_the_whole_pool_logged_in(self):
        pool = self.make_pool()
        alpha = pool.get(ALPHA)
        pool.get(BETA)
        alpha.auth_state = False
        gamma = pool.get(GAMMA)
        gamma.auth_state = None
        self.assertIsNone(pool.auth_state, "明确失败 + 未知，不该猜成已登录")

        gamma.auth_state = True
        self.assertIs(pool.auth_state, True, "有一条真跑通过就是已登录")

    def test_every_runtime_explicitly_failing_is_logged_out(self):
        pool = self.make_pool()
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        alpha.auth_state = False
        beta.auth_state = False
        self.assertIs(pool.auth_state, False)


class TrimTests(_PoolCase):
    """淘汰：只动空闲的、最久未用的、永远不动主运行时。"""

    async def test_no_eviction_below_the_cap(self):
        pool = self.make_pool(max_processes=2)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)
        self.assertEqual(await pool.trim(), 0)
        self.assertEqual(alpha.shutdowns, 0)
        self.assertEqual(beta.shutdowns, 0)

    async def test_eviction_shuts_down_instead_of_closing(self):
        pool = self.make_pool(max_processes=1)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        self.assertEqual(await pool.trim(), 1)
        self.assertEqual(alpha.shutdowns, 1, "淘汰必须 shutdown，不能只是 close")
        self.assertEqual(alpha.closes, 0, "close 之后还能被重新拉起来")
        self.assertEqual(beta.shutdowns, 0)
        # 被淘汰的那条从映射里摘掉，下次领取会造一条新的。
        self.assertEqual(pool.status()["models"], [BETA])
        self.assertIsNot(pool.get(ALPHA), alpha)
        self.assertEqual(len(self.created), 3)

    async def test_the_least_recently_used_idle_runtime_goes_first(self):
        pool = self.make_pool(max_processes=2)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        gamma = pool.get(GAMMA)
        _start(alpha, beta, gamma)
        # 领取顺序：ALPHA, BETA, GAMMA。再碰一下 ALPHA，让它变成最近用过的。
        pool.get(ALPHA)

        self.assertEqual(await pool.trim(), 1)
        self.assertEqual(beta.shutdowns, 1, "最久未用的该先走")
        self.assertEqual(alpha.shutdowns, 0)
        self.assertEqual(gamma.shutdowns, 0)

    async def test_the_primary_runtime_is_never_evicted(self):
        pool = self.make_pool(max_processes=1, primary_model=ALPHA)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)
        # ALPHA 是主运行时，哪怕它最久未用也不许动。
        pool.get(BETA)

        self.assertEqual(await pool.trim(), 1)
        self.assertEqual(alpha.shutdowns, 0, "主运行时永不淘汰")
        self.assertEqual(beta.shutdowns, 1)

    async def test_a_cancelled_trim_still_shuts_down_the_rest(self):
        # 这批已经被摘出映射了，池子不会再回头找它们：取消时撒手，剩下那几条既
        # 不在池子里、进程也还活着，等于凭空漏了 CLI。
        pool = self.make_pool(max_processes=1)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        gamma = pool.get(GAMMA)
        _start(alpha, beta, gamma)

        gate = asyncio.Event()
        release = alpha.shutdown

        async def _slow_shutdown():
            await gate.wait()  # 卡在第一条上，好让取消落在收尾途中
            await release()

        alpha.shutdown = _slow_shutdown

        trimmer = asyncio.ensure_future(pool.trim())
        await asyncio.sleep(0.05)
        trimmer.cancel()
        gate.set()
        with self.assertRaises(asyncio.CancelledError):
            await trimmer
        self.assertEqual(alpha.shutdowns, 1, "被打断的那条也要关完")
        self.assertEqual(beta.shutdowns, 1, "排在后面的不能因为取消就不关了")
        self.assertEqual(gamma.shutdowns, 0, "上限之内那条留着")

    async def test_a_fully_busy_pool_stays_over_the_cap_rather_than_kill_a_stream(self):
        # 生产里的真实顺序就是"先占住、再拉起"：后端在 lease 里才 ensure_started。
        # 所以第二条请求入场时 BETA 还没起，逃过了那一次淘汰；等它起来，两条都在
        # 出字、上限又只有 1，这时唯一正确的选择是暂时超额。
        pool = self.make_pool(max_processes=1)
        async with pool.lease(ALPHA) as alpha:
            await alpha.ensure_started()
            async with pool.lease(BETA) as beta:
                await beta.ensure_started()
                self.assertEqual(pool.status()["running"], [ALPHA, BETA])
                self.assertEqual(
                    await pool.trim(), 0, "宁可暂时超额，也不动正在出字的进程"
                )
                self.assertEqual(alpha.shutdowns, 0)
                self.assertEqual(beta.shutdowns, 0)
        # 最后一条放开的那一刻就收掉了，不用等谁再来 trim 一次。
        self.assertEqual(
            alpha.shutdowns + beta.shutdowns, 1, "两条都放开之后只留一条"
        )
        self.assertEqual(len(pool.status()["running"]), 1, "回到上限以内")
        self.assertEqual(await pool.trim(), 0, "已经在上限内，没有可收的")

    async def test_entering_a_lease_trims_the_idle_runtimes_first(self):
        pool = self.make_pool(max_processes=1)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        async with pool.lease(ALPHA) as runtime:
            self.assertIs(runtime, alpha)
            # 领取本身就先把空闲的那条收掉了：被占着的不动，让位的只能是 BETA。
            self.assertEqual(alpha.shutdowns, 0)
            self.assertEqual(beta.shutdowns, 1)
            self.assertEqual(pool.status()["running"], [ALPHA])
            self.assertEqual(await pool.trim(), 0, "已经在上限内了")

    async def test_the_lease_counter_returns_to_zero_after_the_outer_lease(self):
        pool = self.make_pool(max_processes=1)
        alpha = pool.get(ALPHA)
        _start(alpha)

        async with pool.lease(ALPHA):
            self.assertEqual(pool.busy(ALPHA), 1)
            async with pool.lease(ALPHA):
                self.assertEqual(pool.busy(ALPHA), 2, "嵌套领取要累加，不能覆盖")
            self.assertEqual(pool.busy(ALPHA), 1, "退出一层只减一层")

        # 直接看计数：不靠"该淘汰谁"去反推，淘汰策略一改也不会误报。
        self.assertEqual(pool.busy(ALPHA), 0, "两层都退出了就该归零")
        self.assertEqual(pool.busy(BETA), 0, "没领过的模型读出来也是 0")

        # 归零之后它才重新可淘汰。这条保留 LRU 视角，因为它验的是另一半：
        # 计数归零真的放开了淘汰。
        beta = pool.get(BETA)
        _start(beta)
        self.assertEqual(await pool.trim(), 1)
        self.assertEqual(alpha.shutdowns, 1, "两层都退出了，它就该能被淘汰")
        self.assertEqual(beta.shutdowns, 0)


    async def test_an_eviction_that_leaves_the_child_alive_is_not_counted(self):
        # ``shutdown()`` 正常返回不等于进程没了：``AcpRuntime.close()`` 在子进程死活
        # 不退时就是这么返回的。把它算成"关掉了"，池子就会报出一个不存在的空位。
        pool = self.make_pool(max_processes=1, primary_model=ALPHA)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        async def _returns_without_killing():
            return None  # 进程还在：started 保持真

        beta.shutdown = _returns_without_killing

        self.assertEqual(await pool.trim(), 0, "进程还在，不许报成关掉了")

    async def test_a_child_that_survives_eviction_still_counts_against_the_cap(self):
        # 漏网的那条已经不在映射里，可它还是占着一条进程位。只数映射，池子就会以为
        # 还有一个空位：于是它不再收别的进程，超出去的那部分一直挂在那儿，而且每漏
        # 一次就多放宽一条。
        pool = self.make_pool(max_processes=2, primary_model=ALPHA)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        gamma = pool.get(GAMMA)
        _start(alpha, beta, gamma)

        async def _never_dies():
            return None  # 进程还在：started 保持真

        beta.shutdown = _never_dies

        self.assertEqual(await pool.trim(), 0, "淘汰这条没关掉，不许报成关掉了")
        self.assertTrue(beta.started, "杀不掉的那条仍然占着位子")
        self.assertEqual(pool.status()["models"], [ALPHA, GAMMA])

        # 第二轮到它算账：映射里只剩两条、看起来正好到上限，可账上那条还活着。
        self.assertEqual(await pool.trim(), 1, "它也算位子，所以还得再收一条")
        self.assertEqual(gamma.shutdowns, 1)
        self.assertEqual(alpha.shutdowns, 0, "主运行时永远不动")
        self.assertEqual(pool.status()["models"], [ALPHA])

    async def test_the_next_trim_retries_a_stray_instead_of_leaving_it_there(self):
        # 留在账上不是留个纪念：下一轮超编时先把它再关一次。它已经不在映射里、谁也
        # 领不走，关掉就是白赚一个位子——比再淘汰一条正在待命的进程划算。
        pool = self.make_pool(max_processes=1, primary_model=ALPHA)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        attempts = []

        async def _dies_on_the_second_try():
            attempts.append(1)
            if len(attempts) > 1:
                beta.started = False

        beta.shutdown = _dies_on_the_second_try

        self.assertEqual(await pool.trim(), 0, "第一次没关掉，不许报成关掉了")
        self.assertTrue(beta.started)
        self.assertEqual(pool.status()["models"], [ALPHA])

        self.assertEqual(await pool.trim(), 1, "下一轮超编时再试一次，这次关掉了")
        self.assertFalse(beta.started)
        self.assertEqual(len(attempts), 2)
        self.assertEqual(alpha.shutdowns, 0, "重试就能收掉，不必动待命的那条")


class CloseAllTests(_PoolCase):
    """``close_all`` 只关不摘：登出后仍要能重新拉起同一条。"""

    async def test_close_all_closes_every_runtime_but_keeps_the_mapping(self):
        pool = self.make_pool()
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        await pool.close_all()
        self.assertEqual(alpha.closes, 1)
        self.assertEqual(beta.closes, 1)
        self.assertEqual(alpha.shutdowns, 0, "close_all 不是永久作废")

        # 映射还在：同一个模型拿回的还是同一条对象，登录凭据握着的那条引用不会失联。
        self.assertIs(pool.get(ALPHA), alpha)
        self.assertEqual(pool.status()["models"], [ALPHA, BETA])
        # 但它**已经关了**：拿回来不能直接用，得先 ensure_started 重新拉起。
        self.assertFalse(alpha.started, "close_all 之后那条运行时是停着的")
        self.assertEqual(pool.status()["running"], [])
        await alpha.ensure_started()
        self.assertEqual(alpha.starts, 1)
        self.assertTrue(alpha.started)

    async def test_a_lease_is_refused_while_logging_out(self):
        pool = self.make_pool(primary_model=ALPHA)
        alpha = pool.get(ALPHA)
        _start(alpha)

        entered = []

        async def logout(*, purge_config=False):
            # 假装登出要花点时间：就在这个窗口里，另一条聊天请求进来了。
            entered.append(purge_config)
            with self.assertRaises(AcpError):
                async with pool.lease(BETA):
                    pass  # pragma: no cover - 走不到这里

        alpha.logout = logout
        await pool.logout(purge_config=True)
        self.assertEqual(entered, [True])

        # 登出结束之后正常放行：拒绝只覆盖登出那一段，不是永久关闭。
        async with pool.lease(BETA) as runtime:
            self.assertEqual(runtime.model, BETA)

    async def test_the_pool_is_usable_again_after_a_failed_logout(self):
        pool = self.make_pool(primary_model=ALPHA)
        alpha = pool.get(ALPHA)

        async def boom(*, purge_config=False):
            raise RuntimeError("FICTIONAL-LOGOUT-FAILURE")

        alpha.logout = boom
        with self.assertRaises(RuntimeError):
            await pool.logout(purge_config=True)
        # 登出失败也要把闸门放回去，否则池子从此拒绝一切聊天。
        async with pool.lease(ALPHA) as runtime:
            self.assertIs(runtime, alpha)

    async def test_close_all_survives_a_runtime_that_refuses_to_close(self):
        pool = self.make_pool()
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        async def boom():
            raise RuntimeError("FICTIONAL-CLOSE-FAILURE")

        alpha.close = boom
        await pool.close_all()  # 一条关不掉不能拖累另一条
        self.assertEqual(beta.closes, 1)

    async def test_a_second_cancellation_still_waits_for_the_batch(self):
        # 第一次取消之后我们在等收尾；等的过程中再来一次，单次 suppress 就会放行——
        # 方法抛出去而剩下那几条还在关，之后事件循环把它们一并取消就没人管了。
        pool = self.make_pool()
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        gate = asyncio.Event()
        entered = asyncio.Event()
        real_close = alpha.close

        async def _slow_close():
            entered.set()
            await gate.wait()
            await real_close()

        alpha.close = _slow_close

        closer = asyncio.ensure_future(pool.close_all())
        await entered.wait()
        closer.cancel()
        await asyncio.sleep(0.05)  # 让第一次取消落地，走进"等收尾"那一段
        closer.cancel()
        await asyncio.sleep(0.05)
        self.assertFalse(closer.done(), "第二次取消也不许提前抛出去")

        gate.set()
        with self.assertRaises(asyncio.CancelledError):
            await closer
        self.assertEqual(alpha.closes, 1)
        self.assertEqual(beta.closes, 1, "排在后面的不能因为二次取消就留下")

    async def test_logout_closes_everything_before_purging_credentials(self):
        pool = self.make_pool(primary_model=ALPHA)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        await pool.logout(purge_config=True)
        # 顺序很重要：先关全部进程，再删凭据。反过来，还活着的那条会把 token
        # 原样写回去，而命令已经回复"已退出登录"了。
        self.assertEqual(alpha.closes, 1)
        self.assertEqual(beta.closes, 1)
        self.assertEqual(alpha.logouts, [True])
        self.assertEqual(beta.logouts, [], "登出只在主运行时上做一次")

    async def test_logout_refuses_to_purge_while_a_secondary_still_runs(self):
        # 凭据只有一份。主运行时有关不掉的守卫，备用进程没有——只要有一条还活着，
        # 删掉凭据之后它照样能刷新、能把 token 写回磁盘。
        pool = self.make_pool(primary_model=ALPHA)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        async def _stubborn_close():
            return None  # 关不掉：started 保持真

        beta.close = _stubborn_close

        with self.assertRaises(AcpError):
            await pool.logout(purge_config=True)
        self.assertEqual(alpha.logouts, [], "没关干净就不许删凭据")

    async def test_logout_waits_for_a_lease_taken_before_the_gate(self):
        # 闸门只拦新请求。已经进来的那条还握着运行时，它的 ensure_started() 会在
        # close_all 之后把 CLI 重新拉起来——握着还没删的 token，然后凭据被删。
        pool = self.make_pool(max_processes=2)
        alpha = pool.get(ALPHA)
        _start(alpha)
        entered = asyncio.Event()
        release = asyncio.Event()

        async def _hold_the_lease():
            async with pool.lease(ALPHA):
                entered.set()
                await release.wait()

        holder = asyncio.ensure_future(_hold_the_lease())
        await entered.wait()

        logging_out = asyncio.ensure_future(pool.logout(purge_config=True))
        await asyncio.sleep(0.05)
        self.assertEqual(alpha.closes, 0, "在手租约还没退，不能先关进程")

        release.set()
        await holder
        await logging_out
        self.assertEqual(alpha.closes, 1, "租约退了之后照常关")

    async def test_logout_waits_for_an_eviction_that_is_still_closing(self):
        # trim() 先把运行时摘出映射、再去 await 关进程。摘掉之后 close_all() 就看不见
        # 它了，而那一刻子进程还活着——登出正好落在这段里，就会删掉凭据、留下一条
        # 握着 token 的活进程。所以摘的同时要记账，登出照着账等。
        pool = self.make_pool(max_processes=1, primary_model=ALPHA)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        gate = asyncio.Event()
        real_shutdown = beta.shutdown

        async def _slow_shutdown():
            await gate.wait()  # 正在关，但还没关掉：started 仍是真
            await real_shutdown()

        beta.shutdown = _slow_shutdown

        trimmer = asyncio.ensure_future(pool.trim())
        await asyncio.sleep(0.05)
        self.assertEqual(pool.status()["models"], [ALPHA], "它已经从映射里摘掉了")
        self.assertTrue(beta.started, "可这一刻子进程还活着")

        logging_out = asyncio.ensure_future(pool.logout(purge_config=True))
        await asyncio.sleep(0.05)
        self.assertEqual(alpha.logouts, [], "淘汰没落地之前不许删凭据")

        gate.set()
        await trimmer
        await logging_out
        self.assertEqual(alpha.logouts, [True], "关干净了才删")
        self.assertFalse(beta.started)

    async def test_logout_refuses_to_purge_while_an_eviction_never_dies(self):
        # 等不到就交给存活检查：那条进程真的死不了，凭据就一个字节也不许删。
        pool = self.make_pool(max_processes=1, primary_model=ALPHA)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        gate = asyncio.Event()

        async def _stuck_shutdown():
            await gate.wait()  # 关不掉：started 保持真

        beta.shutdown = _stuck_shutdown

        trimmer = asyncio.ensure_future(pool.trim())
        await asyncio.sleep(0.05)
        self.assertTrue(beta.started)

        # 等的时间压缩到 0.05 秒：这里验的是"等不到之后怎么办"，不是等多久。
        with mock.patch.object(pool_module, "_LOGOUT_DRAIN_SECONDS", 0.05):
            with self.assertRaises(AcpError):
                await pool.logout(purge_config=True)
        self.assertEqual(alpha.logouts, [], "没关干净就不许删凭据")

        gate.set()
        await trimmer


    async def test_a_lease_waiting_inside_trim_is_refused_after_the_gate_closes(self):
        # 租约进门时查过一次闸门，可 ``trim()`` 会等很久：等到登出把凭据删掉、
        # 闸门重新放开。回来时不认这件事，调用方紧接着的 ensure_started() 就会在
        # 用户看到"已退出登录"之后拉起一条 CLI。
        pool = self.make_pool(max_processes=1, primary_model=ALPHA)
        alpha = pool.get(ALPHA)

        gate = asyncio.Event()
        real_trim = pool.trim

        async def _slow_trim():
            await gate.wait()
            return await real_trim()

        pool.trim = _slow_trim

        outcome = []

        async def _hold():
            try:
                async with pool.lease(BETA):
                    outcome.append("ok")
            except AcpError as exc:
                outcome.append(str(exc))

        holder = asyncio.ensure_future(_hold())
        await asyncio.sleep(0.05)  # 让它停在 trim 里

        with mock.patch.object(pool_module, "_LOGOUT_DRAIN_SECONDS", 0.05):
            await pool.logout(purge_config=True)
        self.assertEqual(alpha.logouts, [True], "登出照常删了凭据")

        gate.set()
        await holder
        self.assertEqual(len(outcome), 1)
        self.assertIn("logging out", outcome[0], "闸门关了之后不许再放行")


class ShutdownTests(_PoolCase):
    """``shutdown`` 是永久的：之后一律拒绝。"""

    async def test_shutdown_destroys_every_runtime_and_refuses_new_leases(self):
        pool = self.make_pool()
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        await pool.shutdown()
        self.assertEqual(alpha.shutdowns, 1)
        self.assertEqual(beta.shutdowns, 1)
        self.assertEqual(pool.status()["models"], [])
        with self.assertRaises(AcpError):
            pool.get(ALPHA)
        with self.assertRaises(AcpError):
            async with pool.lease(ALPHA):
                pass  # pragma: no cover - 走不到这里

    async def test_a_cancelled_shutdown_still_closes_the_rest(self):
        # 映射已经清空了，没人再回头找它们：取消时撒手，后面那几条就成了孤儿。
        pool = self.make_pool()
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)
        gate = asyncio.Event()
        release = alpha.shutdown

        async def _slow_shutdown():
            await gate.wait()  # 卡在第一条上，让取消落在收尾途中
            await release()

        alpha.shutdown = _slow_shutdown

        stopper = asyncio.ensure_future(pool.shutdown())
        await asyncio.sleep(0.05)
        stopper.cancel()
        gate.set()
        with self.assertRaises(asyncio.CancelledError):
            await stopper
        self.assertEqual(alpha.shutdowns, 1, "被打断的那条也要关完")
        self.assertEqual(beta.shutdowns, 1, "排在后面的不能因为取消就不关了")

    async def test_shutdown_retries_an_eviction_that_could_not_be_closed(self):
        # 淘汰关不掉的那条留在 ``_evicting`` 上：它已经不在映射里，池一散就再没人
        # 看着它。卸载/重载时必须把它也带上，而且**再试一次**——上一次失败不等于
        # 这一次也失败。
        pool = self.make_pool(max_processes=1, primary_model=ALPHA)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        attempts = []

        async def _first_attempt_fails():
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("FICTIONAL-SHUTDOWN-FAILURE")
            beta.started = False

        beta.shutdown = _first_attempt_fails

        self.assertEqual(await pool.trim(), 0, "关不掉的那条不算关成功")
        self.assertTrue(beta.started, "它还活着，只是已经不在映射里了")
        self.assertEqual(pool.status()["models"], [ALPHA])

        await pool.shutdown()
        self.assertFalse(beta.started, "卸载时要把它也关掉，不然没人再认领这条进程")
        self.assertEqual(len(attempts), 2, "淘汰那次失败，关停要再试一次")
        self.assertEqual(alpha.shutdowns, 1)

    async def test_a_second_shutdown_can_still_close_what_the_first_one_missed(self):
        # 第一次关不掉时，那条进程得留在账上：清单提前清空等于把"没关掉"改写成
        # "没有这条"，连再试一次的把手都不剩。
        pool = self.make_pool(max_processes=1, primary_model=ALPHA)
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        attempts = []

        async def _dies_on_the_third_try():
            attempts.append(1)
            if len(attempts) >= 3:
                beta.started = False

        beta.shutdown = _dies_on_the_third_try

        self.assertEqual(await pool.trim(), 0, "淘汰那一次没关掉")
        await pool.shutdown()
        self.assertTrue(beta.started, "关不掉就是关不掉，账本要说实话")
        self.assertEqual(len(attempts), 2, "卸载时试过一次")

        await pool.shutdown()
        self.assertFalse(beta.started, "第二次还得能顺着账本找到它")
        self.assertEqual(len(attempts), 3)

    async def test_a_second_shutdown_also_finds_a_child_that_came_from_the_mapping(
        self,
    ):
        # 映射清空之后，那几条既不在映射里、也不在账上，就等于两头都没有。从映射来
        # 的那几条也得记到账上，第二次才找得回来。
        pool = self.make_pool()
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)
        _start(alpha, beta)

        attempts = []

        async def _dies_on_the_second_try():
            attempts.append(1)
            if len(attempts) > 1:
                beta.started = False

        beta.shutdown = _dies_on_the_second_try

        await pool.shutdown()
        self.assertTrue(beta.started, "第一次没关掉")
        self.assertEqual(len(attempts), 1)

        await pool.shutdown()
        self.assertFalse(beta.started, "映射清空之后，还得顺着账本找回它")
        self.assertEqual(len(attempts), 2)

    async def test_shutdown_survives_a_runtime_that_refuses_to_stop(self):
        pool = self.make_pool()
        alpha = pool.get(ALPHA)
        beta = pool.get(BETA)

        async def boom():
            raise RuntimeError("FICTIONAL-SHUTDOWN-FAILURE")

        alpha.shutdown = boom
        await pool.shutdown()
        self.assertEqual(beta.shutdowns, 1)
        with self.assertRaises(AcpError):
            pool.get(BETA)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
