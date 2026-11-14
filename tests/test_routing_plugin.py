"""真实插件里的主备路由接线。

这一层验的是**插件自己的接线**，不是路由的决策细节（``tests/test_routing.py``），
也不是网关的对外契约（``tests/test_routing_gateway.py``）。所以它驱动的是真的
``WorkBuddyPlugin``、真的 AstrBot 框架、真的 ``ModelRouter``、真的 ``StateStore``、
真的 ``Gateway``、真的 ``AdminNotifier``，只把两样东西换掉：

* ``Context.send_message``（记录发了几条、内容是什么）；
* 聊天后端换成 ``support.ScriptedChat``——一个"已经按契约出错"的后端，按模型排队
  "这一轮会怎样"。

聊天后端为什么可以换：ACP 适配层自己的错误映射由 ``test_acp_backend`` 用假 CLI 覆盖，
这里只要一个能稳定制造「主用 429、备用正常」的后端。换了之后，路由层、网关层、命令
层、提醒层仍然是生产代码，一行没动。

**这套件从"后端已就位"开始，验的不是装配。** ``_build_gateway`` 里 ``self.chat is
None`` 那条"造后端"的分支在这里永远走不到（下面 ``_use_scripted_chat`` 先塞了后端），
所以生产装配写坏了这 8 条一条都不会红。那条分支由
``tests/test_plugin_integration.py::test_build_gateway_wires_the_backend_to_the_pool_and_the_alert_hook``
单独盯着——要删或改弱它，先把那份责任挪到这里，别让它就这么空着。

验的东西：

* 启用路由的插件能起来，``/v1/models`` 里那份虚拟入口只出现一次，且不带任何"计价"
  字段（它是一条路由，不是一个模型）；
* 主用失败、备用成功时，调用方真的拿到备用那句话，而管理员一条 QQ 都不该收到；
* 两个连续 429 会提前收工，第三条候选永不被问；失败只提醒一次，冷却期内的重复
  请求不再刷屏；
* 重建插件后冷却按**绝对截止时间**恢复，不重启窗口，也不动凭据与别的状态段；
* 一条路由挂在"等首帧"时，另一条并发的聊天失败仍然独立提醒（静音是按次，不是全局）；
* 通知关掉时失败只落状态、不发消息，且不会把开关偷偷打开；
* 请求体上限是插件交给网关的：超限请求在路由之前就被 413 拒绝；
* 401 / 403 / 400 都不换备用——那三种失败换个模型也没用。

``tests.test_plugin_integration`` 当框架沙箱与夹具来源（它在本模块之前把
``ASTRBOT_ROOT`` / ``TMPDIR`` / ``WORKBUDDY_TEST_TMP`` 钉好）；它自己的 TestCase 不被
继承，所以那个套件不会在这里重跑一遍。下面出现的凭据 / token / QQ 全是虚构样本。
"""

import asyncio
import contextlib
import json
import sys
import unittest
import uuid
from pathlib import Path

import aiohttp

# ---------------------------------------------------------------------------
# 让插件与两个支持模块在两种发现方式下都导入得到。``support`` 会在任何 astrbot
# 导入之前钉好沙箱路径，所以它必须排在 ``wb_main`` 前面。
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve()
_PLUGIN_ROOT = _HERE.parents[1]          # .../astrbot_plugin_workbuddy
_TESTS_DIR = _HERE.parent                # .../astrbot_plugin_workbuddy/tests
_SOURCE_ROOT = _HERE.parents[2]          # 含插件包的那一层
for _path in (str(_PLUGIN_ROOT), str(_TESTS_DIR), str(_SOURCE_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

try:  # 包内发现（``tests`` 是真正的包）
    import tests.test_plugin_integration as support
except ImportError:  # 顶层发现
    import test_plugin_integration as support  # type: ignore

try:  # 带记录式 send_message 的上下文（它的 TestCase 同样不被继承）
    import tests.test_notification_integration as notify_support
except ImportError:  # 顶层发现
    import test_notification_integration as notify_support  # type: ignore

#: 底层夹具（脚本化聊天后端、清单替身、错误构造）。``support`` 是"框架沙箱 + 插件
#: 装配"那一层，它自己也是从这里拿夹具的，所以直接用同一份源头。
try:  # 包内发现
    from tests import support as fixtures
except ImportError:  # 顶层发现
    import support as fixtures  # type: ignore

# --- 真框架 / 真插件（沙箱已由 support 钉好） --------------------------------
import astrbot_plugin_workbuddy.main as wb_main  # noqa: E402
from astrbot_plugin_workbuddy.core.routing import ORDERED_MODEL_ID  # noqa: E402

# --- 虚构固定样本 -----------------------------------------------------------
ADMIN_QQ = notify_support.ADMIN_QQ        # 虚构的接收 QQ
PLATFORM_ID = notify_support.PLATFORM_ID  # 虚构的 aiocqhttp 实例
GATEWAY_KEY = support.GATEWAY_KEY

# 配置的有序清单：主用、备用，外加第三条"连续两个 429 就该收工"用例永远不该碰到的。
PRIMARY = "wb-alpha"
STANDBY = "wb-beta"
SPARE = "wb-gamma"

PRIMARY_ANSWER = "这是主模型给出的回答"
STANDBY_ANSWER = "这是备用模型给出的回答"

# ~1.14 MiB 的真实 UTF-8 字节（'订' 三字节一个），超过 1 MiB 上限。
BIG_TEXT = "订" * 400000


class _RoutingPluginIntegrationTestCase(unittest.IsolatedAsyncioTestCase):
    """驱动真插件；把 ``support`` 的夹具方法逐个绑上来。

    ``support._PluginIntegrationTestCase`` 不被继承（那会把它的整套用例再跑一遍），
    只绑需要的那几个方法，另外自己造一个带通知记录的上下文。
    """

    async def asyncSetUp(self) -> None:
        await support._PluginIntegrationTestCase.asyncSetUp(self)
        self._contexts = []

    async def asyncTearDown(self) -> None:
        await support._PluginIntegrationTestCase.asyncTearDown(self)

    # -- 绑上来的 support 夹具 ---------------------------------------------
    def _build_config(self, config_path, **overrides):
        return support._PluginIntegrationTestCase._build_config(
            self, config_path, **overrides
        )

    def _ensure_metadata_name(self):
        return support._PluginIntegrationTestCase._ensure_metadata_name(self)

    def _seed_credentials(self):
        return support._PluginIntegrationTestCase._seed_credentials(self)

    def _credentials_blob(self):
        return support._PluginIntegrationTestCase._credentials_blob(self)

    # -- 配置 ---------------------------------------------------------------
    def _routing_config(self, **overrides):
        base = {
            "gateway": {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 0,
                "api_key": GATEWAY_KEY,
                "concurrency": 4,
                "rpm": 600,
                "queue_size": 8,
                "queue_timeout": 5,
            },
            # 测试机（构建机）上可能真装了 codebuddy：指向一个不存在的可执行文件，
            # 任何一次误启动都会立刻失败，而不是开一个真进程去花钱、去写文件。
            "acp": {"cli_path": "/nonexistent/codebuddy-for-tests", "model": ""},
            # 自动签到关掉：这个模块讲的是聊天路由，不是调度时序。
            "signin": {"enabled": False},
            "routing": {
                "enabled": True,
                "models": [PRIMARY, STANDBY],
                "cooldown_seconds": 60,
                "first_frame_timeout": 30,
            },
        }
        return support._deep_merge(base, overrides)

    def _notify_config(self, **overrides):
        notifications = {
            "enabled": True,
            "signin_success": True,
            "errors": True,
            "admin_qq": ADMIN_QQ,
            "platform_id": "",
            "error_cooldown_seconds": 60,
        }
        notifications.update(overrides.pop("notifications", {}))
        return support._deep_merge(
            self._routing_config(**overrides), {"notifications": notifications}
        )

    # -- 工厂 ---------------------------------------------------------------
    async def _make_plugin(self, *, initialize=True, **overrides):
        cfg_path = support._SESSION_ROOT / ("cfg-" + uuid.uuid4().hex + ".json")
        config = self._build_config(cfg_path, **overrides)
        context = notify_support._NotifyContext(
            config, [notify_support._Instance(PLATFORM_ID)]
        )
        self._ensure_metadata_name()
        plugin = wb_main.WorkBuddyPlugin(context, config)
        self._contexts.append(context)
        self._plugins.append(plugin)
        if initialize:
            await plugin.initialize()
            await self._use_scripted_chat(plugin)
        return plugin, context

    async def _use_scripted_chat(self, plugin) -> None:
        """把聊天后端换成按模型排剧本的假后端，并如实重建网关。

        顺序照生产里"重载插件"那一套来：先换成后端、换掉清单，再停旧网关、重建。
        ``_build_gateway`` 见 ``plugin.chat`` 不为空就复用，所以这里放进去的就是它
        接下来用的那一个。清单也一起换：真清单要 spawn 一次 ``codebuddy --help``，
        清单解析本身由 ``tests/test_models.py`` 覆盖。
        """
        # 静默早退会让后面每条用例都以"问了一个不存在的 CLI"的形式失败，很难定位。
        self.assertTrue(
            plugin.ready and plugin.gateway is not None,
            "插件没起来（{}），没法换后端".format(plugin.error_reason),
        )
        plugin.chat = fixtures.ScriptedChat(on_error=plugin._on_chat_error)
        plugin.catalog = fixtures.StubCatalog([PRIMARY, STANDBY, SPARE])
        await plugin.gateway.stop()
        await plugin._build_gateway()
        # 自证：重建出来的网关必须挂在刚换上的那个后端上。少了这一句，"后端换没换
        # 成功"就只靠后面那些行为断言间接体现，接口一改就可能整片假绿。
        self.assertIs(
            plugin.gateway.limits,
            plugin.chat.limits,
            "网关没有挂到刚换上的后端上",
        )

    # -- 驱动 / 等待 ---------------------------------------------------------
    async def _run_wb(self, plugin, event, action, rest=""):
        return await asyncio.wait_for(
            support._invoke(plugin.wb, event, action, rest), timeout=10
        )

    async def _chat_json(
        self, plugin, *, model=ORDERED_MODEL_ID, text="hi", timeout=20
    ):
        """往插件自己的监听端口发一次真的 chat completion。"""
        port = getattr(plugin.gateway, "bound_port", None)
        self.assertTrue(port, "the real gateway never bound a port")
        timeout_obj = aiohttp.ClientTimeout(total=timeout)
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": text}],
            "stream": False,
        }
        async with aiohttp.ClientSession(timeout=timeout_obj) as session:
            async with session.post(
                "http://127.0.0.1:{}/v1/chat/completions".format(port),
                json=payload,
                headers={"Authorization": "Bearer " + GATEWAY_KEY},
            ) as resp:
                body = await resp.json(content_type=None)
                return resp.status, body

    async def _wait_until(self, predicate, timeout=5.0):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not predicate():
            if loop.time() >= deadline:
                return False
            await asyncio.sleep(0.01)
        return True

    def _pending(self, plugin):
        # ``_notify_tasks`` 在 ``__init__`` 里就建好了，没有"属性不存在"那种回退。
        # 之前那套扫任务名含 "notify" 的兜底是死代码，而且一改任务名就失效。
        return [t for t in list(plugin._notify_tasks) if not t.done()]

    async def _wait_idle(self, plugin, timeout=5.0):
        return await self._wait_until(lambda: not self._pending(plugin), timeout)

    def _sent_texts(self, context):
        return [support._result_text(chain) for _, chain in context.sent]

    def _assert_no_secrets(self, text):
        """灾难兜底，不是日常回归防线。

        余额 / 成长任务那些文案已经删了，提醒模板现在只插状态码与模型名；token 字面量
        能出现在提醒里的唯一途径是整份凭据被倾倒。所以它挡的是"哪天有人把凭据塞进
        提醒"这种事故，而不是"这次改动会不会漏 token"——别拿它算覆盖率。
        """
        for secret in (
            GATEWAY_KEY,
            support.SEED_CREDENTIALS["accessToken"],
            support.SEED_CREDENTIALS["refreshToken"],
        ):
            self.assertNotIn(secret, text)

    # =====================================================================
    # 1. 启用路由的插件能起来；虚拟入口在真 HTTP 上只出现一次；主用失败后备用
    #    真的答上来；一条 QQ 都不发；``wb status`` 报出开关、顺序和最近一次结果。
    # =====================================================================
    async def test_01_failover_serves_standby_and_status_reports_the_route(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(**self._notify_config())
        self.assertTrue(plugin.ready)
        self.assertEqual(plugin.error_reason, "")
        self.assertIsNotNone(
            plugin.chat, "the scripted backend was never installed"
        )

        router = getattr(plugin, "router", None)
        self.assertIsNotNone(router, "the plugin never exposed a real ModelRouter")
        status = router.status()
        self.assertIs(status.get("enabled"), True)
        self.assertEqual(status.get("models"), [PRIMARY, STANDBY])
        self.assertIsNone(status.get("last_result"))
        self.assertIsNone(status.get("last_success_model"))

        # ``status()`` 是纯本地视图：连问三次不许抛，也不许改状态。这里**不再**
        # 断言 ``up.total_hits()`` 不变——ACP 之后聊天根本不经过假上游，那个计数
        # 在任何窗口里都动不了，断言它等于断言 1 == 1。
        for _ in range(3):
            router.status()
        self.assertEqual(router.status().get("models"), [PRIMARY, STANDBY])

        # 真 HTTP 的模型清单里带着那一条虚拟入口。
        port = plugin.gateway.bound_port
        self.assertTrue(port)
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "http://127.0.0.1:{}/v1/models".format(port),
                headers={"Authorization": "Bearer " + GATEWAY_KEY},
            ) as resp:
                self.assertEqual(resp.status, 200)
                listing = await resp.json()
        entries = [item for item in listing.get("data") or [] if isinstance(item, dict)]
        virtual = [item for item in entries if item.get("id") == ORDERED_MODEL_ID]
        self.assertEqual(len(virtual), 1, "the virtual route is not advertised once")
        self.assertNotIn("credits", virtual[0])  # 一条路由，不是一件带价的商品

        # 主用 429、备用正常：调用方真的看到备用那句话。
        plugin.chat.queue_error(PRIMARY, fixtures.upstream_error(429, retry_after=30))
        plugin.chat.queue_text(STANDBY, STANDBY_ANSWER)
        code, body = await self._chat_json(plugin)
        self.assertEqual(code, 200)
        self.assertEqual(body["choices"][0]["message"]["content"], STANDBY_ANSWER)

        self.assertEqual(plugin.chat.sent_models, [PRIMARY, STANDBY])
        route = router.status()
        self.assertEqual(route.get("last_success_model"), STANDBY)
        self.assertEqual(route.get("last_result", {}).get("outcome"), "success")

        # 一次成功的失败转移不是事故：零条 QQ。
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 0)

        # 真命令报出开关、配置的顺序，以及上一次真正成功的候选。
        text = support._text_of(
            await self._run_wb(plugin, support._make_event("wb status"), "status", "")
        )
        self.assertIn("主备路由：已启用", text)
        self.assertIn(PRIMARY, text)
        self.assertIn(STANDBY, text)
        self.assertLess(text.index(PRIMARY), text.index(STANDBY))
        self.assertIn("最近成功模型：" + STANDBY, text)
        # 默认的请求体上限是批准过的 16 MiB，所以总览必须用 MiB 说：写成十进制
        # 的 "MB" 就把上限说错了。
        self.assertIn("16 MiB", text)
        self._assert_no_secrets(text)
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 0)

    # =====================================================================
    # 2. 主用 429 + 备用 429 在第三条候选之前收工，只提醒一次（聊天来源、尝试数、
    #    停止原因）；冷却期内的重复请求不再刷屏。
    # =====================================================================
    async def test_02_two_rate_limits_stop_early_and_notify_once(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(
            **self._notify_config(routing={"models": [PRIMARY, STANDBY, SPARE]})
        )
        self.assertTrue(plugin.ready)
        for model in (PRIMARY, STANDBY):
            plugin.chat.queue_error(model, fixtures.upstream_error(429, retry_after=30))

        code, _ = await self._chat_json(plugin)
        self.assertEqual(code, 429)

        # 两个真实的 429 就收工：第三条候选一次都没被问。
        self.assertEqual(plugin.chat.sent_models, [PRIMARY, STANDBY])
        route = plugin.router.status()
        self.assertEqual(route.get("last_result", {}).get("outcome"), "failed")
        self.assertEqual(
            list(route.get("last_result", {}).get("attempts") or []),
            [PRIMARY, STANDBY],
        )

        # 恰好一条 QQ：说出聊天来源、尝试数，以及"为什么停"。
        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 1)
        text = self._sent_texts(ctx)[0]
        self.assertIn("聊天", text)
        self.assertIn("已尝试 2 个模型", text)
        self.assertTrue(
            any(word in text for word in ("停止", "中止", "冷却", "全部失败")),
            "the routing alert does not explain why the round stopped: " + text,
        )
        self._assert_no_secrets(text)

        # 冷却期内的重复请求被本地拒掉：不再问后端，也不再有第二条 QQ。
        attempts_before = list(plugin.chat.attempts)
        code, _ = await self._chat_json(plugin)
        self.assertEqual(code, 429)
        self.assertEqual(
            plugin.chat.attempts,
            attempts_before,
            "a cooled route must not ask any candidate",
        )
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 1)

    # =====================================================================
    # 3. 一次成功、一次失败之后重建插件（同状态、同配置）：冷却按绝对截止时间恢复，
    #    重建本身不探测上游，凭据与别的状态段一个字节都不动。
    # =====================================================================
    async def test_03_rebuild_keeps_cooldown_and_preserves_state(self):
        self._seed_credentials()
        credentials_before = self._credentials_blob()
        config = self._notify_config()
        plugin, ctx = await self._make_plugin(**config)

        # 一次成功的失败转移 ...
        plugin.chat.queue_error(PRIMARY, fixtures.upstream_error(429, retry_after=30))
        plugin.chat.queue_text(STANDBY, STANDBY_ANSWER)
        code, _ = await self._chat_json(plugin)
        self.assertEqual(code, 200)
        self.assertEqual(plugin.router.status().get("last_success_model"), STANDBY)

        # ... 紧接着一次彻底失败的轮次（主用还在冷却里，只剩备用可问）。
        plugin.chat.queue_error(STANDBY, fixtures.upstream_error(429, retry_after=30))
        code, _ = await self._chat_json(plugin)
        self.assertEqual(code, 429)
        self.assertEqual(
            plugin.router.status().get("last_result", {}).get("outcome"), "failed"
        )
        self.assertEqual(
            plugin.router.status().get("last_success_model"),
            STANDBY,
            "a failed round must not erase the last real success",
        )
        await self._wait_idle(plugin)
        delivery_before = plugin.notifier.status().get("last_delivery")

        # 通过真 StateStore 写一段无关状态 ...
        plugin.state.update("ticket_marker", {"keep": "T3"})
        state_before = plugin.state.load()
        cooldowns_before = plugin.router.status().get("cooldowns")
        self.assertTrue(cooldowns_before, "no route cooldown was persisted")

        # ... 同一数据目录、同一配置重建之后必须还在。
        rebuilt, rebuilt_ctx = await self._make_plugin(**config)
        self.assertTrue(rebuilt.ready)
        state_after = rebuilt.state.load()
        self.assertEqual(state_after.get("ticket_marker"), {"keep": "T3"})
        self.assertEqual(
            state_after.get("notifications"), state_before.get("notifications")
        )
        self.assertEqual(self._credentials_blob(), credentials_before)
        self.assertEqual(
            rebuilt.account.credentials.get("accessToken"),
            support.SEED_CREDENTIALS["accessToken"],
        )

        # 落盘的是绝对截止时间，所以恢复的是"还剩多久"，不是重新开始计时。
        route_after = rebuilt.router.status()
        self.assertTrue(route_after.get("cooldowns"))
        for model, remaining in route_after["cooldowns"].items():
            self.assertIn(model, (PRIMARY, STANDBY))
            self.assertGreater(remaining, 0)
            self.assertLessEqual(remaining, 30.5)
        self.assertEqual(route_after.get("last_success_model"), STANDBY)
        if delivery_before is not None:
            self.assertEqual(
                rebuilt.notifier.status().get("last_delivery"), delivery_before
            )

        # 重建自己不做任何上游探测 ...
        await self._run_wb(rebuilt, support._make_event("wb status"), "status", "")
        rebuilt.router.status()

        # ... 冷却中的路由本地就拒掉下一次聊天，一个候选都不问，也不再爆一轮提醒。
        code, _ = await self._chat_json(rebuilt)
        self.assertEqual(code, 429)
        self.assertEqual(rebuilt.chat.sent_models, [])
        await self._wait_idle(rebuilt)
        await asyncio.sleep(0.05)
        self.assertEqual(rebuilt_ctx.send_calls, 0)

    # =====================================================================
    # 4. 并发隔离：一条路由挂在"等备用首帧"时，另一条并发的路由失败照样独立提醒
    #    （静音是按次，不是全局），随后备用成功也不会补一条。
    #
    #    注意这条**同时依赖三个性质**：PRIMARY 的 429 会进冷却（所以并发那一轮
    #    只剩 STANDBY 可问、只"尝试 1 个模型"）、静音按轮不跨请求、备用成功不补报。
    #    `sent_models == [PRIMARY, STANDBY, STANDBY]` 一行就能定位是哪一环坏了；
    #    看到"已尝试 1 个模型"不要当成断言写错——那是因为 PRIMARY 在冷却里。
    # =====================================================================
    async def test_04_a_hanging_route_does_not_mute_a_concurrent_chat_alert(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(**self._notify_config())
        gate = asyncio.Event()
        started = asyncio.Event()

        plugin.chat.queue_error(PRIMARY, fixtures.upstream_error(429, retry_after=30))
        plugin.chat.queue_wait(STANDBY, gate, text=STANDBY_ANSWER, started=started)
        # 并发那条请求**也走路由**：第一条已经把 PRIMARY 打进冷却，于是它只剩
        # STANDBY 可问，就在这里失败。两条都是路由请求，"静音不跨轮泄漏"才算真的
        # 被验到——换成一条具体模型请求的话，绕过的是路由层，那条 bug 也就测不出来。
        plugin.chat.queue_error(STANDBY, fixtures.upstream_error(502))

        chat_task = asyncio.create_task(self._chat_json(plugin))
        try:
            self.assertTrue(
                await self._wait_until(started.is_set, 5),
                "the standby attempt never reached its first frame"
                "（先确认 PRIMARY 的 429 是否被判成了不可重试）",
            )
            # 路由正卡在这里。另一条请求必须还能跑完，并且它的失败必须照常上报。
            code, body = await self._chat_json(plugin)
            self.assertEqual(code, 502)
            self.assertEqual(body["error"]["message"], "upstream request failed")

            self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
            await self._wait_idle(plugin)
            await asyncio.sleep(0.05)
            self.assertEqual(ctx.send_calls, 1, "并发的那一轮必须自己报，不能被静音吞掉")
            text = self._sent_texts(ctx)[0]
            self.assertIn("聊天", text)
            # 它是一条**被路由**的失败，所以提醒必须是路由结果，且老实说只试了一个
            # 候选（PRIMARY 在冷却里，根本没被问）。
            self.assertIn("路由：", text)
            self.assertIn("已尝试 1 个模型", text)
            self._assert_no_secrets(text)

            # 放行之后这条路由正常收尾。备用成功不该补出第二条候选提醒。
            gate.set()
            code, body = await asyncio.wait_for(chat_task, timeout=20)
            self.assertEqual(code, 200)
            self.assertEqual(body["choices"][0]["message"]["content"], STANDBY_ANSWER)
            self.assertEqual(plugin.chat.sent_models, [PRIMARY, STANDBY, STANDBY])
            await self._wait_idle(plugin)
            await asyncio.sleep(0.05)
            self.assertEqual(ctx.send_calls, 1)
        finally:
            gate.set()
            if not chat_task.done():
                chat_task.cancel()
                with contextlib.suppress(BaseException):
                    await chat_task

    # =====================================================================
    # 5. 通知关掉：失败照旧落状态，但一条不发；显式开关也不会被路由偷偷打开；
    #    配置成关闭的路由仍然给得出一份可读的本地状态。
    # =====================================================================
    async def test_05_disabled_notifications_still_record_a_failed_route(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(
            **self._notify_config(
                notifications={
                    "enabled": False,
                    "signin_success": False,
                    "errors": False,
                }
            )
        )
        self.assertTrue(plugin.ready)
        for model in (PRIMARY, STANDBY):
            plugin.chat.queue_error(model, fixtures.upstream_error(429, retry_after=30))

        code, _ = await self._chat_json(plugin)
        self.assertEqual(code, 429)
        route = plugin.router.status()
        self.assertEqual(route.get("last_result", {}).get("outcome"), "failed")
        persisted = plugin.state.load().get("routing") or {}
        self.assertEqual(persisted.get("last_result", {}).get("outcome"), "failed")

        # 一条都没发，而且没有任何开关被路由偷偷打开。
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 0)
        notify_status = plugin.notifier.status()
        self.assertIs(notify_status.get("enabled"), False)
        self.assertIs(notify_status.get("signin_success"), False)
        self.assertIs(notify_status.get("errors"), False)

        # 配置成关闭的路由仍然建出一个真的、读得懂的 router。
        off_plugin, off_ctx = await self._make_plugin(
            **self._notify_config(routing={"enabled": False, "models": []})
        )
        self.assertTrue(off_plugin.ready)
        off_router = getattr(off_plugin, "router", None)
        self.assertIsNotNone(off_router, "the router must exist even when disabled")
        off_status = off_router.status()
        self.assertIs(off_status.get("enabled"), False)
        self.assertEqual(off_status.get("models"), [])
        await self._wait_idle(off_plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(off_ctx.send_calls, 0)

    # =====================================================================
    # 6. 插件真的把 ``gateway.max_request_body_mb`` 交给了网关：~1.1 MiB 的虚拟
    #    请求是 JSON 413，一个候选都不问、一条 QQ 都不发、也不留下路由结果。
    # =====================================================================
    async def test_06_configured_body_cap_refuses_before_the_route(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(
            **self._notify_config(gateway={"max_request_body_mb": 1})
        )
        self.assertTrue(plugin.ready)
        self.assertEqual(plugin._settings["gateway"]["max_request_body_mb"], 1)

        code, body = await self._chat_json(plugin, text=BIG_TEXT, timeout=60)
        self.assertEqual(code, 413)
        error = body.get("error")
        self.assertIsInstance(error, dict)
        self.assertEqual(error.get("type"), "request_too_large")
        self.assertEqual(error.get("code"), "request_too_large")
        message = error.get("message") or ""
        self.assertIn("1 MiB", message)                    # 配置的上限
        self.assertIn("max_request_body_mb", message)      # 可操作的那个开关

        # 在准入之前、在路由之前就拒掉：没有候选请求、没有路由结果、没有提醒。
        # （这里不再断言 ``up.total_hits()`` 不变：ACP 之后聊天根本不经过假上游，
        # 那个计数在任何窗口里都动不了，断言它等于断言 1 == 1。）
        self.assertEqual(plugin.chat.sent_models, [])
        self.assertIsNone(plugin.router.status().get("last_result"))
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 0)

    # =====================================================================
    # 7. 真 401 与真 403 绝不换备用：各一条对应的登录 / 权限 QQ，带聊天来源，
    #    不带任何上游原文；候选尝试全程对观察者静音。
    # =====================================================================
    async def test_07_auth_and_permission_failures_never_use_the_standby(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(**self._notify_config())

        # (a) 401。刷新现在只有 CLI 一个负责人，这里只关心"401 不换备用"。
        plugin.chat.queue_error(PRIMARY, fixtures.upstream_error(401, code=4010))
        code, _ = await self._chat_json(plugin)
        self.assertEqual(code, 401)
        self.assertEqual(plugin.chat.sent_models, [PRIMARY])

        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
        await self._wait_idle(plugin)
        self.assertEqual(ctx.send_calls, 1)
        auth_text = self._sent_texts(ctx)[0]
        self.assertIn("聊天", auth_text)
        self.assertTrue(
            any(word in auth_text for word in ("登录", "授权", "认证")),
            "the auth alert does not name the login problem: " + auth_text,
        )
        self._assert_no_secrets(auth_text)

        # (b) 403 是权限问题：同样不换备用，再来一条提醒。
        plugin.chat.queue_error(PRIMARY, fixtures.upstream_error(403, code=4030))
        code, _ = await self._chat_json(plugin)
        self.assertEqual(code, 403)
        self.assertEqual(plugin.chat.sent_models, [PRIMARY, PRIMARY])

        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 2))
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 2)
        permission_text = self._sent_texts(ctx)[1]
        self.assertIn("聊天", permission_text)
        self.assertTrue(
            any(word in permission_text for word in ("权限", "授权")),
            "the permission alert does not name the permission problem: "
            + permission_text,
        )
        self._assert_no_secrets(permission_text)

        route = plugin.router.status()
        self.assertEqual(route.get("last_result", {}).get("outcome"), "failed")
        self.assertIsNone(route.get("last_success_model"))
        # 每一次被路由的候选尝试都对观察者静音：失败由路由统一判一次，不该由
        # 后端在每次重试里各报一次。
        self.assertEqual(
            [observe for _, observe in plugin.chat.attempts], [False, False]
        )

    # =====================================================================
    # 8. 主用真 400 不是可重试状态：备用一次都不问，调用方真的看到 400，管理员
    #    仍然只收到一条解释了"为什么停"的聊天提醒。
    # =====================================================================
    async def test_08_upstream_400_stops_the_round_and_alerts_once(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(**self._notify_config())
        self.assertTrue(plugin.ready)

        plugin.chat.queue_error(PRIMARY, fixtures.upstream_error(400, code=4000))

        code, body = await self._chat_json(plugin)
        self.assertEqual(code, 400)
        self.assertEqual(body["error"]["message"], "upstream request failed")

        # 400 不可重试：只有主用真的被问过。
        self.assertEqual(plugin.chat.sent_models, [PRIMARY])
        route = plugin.router.status()
        last = route.get("last_result") or {}
        self.assertEqual(last.get("outcome"), "failed")
        self.assertEqual(last.get("reason"), "upstream")
        self.assertEqual(last.get("status"), 400)
        self.assertIsNone(route.get("last_success_model"))

        # 恰好一条 QQ：聊天来源、只尝试了一次、固定措辞的失败 / 停止说明。
        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 1)
        text = self._sent_texts(ctx)[0]
        self.assertIn("聊天", text)
        self.assertIn("已尝试 1 个模型", text)
        self.assertTrue(
            any(
                word in text
                for word in ("停止", "中止", "失败", "上游返回错误", "上游服务异常")
            ),
            "the routing alert does not explain the failed round: " + text,
        )
        self._assert_no_secrets(text)
        # 业务码进来了也不外传：提醒里不该出现 4000 这种上游内部编号。
        self.assertNotIn("4000", text)
        self.assertNotIn("4000", json.dumps(body, ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
