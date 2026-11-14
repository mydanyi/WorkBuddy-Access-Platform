"""AstrBot entry point for the WorkBuddy plugin.

A single :class:`WorkBuddyPlugin` (a real ``astrbot.api.star.Star``) exposes one
``@filter.command("wb")`` handler.  Every action is gated by a runtime
``is_admin() and is_private_chat()`` check first, so a management-group chat or a
non-admin user never triggers an upstream request and never receives a login
link.

The plugin owns one shared runtime: a state store, a signin-only
``UpstreamClient``, an ``AcpRuntimePool`` that spawns the official CodeBuddy CLI
over ACP stdio (one process per model), an ``AcpChatBackend`` adapting it to the
OpenAI shape, a ``ModelCatalog`` reading the model list from the local CLI, a
local OpenAI-compatible ``Gateway`` and a ``SigninScheduler``.  No AstrBot
persona / agent / history / tool behaviour is changed here; the gateway only
forwards the caller's payload to one throwaway ACP session.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os

import aiohttp

from astrbot.api.star import Context, Star, StarTools, register
from astrbot.api.event import filter, AstrMessageEvent

from .core.acp import (
    AcpChatBackend,
    AcpError,
    AcpRuntime,
    AcpRuntimePool,
    AcpTimeoutError,
    CliCredentials,
    default_argv,
    default_env,
)
from .core.client import UpstreamClient
from .core.config import ConfigError, load_settings
from .core.gateway import Gateway
from .core.models import ModelCatalog, strip_model_alias
from .core.notifications import AdminNotifier
from .core.numbers import is_finite_number
from .core.routing import ORDERED_MODEL_ID, ModelRouter
from .core.scheduler import SigninScheduler
from .core.signin import SigninService
from .core.storage import StateStore

PLUGIN_NAME = "astrbot_plugin_workbuddy"

# 命令回复前最多等这么久把登录链接拿到手：拿到就把链接放进回复里；拿不到就先回
# 一句"稍后查看"，等链接后到时再补一条通知。测试替换这个常量，不睡真 5 秒。
_LOGIN_LINK_SECONDS = 5.0

# 模块级 logger 只用于"本该有人看见"的意外：用户可见文案是固定中文，不该承载
# 诊断信息。
_log = logging.getLogger(PLUGIN_NAME)

# 签到接口的固定边界。没有用户可配置的 URL：测试在本层替换这个常量，再往下都是
# 写死的路径拼接。
BILLING_BASE = "https://www.codebuddy.cn"

# Fixed notifier categories the signin summary may carry; anything else is
# ignored so no arbitrary collaborator field is ever forwarded.
_SIGNIN_ERROR_CATEGORIES = ("network", "upstream", "rate", "auth", "permission")

# Fixed Chinese labels for the router's own stop reasons.  A reason outside this
# table is rendered as an unknown cause; no free-form string is ever displayed.
_ROUTE_REASON_TEXTS = {
    "ok": "成功",
    "cooling": "路由冷却中",
    "rate_limited": "上游限流",
    "upstream": "上游返回错误",
    "stream": "上游流中断",
    # 不叫"首帧超时"：同一个 key 也用在"正文已经吐了一部分、prompt 超时"那一轮上，
    # 那时候第一帧早发过了，文案会指向一个根本不存在的原因。
    "timeout": "超时",
    "error": "未知失败",
    "local_limit": "本地请求预算限制",
}

HELP_TEXT = """WorkBuddy 指令（仅管理员私聊）：
/wb help                 显示本帮助
/wb status               查看只读完整总览（就绪/登录/CLI 进程/监听/签到/通知/模型/主备/请求体上限）
/wb login                获取登录链接
/wb logout               退出登录
/wb models               列出本机 CLI 可用的模型
/wb signin               手动签到
/wb signin resume        恢复被暂停的签到"""


@register(
    "astrbot_plugin_workbuddy",
    "mydanyi",
    "将 WorkBuddy 接入 AstrBot：通过官方 CodeBuddy CLI 的 ACP 通道提供 OpenAI 兼容 API（普通/流式）、扫码登录、自动签到、模型列表与主备模型按顺序调用，以及可配置管理员 QQ 的签到与异常通知。",
    "0.3.0",
)
class WorkBuddyPlugin(Star):
    """The single WorkBuddy entry point."""

    def __init__(self, context: Context, config: dict | None = None) -> None:
        super().__init__(context, config)
        self._raw_config = config if config is not None else {}
        self._settings = None

        self.ready = False
        self.error_reason = ""
        self.data_dir = None
        self.state = None
        self.session = None
        self.account = None
        self.client = None
        self.chat = None
        self.gateway = None
        self.router = None
        self.scheduler = None
        self.notifier = None
        # ACP：按模型分进程的 CLI 池，懒启动——第一次登录或聊天才拉起子进程。
        self.acp = None
        # 本地模型清单：读 CLI 的 --help，不联网、不要登录。
        self.catalog = None

        self._login_task = None
        # 登录、登出、拆卸三条路都要「读走—取消—收尸」同一条 ``_login_task``。
        # 没有这把锁，两条命令交错时谁都能把对方的任务从槽里摘走再各塞一条新的：
        # 留下的那条没人认领，攥着运行时和 token 一直跑下去，而命令已经回了
        # "已退出登录"。锁只圈换任务这一小段——``_begin_login`` 等扫码的那几秒
        # 必须在锁外，不然一条登出能被登录链接堵住。
        self._login_task_lock = asyncio.Lock()
        # 收尾的串行闸门：``terminate()`` 的 shield 会把收尾留在后台跑，而下一次
        # ``initialize()`` 一进门也要收尾。两边同时做，旧的那一轮会在新的运行时
        # 建好之后再把它清一遍（见 ``_release_runtime``）。
        self._teardown_lock = asyncio.Lock()
        # 那把锁只排得清"谁先进去"，排不了"谁还没来排队"：``shield`` 只是把收尾
        # 包成一条任务，从它被创建到第一次被调度之间，``terminate()`` 完全可能先
        # 被取消。这段窗口里 ``initialize()`` 能直接拿到锁，把运行时建好，那条任务
        # 醒来之后再擦一遍——锁一点忙都帮不上。所以任务本身也留一份，
        # ``initialize()`` 先等它真的结束（见 ``_wait_for_pending_teardown``）。
        self._teardown_task = None
        # 定时重读模型清单的那条任务；同一时刻只允许存在一条。
        self._model_refresh_task = None
        # 已经作废、但还有子进程没死的池。``AcpRuntimePool.shutdown()`` 关不掉的
        # 那条会留在池自己的账上；池要是跟着 ``self.acp`` 一起被丢掉，那笔账也就
        # 没了——下一轮 initialize 建的是新池，这条 CLI 从此没人认得。所以留在这里，
        # 每次拆卸都再关一遍，直到它真的空掉。
        self._retired_pools = []
        self._notify_tasks = set()
        self._notify_umo = None
        self._notify_settings = None
        # No proactive notice may be queued until the runtime is fully built.
        self._notify_stopped = True
        self._logout_in_progress = False
        # 登出已经开场、还在清场。跟 ``_logout_in_progress`` 不是一回事：那个是告警
        # 闸门（"这次失败是我自己弄的，别报"），这个是**登录的准入闸门**。
        # ``_logout`` 摘走并取消的只是"它进锁那一刻槽里的那条"；此后新起的登录它
        # 管不着——等人家扫完码把凭据写回去，这边刚好清完场，还回一句"已退出登录"。
        self._logout_pending = False
        self._gateway_bind_failed = False

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #

    async def initialize(self) -> None:
        """Build the runtime; a repeated call releases the previous one first."""
        await self._wait_for_pending_teardown()
        await self._release_runtime()

        try:
            settings = load_settings(self._raw_config)
        except ConfigError as exc:
            self._fail(str(exc))
            return
        self._settings = settings
        self._notify_settings = settings["notifications"]
        self._logout_in_progress = False
        self._gateway_bind_failed = False

        try:
            data_dir = StarTools.get_data_dir(PLUGIN_NAME)
        except Exception:
            self._fail("无法获取插件数据目录，请检查插件安装。")
            return
        self.data_dir = data_dir

        try:
            state = StateStore(data_dir)
            state.load()  # a corrupted state file disables the plugin, never wiped
        except ValueError:
            self._fail("本地状态文件损坏，请联系维护者后再重载。")
            return
        self.state = state

        # CLI 的两个目录都放在插件数据目录下：配置目录管行为，家目录管凭据（CLI 用
        # os.homedir() 解析凭据路径，只有改 HOME 才圈得住），容器重建后登录态仍在。
        acp = settings["acp"]
        self.acp = AcpRuntimePool(
            lambda model: self._make_runtime(model),
            max_processes=acp["max_processes"],
            primary_model=acp["model"] or None,
        )
        # 清单探针必须与聊天进程跑在同一套环境里：``codebuddy --help`` 会带上配置
        # 目录里的自定义模型（``custom-local:``），探针若继承宿主 env，就会和聊天进程
        # 报出两份不一样的清单。``config_path`` 同理——不给路径，``models.json`` 那条
        # 回退永远是死代码：``_from_config`` 见到空路径直接返回空。
        self.catalog = ModelCatalog(
            acp["cli_path"],
            config_path=self._models_path(),
            env=default_env(config_dir=self._config_dir(), home_dir=self._home_dir()),
        )

        # The ordered primary / standby route is a purely local decision layer
        # over the validated settings and the shared state store.  It is built
        # even while disabled (so the overview can always report a real view),
        # starts no background probe of its own, and is never a second producer.
        # Its construction lives in the runtime try below: a failure must fail
        # the plugin instead of leaving ``router`` silently ``None``.
        http = settings["http"]
        timeout = aiohttp.ClientTimeout(
            total=http["total_timeout"],
            connect=http["connect_timeout"],
            sock_read=http["sock_read_timeout"],
        )
        try:
            self.router = ModelRouter(
                settings["routing"], state, on_result=self._on_routing_result
            )
            self.session = aiohttp.ClientSession(timeout=timeout)
            # 凭据只有一个来源：CLI 自己落盘的那份。插件不再跑 OAuth —— 两个
            # 刷新者迟早互相把对方的 token 作废。
            #
            # 拿到的是**池里那条主运行时**（不是临时造一条）：它由池持有，登录成功
            # 后的重启、退出登录时的回收都还管得到它。自己造一条就成了一条谁也不
            # 认领的进程。
            self.account = CliCredentials(self.acp.primary)
            self.client = UpstreamClient(
                self.session,
                self.account,
                billing_base=BILLING_BASE,
                on_error=self._on_upstream_error,
            )
            self.notifier = AdminNotifier(
                self.context, state, settings["notifications"]
            )
            # 聊天后端在 _build_gateway 里按需装配，这里不先建一份：网关与后端
            # 必须成对出现，否则登录成功重建网关时会留下一个没人持有的旧后端。
            await self._build_gateway()

            signin = settings["signin"]
            self.scheduler = SigninScheduler(
                SigninService(self.client),
                state,
                timezone=signin["timezone"],
                primary=signin["primary"],
                recovery=signin["recovery"],
                jitter=signin["jitter"],
                max_attempts=signin["max_attempts"],
                enabled=signin["enabled"],
                on_result=self._on_signin_result,
            )
        except Exception:
            # 这条 except 把任何构造期异常都压成一句"初始化失败"。不打日志，日志里
            # 就真的只剩这一句——真实踩过：网关端口被占和字段读错长得一模一样。
            _log.exception("workbuddy: runtime construction failed")
            await self._release_runtime()
            self._fail("插件初始化失败，请查看日志后重载。")
            return

        # The native model picker is filled from the local catalog.  This read is
        # deliberately outside the runtime try above and outside the notice gate
        # below: a missing CLI only leaves the picker on the saved choices, and
        # can never disable the gateway, the scheduler or the login flow.
        await self._prime_model_options()

        self.ready = True
        self.error_reason = ""

        # The runtime is usable again, so proactive notices may be queued.  A
        # failed gateway bind is only reported as a fixed runtime notice, never
        # with the raw exception text.
        self._notify_stopped = False
        if self._gateway_bind_failed:
            self._gateway_bind_failed = False
            self._queue_notice(
                self._error_notify_on(), lambda: self.notifier.error("runtime")
            )

        # Only an existing, decrypted account may start the automatic loop.
        if settings["signin"]["enabled"] and self.account.credentials:
            try:
                self.scheduler.start()
            except Exception:
                pass

        # 定时重读模型清单。它只碰本机 CLI，与账号、网关都无关，所以不看签到那份
        # 前提；起不来也只是静默跳过，绝不影响已经可用的运行时。
        self._start_model_refresh()

    async def terminate(self) -> None:
        """Release every owned resource; safe to call more than once."""
        # 整段收尾都要罩住，不只是关进程那一步：挂账发生在它之前，而取消落在更早的
        # 等待点（停监听、收登录任务）上时，池连账都没上，罩住后半段也白搭——那正是
        # "没人认得的 CLI"在同一个函数的更早窗口重开。``shield`` 让外层取消照样立刻
        # 抛出，收尾在后台跑完（事件循环整个退出就真没辙了，那属于进程级收尾，接受）。
        #
        # 任务先落地再 shield：只有从 ``_teardown_task`` 上看得见，下一次
        # ``initialize()`` 才等得到它。已经在后台跑的那条直接复用，不另起一条。
        task = self._teardown_task
        if task is None or task.done():
            task = asyncio.ensure_future(self._release_runtime())
            self._teardown_task = task
        await asyncio.shield(task)

    # ------------------------------------------------------------------ #
    # runtime construction / teardown
    # ------------------------------------------------------------------ #

    def _make_runtime(self, model) -> AcpRuntime:
        """给池造一条运行时；``model`` 为 ``None`` 时用 CLI 自己的默认模型。"""
        acp = self._settings["acp"] if self._settings else {}
        data_dir = self.data_dir
        return AcpRuntime(
            argv=default_argv(
                acp.get("cli_path") or "codebuddy",
                tools=acp.get("tools", ""),
                model=model,
            ),
            cwd=data_dir,
            model=model,
            config_dir=self._config_dir(),
            home_dir=self._home_dir(),
            # 等扫码完成的上限。不接进来的话管理员改了没反应，实际永远走运行时里
            # 300 秒的默认值。
            login_timeout=float(acp.get("login_timeout") or 120),
        )

    def _config_dir(self):
        if not self.data_dir:
            return None
        return os.path.join(self.data_dir, "codebuddy")

    def _home_dir(self):
        if not self.data_dir:
            return None
        return os.path.join(self.data_dir, "cli-home")

    def _models_path(self):
        """CLI 自己的模型清单文件（``--help`` 读不到时的回退来源）。

        CLI 读的是 ``<配置目录>/models.json``（CLI 2.182.0 的产物里就是这个相对路径）。
        配置目录由插件指定，所以这里能算准；算不出来时返回 ``None``，回退分支就按
        "没这个文件"处理，而不是去读一个猜出来的路径。
        """
        config_dir = self._config_dir()
        return None if config_dir is None else os.path.join(config_dir, "models.json")

    async def _build_gateway(self) -> None:
        s = self._settings["gateway"]
        self._gateway_bind_failed = False
        if self.chat is None:
            # 聊天直接走 ACP：把进程池交给后端，由它按请求里的模型挑一条进程、
            # 懒启动 CLI，并把 401 与正常收尾回灌到那条运行时的登录态（否则
            # /wb status 会一直显示"已登录"）。
            # 并发门不在这里设：网关会按自己的配置写到 chat.limits 上。
            self.chat = AcpChatBackend(
                None,
                pool=self.acp,
                model=self._settings["acp"]["model"] or None,
                # 聊天失败曾经由 UpstreamClient 的观察者顺手报给管理员；改走
                # ACP 之后那条链路不在了，这里补回来，否则群里开始报错而
                # 管理员一句提醒都收不到。
                on_error=self._on_chat_error,
            )
        self.gateway = Gateway(
            self.chat,
            api_key=s["api_key"],
            host=s["host"],
            port=s["port"],
            concurrency=s["concurrency"],
            rpm=s["rpm"],
            queue_size=s["queue_size"],
            queue_timeout=s["queue_timeout"],
            router=self.router,
            catalog=self.catalog,
            max_request_body_mb=s["max_request_body_mb"],
        )
        if s["enabled"] and s["api_key"]:
            try:
                await self.gateway.start()
            except Exception:
                # A failed bind leaves the gateway unlistening; the plugin stays
                # usable through commands and never fabricates a listener.  Only
                # a fixed runtime notice is queued, never the raw exception.
                self.gateway.bound_port = None
                self._gateway_bind_failed = True
                self._queue_notice(
                    self._error_notify_on(), lambda: self.notifier.error("runtime")
                )

    async def _wait_for_pending_teardown(self) -> None:
        """等上一轮被取消后留在后台的收尾真的结束。

        ``_teardown_lock`` 挡不住"还没来排队"的那一轮：``shield`` 只是把收尾包成
        一条任务，从 ``ensure_future`` 到它第一次被调度之间，``terminate()`` 可以
        先被取消。这段时间进 ``initialize()`` 会拿到一把没人排队的锁、把运行时建
        好，而那条任务醒来之后再清一遍——监听还在、``ready`` 还是 True，路由和通知
        却没了。任务本身留了一份在这里，就先等它，再谈锁。
        """
        task = self._teardown_task
        if task is None or task.done():
            return
        # 连这一等也要 shield。``gather`` 是**我们**等别人的一条等待，外层一旦取消
        # 这一轮初始化，取消会顺着 gather 传到那条收尾任务上——``return_exceptions``
        # 只收结果、不挡取消。而收尾可能在 ``gateway.stop()`` 里等 handler，被砍在
        # 那儿就跳过 runner 清理；偏偏 ``self.gateway`` 已经置空了，后面没人再关得掉
        # 那个监听，下一次初始化绑定同一个端口就失败。任务留着，再来一次还是等它。
        await asyncio.shield(asyncio.gather(task, return_exceptions=True))

    async def _release_runtime(self) -> None:
        """收尾；并发调用排队，后来的那一轮等前一轮真的收完再动手。

        ``terminate()`` 用 shield 把收尾挪到后台：外层取消之后它还在跑。下一次
        ``initialize()`` 的第一件事也是收尾——不排队的话，它会在旧那一轮还卡在
        某个等待点上时就把运行时重建好；旧收尾随后醒过来，继续把 ``router`` /
        ``notifier`` / ``catalog`` / ``_notify_umo`` 清一遍。监听还在、``ready``
        还是 True，路由和通知却没了，而且要等到下一次重载才看得出来。
        """
        async with self._teardown_lock:
            await self._release_runtime_once()

    async def _release_runtime_once(self) -> None:
        self.ready = False
        # Block new notifications before any producer is stopped.
        self._notify_stopped = True

        # 拆卸也要走同一把锁：一条登录命令正卡在换任务那一步时，这里读到的
        # ``None`` 只是"它还没塞进去"，不是"没有登录在跑"。摘空了就等于把一条
        # 攥着运行时和 token 的协程留在拆卸之后。
        async with self._login_task_lock:
            await self._cancel_login_task()

        # 定时重读必须停在 ``self.catalog`` 被丢掉之前：它每轮最后一步是发布候选，
        # 而 ``catalog`` 已置空时发布等于把候选刷成只剩已保存的那几项。cancel 只是
        # 投递，``await`` 才是真的停住，所以两步都要做。
        refresh_task = self._model_refresh_task
        self._model_refresh_task = None
        if refresh_task is not None and not refresh_task.done():
            refresh_task.cancel()
            await asyncio.gather(refresh_task, return_exceptions=True)

        # 先停网关再收 CLI。反过来（先 shutdown 进程池、后停监听）中间隔着调度器
        # 和提醒队列两个 await，那段时间端口还开着，任何进来的聊天都会走完整路由
        # 撞上"运行时已关闭"拿到 502；而 _notify_stopped 在函数开头就置位了，连一条
        # 提醒都不会有——纯静默失败。
        gateway = self.gateway
        self.gateway = None
        if gateway is not None:
            with contextlib.suppress(Exception):
                await gateway.stop()

        # 后端必须跟运行时一起丢。它持着下面这个池，而 shutdown 之后 get() 会永久
        # 抛 AcpError；留下的旧后端会让下一次 initialize 因为 `self.chat is not None`
        # 复用它，聊天从此恒 502。
        self.chat = None

        # Stop the scheduler *before* the runtime and before cancelling the notice
        # tasks. Before the runtime: an in-flight signin would hit the pool while it
        # is being shut down, 401 out of `CliCredentials.refresh()`, and persist
        # `paused=True` — so a reload leaves signin paused until an admin resumes it.
        # Before the notices: nothing new is queued while the pending ones drain.
        scheduler = self.scheduler
        self.scheduler = None
        if scheduler is not None:
            with contextlib.suppress(Exception):
                await scheduler.stop()

        # ACP 的 CLI 子进程必须在这里收掉：登录任务已经取消，再没人用它。
        acp = self.acp
        self.acp = None
        if acp is not None and acp not in self._retired_pools:
            # 先挂账再关：``shutdown()`` 里有等待点，被取消时下面的出账一行都执行
            # 不到，而那一刻恰恰最需要这片地方记着"它还没关干净"。
            self._retired_pools.append(acp)
        await self._drain_retired_pools()

        # 本地清单只是个内存快照，跟着运行时一起丢，下次重读。
        self.catalog = None

        notify_tasks = [t for t in self._notify_tasks if not t.done()]
        self._notify_tasks.clear()
        for task in notify_tasks:
            task.cancel()
        if notify_tasks:
            await asyncio.gather(*notify_tasks, return_exceptions=True)

        # The route is only consumed by the stopped gateway; drop the reference
        # after the listener is gone.
        self.router = None

        session = self.session
        self.session = None
        if session is not None:
            with contextlib.suppress(Exception):
                await session.close()

        # Drop every notification reference once the runtime is torn down.
        self.notifier = None
        self._notify_umo = None

    async def _drain_retired_pools(self) -> None:
        """把账上的池逐个关掉，关干净了才出账。

        用 ``shutdown`` 而不是 ``close_all``：池已经作废，旧引用不能再把 CLI 拉起来。
        **每个池每次都关**，不看 ``has_live_children`` 再决定——那是"关完之后"的判据，
        拿它当前置条件会漏掉一个刚挂上来、还什么都没关过的池（它的映射里全是进程，账上
        却还是空的）。``shutdown()`` 幂等，重复调只多一次关闭超时。

        关不掉的留在账上：它每多活一轮，就多一条"没人认得"的 CLI 的窗口，所以下一次
        拆卸还要再来一遍；关干净了立刻出账，免得这串东西越攒越长。

        只碰账上那几条，一次空循环就过去了，所以放在拆卸路径上不心疼。
        """
        for pool in list(self._retired_pools):
            with contextlib.suppress(Exception):
                await pool.shutdown()
            if not pool.has_live_children:
                with contextlib.suppress(ValueError):
                    self._retired_pools.remove(pool)

    def _fail(self, reason: str) -> None:
        self.ready = False
        self.error_reason = reason

    # ------------------------------------------------------------------ #
    # native model picker (``routing.models`` options / labels)
    #
    # The operator picks from the ids this machine's CLI actually accepts instead
    # of typing them.  Only the *delivered schema* is rewritten in memory: the
    # saved values, the validated settings, the router's order and every file on
    # disk stay exactly as the operator wrote them.  Options and labels are both
    # plain ids -- the multiplier suffix this plugin used to append is gone.
    # ------------------------------------------------------------------ #

    async def _prime_model_options(self) -> None:
        """Publish the saved choices, read the catalog once, publish again."""
        self._publish_model_options()
        await self._refresh_catalog()
        self._publish_model_options()

    async def _refresh_catalog(self) -> list:
        """Re-read the local model list; a failure keeps the last good one.

        **只在同一个 ``ModelCatalog`` 实例上重读，绝不替换 ``self.catalog``。** 定时
        循环每轮记下这个对象、读完比对还是不是它，用来判断"这一轮读的世界还算不算
        数"；重读若把实例换掉，那一轮会被判成"世界变了"而跳过，而且每轮都这样，
        定时从此只读不发。``ModelCatalog.refresh()`` 原地更新，这里也只做转发。
        """
        catalog = self.catalog
        if catalog is None:
            return []
        try:
            return await catalog.refresh()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a picker read never fails the runtime
            return catalog.models

    def _start_model_refresh(self) -> None:
        """Start the single periodic re-read; never reads here.

        这里只起任务、不读清单：启动首读已经由 ``_prime_model_options()`` 做过一
        次，紧接着再 spawn 一个 ``--help`` 只会把启动拖慢一倍。所以第一轮要等满一
        个间隔——顺带也让"刚启动就被重载"不会变成连续两次进程启动。
        """
        settings = self._settings
        refresh = (
            settings.get("model_refresh") if isinstance(settings, dict) else None
        )
        if not isinstance(refresh, dict) or refresh.get("enabled") is not True:
            return
        existing = self._model_refresh_task
        if existing is not None and not existing.done():
            # 幂等：这个方法只管"确保有一条在跑"，不管重启。换配置走一次
            # ``initialize()``——它开头就会 ``_release_runtime()``，旧任务在那里
            # 被收掉。生命周期交叠时也可能撞见一条上家的任务：留着它，让循环自己
            # 按当轮的清单重新取，而不是这里再起一条。
            return
        seconds = _refresh_interval_seconds(refresh.get("interval_minutes"))
        if seconds is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(
            self._model_refresh_loop(seconds), name="wb-model-refresh"
        )
        task.add_done_callback(self._consume_task)
        self._model_refresh_task = task

    async def _model_refresh_loop(self, interval_seconds: int) -> None:
        """Re-read the catalog once per interval, sequentially, until cancelled.

        每轮都走 :meth:`_refresh_catalog`：读失败时它保留上一次成功的清单、并把
        下一次尝试推后，所以这里不需要自己的重试与退避。发布是幂等的，重复发同
        一份候选不会产生副作用。

        **每轮开头先确认这一轮还该不该由我来跑。** 生命周期是"先拆后建"，两次调用
        交叠时，拆的那一次可能已经读过 ``_model_refresh_task`` 之后才被建的任务追上
        来：任务还在跑，但已经没人持有它。让它继续跑只有一个结果——按一份过期的
        （甚至空的）清单把配置页候选重发一遍，每个间隔一遍，而且永不退场。所以要
        查两条：**我还是不是插件握着的那一条**（换了新任务说明这次交接不是我的），
        以及**还有没有清单可发**。

        **发之前还要再判一次，判据是"我读的那份还在不在"。** 读清单那一步会 spawn
        一个 ``--help``，是真的让出点；运行时正好能在这段时间里被拆掉，`self.catalog`
        就在这个 await 里变成 ``None``。只在轮首判一次的话，回来照样按空清单发布，
        而且发布之后没有任何东西会把它纠正回来。检查与发布之间不许有 await，这个
        窗口才关得死。

        **换了一份（不是拆掉）只跳过这一轮，不退出。** 交叠的 ``initialize()`` 会
        重建清单，而它收尾的 ``_start_model_refresh()`` 看到这条任务还活着就不再起新
        的——这时候退出去，定时就永远停在那儿，配置页还显示"已启用"。跳过这一轮，
        下一轮轮首重新取当前那份，自己就接上了。
        """
        while True:
            await asyncio.sleep(interval_seconds)
            if asyncio.current_task() is not self._model_refresh_task:
                return
            catalog = self.catalog
            if catalog is None:
                return
            await self._refresh_catalog()
            if self.catalog is not catalog:
                # 拆了就是拆了（None，下一轮轮首收尾）；换了一份就重来一轮。
                continue
            self._publish_model_options()

    def _publish_model_options(self) -> None:
        """Rebuild the delivered ``routing.models`` options/labels in place.

        A saved value the local catalog no longer contains is kept and marked
        unconfirmed, never dropped and never shown as verified availability.
        """
        catalog = self.catalog
        models = catalog.models if catalog is not None else []
        known = set(models)

        values = []
        labels = []
        for saved in self._saved_model_values():
            pure = strip_model_alias(saved).strip()
            if not pure or pure == ORDERED_MODEL_ID or pure in values:
                continue
            values.append(pure)
            if models and pure not in known:
                labels.append("{}（当前目录无此模型）".format(pure))
            else:
                labels.append(pure)
        for model in models:
            if model in values or model == ORDERED_MODEL_ID:
                continue
            values.append(model)
            labels.append(model)

        # A plain dict config has no schema to extend.
        schema = getattr(self._raw_config, "schema", None)
        section = schema.get("routing") if isinstance(schema, dict) else None
        items = section.get("items") if isinstance(section, dict) else None
        field = items.get("models") if isinstance(items, dict) else None
        if not isinstance(field, dict):
            return
        field["options"] = values
        field["labels"] = labels

    def _saved_model_values(self) -> list:
        """The operator's saved model strings, in their configured order."""
        section = (
            self._raw_config.get("routing")
            if hasattr(self._raw_config, "get")
            else None
        )
        saved = section.get("models") if isinstance(section, dict) else None
        if isinstance(saved, list):
            values = [
                item for item in saved if isinstance(item, str) and item.strip()
            ]
            if values:
                return values
        routing = (
            self._settings.get("routing")
            if isinstance(self._settings, dict)
            else None
        )
        models = routing.get("models") if isinstance(routing, dict) else None
        if isinstance(models, list):
            return [
                item for item in models if isinstance(item, str) and item.strip()
            ]
        return []

    # ------------------------------------------------------------------ #
    # proactive admin notifications
    #
    # Every hook below is *sync* and returns ``None`` immediately.  It must never
    # return an awaitable: the upstream client and the signin scheduler ``await``
    # a hook's return value, so an ``async`` hook (or a returned ``Task``) would
    # block the real request.  Instead a hook queues the notifier coroutine as a
    # controlled background task, and only when the matching switch is on.
    # ------------------------------------------------------------------ #

    def _signin_notify_on(self) -> bool:
        s = self._notify_settings
        return bool(
            isinstance(s, dict)
            and s.get("enabled") is True
            and s.get("signin_success") is True
        )

    def _error_notify_on(self) -> bool:
        s = self._notify_settings
        return bool(
            isinstance(s, dict)
            and s.get("enabled") is True
            and s.get("errors") is True
        )

    def _queue_notice(self, enabled: bool, factory) -> None:
        """Queue one notifier coroutine as a controlled background task."""
        if not enabled or self._notify_stopped or self.notifier is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        try:
            task = loop.create_task(factory(), name="wb-notify")
        except Exception:
            return
        self._notify_tasks.add(task)
        task.add_done_callback(self._consume_notify_task)

    def _consume_notify_task(self, task) -> None:
        """Discard a finished notice task and retrieve its exception."""
        self._notify_tasks.discard(task)
        with contextlib.suppress(BaseException):
            task.exception()

    @staticmethod
    def _signin_error_category(result: dict):
        """Map a signin summary to a fixed notifier category, or ``None``."""
        status = result.get("http_status")
        if status == 401:
            return "auth"
        if status == 403:
            return "permission"
        if status == 429:
            return "rate"
        category = result.get("error_category")
        if category in _SIGNIN_ERROR_CATEGORIES:
            return category
        outcome = result.get("outcome")
        if outcome == "rate_limited":
            return "rate"
        if outcome == "network":
            return "network"
        if outcome == "paused":
            # A 401/403 that paused the scheduler without a surfaced status.
            return "auth"
        if outcome in ("unknown", "unconfirmed"):
            return "signin"
        if outcome == "error":
            return "upstream"
        return None

    def _on_signin_result(self, result) -> None:
        """SigninScheduler ``on_result`` hook: sync, never returns an awaitable."""
        if not isinstance(result, dict):
            return
        if result.get("verified") and result.get("outcome") in (
            "signed",
            "already_signed",
        ):
            self._queue_notice(
                self._signin_notify_on(), lambda: self.notifier.signin(result)
            )
            return
        # notlogin / blocked / a normal inactive day never alert.
        category = self._signin_error_category(result)
        if category is not None:
            self._queue_notice(
                self._error_notify_on(),
                lambda: self.notifier.error(category, source="signin"),
            )

    def _on_upstream_error(self, category) -> None:
        """UpstreamClient ``on_error`` observer: sync, never returns an awaitable."""
        if self.client is None or self.notifier is None:
            return
        # An unauthenticated failure is not an account problem worth alerting.
        if not self.client.credentials.get("accessToken"):
            return
        # A logout in progress clears credentials on purpose; alerts resume once
        # a login completes again.
        if self._logout_in_progress:
            return
        # Never recurse: an alert's own request failing must not spawn a second
        # alert.  Keyed by the running task, so genuine business failures
        # (running under normal tasks) are still reported.
        current = asyncio.current_task()
        if current is not None and current in self._notify_tasks:
            return
        # Read the observer's source synchronously: the queued notifier runs in
        # its own task, where the request-scoped context is already gone.
        source = self.client.error_source
        self._queue_notice(
            self._error_notify_on(),
            lambda: self.notifier.error(category, source=source),
        )

    def _on_chat_error(self, category) -> None:
        """AcpChatBackend ``on_error`` observer: sync, never returns an awaitable.

        与 :meth:`_on_upstream_error` 同一套闸门：未登录时聊天失败是预期的，
        不去打扰管理员；退出登录进行中同理；提醒自身引发的失败也不再提醒。
        """
        if self.notifier is None:
            return
        # 写成正向条件，才与 _on_upstream_error 同形：没有凭据来源、或凭据里
        # 没有 accessToken，都说明"还没登录"，此时聊天失败是预期的。
        if self.account is None or not self.account.credentials.get("accessToken"):
            return
        if self._logout_in_progress:
            return
        current = asyncio.current_task()
        if current is not None and current in self._notify_tasks:
            return
        self._queue_notice(
            self._error_notify_on(),
            lambda: self.notifier.error(category, source="chat"),
        )

    @staticmethod
    def _routing_error_category(event: dict):
        """Map one failed ordered round to a fixed notifier category, or None.

        The explicit HTTP status wins over the router's own reason label, and
        the router's free-form ``category`` field is never trusted.
        """
        status = event.get("status")
        if isinstance(status, int) and not isinstance(status, bool):
            if status == 401:
                return "auth"
            if status == 403:
                return "permission"
            if status == 429:
                return "rate"
            if status >= 500:
                return "upstream"
        reason = event.get("reason")
        if reason == "timeout":
            return "network"
        if reason in ("stream", "error", "upstream"):
            return "upstream"
        return None

    def _routing_cooldown_remaining(self):
        """The router's own route-wide cooldown seconds, read locally only."""
        router = self.router
        if router is None:
            return None
        try:
            status = router.status()
        except Exception:
            return None
        value = status.get("global_cooldown_remaining") if isinstance(status, dict) else None
        return value if is_finite_number(value) else None

    def _on_routing_result(self, event) -> None:
        """ModelRouter ``on_result`` hook: sync, never returns an awaitable."""
        if not isinstance(event, dict):
            return
        if event.get("outcome") != "failed":
            return
        reason = event.get("reason")
        if not isinstance(reason, str) or reason not in _ROUTE_REASON_TEXTS:
            return
        # A local admission-budget refusal is not an upstream failure.
        if reason == "local_limit":
            return
        if self.client is None or self.notifier is None:
            return
        if not self.client.credentials.get("accessToken"):
            return
        if self._logout_in_progress:
            return
        # Never recurse from inside a notifier run.
        current = asyncio.current_task()
        if current is not None and current in self._notify_tasks:
            return
        category = self._routing_error_category(event)
        if category is None:
            return
        attempts = event.get("attempt_count")
        if not isinstance(attempts, int) or isinstance(attempts, bool):
            raw = event.get("attempts")
            attempts = len(raw) if isinstance(raw, list) else 0
        retry_after = event.get("retry_after")
        summary = {
            "attempts": max(0, attempts),
            "reason": reason,
            "retry_after": retry_after if is_finite_number(retry_after) else None,
            "cooldown_remaining": self._routing_cooldown_remaining(),
        }
        self._queue_notice(
            self._error_notify_on(),
            lambda: self.notifier.error(
                category, source="chat", routing_summary=summary
            ),
        )

    # ------------------------------------------------------------------ #
    # the single command entry point
    # ------------------------------------------------------------------ #

    @filter.command("wb")
    async def wb(
        self,
        event: AstrMessageEvent,
        action: str = "help",
        rest: str = "",
    ):
        """One admin-only, private-chat entry point for every action."""
        if not (event.is_admin() and event.is_private_chat()):
            yield event.plain_result("该指令仅限管理员在私聊中使用。")
            return

        action = (action or "help").strip().lower()
        rest = (rest or "").strip().lower()

        if action in ("", "help"):
            yield event.plain_result(HELP_TEXT)
            return

        if not self.ready:
            # 显示已存储的固定安全原因（配置错误 / 文件损坏），不把任何用户原始
            # 配置或 secret 回显给用户。
            yield event.plain_result(
                "插件尚未就绪：" + (self.error_reason or "请重载插件后再试。")
            )
            return

        if action == "status":
            yield event.plain_result(await self._status_text())
            return
        if action == "login":
            yield event.plain_result(await self._begin_login(event))
            return
        if action == "logout":
            yield event.plain_result(await self._logout())
            return
        if action == "models":
            yield event.plain_result(await self._models_text(refresh=True))
            return
        if action == "signin":
            yield event.plain_result(await self._signin_text(rest))
            return

        yield event.plain_result(HELP_TEXT)

    # ------------------------------------------------------------------ #
    # login / logout
    # ------------------------------------------------------------------ #

    async def _cancel_login_task(self) -> None:
        """把在跑的登录协程摘下来、取消、收尸。

        调用方必须持有 ``self._login_task_lock``。「读走—取消—收尸」三件事拆开做，
        两条命令交错时都能读到 ``None``，各自再塞一条新任务进去：后塞的那条没人
        取消，攥着运行时和 token 活到下次登录。
        """
        existing = self._login_task
        self._login_task = None
        if existing is None or existing.done():
            return
        existing.cancel()
        # 收尸不用 ``suppress(BaseException)``：它分不清"我们自己取消的那条"与
        # "外层取消了这个协程"——两者抛的都是 ``CancelledError``，一 suppress 就
        # 把重载/卸载时的取消一起咽了，函数接着往下走（登录处会去发起新的登录，
        # 退出登录处会边被取消边删凭据）。``gather`` 把前者的 ``CancelledError``
        # 收成返回值，后者照常往外抛。
        await asyncio.gather(existing, return_exceptions=True)

    async def _begin_login(self, event: AstrMessageEvent) -> str:
        pool = self.acp
        if pool is None:
            return "ACP 运行时尚未初始化，请重载插件后再试。"
        runtime = pool.primary
        captured = {"url": None}
        ready = asyncio.Event()
        # 这条命令的回复一旦交出去，再推一次链接就是第二份。只有"回复里没有链接"的
        # 那种情况才需要补发，见下面 ``_on_auth_url``。
        answered = {"done": False}
        # 回调有没有真把这条链接推出去过。取消除草时靠它判断"手上这条还需不需要补发"。
        pushed = {"sent": False}

        def _on_auth_url(url: str) -> None:
            captured["url"] = url
            ready.set()
            # 链接赶在回复之前到：回复里会带它，这里再推一遍，同一个会话就收到两条
            # 一模一样的链接（真机上就是这么弹了两个）。回复已经交出去了才要补一条：
            # 那种情况下用户手里只有"稍后查看"，不补就永远等不到。
            if answered["done"]:
                pushed["sent"] = True
                task = asyncio.create_task(self._notify("登录链接：\n" + url))
                task.add_done_callback(self._consume_task)

        # 换任务这一段要在锁里：否则正在跑的登出会把"槽里是 None"当成"没有登录在
        # 跑"，取消落空之后再让它把这条新任务塞进去。
        async with self._login_task_lock:
            if self._logout_pending:
                # 登出还在清场。这条窗口里起来的登录没人会取消它：等它扫完码把凭据
                # 写回去，那边刚好清完场，回复还是"已退出登录"。在锁里看，跟登出
                # 置位是同一把锁，不会看漏。
                return "正在退出登录，请稍后再试。"
            await self._cancel_login_task()
            self._notify_umo = event.unified_msg_origin
            task = asyncio.create_task(
                self._run_acp_login(runtime, _on_auth_url), name="wb-acp-login"
            )
            self._login_task = task
            task.add_done_callback(self._consume_task)

        # 链接通常几百毫秒内到达；最多等几秒，拿到就回复，拿不到也先把登录任务
        # 留着等扫码，不重复发起第二次 authenticate。
        waiter = asyncio.create_task(ready.wait())
        try:
            with contextlib.suppress(Exception):
                await asyncio.wait(
                    {waiter, task},
                    timeout=_LOGIN_LINK_SECONDS,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            if not waiter.done():
                waiter.cancel()
            # 同上：``suppress`` 分不清"我们取消的 waiter"与"外层取消了这个协程"。
            await asyncio.gather(waiter, return_exceptions=True)
        except BaseException:
            # 取消路径上没有"回复"这回事——命令已经结束了。但登录协程还留着等扫码，
            # 这时不把通道切到通知，后到的链接会以为"回复里带了"，一条也不发，用户
            # 只能干等到超时。已经到手、回调却还没推过的那条（那一刻门还是 False）
            # 同样要在这里补一次：它本来要跟着回复一起交出去的，回复没了它也就没了。
            answered["done"] = True
            if captured["url"] and not pushed["sent"]:
                late = asyncio.create_task(self._notify("登录链接：\n" + captured["url"]))
                late.add_done_callback(self._consume_task)
            waiter.cancel()
            raise

        # 从这行起，链接再到达就只能走通知。到读 captured 之间没有 await，回调插不进来。
        answered["done"] = True
        if self.acp is not pool:
            # 池子已经换了，这条登录多半跟着作废；但链接是此刻唯一还能给出去的东西，
            # 别在这儿把它吞掉——扫得动算捡到，扫不动至少知道自己拿到过什么。
            if captured["url"]:
                return (
                    "插件状态已变化，请重新执行 /wb login。\n"
                    "本次已生成的链接（可能已失效）：\n" + captured["url"]
                )
            return "插件状态已变化，请重新执行 /wb login。"
        if captured["url"]:
            return "请使用手机或浏览器打开以下链接完成登录：\n" + captured["url"]
        if task.done() and not task.cancelled():
            # 登录协程把失败都收成了固定文案，靠返回值判断，不看异常。
            if task.result():
                return "已完成登录。"
            return "登录失败：CLI 没有给出登录链接，请检查 ACP 配置后重试。"
        return "登录已发起，但暂时没拿到链接，请稍后用 /wb status 查看。"

    async def _run_acp_login(self, runtime, on_auth_url) -> bool:
        try:
            await runtime.login(on_auth_url=on_auth_url)
        except asyncio.CancelledError:
            raise
        except AcpTimeoutError:
            await self._notify("登录未完成：等待扫码超时，请重新执行 /wb login。")
            return False
        except Exception:
            # 失败原文里可能有上游报文，绝不外传；只报固定文案。
            await self._notify("登录失败：无法完成 ACP 登录，请检查 CLI 后重试。")
            return False
        await self._on_login_success()
        return True

    async def _on_login_success(self) -> None:
        """登录成功后的共同善后：重启全部 CLI、解除暂停、恢复告警与签到。

        登录只发生在一条进程上，其它模型的进程是登录前起的，手里还记着"未登录"。
        这里统一重启整池，否则那些模型会一直 401，而用户明明已经登上了。
        """
        self._logout_in_progress = False
        pool = self.acp
        if pool is not None:
            with contextlib.suppress(Exception):
                await pool.close_all()
        try:
            old_gateway = self.gateway
            if old_gateway is not None:
                await old_gateway.stop()
            self.gateway = None
            await self._build_gateway()
        except Exception:
            pass
        if self.scheduler is not None:
            with contextlib.suppress(Exception):
                self.scheduler.resume()
        if self._settings is not None and self._settings["signin"]["enabled"]:
            with contextlib.suppress(Exception):
                self.scheduler.start()
        # 清单只取决于本地安装，跟登录无关；顺手重读一次，让配置页的候选是新的。
        with contextlib.suppress(Exception):
            await self._refresh_catalog()
            self._publish_model_options()
        await self._notify("登录成功，服务已恢复。")

    def _purge_legacy_credential_file(self) -> bool:
        """删掉 v0.1.3 留下的 ``credentials.enc``；删不掉就返回 ``False``。

        那条路径早就没人读了——登录凭据改由 CLI 自己落盘——但**文件还在原地**，
        里面是加密过的 access / refresh token。不删就是一句谎话：命令回复"已退出
        登录"，而谁拿到这个文件、再加上当初那把 master key，谁还能把令牌解出来。
        删不掉要如实报告，理由同 ``AcpRuntime._purge_credential_dir``。
        """
        if not self.data_dir:
            return True
        path = os.path.join(str(self.data_dir), "credentials.enc")
        try:
            os.remove(path)
        except FileNotFoundError:
            return True
        except OSError:
            _log.warning("legacy credential file could not be removed")
            return False
        return True

    async def _logout(self) -> str:
        # 跟 ``_begin_login`` 用同一把锁，只圈"摘走并取消"这一小段。后面删凭据、
        # 关进程要几秒，攥着锁会把一条并发的登录整个堵住。
        async with self._login_task_lock:
            # 关门跟摘任务是同一个动作的两半。只取消、不关门的话，这之后起来的登录
            # 谁也不管——等它扫完码把凭据写回去，这边刚好清完场，回复还是"已退出
            # 登录"。标志在锁里置位，``_begin_login`` 也在锁里看它，两边同一把锁。
            self._logout_pending = True
            await self._cancel_login_task()
        try:
            return await self._finish_logout()
        finally:
            # 失败也要开闸：卡在 True 上等于从此不许登录，只能重载插件。
            self._logout_pending = False

    async def _finish_logout(self) -> str:
        """登出已经定了，剩下的是清场；``_logout_pending`` 由调用方开合。"""
        # 标记"这次登出是有意的"：凭据是被主动清掉的，中途出岔子也不该弹一条假的
        # 告警。**置在这里而不是函数开头**——上面那步被取消时进程没关、凭据也没删，
        # 什么都不该留下；而这个标志是告警的闸门，漏在 True 上就等于从此不再报任何
        # 失败，直到下一次登录成功才被 ``_on_login_success`` 解开。
        self._logout_in_progress = True
        # 退出登录要删两处：CLI 配置目录，以及真正的凭据目录（在 ~/.local/share
        # 下，只有改了 HOME 才圈得住）。只删配置等于没退。
        purge_failed = False
        # 作废池里的子进程同样攥着 token，而 ``pool.logout()`` 只看活着的那个池。
        # 先再关一遍（关不掉的那条会留在账上），再判能不能删：删了它会把凭据原样
        # 写回去，而命令已经回复"已退出登录"——池子那边写死的规矩，这里也一样。
        await self._drain_retired_pools()
        if any(item.has_live_children for item in self._retired_pools):
            _log.warning(
                "logout refused: %d retired acp pool(s) still have live children",
                len(self._retired_pools),
            )
            purge_failed = True
        pool = self.acp
        if pool is not None and not purge_failed:
            try:
                # 池自己先关掉**全部**进程再删：留着的那条握着 token，会把凭据
                # 原样写回去，而命令已经回复"已退出登录"。
                await pool.logout(purge_config=True)
            except AcpError:
                # 预期内的失败：进程没死透、目录删不掉、home_dir 没配。命令层回
                # 固定文案就够，用户能照着去查。
                purge_failed = True
            except Exception:
                # 非 AcpError 是代码缺陷，不能混进"请检查权限"里静默掉。
                _log.exception("acp logout failed unexpectedly")
                purge_failed = True
        if self.scheduler is not None:
            with contextlib.suppress(Exception):
                await self.scheduler.stop()
        # Stop the listener first so no in-flight chat can refresh + re-store a
        # credential after the logout has cleared it.
        if self.gateway is not None:
            with contextlib.suppress(Exception):
                await self.gateway.stop()
        with contextlib.suppress(Exception):
            await self.account.logout()
        # 旧版那份额外凭据不归池管，得单独删；删不掉也算"没清干净"。
        if not self._purge_legacy_credential_file():
            purge_failed = True
        if purge_failed:
            return "已断开连接，但登录凭据未能清除，请检查插件数据目录权限或查看日志。"
        return "已退出登录，本地网关已停止监听。"

    # ------------------------------------------------------------------ #
    # command renderers (fixed Chinese summaries, no secrets)
    # ------------------------------------------------------------------ #

    async def _status_text(self) -> str:
        lines = ["WorkBuddy 插件状态：", "就绪：是"]
        creds = self.account.credentials if self.account is not None else {}
        acp_state = self.acp.auth_state if self.acp is not None else None
        if acp_state is True:
            lines.append("登录：已登录（CLI）")
        elif acp_state is False:
            lines.append("登录：未登录（CLI）")
        elif creds.get("accessToken"):
            # 凭据文件在，但本次运行还没真跑过一轮；不冒充"已验证"。
            lines.append("登录：已登录（凭据在，尚未实测）")
        else:
            lines.append("登录：未登录")
        if self.acp is not None:
            st = self.acp.status()
            running = st.get("running") or []
            lines.append(
                "ACP：{} 条 CLI 进程在跑（上限 {}）".format(
                    len(running), st.get("max_processes")
                )
            )
            if running:
                lines.append("ACP 模型：" + "、".join(_model_label(m) for m in running))
        if self.gateway is not None and getattr(self.gateway, "bound_port", None):
            lines.append("网关：监听中（端口 {}）".format(self.gateway.bound_port))
        else:
            lines.append("网关：未监听")

        lines.extend(self._format_model_refresh_status())

        if self.scheduler is not None:
            st = self.scheduler.status()
            signin_cfg = self._settings["signin"] if self._settings else {}
            lines.append(
                "签到自动：{}".format("已启用" if st.get("enabled") else "未启用")
            )
            lines.append(
                "签到状态：{}".format("已暂停" if st.get("paused") else "正常")
            )
            lines.append(
                "签到定时：每日 {} 主 / {} 恢复（时区 {}）".format(
                    st.get("primary") or signin_cfg.get("primary") or "无",
                    st.get("recovery") or signin_cfg.get("recovery") or "无",
                    st.get("timezone") or signin_cfg.get("timezone") or "无",
                )
            )
            lines.append(
                "签到今日尝试 {}，上次成功 {}".format(
                    st.get("attempts_today", 0),
                    st.get("last_success_date") or "无",
                )
            )
            if st.get("last_outcome"):
                lines.append("签到最近结果：" + str(st["last_outcome"]))

        # The notification overview is rendered purely from the notifier's own
        # read-only status: it never issues a request of its own.
        if self.notifier is not None:
            ns = self.notifier.status()
            lines.append("通知：{}".format("已启用" if ns.get("enabled") else "未启用"))
            lines.append(
                "通知成功播报：{}".format("开" if ns.get("signin_success") else "关")
            )
            lines.append(
                "通知异常播报：{}".format("开" if ns.get("errors") else "关")
            )
            lines.append(
                "通知接收 QQ：{}".format(ns.get("admin_qq") or "未配置")
            )
            lines.append("通知实例：" + self._format_notify_platform(ns))
            lines.append(
                "通知最近发送：" + self._format_notify_delivery(ns.get("last_delivery"))
            )

        # 模型清单是纯本地的内存快照：读它不发任何请求，所以状态页可以放心带上。
        lines.append(await self._models_text(refresh=False))

        # Fixed local overview lines: the gateway request-body limit and the
        # route view.  Nothing here queries anything.
        max_mb = None
        if self._settings is not None:
            gateway_cfg = self._settings.get("gateway")
            if isinstance(gateway_cfg, dict):
                max_mb = gateway_cfg.get("max_request_body_mb")
        lines.append("请求体上限：{} MiB".format(self._format_amount(max_mb)))
        if self.router is not None:
            lines.extend(self._format_routing_status(self.router.status()))
        return "\n".join(lines)

    def _routing_enabled(self) -> bool:
        """Whether the ordered primary / standby route is switched on."""
        router = self.router
        if router is None:
            return False
        try:
            status = router.status()
        except Exception:
            return False
        return isinstance(status, dict) and status.get("enabled") is True

    def _format_routing_status(self, status: dict) -> list:
        """Render the local route view; only ids from the user's own config."""
        lines = [
            "主备路由：{}".format("已启用" if status.get("enabled") else "未启用")
        ]
        raw_models = status.get("models")
        models = (
            [m for m in raw_models if isinstance(m, str) and m]
            if isinstance(raw_models, list)
            else []
        )
        cooldowns = status.get("cooldowns")
        cooldowns = cooldowns if isinstance(cooldowns, dict) else {}
        if models:
            lines.append("主备顺序（共 {} 个）：".format(len(models)))
            for index, model in enumerate(models, 1):
                remaining = self._format_cooldown(cooldowns.get(model))
                suffix = "" if remaining == "无" else "（冷却剩余 {}）".format(remaining)
                lines.append("- 第{}：{}{}".format(index, model, suffix))
        else:
            lines.append("主备顺序：未配置")
        lines.append(
            "整体冷却："
            + self._format_cooldown(status.get("global_cooldown_remaining"))
        )
        last_success = status.get("last_success_model")
        lines.append(
            "最近成功模型："
            + (last_success if isinstance(last_success, str) and last_success else "无")
        )
        lines.append(self._format_route_result(status.get("last_result")))
        return lines

    @staticmethod
    def _format_cooldown(value) -> str:
        """A fixed seconds duration for a cooldown, else ``无``."""
        if not is_finite_number(value) or value <= 0:
            return "无"
        if value < 1:
            return "不到 1 秒"
        return "{} 秒".format(int(round(float(value))))

    @staticmethod
    def _format_route_result(last) -> str:
        """Fixed success/failure summary of the last finished route round."""
        if not isinstance(last, dict):
            return "最近路由结果：无"
        outcome = last.get("outcome")
        if outcome == "success":
            result = "成功"
        elif outcome == "failed":
            result = "失败"
        else:
            result = "未知"
        attempts = last.get("attempts")
        count = len(attempts) if isinstance(attempts, list) else 0
        parts = [
            "最近路由结果：{}".format(result),
            "尝试 {} 个模型".format(count),
        ]
        reason = last.get("reason")
        label = _ROUTE_REASON_TEXTS.get(reason) if isinstance(reason, str) else None
        if label:
            parts.append("停止原因：" + label)
        return "，".join(parts)

    async def _signin_text(self, rest: str) -> str:
        if rest == "resume":
            self.scheduler.resume()
            return "已恢复签到。"
        creds = self.account.credentials if self.account is not None else {}
        if not creds.get("accessToken"):
            return "尚未登录，无法签到。"
        result = await self.scheduler.run_once(manual=True)
        outcome = result.get("outcome")
        if result.get("verified") and outcome != "already_signed":
            return "今日签到成功。"
        if outcome == "already_signed":
            return "今日已签到。"
        if outcome == "unconfirmed":
            return "签到结果未确认，请稍后查看状态。"
        if outcome == "paused":
            return "签到已暂停，请重新登录或使用 /wb signin resume 恢复。"
        if outcome == "rate_limited":
            return "签到被限流，请稍后再试。"
        if outcome == "blocked":
            return "当前不可签到（{}）。".format(result.get("reason", "已阻止"))
        return "签到未完成，请稍后重试。"

    def _format_model_refresh_status(self) -> list:
        """Two purely local overview lines: switch and interval, no read.

        只读已校验的设置，所以这个视图既不会额外 spawn 一次 ``--help``，也不会
        编一个"上次刷新时间"出来——清单的年龄由 ``/wb models`` 那个视图负责。
        """
        refresh = None
        if self._settings is not None:
            value = self._settings.get("model_refresh")
            refresh = value if isinstance(value, dict) else None
        enabled = isinstance(refresh, dict) and refresh.get("enabled") is True
        interval = refresh.get("interval_minutes") if isinstance(refresh, dict) else None
        if isinstance(interval, bool) or not isinstance(interval, int):
            interval = None
        lines = [
            "模型自动刷新：{}（间隔 {} 分钟，仅重读本机 CLI 清单）".format(
                "已启用" if enabled else "未启用",
                interval if interval is not None else "未知",
            )
        ]
        if enabled:
            lines.append(
                "刷新范围：仅定时更新模型顺序列表的候选；"
                "启动首读、登录后读取与 /wb models 不受此开关影响"
            )
        return lines

    async def _models_text(self, *, refresh: bool) -> str:
        """List the ids this machine's CLI accepts; no network, no login needed."""
        if refresh:
            await self._refresh_catalog()
            # 清单刚被重读过，配置页那份候选也要跟着换：否则命令说"现在有 A、B、
            # D"，运维回到配置页看到的还是旧的三项，两边对不上。发布是幂等的。
            self._publish_model_options()
        catalog = self.catalog
        models = catalog.models if catalog is not None else []
        if not models:
            return (
                "当前没有可用模型：本机没读到 CLI 的模型清单。\n"
                "请确认 acp.cli_path 指向已安装的 CodeBuddy CLI，"
                "或直接运行一次 codebuddy --help 看是否有 --model 那一行。"
            )
        lines = ["可用模型（来源：{}）：".format(_source_label(catalog.source))]
        for model in models:
            lines.append("- " + model)
        if self._routing_enabled():
            lines.append(
                "- {}（按顺序调用，不是真实模型）".format(ORDERED_MODEL_ID)
            )
        # 来源只说明"上次读成功是从哪读的"，不说明"多久以前"。读失败时清单会被
        # 原样保留（这是刻意的，见 core/models.py），所以这一行必须把年龄说出来：
        # 少了它，命令会在清单早已过期时仍然宣称"来源：CLI 帮助文本"，等于替一份
        # 可能已经不对的数据背书。
        age = catalog.age
        if age is not None:
            lines.append("读取于：{}".format(_age_label(age)))
        if catalog.stale:
            lines.append(
                "注意：这份清单已过了保鲜期，可能与本机 CLI 的现状不符；"
                "用 /wb models 重读一次。"
            )
        if not getattr(catalog, "authoritative", True):
            # 只读到 ``models.json`` 那份子集时，聊天入口不拿它当允许列表（见
            # ``core/gateway.py`` 的 ``_known_model``）。这件事必须说出来：否则运维
            # 看到"来源：models.json + 三个网关"，会以为这台机器只认这三个，而真相是
            # "CLI 的清单没读到，校验暂时关了"。
            lines.append(
                "注意：只读到 models.json 里的自定义网关，CLI 的完整清单没读到；"
                "此时聊天入口不校验模型名，打错的 id 会由 CLI 自己报错。"
            )
        lines.append("说明：清单来自本机 CLI，与账号登录状态无关。")
        return "\n".join(lines)

    @staticmethod
    def _format_amount(value) -> str:
        """Render a number as a plain value (no trailing ``.0``)."""
        if isinstance(value, bool) or value is None:
            return "未知"
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)

    @staticmethod
    def _format_notify_platform(status) -> str:
        """Fixed platform routing line: configured / auto-selected / not found."""
        platform_id = status.get("platform_id")
        if isinstance(platform_id, str) and platform_id:
            return "实例配置（{}）".format(platform_id)
        resolved = status.get("resolved_platform_id")
        if isinstance(resolved, str) and resolved:
            return "自动选择（{}）".format(resolved)
        return "未找到"

    @staticmethod
    def _format_notify_delivery(delivery) -> str:
        """Fixed, safe last-delivery line; only a fixed reason is ever shown."""
        if not isinstance(delivery, dict):
            return "暂无记录"
        outcome = delivery.get("outcome")
        category = delivery.get("category")
        if outcome == "sent":
            return "sent（{}）".format(category)
        if outcome == "failed":
            return "failed（{}，{}）".format(category, delivery.get("reason"))
        return "暂无记录"

    # ------------------------------------------------------------------ #
    # notifications / task hygiene
    # ------------------------------------------------------------------ #

    async def _notify(self, text: str) -> None:
        if not self._notify_umo:
            return
        chain = self._make_chain(text)
        if chain is None:
            return
        try:
            await self.context.send_message(self._notify_umo, chain)
        except Exception:
            # A notification failure never reverts credentials.
            pass

    @staticmethod
    def _make_chain(text: str):
        try:
            from astrbot.core.message.message_event_result import MessageChain

            chain = MessageChain()
            if hasattr(chain, "message"):
                return chain.message(text)
            return MessageChain(text)
        except Exception:
            return None

    @staticmethod
    def _consume_task(task) -> None:
        """Consume a background task's result so no exception is left unretrieved."""
        with contextlib.suppress(BaseException):
            task.exception()


def _refresh_interval_seconds(minutes):
    """把配置里的分钟换成秒；不是正整数就返回 ``None``（= 别起任务）。

    单独拿出来是因为这是整个定时链上唯一一处单位换算：写成 ``* 3600`` 或者干脆
    把分钟当秒睡，跑起来一切正常，只是频率差 60 倍，等到发现时早就上线了。
    """
    if isinstance(minutes, bool) or not isinstance(minutes, int) or minutes < 1:
        return None
    return minutes * 60


def _model_label(model) -> str:
    """状态页里显示一个模型 ID；``None`` 是"CLI 自己的默认模型"。"""
    return model if isinstance(model, str) and model else "默认模型"


def _source_label(source) -> str:
    """模型清单的来源标签（固定文案，不回显任何原始字符串）。"""
    if source == "help":
        return "CLI 帮助文本"
    if source == "config":
        return "本地 models.json"
    return "未知"


def _age_label(seconds) -> str:
    """把"多少秒之前"写成一句人话；只用于本地清单的年龄。"""
    if not isinstance(seconds, (int, float)) or seconds < 0:
        return "未知"
    if seconds < 90:
        return "{} 秒前".format(int(seconds))
    if seconds < 5400:
        return "{} 分钟前".format(int(seconds // 60))
    return "{} 小时前".format(int(seconds // 3600))
