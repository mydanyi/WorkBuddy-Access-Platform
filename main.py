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
    "timeout": "首帧超时",
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
    "0.2.0",
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
        self._notify_tasks = set()
        self._notify_umo = None
        self._notify_settings = None
        # No proactive notice may be queued until the runtime is fully built.
        self._notify_stopped = True
        self._logout_in_progress = False
        self._gateway_bind_failed = False

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #

    async def initialize(self) -> None:
        """Build the runtime; a repeated call releases the previous one first."""
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

    async def terminate(self) -> None:
        """Release every owned resource; safe to call more than once."""
        await self._release_runtime()

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

    async def _release_runtime(self) -> None:
        self.ready = False
        # Block new notifications before any producer is stopped.
        self._notify_stopped = True

        login_task = self._login_task
        self._login_task = None
        if login_task is not None and not login_task.done():
            login_task.cancel()
            with contextlib.suppress(BaseException):
                await login_task

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

        # ACP 的 CLI 子进程必须在这里收掉：登录任务已经取消，再没人用它。
        acp = self.acp
        self.acp = None
        if acp is not None:
            with contextlib.suppress(Exception):
                # shutdown 而不是 close_all：永久关闭，旧引用不能再把 CLI 拉起来。
                await acp.shutdown()

        # 本地清单只是个内存快照，跟着运行时一起丢，下次重读。
        self.catalog = None

        # Stop the scheduler *before* cancelling the notice tasks, so nothing
        # new is queued while the pending notices are drained.
        scheduler = self.scheduler
        self.scheduler = None
        if scheduler is not None:
            with contextlib.suppress(Exception):
                await scheduler.stop()

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
        """Re-read the local model list; a failure keeps the last good one."""
        catalog = self.catalog
        if catalog is None:
            return []
        try:
            return await catalog.refresh()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a picker read never fails the runtime
            return catalog.models

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

    async def _begin_login(self, event: AstrMessageEvent) -> str:
        pool = self.acp
        if pool is None:
            return "ACP 运行时尚未初始化，请重载插件后再试。"
        runtime = pool.primary
        existing = self._login_task
        self._login_task = None
        if existing is not None and not existing.done():
            existing.cancel()
            with contextlib.suppress(BaseException):
                await existing

        self._notify_umo = event.unified_msg_origin
        captured = {"url": None}
        ready = asyncio.Event()

        def _on_auth_url(url: str) -> None:
            captured["url"] = url
            ready.set()
            # 链接同时推到管理员 QQ：私聊窗口关掉也不至于卡在等链接。
            task = asyncio.create_task(self._notify("登录链接：\n" + url))
            task.add_done_callback(self._consume_task)

        task = asyncio.create_task(
            self._run_acp_login(runtime, _on_auth_url), name="wb-acp-login"
        )
        self._login_task = task
        task.add_done_callback(self._consume_task)

        # 链接通常几百毫秒内到达；最多等 5 秒，拿到就回复，拿不到也先把登录任务
        # 留着等扫码，不重复发起第二次 authenticate。
        waiter = asyncio.create_task(ready.wait())
        with contextlib.suppress(Exception):
            await asyncio.wait(
                {waiter, task},
                timeout=5.0,
                return_when=asyncio.FIRST_COMPLETED,
            )
        if not waiter.done():
            waiter.cancel()
        with contextlib.suppress(BaseException):
            await waiter

        if self.acp is not pool:
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

    async def _logout(self) -> str:
        # Mark the intentional logout first: credentials are cleared on purpose,
        # so even a failure mid-logout must never raise a spurious error notice.
        self._logout_in_progress = True
        existing = self._login_task
        self._login_task = None
        if existing is not None and not existing.done():
            existing.cancel()
            with contextlib.suppress(BaseException):
                await existing
        # 退出登录要删两处：CLI 配置目录，以及真正的凭据目录（在 ~/.local/share
        # 下，只有改了 HOME 才圈得住）。只删配置等于没退。
        purge_failed = False
        pool = self.acp
        if pool is not None:
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
