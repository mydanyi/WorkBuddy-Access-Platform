"""AstrBot entry point for the WorkBuddy plugin.

A single :class:`WorkBuddyPlugin` (a real ``astrbot.api.star.Star``) exposes one
``@filter.command("wb")`` handler.  Every action is gated by a runtime
``is_admin() and is_private_chat()`` check first, so a management-group chat or a
non-admin user never triggers an upstream request and never receives a login
link.

The plugin owns one shared runtime: a ``aiohttp`` session, the encrypted
credential store, the OAuth manager, an ``UpstreamClient``, a ``ChatService``,
a local OpenAI-compatible ``Gateway``, a ``SigninScheduler`` and a
``GrowthService``.  No AstrBot persona / agent / history / tool behaviour is
changed here; the gateway only forwards the caller's payload (with ``stream``
forced) to the upstream.
"""

from __future__ import annotations

import asyncio
import contextlib
import os

import aiohttp

from astrbot.api.star import Context, Star, StarTools, register
from astrbot.api.event import filter, AstrMessageEvent

from .core.auth import AuthManager
from .core.chat import ChatService
from .core.client import UpstreamClient
from .core.config import ConfigError, load_settings
from .core.credits import display_model_id, is_finite_number, upstream_model_id
from .core.gateway import Gateway
from .core.growth import GrowthService
from .core.notifications import AdminNotifier
from .core.routing import ORDERED_MODEL_ID, ModelRouter
from .core.scheduler import SigninScheduler
from .core.signin import SigninService
from .core.storage import CredentialStore, StateStore

PLUGIN_NAME = "astrbot_plugin_workbuddy"

# Fixed domestic upstream boundaries.  No raw URL is user-configurable; the
# tests patch these three module-level constants at the network edge.
CHAT_BASE = "https://copilot.tencent.com"
BILLING_BASE = "https://www.codebuddy.cn"
WEB_BASE = "https://www.workbuddy.cn"

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
/wb status               查看只读完整总览（就绪/登录/监听/签到/通知/积分/模型/主备/成长/请求体上限）
/wb login                获取登录链接
/wb logout               退出登录
/wb models               列出可用模型（带倍率与促销说明）
/wb balance              查看账户积分
/wb signin               手动签到
/wb signin resume        恢复被暂停的签到
/wb tasks                只读列出成长任务
/wb tasks run            后台执行成长任务
/wb tasks claim          后台领取已完成任务奖励
/wb tasks status         查看成长任务状态
/wb tasks cancel         取消成长任务
/wb tasks resume         恢复被暂停的成长任务"""


@register(
    "astrbot_plugin_workbuddy",
    "mydanyi",
    "将 WorkBuddy 接入 AstrBot，提供 OpenAI 兼容 API（普通/流式）、自动签到、成长任务一键执行与领奖、积分与模型倍率总览、模型列表定时刷新、主备模型按顺序调用，以及可配置管理员 QQ 的签到与异常通知。",
    "0.1.3",
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
        self.store = None
        self.state = None
        self.session = None
        self.auth = None
        self.client = None
        self.chat = None
        self.gateway = None
        self.router = None
        self.scheduler = None
        self.growth = None
        self.notifier = None

        self._login_task = None
        # The single periodic model-catalog refresh task (``None`` while the
        # local policy is switched off or before the runtime is ready).
        self._model_refresh_task = None
        self._notify_tasks = set()
        self._notify_umo = None
        self._notify_settings = None
        # A read-only snapshot of the last successful model catalog read; the
        # overview reuses it for the route multipliers instead of re-querying.
        self._model_meta = None
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

        env_name = settings["auth"]["master_key_env"]
        master_key = os.environ.get(env_name)
        if not master_key:
            self._fail("未设置主密钥环境变量 {}，请配置后重载插件。".format(env_name))
            return
        try:
            store = CredentialStore(data_dir, master_key)
        except ValueError:
            self._fail("主密钥无效：需为标准 base64 且解码后恰好 32 字节。")
            return
        try:
            state = StateStore(data_dir)
            state.load()  # a corrupted state file disables the plugin, never wiped
        except ValueError:
            self._fail("本地状态文件损坏，请联系维护者后再重载。")
            return
        try:
            store.load()  # a corrupted credential file disables the plugin
        except ValueError:
            self._fail("本地凭据无法解密，请检查主密钥或重新登录。")
            return
        self.store = store
        self.state = state

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
            self.auth = AuthManager(
                self.session,
                store,
                chat_base=CHAT_BASE,
                poll_interval=settings["auth"]["poll_interval"],
                login_timeout=settings["auth"]["login_timeout"],
            )
            self.client = UpstreamClient(
                self.session,
                self.auth,
                chat_base=CHAT_BASE,
                billing_base=BILLING_BASE,
                web_base=WEB_BASE,
                on_error=self._on_upstream_error,
                on_models=self._on_models,
            )
            self.notifier = AdminNotifier(
                self.context, state, settings["notifications"], self.client
            )
            self.chat = ChatService(self.client)
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
                on_success=self._on_signin_success,
                on_result=self._on_signin_result,
            )

            growth = settings["growth"]
            self.growth = GrowthService(
                self.client,
                self.chat,
                state,
                task_interval=growth["task_interval"],
                poll_interval=growth["poll_interval"],
                progress_timeout=growth["progress_timeout"],
                max_retries=growth["max_retries"],
                max_chat_requests=growth["max_chat_requests"],
                auto_claim=growth["auto_claim"],
                allowed_tasks=growth["allowed_tasks"],
                allow_telemetry=growth["allow_telemetry"],
                allow_theme_change=growth["allow_theme_change"],
                allow_adoption=growth["allow_adoption"],
                on_result=self._on_growth_result,
            )
        except Exception:
            await self._release_runtime()
            self._fail("插件初始化失败，请查看日志后重载。")
            return

        # The native model picker is filled from the account's real catalog.
        # This read is deliberately outside the runtime try above and outside
        # the notice gate below: a 429 / network / business failure only leaves
        # the picker on the saved choices, and can never disable the gateway,
        # the scheduler or the login flow, and never queues a notice.
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
        if settings["signin"]["enabled"] and self.auth.credentials:
            try:
                self.scheduler.start()
            except Exception:
                pass

        # The periodic catalog refresh is started only now: the runtime is
        # ready and the initial prime already ran, so this holds one sleeping
        # timer and never issues an immediate second read.  It is not tied to
        # the login state, so a plugin without credentials still keeps one
        # sleeping timer; the shared refresh helper stays silent until a login.
        self._start_model_refresh()

    async def terminate(self) -> None:
        """Release every owned resource; safe to call more than once."""
        await self._release_runtime()

    # ------------------------------------------------------------------ #
    # runtime construction / teardown
    # ------------------------------------------------------------------ #

    async def _build_gateway(self) -> None:
        s = self._settings["gateway"]
        self._gateway_bind_failed = False
        if self.chat is None:
            self.chat = ChatService(self.client)
        self.gateway = Gateway(
            self.chat,
            self.client,
            api_key=s["api_key"],
            host=s["host"],
            port=s["port"],
            concurrency=s["concurrency"],
            rpm=s["rpm"],
            queue_size=s["queue_size"],
            queue_timeout=s["queue_timeout"],
            router=self.router,
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

        # Stop the periodic catalog refresh *before* the shared client and HTTP
        # session are released below, so a blocked refresh is cancelled (its
        # request too) and no fetch can outlive the teardown.  Safe to repeat.
        refresh_task = self._model_refresh_task
        self._model_refresh_task = None
        if refresh_task is not None and not refresh_task.done():
            refresh_task.cancel()
            with contextlib.suppress(BaseException):
                await refresh_task

        # Stop the scheduler / growth producers *before* cancelling the notice
        # tasks, so nothing new is queued while the pending notices are drained.
        scheduler = self.scheduler
        self.scheduler = None
        if scheduler is not None:
            with contextlib.suppress(Exception):
                await scheduler.stop()

        growth = self.growth
        self.growth = None
        if growth is not None:
            with contextlib.suppress(Exception):
                await growth.cancel()

        notify_tasks = [t for t in self._notify_tasks if not t.done()]
        self._notify_tasks.clear()
        for task in notify_tasks:
            task.cancel()
        if notify_tasks:
            await asyncio.gather(*notify_tasks, return_exceptions=True)

        gateway = self.gateway
        self.gateway = None
        if gateway is not None:
            with contextlib.suppress(Exception):
                await gateway.stop()

        # The route is only consumed by the stopped gateway; drop the reference
        # (and its read-only multiplier snapshot) after the listener is gone.
        self.router = None
        self._model_meta = None

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
    # The operator picks from the account's real WorkBuddy catalog instead of
    # typing model ids.  Only the *delivered schema* is rewritten in memory: the
    # saved values, the validated settings, the router's order and every file on
    # disk stay exactly as the operator wrote them.  The option values are pure
    # upstream ids (plus any legacy alias the operator already saved), and the
    # parallel labels are the existing ``display_model_id`` metadata.
    # ------------------------------------------------------------------ #

    async def _prime_model_options(self) -> None:
        """Publish the saved choices, then attempt exactly one catalog read."""
        self._publish_model_options(None)
        await self._refresh_model_options()

    async def _refresh_model_options(self) -> None:
        """One noncritical catalog read; a failure keeps the last options.

        Only an authenticated account is read.  The successful list is published
        by the client's own ``on_models`` observer (``_on_models``), so no
        second read is ever issued and a failed read changes nothing.
        """
        client = self.client
        if client is None or not client.credentials.get("accessToken"):
            return
        try:
            await client.model_details()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a picker read never fails the runtime
            return

    def _start_model_refresh(self) -> None:
        """Start the single periodic catalog task; never fetches here.

        The switch and the interval come from the validated settings, so a
        disabled group leaves ``_model_refresh_task`` at ``None``.  A live task
        is never replaced, so a repeated start cannot stack a second timer.
        """
        settings = self._settings
        refresh = settings.get("model_refresh") if isinstance(settings, dict) else None
        if not isinstance(refresh, dict) or refresh.get("enabled") is not True:
            return
        existing = self._model_refresh_task
        if existing is not None and not existing.done():
            return
        minutes = refresh.get("interval_minutes")
        if isinstance(minutes, bool) or not isinstance(minutes, int) or minutes < 1:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        try:
            task = loop.create_task(
                self._model_refresh_loop(minutes * 60), name="wb-model-refresh"
            )
        except Exception:  # noqa: BLE001 - a timer is never worth a failure
            return
        task.add_done_callback(self._consume_task)
        self._model_refresh_task = task

    async def _model_refresh_loop(self, interval_seconds) -> None:
        """Refresh the catalog once per interval, sequentially, forever.

        The first read waits one whole interval, so the startup prime stays the
        only immediate read.  ``_refresh_model_options`` already checks the
        credentials and swallows ordinary fetch failures, so a failed interval
        keeps the last good options and simply waits for the next one; no tight
        retry.  One serial loop can never overlap two timer reads.
        """
        while True:
            await asyncio.sleep(interval_seconds)
            await self._refresh_model_options()

    def _on_models(self, details) -> None:
        """UpstreamClient ``on_models`` observer: sync, returns no awaitable."""
        self._publish_model_options(details)

    def _publish_model_options(self, details) -> None:
        """Rebuild the delivered ``routing.models`` options/labels in place.

        ``details`` is the last successful ``model_details`` snapshot, or
        ``None`` to publish only the operator's already saved values (startup /
        reset).  A value the live catalog no longer contains is kept and marked
        unconfirmed, never dropped and never shown as verified availability.
        """
        meta = {}
        catalog = []
        if isinstance(details, list):
            for detail in details:
                if not isinstance(detail, dict):
                    continue
                model_id = detail.get("id")
                if not isinstance(model_id, str) or not model_id:
                    continue
                if model_id in meta:
                    continue
                meta[model_id] = dict(detail)
                catalog.append(model_id)
        self._model_meta = meta or None
        has_catalog = bool(catalog)

        values = []
        labels = []
        seen = set()
        for saved in self._saved_model_values():
            pure = upstream_model_id(saved)
            pure = pure.strip() if isinstance(pure, str) else ""
            if not pure or pure == ORDERED_MODEL_ID or saved in seen:
                continue
            seen.add(saved)
            values.append(saved)
            labels.append(
                self._model_option_label(saved, pure, meta.get(pure), has_catalog)
            )
        for model_id in catalog:
            if model_id in seen or model_id == ORDERED_MODEL_ID:
                continue
            seen.add(model_id)
            values.append(model_id)
            labels.append(display_model_id(meta[model_id]))

        # A plain dict config has no schema to extend: the metadata snapshot
        # above is still published for the ``/wb status`` route view.
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

    @staticmethod
    def _model_option_label(value, model_id, detail, has_catalog) -> str:
        """The display label of one picker option (never a fabricated zero)."""
        if isinstance(detail, dict):
            label = display_model_id(detail)
            if value == model_id:
                return label
            # A saved legacy alias keeps its exact stored value as the option,
            # so the live metadata is only appended as an unconfirmed note.
            return "{}（旧倍率别名；当前目录为 {}）".format(value, label)
        if value != model_id:
            return "{}（旧倍率别名，未确认）".format(value)
        if has_catalog:
            return "{} [未确认：当前目录无此模型]".format(value)
        return display_model_id({"id": model_id})

    def _on_signin_success(self) -> None:
        """SigninScheduler callback: only the background growth run is started."""
        if self.growth is None or self._settings is None:
            return
        if self._settings["growth"]["auto_after_signin"]:
            try:
                self.growth.start()
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    # proactive admin notifications
    #
    # Every hook below is *sync* and returns ``None`` immediately.  It must never
    # return an awaitable: the upstream client and the signin scheduler ``await``
    # a hook's return value, so an ``async`` hook (or a returned ``Task``) would
    # block the real request.  Instead a hook queues the notifier coroutine as a
    # controlled background task, and only when the matching switch is on, so a
    # disabled notice can never trigger an extra balance read.
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

    @staticmethod
    def _growth_error_category(summary: dict):
        """Map a growth summary to a fixed notifier category, or ``None``."""
        code = summary.get("code")
        if code == "rate":
            return "rate"
        if code == "network":
            return "network"
        if code == "auth":
            # 401/403 is already reported -- and distinguished -- by the final
            # upstream-error observer, so never send a second auth notice.
            return None
        return "growth"

    def _on_growth_result(self, summary) -> None:
        """GrowthService ``on_result`` hook: sync, never returns an awaitable."""
        if not isinstance(summary, dict) or not summary.get("needs_attention"):
            return
        category = self._growth_error_category(summary)
        if category is None:
            return
        self._queue_notice(
            self._error_notify_on(),
            lambda: self.notifier.error(category, source="growth"),
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
        # Never recurse: an alert's own balance read failing must not spawn a
        # second alert.  Keyed by the running task, so genuine business failures
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
        # Never recurse from inside a notifier run (its own balance read).
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
            # 显示已存储的固定安全原因（配置错误 / 文件损坏 / 缺 key 处理不同），
            # 不把任何用户原始配置或 secret 回显给用户。
            reason = self.error_reason or (
                "请设置环境变量 {} 后重载插件。".format(self._master_key_env_name())
            )
            yield event.plain_result("插件尚未就绪：" + reason)
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
            yield event.plain_result(await self._models_text())
            return
        if action == "balance":
            yield event.plain_result(await self._balance_text())
            return
        if action == "signin":
            yield event.plain_result(await self._signin_text(rest))
            return
        if action == "tasks":
            yield event.plain_result(await self._tasks_text(rest))
            return
        if action.startswith("tasks "):
            yield event.plain_result(await self._tasks_text(action.split(" ", 1)[1]))
            return

        yield event.plain_result(HELP_TEXT)

    def _master_key_env_name(self) -> str:
        if self._settings is not None:
            return self._settings["auth"]["master_key_env"]
        section = self._raw_config.get("auth") if hasattr(self._raw_config, "get") else None
        if isinstance(section, dict) and isinstance(section.get("master_key_env"), str):
            return section["master_key_env"]
        return "WORKBUDDY_MASTER_KEY"

    # ------------------------------------------------------------------ #
    # login / logout
    # ------------------------------------------------------------------ #

    async def _begin_login(self, event: AstrMessageEvent) -> str:
        existing = self._login_task
        self._login_task = None
        if existing is not None and not existing.done():
            existing.cancel()
            with contextlib.suppress(BaseException):
                await existing
        # A new login supersedes any previous pending state / in-flight poll.
        self.auth.cancel_login()
        try:
            info = await self.auth.begin_login()
        except Exception:
            return "登录请求失败，请稍后重试。"
        url = info.get("authUrl") if isinstance(info, dict) else None
        state = info.get("state") if isinstance(info, dict) else None
        if not url:
            return "登录请求失败，请稍后重试。"
        self._notify_umo = event.unified_msg_origin
        self._login_task = asyncio.create_task(
            self._poll_login(state), name="wb-login-poll"
        )
        self._login_task.add_done_callback(self._consume_task)
        return "请使用手机或浏览器打开以下链接完成登录：\n" + url

    async def _poll_login(self, state) -> None:
        try:
            await self.auth.complete_login(state)
        except asyncio.CancelledError:
            raise
        except Exception:
            # A poll failure never reverts credentials; report nothing sensitive.
            return
        # Login succeeded: a fresh credential must not stay behind a permanently
        # paused scheduler / growth service.  Alerts resume from here on.  Cancel
        # the in-flight growth run, rebuild the shared gateway (an old, closed
        # gate is unusable), then lift the 401/403 pause in both services.  A
        # server-forced 429 cooldown (if any) is always preserved by both
        # resume() calls.
        self._logout_in_progress = False
        growth = self.growth
        if growth is not None:
            with contextlib.suppress(Exception):
                await growth.cancel()
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
        if self.growth is not None:
            with contextlib.suppress(Exception):
                self.growth.resume()
        if self._settings is not None and self._settings["signin"]["enabled"]:
            try:
                self.scheduler.start()
            except Exception:
                pass
        # A fresh credential may also unlock the model catalog: exactly one
        # refresh, after the auth semantics above, so a catalog failure can
        # never turn a successful login into a failure or a second notice.
        await self._refresh_model_options()
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
        if self.growth is not None:
            with contextlib.suppress(Exception):
                await self.growth.cancel()
        if self.scheduler is not None:
            with contextlib.suppress(Exception):
                await self.scheduler.stop()
        # Stop the listener first so no in-flight chat can refresh + re-store a
        # credential after the logout has cleared it.
        if self.gateway is not None:
            with contextlib.suppress(Exception):
                await self.gateway.stop()
        with contextlib.suppress(Exception):
            await self.auth.logout()
        return "已退出登录，本地网关已停止监听。"

    # ------------------------------------------------------------------ #
    # command renderers (fixed Chinese summaries, no secrets)
    # ------------------------------------------------------------------ #

    async def _status_text(self) -> str:
        lines = ["WorkBuddy 插件状态：", "就绪：是"]
        creds = self.auth.credentials if self.auth is not None else {}
        logged_in = bool(creds.get("accessToken"))
        lines.append("登录：{}".format("已登录" if logged_in else "未登录"))
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
        # read-only status: it never issues a balance request of its own, so a
        # status view cannot trigger an extra upstream read.
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

        if self.growth is not None:
            lines.append(self._format_growth_status(self.growth.status()))

        # A logged-in overview reads each source at most once, and the three
        # renderers are already safe on their own; gather them so one slow query
        # never serializes the whole status and one failure never hides another.
        balance_text, models_text, tasks_text = await asyncio.gather(
            self._balance_text(),
            self._models_text(),
            self._tasks_text(""),
            return_exceptions=True,
        )
        if isinstance(balance_text, BaseException):
            balance_text = "查询余额失败，请稍后重试。"
        if isinstance(models_text, BaseException):
            models_text = "获取模型列表失败，请稍后重试。"
        if isinstance(tasks_text, BaseException):
            tasks_text = "获取任务列表失败，请稍后重试。"
        lines.append(balance_text)
        lines.append(models_text)
        lines.append(tasks_text)

        # Fixed local overview lines: the gateway request-body limit and the
        # route view.  Nothing here queries the upstream.
        max_mb = None
        if self._settings is not None:
            gateway_cfg = self._settings.get("gateway")
            if isinstance(gateway_cfg, dict):
                max_mb = gateway_cfg.get("max_request_body_mb")
        lines.append("请求体上限：{} MiB".format(self._format_amount(max_mb)))
        lines.extend(self._format_model_refresh_status())
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
        meta = self._model_meta if isinstance(self._model_meta, dict) else {}
        if models:
            lines.append("主备顺序（共 {} 个）：".format(len(models)))
            for index, model in enumerate(models, 1):
                detail = meta.get(model)
                label = (
                    display_model_id(detail)
                    if isinstance(detail, dict)
                    else "{} [倍率未知]".format(model)
                )
                remaining = self._format_cooldown(cooldowns.get(model))
                suffix = "" if remaining == "无" else "（冷却剩余 {}）".format(remaining)
                lines.append("- 第{}：{}{}".format(index, label, suffix))
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

    def _format_model_refresh_status(self) -> list:
        """Two purely local overview lines: switch and interval, no request.

        The values are read from the validated settings only, so this view can
        never trigger an extra catalog read or invent a last-refresh time.
        """
        refresh = None
        if self._settings is not None:
            value = self._settings.get("model_refresh")
            refresh = value if isinstance(value, dict) else None
        enabled = isinstance(refresh, dict) and refresh.get("enabled") is True
        interval = refresh.get("interval_minutes") if isinstance(refresh, dict) else None
        if isinstance(interval, bool) or not isinstance(interval, int):
            interval = None
        return [
            "模型自动刷新：{}（间隔 {} 分钟，仅本地轮询策略）".format(
                "已启用" if enabled else "未启用",
                interval if interval is not None else "未知",
            ),
            "刷新范围：仅定时拉取候选与倍率；初始、登录与手动获取不受此开关影响",
        ]

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
        creds = self.auth.credentials if self.auth is not None else {}
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

    async def _models_text(self) -> str:
        creds = self.auth.credentials if self.auth is not None else {}
        if not creds.get("accessToken"):
            return "尚未登录，无法获取模型列表。"
        try:
            details = await self.client.model_details()
        except Exception:
            return "获取模型列表失败，请稍后重试。"
        catalog = (
            [detail for detail in details if isinstance(detail, dict)]
            if isinstance(details, list)
            else []
        )
        # The read-only catalog snapshot behind the ``/wb status`` route view is
        # published by the client's own success observer (``_on_models``), so
        # this renderer never duplicates it and never issues another read.
        lines = ["可用模型："]
        for detail in catalog:
            alias = display_model_id(detail)
            note = detail.get("promo_note")
            if isinstance(note, str) and note.strip():
                lines.append("- {}（{}）".format(alias, note.strip()))
            else:
                lines.append("- " + alias)
        if self._routing_enabled():
            # The virtual entry has no fixed price: the real model decides.
            lines.append(
                "- {}（倍率随实际模型变化）".format(ORDERED_MODEL_ID)
            )
        if len(lines) == 1:
            return "当前没有可用模型。"
        lines.append("提示：倍率为查询快照，实际扣费以官方结算为准。")
        return "\n".join(lines)

    async def _balance_text(self) -> str:
        creds = self.auth.credentials if self.auth is not None else {}
        if not creds.get("accessToken"):
            return "尚未登录，无法查询余额。"
        try:
            summary = await self.client.balance()
        except Exception:
            return "查询余额失败，请稍后重试。"
        if not isinstance(summary, dict) or not summary.get("known"):
            return "积分信息：未知（上游数据不完整）"
        return (
            "账户积分：\n"
            "剩余积分：{}\n"
            "已用积分：{}\n"
            "总额度：{}\n"
            "积分包：{}\n"
            "（本次返回积分包合计，非月度额度）"
        ).format(
            self._format_amount(summary.get("remaining")),
            self._format_amount(summary.get("used")),
            self._format_amount(summary.get("total")),
            self._format_amount(summary.get("package_count")),
        )

    @staticmethod
    def _format_amount(value) -> str:
        """Render a credit amount as a plain number (no trailing ``.0``)."""
        if isinstance(value, bool) or value is None:
            return "未知"
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)

    async def _tasks_text(self, rest: str) -> str:
        creds = self.auth.credentials if self.auth is not None else {}
        if not creds.get("accessToken"):
            return "尚未登录，无法执行任务。"
        if rest == "":
            try:
                data = await self.growth.list_tasks()
            except Exception:
                return "获取任务列表失败，请稍后重试。"
            return self._format_task_list(data.get("tasks", []))
        if rest == "run":
            started = self.growth.start()
            return (
                "已开始后台执行成长任务。"
                if started.get("started")
                else "成长任务已在执行中。"
            )
        if rest == "claim":
            started = self.growth.start(claim_only=True)
            return (
                "已开始后台领取已完成任务的奖励。"
                if started.get("started")
                else "成长任务已在执行中。"
            )
        if rest == "cancel":
            await self.growth.cancel()
            return "已取消成长任务。"
        if rest == "resume":
            self.growth.resume()
            return "已恢复成长任务。"
        if rest == "status":
            return self._format_growth_status(self.growth.status())
        return HELP_TEXT

    @staticmethod
    def _format_task_list(tasks) -> str:
        lines = ["成长任务列表："]
        for task in tasks:
            if not isinstance(task, dict):
                continue
            progress = task.get("progress") or {}
            # 先判定 claimed 再判定 complete：已领取的任务不能误报成仅「已完成」。
            if task.get("accept_status") == "claimed":
                state = "已领取"
            elif task.get("complete"):
                state = "已完成"
            else:
                state = "进行中"
            lines.append(
                "- {}：{}（{}/{}, {}）".format(
                    task.get("task_code"),
                    task.get("title") or "",
                    progress.get("current", 0),
                    progress.get("target", 0),
                    state,
                )
            )
        if len(lines) == 1:
            lines.append("暂无任务。")
        return "\n".join(lines)

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

    @staticmethod
    def _format_growth_status(status) -> str:
        parts = ["成长任务状态："]
        parts.append("运行中" if status.get("running") else "空闲")
        parts.append(
            "今日聊天 {}/{}".format(
                status.get("chat_requests_today", 0),
                status.get("max_chat_requests", 0),
            )
        )
        if status.get("paused"):
            parts.append("已暂停")
        if status.get("retry_after"):
            parts.append("冷却剩余 {} 秒".format(int(status["retry_after"])))
        last = status.get("last_result")
        if isinstance(last, dict):
            # 动作 / 服务端进度 / 确认奖励分开展示，绝不伪造进度或奖励值。
            for item in last.get("actions") or []:
                if isinstance(item, dict):
                    parts.append(
                        "动作：{} {}（{}）".format(
                            item.get("task_code"),
                            item.get("action"),
                            item.get("outcome"),
                        )
                    )
            progress = last.get("progress")
            if isinstance(progress, dict):
                for code, value in progress.items():
                    if isinstance(value, dict):
                        parts.append(
                            "进度：{} {}/{}".format(
                                code,
                                value.get("current", 0),
                                value.get("target", 0),
                            )
                        )
            for reward in last.get("rewards") or []:
                if isinstance(reward, dict):
                    credit = reward.get("credit")
                    energy = reward.get("energy")
                    if credit is None and energy is None:
                        detail = "已确认领取，数值未知"
                    else:
                        detail = "credit={} energy={}".format(credit, energy)
                    parts.append(
                        "奖励：{} {}".format(reward.get("task_code"), detail)
                    )
            for item in last.get("pending") or []:
                if isinstance(item, dict):
                    parts.append(
                        "待处理：{}（{}）".format(
                            item.get("task_code"), item.get("reason")
                        )
                    )
            unconfirmed = last.get("unconfirmed") or []
            if unconfirmed:
                parts.append("未确认：" + "、".join(str(c) for c in unconfirmed))
        return "\n".join(parts)

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
