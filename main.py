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
from .core.gateway import Gateway
from .core.growth import GrowthService
from .core.scheduler import SigninScheduler
from .core.signin import SigninService
from .core.storage import CredentialStore, StateStore

PLUGIN_NAME = "astrbot_plugin_workbuddy"

# Fixed domestic upstream boundaries.  No raw URL is user-configurable; the
# tests patch these three module-level constants at the network edge.
CHAT_BASE = "https://copilot.tencent.com"
BILLING_BASE = "https://www.codebuddy.cn"
WEB_BASE = "https://www.workbuddy.cn"

HELP_TEXT = """WorkBuddy 指令（仅管理员私聊）：
/wb help                 显示本帮助
/wb status               查看插件与账号状态
/wb login                获取登录链接
/wb logout               退出登录
/wb models               列出可用模型
/wb balance              查看账户余额
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
    "WorkBuddy API、每日签到与成长任务",
    "0.1.0",
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
        self.scheduler = None
        self.growth = None

        self._login_task = None
        self._notify_tasks = set()
        self._notify_umo = None

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

        http = settings["http"]
        timeout = aiohttp.ClientTimeout(
            total=http["total_timeout"],
            connect=http["connect_timeout"],
            sock_read=http["sock_read_timeout"],
        )
        try:
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
            )
        except Exception:
            await self._release_runtime()
            self._fail("插件初始化失败，请查看日志后重载。")
            return

        self.ready = True
        self.error_reason = ""

        # Only an existing, decrypted account may start the automatic loop.
        if settings["signin"]["enabled"] and self.auth.credentials:
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

    async def _build_gateway(self) -> None:
        s = self._settings["gateway"]
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
        )
        if s["enabled"] and s["api_key"]:
            try:
                await self.gateway.start()
            except Exception:
                # A failed bind leaves the gateway unlistening; the plugin stays
                # usable through commands and never fabricates a listener.
                self.gateway.bound_port = None

    async def _release_runtime(self) -> None:
        self.ready = False

        login_task = self._login_task
        self._login_task = None
        if login_task is not None and not login_task.done():
            login_task.cancel()
            with contextlib.suppress(BaseException):
                await login_task

        notify_tasks = [t for t in self._notify_tasks if not t.done()]
        self._notify_tasks.clear()
        for task in notify_tasks:
            task.cancel()
        if notify_tasks:
            await asyncio.gather(*notify_tasks, return_exceptions=True)

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

        gateway = self.gateway
        self.gateway = None
        if gateway is not None:
            with contextlib.suppress(Exception):
                await gateway.stop()

        session = self.session
        self.session = None
        if session is not None:
            with contextlib.suppress(Exception):
                await session.close()

    def _fail(self, reason: str) -> None:
        self.ready = False
        self.error_reason = reason

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
            yield event.plain_result(self._status_text())
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
        # paused scheduler / growth service.  Cancel the in-flight growth run,
        # rebuild the shared gateway (an old, closed gate is unusable), then lift
        # the 401/403 pause in both services.  A server-forced 429 cooldown (if
        # any) is always preserved by both resume() calls.
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
        await self._notify("登录成功，服务已恢复。")

    async def _logout(self) -> str:
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

    def _status_text(self) -> str:
        lines = ["WorkBuddy 插件状态：", "就绪：是"]
        creds = self.auth.credentials if self.auth is not None else {}
        lines.append("登录：{}".format("已登录" if creds.get("accessToken") else "未登录"))
        if self.gateway is not None and getattr(self.gateway, "bound_port", None):
            lines.append("网关：监听中（端口 {}）".format(self.gateway.bound_port))
        else:
            lines.append("网关：未监听")
        if self.scheduler is not None:
            st = self.scheduler.status()
            lines.append(
                "签到：{}，今日尝试 {}，上次成功 {}".format(
                    "已暂停" if st["paused"] else "正常",
                    st["attempts_today"],
                    st.get("last_success_date") or "无",
                )
            )
            if st.get("last_outcome"):
                lines.append("签到最近结果：" + str(st["last_outcome"]))
        if self.growth is not None:
            gs = self.growth.status()
            lines.append(
                "成长：{}，今日聊天 {}/{}".format(
                    "运行中" if gs["running"] else "空闲",
                    gs["chat_requests_today"],
                    gs["max_chat_requests"],
                )
            )
        return "\n".join(lines)

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
            ids = await self.client.models()
        except Exception:
            return "获取模型列表失败，请稍后重试。"
        if not ids:
            return "当前没有可用模型。"
        return "可用模型：\n" + "\n".join("- " + str(m) for m in ids)

    async def _balance_text(self) -> str:
        creds = self.auth.credentials if self.auth is not None else {}
        if not creds.get("accessToken"):
            return "尚未登录，无法查询余额。"
        try:
            data = await self.client.balance()
        except Exception:
            return "查询余额失败，请稍后重试。"
        parts = ["账户资源："]
        for key in ("credit", "credits", "energy", "balance", "remain", "remaining"):
            value = data.get(key) if isinstance(data, dict) else None
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                parts.append("{}：{}".format(key, value))
        if len(parts) == 1:
            parts.append("暂无可展示的余额字段。")
        return "\n".join(parts)

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
