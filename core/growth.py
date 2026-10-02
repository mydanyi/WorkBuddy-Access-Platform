"""Server-side growth task runner (成长任务服务).

管理员一键执行「当前服务端动态可执行」的成长任务：有限重试、服务端计分回读、
条件领奖、持久恢复 / 预算、可取消。

设计红线（与 ``tests/test_growth.py`` / ``tests/test_growth_regressions.py`` 对齐）：

* **动作 / 进度 / 积分分离**：HTTP 200 / ``code:0`` 只能证明动作 attempt，
  绝不等于计分到账。进度靠重新 ``GET`` 任务列表回读，领奖靠回读 ``claimed``。
* 只做白名单内、有真实动作链的任务；其余报 ``unsupported`` / ``manual``。
* 每次真实 chat 尝试（失败 / 取消也算）先消耗持久预算，重启不退还；预算按
  Asia/Shanghai 日期，同日多轮共享。
* 普通 API chat 与成长 chat 共用传入 ``ChatService.complete`` 的限流门，
  不新建 chat、不调用无限制 stream。
* 轮询用 ``monotonic`` deadline，不依赖注入的 ``FakeClock`` 前进；
  ``TaskActions`` 首版白名单见 ``core.growth_tasks``。
* 402/403 -> 暂停本轮；429 / ``LimitExceeded`` -> 服务端冷却（``cooldown_until``）。
  ``resume()`` 只解除暂停，绝不抹掉服务端冷却；日界 / 重启都不能绕过冷却。
* 401/403/429 出现在 list / models / accept / report / claim / 回读 的任意一环，
  都按同一策略停下本轮，绝不降级成 ``model_unavailable`` / 成功。

协议端点（chat / billing 域）::

    GET  {chat_base}/v2/activity/growth/tasks                 -> data.tasks[]
    POST {chat_base}/v2/activity/growth/tasks/accept          {"task_codes":[code]}
    POST {web_base}/activity/growth/tasks/{code}/claim        {}  x-client-platform: web
    POST {billing_base}/v2/report                             [ {eventCode, ...} ]
    POST {chat_base}/v2/report                                [ desktop fingerprint ]

来源：``references/workbuddy2api-panel/internal/upstream/desktop.go``（130-210 行
桌面端成功对话链）与 ``internal/panel/autotask.go``（986-1020 行 appearance /
template 动作）的实测形状；本实现只保留真实关键链，不照抄虚构的 CPU / OS /
释放日期 / commit 等字段。
"""

from __future__ import annotations

import asyncio
import hashlib
import secrets
from datetime import datetime
from urllib.parse import quote
from zoneinfo import ZoneInfo

import aiohttp

from .errors import ChatStreamError, UpstreamError
from .growth_tasks import TaskActions
from .limits import LimitExceeded

GROWTH_LIST_PATH = "/v2/activity/growth/tasks"
ACCEPT_PATH = "/v2/activity/growth/tasks/accept"
REPORT_PATH = "/v2/report"
CLAIM_PATH_TMPL = "/activity/growth/tasks/{code}/claim"
APPEARANCE_SET_PATH = "/v2/user-asset/appearance/set"
BUDDY_AGREEMENT_PATH = "/activity/growth/buddy/agreement"
BUDDY_FIRST_PATH = "/activity/growth/buddy/first"

# 真实、简短的问候 prompt（真实 prompt 长度用于上报 inputLength）。
PROMPT = "你好，请只回复一句简短问候。"
FAST_MODEL = "fast-model"
GLM_MODEL = "glm-5.2"
# ChatService 对缺失 id 的合成占位符：绝不把它当作真实服务端 id 上报。
_SYNTHETIC_ID = "chatcmpl-wb"

_STATE_SECTION = "growth"
_TZ = ZoneInfo("Asia/Shanghai")

_NETWORK_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError)

# 默认冷却（429 / LimitExceeded 未带可用 Retry-After 时）。
_DEFAULT_COOLDOWN = 60.0


def _as_int(value) -> int:
    """仅接受真正的整数；布尔不当整数，其余一律 0。"""
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


def _parse_progress(item: dict) -> tuple[int, int]:
    """``progress`` 可为 ``{current,target}`` 对象或顶层扁平字段。"""
    progress = item.get("progress")
    if isinstance(progress, dict):
        return _as_int(progress.get("current")), _as_int(progress.get("target"))
    return _as_int(item.get("current")), _as_int(item.get("target"))


def _to_seconds(value) -> float:
    """把秒或毫秒时间戳统一成秒。"""
    number = float(value)
    return number / 1000.0 if number > 1e12 else number


class GrowthService:
    """One account's growth runner (single-flight, resumable, cancellable)."""

    def __init__(
        self,
        client,
        chat,
        state,
        *,
        task_interval=2,
        poll_interval=5,
        progress_timeout=120,
        max_retries=2,
        max_chat_requests=20,
        auto_claim=True,
        clock=None,
        allowed_tasks=None,
        allow_telemetry=True,
        allow_theme_change=False,
        allow_adoption=False,
    ):
        self._client = client
        self._chat = chat
        self._state = state
        self._task_interval = max(0.0, float(task_interval))
        self._poll_interval = max(0.0, float(poll_interval))
        self._progress_timeout = max(0.0, float(progress_timeout))
        self._max_retries = max(0, int(max_retries))
        self._max_chat_requests = max(0, int(max_chat_requests))
        self._auto_claim = bool(auto_claim)
        self._clock = clock or (lambda: datetime.now(_TZ).timestamp())
        # 空序列（``[]``）与 ``None`` 同义：都表示「所有基础白名单 + 可领取完成态」。
        self._allowed_tasks = set(allowed_tasks) if allowed_tasks else None
        self._allow_telemetry = bool(allow_telemetry)
        self._allow_theme_change = bool(allow_theme_change)
        self._allow_adoption = bool(allow_adoption)

        self._busy = False
        self._cancelled = False
        self._inflight = None
        self._task = None

    # ------------------------------------------------------------------ #
    # 公开接口
    # ------------------------------------------------------------------ #

    async def list_tasks(self) -> dict:
        """只读列出当前服务端任务（供 ``/wb tasks``），不发任何动作。

        list 失败时把安全的 :class:`UpstreamError` 抛给调用方（main）处理，
        绝不吞掉 401/403/429。
        """
        tasks = await self._list()
        return {
            "tasks": [
                {
                    "task_code": t["task_code"],
                    "title": t["title"],
                    "status": t["status"],
                    "accept_status": t["accept"],
                    "locked": t["locked"],
                    "expired": t["expired"],
                    "complete": self._is_complete(t),
                    "progress": {"current": t["current"], "target": t["target"]},
                }
                for t in tasks
            ]
        }

    async def run(self, claim_only=False) -> dict:
        """前台执行一轮；同账号已有 run 在途则立即返回 ``busy``。"""
        if self._busy:
            return self._busy_result(claim_only)
        self._busy = True
        self._cancelled = False
        self._inflight = asyncio.current_task()
        try:
            return await self._run(bool(claim_only))
        except asyncio.CancelledError:
            self._mark_cancelled()
            raise
        finally:
            self._busy = False
            self._inflight = None

    def start(self, claim_only=False) -> dict:
        """后台启动一轮。返回 ``{started, busy}``；先占位再建任务，避免自拒。"""
        if self._busy:
            return {"started": False, "busy": True}
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return {"started": False, "busy": False}
        self._busy = True
        self._cancelled = False
        self._task = loop.create_task(
            self._run_background(bool(claim_only)), name="wb-growth-run"
        )
        return {"started": True, "busy": False}

    async def cancel(self) -> dict:
        """取消（并 await）在途 run / 后台任务，保留当日已消耗预算。"""
        candidates = []
        for candidate in (self._inflight, self._task):
            if candidate is not None and not candidate.done() and candidate not in candidates:
                candidates.append(candidate)
        for candidate in candidates:
            candidate.cancel()
        if candidates:
            await asyncio.gather(*candidates, return_exceptions=True)
        self._busy = False
        self._inflight = None
        self._mark_cancelled()
        return {"cancelled": True, "running": False}

    def resume(self) -> None:
        """只解除 401/403/429 造成的暂停；**不**抹掉服务端 429 冷却。

        冷却（``cooldown_until``）是服务端强制的：``resume`` 之后若冷却仍在，
        ``run`` 依旧不会发任何动作，日界 / 重启也一样生效。
        """
        state = self._ensure_day()
        state["paused"] = False
        self._save_state(state)

    def status(self) -> dict:
        state = self._ensure_day()
        today = self._local_date()
        used = int(state.get("chat_requests", 0)) if state.get("date") == today else 0
        pending = state.get("pending")
        cooldown_until = state.get("cooldown_until")
        retry_after = None
        if isinstance(cooldown_until, (int, float)) and not isinstance(cooldown_until, bool):
            remaining = float(cooldown_until) - float(self._clock())
            if remaining > 0:
                retry_after = remaining
        return {
            "running": bool(self._busy),
            "paused": bool(state.get("paused", False)),
            "cancelled": bool(self._cancelled),
            "interrupted": bool(state.get("interrupted", False)),
            "date": today,
            "chat_requests_today": used,
            "max_chat_requests": self._max_chat_requests,
            "cooldown_until": cooldown_until,
            "retry_after": retry_after,
            "pending": list(pending) if isinstance(pending, list) else [],
            "last_result": state.get("last_result"),
        }

    # ------------------------------------------------------------------ #
    # run 主体
    # ------------------------------------------------------------------ #

    async def _run_background(self, claim_only):
        self._inflight = asyncio.current_task()
        try:
            return await self._run(claim_only)
        except asyncio.CancelledError:
            self._mark_cancelled()
            raise
        finally:
            self._busy = False
            self._inflight = None

    async def _run(self, claim_only: bool) -> dict:
        self._begin_run()
        result = self._empty_result(claim_only)
        try:
            try:
                tasks = await self._list()
                result["listed"] = len(tasks)
                for task in tasks:
                    result["progress"][task["task_code"]] = {
                        "current": task["current"],
                        "target": task["target"],
                    }

                models = {"value": None}
                for index, task in enumerate(tasks):
                    if self._cancelled or self._blocked():
                        break
                    if index:
                        await self._sleep_interval()
                    await self._process(task, result, claim_only, models)
                    # ``_process`` 会就地回读刷新 ``task.current/target``；每个任务
                    # 结束后用最新已知值更新 result.progress。错误 / 取消时保留最近值，
                    # 绝不假造尚未确认的到账进度。
                    result["progress"][task["task_code"]] = {
                        "current": task["current"],
                        "target": task["target"],
                    }
            except asyncio.CancelledError:
                raise
            except UpstreamError as exc:
                category = self._apply_error_policy(exc)
                result["needs_attention"] = True
                result["status"] = "paused" if category in ("auth", "rate") else "failed"
                result["code"] = category
            except ChatStreamError:
                result["needs_attention"] = True
                result["status"] = "failed"
                result["code"] = "chat_stream_error"
            except LimitExceeded as exc:
                self._install_cooldown(getattr(exc, "retry_after", None))
                result["needs_attention"] = True
                result["status"] = "paused"
                result["code"] = "rate"
            except _NETWORK_ERRORS:
                result["needs_attention"] = True
                result["status"] = "failed"
                result["code"] = "network"
            except Exception:  # noqa: BLE001 - 任何预期外错误都收敛成固定结果
                result["needs_attention"] = True
                result["status"] = "failed"
                result["code"] = "error"
        finally:
            # 暂停或服务端硬冷却（含 report 429 后的冷却）都算需要关注；仅看 paused
            # 会把「resume 后冷却仍在」的轮次误报成无需关注。
            result["needs_attention"] = bool(
                result["needs_attention"] or self._blocked() or result["unconfirmed"]
            )
            self._finish_run(result)
        return result

    async def _sleep_interval(self) -> None:
        if self._task_interval <= 0:
            return
        await asyncio.sleep(self._task_interval)

    @staticmethod
    def _empty_result(claim_only) -> dict:
        return {
            "claim_only": bool(claim_only),
            "listed": 0,
            "actions": [],
            "progress": {},
            "rewards": [],
            "pending": [],
            "unconfirmed": [],
            "needs_attention": False,
            "status": None,
            "code": None,
        }

    def _busy_result(self, claim_only) -> dict:
        return {
            "busy": True,
            "claim_only": bool(claim_only),
            "listed": 0,
            "actions": [],
            "progress": {},
            "rewards": [],
            "pending": [],
            "unconfirmed": [],
            "needs_attention": False,
        }

    # ------------------------------------------------------------------ #
    # 任务分类 / 单任务处理
    # ------------------------------------------------------------------ #

    async def _process(self, task, result, claim_only, models) -> None:
        code = task["task_code"]
        if task["locked"]:
            self._pending(result, code, "locked")
            return
        if task["expired"]:
            self._pending(result, code, "expired")
            return
        if task["claimed"]:
            self._pending(result, code, "claimed")
            return
        if self._allowed_tasks and code not in self._allowed_tasks:
            self._pending(result, code, TaskActions.manual_reason(code))
            return

        # 先判定服务端「已完成」的既有任务：即便不在动作白名单内，也可以领取
        # （普通 run 由 auto_claim 决定；显式 claim_only 是人工授权，不受其约束）。
        complete = self._is_complete(task)
        if claim_only:
            if complete:
                await self._claim(task, result)
            else:
                self._pending(result, code, self._unknown_progress_reason(task))
            return

        action = self._action_for(code)
        if action is None:
            if complete:
                if self._auto_claim:
                    await self._claim(task, result)
                else:
                    self._pending(result, code, "complete")
            else:
                self._pending(result, code, TaskActions.manual_reason(code))
            return

        if complete:
            if self._auto_claim:
                await self._claim(task, result)
            else:
                self._pending(result, code, "complete")
            return

        if action in ("chat", "model_chat", "black_cat"):
            await self._run_chat_action(task, action, result, models)
        elif action == "richmeow":
            if not self._allow_telemetry:
                self._pending(result, code, "telemetry_disabled")
                return
            await self._run_chat_action(task, action, result, models, desktop=True)
        elif action == "library":
            if not self._allow_telemetry:
                self._pending(result, code, "telemetry_disabled")
                return
            await self._library_action(task, result)
        elif action == "appearance":
            await self._appearance_action(task, result)
        elif action == "first_buddy":
            await self._first_buddy_action(task, result, models)

    def _action_for(self, code):
        action = TaskActions.action_for(code)
        if action is None:
            # 实例级开关才纳入的动作（默认 manual）。
            if code == "Hp_Appearance":
                if self._allow_theme_change and self._allow_telemetry:
                    return "appearance"
                return None
            if code == "first_buddy":
                return "first_buddy" if self._allow_adoption else None
            return None
        if TaskActions.requires_telemetry(code) and not self._allow_telemetry:
            return None
        return action

    @staticmethod
    def _unknown_progress_reason(task) -> str:
        target = task.get("target", 0)
        if not isinstance(target, int) or isinstance(target, bool) or target <= 0:
            return "progress_unknown"
        return "incomplete"

    # ------------------------------------------------------------------ #
    # chat 动作（chat_5 / Model_chat_GLM5.2 / black_cat / RichMeow）
    # ------------------------------------------------------------------ #

    async def _run_chat_action(self, task, action, result, models, *, desktop=False) -> None:
        code = task["task_code"]
        if action == "black_cat" and not self._night_window():
            self._pending(result, code, "night_window")
            return

        current = task["current"]
        target = task["target"]
        # 缺进度（target 缺失 / 0）且未完成：绝不按 0>=0 当成成功。
        if target <= 0:
            result["actions"].append(
                {"task_code": code, "action": action, "outcome": "progress_unknown"}
            )
            self._pending(result, code, "progress_unknown")
            return

        model = await self._resolve_model(action, models)
        if model is None:
            self._pending(result, code, "model_unavailable")
            return

        if not self._accepted(task):
            if not await self._ensure_accepted(task):
                result["actions"].append(
                    {"task_code": code, "action": action, "outcome": "accept_unconfirmed"}
                )
                self._pending(result, code, "accept_unconfirmed")
                return

        # accept 的回读可能已更新进度：重新取用，避免用陈旧差额计划。
        current = task["current"]
        target = task["target"]
        if target <= 0:
            result["actions"].append(
                {"task_code": code, "action": action, "outcome": "progress_unknown"}
            )
            self._pending(result, code, "progress_unknown")
            return
        if self._is_complete(task):
            await self._maybe_claim(task, result, task["claimed"])
            result["actions"].append(
                {"task_code": code, "action": action, "outcome": "completed"}
            )
            return

        failures = 0
        first = True
        budget_stop = False

        # 计划步数 = 真实差额；max_retries 只约束「连续失败重试」，不限制正常成功步骤。
        while current < target:
            if self._blocked():
                self._pending(result, code, "paused")
                return
            if not self._has_budget():
                budget_stop = True
                break
            if not first:
                await self._sleep_interval()
            first = False
            self._consume_budget()
            conversation_id = self._new_conversation_id()
            try:
                completion = await self._chat_once(model)
            except LimitExceeded as exc:
                self._install_cooldown(getattr(exc, "retry_after", None))
                result["actions"].append(
                    {"task_code": code, "action": action, "outcome": "rate_limited"}
                )
                self._pending(result, code, "rate_limited")
                return
            except UpstreamError as exc:
                category = self._apply_error_policy(exc)
                if category in ("auth", "rate"):
                    outcome = "auth_paused" if category == "auth" else "rate_limited"
                    result["actions"].append(
                        {"task_code": code, "action": action, "outcome": outcome}
                    )
                    self._pending(result, code, outcome)
                    return
                if exc.status is None or (isinstance(exc.status, int) and exc.status >= 500):
                    # 有限网络 / 5xx：先回读列表，再决定是否有限重试。
                    refreshed_current, refreshed_target, claimed = await self._refresh(
                        task, current, target
                    )
                    if claimed:
                        result["actions"].append(
                            {"task_code": code, "action": action, "outcome": "recovered"}
                        )
                        return
                    if self._is_complete(task):
                        await self._maybe_claim(task, result, False)
                        result["actions"].append(
                            {"task_code": code, "action": action, "outcome": "recovered"}
                        )
                        return
                    current, target = refreshed_current, refreshed_target
                    failures += 1
                    if failures > self._max_retries:
                        result["unconfirmed"].append(code)
                        result["actions"].append(
                            {"task_code": code, "action": action, "outcome": "upstream_error"}
                        )
                        return
                    continue
                # HTTP200 业务非 0 / 业务错误：不盲重试。
                result["actions"].append(
                    {"task_code": code, "action": action, "outcome": "upstream_error"}
                )
                self._pending(result, code, "upstream_error")
                return
            except _NETWORK_ERRORS:
                # 有限网络中断：先回读列表，再决定是否有限重试。
                refreshed_current, refreshed_target, claimed = await self._refresh(
                    task, current, target
                )
                if claimed:
                    result["actions"].append(
                        {"task_code": code, "action": action, "outcome": "recovered"}
                    )
                    return
                if self._is_complete(task):
                    await self._maybe_claim(task, result, False)
                    result["actions"].append(
                        {"task_code": code, "action": action, "outcome": "recovered"}
                    )
                    return
                current, target = refreshed_current, refreshed_target
                failures += 1
                if failures > self._max_retries:
                    result["unconfirmed"].append(code)
                    result["actions"].append(
                        {"task_code": code, "action": action, "outcome": "upstream_error"}
                    )
                    return
                continue
            except ChatStreamError:
                # 已出流内容 / 流损坏：ChatService 不重放，这里也不重发 chat。
                _, _, claimed = await self._refresh(task, current, target)
                if claimed:
                    result["actions"].append(
                        {"task_code": code, "action": action, "outcome": "recovered"}
                    )
                    return
                if self._is_complete(task):
                    await self._maybe_claim(task, result, False)
                    result["actions"].append(
                        {"task_code": code, "action": action, "outcome": "recovered"}
                    )
                    return
                result["actions"].append(
                    {"task_code": code, "action": action, "outcome": "chat_stream_error"}
                )
                self._pending(result, code, "chat_stream_error")
                return

            request_id = completion.get("id")
            real_model = completion.get("model") or model
            if not request_id or request_id == _SYNTHETIC_ID:
                result["actions"].append(
                    {"task_code": code, "action": action, "outcome": "no_response_id"}
                )
                self._pending(result, code, "no_response_id")
                return

            # chat 成功；上报失败绝不重播 chat，只按同一真实 requestId 回读 / 分类。
            try:
                await self._report_chat(completion, request_id, real_model, conversation_id)
                if desktop:
                    await self._report_desktop_sequence(
                        completion, request_id, real_model, conversation_id
                    )
            except UpstreamError as exc:
                category = self._apply_error_policy(exc)
                await self._refresh(task, current, target)
                if self._is_complete(task) and not task["claimed"]:
                    await self._maybe_claim(task, result, False)
                    result["actions"].append(
                        {"task_code": code, "action": action, "outcome": "recovered"}
                    )
                    return
                outcome = (
                    "auth_paused"
                    if category == "auth"
                    else ("rate_limited" if category == "rate" else "report_error")
                )
                result["actions"].append(
                    {"task_code": code, "action": action, "outcome": outcome}
                )
                self._pending(result, code, outcome)
                return
            except _NETWORK_ERRORS:
                await self._refresh(task, current, target)
                if self._is_complete(task) and not task["claimed"]:
                    await self._maybe_claim(task, result, False)
                    result["actions"].append(
                        {"task_code": code, "action": action, "outcome": "recovered"}
                    )
                    return
                result["actions"].append(
                    {"task_code": code, "action": action, "outcome": "report_error"}
                )
                self._pending(result, code, "report_error")
                return

            refreshed_current, refreshed_target, claimed = await self._poll_progress(
                task, current, target
            )
            if claimed:
                result["actions"].append(
                    {"task_code": code, "action": action, "outcome": "completed"}
                )
                return
            if self._is_complete(task):
                await self._maybe_claim(task, result, False)
                result["actions"].append(
                    {"task_code": code, "action": action, "outcome": "completed"}
                )
                return
            if refreshed_current > current:
                # 真实进度前进：继续按差额补下一次，成功步骤不消耗失败重试额度。
                current, target = refreshed_current, refreshed_target
                failures = 0
                continue
            # 成功但回读至 deadline 仍无进度：立即 progress_unconfirmed，不再重发。
            result["unconfirmed"].append(code)
            result["actions"].append(
                {"task_code": code, "action": action, "outcome": "progress_unconfirmed"}
            )
            return

        if self._blocked():
            self._pending(result, code, "paused")
            return
        if budget_stop:
            result["actions"].append(
                {"task_code": code, "action": action, "outcome": "budget_exhausted"}
            )
            self._pending(result, code, "chat_budget")
            return
        if current >= target:
            await self._maybe_claim(task, result, False)
            result["actions"].append(
                {"task_code": code, "action": action, "outcome": "completed"}
            )
            return
        result["unconfirmed"].append(code)
        result["actions"].append(
            {"task_code": code, "action": action, "outcome": "progress_unconfirmed"}
        )

    async def _maybe_claim(self, task, result, already_claimed) -> None:
        if already_claimed:
            return
        # 硬冷却（服务端 429）或暂停时绝不发送 claim：report 429 后即便任务已计分，
        # 也必须等冷却结束，不能立即违规领奖。
        if self._blocked():
            return
        if self._auto_claim:
            await self._claim(task, result)

    async def _resolve_model(self, action, models):
        if models.get("value") is None:
            try:
                models["value"] = await self._client.models()
            except UpstreamError as exc:
                category = self._apply_error_policy(exc)
                if category in ("auth", "rate"):
                    raise
                # 业务层「无可用模型」：不编造模型，明确 model_unavailable。
                models["value"] = []
            except _NETWORK_ERRORS:
                # 真正网络失败：抛到本轮安全边界，固定 error，不降级。
                raise
        listed = models["value"] or []
        if action in ("model_chat", "black_cat"):
            return GLM_MODEL if GLM_MODEL in listed else None
        if FAST_MODEL in listed:
            return FAST_MODEL
        return listed[0] if listed else None

    async def _chat_once(self, model) -> dict:
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": PROMPT}],
            "max_tokens": 128,
        }
        return await self._chat.complete(payload)

    # ------------------------------------------------------------------ #
    # 领奖 / accept
    # ------------------------------------------------------------------ #

    async def _ensure_accepted(self, task) -> bool:
        code = task["task_code"]
        try:
            await self._client.request_json(
                "POST", ACCEPT_PATH, json={"task_codes": [code]}
            )
        except UpstreamError as exc:
            category = self._apply_error_policy(exc)
            if category in ("auth", "rate"):
                raise
            return False
        except _NETWORK_ERRORS:
            return False
        # 只有回读到 accepted / claimed / complete 才算确认；Unknown 绝不吞成成功。
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._progress_timeout
        while True:
            refreshed = await self._fetch_task(code)
            if refreshed is not None:
                task.update(refreshed)
                if self._accepted(task) or self._is_complete(task):
                    return True
            now = loop.time()
            if now >= deadline:
                return self._accepted(task) or self._is_complete(task)
            await asyncio.sleep(min(self._poll_interval, max(0.0, deadline - now)))

    async def _claim(self, task, result) -> None:
        code = task["task_code"]
        # claim 前必须 fresh 回读完成态 / claimed，而不是依赖 old task 的缓存。
        fresh = await self._fetch_task(code)
        if fresh is None:
            # 回读无法确认：绝不发送 claim（避免用陈旧 complete 造成双领奖）。
            result["unconfirmed"].append(code)
            result["actions"].append(
                {"task_code": code, "action": "claim", "outcome": "claim_unconfirmed"}
            )
            return
        task.update(fresh)
        if fresh["claimed"]:
            result["actions"].append(
                {"task_code": code, "action": "claim", "outcome": "already_claimed"}
            )
            return
        if fresh["locked"]:
            self._pending(result, code, "locked")
            return
        if fresh["expired"]:
            self._pending(result, code, "expired")
            return
        if not self._is_complete(task):
            self._pending(result, code, self._unknown_progress_reason(task))
            return

        # 硬冷却 / 暂停：报告固定等待结果，绝不发 POST。
        if self._blocked():
            self._report_claim_blocked(result, code)
            return

        path = CLAIM_PATH_TMPL.format(code=quote(code, safe=""))
        headers = {
            "x-client-platform": "web",
            "Origin": self._client.web_base,
            "Referer": self._client.web_base + "/profile/growth-center",
        }
        attempt = 0
        while True:
            if self._blocked():
                self._report_claim_blocked(result, code)
                return
            attempt += 1
            credit = None
            energy = None
            retryable = False
            try:
                data = await self._client.request_json(
                    "POST", path, base=self._client.web_base, json={}, headers=headers
                )
                if isinstance(data, dict):
                    # 只采信服务端真实返回；缺失时保持 None，绝不虚构。
                    credit = data.get("credit")
                    energy = data.get("energy")
            except UpstreamError as exc:
                category = self._apply_error_policy(exc)
                if category in ("auth", "rate"):
                    outcome = "auth_paused" if category == "auth" else "rate_limited"
                    result["actions"].append(
                        {"task_code": code, "action": "claim", "outcome": outcome}
                    )
                    self._pending(result, code, outcome)
                    return
                status = getattr(exc, "status", None)
                # 只有网络失败 / HTTP>=500 才可能在 max_retries 内重 claim；业务
                # （HTTP200 非 0 code）或其它 4xx 是确定性结果，绝不重领。
                retryable = status is None or (
                    isinstance(status, int) and status >= 500
                )
            except _NETWORK_ERRORS:
                retryable = True

            confirmed, refreshed = await self._poll_claimed(code)
            if confirmed:
                result["rewards"].append(
                    {"task_code": code, "credit": credit, "energy": energy}
                )
                result["actions"].append(
                    {"task_code": code, "action": "claim", "outcome": "verified"}
                )
                return
            if refreshed is None:
                # 回读不确定：停止，不盲重发，避免双领奖。
                result["unconfirmed"].append(code)
                result["actions"].append(
                    {"task_code": code, "action": "claim", "outcome": "claim_unconfirmed"}
                )
                return
            task.update(refreshed)
            # 仅当本次是网络 / 5xx 失败、且 fresh 回读确认任务仍 complete（未
            # claimed / locked / expired）时，才在 max_retries 内有限重 claim。
            if (
                retryable
                and attempt <= self._max_retries
                and self._is_complete(task)
                and not task["locked"]
                and not task["expired"]
            ):
                continue
            result["unconfirmed"].append(code)
            result["actions"].append(
                {"task_code": code, "action": "claim", "outcome": "claim_unconfirmed"}
            )
            return

    def _report_claim_blocked(self, result, code) -> None:
        """在硬冷却 / 暂停时报告固定等待结果，绝不发送 claim POST。"""
        reason = "paused" if self._is_paused() else "cooldown"
        result["actions"].append(
            {"task_code": code, "action": "claim", "outcome": "blocked"}
        )
        self._pending(result, code, reason)

    async def _poll_claimed(self, code):
        """有界轮询等待异步到账；返回 ``(confirmed, refreshed_task|None)``。"""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._progress_timeout
        last = None
        while True:
            refreshed = await self._fetch_task(code)
            if refreshed is not None:
                last = refreshed
                if refreshed["claimed"]:
                    return True, refreshed
            now = loop.time()
            if now >= deadline:
                return False, last
            await asyncio.sleep(min(self._poll_interval, max(0.0, deadline - now)))

    # ------------------------------------------------------------------ #
    # 可选动作：Library_read / Hp_Appearance / first_buddy
    # ------------------------------------------------------------------ #

    async def _library_action(self, task, result) -> None:
        code = task["task_code"]
        creds = self._client.credentials
        uid = creds.get("uid") or ""
        page = "https://www.workbuddy.cn/space/d/o0KWYeynteVv06UnAZqIFm"
        now = int(float(self._clock()) * 1000)
        event = {
            "eventCode": "web_element_click",
            "pageURL": page,
            "elementId": "library_doc_intro_click",
            "elementName": "WorkBuddy资料库介绍",
            "userId": uid,
            "enterpriseId": creds.get("enterpriseId") or "",
            "userNickname": creds.get("nickname") or "",
            "timestamp": now,
            "machineId": self._derive_id(uid, "webmachine"),
        }
        headers = {
            "x-client-platform": "web",
            "Origin": self._client.web_base,
            "Referer": page,
        }
        try:
            await self._client.request_json(
                "POST",
                REPORT_PATH,
                base=self._client.web_base,
                json=[event],
                headers=headers,
            )
        except UpstreamError as exc:
            category = self._apply_error_policy(exc)
            if category in ("auth", "rate"):
                raise
            self._pending(result, code, "report_error")
            return
        except _NETWORK_ERRORS:
            self._pending(result, code, "report_error")
            return
        await self._settle_after_report(task, result, "library")

    async def _appearance_action(self, task, result) -> None:
        code = task["task_code"]
        if not self._allow_telemetry:
            self._pending(result, code, "telemetry_disabled")
            return
        try:
            await self._client.request_json(
                "POST",
                APPEARANCE_SET_PATH,
                base=self._client.chat_base,
                json={"kind": "theme", "resource_key": "theme-tkmw7j"},
                headers=self._desktop_headers(),
            )
            # 来源：autotask.go 986-1020（set API 留痕 + appearance_skin_apply 事件）。
            await self._report_desktop_events(
                [
                    {
                        "eventCode": "appearance_skin_apply",
                        "action": "apply",
                        "source": "settings_close",
                        "id": "theme-tkmw7j",
                        "vipLevel": 0,
                        "series": "",
                        "type": "unknown",
                    }
                ]
            )
        except UpstreamError as exc:
            category = self._apply_error_policy(exc)
            if category in ("auth", "rate"):
                raise
            self._pending(result, code, "upstream_error")
            return
        except _NETWORK_ERRORS:
            self._pending(result, code, "report_error")
            return
        await self._settle_after_report(task, result, "appearance")

    async def _first_buddy_action(self, task, result, models) -> None:
        code = task["task_code"]
        model = await self._resolve_model("chat", models)
        if model is None:
            self._pending(result, code, "model_unavailable")
            return
        if not self._accepted(task):
            if not await self._ensure_accepted(task):
                self._pending(result, code, "accept_unconfirmed")
                return
        if not self._has_budget():
            self._pending(result, code, "chat_budget")
            return
        self._consume_budget()
        conversation_id = self._new_conversation_id()
        try:
            completion = await self._chat_once(model)
        except UpstreamError as exc:
            category = self._apply_error_policy(exc)
            if category in ("auth", "rate"):
                raise
            self._pending(result, code, "chat_failed")
            return
        except (ChatStreamError,) + _NETWORK_ERRORS:
            self._pending(result, code, "chat_failed")
            return
        request_id = completion.get("id")
        if not request_id or request_id == _SYNTHETIC_ID:
            self._pending(result, code, "no_response_id")
            return
        try:
            await self._report_chat(
                completion, request_id, completion.get("model") or model, conversation_id
            )
            await self._client.request_json(
                "POST", BUDDY_AGREEMENT_PATH, base=self._client.chat_base, json={"agree": True}
            )
            await self._client.request_json(
                "POST", BUDDY_FIRST_PATH, base=self._client.chat_base, json={}
            )
        except UpstreamError as exc:
            category = self._apply_error_policy(exc)
            if category in ("auth", "rate"):
                raise
            self._pending(result, code, "upstream_error")
            return
        except _NETWORK_ERRORS:
            self._pending(result, code, "report_error")
            return
        await self._settle_after_report(task, result, "first_buddy")

    async def _settle_after_report(self, task, result, action) -> None:
        """动作 / 上报后有界回读计分；完成且未领奖时条件领奖。"""
        code = task["task_code"]
        _, _, claimed = await self._poll_progress(task, task["current"], task["target"])
        if claimed:
            result["actions"].append(
                {"task_code": code, "action": action, "outcome": "already_claimed"}
            )
            return
        if self._is_complete(task):
            if self._auto_claim:
                await self._claim(task, result)
            else:
                result["actions"].append(
                    {"task_code": code, "action": action, "outcome": "completed"}
                )
            return
        result["unconfirmed"].append(code)
        result["actions"].append(
            {"task_code": code, "action": action, "outcome": "progress_unconfirmed"}
        )

    # ------------------------------------------------------------------ #
    # 上报
    # ------------------------------------------------------------------ #

    async def _report_chat(self, completion, request_id, model, conversation_id) -> None:
        creds = self._client.credentials
        uid = creds.get("uid") or ""
        now = int(float(self._clock()) * 1000)
        event = {
            "eventCode": "chat_request_send",
            "timestamp": now,
            "presentAt": now,
            "reportDelay": 0,
            "mode": "craft",
            "conversationId": conversation_id,
            "requestId": request_id,
            "userId": uid,
            "requestModelId": model,
            "requestModelName": model,
            "rootRequestId": request_id,
            "parentConversationId": conversation_id,
            "agentName": "cli",
            "agentType": "main",
            "inputLength": len(PROMPT),
            "isPlan": False,
            "isAutoExecuteTerminal": False,
            "isAutoModify": False,
            "codebaseEnable": False,
            "maxToken": 128,
            "maxSteps": 0,
            "temperature": 0,
            "maxRetries": 0,
            "mentionContexts": [],
            "knowledgeId": [],
            "knowledgeName": [],
            "codebaseId": "",
            "command": "",
            "expertId": "",
            "recommendId": "",
            "skillId": "",
            "fileUri": "",
            "mentionContextCount": 0,
            "skillCount": 0,
            "totalCount": 0,
            "traceId": request_id,
        }
        await self._client.request_json(
            "POST", REPORT_PATH, base=self._client.billing_base, json=[event]
        )

    def _desktop_headers(self) -> dict:
        creds = self._client.credentials
        uid = creds.get("uid") or ""
        return {
            "User-Agent": "WorkBuddy/5.5.6 WorkBuddy/5.5.6 CLI/2.137.1",
            "X-Product": "SaaS",
            "X-User-Id": uid,
            "X-Domain": self._client.chat_base,
        }

    def _desktop_fingerprint(self) -> dict:
        creds = self._client.credentials
        uid = creds.get("uid") or ""
        nickname = (
            creds.get("nickname")
            or creds.get("userNickname")
            or creds.get("username")
            or ""
        )
        now = int(float(self._clock()) * 1000)
        return {
            "ideName": "WorkBuddy",
            "ideType": "WorkBuddy",
            "ideVersion": "5.5.6",
            "extVersion": "5.5.6",
            "extName": "workbuddy-desktop",
            "product": "SaaS",
            "userId": uid,
            "username": nickname,
            "userNickname": nickname,
            "timezone": "Asia/Shanghai",
            "timestamp": now,
            "presentAt": now,
            "reportDelay": 2000,
            "machineId": self._derive_id(uid, "machine"),
            "sessionId": self._derive_id(uid, "session"),
        }

    async def _report_desktop_events(self, events) -> None:
        fingerprint = self._desktop_fingerprint()
        payload = []
        for event in events:
            merged = dict(fingerprint)
            merged.update(event)
            payload.append(merged)
        await self._client.request_json(
            "POST",
            REPORT_PATH,
            base=self._client.chat_base,
            json=payload,
            headers=self._desktop_headers(),
        )

    async def _report_desktop_sequence(self, completion, request_id, model, conversation_id) -> None:
        """桌面端成功对话六连事件（来源：desktop.go 130-210 DesktopChatSequence）。

        只保留真实关键链：conversationId/rootRequestId/traceId/
        codebuddy.conversation_request_id 全部来自真实 chat；token 只取实际
        completion 的 usage，绝不编造；isSuccessful 仅真实成功后为 true。
        """
        usage = completion.get("usage") if isinstance(completion, dict) else None
        usage = usage if isinstance(usage, dict) else {}
        choices = completion.get("choices") if isinstance(completion, dict) else None
        choices = choices if isinstance(choices, list) else []
        finish = "stop"
        tool_count = 0
        if choices and isinstance(choices[0], dict):
            finish = choices[0].get("finish_reason") or "stop"
            message = choices[0].get("message") or {}
            tool_calls = message.get("tool_calls") if isinstance(message, dict) else None
            if isinstance(tool_calls, list):
                tool_count = len(tool_calls)

        message_id = request_id + "-assistant"
        tokens = {}
        if "prompt_tokens" in usage:
            tokens["inputToken"] = usage["prompt_tokens"]
        if "completion_tokens" in usage:
            tokens["outputToken"] = usage["completion_tokens"]
        if "total_tokens" in usage:
            tokens["totalToken"] = usage["total_tokens"]

        link = {
            "agentName": "cli",
            "agentType": "main",
            "traceId": request_id,
            "rootRequestId": request_id,
            "parentConversationId": conversation_id,
            "codebuddy.session_id": conversation_id,
            "codebuddy.conversation_request_id": request_id,
        }
        events = [
            {
                "eventCode": "agent_task_created",
                "source": "LOCAL",
                "name": "working",
                "task_target": "local",
                "mode": "craft",
                "requestModelId": model,
                "requestModelName": model,
                "conversationId": conversation_id,
                "messageId": message_id,
            },
            {"eventCode": "chat_message_send", "messageId": message_id, **link},
            {
                "eventCode": "chat_request_send",
                "messageId": message_id,
                "inputLength": len(PROMPT),
                "maxToken": 128,
                **link,
            },
            {
                "eventCode": "chat_message_response",
                "messageId": message_id,
                "responseModelId": model,
                "isSuccessful": True,
                "finishReason": finish,
                "conversationId": conversation_id,
                **tokens,
                **link,
            },
            {
                "eventCode": "chat_message_status",
                "messageId": message_id,
                "messageErrorCode": "0",
                **link,
            },
            {
                "eventCode": "chat_request_response",
                "mode": "craft",
                "toolCallCount": tool_count,
                "isSuccessful": True,
                "finishReason": finish,
                **tokens,
                "rootRequestId": request_id,
                "parentConversationId": conversation_id,
            },
        ]
        await self._report_desktop_events(events)

    # ------------------------------------------------------------------ #
    # 列表 / 回读
    # ------------------------------------------------------------------ #

    async def _list(self) -> list:
        data = await self._client.request_json("GET", GROWTH_LIST_PATH)
        raw = data.get("tasks") if isinstance(data, dict) else None
        if not isinstance(raw, list):
            return []
        tasks = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            code = item.get("task_code")
            if not isinstance(code, str) or not code:
                continue
            tasks.append(self._normalize(item, code))
        return tasks

    def _normalize(self, item: dict, code: str) -> dict:
        current, target = _parse_progress(item)
        accept = item.get("accept_status")
        claimed = item.get("claimed") is True or accept == "claimed"
        locked = item.get("locked") is True
        expired = item.get("expired") is True
        if not expired:
            expires_at = item.get("expires_at")
            if isinstance(expires_at, (int, float)) and not isinstance(expires_at, bool):
                if _to_seconds(expires_at) <= float(self._clock()):
                    expired = True
        return {
            "task_code": code,
            "title": item.get("title"),
            "status": item.get("status"),
            "accept": accept,
            "claimed": claimed,
            "locked": locked,
            "expired": expired,
            "current": current,
            "target": target,
        }

    async def _fetch_task(self, code):
        """回读单个任务；不吞 401/403/429（按策略处理后重抛给本轮边界）。"""
        try:
            tasks = await self._list()
        except UpstreamError as exc:
            category = self._apply_error_policy(exc)
            if category in ("auth", "rate"):
                raise
            return None
        except _NETWORK_ERRORS:
            return None
        for task in tasks:
            if task["task_code"] == code:
                return task
        return None

    async def _refresh(self, task, current, target):
        """一次性回读（错误路径：不长时间轮询）。返回 ``(current,target,claimed)``。"""
        refreshed = await self._fetch_task(task["task_code"])
        if refreshed is None:
            return current, target, False
        task.update(refreshed)
        return refreshed["current"], refreshed["target"], refreshed["claimed"]

    async def _poll_progress(self, task, current, target):
        """轮询直至进度前进 / 明确完成（含 status complete）/ claimed / deadline。

        回读会就地更新 ``task``，调用方用 :meth:`_is_complete` 判定完成态。
        返回 ``(current, target, claimed)``。
        """
        code = task["task_code"]
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._progress_timeout
        seen_current, seen_target = current, target
        while True:
            refreshed = await self._fetch_task(code)
            if refreshed is not None:
                task.update(refreshed)
                seen_current, seen_target = refreshed["current"], refreshed["target"]
                if refreshed["claimed"]:
                    return seen_current, seen_target, True
                if self._is_complete(refreshed):
                    return seen_current, seen_target, False
                if seen_current > current:
                    return seen_current, seen_target, False
            now = loop.time()
            if now >= deadline:
                return seen_current, seen_target, False
            await asyncio.sleep(min(self._poll_interval, max(0.0, deadline - now)))

    # ------------------------------------------------------------------ #
    # 持久状态 / 预算
    # ------------------------------------------------------------------ #

    def _load_state(self) -> dict:
        stored = self._state.load().get(_STATE_SECTION)
        return dict(stored) if isinstance(stored, dict) else {}

    def _save_state(self, state: dict) -> None:
        self._state.update(_STATE_SECTION, dict(state))

    def _local_date(self) -> str:
        return datetime.fromtimestamp(float(self._clock()), _TZ).strftime("%Y-%m-%d")

    def _ensure_day(self) -> dict:
        state = self._load_state()
        today = self._local_date()
        if state.get("date") != today:
            state = {
                "date": today,
                "chat_requests": 0,
                "paused": bool(state.get("paused", False)),
                # 服务端 429 冷却是绝对时间戳：跨日界 / 重启都保留，不能绕过。
                "cooldown_until": state.get("cooldown_until"),
                "last_result": state.get("last_result"),
                "pending": state.get("pending") if isinstance(state.get("pending"), list) else [],
                "running": False,
                "interrupted": bool(state.get("running", False)),
            }
            self._save_state(state)
        return state

    def _begin_run(self) -> None:
        state = self._ensure_day()
        # 重启恢复：上次仍是 running -> 视为 interrupted；保留 results/date/预算。
        if state.get("running"):
            state["interrupted"] = True
        state["running"] = True
        self._save_state(state)

    def _finish_run(self, result: dict) -> None:
        state = self._ensure_day()
        state["running"] = False
        state["last_result"] = result
        state["pending"] = result.get("pending", [])
        self._save_state(state)

    def _mark_cancelled(self) -> None:
        self._cancelled = True
        state = self._ensure_day()
        state["running"] = False
        state["interrupted"] = True
        self._save_state(state)

    def _has_budget(self) -> bool:
        state = self._ensure_day()
        if state.get("date") != self._local_date():
            return True
        return int(state.get("chat_requests", 0)) < self._max_chat_requests

    def _consume_budget(self) -> None:
        state = self._ensure_day()
        state["chat_requests"] = int(state.get("chat_requests", 0)) + 1
        self._save_state(state)

    def _is_paused(self) -> bool:
        return bool(self._ensure_day().get("paused", False))

    def _in_cooldown(self) -> bool:
        state = self._ensure_day()
        cooldown_until = state.get("cooldown_until")
        if isinstance(cooldown_until, (int, float)) and not isinstance(cooldown_until, bool):
            return float(self._clock()) < float(cooldown_until)
        return False

    def _blocked(self) -> bool:
        """本轮是否禁止发任何动作：暂停或服务端硬冷却（与 paused 开关无关）。"""
        return self._is_paused() or self._in_cooldown()

    def _pause(self) -> None:
        state = self._ensure_day()
        state["paused"] = True
        self._save_state(state)

    def _install_cooldown(self, retry_after) -> None:
        state = self._ensure_day()
        if (
            isinstance(retry_after, (int, float))
            and not isinstance(retry_after, bool)
            and retry_after > 0
        ):
            delay = float(retry_after)
        else:
            delay = _DEFAULT_COOLDOWN
        state["cooldown_until"] = float(self._clock()) + delay
        state["paused"] = True
        self._save_state(state)

    @staticmethod
    def _error_category(exc) -> str:
        status = getattr(exc, "status", None)
        if status in (401, 403):
            return "auth"
        if status == 429:
            return "rate"
        return "other"

    def _apply_error_policy(self, exc) -> str:
        """统一处理上游错误：401/403 -> 暂停；429/LimitExceeded -> 冷却。"""
        category = self._error_category(exc)
        if category == "auth":
            self._pause()
        elif category == "rate":
            self._install_cooldown(getattr(exc, "retry_after", None))
        return category

    # ------------------------------------------------------------------ #
    # 小工具
    # ------------------------------------------------------------------ #

    @staticmethod
    def _pending(result, code, reason) -> None:
        result["pending"].append({"task_code": code, "reason": reason})

    @staticmethod
    def _is_complete(task) -> bool:
        if task["status"] == "complete":
            return True
        return task["target"] > 0 and task["current"] >= task["target"]

    @staticmethod
    def _accepted(task) -> bool:
        return task["claimed"] or task.get("accept") in ("accepted", "claimed")

    def _night_window(self) -> bool:
        hour = datetime.fromtimestamp(float(self._clock()), _TZ).hour
        return hour >= 23 or hour < 8

    @staticmethod
    def _derive_id(uid, salt) -> str:
        """由 uid + salt 稳定派生 36 位 hex 设备 / 会话标识（来源：desktop.go deriveID）。"""
        if not uid:
            return ""
        return hashlib.sha256(
            (salt + ":" + str(uid)).encode("utf-8")
        ).hexdigest()[:36]

    @staticmethod
    def _new_conversation_id() -> str:
        return "wb-growth-" + secrets.token_hex(8)
