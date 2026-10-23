"""Proactive admin-QQ notifications for the WorkBuddy plugin.

Scope: a single small :class:`AdminNotifier` that pushes two kinds of proactive
private messages to one configured administrator QQ number through a *real*
AstrBot ``aiocqhttp`` adapter:

* a daily "signin finished" summary (at most once per ``result.date``), and
* a categorised failure alert with a persisted per-category cooldown.

Design boundaries (pinned by the task's behavioural tests):

* Routing uses only ``context.platform_manager.platform_insts`` and each
  instance's ``.meta().name`` / ``.id``.  Only a real ``aiocqhttp`` instance is
  eligible; an unset ``platform_id`` auto-selects solely when exactly one QQ
  instance exists -- otherwise nothing is guessed.
* Delivery builds a real ``MessageSession`` / ``MessageChain`` and goes through
  ``await context.send_message``.  The return value must be exactly ``True`` for
  a delivery to count as ``sent``; ``False`` / ``None`` / an exception is
  ``failed`` and the original exception text is never surfaced.
* ``signin`` only fires for a ``verified`` round whose outcome is ``signed`` /
  ``already_signed``, is de-duplicated per day across rebuilds (persisted in the
  ``notifications`` section of the real ``StateStore``) and is safe under
  concurrency (an ``asyncio.Lock``).
* ``error`` accepts only the documented categories, sends a fixed safe Chinese
  line (never the upstream body / secret) and cools each category down using
  that category's *own* last timestamp only, even when the delivery itself
  failed (anti-spam).
* The last delivery record (``sent`` / ``failed`` + a fixed reason) is
  persisted to the ``notifications`` section of the real ``StateStore`` so it
  survives a rebuild, and every merged write only touches its own key.
* A failure of this module's *own* balance read never recurses into an error
  notification and never hides the successful signin result.
"""

from __future__ import annotations

import asyncio
import time

from astrbot.api.event import MessageChain
from astrbot.core.message.components import Plain
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.platform.message_type import MessageType

from .credits import is_finite_number

__all__ = ["AdminNotifier"]

_AIOCQHTTP = "aiocqhttp"

_UNKNOWN = "未知"

# Fixed, non-sensitive delivery reasons (never the raw response / exception).
_REASON_SEND_FAILED = "通知发送失败"
_REASON_NOT_CONFIRMED = "通知未确认送达"

# Fixed routing-failure reasons (never echo a raw id / metadata / exception).
_REASON_NO_ADMIN_QQ = "未配置接收管理员QQ"
_REASON_MULTIPLE_QQ = "有多个QQ机器人，请指定实例ID"
_REASON_NO_INSTANCE = "未找到可用的QQ机器人实例"

# The documented error categories mapped to a fixed, safe Chinese notice.  A
# category that is not listed here is ignored outright and is never echoed.
_CATEGORY_TEXTS = {
    "auth": "WorkBuddy 通知：登录授权失败，请重新登录。",
    "permission": "WorkBuddy 通知：账号权限不足，请检查授权。",
    "rate": "WorkBuddy 通知：请求过于频繁，已触发限流。",
    "network": "WorkBuddy 通知：网络连接异常。",
    "upstream": "WorkBuddy 通知：上游服务异常。",
    "signin": "WorkBuddy 通知：签到任务异常。",
    "growth": "WorkBuddy 通知：成长任务异常。",
    "runtime": "WorkBuddy 通知：插件运行时异常。",
}

# Fixed request sources mapped to a fixed Chinese label.  A source outside this
# table is ignored outright, so no free-form string is ever echoed.
_SOURCE_TEXTS = {
    "chat": "聊天",
    "models": "模型列表",
    "balance": "积分查询",
    "signin": "签到",
    "growth": "成长任务",
    "api": "接口请求",
}

# Fixed stop-reason labels for one finished ordered round (never an upstream
# message, business code or exception text).
_ROUTE_REASON_TEXTS = {
    "rate_limited": "上游限流",
    "upstream": "上游返回错误",
    "stream": "上游流中断",
    "timeout": "首帧超时",
    "cooling": "路由冷却中",
    "error": "未知失败",
    "local_limit": "本地请求预算限制",
}


def _fixed_seconds(value) -> str:
    """A fixed ``N 秒`` duration label for a finite positive value, else ``""``."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return ""
    if not is_finite_number(value) or value <= 0:
        return ""
    if value < 1:
        return "不到 1 秒"
    return "{} 秒".format(int(round(float(value))))


def _field(obj, name, default=None):
    """Read ``name`` from a mapping-like or attribute-like result object."""
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _format_amount(value) -> str:
    """Render a credit amount as a plain number (no trailing ``.0``)."""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


class AdminNotifier:
    """Deliver proactive signin / error notices to one admin QQ.

    ``settings`` is the already-normalized ``load_settings()['notifications']``
    mapping.  ``clock`` is an injectable zero-argument callable (defaults to
    ``time.time``) used for the persisted per-category cooldown.  ``client`` is
    the balance boundary: only its ``balance()`` coroutine is ever touched.
    """

    def __init__(self, context, state, settings, client, clock=None) -> None:
        self._context = context
        self._state = state
        self._client = client
        self._settings = settings if isinstance(settings, dict) else {}
        self._clock = time.time if clock is None else clock
        self._lock = asyncio.Lock()

        self._enabled = self._settings.get("enabled", False) is True
        self._signin_success = self._settings.get("signin_success", True) is True
        self._errors = self._settings.get("errors", True) is True

        admin_qq = self._settings.get("admin_qq", "")
        self._admin_qq = admin_qq if isinstance(admin_qq, str) else ""
        platform_id = self._settings.get("platform_id", "")
        self._platform_id = platform_id if isinstance(platform_id, str) else ""

        cooldown = self._settings.get("error_cooldown_seconds", 1800)
        if isinstance(cooldown, bool) or not isinstance(cooldown, int):
            cooldown = 1800
        self._cooldown = cooldown

        self._resolved_platform_id = None
        self._last_delivery = self._load_last_delivery()

    # ------------------------------------------------------------------ #
    # public status / API
    # ------------------------------------------------------------------ #
    def status(self) -> dict:
        """Return the read-only runtime view (never a raw response / token)."""
        return {
            "enabled": self._enabled,
            "signin_success": self._signin_success,
            "errors": self._errors,
            "admin_qq": self._admin_qq,
            "platform_id": self._platform_id,
            "resolved_platform_id": self._route()[0],
            "last_delivery": (
                dict(self._last_delivery) if self._last_delivery is not None else None
            ),
        }

    async def signin(self, result) -> None:
        """Notify the admin about one finished, verified signin round."""
        if not (self._enabled and self._signin_success):
            return
        if _field(result, "verified") is not True:
            return
        if _field(result, "outcome") not in ("signed", "already_signed"):
            return

        date = _field(result, "date")
        date_key = date if isinstance(date, str) and date else None
        reward_value = _field(result, "reward_credits")

        async with self._lock:
            resolved, reason = self._route()
            if resolved is None:
                # Not routable: never send, never read the balance, but record
                # a fixed, non-sensitive failure so the gap stays observable.
                self._record_unroutable("signin", reason)
                return

            section = self._load_section()
            dates = section.get("signin_dates")
            dates = list(dates) if isinstance(dates, list) else []
            if date_key is not None and date_key in dates:
                return

            reward = self._format_reward(reward_value)
            remaining = await self._balance_text()
            text = (
                "WorkBuddy 签到通知：\n"
                "本次领取积分：{}\n"
                "当前可用总积分：{}"
            ).format(reward, remaining)

            self._resolved_platform_id = resolved
            await self._deliver(self._build_chain(text), "signin")

            # Record the attempt *including* a failed delivery so a broken
            # route never re-notifies the same day.  Re-load the section after
            # the await so this merged write never clobbers the ``last_delivery``
            # that ``_deliver`` just persisted; only our own key is changed.
            section = self._load_section()
            dates = section.get("signin_dates")
            dates = list(dates) if isinstance(dates, list) else []
            if date_key is not None and date_key not in dates:
                dates.append(date_key)
            section["signin_dates"] = dates
            self._state.update("notifications", section)

    async def error(self, category, *, source=None, routing_summary=None) -> None:
        """Notify the admin about a known failure category (cooldown-gated).

        ``source`` is one documented request key (``chat`` / ``models`` /
        ``balance`` / ``signin`` / ``growth`` / ``api``) and only adds a fixed
        Chinese "来源" line; ``routing_summary`` carries the safe fields of a
        finished ordered round (attempt count, fixed reason, cooldown hint) and
        only adds a fixed "路由" line.  With both omitted the message is exactly
        the historic one, and the switch / cooldown / delivery logic is unchanged.
        """
        if category not in _CATEGORY_TEXTS:
            return
        if not (self._enabled and self._errors):
            return

        async with self._lock:
            resolved, reason = self._route()
            if resolved is None:
                self._record_unroutable(category, reason)
                return

            now = self._now()
            section = self._load_section()
            cooldowns = section.get("error_cooldowns")
            cooldowns = dict(cooldowns) if isinstance(cooldowns, dict) else {}
            if not self._cooldown_allows(cooldowns, category, now):
                return

            # Persist the cooldown *before* the attempt so even a failed
            # delivery still suppresses a repeat of the same category.  Re-load
            # fresh and only touch our own key so a concurrent last_delivery /
            # signin_dates write is never overwritten.
            section = self._load_section()
            cooldowns = section.get("error_cooldowns")
            cooldowns = dict(cooldowns) if isinstance(cooldowns, dict) else {}
            cooldowns[category] = now
            section["error_cooldowns"] = cooldowns
            self._state.update("notifications", section)

            self._resolved_platform_id = resolved
            await self._deliver(
                self._build_chain(
                    self._error_text(
                        category, source=source, routing_summary=routing_summary
                    )
                ),
                category,
            )

    def _error_text(self, category, *, source=None, routing_summary=None) -> str:
        """The fixed safe notice text; the optional lines are additive only."""
        text = _CATEGORY_TEXTS[category]
        lines = []
        label = _SOURCE_TEXTS.get(source) if isinstance(source, str) else None
        if label:
            lines.append("来源：" + label)
        route = self._route_text(routing_summary)
        if route:
            lines.append(route)
        if not lines:
            return text
        return text + "\n" + "\n".join(lines)

    @staticmethod
    def _route_text(routing_summary) -> str:
        """A fixed one-line route result; only safe fields are rendered."""
        if not isinstance(routing_summary, dict):
            return ""
        parts = []
        attempts = routing_summary.get("attempts")
        if isinstance(attempts, int) and not isinstance(attempts, bool) and attempts > 0:
            parts.append("已尝试 {} 个模型".format(attempts))
        else:
            parts.append("已尝试模型数未知")
        reason = routing_summary.get("reason")
        label = _ROUTE_REASON_TEXTS.get(reason) if isinstance(reason, str) else None
        if label:
            parts.append("停止原因：" + label)
        cooldown = _fixed_seconds(routing_summary.get("cooldown_remaining"))
        if cooldown:
            parts.append("整体冷却剩余 " + cooldown)
        else:
            retry_after = _fixed_seconds(routing_summary.get("retry_after"))
            if retry_after:
                parts.append("上游建议等待 " + retry_after)
        return "路由：" + "，".join(parts)

    # ------------------------------------------------------------------ #
    # routing
    # ------------------------------------------------------------------ #
    def _candidates(self):
        """Return the eligible ``aiocqhttp`` instance ids (read-only)."""
        manager = getattr(self._context, "platform_manager", None)
        insts = getattr(manager, "platform_insts", None)
        if not isinstance(insts, list):
            return []

        candidates = []
        for inst in insts:
            try:
                meta = inst.meta()
            except Exception:
                continue
            if getattr(meta, "name", None) != _AIOCQHTTP:
                continue
            instance_id = getattr(meta, "id", None)
            if isinstance(instance_id, str) and instance_id:
                candidates.append(instance_id)
        return candidates

    def _resolve_platform(self):
        """Return the single eligible ``aiocqhttp`` instance id, or ``None``."""
        candidates = self._candidates()
        if self._platform_id:
            return self._platform_id if self._platform_id in candidates else None
        if len(candidates) == 1:
            return candidates[0]
        return None

    def _route(self):
        """Return ``(resolved_id, failure_reason)`` for the configured admin.

        ``resolved_id`` is ``None`` when nothing may be delivered; the fixed,
        non-sensitive ``failure_reason`` explains why (never a raw id / error /
        metadata).  This never sends and never reads the balance.
        """
        if not self._admin_qq:
            return None, _REASON_NO_ADMIN_QQ
        candidates = self._candidates()
        if self._platform_id:
            if self._platform_id in candidates:
                return self._platform_id, None
            return None, _REASON_NO_INSTANCE
        if len(candidates) == 1:
            return candidates[0], None
        if len(candidates) > 1:
            return None, _REASON_MULTIPLE_QQ
        return None, _REASON_NO_INSTANCE

    # ------------------------------------------------------------------ #
    # delivery
    # ------------------------------------------------------------------ #
    async def _deliver(self, chain, category) -> bool:
        at = self._now()
        session = MessageSession(
            platform_name=self._resolved_platform_id,
            message_type=MessageType.FRIEND_MESSAGE,
            session_id=self._admin_qq,
        )
        try:
            result = await self._context.send_message(session, chain)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._record_delivery(
                {
                    "outcome": "failed",
                    "category": category,
                    "at": at,
                    "reason": _REASON_SEND_FAILED,
                }
            )
            return False

        if result is True:
            self._record_delivery(
                {"outcome": "sent", "category": category, "at": at}
            )
            return True

        self._record_delivery(
            {
                "outcome": "failed",
                "category": category,
                "at": at,
                "reason": _REASON_NOT_CONFIRMED,
            }
        )
        return False

    def _record_delivery(self, record: dict) -> None:
        """Cache and persist a fixed, non-sensitive delivery record.

        Persisting re-loads the section so only ``last_delivery`` is written;
        the caller's ``signin_dates`` / ``error_cooldowns`` keys are preserved.
        """
        self._last_delivery = record
        section = self._load_section()
        section["last_delivery"] = dict(record)
        self._state.update("notifications", section)

    def _record_unroutable(self, category, reason) -> None:
        """Record a fixed routing failure (no send, no balance read)."""
        self._record_delivery(
            {
                "outcome": "failed",
                "category": category,
                "at": self._now(),
                "reason": reason,
            }
        )

    def _build_chain(self, text: str):
        chain = MessageChain()
        message = getattr(chain, "message", None)
        if callable(message):
            produced = message(text)
            return produced if produced is not None else chain
        return MessageChain([Plain(text)])

    # ------------------------------------------------------------------ #
    # balance (this module's own read; never recurses into ``error``)
    # ------------------------------------------------------------------ #
    async def _balance_text(self) -> str:
        try:
            summary = await self._client.balance()
        except asyncio.CancelledError:
            raise
        except Exception:
            return _UNKNOWN
        return self._format_remaining(summary)

    def _format_remaining(self, summary) -> str:
        """Return the finite non-negative ``remaining`` or ``未知``.

        Only a known balance whose ``remaining`` is a finite, non-negative
        number is trusted; the package ``total`` is never used as available.
        """
        if not isinstance(summary, dict):
            return _UNKNOWN
        if _field(summary, "known") is not True:
            return _UNKNOWN
        remaining = _field(summary, "remaining")
        if not is_finite_number(remaining) or remaining < 0:
            return _UNKNOWN
        return _format_amount(remaining)

    @staticmethod
    def _format_reward(value) -> str:
        """Return a finite non-negative reward (``0`` kept) or ``未知``."""
        if not is_finite_number(value) or value < 0:
            return _UNKNOWN
        return _format_amount(value)

    # ------------------------------------------------------------------ #
    # persisted cooldown / dedup helpers
    # ------------------------------------------------------------------ #
    def _cooldown_allows(self, cooldowns, category, now) -> bool:
        """Whether ``category`` may fire now.

        A category that has never been notified may always fire once.  A
        notified category is suppressed for ``cooldown`` seconds counted from
        that category's *own* last notification only -- a more recent notice of
        a different category never extends this one's cooldown.
        """
        value = cooldowns.get(category)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return True
        return (now - value) >= self._cooldown

    def _load_section(self) -> dict:
        state = self._state.load()
        section = state.get("notifications") if isinstance(state, dict) else None
        return dict(section) if isinstance(section, dict) else {}

    def _load_last_delivery(self):
        record = self._load_section().get("last_delivery")
        return dict(record) if isinstance(record, dict) else None

    def _now(self):
        return self._clock()
