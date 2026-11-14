"""模型积分倍率：只读、可失败、绝不挡聊天。

聊天走 ACP 之后，模型清单来自本机 CLI（``--help`` 里 ``--model`` 那一行），那份东西
里没有价格。想在模型名后面看到倍率，只能另外问平台一次——读这两个接口：

* ``/console/enterprises/personal/models``：企业模型文档，``credits`` 就是倍率；
* ``/v3/config``：IDE 形状，同 id 的倍率补前一份的空缺，还带 ``modelPromotions``。

两个都用 CLI 自己那份 token，跟签到共用同一个 :class:`core.client.UpstreamClient`
（含 401 刷新）。**这条链路只影响显示**：读失败就退化成"没有倍率"，模型名退回裸
ID；绝不让 ``/v1/models`` 变成 5xx，也绝不拦住聊天。所以这条路上的失败不进管理员
通知（``observe=False``）——它只是少了一行字，不是故障。

不落盘、不写死价目表：倍率与促销都来自上游文档，缓存只在内存里活一个 ``ttl``。
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

from .credits import (
    format_credits,
    parse_credits,
    promotion_discount,
    promotion_label,
    promotion_note,
    select_promotion,
)

__all__ = [
    "ENTERPRISE_MODELS_PATH",
    "V3_CONFIG_PATH",
    "IDE_HEADERS",
    "build_price_map",
    "ModelPricing",
]

ENTERPRISE_MODELS_PATH = "/console/enterprises/personal/models"
V3_CONFIG_PATH = "/v3/config"

# ``/v3/config`` 只认 IDE 身份；不带这两个头会直接 400。值取自桌面端，不是什么
# 秘密，也不参与鉴权（鉴权仍然是 Bearer）。
IDE_HEADERS = {
    "User-Agent": "CodeBuddyIDE/4.12.0 CodeBuddy/4.12.0",
    "X-Product": "SaaS",
}

# 倍率缓存多久。它决定的是"改价后多久能看到"，不是正确性：显示用晚几分钟无所谓，
# 每来一次 ``/v1/models`` 就去问一次上游才是问题。
DEFAULT_TTL = 300.0

# 读失败后的最短重试间隔，以及连败退避的上限。
DEFAULT_RETRY_AFTER = 30.0
MAX_RETRY_AFTER = 900.0


def _entries(body) -> list:
    """掏出模型数组。

    **信封已经被客户端拆过一层**：``UpstreamClient._parse`` 返回的就是 ``data``
    那一层，所以线上拿到的是 ``{"models": [...]}`` 而不是 ``{"code": 0, "data": ...}``。
    这里两种都吃（裸数组、``{"models": [...]}``、再嵌一层的 ``{"data": {...}}``），
    少猜一层总比线上悄悄空表强。
    """
    if isinstance(body, list):
        return body
    if not isinstance(body, dict):
        return []
    for key in ("models", "data", "list", "items"):
        value = body.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            for inner in ("models", "list", "items"):
                nested = value.get(inner)
                if isinstance(nested, list):
                    return nested
    return []


def build_price_map(enterprise_entries, v3_entries) -> dict:
    """把两份文档合成 ``{model_id: base_multiplier}``，前一份优先。"""
    price = {}
    for items in (enterprise_entries, v3_entries):
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            model_id = item.get("id")
            if not isinstance(model_id, str) or not model_id:
                continue
            if price.get(model_id) is not None:
                continue
            parsed = parse_credits(item.get("credits"))
            if parsed is not None:
                price[model_id] = parsed
            else:
                price.setdefault(model_id, None)
    return price


class ModelPricing:
    """内存里的倍率表；``ensure()`` 自带保鲜期、失败退避与单飞。

    ``client`` 是那个带认证的 :class:`core.client.UpstreamClient`。没有登录态时
    直接返回空表：拿一个注定 401 的请求去问上游，只会把日志刷满。
    """

    def __init__(self, client, *, ttl=None, clock=None):
        self._client = client
        self._ttl = DEFAULT_TTL if ttl is None else float(ttl)
        self._clock = clock if clock is not None else time.time
        self._details = {}
        self._fetched_at = None
        self._next_try = 0.0
        self._failures = 0
        self._lock = asyncio.Lock()

    # -- 读 ---------------------------------------------------------------

    def _has_credentials(self) -> bool:
        try:
            creds = self._client.credentials
        except Exception:
            return False
        return isinstance(creds, dict) and bool(creds.get("accessToken"))

    async def _read(self, path, headers=None):
        return await self._client.request_json(
            "GET", path, headers=headers, observe=False
        )

    async def _fetch(self, now) -> dict:
        enterprise = _entries(await self._read(ENTERPRISE_MODELS_PATH))
        try:
            v3 = await self._read(V3_CONFIG_PATH, IDE_HEADERS)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - 促销读不到不影响基础倍率
            v3 = {}
        price = build_price_map(enterprise, _entries(v3))
        promotions = v3.get("modelPromotions") if isinstance(v3, dict) else None
        # ``data`` 也可能直接是那个 IDE 形状的字典
        if promotions is None and isinstance(v3, dict):
            data = v3.get("data")
            if isinstance(data, dict):
                promotions = data.get("modelPromotions")

        moment = datetime.now(timezone.utc)
        details = {}
        for model_id, base in price.items():
            if base is None:
                continue
            effective = format_credits(base)
            if effective is None:
                continue
            label = ""
            note = ""
            promo = select_promotion(promotions, model_id, moment)
            if promo is not None:
                label = promotion_label(promo)
                note = promotion_note(promo)
                discounted = promotion_discount(promo, base)
                if discounted is not None:
                    effective = format_credits(discounted) or effective
            details[model_id] = {
                "id": model_id,
                "credits": base,
                "effective_credits": effective,
                "promo_label": label,
                "promo_note": note,
            }
        return details

    def _backoff(self) -> None:
        self._failures += 1
        delay = DEFAULT_RETRY_AFTER * (2 ** (self._failures - 1))
        self._next_try = self._clock() + min(delay, MAX_RETRY_AFTER)

    # -- 对外 -------------------------------------------------------------

    async def ensure(self) -> dict:
        """当前这份倍率表；过期就重读，读失败沿用上一份（可能为空）。"""
        now = self._clock()
        if self._fetched_at is not None and now - self._fetched_at < self._ttl:
            return self._details
        async with self._lock:
            now = self._clock()
            if self._fetched_at is not None and now - self._fetched_at < self._ttl:
                return self._details
            if now < self._next_try or not self._has_credentials():
                return self._details
            try:
                details = await self._fetch(now)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - 显示层的数据，坏了就退回裸 ID
                self._backoff()
                return self._details
            self._details = details
            self._fetched_at = now
            self._failures = 0
            self._next_try = 0.0
            return self._details

    async def refresh(self) -> dict:
        """无视保鲜期与退避重读一次（``/wb models`` 手动刷新用）。"""
        self._fetched_at = None
        self._next_try = 0.0
        self._failures = 0
        return await self.ensure()

    def detail_for(self, model_id):
        return self._details.get(model_id)
