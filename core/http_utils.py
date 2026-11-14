"""出站 HTTP 的两个纯工具：公共请求头与 ``Retry-After`` 解析。

它们原先住在 ``core/auth.py``。``auth.py`` 是旧 OAuth 登录/刷新的整套实现，已经
随私有聊天路径一起下线；但这两个函数跟登录没有关系——签到接口还要用同一套头，
错误映射还要用同一个 ``Retry-After``。所以搬到这里，而不是跟着 ``auth.py`` 一起删。

两个函数都不碰网络、不读配置、不看全局状态：给输入就出结果，于是可以被单独测。
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

# 出站指纹固定这两组版本号。它们是**身份**而不是可调参数：上游按客户端版本决定
# 返回什么形状，让运维改它只会让插件在不认识的响应上崩掉。
_CLIENT_VERSION = "5.5.4"
_CLI_VERSION = "2.137.1"
_ORIGIN_CN = "https://www.codebuddy.cn"
_ORIGIN_GLOBAL = "https://www.workbuddy.ai"


def _is_global(realm) -> bool:
    return realm == "global"


def _origin_for(realm) -> str:
    return _ORIGIN_GLOBAL if _is_global(realm) else _ORIGIN_CN


def _accept_language(realm) -> str:
    return "en-US" if _is_global(realm) else "zh-CN"


def common_headers(realm, token=None) -> dict:
    """每个接口共用的一组出站头。

    ``realm`` 只影响 Origin / Referer / Accept-Language 三处；``token`` 给了就带
    ``Authorization``。``X-Refresh-Token`` **不**在这里出现——它只属于刷新接口，
    出现在普通请求上就是把长期凭据主动递出去。
    """
    origin = _origin_for(realm)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": origin,
        "Referer": origin + "/",
        "User-Agent": "WorkBuddy/{} WorkBuddy/{} CLI/{}".format(
            _CLIENT_VERSION, _CLIENT_VERSION, _CLI_VERSION
        ),
        "X-CodeBuddy-Request": "1",
        "Accept-Language": _accept_language(realm),
    }
    if token:
        headers["Authorization"] = "Bearer " + token
    return headers


def parse_retry_after(resp):
    """把 ``Retry-After`` 解析成秒数，认不出来返回 ``None``。

    两种合法形状都要认：延迟秒数（``"120"``）与 HTTP-date。缺头、空串、负数以外的
    垃圾一律 ``None``，绝不编一个等待时间出来——上层拿 ``None`` 就是不提示。

    ``inf`` / ``nan`` 也算垃圾。``float("Infinity")`` 是不报错的，放过去的话上层
    会拿它当冷却时长：``cooldown_until`` 变成无穷大，``_block_reason`` 从此永远返回
    "冷却中"，``resume()`` 只清暂停标记、清不掉它，这个值还会以 ``Infinity`` 的
    形式落进状态文件。``nan`` 更糟——它跟任何数比较都是 False，冷却等于零。
    """
    value = resp.headers.get("Retry-After")
    if not value:
        return None
    value = value.strip()
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        pass
    else:
        if not math.isfinite(seconds):
            return None
        return max(0.0, seconds)
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())


__all__ = ["common_headers", "parse_retry_after"]
