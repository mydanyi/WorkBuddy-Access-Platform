"""Configuration reading and validation for the WorkBuddy plugin.

This module only *reads and validates* the nested plugin configuration produced
by AstrBot's ``_conf_schema.json`` machinery.  It introduces no new dependency
and no runtime abstraction: it returns a small nested dict of normalized values
or raises :class:`ConfigError` with a fixed, non-sensitive message.

Rules pinned by the task:

* ``gateway.port`` must be an integer in ``0..65535`` (``0`` means "let the OS
  pick a port" and is only meaningful for tests);
* ``signin.timezone`` must be a valid IANA zone and ``primary`` / ``recovery``
  must be ``HH:MM``;
* the public ``signin.max_attempts`` is capped at 5;
* ``routing.models`` is an ordered list of pure model ids (any legacy display
  suffix this plugin used to append is stripped), order preserved, no
  duplicates, no empty entry and an enabled route needs at least one model;
* ``acp`` carries the CLI launch contract: a non-empty ``cli_path``, the tool
  list (``""`` disables every built-in tool), the optional default model and the
  process cap;
* a bad value raises a *fixed* error that never echoes the raw setting (so a
  secret-looking value can never leak through a validation log line).
"""

from __future__ import annotations

import math
from zoneinfo import ZoneInfo

from .models import strip_model_alias
from .routing import ORDERED_MODEL_ID

MAX_SIGNIN_ATTEMPTS = 5

# 同时常驻的 CLI 进程数上限。每条都是一个 Node 运行时，默认给两个：默认模型一条，
# 外加一个"另一个模型"的口子；再往上算力与内存都不划算。
DEFAULT_MAX_PROCESSES = 2
MAX_PROCESSES_CAP = 16


class ConfigError(ValueError):
    """A configuration value is invalid (message is fixed, never raw)."""


def _bool(value, name, default):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raise ConfigError("配置项 {} 必须为布尔值".format(name))


def _int(value, name, default, minimum=None, maximum=None):
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError("配置项 {} 必须为整数".format(name))
    if minimum is not None and value < minimum:
        raise ConfigError("配置项 {} 不能小于 {}".format(name, minimum))
    if maximum is not None and value > maximum:
        raise ConfigError("配置项 {} 不能大于 {}".format(name, maximum))
    return value


def _number(value, name, default, minimum=None):
    """A non-negative int-or-float seconds value (timing knobs).

    The schema default is an integer, but a caller may set a sub-second value;
    a real AstrBot config only derives defaults from the schema, so a float is
    accepted here while ``bool`` is still rejected.
    """
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError("配置项 {} 必须为数字".format(name))
    if minimum is not None and value < minimum:
        raise ConfigError("配置项 {} 不能小于 {}".format(name, minimum))
    return value


def _str(value, name, default):
    if value is None:
        return default
    if not isinstance(value, str):
        raise ConfigError("配置项 {} 必须为字符串".format(name))
    return value


def _list(value, name, default):
    if value is None:
        return default
    if not isinstance(value, list):
        raise ConfigError("配置项 {} 必须为列表".format(name))
    return list(value)


def _hhmm(value, name, default):
    text = _str(value, name, default)
    if not text or text.count(":") != 1:
        raise ConfigError("配置项 {} 必须为 HH:MM 格式".format(name))
    head, tail = text.split(":")
    try:
        hour = int(head)
        minute = int(tail)
    except (TypeError, ValueError):
        raise ConfigError("配置项 {} 必须为 HH:MM 格式".format(name)) from None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ConfigError("配置项 {} 必须为 HH:MM 格式".format(name))
    return text


def _timezone(value, name, default):
    text = _str(value, name, default)
    try:
        ZoneInfo(text)
    except Exception:
        raise ConfigError("配置项 {} 不是合法时区".format(name)) from None
    return text


def _field(section, key):
    """Return ``(present, value)`` so an explicit ``None`` differs from absent."""
    if isinstance(section, dict) and key in section:
        return True, section[key]
    return False, None


def _str_present(value, name, default, present):
    """A string field: absent uses ``default``; a *present* non-string is invalid."""
    if not present:
        return default
    if not isinstance(value, str):
        raise ConfigError("配置项 {} 必须为字符串".format(name))
    return value


def _admin_qq(value, name, default, present):
    """An admin QQ: empty is allowed, otherwise a positive ASCII digit string.

    No group-id heuristic leaks here (a long digit run is still a valid id);
    only ``0``/``00``/signs/suffixes/unicode digits/other types are rejected.
    """
    if not present:
        return default
    if not isinstance(value, str):
        raise ConfigError("配置项 {} 必须为字符串".format(name))
    if value == "":
        return ""
    if not (value.isascii() and value.isdigit()):
        raise ConfigError("配置项 {} 必须为纯数字".format(name))
    if int(value) <= 0:
        raise ConfigError("配置项 {} 必须为正数".format(name))
    return value


def _notifications(section) -> dict:
    present, value = _field(section, "enabled")
    enabled = _bool(value, "notifications.enabled", False)
    present, value = _field(section, "signin_success")
    signin_success = _bool(value, "notifications.signin_success", True)
    present, value = _field(section, "errors")
    errors = _bool(value, "notifications.errors", True)
    present, value = _field(section, "admin_qq")
    admin_qq = _admin_qq(value, "notifications.admin_qq", "", present)
    present, value = _field(section, "platform_id")
    platform_id = _str_present(value, "notifications.platform_id", "", present)
    present, value = _field(section, "error_cooldown_seconds")
    cooldown = _int(
        value, "notifications.error_cooldown_seconds", 1800, 60, 86400
    )
    return {
        "enabled": enabled,
        "signin_success": signin_success,
        "errors": errors,
        "admin_qq": admin_qq,
        "platform_id": platform_id,
        "error_cooldown_seconds": cooldown,
    }


def _positive_seconds(value, name, default):
    """A finite, strictly positive number of seconds (never ``bool``/NaN/inf)."""
    number = _number(value, name, default, 0)
    if not math.isfinite(number) or number <= 0:
        raise ConfigError("配置项 {} 必须为有限正数".format(name))
    return number


def _routing_models(value) -> list:
    """Normalize the ordered model list to pure model ids, keeping order.

    A legacy value may still carry this plugin's old display suffix
    (``"wb-1 [0.11x · 夜间折扣]"`` -> ``"wb-1"``); a duplicate, an empty entry,
    a non-string entry or the virtual entry itself is rejected with a fixed
    message that never echoes the raw value.
    """
    items = _list(value, "routing.models", [])
    models = []
    seen = set()
    for item in items:
        if not isinstance(item, str):
            raise ConfigError("配置项 routing.models 只能包含模型 ID 字符串")
        model = strip_model_alias(item).strip()
        if not model:
            raise ConfigError("配置项 routing.models 不能包含空模型 ID")
        if model == ORDERED_MODEL_ID:
            raise ConfigError("配置项 routing.models 不能包含虚拟入口自身")
        if model in seen:
            raise ConfigError("配置项 routing.models 不能包含重复模型 ID")
        seen.add(model)
        models.append(model)
    return models


def _routing(section) -> dict:
    """The ordered primary / standby model list (``models[0]`` is primary)."""
    present, value = _field(section, "enabled")
    enabled = _bool(value, "routing.enabled", False)
    present, value = _field(section, "models")
    models = _routing_models(value) if present else []
    if enabled and not models:
        raise ConfigError("配置项 routing.models 在启用时不能为空")
    present, value = _field(section, "cooldown_seconds")
    cooldown = _positive_seconds(value, "routing.cooldown_seconds", 60)
    present, value = _field(section, "first_frame_timeout")
    timeout = _positive_seconds(value, "routing.first_frame_timeout", 90)
    return {
        "enabled": enabled,
        "models": models,
        "cooldown_seconds": cooldown,
        "first_frame_timeout": timeout,
    }


def _acp(section) -> dict:
    """The CLI launch contract.

    ``cli_path`` must be non-empty (an empty one would only fail much later, as a
    502 with no explanation). ``tools`` is a plain string passed through as
    ``--tools``: the empty string is the safe default and disables every
    built-in tool. ``model`` is what a request that carries no model falls back
    to; an empty value means "let the CLI pick its own default", which is why it
    is optional rather than required. ``max_processes`` bounds how many CLI
    processes may stay resident at once.
    """
    present, value = _field(section, "cli_path")
    cli_path = _str_present(value, "acp.cli_path", "codebuddy", present).strip()
    if not cli_path:
        raise ConfigError("配置项 acp.cli_path 不能为空")
    present, value = _field(section, "tools")
    tools = _str_present(value, "acp.tools", "", present)
    present, value = _field(section, "model")
    model = strip_model_alias(_str_present(value, "acp.model", "", present)).strip()
    if model == ORDERED_MODEL_ID:
        # 虚拟入口是网关的路由占位符，不是 CLI 认识的模型 ID；当默认模型存下来
        # 只会让每条"没带模型"的请求都起一条注定失败的进程。
        raise ConfigError("配置项 acp.model 不能是虚拟入口模型")
    present, value = _field(section, "max_processes")
    max_processes = _int(
        value,
        "acp.max_processes",
        DEFAULT_MAX_PROCESSES,
        1,
        MAX_PROCESSES_CAP,
    )
    present, value = _field(section, "login_timeout")
    login_timeout = _int(value, "acp.login_timeout", 120, 1)
    return {
        "cli_path": cli_path,
        "tools": tools,
        "model": model,
        "max_processes": max_processes,
        "login_timeout": login_timeout,
    }


def _section(raw, name) -> dict:
    value = raw.get(name) if hasattr(raw, "get") else None
    return value if isinstance(value, dict) else {}


def load_settings(raw) -> dict:
    """Return a validated, normalized settings dict from the raw plugin config."""
    gateway = _section(raw, "gateway")
    signin = _section(raw, "signin")
    acp = _section(raw, "acp")
    http = _section(raw, "http")
    notifications = _section(raw, "notifications")
    routing = _section(raw, "routing")

    return {
        "gateway": {
            "enabled": _bool(gateway.get("enabled"), "gateway.enabled", True),
            "host": _str(gateway.get("host"), "gateway.host", "127.0.0.1"),
            "port": _int(gateway.get("port"), "gateway.port", 17863, 0, 65535),
            "api_key": _str(gateway.get("api_key"), "gateway.api_key", ""),
            "concurrency": _int(gateway.get("concurrency"), "gateway.concurrency", 1, 1),
            "rpm": _int(gateway.get("rpm"), "gateway.rpm", 6, 1),
            "queue_size": _int(gateway.get("queue_size"), "gateway.queue_size", 2, 0),
            "queue_timeout": _int(
                gateway.get("queue_timeout"), "gateway.queue_timeout", 30, 1
            ),
            # The approved request-body cap: whole MiB, at least 1, never
            # ``bool``/float/string and never ``0`` ("unlimited" does not exist).
            "max_request_body_mb": _int(
                gateway.get("max_request_body_mb"),
                "gateway.max_request_body_mb",
                16,
                1,
            ),
        },
        "signin": {
            "enabled": _bool(signin.get("enabled"), "signin.enabled", True),
            "timezone": _timezone(
                signin.get("timezone"), "signin.timezone", "Asia/Shanghai"
            ),
            "primary": _hhmm(signin.get("primary"), "signin.primary", "00:10"),
            "recovery": _hhmm(signin.get("recovery"), "signin.recovery", "12:10"),
            "jitter": _int(signin.get("jitter"), "signin.jitter", 120, 0),
            "max_attempts": _int(
                signin.get("max_attempts"),
                "signin.max_attempts",
                MAX_SIGNIN_ATTEMPTS,
                0,
                MAX_SIGNIN_ATTEMPTS,
            ),
        },
        "acp": _acp(acp),
        "http": {
            "total_timeout": _int(
                http.get("total_timeout"), "http.total_timeout", 180, 1
            ),
            "connect_timeout": _int(
                http.get("connect_timeout"), "http.connect_timeout", 15, 1
            ),
            "sock_read_timeout": _int(
                http.get("sock_read_timeout"), "http.sock_read_timeout", 120, 1
            ),
        },
        "notifications": _notifications(notifications),
        "routing": _routing(routing),
    }
