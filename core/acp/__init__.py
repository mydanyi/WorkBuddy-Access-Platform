"""ACP 传输层与运行时。

对外三层：一条管道（:class:`AcpTransport`）、一套协议常量与归一化
（:mod:`core.acp.protocol`）、握手与会话时序（:class:`AcpClient`），
再由 :class:`AcpRuntime` 负责"什么时候拉起进程、登录态怎么记、退出登录删哪层目录"，
:class:`AcpRuntimePool` 负责"哪个模型用哪条进程、进程多了淘汰谁"。

OpenAI 兼容适配在 :mod:`core.acp.backend`。
"""

from .backend import AcpChatBackend, build_prompt
from .client import KIND_DONE, AcpClient
from .credentials import CliCredentials, map_credentials
from .pool import AcpRuntimePool
from .protocol import (
    CATEGORY_AUTH,
    KIND_OTHER,
    KIND_TEXT,
    KIND_THOUGHT,
    KIND_TOOL,
    STOP_CANCELLED,
    STOP_END_TURN,
    STOP_MAX_TOKENS,
    STOP_REFUSAL,
    AcpError,
    AcpProcessError,
    AcpProtocolError,
    AcpRpcError,
    AcpTimeoutError,
)
from .runtime import (
    AUTH_SUBPATH,
    CONFIG_DIR_NAME,
    CREDENTIAL_FILE,
    CREDENTIAL_LOGGED_OUT,
    DEFAULT_LOGIN_TIMEOUT,
    PRODUCT_DATA_DIR,
    AcpRuntime,
    default_argv,
)
from .transport import SESSION_CLOSED, AcpTransport

__all__ = [
    "AcpTransport",
    "AcpClient",
    "AcpChatBackend",
    "build_prompt",
    "AcpRuntime",
    "AcpRuntimePool",
    "CliCredentials",
    "map_credentials",
    "default_argv",
    "KIND_DONE",
    "SESSION_CLOSED",
    "AcpError",
    "AcpProtocolError",
    "AcpRpcError",
    "AcpProcessError",
    "AcpTimeoutError",
    "CATEGORY_AUTH",
    "CONFIG_DIR_NAME",
    "PRODUCT_DATA_DIR",
    "AUTH_SUBPATH",
    "CREDENTIAL_FILE",
    "CREDENTIAL_LOGGED_OUT",
    "DEFAULT_LOGIN_TIMEOUT",
    "KIND_TEXT",
    "KIND_THOUGHT",
    "KIND_TOOL",
    "KIND_OTHER",
    "STOP_END_TURN",
    "STOP_MAX_TOKENS",
    "STOP_REFUSAL",
    "STOP_CANCELLED",
]
