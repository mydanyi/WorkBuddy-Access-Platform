"""Real-AstrBot integration tests for the WorkBuddy plugin entry point.

Scope: only the documented public entry contract of a plugin running inside a
**real** AstrBot 4.27.4 install.  The plugin entry (``main.WorkBuddyPlugin``),
``_conf_schema.json``, ``metadata.yaml`` and ``requirements.txt`` do not exist
yet on purpose: the top-level ``import astrbot_plugin_workbuddy.main`` below is
expected to fail (``ImportError``) until that entry point is implemented and
verified remotely in the Docker host.  Do **not** pre-build ``main`` / config /
metadata just to make these tests pass; the red light is the point.

Nothing here substitutes the framework: ``astrbot.api.star.Star``,
``star_handlers_registry``, ``AstrMessageEvent`` / ``AstrBotMessage`` /
``MessageMember`` / ``PlatformMetadata``, ``AstrBotConfig``, ``PluginManager``
and ``ProviderOpenAIOfficial`` are the *real* classes.  Only two boundaries are
external and allowed to be minimal:

* the ``Context`` shim: ``get_config`` / ``send_message`` / ``get_all_stars`` /
  ``get_registered_star`` — never the plugin's own behaviour;
* a real ``aiohttp`` fake *WorkBuddy upstream* on 127.0.0.1:0.  It only serves
  the two check-in routes: chat goes through ACP, the model list comes from the
  local CLI, and the balance / multiplier endpoints are gone with ``core/auth``.

The module doubles as the *bootstrap* for the rest of the suite: its header pins
``ASTRBOT_ROOT`` / ``TMPDIR`` / ``WORKBUDDY_TEST_TMP`` **before any** ``astrbot``
import, so ``test_admin_notifications`` / ``test_model_picker`` /
``test_notification_integration`` / ``test_routing_plugin`` import this module
first and reuse its harness.  Shared low-level fixtures live in
``tests/support.py``; nothing here re-exports a *test* class.

Every credential / token / key below is a FICTIONAL FIXED SAMPLE.
"""

import asyncio
import contextlib
import functools
import json
import os
import shutil
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

import aiohttp

# ---------------------------------------------------------------------------
# Header environment pin — MUST run before importing astrbot/api/main.
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]  # directory that contains astrbot_plugin_workbuddy/
_PLUGIN_ROOT = _SOURCE_ROOT / "astrbot_plugin_workbuddy"
_TESTS_DIR = _HERE.parent


def _resolve_base_tmp() -> Path:
    """Temp root: env override, else the current task's NAS tmp dir."""
    env = os.environ.get("WORKBUDDY_TEST_TMP")
    if env:
        return Path(env)
    task_root = _HERE.parents[3]
    return task_root / "tmp"


BASE_TMP = _resolve_base_tmp()
BASE_TMP.mkdir(parents=True, exist_ok=True)

# A single unique sandbox for this test module; torn down at module exit.
_SESSION_ROOT = Path(tempfile.mkdtemp(prefix="wb-plugin-int-", dir=str(BASE_TMP)))
_TMP_DIR = _SESSION_ROOT / "tmp"
_TMP_DIR.mkdir(parents=True, exist_ok=True)
_WB_TMP_DIR = _SESSION_ROOT / "workbuddy_tmp"
_WB_TMP_DIR.mkdir(parents=True, exist_ok=True)
_ASTRBOT_ROOT = _SESSION_ROOT / "astrbot_root"
_ASTRBOT_ROOT.joinpath("data").mkdir(parents=True, exist_ok=True)
# The real ``PluginManager`` writes plugin configs under ``<root>/data/config``;
# create it up-front so a first-deploy ``AstrBotConfig`` can persist there.
_ASTRBOT_ROOT.joinpath("data", "config").mkdir(parents=True, exist_ok=True)

# Preserve the existing environment; only override the controllable roots.
os.environ["ASTRBOT_ROOT"] = str(_ASTRBOT_ROOT)
os.environ["TMPDIR"] = str(_TMP_DIR)
os.environ["TEMP"] = str(_TMP_DIR)
os.environ["TMP"] = str(_TMP_DIR)
os.environ["WORKBUDDY_TEST_TMP"] = str(_WB_TMP_DIR)

# ``astrbot_plugin_workbuddy`` discoverable from anywhere, plus the sandbox root
# so the real ``PluginManager`` can import ``data.plugins.astrbot_plugin_workbuddy``.
for _path in (str(_SOURCE_ROOT), str(_PLUGIN_ROOT), str(_TESTS_DIR), str(_ASTRBOT_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

# --- Real framework imports (no fake framework, no sys.modules surgery). ---
from astrbot.api.star import Star  # noqa: E402
from astrbot.core.config import AstrBotConfig  # noqa: E402
from astrbot.core.provider.sources.openai_source import ProviderOpenAIOfficial  # noqa: E402
from astrbot.core.star.star import star_map, star_registry  # noqa: E402
from astrbot.core.star.star_handler import star_handlers_registry  # noqa: E402
from astrbot.core.star.star_manager import PluginManager  # noqa: E402
from astrbot.core.platform.astr_message_event import AstrMessageEvent  # noqa: E402
from astrbot.core.platform.astrbot_message import (  # noqa: E402
    AstrBotMessage,
    Group,
    MessageMember,
)
from astrbot.core.platform.platform_metadata import PlatformMetadata  # noqa: E402
from astrbot.core.platform.message_type import MessageType  # noqa: E402
from astrbot.core.message.components import Plain  # noqa: E402

# --- Plugin entry under test. ---
import astrbot_plugin_workbuddy.main as wb_main  # noqa: E402

# Shared low-level fixtures (real fake upstream, scripted backend, seed values).
try:  # package discovery
    from tests import support
except ImportError:  # top-level discovery
    import support  # type: ignore

# Re-exported for the sibling suites that use this module as their bootstrap.
MASTER_KEY = support.MASTER_KEY
SEED_CREDENTIALS = support.SEED_CREDENTIALS
seed_cli_credentials = support.seed_cli_credentials
MODEL = support.MODEL

# --- FICTIONAL FIXED SAMPLE values (never real). ---
GATEWAY_KEY = support.GATEWAY_KEY
PLUGIN_NAME = "astrbot_plugin_workbuddy"

CHECKIN_STATUS_PATH = "/v2/billing/meter/checkin-activity-status"
CHECKIN_CLAIM_PATH = "/v2/billing/meter/daily-checkin"

_PLUGIN_DATA = _ASTRBOT_ROOT / "data" / "plugin_data" / PLUGIN_NAME
_SANDBOX_PLUGINS = _ASTRBOT_ROOT / "data" / "plugins"

# Every documented config key, so tests can assert real schema defaults.
DOCUMENTED_CONFIG = {
    "gateway": {
        "enabled": True,
        "host": "127.0.0.1",
        "port": 17863,
        "api_key": "",
        "concurrency": 1,
        "rpm": 6,
        "queue_size": 2,
        "queue_timeout": 30,
        "max_request_body_mb": 16,
    },
    "routing": {
        "enabled": False,
        "models": [],
        "cooldown_seconds": 60,
        "first_frame_timeout": 90,
    },
    "acp": {
        "cli_path": "codebuddy",
        "tools": "",
        "model": "",
        "max_processes": 2,
        "login_timeout": 120,
    },
    "signin": {
        "enabled": True,
        "timezone": "Asia/Shanghai",
        "primary": "00:10",
        "recovery": "12:10",
        "jitter": 120,
        "max_attempts": 5,
    },
    "http": {
        "total_timeout": 180,
        "connect_timeout": 15,
        "sock_read_timeout": 120,
    },
    "notifications": {
        "enabled": False,
        "signin_success": True,
        "errors": True,
        "admin_qq": "",
        "platform_id": "",
        "error_cooldown_seconds": 1800,
    },
}


def _deep_merge(base: dict, overrides: dict) -> dict:
    merged = dict(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


class _IntegrationUpstream(support.FakeUpstream):
    """The real HTTP upstream the plugin's signin service talks to.

    Only the two check-in routes exist.  Chat goes through ACP (no HTTP), the
    model list comes from the local CLI, and the balance / multiplier endpoints
    went away with ``core/auth`` and ``core/credits`` — so an unmapped route is
    a real 404 and "no upstream work" is a plain hit count of zero.

    Only the *external server* is extended; no plugin component is replaced.
    """

    def __init__(self) -> None:
        super().__init__()
        self.checkin = {"active": True, "today_checked_in": False}
        self.claim = {"code": 0, "data": {"credit": 10}}
        # Test-only switches: flip ``claim`` to answer a business error.
        # 两条路由都是 **POST**（见 ``core/signin.py``），用 GET 注册只会得到 404。
        self.on("POST", CHECKIN_STATUS_PATH, self._checkin_status)
        self.on("POST", CHECKIN_CLAIM_PATH, self._checkin_claim)

    async def _checkin_status(self, request):
        return support.ok(dict(self.checkin))

    async def _checkin_claim(self, request):
        if self.claim.get("code") != 0:
            return support.biz(
                self.claim["code"],
                self.claim.get("msg", "FICTIONAL"),
                self.claim.get("status", 200),
            )
        self.checkin["today_checked_in"] = True
        return support.ok(dict(self.claim.get("data") or {}))

    @property
    def extra_hits(self) -> int:
        """Every hit is an "extra" read now: there is no baseline request."""
        return sum(self.hits.values())

    def total_hits(self) -> int:
        return sum(self.hits.values())


class _Context:
    """Minimal *external* framework boundary.

    Only the collaborators the plugin legitimately needs are provided; no plugin
    method is patched.  ``get_all_stars`` / ``get_registered_star`` are mapped to
    the real ``star_registry`` so the real ``PluginManager`` works unchanged.
    """

    def __init__(self, config) -> None:
        self._config = config
        self.sent = []
        self._star_manager = None

    def get_config(self, *args, **kwargs):
        return self._config

    async def send_message(self, session, message_chain):
        self.sent.append((str(session), message_chain))
        return True

    def get_all_stars(self):
        return list(star_registry)

    def get_registered_star(self, name):
        for star in star_registry:
            if star.name == name or star.root_dir_name == name:
                return star
        return None


def _make_event(
    text: str,
    *,
    admin: bool = True,
    private: bool = True,
    user_id: str = "wb-test-user",
    group_id: str = "wb-test-group",
) -> AstrMessageEvent:
    message = AstrBotMessage()
    message.type = MessageType.FRIEND_MESSAGE if private else MessageType.GROUP_MESSAGE
    message.self_id = "wb-test-bot"
    message.session_id = user_id if private else group_id
    message.message_id = uuid.uuid4().hex
    message.sender = MessageMember(user_id=user_id, nickname="tester")
    message.message = [Plain(text)]
    message.message_str = text
    message.raw_message = None
    message.group = None if private else Group(group_id=group_id)
    platform = PlatformMetadata(
        name="aiocqhttp", description="integration test", id="wb-test-platform"
    )
    event = AstrMessageEvent(
        text, message, platform, message.session_id
    )
    event.role = "admin" if admin else "member"
    return event


async def _invoke(handler, event, action: str, rest: str = ""):
    """Drive the real ``wb`` entry point.

    It is documented as an async generator; a plain coroutine is also accepted so
    a small interface change does not silently break the assertions.
    """
    outcome = handler(event, action=action, rest=rest)
    if hasattr(outcome, "__aiter__"):
        collected = []
        async for item in outcome:
            collected.append(item)
        return collected
    return [await outcome]


def _result_text(item) -> str:
    """Best-effort plain text of one yielded handler result.

    ``wb`` yields ``event.plain_result(text)`` (a real ``MessageEventResult``),
    so the text lives in its ``MessageChain`` rather than in ``str(result)``.
    Every strategy falls back to ``str`` so a change in the framework shape can
    never silently turn an ``assertIn`` into a no-op.
    """
    if isinstance(item, str):
        return item
    getter = getattr(item, "get_plain_text", None)
    if callable(getter):
        try:
            text = getter()
            if isinstance(text, str) and text:
                return text
        except Exception:  # noqa: BLE001 - fall through to the chain walk
            pass
    for attr in ("message_str", "text"):
        value = getattr(item, attr, None)
        if isinstance(value, str) and value:
            return value
    chain = getattr(item, "chain", None)
    if chain is not None:
        collected = []
        try:
            for component in chain:
                text = getattr(component, "text", None)
                if isinstance(text, str):
                    collected.append(text)
        except TypeError:
            collected = []
        if collected:
            return "".join(collected)
    return str(item)


def _text_of(items) -> str:
    return "\n".join(_result_text(item) for item in items)


class _StubAcpRuntime:
    """ACP 运行时的替身。

    这里只验证命令层接线（链接转达、重复登录取代、退出后收尾）；真实的进程
    生命周期、登录时序、配置目录清理由 ``tests/test_acp_runtime.py`` 用假 CLI
    覆盖，两边不重复。
    """

    def __init__(self, url="https://example.com/login?state=stub", *, block=False):
        self.url = url
        self.block = block
        self.calls = 0
        self.closed = False
        self._gate = asyncio.Event()

    @property
    def auth_state(self):
        return True if self._gate.is_set() else None

    def status(self) -> dict:
        return {
            "started": False,
            "auth": self.auth_state,
            "has_config_dir": False,
        }

    async def login(self, *, on_auth_url=None, timeout=None):
        self.calls += 1
        if on_auth_url is not None:
            on_auth_url(self.url)
        if self.block:
            await self._gate.wait()
        else:
            self._gate.set()
        return {"authUrl": self.url, "result": {}}

    async def logout(self, *, purge_config=False):
        self._gate.set()
        if purge_config:
            # 镜像真实运行时：凭据在 data_dir/cli-home 下，退出登录就删掉。
            shutil.rmtree(os.path.join(str(_PLUGIN_DATA), "cli-home"), ignore_errors=True)

    async def close(self):
        self.closed = True

    async def shutdown(self):
        self.closed = True

    def note_auth_failure(self):
        pass

    def note_session_ok(self):
        pass


class _StubAcpPool:
    """进程池的替身：只验命令层接线（链接转达、重复登录取代、退出后收尾）。

    真实进程池的懒创建、占用、淘汰、``close_all`` 语义由
    ``tests/test_acp_pool.py`` 覆盖；真实的进程生命周期、登录时序、目录清理由
    ``tests/test_acp_runtime.py`` 用假 CLI 覆盖。这里只模拟 ``main`` 用到的那几个
    入口，好让命令层的断言不被一个 Node 进程牵着走。
    """

    def __init__(self, runtime=None, *, max_processes=2):
        self.primary = runtime if runtime is not None else _StubAcpRuntime()
        self._max = max_processes
        self.close_alls = 0
        self.logouts = 0
        self.shutdowns = 0

    @property
    def auth_state(self):
        return self.primary.auth_state

    def status(self) -> dict:
        return {
            "max_processes": self._max,
            "primary_model": None,
            "models": [None],
            "running": [],
        }

    def get(self, model=None):
        return self.primary

    async def close_all(self) -> None:
        self.close_alls += 1

    async def logout(self, *, purge_config=False) -> None:
        # 与真实池同序：先关掉全部进程，再删凭据。顺序错了（先删后关），那条还活着
        # 的进程会把 token 原样写回去，而命令已经回复"已退出登录"。
        self.logouts += 1
        await self.primary.logout(purge_config=purge_config)

    async def shutdown(self) -> None:
        self.shutdowns += 1
        await self.primary.shutdown()


_FAKE_CLI = Path(__file__).resolve().parent / "fake_acp_cli.py"


async def _acp_client(scenario: str):
    """一个真连假 CLI、且已完成握手的 ACP 客户端。

    聊天现在只有一条路：Provider → 网关 → ``AcpChatBackend`` → ACP stdio。
    要验的是这条链真的通，所以这里不替换适配层，只在最外面换成
    ``tests/fake_acp_cli.py``——它已经是 ``tests/test_acp_backend.py`` 的
    标准夹具，两边共用同一个假 CLI。

    握手必须在这里做：生产链路上是 ``AcpRuntime.ensure_started()`` 代劳的，
    适配层只接收一个已经就绪的客户端，自己不发 ``initialize``。
    """
    from astrbot_plugin_workbuddy.core.acp import AcpClient, AcpTransport

    transport = AcpTransport(
        [sys.executable, str(_FAKE_CLI), "--scenario", scenario],
        request_timeout=10.0,
    )
    await transport.start()
    client = AcpClient(transport, cwd=str(_PLUGIN_DATA))
    await client.initialize()
    return transport, client


class _RecordingBackend:
    """记录后端收到的 payload，再原样转给真正的 ACP 后端。

    网关交给后端的模型 ID 只有看见 payload 才能断言，而 OpenAI 响应里未必有
    ``model`` 的落点（假 CLI 自己不回填）。
    """

    def __init__(self, inner):
        self._inner = inner
        self.payloads = []

    @property
    def limits(self):
        return self._inner.limits

    @limits.setter
    def limits(self, value):
        self._inner.limits = value

    async def stream(self, payload, observe_errors=True):
        self.payloads.append(payload)
        async for chunk in self._inner.stream(payload, observe_errors=observe_errors):
            yield chunk


class _PluginIntegrationTestCase(unittest.IsolatedAsyncioTestCase):
    # -- lifecycle --------------------------------------------------------
    async def asyncSetUp(self) -> None:
        # Isolate per-test plugin data (real StarTools.get_data_dir location).
        shutil.rmtree(_PLUGIN_DATA, ignore_errors=True)
        os.environ["WORKBUDDY_MASTER_KEY"] = MASTER_KEY
        self.up = _IntegrationUpstream()
        await self.up.start()
        # 签到链只剩这一个可替换边界：模块级的固定来源。
        wb_main.BILLING_BASE = self.up.base
        self._plugins = []
        self._providers = []

    async def asyncTearDown(self) -> None:
        for plugin in self._plugins:
            with contextlib.suppress(Exception):
                await plugin.terminate()
        for provider in self._providers:
            with contextlib.suppress(Exception):
                await provider.terminate()
        await self.up.stop()
        shutil.rmtree(_PLUGIN_DATA, ignore_errors=True)

    # -- factories --------------------------------------------------------
    def _build_config(self, config_path: Path, **overrides):
        """Build a real ``AstrBotConfig`` from the plugin's real schema."""
        schema_path = _PLUGIN_ROOT / "_conf_schema.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        config = AstrBotConfig(config_path=str(config_path), schema=schema)
        if overrides:
            config.update(_deep_merge(dict(config), overrides))
            config.save_config()
        return config

    def _ensure_metadata_name(self) -> None:
        """``StarTools.get_data_dir`` resolves the name from ``star_map``.

        In a direct (non-loader) instantiation the ``__init_subclass__`` record
        has no name yet; the loader sets it.  The harness fills the external
        boundary so the plugin's own storage code runs unchanged.
        """
        metadata = star_map.get(wb_main.WorkBuddyPlugin.__module__)
        if metadata is not None and not metadata.name:
            metadata.name = PLUGIN_NAME

    async def _make_plugin(self, *, initialize=True, **overrides):
        cfg_path = _SESSION_ROOT / ("cfg-" + uuid.uuid4().hex + ".json")
        config = self._build_config(cfg_path, **overrides)
        context = _Context(config)
        self._ensure_metadata_name()
        plugin = wb_main.WorkBuddyPlugin(context, config)
        self._plugins.append(plugin)
        if initialize:
            await plugin.initialize()
        return plugin

    def _make_ready_plugin_config(self, **overrides):
        base = {
            "gateway": {"enabled": True, "api_key": GATEWAY_KEY, "port": 0},
            # The automatic check-in loop is off by default so a logged-in
            # account never fires a signin round behind a test's back (e.g. the
            # "rejected commands touch zero upstream" assertion).
            "signin": {"enabled": False},
            # 测试一律不许拉起真 CLI：本机/构建机上可能真的装了 codebuddy，
            # 一次 token 刷新就会开一个真进程，慢且会写到测试不该碰的地方。
            # 明确指向不存在的可执行文件，让 ``ensure_started`` 立刻失败。
            "acp": {"cli_path": "/nonexistent/codebuddy-for-tests"},
        }
        return _deep_merge(base, overrides)

    def _seed_credentials(self) -> None:
        """Write the *only* credential source one boot reads: the CLI's own file.

        加密凭据存储属于已经删掉的 OAuth 通道；插件现在只读 CLI 落盘的那份明文
        JSON（见 ``core/acp/credentials.py``）。所以"已登录"就是往那儿写。
        """
        from astrbot.api.star import StarTools

        data_dir = StarTools.get_data_dir(PLUGIN_NAME)
        seed_cli_credentials(data_dir)

    async def _make_acp_plugin(self, scenario, *, wrap=None, catalog=None):
        """装配一个把聊天交给真 ACP 客户端的插件。

        注入必须排在 ``initialize()`` 之后：它开头会 ``_release_runtime()``，
        而释放会把后端一起丢掉（后端持着已被 shutdown 的运行时，留着就会在下次
        聊天里恒 502）。所以顺序按生产里"重载插件"那一套来——先正常装好，再换
        后端，最后重建网关；``_build_gateway`` 会复用已放好的后端。

        ``catalog`` 用来换掉本机清单：真清单要 spawn 一次 ``codebuddy --help``，
        而测试机上没有那个 CLI。清单本身的解析由 ``tests/test_models.py`` 覆盖。
        """
        from astrbot_plugin_workbuddy.core.acp import AcpChatBackend
        from astrbot_plugin_workbuddy.core.routing import ORDERED_MODEL_ID

        transport, client = await _acp_client(scenario)
        self.addAsyncCleanup(transport.close)
        plugin = await self._make_plugin(**self._make_ready_plugin_config())
        self.assertTrue(plugin.ready)
        backend = AcpChatBackend(client, model=ORDERED_MODEL_ID)
        plugin.chat = backend if wrap is None else wrap(backend)
        if catalog is not None:
            plugin.catalog = catalog
        await plugin.gateway.stop()
        await plugin._build_gateway()
        return plugin

    def _corrupt_state(self) -> str:
        """Write invalid state bytes; returns the file's content afterwards."""
        from astrbot_plugin_workbuddy.core.storage import StateStore

        _PLUGIN_DATA.mkdir(parents=True, exist_ok=True)
        path = StateStore(_PLUGIN_DATA).path
        path.write_bytes(b"FICTIONAL-CORRUPT-STATE-PLACEHOLDER{")
        return path.read_bytes().decode("latin-1")

    def _state_blob(self) -> str:
        from astrbot_plugin_workbuddy.core.storage import StateStore

        path = StateStore(_PLUGIN_DATA).path
        return path.read_bytes().decode("latin-1") if path.is_file() else ""

    def _credentials_blob(self) -> str:
        """The CLI credential file exactly as it sits on disk (byte comparison)."""
        path = support.cli_credential_path(_PLUGIN_DATA)
        return path.read_bytes().decode("latin-1") if path.is_file() else ""

    # -- 1. registration / schema / metadata / requirements ----------------
    async def test_star_registration_schema_metadata_and_requirements(self):
        self.assertTrue(issubclass(wb_main.WorkBuddyPlugin, Star))
        self.assertTrue(callable(getattr(wb_main.WorkBuddyPlugin, "initialize", None)))
        self.assertTrue(callable(getattr(wb_main.WorkBuddyPlugin, "terminate", None)))
        self.assertTrue(hasattr(wb_main.WorkBuddyPlugin, "wb"))

        # The handler decorator registered against the real handler registry.
        names = {h.handler_name for h in star_handlers_registry}
        self.assertIn("wb", names)

        # Real schema parses and reproduces every documented default.
        schema_path = _PLUGIN_ROOT / "_conf_schema.json"
        self.assertTrue(schema_path.is_file())
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        for section, values in DOCUMENTED_CONFIG.items():
            self.assertIn(section, schema)
            for key in values:
                self.assertIn(key, schema[section]["items"])

        # metadata.yaml is legal and does not fabricate a repoURL when unknown.
        import yaml

        metadata_path = _PLUGIN_ROOT / "metadata.yaml"
        self.assertTrue(metadata_path.is_file())
        metadata = yaml.safe_load(metadata_path.read_text(encoding="utf-8"))
        for field in ("name", "author", "desc", "version"):
            self.assertIn(field, metadata)
        self.assertEqual(metadata["name"], PLUGIN_NAME)
        repo = str(metadata.get("repo", ""))
        self.assertNotIn("REPLACE_ME", repo)
        self.assertNotIn("example.com", repo)

        # requirements are only the two runtime dependencies.
        requirements = (_PLUGIN_ROOT / "requirements.txt").read_text(
            encoding="utf-8"
        ).lower()
        self.assertIn("aiohttp", requirements)
        self.assertIn("cryptography", requirements)

    # -- 2. real PluginManager.load ---------------------------------------
    async def test_real_plugin_manager_loads_plugin_from_plugins_dir(self):
        target = _SANDBOX_PLUGINS / PLUGIN_NAME
        _SANDBOX_PLUGINS.mkdir(parents=True, exist_ok=True)
        shutil.copytree(_PLUGIN_ROOT, target, dirs_exist_ok=True)

        cfg_path = _SESSION_ROOT / ("loader-cfg-" + uuid.uuid4().hex + ".json")
        config = AstrBotConfig(
            config_path=str(cfg_path),
            schema=json.loads(
                (target / "_conf_schema.json").read_text(encoding="utf-8")
            ),
        )
        # ``PluginManager`` needs the *global* config too; an empty schema
        # reproduces the real default without colliding with the plugin schema.
        manager_cfg_path = _SESSION_ROOT / (
            "loader-manager-cfg-" + uuid.uuid4().hex + ".json"
        )
        manager_config = AstrBotConfig(config_path=str(manager_cfg_path), schema={})
        context = _Context(config)

        # The real loader reads/writes external shared-preferences storage
        # (``sp.global_get`` / ``sp.global_put``); provide an in-memory boundary
        # for only those two methods and restore them afterwards.
        import astrbot.core as astrbot_core

        shared = {}
        original_get = astrbot_core.sp.global_get
        original_put = astrbot_core.sp.global_put

        async def _global_get(key, default=None):
            return shared.get(key, default)

        async def _global_put(key, value):
            shared[key] = value

        astrbot_core.sp.global_get = _global_get
        astrbot_core.sp.global_put = _global_put
        try:
            manager = PluginManager(context, manager_config)
            success, error = await manager.load(specified_dir_name=PLUGIN_NAME)
            self.assertTrue(success, msg=str(error))
        finally:
            astrbot_core.sp.global_get = original_get
            astrbot_core.sp.global_put = original_put

        module_path = f"data.plugins.{PLUGIN_NAME}.main"
        metadata = star_map.get(module_path)
        self.assertIsNotNone(metadata)
        self.assertIsNotNone(metadata.star_cls)
        # The loader imports a *distinct* real module
        # (``data.plugins.<name>.main``) from the top-level ``wb_main``
        # (``astrbot_plugin_workbuddy.main``), so class identity is checked
        # against that module, not ``wb_main``.
        self.assertIsInstance(metadata.star_cls, Star)
        self.assertIs(type(metadata.star_cls), metadata.module.WorkBuddyPlugin)

        # The decorator-registered handler is bound to *this* instance through
        # the real ``functools.partial(raw_func, instance)`` loader binding.
        handlers = [
            h
            for h in star_handlers_registry.get_handlers_by_module_name(module_path)
            if h.handler_name == "wb"
        ]
        self.assertTrue(handlers)
        bound = handlers[0].handler
        self.assertTrue(isinstance(bound, functools.partial))
        self.assertIs(bound.args[0], metadata.star_cls)

        # terminate() releases the plugin's owned resources and is idempotent.
        await metadata.star_cls.terminate()
        await metadata.star_cls.terminate()

    # -- 3. no key material is needed to start; terminate is idempotent ---
    async def test_startup_needs_no_master_key_and_terminate_is_idempotent(self):
        """主密钥不再是启动前提。

        它当年只服务于已经删掉的 OAuth 通道：插件自己加密存 token。现在唯一的凭据
        来源是 CLI 落盘的明文 JSON，插件只读——没有东西要用那把钥匙。留一道"没配
        主密钥就不启动"的闸，只会让从旧版升级上来的部署凭空停摆。

        环境变量仍然被写进来（其它用例要验它不出现在输出里），所以这里显式删掉它，
        证明启动完全不看它。
        """
        os.environ.pop("WORKBUDDY_MASTER_KEY", None)
        plugin = await self._make_plugin(**self._make_ready_plugin_config())
        self.assertTrue(plugin.ready)
        self.assertEqual(plugin.error_reason, "")
        self.assertEqual(self.up.extra_hits, 0)

        # terminate() twice must be clean and leak no coroutines.
        before = {t for t in asyncio.all_tasks() if t is not asyncio.current_task()}
        await plugin.terminate()
        await plugin.terminate()
        await asyncio.sleep(0)
        leaked = {
            t
            for t in asyncio.all_tasks()
            if t is not asyncio.current_task() and not t.done() and t not in before
        }
        self.assertFalse(leaked, f"terminate leaked tasks: {leaked}")

        # 一个解不开的垃圾值同样不影响启动：没有任何地方会去解它。
        os.environ["WORKBUDDY_MASTER_KEY"] = "not-base64-not-32-bytes"
        plugin2 = await self._make_plugin(**self._make_ready_plugin_config())
        self.assertTrue(plugin2.ready)
        await plugin2.terminate()

    # -- 3b. empty api_key never opens the listener -----------------------
    async def test_valid_settings_bind_health_and_empty_api_key_disables_listener(self):
        os.environ["WORKBUDDY_MASTER_KEY"] = MASTER_KEY
        plugin = await self._make_plugin(**self._make_ready_plugin_config())
        self.assertTrue(plugin.ready)
        port = plugin.gateway.bound_port
        self.assertTrue(port)
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/health") as resp:
                self.assertEqual(resp.status, 200)
                text = await resp.text()
                for secret in (
                    MASTER_KEY,
                    GATEWAY_KEY,
                    SEED_CREDENTIALS["accessToken"],
                    SEED_CREDENTIALS["refreshToken"],
                ):
                    self.assertNotIn(secret, text)

        # Empty api_key even when enabled must not open the chat listener.
        plugin2 = await self._make_plugin(
            **self._make_ready_plugin_config(
                gateway={"enabled": True, "api_key": "", "host": "127.0.0.1", "port": 0}
            )
        )
        gw = getattr(plugin2, "gateway", None)
        self.assertTrue(gw is None or not getattr(gw, "bound_port", 0))

    # -- 3c. a corrupt state file disables the plugin, never wiped --------
    async def test_corrupt_state_file_disables_plugin_without_wiping(self):
        os.environ["WORKBUDDY_MASTER_KEY"] = MASTER_KEY
        # 本地状态文件是插件唯一会解析的自家文件（凭据归 CLI 管，插件只读）。它坏了
        # 就得停：把坏文件当空状态吞下去，下一次写入会覆盖掉运维唯一能拿来排查的副本。
        before = self._corrupt_state()
        plugin = await self._make_plugin(**self._make_ready_plugin_config())

        self.assertFalse(plugin.ready)
        self.assertTrue(str(plugin.error_reason))
        # No session is left dangling and no listener was opened.
        self.assertIsNone(getattr(plugin, "session", None))
        self.assertIsNone(getattr(plugin, "gateway", None))
        self.assertEqual(self.up.extra_hits, 0)
        # The corrupt file is preserved -- never silently wiped.
        self.assertEqual(self._state_blob(), before)
        await plugin.terminate()

    # -- 4. authorization gate on commands --------------------------------
    async def test_group_or_non_admin_actions_are_rejected_without_upstream(self):
        # A genuinely logged-in, ready plugin: the runtime authorization gate
        # must still reject a management-group / non-admin command with zero
        # upstream work and without touching the stored credentials.
        self._seed_credentials()
        before = self._credentials_blob()
        plugin = await self._make_plugin(**self._make_ready_plugin_config())
        self.assertTrue(plugin.ready)
        # 启动只读本机清单（一次 --help），不碰任何 HTTP 上游；这条基线就是被拒
        # 命令不许越过的那条线。
        reads_before = self.up.extra_hits

        rejected = [
            _make_event("wb login", admin=True, private=False),
            _make_event("wb signin", admin=True, private=False),
            _make_event("wb tasks run", admin=True, private=False),
            _make_event("wb claim", admin=True, private=False),
            _make_event("wb logout", admin=False, private=True),
            _make_event("wb login", admin=False, private=True),
        ]
        for event in rejected:
            for action in ("login", "signin", "tasks run", "claim", "logout"):
                await _invoke(plugin.wb, event, action, "")

        # No upstream action was triggered at all, and credentials are untouched.
        self.assertEqual(self.up.extra_hits, reads_before)
        self.assertEqual(self._credentials_blob(), before)

        # help / status must not leak the key or raw credential state.
        event = _make_event("wb help")
        text = _text_of(await _invoke(plugin.wb, event, "help", ""))
        text += _text_of(await _invoke(plugin.wb, event, "status", ""))
        for secret in (
            GATEWAY_KEY,
            MASTER_KEY,
            SEED_CREDENTIALS["accessToken"],
            SEED_CREDENTIALS["refreshToken"],
        ):
            self.assertNotIn(secret, text)

    # -- 5. login flow: link + supersede ---------------------------------
    async def test_login_returns_link_and_repeated_login_supersedes_pending(self):
        plugin = await self._make_plugin(**self._make_ready_plugin_config())
        runtime = _StubAcpRuntime(block=True)
        plugin.acp = _StubAcpPool(runtime)
        event = _make_event("wb login")

        # The login link is the *yielded* handler result (normal AstrBot mode);
        # the plugin never forces a synchronous ``context.send_message``.
        first = await _invoke(plugin.wb, event, "login", "")
        first_text = _text_of(first)
        self.assertIn("https", first_text)
        first_task = plugin._login_task
        self.assertIsNotNone(first_task)
        self.assertEqual(runtime.calls, 1)

        # A second login while one is pending must supersede: the old task is
        # cancelled and a new authenticate really starts.
        second = await _invoke(plugin.wb, event, "login", "")
        self.assertIn("https", _text_of(second))
        self.assertEqual(runtime.calls, 2)
        self.assertIsNot(plugin._login_task, first_task)
        self.assertTrue(first_task.done())

        # Let the replacement finish so no task outlives the test.
        runtime._gate.set()
        await asyncio.wait_for(plugin._login_task, timeout=5)

    # -- 5b. logout revokes; manual signin allowed when auto is off -------
    async def test_logout_revokes_and_manual_signin_allowed_when_auto_disabled(self):
        os.environ["WORKBUDDY_MASTER_KEY"] = MASTER_KEY
        # 先在 CLI 凭据文件里放一份：退出登录必须把它清掉，与登录走的是哪条通道无关。
        self._seed_credentials()
        plugin = await self._make_plugin(**self._make_ready_plugin_config())
        pool = _StubAcpPool()
        plugin.acp = pool

        await _invoke(plugin.wb, _make_event("wb login"), "login", "")
        self.assertIsNotNone(plugin._login_task)
        await asyncio.wait_for(plugin._login_task, timeout=5)
        self.assertTrue(plugin.account.credentials)
        self.assertEqual(
            plugin.account.credentials.get("accessToken"),
            SEED_CREDENTIALS["accessToken"],
        )
        # 登录成功后整池重启：别的模型的进程记着"未登录"，不重启会一直 401。
        self.assertEqual(pool.close_alls, 1)

        await _invoke(plugin.wb, _make_event("wb logout"), "logout", "")
        await asyncio.sleep(0)
        self.assertEqual(pool.logouts, 1, "退出登录必须走池：先关全部进程，再删凭据")
        self.assertFalse(plugin.account.credentials)

        # A fresh manual instance must be re-seeded before ``initialize`` so its
        # explicit sign-in (with the auto switch off) really runs.
        self._seed_credentials()
        manual = await self._make_plugin(
            **self._make_ready_plugin_config(
                signin={"enabled": False, "timezone": "Asia/Shanghai"}
            )
        )
        self.assertFalse(manual.scheduler.status()["enabled"])
        await _invoke(manual.wb, _make_event("wb signin"), "signin", "")
        self.assertGreaterEqual(
            self.up.hits[("POST", CHECKIN_STATUS_PATH)] + self.up.extra_hits, 1
        )

        # No token may appear in the command output.
        text = _text_of(await _invoke(manual.wb, _make_event("wb status"), "status", ""))
        for secret in (
            SEED_CREDENTIALS["accessToken"],
            SEED_CREDENTIALS["refreshToken"],
            GATEWAY_KEY,
        ):
            self.assertNotIn(secret, text)

    # -- 6. the retired growth commands fall back to help ------------------
    async def test_retired_growth_commands_answer_help_and_touch_no_upstream(self):
        self._seed_credentials()
        plugin = await self._make_plugin(**self._make_ready_plugin_config())
        self.assertTrue(plugin.ready)

        before = self.up.total_hits()
        for action, rest in (("tasks", ""), ("tasks", "run"), ("tasks", "claim")):
            text = _text_of(await _invoke(plugin.wb, _make_event("wb tasks"), action, rest))
            # 成长任务已下线：任何 tasks 变体都只回帮助，不读上游、不起后台。
            self.assertIn("/wb help", text)
            self.assertNotIn("成长任务", text)
        self.assertEqual(self.up.total_hits(), before)
        self.assertFalse(hasattr(plugin, "growth"))

    # -- 7. real Provider -> real Gateway -> real ACP ----------------------
    async def test_real_provider_chat_and_stream_through_acp(self):
        plugin = await self._make_acp_plugin("echo_prompt")
        port = plugin.gateway.bound_port
        base = f"http://127.0.0.1:{port}/v1"

        provider = ProviderOpenAIOfficial(
            provider_config={
                "id": "wb-test",
                "type": "openai_chat_completion",
                "api_base": base,
                "key": [GATEWAY_KEY],
                "model": MODEL,
                "timeout": 15,
            },
            provider_settings={},
        )
        self._providers.append(provider)

        response = await provider.text_chat(
            prompt="真实中文提示词",
            contexts=[{"role": "user", "content": "历史上下文"}],
            system_prompt="You are persona X.",
            request_max_retries=1,
        )
        # 假 CLI 把收到的 prompt 原样回显：系统提示、历史、当前提问都在里面，
        # 说明它们真的穿过了网关和适配层，而不只是"看起来发出去了"。
        self.assertIn("You are persona X.", response.completion_text)
        self.assertIn("历史上下文", response.completion_text)
        self.assertIn("真实中文提示词", response.completion_text)

        # Streaming path through the same real Provider.
        collected = []
        async for part in provider.text_chat_stream(
            prompt="再问一次",
            contexts=[],
            system_prompt="You are persona X.",
            request_max_retries=1,
        ):
            collected.append(part)
        streamed = "".join(p.completion_text or "" for p in collected)
        self.assertIn("再问一次", streamed)

        # 聊天不再走私有 HTTP：上游一次都没被 POST 过。
        self.assertEqual(self.up.total_hits(), 0)

    # -- 8. shutdown owns every runtime resource --------------------------
    async def test_build_gateway_wires_the_backend_to_the_pool_and_the_alert_hook(self):
        """生产装配分支：chat 为空时由 ``_build_gateway`` 造后端并接好三样东西。

        这一条盯的是别的用例盯不到的地方：其余用例都在 ``initialize`` 之前塞好
        ``plugin.chat``，于是 ``_build_gateway`` 走的是"复用"分支。把 ``pool=``、
        ``model=`` 或 ``on_error=`` 从生产装配里删掉，除了这里没有一条会红。
        """
        from astrbot_plugin_workbuddy.core.acp import AcpChatBackend

        plugin = await self._make_plugin(
            **self._make_ready_plugin_config(acp={"model": MODEL})
        )
        self.assertTrue(plugin.ready)
        self.assertIsInstance(plugin.chat, AcpChatBackend)
        # 模型决定进程：装配时把 acp.model 交给后端，后端再交给池的 lease()。
        self.assertIs(plugin.chat.pool, plugin.acp)
        self.assertEqual(plugin.chat.model, MODEL)
        self.assertEqual(plugin.chat.on_error, plugin._on_chat_error)
        self.assertIs(plugin.account._runtime, plugin.acp.primary)

        # 运行时被 shutdown 之后永久不可用；旧后端留着，下一次 initialize 就会
        # 复用它，聊天从此恒 502。释放必须把两个一起丢掉。
        await plugin.terminate()
        self.assertIsNone(plugin.chat)
        self.assertIsNone(plugin.gateway)

    async def test_terminate_closes_provider_gateway_and_upstream_sockets(self):
        plugin = await self._make_plugin(**self._make_ready_plugin_config())
        port = plugin.gateway.bound_port
        provider = ProviderOpenAIOfficial(
            provider_config={
                "id": "wb-test",
                "type": "openai_chat_completion",
                "api_base": f"http://127.0.0.1:{port}/v1",
                "key": [GATEWAY_KEY],
                "model": MODEL,
                "timeout": 5,
            },
            provider_settings={},
        )
        self._providers.append(provider)

        await provider.terminate()
        await plugin.terminate()
        await self.up.stop()

        # The gateway port can no longer be reconnected to.
        with self.assertRaises(aiohttp.ClientError):
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://127.0.0.1:{port}/health"):
                    pass

        # The plugin owns no surviving background tasks.
        await asyncio.sleep(0)
        leftovers = [
            t
            for t in asyncio.all_tasks()
            if t is not asyncio.current_task()
            and not t.done()
            and "workbuddy" in repr(t.get_coro()).lower()
        ]
        self.assertFalse(leftovers)

    # -- 9. an account 401 never clears a server-forced hard cooldown -----
    async def test_login_success_clears_pause_but_keeps_hard_cooldown(self):
        import time as _time

        plugin = await self._make_plugin(
            **self._make_ready_plugin_config(signin={"enabled": False})
        )
        self.assertTrue(plugin.ready)
        plugin.acp = _StubAcpPool()

        # A real 429-style pause + hard cooldown recorded by the scheduler.
        cooldown = _time.time() + 300.0
        plugin.state.update("scheduler", {"paused": True, "cooldown_until": cooldown})
        original_scheduler = plugin.state.load()["scheduler"]["cooldown_until"]
        self.assertTrue(plugin.scheduler.status()["paused"])

        # A real login must lift the pause but never the server-forced cooldown.
        await _invoke(plugin.wb, _make_event("wb login"), "login", "")
        self.assertIsNotNone(plugin._login_task)
        await asyncio.wait_for(plugin._login_task, timeout=5)

        self.assertFalse(plugin.scheduler.status()["paused"])
        self.assertEqual(
            plugin.state.load()["scheduler"]["cooldown_until"], original_scheduler
        )

    # -- 10. an unlogged ACP session maps to a safe 401 -------------------
    async def test_unlogged_acp_session_maps_to_a_safe_401(self):
        # 假 CLI 的 ``auth_required`` 场景就是真实 CLI 未登录时的样子：
        # session/new 回 -32000 且 category=auth。
        plugin = await self._make_acp_plugin("auth_required")
        self.assertFalse(plugin.account.credentials)

        port = plugin.gateway.bound_port
        self.assertTrue(port)
        base = "http://127.0.0.1:{}".format(port)
        headers = {"Authorization": "Bearer " + GATEWAY_KEY}

        async with aiohttp.ClientSession() as session:
            for stream in (False, True):
                payload = {
                    "model": MODEL,
                    "messages": [{"role": "user", "content": "hi"}],
                    "stream": stream,
                }
                async with session.post(
                    base + "/v1/chat/completions", json=payload, headers=headers
                ) as resp:
                    self.assertEqual(resp.status, 401)
                    body = await resp.json()
                # 固定文案，绝不外传 ACP 的错误原文。
                self.assertIsInstance(body.get("error"), dict)
                self.assertEqual(body["error"]["message"], "upstream request failed")

        # 未登录的聊天不会去碰任何私有 HTTP 路由，也不会凭空造出凭据。
        self.assertEqual(self.up.total_hits(), 0)
        self.assertFalse(plugin.account.credentials)

    # -- 11. status overview is local-only -------------------------------
    async def test_status_reports_the_acp_runtime_without_reading_any_upstream(self):
        """``/wb status`` 现在是纯本地总览：一条 HTTP 都不该发出去。

        迁移前这一页要读模型清单、/v3/config 和余额三处；三处全下线了，模型清单
        改成读本机 CLI。于是这条用例的断言从"各读一次"变成"一次都不读"——更强，
        因为任何一处回潮都会让它红。
        """
        self._seed_credentials()
        plugin = await self._make_plugin(**self._make_ready_plugin_config())
        self.assertTrue(plugin.ready)
        self.assertEqual(self.up.total_hits(), 0, "启动本身不碰任何上游接口")

        text = _text_of(await _invoke(plugin.wb, _make_event("wb status"), "status", ""))

        self.assertIn("就绪：是", text)
        self.assertIn("ACP：0 条 CLI 进程在跑（上限 2）", text)
        self.assertIn("00:10", text)           # 签到定时
        self.assertIn("Asia/Shanghai", text)   # 时区
        self.assertIn("请求体上限：16 MiB", text)
        self.assertIn("主备路由：未启用", text)
        # 清单读不到时如实说读不到：不编一份模型名出来，也不让状态页崩掉。
        self.assertIn("当前没有可用模型", text)

        self.assertEqual(self.up.total_hits(), 0, "总览必须只读本地状态")
        # 任何带副作用的接口都绝不能被碰。
        self.assertEqual(self.up.hits[("POST", CHECKIN_STATUS_PATH)], 0)
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 0)

    # -- 14. unlogged / unauthorized status touches no upstream ---------
    async def test_status_without_login_or_permission_touches_no_upstream(self):
        # Unlogged: nothing to read.
        plugin = await self._make_plugin(**self._make_ready_plugin_config())
        self.assertFalse(plugin.account.credentials)
        await _invoke(plugin.wb, _make_event("wb status"), "status", "")
        self.assertEqual(self.up.total_hits(), 0)

        # Logged in, but a management-group and a non-admin event are both
        # rejected before any upstream work.
        self._seed_credentials()
        plugin2 = await self._make_plugin(**self._make_ready_plugin_config())
        self.assertTrue(plugin2.account.credentials)
        before = self.up.total_hits()
        for event in (
            _make_event("wb status", admin=True, private=False),
            _make_event("wb status", admin=False, private=True),
        ):
            await _invoke(plugin2.wb, event, "status", "")
        self.assertEqual(self.up.total_hits(), before)

    # -- 15. the list the provider sees is the local CLI's list ----------
    async def test_real_provider_lists_the_local_catalog_and_forwards_the_pure_id(self):
        """``/v1/models`` 的内容来自本机清单，网关交给进程的是同一个 id。

        迁移前这一条验的是"显示别名 → 上游 id"的翻译；别名随倍率一起下线了，
        现在模型 ID 本身就是进程的 ``--model`` 参数，中间不该再有任何改写。
        """
        plugin = await self._make_acp_plugin(
            "happy", wrap=_RecordingBackend, catalog=support.StubCatalog([MODEL])
        )
        port = plugin.gateway.bound_port
        base = f"http://127.0.0.1:{port}/v1"

        provider = ProviderOpenAIOfficial(
            provider_config={
                "id": "wb-test",
                "type": "openai_chat_completion",
                "api_base": base,
                "key": [GATEWAY_KEY],
                "model": MODEL,
                "timeout": 15,
            },
            provider_settings={},
        )
        self._providers.append(provider)

        models = await provider.get_models()
        self.assertEqual(models, [MODEL])

        response = await provider.text_chat(
            prompt="hi",
            contexts=[],
            system_prompt="You are persona X.",
            request_max_retries=1,
        )
        self.assertEqual(response.completion_text, "Hello world!")

        # 后端收到的模型与清单里的一模一样：中间没有任何别名/ID 翻译。
        self.assertEqual(plugin.chat.payloads[0]["model"], MODEL)

    # -- 16. status separates "auto disabled" from "paused" --------------
    async def test_status_distinguishes_disabled_from_paused(self):
        self._seed_credentials()
        # Default ready config has signin.enabled False -> auto is *disabled*,
        # which is a different state from a manually *paused* scheduler.
        plugin = await self._make_plugin(**self._make_ready_plugin_config())
        self.assertTrue(plugin.ready)

        text = _text_of(await _invoke(plugin.wb, _make_event("wb status"), "status", ""))
        self.assertIn("签到自动：未启用", text)
        self.assertIn("签到状态：正常", text)

        # The real scheduler has no pause(); a server-forced pause is recorded
        # in the shared state store, exactly as the live 429 path does.
        plugin.state.update("scheduler", {"paused": True})
        text = _text_of(await _invoke(plugin.wb, _make_event("wb status"), "status", ""))
        self.assertIn("签到自动：未启用", text)
        self.assertIn("签到状态：已暂停", text)

        # Auto enabled: stop the real background loop immediately, then pause.
        self._seed_credentials()
        auto = await self._make_plugin(
            **self._make_ready_plugin_config(signin={"enabled": True})
        )
        await auto.scheduler.stop()
        auto.state.update("scheduler", {"paused": True})
        text = _text_of(await _invoke(auto.wb, _make_event("wb status"), "status", ""))
        self.assertIn("签到自动：已启用", text)
        self.assertIn("签到状态：已暂停", text)

        # A status read never triggers any claim side effect.
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 0)


def tearDownModule() -> None:
    """Delete only this module's exact sandbox directory."""
    shutil.rmtree(_SESSION_ROOT, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
