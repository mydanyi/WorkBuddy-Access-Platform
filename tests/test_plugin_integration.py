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
* a real ``aiohttp`` fake *WorkBuddy upstream* on 127.0.0.1:0.

Before **any** ``astrbot`` / ``api`` / ``main`` import the header pins
``ASTRBOT_ROOT`` / ``TMPDIR`` / ``WORKBUDDY_TEST_TMP`` to a unique directory
under the NAS-hosted ``BASE_TMP`` (existing environment is preserved, never
wiped) so the real framework cannot create ``/AstrBot/data`` or leak
controllable files into the system temp dir.  ``tearDownModule`` deletes only
that exact directory.

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
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))
if str(_ASTRBOT_ROOT) not in sys.path:
    sys.path.insert(0, str(_ASTRBOT_ROOT))

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

# --- Plugin entry under test.  RED LIGHT until main.py is implemented. ---
import astrbot_plugin_workbuddy.main as wb_main  # noqa: E402

# Reuse the gateway suite's real frame helpers / class, but NOT its test case.
try:  # package discovery
    from tests.test_chat_gateway import (  # noqa: E402
        _FakeUpstream as _BaseUpstream,
        _basic_sse,
        MASTER_KEY,
        SEED_CREDENTIALS,
        MODEL,
    )
except ImportError:  # top-level discovery
    from test_chat_gateway import (  # noqa: E402
        _FakeUpstream as _BaseUpstream,
        _basic_sse,
        MASTER_KEY,
        SEED_CREDENTIALS,
        MODEL,
    )

# --- FICTIONAL FIXED SAMPLE values (never real). ---
GATEWAY_KEY = "FICTIONAL-PLUGIN-GATEWAY-KEY"
PLUGIN_NAME = "astrbot_plugin_workbuddy"

CHECKIN_STATUS_PATH = "/v2/billing/meter/checkin-activity-status"
CHECKIN_CLAIM_PATH = "/v2/billing/meter/daily-checkin"
GROWTH_LIST_PATH = "/v2/activity/growth/tasks"
ACCEPT_PATH = "/v2/activity/growth/tasks/accept"
CLAIM_ROUTE = "/activity/growth/tasks/{code}/claim"
REPORT_PATH = "/v2/report"
MODELS_CATALOG_PATH = "/console/enterprises/personal/models"
BALANCE_PATH = "/v2/billing/meter/get-user-resource"
V3_CONFIG_PATH = "/v3/config"
# OAuth endpoints the real ``AuthManager`` talks to: a fresh per-login ``state``,
# the pending/success token poll, and the account lookup that matches it.
STATE_PATH = "/v2/plugin/auth/state"
TOKEN_PATH = "/v2/plugin/auth/token"
ACCOUNT_PATH = "/v2/plugin/login/account"

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
    },
    "signin": {
        "enabled": True,
        "timezone": "Asia/Shanghai",
        "primary": "00:10",
        "recovery": "12:10",
        "jitter": 120,
        "max_attempts": 5,
    },
    "growth": {
        "auto_after_signin": False,
        "auto_claim": True,
        "max_chat_requests": 20,
        "max_retries": 2,
        "task_interval": 2,
        "poll_interval": 5,
        "progress_timeout": 120,
        "allowed_tasks": [],
        "allow_telemetry": True,
        "allow_theme_change": False,
        "allow_adoption": False,
    },
    "auth": {
        "master_key_env": "WORKBUDDY_MASTER_KEY",
        "login_timeout": 120,
        "poll_interval": 1,
    },
    "http": {
        "total_timeout": 180,
        "connect_timeout": 15,
        "sock_read_timeout": 120,
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


class _IntegrationUpstream(_BaseUpstream):
    """The real chat/refresh upstream plus the billing + growth routes the
    plugin's signin / growth services talk to.  Only the *external server* is
    extended; no plugin component is replaced."""

    def __init__(self) -> None:
        super().__init__()
        self.tasks = []
        self.models = [
            {"id": MODEL, "name": MODEL, "credits": 0.11},
            {"id": "glm-5.2", "name": "glm-5.2"},
        ]
        self.checkin = {"active": True, "today_checked_in": False}
        self.accept_calls = []
        self.claim_calls = []
        self.report_calls = []
        self._extra_hits = 0
        # Registrable external fixtures for the balance read and /v3/config;
        # tests may replace ``handlers`` entries or flip ``balance_status``.
        self.handlers = {}
        self.balance_status = 200
        self.v3 = {"models": [], "modelPromotions": []}
        self.handlers[("POST", BALANCE_PATH)] = self._balance_handler
        self.handlers[("GET", V3_CONFIG_PATH)] = self._v3_handler
        # Login state machine: ``begin_login`` mints a unique state; the token
        # poll is pending until the test flips ``token_ready`` on, and the
        # account route then names the account the current state authorized.
        self._login_seq = 0
        self._active_state = None
        self.token_ready = False
        # Test-only switch: when on, the *models catalog* and the *chat* route
        # answer a fixed 401 without touching the OAuth routes at all.
        self.reject_auth = False

    # -- external route handlers ------------------------------------------
    async def _dispatch(self, request):
        path = request.path
        # An unlogged account whose only available routes reject the caller:
        # the two chat-facing routes answer a fixed 401 business body, while
        # every OAuth route keeps working unchanged.
        if self.reject_auth and (
            (request.method == "GET" and path == MODELS_CATALOG_PATH)
            or (request.method == "POST" and path == "/v2/chat/completions")
        ):
            body = await request.read()
            self._record(request, body)
            return self._json(
                {
                    "code": 4010,
                    "msg": "FICTIONAL-UPSTREAM-AUTH",
                    "data": {"token": "FICTIONAL-LEAKED-TOKEN"},
                },
                status=401,
            )
        # Registrable external fixtures (balance read / /v3/config).
        handler = self.handlers.get((request.method, path))
        if handler is not None:
            body = await request.read()
            self._record(request, body)
            result = handler(request)
            if hasattr(result, "__await__"):
                result = await result
            return result
        if request.method == "POST" and path == STATE_PATH:
            body = await request.read()
            self._record(request, body)
            self._login_seq += 1
            state = "s-{}".format(self._login_seq)
            self._active_state = state
            return self._json(
                {
                    "code": 0,
                    "data": {
                        "state": state,
                        "authUrl": "https://login.invalid/{}".format(state),
                    },
                }
            )
        if request.method == "GET" and path == TOKEN_PATH:
            body = await request.read()
            self._record(request, body)
            if self.token_ready:
                return self._json(
                    {
                        "code": 0,
                        "data": {
                            "accessToken": SEED_CREDENTIALS["accessToken"],
                            "refreshToken": SEED_CREDENTIALS["refreshToken"],
                            "expiresIn": 3600,
                            "domain": SEED_CREDENTIALS["domain"],
                        },
                    }
                )
            # Still authorizing: the real endpoint reports a non-zero business
            # code with a "login ing" message while pending.
            return self._json({"code": 1001, "msg": "login ing"})
        if request.method == "GET" and path == ACCOUNT_PATH:
            body = await request.read()
            self._record(request, body)
            return self._json(
                {
                    "code": 0,
                    "data": {
                        "uid": SEED_CREDENTIALS["uid"],
                        "nickname": "T",
                    },
                }
            )
        if path in (
            CHECKIN_STATUS_PATH,
            CHECKIN_CLAIM_PATH,
            GROWTH_LIST_PATH,
            ACCEPT_PATH,
            REPORT_PATH,
            MODELS_CATALOG_PATH,
        ) or path.endswith("/claim"):
            body = await request.read()
            # Record per-path (for "queried exactly once" assertions) while
            # keeping the legacy aggregate counter other tests rely on.
            self._record(request, body)
            self._extra_hits += 1
            if path == CHECKIN_STATUS_PATH:
                return self._json({"code": 0, "data": dict(self.checkin)})
            if path == CHECKIN_CLAIM_PATH:
                self.checkin["today_checked_in"] = True
                return self._json({"code": 0, "data": {"credit": 10}})
            if path == GROWTH_LIST_PATH:
                return self._json({"code": 0, "data": {"tasks": list(self.tasks)}})
            if path == ACCEPT_PATH:
                return self._json({"code": 0, "data": {}})
            if path == REPORT_PATH:
                return self._json({"code": 0, "data": {}})
            if path == MODELS_CATALOG_PATH:
                return self._json(
                    {
                        "code": 0,
                        "data": {
                            "models": [
                                {
                                    "id": m["id"],
                                    "name": m["name"],
                                    **(
                                        {"credits": m["credits"]}
                                        if "credits" in m
                                        else {}
                                    ),
                                }
                                for m in self.models
                            ],
                            "agents": [
                                {
                                    "name": "cli",
                                    "models": [m["id"] for m in self.models],
                                }
                            ],
                        },
                    }
                )
            # /activity/growth/tasks/{code}/claim
            code = path.split("/")[-2] if path.endswith("/claim") else ""
            self.claim_calls.append(code)
            for task in self.tasks:
                if task.get("task_code") == code:
                    task["accept_status"] = "claimed"
            return self._json(
                {"code": 0, "data": {"claimed": True, "credit": 100, "energy": 5}}
            )
        return await super()._dispatch(request)

    def _record(self, request, body) -> None:
        """Record through the real base recorder's shape (state/token/account).

        ``_json`` is synchronous (handlers ``return self._json(...)``), so the
        OAuth routes record their own hit/call here, exactly matching the base
        upstream's ``(method, path)`` counter.
        """
        self.calls.append(
            {
                "method": request.method,
                "path": request.path,
                "query": dict(request.query),
                "headers": {k.lower(): v for k, v in request.headers.items()},
                "body": body,
            }
        )
        self.hits[(request.method, request.path)] += 1

    def _json(self, body, status=200, headers=None):
        from aiohttp import web

        # A real aiohttp JSON response; ``def`` (not ``async def``) because the
        # handlers ``return self._json(...)`` without awaiting it.
        return web.json_response(body, status=status, headers=headers or {})

    def _balance_handler(self, request):
        if self.balance_status != 200:
            return self._json(
                {"code": 5000, "msg": "FICTIONAL-BALANCE-ERROR"},
                status=self.balance_status,
            )
        return self._json(
            {
                "code": 0,
                "data": {
                    "Response": {
                        "Data": {
                            "Accounts": [
                                {"CapacitySize": 150, "CapacityRemain": 105, "CapacityUsed": 45}
                            ],
                            "TotalCount": 1,
                        }
                    }
                },
            }
        )

    def _v3_handler(self, request):
        return self._json({"code": 0, "data": dict(self.v3)})

    @property
    def extra_hits(self) -> int:
        return self._extra_hits

    def total_hits(self) -> int:
        return sum(self.hits.values()) + self._extra_hits


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


class _PluginIntegrationTestCase(unittest.IsolatedAsyncioTestCase):
    # -- lifecycle --------------------------------------------------------
    async def asyncSetUp(self) -> None:
        # Isolate per-test plugin data (real StarTools.get_data_dir location).
        shutil.rmtree(_PLUGIN_DATA, ignore_errors=True)
        os.environ["WORKBUDDY_MASTER_KEY"] = MASTER_KEY
        self.up = _IntegrationUpstream()
        await self.up.start()
        wb_main.CHAT_BASE = self.up.base
        wb_main.BILLING_BASE = self.up.base
        wb_main.WEB_BASE = self.up.base
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
        }
        return _deep_merge(base, overrides)

    def _seed_credentials(self) -> None:
        """Persist real, decryptable credentials *before* ``initialize``.

        Uses the real ``CredentialStore`` at the very directory the plugin
        resolves through ``StarTools.get_data_dir`` so a logged-in test drives
        the genuine credential path (not an invalid placeholder that merely
        forces ``ready=False``).
        """
        from astrbot.api.star import StarTools
        from astrbot_plugin_workbuddy.core.storage import CredentialStore

        data_dir = StarTools.get_data_dir(PLUGIN_NAME)
        CredentialStore(data_dir, MASTER_KEY).save(dict(SEED_CREDENTIALS))

    def _write_credentials(self) -> None:
        """Write invalid credential bytes (only for the decrypt-failure case)."""
        _PLUGIN_DATA.mkdir(parents=True, exist_ok=True)
        store_path = _PLUGIN_DATA / "credentials.enc"
        store_path.write_bytes(b"FICTIONAL-ENCRYPTED-CREDENTIAL-PLACEHOLDER")

    def _credentials_blob(self) -> str:
        blob = b""
        for name in ("credentials.enc", "credentials.json"):
            path = _PLUGIN_DATA / name
            if path.is_file():
                blob += path.read_bytes()
        return blob.decode("latin-1")

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

    # -- 3. key gate: missing / invalid never binds ------------------------
    async def test_missing_or_invalid_master_key_disables_listener_and_terminate_is_idempotent(self):
        # No master key at all -> safe, not ready, no listener.
        os.environ.pop("WORKBUDDY_MASTER_KEY", None)
        plugin = await self._make_plugin(
            **self._make_ready_plugin_config(
                gateway={"enabled": True, "api_key": GATEWAY_KEY, "host": "127.0.0.1", "port": 0}
            )
        )
        self.assertFalse(plugin.ready)
        self.assertTrue(str(plugin.error_reason))
        self.assertTrue(getattr(plugin, "gateway", None) in (None,))
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

        # Invalid master key behaves the same.
        os.environ["WORKBUDDY_MASTER_KEY"] = "not-base64-not-32-bytes"
        plugin2 = await self._make_plugin(**self._make_ready_plugin_config())
        self.assertFalse(plugin2.ready)
        await plugin2.terminate()

    # -- 3b. key gate: valid binds, empty api_key does not ----------------
    async def test_valid_key_binds_health_and_empty_api_key_disables_listener(self):
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

    # -- 3c. a corrupt credential file disables the plugin, never wiped ----
    async def test_corrupt_credential_file_disables_plugin_without_wiping(self):
        os.environ["WORKBUDDY_MASTER_KEY"] = MASTER_KEY
        # Invalid, undecryptable bytes: the only place ``_write_credentials``
        # (an invalid placeholder) is used.
        self._write_credentials()
        before = self._credentials_blob()
        plugin = await self._make_plugin(**self._make_ready_plugin_config())

        self.assertFalse(plugin.ready)
        self.assertTrue(str(plugin.error_reason))
        # No session is left dangling and no listener was opened.
        self.assertIsNone(getattr(plugin, "session", None))
        self.assertIsNone(getattr(plugin, "gateway", None))
        self.assertEqual(self.up.extra_hits, 0)
        # The corrupt file is preserved -- never silently wiped, so the operator
        # can recover the account.
        self.assertEqual(self._credentials_blob(), before)
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
        # An authenticated startup reads the model catalog once for the native
        # picker; that single read is the baseline the rejected commands must
        # not extend.
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
        self.assertEqual(self.up.hits[("POST", STATE_PATH)], 0)
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

    # -- 5. login flow: link + background poll + supersede ---------------
    async def test_login_returns_link_and_repeated_login_supersedes_pending(self):
        plugin = await self._make_plugin(**self._make_ready_plugin_config())
        event = _make_event("wb login")

        # The login link is the *yielded* handler result (normal AstrBot mode);
        # the plugin never forces a synchronous ``context.send_message``.
        first = await _invoke(plugin.wb, event, "login", "")
        first_text = _text_of(first)
        self.assertIn("https", first_text)
        first_state = plugin.auth._pending_state
        self.assertTrue(first_state)
        first_task = plugin._login_task
        self.assertIsNotNone(first_task)
        self.assertEqual(self.up.hits[("POST", STATE_PATH)], 1)

        # A second login while one is pending must supersede: the old poll is
        # cancelled and a *different* state is issued -- not merely a second
        # message sent.
        second = await _invoke(plugin.wb, event, "login", "")
        second_text = _text_of(second)
        self.assertIn("https", second_text)
        second_state = plugin.auth._pending_state
        self.assertTrue(second_state)
        self.assertNotEqual(first_state, second_state)
        self.assertEqual(self.up.hits[("POST", STATE_PATH)], 2)
        self.assertIsNot(plugin._login_task, first_task)
        self.assertTrue(first_task.done())

    # -- 5b. logout revokes; manual signin allowed when auto is off -------
    async def test_logout_revokes_and_manual_signin_allowed_when_auto_disabled(self):
        os.environ["WORKBUDDY_MASTER_KEY"] = MASTER_KEY
        # A complete token/account fixture lets the real background poll persist
        # credentials, proving the account is genuinely authorized before logout.
        self.up.token_ready = True
        plugin = await self._make_plugin(**self._make_ready_plugin_config())

        await _invoke(plugin.wb, _make_event("wb login"), "login", "")
        self.assertIsNotNone(plugin._login_task)
        await asyncio.wait_for(plugin._login_task, timeout=5)
        self.assertTrue(plugin.auth.credentials)
        self.assertEqual(
            plugin.auth.credentials.get("accessToken"),
            SEED_CREDENTIALS["accessToken"],
        )

        await _invoke(plugin.wb, _make_event("wb logout"), "logout", "")
        await asyncio.sleep(0)
        self.assertFalse(plugin.auth.credentials)

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

    # -- 6. tasks / claim / help semantics --------------------------------
    async def test_tasks_list_is_readonly_run_is_background_and_claim_targets_completed(self):
        self.up.tasks = [
            {"task_code": "done_task", "current": 3, "target": 3,
             "status": "complete", "accept_status": "accepted"},
            {"task_code": "partial_task", "current": 1, "target": 3,
             "accept_status": "accepted"},
        ]
        self._seed_credentials()
        plugin = await self._make_plugin(
            **self._make_ready_plugin_config(
                growth={
                    "auto_claim": False,
                    "task_interval": 0,
                    "poll_interval": 0.01,
                    "progress_timeout": 0.2,
                }
            )
        )
        self.assertTrue(plugin.ready)
        event = _make_event("wb tasks")

        # ``tasks`` (rest == "") is a pure read-only list: exactly one upstream
        # read and zero chat work.
        before_reads = self.up.extra_hits
        listing = _text_of(await _invoke(plugin.wb, event, "tasks", ""))
        self.assertIn("done_task", listing)
        self.assertIn("partial_task", listing)
        self.assertEqual(self.up.extra_hits - before_reads, 1)
        self.assertEqual(self.up.hits[("POST", "/v2/chat/completions")], 0)

        # ``tasks run`` starts a *real* background run; wait for it to settle.
        run_reply = _text_of(await _invoke(plugin.wb, event, "tasks", "run"))
        self.assertIn("已开始", run_reply)
        run_task = plugin.growth._task
        self.assertIsNotNone(run_task)
        await asyncio.wait_for(run_task, timeout=5)

        # ``tasks cancel`` cancels the (now finished) real background run.
        await _invoke(plugin.wb, event, "tasks", "cancel")

        # ``tasks claim`` claims only the completed task (auto_claim is off) and
        # awaits the real background run it started.
        await _invoke(plugin.wb, event, "tasks", "claim")
        claim_task = plugin.growth._task
        self.assertIsNotNone(claim_task)
        await asyncio.wait_for(claim_task, timeout=5)
        claimed = set(self.up.claim_calls)
        self.assertIn("done_task", claimed)
        self.assertNotIn("partial_task", claimed)
        self.assertEqual(self.up.hits[("POST", "/v2/chat/completions")], 0)

        unknown = _text_of(await _invoke(plugin.wb, _make_event("wb nonsense"), "nonsense", ""))
        self.assertIn("help", unknown.lower())

    # -- 7. real Provider -> real Gateway -> real upstream -----------------
    async def test_real_provider_chat_and_stream_through_the_gateway(self):
        plugin = await self._make_plugin(**self._make_ready_plugin_config())
        port = plugin.gateway.bound_port
        base = f"http://127.0.0.1:{port}/v1"

        self.up.queue_sse(_basic_sse("你好，来自网关", crlf=False))
        provider = ProviderOpenAIOfficial(
            provider_config={
                "id": "wb-test",
                "type": "openai_chat_completion",
                "api_base": base,
                "key": [GATEWAY_KEY],
                "model": MODEL,
                "timeout": 5,
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
        self.assertEqual(response.completion_text, "你好，来自网关")

        # The upstream saw the history / persona unrewritten.
        sent = self.up.chat_bodies()[0]
        self.assertEqual(sent["model"], MODEL)
        roles = [m["role"] for m in sent["messages"]]
        self.assertEqual(roles[0], "system")
        self.assertIn("You are persona X.", json.dumps(sent, ensure_ascii=False))
        self.assertIn("真实中文提示词", json.dumps(sent, ensure_ascii=False))

        # Streaming path through the same real Provider.
        self.up.queue_sse(_basic_sse("流式文本"))
        collected = []
        async for part in provider.text_chat_stream(
            prompt="再问一次",
            contexts=[],
            system_prompt="You are persona X.",
            request_max_retries=1,
        ):
            collected.append(part)
        streamed = "".join(p.completion_text or "" for p in collected)
        self.assertIn("流式文本", streamed)

        # At least two real Provider chats reached the gateway.
        self.assertGreaterEqual(self.up.hits[("POST", "/v2/chat/completions")], 2)

    # -- 8. shutdown owns every runtime resource --------------------------
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

        self.up.token_ready = True
        plugin = await self._make_plugin(
            **self._make_ready_plugin_config(signin={"enabled": False})
        )
        self.assertTrue(plugin.ready)

        # A real 429-style pause + hard cooldown recorded by both services.
        cooldown = _time.time() + 300.0
        plugin.state.update("scheduler", {"paused": True, "cooldown_until": cooldown})
        plugin.state.update("growth", {"paused": True, "cooldown_until": cooldown})
        original_scheduler = plugin.state.load()["scheduler"]["cooldown_until"]
        original_growth = plugin.state.load()["growth"]["cooldown_until"]
        self.assertTrue(plugin.scheduler.status()["paused"])
        self.assertTrue(plugin.growth.status()["paused"])

        # A real login must lift the pause but never the server-forced cooldown.
        await _invoke(plugin.wb, _make_event("wb login"), "login", "")
        self.assertIsNotNone(plugin._login_task)
        await asyncio.wait_for(plugin._login_task, timeout=5)

        self.assertFalse(plugin.scheduler.status()["paused"])
        self.assertFalse(plugin.growth.status()["paused"])
        self.assertEqual(
            plugin.state.load()["scheduler"]["cooldown_until"], original_scheduler
        )
        self.assertEqual(
            plugin.state.load()["growth"]["cooldown_until"], original_growth
        )

    # -- 10. an unlogged upstream 401 maps to a safe gateway error --------
    async def test_unlogged_upstream_401_maps_to_safe_gateway_error(self):
        self.up.reject_auth = True
        plugin = await self._make_plugin(**self._make_ready_plugin_config())
        self.assertTrue(plugin.ready)
        self.assertFalse(plugin.auth.credentials)

        port = plugin.gateway.bound_port
        self.assertTrue(port)
        base = "http://127.0.0.1:{}".format(port)
        headers = {"Authorization": "Bearer " + GATEWAY_KEY}
        raw_marker = "FICTIONAL-UPSTREAM-AUTH"

        async with aiohttp.ClientSession() as session:
            async with session.get(base + "/v1/models", headers=headers) as resp:
                self.assertEqual(resp.status, 401)
                body = await resp.json()
            self.assertIsInstance(body.get("error"), dict)
            self.assertNotIn(raw_marker, json.dumps(body))

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
                self.assertIsInstance(body.get("error"), dict)
                self.assertNotIn(raw_marker, json.dumps(body))

        # The missing-refresh-token path must never attempt a refresh nor mint
        # any credential.
        self.assertEqual(self.up.refresh_hits, 0)
        self.assertFalse(plugin.auth.credentials)

    # -- 11. growth status / task list render explicit Chinese phases -----
    async def test_growth_status_and_task_list_render_explicit_phases(self):
        sample = {
            "running": False,
            "chat_requests_today": 5,
            "max_chat_requests": 20,
            "last_result": {
                "actions": [
                    {"task_code": "chat_5", "action": "chat", "outcome": "completed"}
                ],
                "progress": {"chat_5": {"current": 5, "target": 5}},
                "rewards": [
                    {"task_code": "chat_5", "credit": None, "energy": None}
                ],
                "pending": [],
                "unconfirmed": [],
            },
        }
        text = wb_main.WorkBuddyPlugin._format_growth_status(sample)
        for token in ("动作", "进度", "5/5", "奖励", "已确认领取", "数值未知"):
            self.assertIn(token, text)

        listing = wb_main.WorkBuddyPlugin._format_task_list(
            [
                {
                    "task_code": "done_task",
                    "title": "示例任务",
                    "complete": True,
                    "accept_status": "claimed",
                    "progress": {"current": 5, "target": 5},
                }
            ]
        )
        self.assertIn("done_task", listing)
        self.assertIn("已领取", listing)

    # -- 12. status overview: balance + credits + signin + growth --------
    def _off_growth(self, **extra):
        growth = {"auto_after_signin": False, "auto_claim": False}
        growth.update(extra)
        return self._make_ready_plugin_config(growth=growth)

    async def test_status_shows_balance_and_overview_without_extra_upstream(self):
        self._seed_credentials()
        self.up.tasks = [
            {
                "task_code": "grow_task",
                "title": "成长任务",
                "current": 0,
                "target": 1,
                "progress": {"current": 0, "target": 1},
                "accept_status": "claimed",
            }
        ]
        plugin = await self._make_plugin(**self._off_growth())
        self.assertTrue(plugin.ready)
        # The authenticated startup already performed exactly one catalog read
        # (plus its /v3/config supplement) for the native model picker.
        catalog_reads = self.up.hits[("GET", MODELS_CATALOG_PATH)]
        v3_reads = self.up.hits[("GET", V3_CONFIG_PATH)]

        text = _text_of(await _invoke(plugin.wb, _make_event("wb status"), "status", ""))

        self.assertIn("剩余积分：105", text)
        self.assertIn("已用积分：45", text)
        self.assertIn("总额度：150", text)
        self.assertIn("wb-1 [0.11x]", text)
        self.assertIn("00:10", text)           # 签到定时
        self.assertIn("Asia/Shanghai", text)   # 时区
        self.assertIn("聊天", text)             # 成长聊天预算
        self.assertIn("0/1", text)             # 任务进度
        self.assertIn("已领取", text)

        # Read-only overview: model catalog / config / balance / tasks, once each
        # (the overview adds exactly one read on top of the startup read).
        self.assertEqual(self.up.hits[("GET", MODELS_CATALOG_PATH)], catalog_reads + 1)
        self.assertEqual(self.up.hits[("GET", V3_CONFIG_PATH)], v3_reads + 1)
        self.assertEqual(self.up.hits[("POST", BALANCE_PATH)], 1)
        self.assertEqual(self.up.hits[("GET", GROWTH_LIST_PATH)], 1)
        # No side-effecting route is ever touched.
        self.assertEqual(self.up.hits[("POST", "/v2/chat/completions")], 0)
        self.assertEqual(self.up.hits[("GET", CHECKIN_STATUS_PATH)], 0)
        self.assertEqual(self.up.hits[("POST", CHECKIN_STATUS_PATH)], 0)
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 0)
        self.assertEqual(self.up.hits[("POST", ACCEPT_PATH)], 0)
        self.assertEqual(self.up.hits[("POST", REPORT_PATH)], 0)
        self.assertEqual(self.up.claim_calls, [])

    # -- 13. a balance failure degrades only the balance block ----------
    async def test_status_survives_balance_failure_without_leaking(self):
        self._seed_credentials()
        self.up.balance_status = 500
        plugin = await self._make_plugin(**self._off_growth())

        text = _text_of(await _invoke(plugin.wb, _make_event("wb status"), "status", ""))

        # Model / signin / task blocks still render.
        self.assertIn("wb-1 [0.11x]", text)
        self.assertIn("00:10", text)
        self.assertIn("Asia/Shanghai", text)
        self.assertIn("聊天", text)
        # The raw upstream body and any token must never leak.
        self.assertNotIn("FICTIONAL-BALANCE-ERROR", text)
        self.assertNotIn(SEED_CREDENTIALS["accessToken"], text)
        self.assertNotIn(SEED_CREDENTIALS["refreshToken"], text)
        self.assertNotIn(GATEWAY_KEY, text)

    # -- 14. unlogged / unauthorized status touches no upstream ---------
    async def test_status_without_login_or_permission_touches_no_upstream(self):
        # Unlogged: nothing to read.
        plugin = await self._make_plugin(**self._off_growth())
        self.assertFalse(plugin.auth.credentials)
        await _invoke(plugin.wb, _make_event("wb status"), "status", "")
        self.assertEqual(self.up.total_hits(), 0)

        # Logged in, but a management-group and a non-admin event are both
        # rejected before any upstream work.
        self._seed_credentials()
        plugin2 = await self._make_plugin(**self._off_growth())
        self.assertTrue(plugin2.auth.credentials)
        before = self.up.total_hits()
        for event in (
            _make_event("wb status", admin=True, private=False),
            _make_event("wb status", admin=False, private=True),
        ):
            await _invoke(plugin2.wb, event, "status", "")
        self.assertEqual(self.up.total_hits(), before)

    # -- 15. real Provider lists the alias and maps it back -------------
    async def test_real_provider_lists_alias_and_maps_back_on_completion(self):
        self._seed_credentials()
        plugin = await self._make_plugin(**self._off_growth())
        port = plugin.gateway.bound_port
        base = f"http://127.0.0.1:{port}/v1"

        self.up.queue_sse(_basic_sse("你好，来自网关"))
        provider = ProviderOpenAIOfficial(
            provider_config={
                "id": "wb-test",
                "type": "openai_chat_completion",
                "api_base": base,
                "key": [GATEWAY_KEY],
                "model": "wb-1 [0.11x]",
                "timeout": 5,
            },
            provider_settings={},
        )
        self._providers.append(provider)

        models = await provider.get_models()
        self.assertIn("wb-1 [0.11x]", models)

        response = await provider.text_chat(
            prompt="hi",
            contexts=[],
            system_prompt="You are persona X.",
            request_max_retries=1,
        )
        self.assertTrue(response.completion_text)

        # The gateway maps the display alias back to the pure upstream id.
        sent = self.up.chat_bodies()[0]
        self.assertEqual(sent["model"], "wb-1")

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

        # A status read never triggers any claim / accept side effect.
        self.assertEqual(self.up.claim_calls, [])
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 0)
        self.assertEqual(self.up.hits[("POST", ACCEPT_PATH)], 0)


def tearDownModule() -> None:
    """Delete only this module's exact sandbox directory."""
    shutil.rmtree(_SESSION_ROOT, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
