"""Real-plugin integration tests for admin-QQ notifications.

Scope: the notification path that starts from the **real** plugin runtime and
from the **final upstream errors** the plugin observes -- never by calling
``AdminNotifier`` by hand.  The unit contract of ``core.notifications`` and of
``core.config.load_settings`` lives in ``tests/test_admin_notifications.py``;
this module proves the *wiring*: the real plugin builds a notifier from the real
``notifications`` settings, routes scheduler / growth results and final upstream
errors to it, reports it through ``wb status``, and can be torn down mid-send.

Contract exercised here -- the real plugin wiring this module asserts:

* config section ``notifications``: ``enabled`` (default ``False``),
  ``signin_success``, ``errors``, ``admin_qq``, ``platform_id``,
  ``error_cooldown_seconds``;
* the plugin exposes ``plugin.notifier`` with the ``AdminNotifier`` contract
  (``await signin(result)`` / ``await error(category)`` / ``status()``);
* a successful real check-in (automatic or manual) notifies the configured QQ,
  not the user who ran the command, and reads the balance exactly once for the
  message (``remaining``, never ``total``);
* final upstream errors are forwarded through the ``UpstreamClient(...,
  on_error=cb)`` observer and through the scheduler/growth ``on_result`` hooks;
* observer categories are the fixed strings ``auth`` / ``permission`` / ``rate``
  / ``network`` / ``upstream`` -- never a raw body or token;
* a cancelled send writes nothing and the runtime is fully drained on
  ``terminate`` / re-``initialize``.

Only the external boundaries are replaced: ``Context.send_message`` (recording,
can return ``False`` / raise / block), the platform instance list
(``context.platform_manager.platform_insts`` with real ``PlatformMetadata``),
and the real ``aiohttp`` fake upstream.  Everything else -- ``WorkBuddyPlugin``,
``SigninScheduler`` / ``SigninService`` / ``UpstreamClient`` / ``AuthManager`` /
``StateStore`` / ``CredentialStore`` -- is the real production code.

``tests.test_plugin_integration`` is imported as ``support`` to initialize the
controlled AstrBot root; its test-case classes are deliberately not inherited or
imported, so this module never re-runs the integration suite.  Every credential
/ token / QQ below is a FICTIONAL FIXED SAMPLE.
"""

import asyncio
import contextlib
import shutil
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

import aiohttp
from aiohttp import web

# ---------------------------------------------------------------------------
# Make ``tests.test_plugin_integration`` importable in both discovery modes,
# then import it as a support module (it pins ASTRBOT_ROOT / TMPDIR /
# WORKBUDDY_TEST_TMP before any astrbot import).
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve()
_PLUGIN_ROOT = _HERE.parents[1]          # .../astrbot_plugin_workbuddy
_TESTS_DIR = _HERE.parent                # .../astrbot_plugin_workbuddy/tests
_SOURCE_ROOT = _HERE.parents[2]          # directory containing the plugin pkg
for _path in (str(_PLUGIN_ROOT), str(_TESTS_DIR), str(_SOURCE_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

try:  # package discovery (``tests`` is a real package)
    import tests.test_plugin_integration as support
except ImportError:  # top-level discovery
    import test_plugin_integration as support  # type: ignore

# --- Real framework imports (sandbox already pinned by ``support``). ---
from astrbot.core.platform.platform_metadata import PlatformMetadata  # noqa: E402
from astrbot.core.platform.message_type import MessageType  # noqa: E402
from astrbot.api.event import MessageChain  # noqa: E402
from astrbot.core.platform.message_session import MessageSession  # noqa: E402

import astrbot_plugin_workbuddy.main as wb_main  # noqa: E402
from astrbot_plugin_workbuddy.core.auth import AuthManager  # noqa: E402
from astrbot_plugin_workbuddy.core.client import UpstreamClient  # noqa: E402
from astrbot_plugin_workbuddy.core.errors import UpstreamError  # noqa: E402
from astrbot_plugin_workbuddy.core.storage import CredentialStore  # noqa: E402

# --- FICTIONAL FIXED SAMPLE values (never real). ---
ADMIN_QQ = "123456789"                   # fictional receiving QQ
PLATFORM_ID = "qq-notify-test"           # fictional aiocqhttp instance id
NEW_AT = "FICTIONAL-REFRESHED-ACCESS-TOKEN"
NEW_RT = "FICTIONAL-REFRESHED-REFRESH-TOKEN"
REFRESH_PATH = "/v2/plugin/auth/token/refresh"
PROTECTED = "/v2/protected/probe"

VALID_CATEGORIES = ("auth", "permission", "rate", "network", "upstream")

# A module-scoped sandbox, strictly under the task tmp root provided by support.
_MODULE_TMP = Path(
    tempfile.mkdtemp(prefix="wb-notify-int-", dir=str(support.BASE_TMP))
)


# ---------------------------------------------------------------------------
# External boundaries (replaced) + small fixtures.
# ---------------------------------------------------------------------------
class _Instance:
    """A fake platform instance whose ``meta()`` is a real ``PlatformMetadata``."""

    def __init__(self, instance_id, name="aiocqhttp"):
        self._id = instance_id
        self._name = name

    def meta(self):
        return PlatformMetadata(name=self._name, description="test", id=self._id)


class _PlatformManager:
    def __init__(self, instances):
        self.platform_insts = list(instances)


class _NotifyContext(support._Context):
    """The plugin's framework boundary with a controllable ``send_message``.

    Inherits the real ``get_config`` / ``get_all_stars`` / ``get_registered_star``
    boundary from ``support._Context`` and adds the platform instance list, so
    the plugin's own routing code runs unchanged.  ``send_message`` can be
    counted, forced to ``False``, made to raise, or blocked on an ``asyncio.Event``
    for lifecycle tests.
    """

    def __init__(self, config, instances, send_result=True, send_error=None, gate=None):
        super().__init__(config)
        self.platform_manager = _PlatformManager(instances)
        self.send_result = send_result
        self.send_error = send_error
        self.gate = gate
        self.send_calls = 0
        self.sent = []

    async def send_message(self, session, message_chain):
        self.send_calls += 1
        if self.gate is not None:
            await self.gate.wait()
        if self.send_error is not None:
            raise self.send_error
        self.sent.append((session, message_chain))
        return self.send_result


# ---------------------------------------------------------------------------
# Reusable external route handlers for the fake upstream.
# ---------------------------------------------------------------------------
def _refresh_success(request):
    return web.json_response(
        {
            "code": 0,
            "data": {
                "accessToken": NEW_AT,
                "refreshToken": NEW_RT,
                "expiresIn": 3600,
                "domain": "example.test.invalid",
            },
        }
    )


def _refresh_failure(request):
    return web.json_response({"code": 5000, "msg": "REFRESH-FAILED-RAW"}, status=500)


def _always_401(request):
    return web.json_response({"code": 4010, "msg": "AUTH-RAW-SECRET"}, status=401)


def _bearer_401_then_ok(request):
    if request.headers.get("Authorization") == "Bearer " + NEW_AT:
        return web.json_response({"code": 0, "data": {"ok": True}})
    return web.json_response({"code": 1001, "msg": "expired"}, status=401)


def _status_handler(status, body=None, headers=None):
    def handler(request):
        return web.json_response(body or {}, status=status, headers=headers or {})

    return handler


class _NotificationIntegrationTestCase(unittest.IsolatedAsyncioTestCase):
    """Drives the real plugin; binds the shared fixtures from ``support``.

    The ``support`` class is *not* inherited (that would re-run its tests); only
    the individual fixture functions are bound, plus a local ``_make_plugin``
    that swaps in the notification-aware context.
    """

    async def asyncSetUp(self) -> None:
        await support._PluginIntegrationTestCase.asyncSetUp(self)
        self._contexts = []
        self._temp_dirs = []

    async def asyncTearDown(self) -> None:
        for session in getattr(self, "_extra_sessions", []):
            with contextlib.suppress(Exception):
                await session.close()
        for directory in self._temp_dirs:
            shutil.rmtree(directory, ignore_errors=True)
        await support._PluginIntegrationTestCase.asyncTearDown(self)

    # -- bound support fixtures ------------------------------------------
    def _build_config(self, config_path, **overrides):
        return support._PluginIntegrationTestCase._build_config(
            self, config_path, **overrides
        )

    def _ensure_metadata_name(self):
        return support._PluginIntegrationTestCase._ensure_metadata_name(self)

    def _seed_credentials(self):
        return support._PluginIntegrationTestCase._seed_credentials(self)

    def _ready_config(self, **overrides):
        return support._PluginIntegrationTestCase._make_ready_plugin_config(
            self, **overrides
        )

    # -- factories --------------------------------------------------------
    async def _make_plugin(
        self,
        *,
        initialize=True,
        instances=None,
        send_result=True,
        send_error=None,
        gate=None,
        **overrides,
    ):
        cfg_path = support._SESSION_ROOT / ("cfg-" + uuid.uuid4().hex + ".json")
        config = self._build_config(cfg_path, **overrides)
        context = _NotifyContext(
            config,
            instances if instances is not None else [_Instance(PLATFORM_ID)],
            send_result=send_result,
            send_error=send_error,
            gate=gate,
        )
        self._ensure_metadata_name()
        plugin = wb_main.WorkBuddyPlugin(context, config)
        self._contexts.append(context)
        self._plugins.append(plugin)
        if initialize:
            await plugin.initialize()
        return plugin, context

    def _temp_dir(self) -> Path:
        directory = Path(
            tempfile.mkdtemp(prefix="wb-notify-data-", dir=str(support.BASE_TMP))
        )
        self._temp_dirs.append(directory)
        return directory

    @staticmethod
    def _reset_plugin_data():
        """Drop the real plugin data dir so a rebuilt plugin starts unsigned.

        Only this test module's isolated location (already removed by the
        ``support`` fixtures at setup/teardown) is touched.
        """
        shutil.rmtree(support._PLUGIN_DATA, ignore_errors=True)

    # -- driving / waiting helpers ---------------------------------------
    async def _run_wb(self, plugin, event, action, rest=""):
        """Drive the real ``wb`` entry point with a bounded timeout.

        A timeout turns "the command blocked on the notification send" into a
        fast, explicit failure instead of hanging the whole suite.
        """
        return await asyncio.wait_for(
            support._invoke(plugin.wb, event, action, rest), timeout=5
        )

    async def _wait_until(self, predicate, timeout=5.0):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not predicate():
            if loop.time() >= deadline:
                return False
            await asyncio.sleep(0.01)
        return True

    def _pending(self, plugin):
        """Pending plugin-owned notification tasks (best effort, no mocks)."""
        tasks = getattr(plugin, "_notify_tasks", None)
        if tasks is not None:
            return [t for t in list(tasks) if not t.done()]
        current = asyncio.current_task()
        return [
            t
            for t in asyncio.all_tasks()
            if t is not current
            and not t.done()
            and "notify" in (t.get_name() or "").lower()
        ]

    async def _wait_idle(self, plugin, timeout=5.0):
        return await self._wait_until(lambda: not self._pending(plugin), timeout)

    @staticmethod
    def _delivery(plugin):
        notifier = getattr(plugin, "notifier", None)
        if notifier is None:
            return None
        try:
            return notifier.status().get("last_delivery")
        except Exception:  # noqa: BLE001 - an unusable status is "no delivery"
            return None

    async def _enable_notify_plugin(self, **config_overrides):
        """A ready, logged-in plugin with notifications fully enabled."""
        self._seed_credentials()
        notify = {
            "enabled": True,
            "signin_success": True,
            "errors": True,
            "admin_qq": ADMIN_QQ,
            "platform_id": "",
            "error_cooldown_seconds": 60,
        }
        notify.update(config_overrides.pop("notifications", {}))
        overrides = self._ready_config(signin={"enabled": False}, **config_overrides)
        overrides = support._deep_merge(overrides, {"notifications": notify})
        return await self._make_plugin(**overrides)

    # =====================================================================
    # 1. notifications off: no QQ send, no extra balance read; the legacy
    #    daily growth callback is preserved.
    # =====================================================================
    async def test_01_default_off_sends_no_qq_and_keeps_growth_callback(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(
            **support._deep_merge(
                self._ready_config(
                    signin={"enabled": False},
                    growth={
                        "auto_after_signin": True,
                        "auto_claim": False,
                        "task_interval": 0,
                        "poll_interval": 0.01,
                        "progress_timeout": 0.05,
                    },
                ),
                {"notifications": {"enabled": False}},
            )
        )
        self.assertTrue(plugin.ready)
        # The switch is off by default.
        self.assertIs(plugin._settings["notifications"]["enabled"], False)

        self.up.tasks = []
        reply = support._text_of(
            await self._run_wb(plugin, support._make_event("wb signin"), "signin", "")
        )
        self.assertTrue(reply.strip())

        # The real check-in reached the upstream...
        self.assertGreaterEqual(self.up.hits[("POST", support.CHECKIN_STATUS_PATH)], 1)
        # ...but nothing was sent and the balance was never queried for a message.
        await self._wait_idle(plugin)
        self.assertEqual(ctx.send_calls, 0)
        self.assertEqual(self.up.hits[("POST", support.BALANCE_PATH)], 0)

        # The pre-existing daily-growth callback still fired (auto_after_signin).
        growth_task = plugin.growth._task
        self.assertIsNotNone(growth_task)
        await asyncio.wait_for(growth_task, timeout=5)

    # =====================================================================
    # 2. fully on: a real check-in notifies the configured QQ (not the
    #    caller) with reward + remaining; repeat / rebuild dedup.
    # =====================================================================
    async def test_02_enabled_signin_notifies_admin_qq_once_and_dedups(self):
        plugin, ctx = await self._enable_notify_plugin()
        self.assertTrue(plugin.ready)

        await self._run_wb(plugin, support._make_event("wb signin"), "signin", "")
        ok = await self._wait_until(lambda: ctx.send_calls >= 1)
        self.assertTrue(ok, "no QQ notification after a real sign-in")
        await self._wait_idle(plugin)

        self.assertEqual(ctx.send_calls, 1)
        session, chain = ctx.sent[0]
        self.assertIsInstance(session, MessageSession)
        self.assertIsInstance(chain, MessageChain)
        self.assertEqual(session.platform_name, PLATFORM_ID)
        self.assertEqual(session.session_id, ADMIN_QQ)
        self.assertEqual(session.message_type, MessageType.FRIEND_MESSAGE)
        # The notification targets the configured QQ, never the command user.
        self.assertNotEqual(session.session_id, "wb-test-user")

        text = support._result_text(chain)
        self.assertIn("10", text)       # this round's reward
        self.assertIn("105", text)      # balance remaining
        self.assertNotIn("150", text)   # never the total quota
        self.assertEqual(self.up.hits[("POST", support.BALANCE_PATH)], 1)

        # A repeated manual check-in the same day must not send again.
        await self._run_wb(plugin, support._make_event("wb signin"), "signin", "")
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 1)

        # Rebuilding the plugin over the same StateStore stays deduped.
        rebuilt, rebuilt_ctx = await self._enable_notify_plugin()
        await self._run_wb(rebuilt, support._make_event("wb signin"), "signin", "")
        await self._wait_idle(rebuilt)
        await asyncio.sleep(0.05)
        self.assertEqual(rebuilt_ctx.send_calls, 0)

    # =====================================================================
    # 3. wb status overview includes the notification state; a rejected
    #    (group / non-admin) status leaks nothing and touches no upstream.
    # =====================================================================
    async def test_03_status_overview_exposes_notifications_and_gate(self):
        plugin, ctx = await self._enable_notify_plugin()
        await self._run_wb(plugin, support._make_event("wb signin"), "signin", "")
        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
        await self._wait_idle(plugin)

        text = support._text_of(
            await self._run_wb(plugin, support._make_event("wb status"), "status", "")
        )
        self.assertIn("通知", text)
        self.assertIn(ADMIN_QQ, text)
        self.assertIn(PLATFORM_ID, text)
        self.assertIn("发送", text)  # "最近发送结果"
        for secret in (
            support.GATEWAY_KEY,
            support.SEED_CREDENTIALS["accessToken"],
            support.SEED_CREDENTIALS["refreshToken"],
        ):
            self.assertNotIn(secret, text)

        # Group / non-admin status is rejected before any upstream work.
        before = self.up.total_hits()
        for event in (
            support._make_event("wb status", admin=True, private=False),
            support._make_event("wb status", admin=False, private=True),
        ):
            rejected = support._text_of(await self._run_wb(plugin, event, "status", ""))
            self.assertNotIn(ADMIN_QQ, rejected)
            self.assertNotIn(PLATFORM_ID, rejected)
        self.assertEqual(self.up.total_hits(), before)

    # =====================================================================
    # 4. balance upstream 500: the sign-in stays verified, the message shows
    #    an unknown balance, and the internal read never recurses.
    # =====================================================================
    async def test_04_balance_failure_keeps_signin_and_never_recurses(self):
        plugin, ctx = await self._enable_notify_plugin()
        self.up.balance_status = 500

        reply = support._text_of(
            await self._run_wb(plugin, support._make_event("wb signin"), "signin", "")
        )
        self.assertTrue(reply.strip())
        self.assertIn(
            plugin.scheduler.status()["last_outcome"], ("signed", "already_signed")
        )

        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)

        # Exactly one message (the sign-in result) -- the balance failure must
        # not create a second error notification.
        self.assertEqual(ctx.send_calls, 1)
        text = support._result_text(ctx.sent[0][1])
        self.assertIn("未知", text)
        self.assertNotIn("FICTIONAL-BALANCE-ERROR", text)
        self.assertEqual(self.up.hits[("POST", support.BALANCE_PATH)], 1)

    # =====================================================================
    # 5. final auth failures notify from a real JSON/model query; a rescued
    #    401, an unlogged plugin and an active logout do not.
    # =====================================================================
    async def test_05_final_auth_failure_notifies_but_rescued_and_logout_do_not(self):
        # (a) Unlogged: the models command short-circuits, no observer, no send.
        unlogged, unlogged_ctx = await self._make_plugin(
            **support._deep_merge(
                self._ready_config(signin={"enabled": False}),
                {
                    "notifications": {
                        "enabled": True,
                        "admin_qq": ADMIN_QQ,
                        "error_cooldown_seconds": 60,
                    }
                },
            )
        )
        self.assertFalse(unlogged.auth.credentials)
        await self._run_wb(unlogged, support._make_event("wb models"), "models", "")
        await self._wait_idle(unlogged)
        self.assertEqual(unlogged_ctx.send_calls, 0)

        # (b) Active logout must not notify.
        self._seed_credentials()
        logged_out, logged_out_ctx = await self._enable_notify_plugin()
        await self._run_wb(logged_out, support._make_event("wb logout"), "logout", "")
        await self._wait_idle(logged_out)
        self.assertEqual(logged_out_ctx.send_calls, 0)

        # (c) A final 401 (refresh cannot rescue) from a real model query.
        self._seed_credentials()
        plugin, ctx = await self._enable_notify_plugin()
        self.up.handlers[("POST", REFRESH_PATH)] = _refresh_failure
        self.up.handlers[("GET", support.MODELS_CATALOG_PATH)] = _always_401

        await self._run_wb(plugin, support._make_event("wb models"), "models", "")
        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
        await self._wait_idle(plugin)
        self.assertEqual(ctx.send_calls, 1)
        self.assertEqual(self.up.hits[("POST", REFRESH_PATH)], 1)  # exactly one refresh
        text = support._result_text(ctx.sent[0][1])
        self.assertTrue(text.strip())
        self.assertNotIn("AUTH-RAW-SECRET", text)

        # (d) A 401 that a successful refresh rescues must not notify.
        self._seed_credentials()
        rescued, rescued_ctx = await self._enable_notify_plugin()
        self.up.handlers[("POST", REFRESH_PATH)] = _refresh_success
        self.up.handlers[("GET", support.MODELS_CATALOG_PATH)] = _bearer_401_then_ok

        await self._run_wb(rescued, support._make_event("wb models"), "models", "")
        await self._wait_idle(rescued)
        await asyncio.sleep(0.05)
        self.assertEqual(rescued_ctx.send_calls, 0)
        self.assertGreaterEqual(self.up.hits[("POST", REFRESH_PATH)], 1)

    # =====================================================================
    # 6. final chat 401 / 403 / 429 / 5xx notify from the real chat path
    #    (no real chat conversation; a fake SSE/HTTP upstream is enough).
    # =====================================================================
    async def test_06_chat_path_final_errors_notify(self):
        plugin, ctx = await self._enable_notify_plugin()
        self.up.handlers[("POST", REFRESH_PATH)] = _refresh_failure

        cases = (
            (401, None),
            (403, None),
            (429, {"Retry-After": "7"}),
            (500, None),
        )
        expected = 0
        for status, headers in cases:
            self.up.queue_status(
                status, {"code": 9, "msg": "RAW-CHAT-SECRET"}, headers
            )
            with contextlib.suppress(Exception):
                await plugin.chat.complete(
                    {"model": support.MODEL, "messages": [{"role": "user", "content": "hi"}]}
                )
            expected += 1
            self.assertTrue(
                await self._wait_until(lambda: ctx.send_calls >= expected),
                f"no notification after chat status {status}",
            )
            await self._wait_idle(plugin)

        self.assertEqual(ctx.send_calls, expected)
        for _, chain in ctx.sent:
            text = support._result_text(chain)
            self.assertTrue(text.strip())
            self.assertNotIn("RAW-CHAT-SECRET", text)

    # =====================================================================
    # 7. unconfirmed sign-in and a needs_attention growth run send a fixed
    #    safe error message; a normal success / cancel never does.
    # =====================================================================
    async def test_07_unconfirmed_and_needs_attention_notify_success_cancel_do_not(self):
        plugin, ctx = await self._enable_notify_plugin(
            growth={
                "auto_claim": False,
                "task_interval": 0,
                "poll_interval": 0.01,
                "progress_timeout": 0.05,
            }
        )

        # (a) A real sign-in whose claim reports a business error is unconfirmed.
        self.up.handlers[("POST", support.CHECKIN_CLAIM_PATH)] = _status_handler(
            200, {"code": 10000, "msg": "RAW-CLAIM-SECRET"}
        )
        await self._run_wb(plugin, support._make_event("wb signin"), "signin", "")
        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
        await self._wait_idle(plugin)
        self.assertEqual(plugin.scheduler.status()["last_outcome"], "unconfirmed")
        first = support._result_text(ctx.sent[0][1])
        self.assertTrue(first.strip())
        self.assertNotIn("RAW-CLAIM-SECRET", first)

        # (b) A real growth run whose listing fails is needs_attention.
        fail = {"on": True}

        def _growth_list(request):
            if fail["on"]:
                return web.json_response(
                    {"code": 4030, "msg": "RAW-GROWTH-SECRET"}, status=403
                )
            return web.json_response({"code": 0, "data": {"tasks": list(self.up.tasks)}})

        self.up.handlers[("GET", support.GROWTH_LIST_PATH)] = _growth_list
        self.assertTrue(plugin.growth.start().get("started"))
        await asyncio.wait_for(plugin.growth._task, timeout=5)
        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 2))
        await self._wait_idle(plugin)
        self.assertEqual(ctx.send_calls, 2)
        for _, chain in ctx.sent:
            self.assertNotIn("RAW-GROWTH-SECRET", support._result_text(chain))

        # (c) A normal, successful growth run does not notify.
        fail["on"] = False
        plugin.growth.resume()
        self.up.tasks = []
        self.assertTrue(plugin.growth.start().get("started"))
        await asyncio.wait_for(plugin.growth._task, timeout=5)
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 2)

        # (d) A normal cancel does not notify.
        await plugin.growth.cancel()
        await self._wait_idle(plugin)
        self.assertEqual(ctx.send_calls, 2)

    # =====================================================================
    # 8. send_message returning False records a failed delivery but never
    #    changes the sign-in success.
    # =====================================================================
    async def test_08_send_failure_is_recorded_without_changing_signin(self):
        plugin, ctx = await self._enable_notify_plugin()
        ctx.send_result = False

        reply = support._text_of(
            await self._run_wb(plugin, support._make_event("wb signin"), "signin", "")
        )
        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
        await self._wait_idle(plugin)

        # The sign-in itself still succeeded and was reported to the user.
        self.assertIn(
            plugin.scheduler.status()["last_outcome"], ("signed", "already_signed")
        )
        self.assertTrue(reply.strip())
        # The delivery is recorded as failed, not as a success.
        delivery = self._delivery(plugin)
        self.assertIsNotNone(delivery)
        self.assertEqual(delivery["outcome"], "failed")
        self.assertTrue(delivery["reason"])

    # =====================================================================
    # 9. terminate / re-initialize cancel and await the send tasks, drain the
    #    queue, and a cancelled send writes nothing.
    # =====================================================================
    async def test_09_lifecycle_cancels_awaits_and_drains_send_tasks(self):
        # (a) terminate cancels and awaits a blocked send.
        self._reset_plugin_data()
        gate = asyncio.Event()
        plugin, ctx = await self._enable_notify_plugin()
        ctx.gate = gate
        before = {t for t in asyncio.all_tasks() if t is not asyncio.current_task()}

        await self._run_wb(plugin, support._make_event("wb signin"), "signin", "")
        self.assertTrue(
            await self._wait_until(lambda: ctx.send_calls >= 1 and self._pending(plugin))
        )
        self.assertEqual(ctx.sent, [])  # blocked before it could write
        blocked = self._pending(plugin)
        self.assertTrue(blocked)
        notifier = getattr(plugin, "notifier", None)

        await plugin.terminate()
        await asyncio.sleep(0)
        for task in blocked:
            self.assertTrue(task.done(), "blocked send survived terminate()")
        leaked = {
            t
            for t in asyncio.all_tasks()
            if t is not asyncio.current_task() and not t.done() and t not in before
        }
        self.assertFalse(leaked, f"terminate leaked tasks: {leaked}")
        # A cancelled send must not record a successful delivery or write.
        if notifier is not None:
            delivery = notifier.status().get("last_delivery")
            self.assertTrue(delivery is None or delivery.get("outcome") != "sent")
        self.assertEqual(ctx.sent, [])

        # (b) a repeated initialize releases the previous runtime's send tasks.
        self._reset_plugin_data()
        gate2 = asyncio.Event()
        plugin2, ctx2 = await self._enable_notify_plugin()
        ctx2.gate = gate2
        await self._run_wb(plugin2, support._make_event("wb signin"), "signin", "")
        self.assertTrue(
            await self._wait_until(lambda: ctx2.send_calls >= 1 and self._pending(plugin2))
        )
        blocked2 = self._pending(plugin2)
        self.assertTrue(blocked2)

        await plugin2.initialize()
        await asyncio.sleep(0)
        for task in blocked2:
            self.assertTrue(task.done(), "previous send survived re-initialize()")
        self.assertEqual(ctx2.sent, [])
        gate2.set()

        # (c) the notify queue drains every queued notification.
        self._reset_plugin_data()
        open_gate = asyncio.Event()
        open_gate.set()
        plugin3, ctx3 = await self._enable_notify_plugin()
        ctx3.gate = open_gate
        await self._run_wb(plugin3, support._make_event("wb signin"), "signin", "")
        self.assertTrue(await self._wait_until(lambda: ctx3.send_calls >= 1))
        # A second, distinct real trigger: a failing model query (network error).
        self.up.handlers[("GET", support.MODELS_CATALOG_PATH)] = _status_handler(
            500, {"code": 9, "msg": "RAW"}
        )
        await self._run_wb(plugin3, support._make_event("wb models"), "models", "")
        self.assertTrue(await self._wait_until(lambda: ctx3.send_calls >= 2))
        await self._wait_idle(plugin3)
        self.assertEqual(ctx3.send_calls, 2)
        await plugin3.terminate()

    # =====================================================================
    # 10. the UpstreamClient(..., on_error=cb) observer contract.
    # =====================================================================
    async def test_10_upstream_observer_contract(self):
        session = aiohttp.ClientSession()
        self._extra_sessions = [session]
        store = CredentialStore(self._temp_dir(), support.MASTER_KEY)
        store.save(dict(support.SEED_CREDENTIALS))
        manager = AuthManager(
            session, store, chat_base=self.up.base, poll_interval=0.01, login_timeout=1.0
        )

        def make_client(observer):
            return UpstreamClient(
                session,
                manager,
                chat_base=self.up.base,
                billing_base=self.up.base,
                web_base=self.up.base,
                on_error=observer,
            )

        # -- categories are the fixed strings, never a raw body / token. ----
        events = []
        client = make_client(events.append)
        self.up.handlers[("GET", PROTECTED + "/forbidden")] = _status_handler(403)
        self.up.handlers[("GET", PROTECTED + "/throttled")] = _status_handler(
            429, {"code": 4290, "msg": "RAW-429"}, {"Retry-After": "7"}
        )
        self.up.handlers[("GET", PROTECTED + "/broken")] = _status_handler(
            500, {"code": 9, "msg": "RAW-500"}
        )
        self.up.handlers[("GET", PROTECTED + "/business")] = _status_handler(
            200, {"code": 123, "msg": "RAW-BUSINESS"}
        )

        for path, expected in (
            ("/forbidden", "permission"),
            ("/throttled", "rate"),
            ("/broken", "network"),
            ("/business", "upstream"),
        ):
            with self.assertRaises(UpstreamError):
                await client.request_json("GET", PROTECTED + path)
            self.assertEqual(events[-1], expected)

        # Every recorded observation is a bare, known category string.
        self.assertTrue(all(isinstance(item, str) for item in events))
        self.assertTrue(set(events) <= set(VALID_CATEGORIES))
        joined = " ".join(events)
        for raw in ("RAW-429", "RAW-500", "RAW-BUSINESS", "AUTH-RAW-SECRET"):
            self.assertNotIn(raw, joined)

        # -- a rescued 401 does not call the observer, and refreshes once. ---
        events_rescued = []
        rescued = make_client(events_rescued.append)
        self.up.handlers[("POST", REFRESH_PATH)] = _refresh_success
        self.up.handlers[("GET", PROTECTED + "/rescued")] = _bearer_401_then_ok
        self.up.hits[("POST", REFRESH_PATH)] = 0
        data = await rescued.request_json("GET", PROTECTED + "/rescued")
        self.assertEqual(data.get("ok"), True)
        self.assertEqual(events_rescued, [])
        self.assertEqual(self.up.hits[("POST", REFRESH_PATH)], 1)

        # -- a final 401 fires the observer exactly once as ``auth``. --------
        events_final = []
        final = make_client(events_final.append)
        self.up.handlers[("POST", REFRESH_PATH)] = _refresh_failure
        self.up.handlers[("GET", PROTECTED + "/final")] = _always_401
        self.up.hits[("POST", REFRESH_PATH)] = 0
        with self.assertRaises(UpstreamError) as caught:
            await final.request_json("GET", PROTECTED + "/final")
        self.assertEqual(caught.exception.status, 401)
        self.assertEqual(events_final, ["auth"])
        self.assertEqual(self.up.hits[("POST", REFRESH_PATH)], 1)

        # -- an observer that raises never changes the original request error.
        def _raising_observer(_category):
            raise RuntimeError("OBSERVER-RAW-SECRET")

        raising = make_client(_raising_observer)
        self.up.handlers[("GET", PROTECTED + "/raise")] = _status_handler(403)
        with self.assertRaises(UpstreamError) as caught:
            await raising.request_json("GET", PROTECTED + "/raise")
        self.assertEqual(caught.exception.status, 403)
        self.assertNotIn("OBSERVER-RAW-SECRET", repr(caught.exception))


def tearDownModule():
    """Delete only this module's exact sandbox directory."""
    shutil.rmtree(_MODULE_TMP, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
