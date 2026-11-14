"""Real-plugin integration tests for admin-QQ notifications.

Scope: the notification path that starts from the **real** plugin runtime and
from the **final upstream errors** the plugin observes -- never by calling
``AdminNotifier`` by hand.  The unit contract of ``core.notifications`` and of
``core.config.load_settings`` lives in ``tests/test_admin_notifications.py``;
this module proves the *wiring*: the real plugin builds a notifier from the real
``notifications`` settings, routes scheduler results and final upstream
errors to it, reports it through ``wb status``, and can be torn down mid-send.

Contract exercised here -- the real plugin wiring this module asserts:

* config section ``notifications``: ``enabled`` (default ``False``),
  ``signin_success``, ``errors``, ``admin_qq``, ``platform_id``,
  ``error_cooldown_seconds``;
* the plugin exposes ``plugin.notifier`` with the ``AdminNotifier`` contract
  (``await signin(result)`` / ``await error(category)`` / ``status()``);
* a successful real check-in (automatic or manual) notifies the configured QQ,
  not the user who ran the command, and carries only the reward the claim itself
  returned -- the "current total credits" read is gone with the credit feature;
* final upstream errors are forwarded through the ``UpstreamClient(...,
  on_error=cb)`` observer and through the scheduler ``on_result`` hook;
* observer categories are the fixed strings ``auth`` / ``permission`` / ``rate``
  / ``network`` / ``upstream`` -- never a raw body or token;
* a cancelled send writes nothing and the runtime is fully drained on
  ``terminate`` / re-``initialize``.

Only the external boundaries are replaced: ``Context.send_message`` (recording,
can return ``False`` / raise / block), the platform instance list
(``context.platform_manager.platform_insts`` with real ``PlatformMetadata``),
and the real ``aiohttp`` fake upstream.  Everything else -- ``WorkBuddyPlugin``,
``SigninScheduler`` / ``SigninService`` / ``UpstreamClient`` / ``CliCredentials``
/ ``StateStore`` -- is the real production code.

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

# --- FICTIONAL FIXED SAMPLE values (never real). ---
ADMIN_QQ = "123456789"                   # fictional receiving QQ
PLATFORM_ID = "qq-notify-test"           # fictional aiocqhttp instance id

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
def _status_handler(status, body=None, headers=None):
    def handler(request):
        return web.json_response(body or {}, status=status, headers=headers or {})

    return handler


# ---------------------------------------------------------------------------
# ACP client stand-ins: they cover the two failures the real CLI can produce
# at the protocol level, without spawning one.
# ---------------------------------------------------------------------------
class _AuthFailingClient:
    """``session/new`` 回 ``-32000``/``category=auth``：真实 CLI 未登录的样子。"""

    def prompt_once(self, text, *, cwd=None, timeout=None):
        return self._fail()

    async def _fail(self):
        from astrbot_plugin_workbuddy.core.acp import AcpRpcError

        raise AcpRpcError(-32000, "auth", "auth required")
        yield  # pragma: no cover - 只为让它成为 async generator


class _ProcessFailingClient:
    """CLI 进程死了：适配层要翻成 502，而不是把原文透出去。"""

    def __init__(self, message="cli died"):
        self.message = message

    def prompt_once(self, text, *, cwd=None, timeout=None):
        return self._fail()

    async def _fail(self):
        from astrbot_plugin_workbuddy.core.acp import AcpProcessError

        raise AcpProcessError(self.message)
        yield  # pragma: no cover


class _NotificationIntegrationTestCase(unittest.IsolatedAsyncioTestCase):
    """Drives the real plugin; binds the shared fixtures from ``support``.

    The ``support`` class is *not* inherited (that would re-run its tests); only
    the individual fixture functions are bound, plus a local ``_make_plugin``
    that swaps in the notification-aware context.
    """

    async def asyncSetUp(self) -> None:
        await support._PluginIntegrationTestCase.asyncSetUp(self)
        self._contexts = []

    async def asyncTearDown(self) -> None:
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

    async def _use_acp_chat(self, plugin, client) -> None:
        """把聊天后端换成"真适配层 + 假 ACP 客户端"。

        适配层的错误分类与 ``on_error`` 观察者都是真的；换掉的只有客户端——要制造
        "CLI 说没登录""进程死了"这两种协议级故障，没必要真起一个进程。

        这里**不**接运行时：``AcpChatBackend`` 注入了客户端就不会再去要进程
        （见 ``_lease``），接进来也只是一个永远不被读的字段。401 回灌到运行时登录态
        那条分支由 ``tests/test_acp_backend.py`` 覆盖。
        """
        from astrbot_plugin_workbuddy.core.acp import AcpChatBackend
        from astrbot_plugin_workbuddy.core.routing import ORDERED_MODEL_ID

        plugin.chat = AcpChatBackend(
            client,
            model=ORDERED_MODEL_ID,
            on_error=plugin._on_chat_error,
        )
        await plugin.gateway.stop()
        await plugin._build_gateway()

    async def _post_chat(self, plugin, payload) -> int:
        """打一次网关的 ``/v1/chat/completions``，返回 HTTP 状态码。"""
        port = plugin.gateway.bound_port
        self.assertTrue(port)
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "http://127.0.0.1:{}/v1/chat/completions".format(port),
                json=payload,
                headers={"Authorization": "Bearer " + support.GATEWAY_KEY},
            ) as resp:
                await resp.read()
                return resp.status

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
    # 1. notifications off: no QQ send.
    # =====================================================================
    async def test_01_default_off_sends_no_qq_and_keeps_the_signin_chain_alone(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(
            **support._deep_merge(
                self._ready_config(signin={"enabled": False}),
                {"notifications": {"enabled": False}},
            )
        )
        self.assertTrue(plugin.ready)
        # The switch is off by default.
        self.assertIs(plugin._settings["notifications"]["enabled"], False)

        reply = support._text_of(
            await self._run_wb(plugin, support._make_event("wb signin"), "signin", "")
        )
        self.assertTrue(reply.strip())

        # The real check-in reached the upstream...
        self.assertGreaterEqual(self.up.hits[("POST", support.CHECKIN_STATUS_PATH)], 1)
        # ...but nothing was sent, and no route outside the check-in pair was
        # touched (the notification used to pull the balance as well).
        await self._wait_idle(plugin)
        self.assertEqual(ctx.send_calls, 0)
        touched = {path for (_method, path) in self.up.hits}
        self.assertEqual(
            touched,
            {support.CHECKIN_STATUS_PATH, support.CHECKIN_CLAIM_PATH},
        )

    # =====================================================================
    # 2. fully on: a real check-in notifies the configured QQ (not the
    #    caller) with the claim's own reward; repeat / rebuild dedup.
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
        self.assertIn("10", text)       # this round's reward, from the claim
        # 余额那条线随私有接口一起下线了：消息里不该再出现任何"总积分"字样。
        self.assertNotIn("可用总积分", text)

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
    # 4. a claim that answers HTTP 500 sends exactly one error notice: the
    #    client observer and the scheduler hook must not each send their own.
    # =====================================================================
    async def test_04_a_failed_claim_sends_one_notice_not_two(self):
        plugin, ctx = await self._enable_notify_plugin()
        self.up.on(
            "POST",
            support.CHECKIN_CLAIM_PATH,
            _status_handler(500, {"code": 9, "msg": "RAW-CLAIM-500"}),
        )

        reply = support._text_of(
            await self._run_wb(plugin, support._make_event("wb signin"), "signin", "")
        )
        self.assertTrue(reply.strip())

        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)

        # 客户端观察者（source=api）与调度器 on_result（source=signin）都会想报一条，
        # 同一 category 的冷却必须把它们收成一条——否则管理员每次失败都收两条。
        self.assertEqual(ctx.send_calls, 1)
        text = support._result_text(ctx.sent[0][1])
        self.assertTrue(text.strip())
        self.assertNotIn("RAW-CLAIM-500", text)

    # =====================================================================
    # 5. final auth failures notify; an unlogged plugin and an active logout
    #    do not.
    # =====================================================================
    async def test_05_final_auth_failure_notifies_but_unlogged_and_logout_do_not(self):
        # (a) Unlogged: the signin command short-circuits, no observer, no send.
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
        self.assertFalse(unlogged.account.credentials)
        reply = support._text_of(
            await self._run_wb(unlogged, support._make_event("wb signin"), "signin", "")
        )
        self.assertIn("尚未登录", reply)
        await self._wait_idle(unlogged)
        self.assertEqual(unlogged_ctx.send_calls, 0)

        # (b) Active logout must not notify.
        self._seed_credentials()
        logged_out, logged_out_ctx = await self._enable_notify_plugin()
        await self._run_wb(logged_out, support._make_event("wb logout"), "logout", "")
        await self._wait_idle(logged_out)
        self.assertEqual(logged_out_ctx.send_calls, 0)

        # (c) A 401 the CLI cannot rescue (it is not even installed here) is a
        #     final auth failure: one fixed notice, never the raw body.
        self._seed_credentials()
        plugin, ctx = await self._enable_notify_plugin()
        self.up.on(
            "POST",
            support.CHECKIN_STATUS_PATH,
            _status_handler(401, {"code": 4010, "msg": "AUTH-RAW-SECRET"}),
        )

        await self._run_wb(plugin, support._make_event("wb signin"), "signin", "")
        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
        await self._wait_idle(plugin)
        self.assertEqual(ctx.send_calls, 1)
        text = support._result_text(ctx.sent[0][1])
        self.assertTrue(text.strip())
        self.assertNotIn("AUTH-RAW-SECRET", text)

    # =====================================================================
    # 6. a chat failure notifies the admin exactly once, with a fixed text.
    #    The ACP backend is real; only the client it talks to is a stub, so
    #    the category mapping and the observer wiring are both exercised.
    # =====================================================================
    async def test_06_chat_failures_notify_once_with_a_fixed_text(self):
        plugin, ctx = await self._enable_notify_plugin()
        payload = {
            "model": support.MODEL,
            "messages": [{"role": "user", "content": "hi"}],
        }

        # 401: 真实 CLI 未登录时 session/new 会回 -32000/category=auth。
        await self._use_acp_chat(plugin, _AuthFailingClient())
        self.assertEqual(await self._post_chat(plugin, payload), 401)
        self.assertTrue(
            await self._wait_until(lambda: ctx.send_calls >= 1),
            "no notification after a chat 401",
        )
        await self._wait_idle(plugin)

        # 502: 进程不可用，分类是上游异常（与 auth 不同，不会被冷却去重掉）。
        await self._use_acp_chat(plugin, _ProcessFailingClient("CLI-DIED-RAW"))
        self.assertEqual(await self._post_chat(plugin, payload), 502)
        self.assertTrue(
            await self._wait_until(lambda: ctx.send_calls >= 2),
            "no notification after a chat 502",
        )
        await self._wait_idle(plugin)

        self.assertEqual(ctx.send_calls, 2)
        for _, chain in ctx.sent:
            text = support._result_text(chain)
            self.assertTrue(text.strip())
            self.assertNotIn("CLI-DIED-RAW", text)

    # =====================================================================
    # 7. an unconfirmed sign-in sends a fixed safe error message.
    # =====================================================================
    async def test_07_unconfirmed_signin_notifies_without_leaking(self):
        plugin, ctx = await self._enable_notify_plugin()

        # A real sign-in whose claim reports a business error is unconfirmed.
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
        self.assertEqual(ctx.send_calls, 1)

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
        # A second, distinct real trigger from a different producer: a chat
        # failure (category upstream, so the cooldown does not swallow it).
        await self._use_acp_chat(plugin3, _ProcessFailingClient())
        self.assertEqual(
            await self._post_chat(
                plugin3,
                {"model": support.MODEL, "messages": [{"role": "user", "content": "hi"}]},
            ),
            502,
        )
        self.assertTrue(await self._wait_until(lambda: ctx3.send_calls >= 2))
        await self._wait_idle(plugin3)
        self.assertEqual(ctx3.send_calls, 2)
        await plugin3.terminate()

    # =====================================================================
    # 10. -- 已删除 --
    #
    # 这一条曾经用真 ``AuthManager`` 验 ``UpstreamClient(..., on_error=cb)`` 的分类
    # 契约：403→permission / 429→rate / 500→network / 业务码→upstream，以及「刷
    # 一次就救回来的 401 不算事故」。那套契约现在由
    # ``tests/test_http.py::ClientEnvelopeTests`` 用最小凭据来源替身逐条覆盖，断言
    # 一字不差；``AuthManager`` 本身随私有聊天路径一起删掉了。留一份需要七条 OAuth
    # 路由才跑得起来的副本，只会让删掉的路由在测试里复活。


def tearDownModule():
    """Delete only this module's exact sandbox directory."""
    shutil.rmtree(_MODULE_TMP, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
