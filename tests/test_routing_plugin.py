"""Real-plugin integration tests for ordered primary / standby model routing.

Scope: the *wiring* the real plugin must own, never the router's own unit
contract (``tests/test_routing.py``) and never the gateway/body-cap contract
(``tests/test_routing_gateway.py``).  This module drives the **real**
``WorkBuddyPlugin`` inside the **real** AstrBot framework and asserts:

* ``plugin.router`` is the real ``core.routing.ModelRouter`` built from the real
  ``routing`` settings -- present even while the route is disabled -- and its
  ``status()`` is a purely local view (it never probes the upstream);
* ``_build_gateway`` hands that router *and* ``gateway.max_request_body_mb`` to
  the real ``Gateway``, so the virtual ordered entry is advertised over real
  HTTP, a failover really answers with the standby text, and an oversized chat
  request is refused with the approved JSON 413 before anything is routed;
* the chat round is notified to the configured admin QQ exactly once, carrying
  the chat source and the attempt count, and never the upstream body / token;
* the ``UpstreamClient(..., on_error=cb)`` observer keeps its single-argument
  ``category`` contract while the *source* of the failure (chat / models /
  balance / signin / growth / api) is handed to ``main`` during the callback, so
  a concurrent balance 429 notifies on its own while a standby stream hangs;
* the persisted ``routing`` section survives a rebuild (real ``StateStore``)
  without touching the stored credentials or any other section.

Only external boundaries are replaced: ``Context.send_message`` (recording),
the platform instance list (real ``PlatformMetadata``) and the real ``aiohttp``
fake upstream.  Everything else -- ``WorkBuddyPlugin``, ``ModelRouter``,
``Gateway``, ``ChatService``, ``UpstreamClient``, ``AuthManager``,
``StateStore``, ``CredentialStore``, ``AdminNotifier`` -- is production code.

``tests.test_plugin_integration`` is imported as ``support`` for the framework
sandbox and its fixtures; ``tests.test_notification_integration`` supplies the
notification-aware context (recording ``send_message``) and the fixed fictional
admin QQ / platform id.  Neither test-case class is inherited, so the older
suites never run again from here.  Every credential / token / QQ below is a
FICTIONAL FIXED SAMPLE.
"""

import asyncio
import contextlib
import json
import sys
import unittest
import uuid
from pathlib import Path

import aiohttp
from aiohttp import web

# ---------------------------------------------------------------------------
# Make the plugin and both support modules importable in either discovery mode
# (``support`` pins ASTRBOT_ROOT / TMPDIR / WORKBUDDY_TEST_TMP before astrbot).
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

try:  # the notification fixture module (its TestCase is never inherited)
    import tests.test_notification_integration as notify_support
except ImportError:  # top-level discovery
    import test_notification_integration as notify_support  # type: ignore

# --- Real framework / plugin imports (sandbox already pinned by ``support``). ---
import astrbot_plugin_workbuddy.main as wb_main  # noqa: E402
from astrbot_plugin_workbuddy.core.routing import ORDERED_MODEL_ID  # noqa: E402

# --- FICTIONAL FIXED SAMPLE values (never real). ---
ADMIN_QQ = notify_support.ADMIN_QQ        # fictional receiving QQ
PLATFORM_ID = notify_support.PLATFORM_ID  # fictional aiocqhttp instance
GATEWAY_KEY = support.GATEWAY_KEY

CHAT_PATH = "/v2/chat/completions"
BALANCE_PATH = support.BALANCE_PATH
REFRESH_PATH = "/v2/plugin/auth/token/refresh"

# The configured ordered list: primary, standby, plus an unused third entry the
# "two consecutive 429s stop the round early" case must never reach.
PRIMARY = "wb-alpha"
STANDBY = "wb-beta"
SPARE = "wb-gamma"

STANDBY_ANSWER = "这是备用模型给出的回答"
PRIMARY_ANSWER = "这是主模型给出的回答"
SECRET_MARKER = "FICTIONAL-ROUTING-RAW-BODY"
# ~1.14 MiB of real UTF-8 bytes ('订' is three bytes), i.e. above a 1 MiB cap.
BIG_TEXT = "订" * 400000

# The documented failure sources ``main`` may read during an error callback.
VALID_SOURCES = ("chat", "models", "balance", "signin", "growth", "api")


# ---------------------------------------------------------------------------
# Fake-upstream chat behaviour (the only injected external HTTP responses).
# ---------------------------------------------------------------------------
def _frame(obj):
    return b"data: " + json.dumps(obj, ensure_ascii=False).encode("utf-8") + b"\n\n"


def _sse_frames(text, model):
    """A minimal, valid OpenAI SSE stream tagged with a real candidate model."""
    base = {
        "id": "chatcmpl-" + str(model),
        "object": "chat.completion.chunk",
        "created": 1,
        "model": model,
    }
    chunks = [
        dict(base, choices=[{"index": 0, "delta": {"role": "assistant"},
                             "finish_reason": None}]),
        dict(base, choices=[{"index": 0, "delta": {"content": text},
                             "finish_reason": None}]),
        dict(base, choices=[{"index": 0, "delta": {}, "finish_reason": "stop"}]),
    ]
    return [_frame(chunk) for chunk in chunks] + [b"data: [DONE]\n\n"]


def _last_chat_model(up):
    """The ``model`` of the chat POST the fake upstream just recorded."""
    for call in reversed(up.calls):
        if call.get("path") != CHAT_PATH:
            continue
        try:
            body = json.loads(call["body"].decode("utf-8"))
        except Exception:  # noqa: BLE001 - an unreadable body names no model
            return None
        model = body.get("model") if isinstance(body, dict) else None
        return model if isinstance(model, str) else None
    return None


class _RoutingPluginIntegrationTestCase(unittest.IsolatedAsyncioTestCase):
    """Drives the real plugin; binds the shared fixtures from ``support``.

    ``support._PluginIntegrationTestCase`` is *not* inherited (that would re-run
    its suite); only its individual fixture methods are bound below, plus local
    factories that swap in the notification-aware context.
    """

    async def asyncSetUp(self) -> None:
        await support._PluginIntegrationTestCase.asyncSetUp(self)
        self._contexts = []
        # A chat body above the gateway's own cap must be refused by the plugin,
        # never truncated by the *external* fake upstream's default 1 MiB limit.
        app = getattr(self.up, "app", None)
        if app is not None and hasattr(app, "_client_max_size"):
            app._client_max_size = 32 * 1024 * 1024
        self.behaviours = {}
        self.up.handlers[("POST", CHAT_PATH)] = self._chat_handler

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

    def _credentials_blob(self):
        return support._PluginIntegrationTestCase._credentials_blob(self)

    # -- config helpers ----------------------------------------------------
    def _routing_config(self, **overrides):
        base = {
            "gateway": {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 0,
                "api_key": GATEWAY_KEY,
                "concurrency": 4,
                "rpm": 600,
                "queue_size": 8,
                "queue_timeout": 5,
            },
            # The automatic sign-in / growth triggers stay off: this module is
            # about the chat route, not about scheduler timing.
            "signin": {"enabled": False},
            "growth": {
                "auto_after_signin": False,
                "auto_claim": False,
                "task_interval": 0,
            },
            "routing": {
                "enabled": True,
                "models": [PRIMARY, STANDBY],
                "cooldown_seconds": 60,
                "first_frame_timeout": 30,
            },
        }
        return support._deep_merge(base, overrides)

    def _notify_config(self, **overrides):
        notifications = {
            "enabled": True,
            "signin_success": True,
            "errors": True,
            "admin_qq": ADMIN_QQ,
            "platform_id": "",
            "error_cooldown_seconds": 60,
        }
        notifications.update(overrides.pop("notifications", {}))
        return support._deep_merge(
            self._routing_config(**overrides), {"notifications": notifications}
        )

    # -- factories ---------------------------------------------------------
    async def _make_plugin(self, *, initialize=True, **overrides):
        cfg_path = support._SESSION_ROOT / ("cfg-" + uuid.uuid4().hex + ".json")
        config = self._build_config(cfg_path, **overrides)
        context = notify_support._NotifyContext(
            config, [notify_support._Instance(PLATFORM_ID)]
        )
        self._ensure_metadata_name()
        plugin = wb_main.WorkBuddyPlugin(context, config)
        self._contexts.append(context)
        self._plugins.append(plugin)
        if initialize:
            await plugin.initialize()
        return plugin, context

    # -- fake upstream chat behaviour --------------------------------------
    async def _chat_handler(self, request):
        """Answer by candidate ``model``: a real status or a real SSE stream."""
        model = _last_chat_model(self.up)
        spec = self.behaviours.get(model) or {"kind": "sse", "text": "默认回答"}
        if spec.get("kind") == "status":
            return web.json_response(
                spec.get("body") or {"code": 9, "msg": SECRET_MARKER},
                status=spec["status"],
                headers=spec.get("headers") or {},
            )
        frames = _sse_frames(spec.get("text", "默认回答"), spec.get("model") or model)
        gate = spec.get("gate")
        if gate is None:
            return web.Response(
                body=b"".join(frames),
                headers={"Content-Type": "text/event-stream"},
            )
        # A standby attempt that really opens its stream and then holds the
        # first frame back until the test releases the gate.
        response = web.StreamResponse(
            status=200, headers={"Content-Type": "text/event-stream"}
        )
        await response.prepare(request)
        started = spec.get("started")
        if started is not None:
            started.set()
        await gate.wait()
        for frame in frames:
            await response.write(frame)
            await asyncio.sleep(0)
        with contextlib.suppress(Exception):
            await response.write_eof()
        return response

    # -- driving / waiting helpers ----------------------------------------
    async def _run_wb(self, plugin, event, action, rest=""):
        return await asyncio.wait_for(
            support._invoke(plugin.wb, event, action, rest), timeout=10
        )

    async def _chat_json(
        self, plugin, *, model=ORDERED_MODEL_ID, text="hi", timeout=20
    ):
        """POST one real chat completion to the plugin's own gateway listener."""
        port = getattr(plugin.gateway, "bound_port", None)
        self.assertTrue(port, "the real gateway never bound a port")
        timeout_obj = aiohttp.ClientTimeout(total=timeout)
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": text}],
            "stream": False,
        }
        async with aiohttp.ClientSession(timeout=timeout_obj) as session:
            async with session.post(
                "http://127.0.0.1:{}/v1/chat/completions".format(port),
                json=payload,
                headers={"Authorization": "Bearer " + GATEWAY_KEY},
            ) as resp:
                body = await resp.json(content_type=None)
                return resp.status, body

    async def _wait_until(self, predicate, timeout=5.0):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not predicate():
            if loop.time() >= deadline:
                return False
            await asyncio.sleep(0.01)
        return True

    def _pending(self, plugin):
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

    def _sent_texts(self, context):
        return [support._result_text(chain) for _, chain in context.sent]

    # =====================================================================
    # 1. an enabled route makes the real plugin ready, advertises the virtual
    #    entry over real HTTP, fails over A -> B, sends no QQ at all, and the
    #    real ``wb status`` reports the switch, the order and the last success.
    # =====================================================================
    async def test_01_failover_serves_standby_and_status_reports_the_route(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(**self._notify_config())
        self.assertTrue(plugin.ready)
        self.assertEqual(plugin.error_reason, "")

        router = getattr(plugin, "router", None)
        self.assertIsNotNone(router, "the plugin never exposed a real ModelRouter")
        status = router.status()
        self.assertIs(status.get("enabled"), True)
        self.assertEqual(status.get("models"), [PRIMARY, STANDBY])
        self.assertIsNone(status.get("last_result"))
        self.assertIsNone(status.get("last_success_model"))

        # ``status()`` is a local view: it must never probe the upstream.
        before = self.up.total_hits()
        for _ in range(3):
            router.status()
        self.assertEqual(self.up.total_hits(), before)

        # The real HTTP model list carries the virtual ordered entry.
        port = plugin.gateway.bound_port
        self.assertTrue(port)
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "http://127.0.0.1:{}/v1/models".format(port),
                headers={"Authorization": "Bearer " + GATEWAY_KEY},
            ) as resp:
                self.assertEqual(resp.status, 200)
                listing = await resp.json()
        entries = [item for item in listing.get("data") or [] if isinstance(item, dict)]
        virtual = [item for item in entries if item.get("id") == ORDERED_MODEL_ID]
        self.assertEqual(len(virtual), 1, "the virtual route is not advertised once")
        self.assertNotIn("credits", virtual[0])  # a route, not a price-bearing model

        # Primary 429, standby answers: the caller really sees the standby text.
        self.behaviours[PRIMARY] = {
            "kind": "status",
            "status": 429,
            "body": {"code": 4290, "msg": SECRET_MARKER},
            "headers": {"Retry-After": "30"},
        }
        self.behaviours[STANDBY] = {"kind": "sse", "text": STANDBY_ANSWER}
        code, body = await self._chat_json(plugin)
        self.assertEqual(code, 200)
        self.assertEqual(body["choices"][0]["message"]["content"], STANDBY_ANSWER)

        sent_models = [entry["model"] for entry in self.up.chat_bodies()]
        self.assertEqual(sent_models, [PRIMARY, STANDBY])
        route = router.status()
        self.assertEqual(route.get("last_success_model"), STANDBY)
        self.assertEqual(route.get("last_result", {}).get("outcome"), "success")

        # A successful failover is not an alert: zero QQ messages.
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 0)

        # The real command reports the switch, the configured order and the
        # candidate that actually succeeded last.
        text = support._text_of(
            await self._run_wb(plugin, support._make_event("wb status"), "status", "")
        )
        self.assertTrue(
            any(word in text for word in ("主备", "路由")),
            "wb status does not report the routing switch: " + text,
        )
        self.assertIn(PRIMARY, text)
        self.assertIn(STANDBY, text)
        self.assertLess(text.index(PRIMARY), text.index(STANDBY))
        self.assertIn("最近", text)
        # The default body cap is the approved 16 MiB (1024*1024 bytes), so the
        # overview must label it in MiB: a decimal "MB" would misstate the limit.
        self.assertIn("16 MiB", text)
        for secret in (
            SECRET_MARKER,
            GATEWAY_KEY,
            support.SEED_CREDENTIALS["accessToken"],
            support.SEED_CREDENTIALS["refreshToken"],
        ):
            self.assertNotIn(secret, text)
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 0)

    # =====================================================================
    # 2. A429 + B429 stop the round before the third candidate and notify the
    #    admin exactly once (chat source, attempt count, stop reason); a repeat
    #    request during the persisted cooldown never spams again.
    # =====================================================================
    async def test_02_two_rate_limits_stop_early_and_notify_once(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(
            **self._notify_config(routing={"models": [PRIMARY, STANDBY, SPARE]})
        )
        self.assertTrue(plugin.ready)
        for model in (PRIMARY, STANDBY):
            self.behaviours[model] = {
                "kind": "status",
                "status": 429,
                "body": {"code": 4290, "msg": SECRET_MARKER},
                "headers": {"Retry-After": "30"},
            }
        self.behaviours[SPARE] = {"kind": "sse", "text": "第三个模型不该被调用"}

        code, _ = await self._chat_json(plugin)
        self.assertEqual(code, 429)

        # Two real 429 attempts stop the round: the spare is never asked.
        self.assertEqual(
            [entry["model"] for entry in self.up.chat_bodies()], [PRIMARY, STANDBY]
        )
        self.assertEqual(self.up.chat_bodies()[0]["model"], PRIMARY)
        route = plugin.router.status()
        self.assertEqual(route.get("last_result", {}).get("outcome"), "failed")
        self.assertEqual(
            list(route.get("last_result", {}).get("attempts") or []),
            [PRIMARY, STANDBY],
        )

        # Exactly one QQ, naming the chat source, the attempt count and a stop.
        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 1)
        text = self._sent_texts(ctx)[0]
        self.assertIn("聊天", text)
        self.assertIn("2", text)
        self.assertTrue(
            any(word in text for word in ("停止", "中止", "冷却", "全部失败")),
            "the routing alert does not explain why the round stopped: " + text,
        )
        self.assertNotIn(SECRET_MARKER, text)
        for secret in (
            support.SEED_CREDENTIALS["accessToken"],
            support.SEED_CREDENTIALS["refreshToken"],
            GATEWAY_KEY,
        ):
            self.assertNotIn(secret, text)

        # A repeat request is refused by the persisted cooldown: no upstream hit,
        # no second QQ.
        hits_before = self.up.hits[("POST", CHAT_PATH)]
        code, _ = await self._chat_json(plugin)
        self.assertEqual(code, 429)
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], hits_before)
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 1)

    # =====================================================================
    # 3. after a successful and a failed round a rebuilt plugin (same state,
    #    same settings) keeps the route cooldown, probes nothing on its own,
    #    and neither the credentials nor any other state section is rewritten.
    # =====================================================================
    async def test_03_rebuild_keeps_cooldown_and_preserves_state(self):
        self._seed_credentials()
        credentials_before = self._credentials_blob()
        config = self._notify_config()
        plugin, ctx = await self._make_plugin(**config)

        # One successful failover round ...
        self.behaviours[PRIMARY] = {
            "kind": "status",
            "status": 429,
            "body": {"code": 4290, "msg": SECRET_MARKER},
            "headers": {"Retry-After": "30"},
        }
        self.behaviours[STANDBY] = {"kind": "sse", "text": STANDBY_ANSWER}
        code, _ = await self._chat_json(plugin)
        self.assertEqual(code, 200)
        self.assertEqual(plugin.router.status().get("last_success_model"), STANDBY)

        # ... then one fully failed round (the primary is still cooling).
        self.behaviours[STANDBY] = {
            "kind": "status",
            "status": 429,
            "body": {"code": 4290, "msg": SECRET_MARKER},
            "headers": {"Retry-After": "30"},
        }
        code, _ = await self._chat_json(plugin)
        self.assertEqual(code, 429)
        self.assertEqual(
            plugin.router.status().get("last_result", {}).get("outcome"), "failed"
        )
        self.assertEqual(
            plugin.router.status().get("last_success_model"),
            STANDBY,
            "a failed round must not erase the last real success",
        )
        await self._wait_idle(plugin)
        delivery_before = plugin.notifier.status().get("last_delivery")

        # An unrelated section written through the real StateStore...
        plugin.state.update("ticket_marker", {"keep": "T3"})
        state_before = plugin.state.load()
        cooldowns_before = plugin.router.status().get("cooldowns")
        self.assertTrue(cooldowns_before, "no route cooldown was persisted")

        # ... must survive a rebuild over the same data dir and settings.
        rebuilt, rebuilt_ctx = await self._make_plugin(**config)
        self.assertTrue(rebuilt.ready)
        state_after = rebuilt.state.load()
        self.assertEqual(state_after.get("ticket_marker"), {"keep": "T3"})
        self.assertEqual(
            state_after.get("notifications"), state_before.get("notifications")
        )
        self.assertEqual(self._credentials_blob(), credentials_before)
        self.assertEqual(
            rebuilt.auth.credentials.get("accessToken"),
            support.SEED_CREDENTIALS["accessToken"],
        )

        # The persisted deadline is restored, not restarted.
        route_after = rebuilt.router.status()
        self.assertTrue(route_after.get("cooldowns"))
        for model, remaining in route_after["cooldowns"].items():
            self.assertIn(model, (PRIMARY, STANDBY))
            self.assertGreater(remaining, 0)
            self.assertLessEqual(remaining, 30.5)
        self.assertEqual(route_after.get("last_success_model"), STANDBY)
        if delivery_before is not None:
            self.assertEqual(
                rebuilt.notifier.status().get("last_delivery"), delivery_before
            )

        # A rebuild performs no upstream probe of its own ...
        await self._run_wb(rebuilt, support._make_event("wb status"), "status", "")
        router_hits = self.up.total_hits()
        rebuilt.router.status()

        # ... and the cooled route refuses the next chat locally, without any
        # candidate request and without a second alert burst.
        code, _ = await self._chat_json(rebuilt)
        self.assertEqual(code, 429)
        self.assertEqual(self.up.total_hits(), router_hits)
        await self._wait_idle(rebuilt)
        await asyncio.sleep(0.05)
        self.assertEqual(rebuilt_ctx.send_calls, 0)

    # =====================================================================
    # 4. concurrency isolation: while the standby's first frame hangs, a real
    #    balance 429 on the same client still notifies on its own (source
    #    visible as credits/balance) and the later standby success adds nothing.
    # =====================================================================
    async def test_04_hanging_standby_does_not_silence_a_balance_alert(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(**self._notify_config())
        gate = asyncio.Event()
        started = asyncio.Event()
        self.behaviours[PRIMARY] = {
            "kind": "status",
            "status": 429,
            "body": {"code": 4290, "msg": SECRET_MARKER},
            "headers": {"Retry-After": "30"},
        }
        self.behaviours[STANDBY] = {
            "kind": "sse",
            "text": STANDBY_ANSWER,
            "gate": gate,
            "started": started,
        }
        self.up.handlers[("POST", BALANCE_PATH)] = notify_support._status_handler(
            429, {"code": 4290, "msg": SECRET_MARKER}, {"Retry-After": "20"}
        )

        chat_task = asyncio.create_task(self._chat_json(plugin))
        try:
            self.assertTrue(
                await self._wait_until(started.is_set, 10),
                "the standby attempt never opened its stream",
            )
            # The routed round now waits for the standby's first frame.  A
            # separate, real balance request on the same client must still be
            # observed -- the mute is per standby attempt, not global.
            reply = support._text_of(
                await self._run_wb(
                    plugin, support._make_event("wb balance"), "balance", ""
                )
            )
            self.assertTrue(reply.strip())
            self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
            await self._wait_idle(plugin)

            self.assertEqual(ctx.send_calls, 1)
            text = self._sent_texts(ctx)[0]
            self.assertTrue(
                any(word in text for word in ("积分", "余额")),
                "the balance alert does not name its source: " + text,
            )
            self.assertNotIn(SECRET_MARKER, text)

            # Releasing the standby's first frame completes the route normally.
            gate.set()
            code, body = await asyncio.wait_for(chat_task, timeout=20)
            self.assertEqual(code, 200)
            self.assertEqual(body["choices"][0]["message"]["content"], STANDBY_ANSWER)
            self.assertEqual(
                [entry["model"] for entry in self.up.chat_bodies()],
                [PRIMARY, STANDBY],
            )
            # The successful standby answer never adds a candidate alert.
            await self._wait_idle(plugin)
            await asyncio.sleep(0.05)
            self.assertEqual(ctx.send_calls, 1)
        finally:
            gate.set()
            if not chat_task.done():
                chat_task.cancel()
                with contextlib.suppress(BaseException):
                    await chat_task

    # =====================================================================
    # 5. notifications off: a failed route writes its state but sends nothing,
    #    and the explicit success/error switches stay exactly as configured;
    #    a disabled route still exposes a readable local status.
    # =====================================================================
    async def test_05_disabled_notifications_still_record_a_failed_route(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(
            **self._notify_config(
                notifications={
                    "enabled": False,
                    "signin_success": False,
                    "errors": False,
                }
            )
        )
        self.assertTrue(plugin.ready)
        for model in (PRIMARY, STANDBY):
            self.behaviours[model] = {
                "kind": "status",
                "status": 429,
                "body": {"code": 4290, "msg": SECRET_MARKER},
                "headers": {"Retry-After": "30"},
            }

        code, _ = await self._chat_json(plugin)
        self.assertEqual(code, 429)
        route = plugin.router.status()
        self.assertEqual(route.get("last_result", {}).get("outcome"), "failed")
        persisted = plugin.state.load().get("routing") or {}
        self.assertEqual(persisted.get("last_result", {}).get("outcome"), "failed")

        # Nothing was sent, and no switch was silently turned on by the route.
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 0)
        notify_status = plugin.notifier.status()
        self.assertIs(notify_status.get("enabled"), False)
        self.assertIs(notify_status.get("signin_success"), False)
        self.assertIs(notify_status.get("errors"), False)

        # A route that is configured off still builds a real, readable router.
        off_plugin, off_ctx = await self._make_plugin(
            **self._notify_config(routing={"enabled": False, "models": []})
        )
        self.assertTrue(off_plugin.ready)
        off_router = getattr(off_plugin, "router", None)
        self.assertIsNotNone(off_router, "the router must exist even when disabled")
        off_status = off_router.status()
        self.assertIs(off_status.get("enabled"), False)
        self.assertEqual(off_status.get("models"), [])
        await self._wait_idle(off_plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(off_ctx.send_calls, 0)

    # =====================================================================
    # 6. main really hands ``gateway.max_request_body_mb`` to the real Gateway:
    #    a ~1.1 MiB virtual chat request is a JSON 413 with the configured cap,
    #    no candidate request, no QQ, and no route result.
    # =====================================================================
    async def test_06_configured_body_cap_refuses_before_the_route(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(
            **self._notify_config(gateway={"max_request_body_mb": 1})
        )
        self.assertTrue(plugin.ready)
        self.assertEqual(plugin._settings["gateway"]["max_request_body_mb"], 1)
        self.behaviours[PRIMARY] = {"kind": "sse", "text": PRIMARY_ANSWER}
        self.behaviours[STANDBY] = {"kind": "sse", "text": STANDBY_ANSWER}
        hits_before = self.up.total_hits()

        code, body = await self._chat_json(plugin, text=BIG_TEXT, timeout=60)
        self.assertEqual(code, 413)
        error = body.get("error")
        self.assertIsInstance(error, dict)
        self.assertEqual(error.get("type"), "request_too_large")
        self.assertEqual(error.get("code"), "request_too_large")
        message = error.get("message") or ""
        self.assertIn("1", message)                       # the configured cap
        self.assertIn("MiB", message)
        self.assertIn("max_request_body_mb", message)     # the actionable knob
        self.assertNotIn(SECRET_MARKER, json.dumps(body, ensure_ascii=False))

        # Refused before admission and before the route: no candidate request,
        # no route result, no notification.
        self.assertEqual(self.up.total_hits(), hits_before)
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 0)
        self.assertIsNone(plugin.router.status().get("last_result"))
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 0)

    # =====================================================================
    # 7. a real 401 (refresh cannot rescue) and a real 403 never switch to the
    #    standby candidate: one matching login / permission QQ each, carrying
    #    the chat source and never the upstream body.
    # =====================================================================
    async def test_07_auth_and_permission_failures_never_use_the_standby(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(**self._notify_config())
        self.up.handlers[("POST", REFRESH_PATH)] = notify_support._refresh_failure
        self.behaviours[STANDBY] = {"kind": "sse", "text": "备用模型不该被调用"}

        # (a) 401 whose single refresh fails.
        self.behaviours[PRIMARY] = {
            "kind": "status",
            "status": 401,
            "body": {"code": 4010, "msg": SECRET_MARKER},
        }
        code, _ = await self._chat_json(plugin)
        self.assertEqual(code, 401)
        self.assertEqual(self.up.hits[("POST", REFRESH_PATH)], 1)
        self.assertEqual(
            [entry["model"] for entry in self.up.chat_bodies()], [PRIMARY]
        )

        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
        await self._wait_idle(plugin)
        self.assertEqual(ctx.send_calls, 1)
        auth_text = self._sent_texts(ctx)[0]
        self.assertIn("聊天", auth_text)
        self.assertTrue(
            any(word in auth_text for word in ("登录", "授权", "认证")),
            "the auth alert does not name the login problem: " + auth_text,
        )
        self.assertNotIn(SECRET_MARKER, auth_text)
        self.assertNotIn(support.SEED_CREDENTIALS["accessToken"], auth_text)

        # (b) a 403 is a permission problem: still no standby, one more alert.
        self.behaviours[PRIMARY] = {
            "kind": "status",
            "status": 403,
            "body": {"code": 4030, "msg": SECRET_MARKER},
        }
        code, _ = await self._chat_json(plugin)
        self.assertEqual(code, 403)
        self.assertEqual(
            [entry["model"] for entry in self.up.chat_bodies()], [PRIMARY, PRIMARY]
        )

        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 2))
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 2)
        permission_text = self._sent_texts(ctx)[1]
        self.assertIn("聊天", permission_text)
        self.assertTrue(
            any(word in permission_text for word in ("权限", "授权")),
            "the permission alert does not name the permission problem: "
            + permission_text,
        )
        self.assertNotIn(SECRET_MARKER, permission_text)
        self.assertNotIn(support.SEED_CREDENTIALS["refreshToken"], permission_text)

        route = plugin.router.status()
        self.assertEqual(route.get("last_result", {}).get("outcome"), "failed")
        self.assertIsNone(route.get("last_success_model"))
        # The observer contract itself is unchanged: one bare category string,
        # and the source is only ever one of the documented read-only keys.
        source = getattr(plugin.client, "error_source", None)
        self.assertTrue(
            source is None or source in VALID_SOURCES,
            "the client error source is not a documented key: {!r}".format(source),
        )


    # =====================================================================
    # 8. a real 400 from the primary is not a retryable status: the standby is
    #    never asked, the caller really sees the 400, and the admin still gets
    #    exactly one chat-source alert that explains the stopped round.
    # =====================================================================
    async def test_08_upstream_400_stops_the_round_and_alerts_once(self):
        self._seed_credentials()
        plugin, ctx = await self._make_plugin(**self._notify_config())
        self.assertTrue(plugin.ready)

        self.behaviours[PRIMARY] = {
            "kind": "status",
            "status": 400,
            "body": {"code": 4000, "msg": SECRET_MARKER},
        }
        self.behaviours[STANDBY] = {"kind": "sse", "text": "备用模型不该被调用"}

        code, body = await self._chat_json(plugin)
        self.assertEqual(code, 400)
        self.assertNotIn(SECRET_MARKER, json.dumps(body, ensure_ascii=False))

        # A 400 is not retryable: only the primary is really asked.
        self.assertEqual(
            [entry["model"] for entry in self.up.chat_bodies()], [PRIMARY]
        )
        route = plugin.router.status()
        last = route.get("last_result") or {}
        self.assertEqual(last.get("outcome"), "failed")
        self.assertEqual(last.get("reason"), "upstream")
        self.assertEqual(last.get("status"), 400)
        self.assertIsNone(route.get("last_success_model"))

        # Exactly one QQ, naming the chat source, the single attempt and a
        # fixed failure / stop explanation -- never the upstream body or a token.
        self.assertTrue(await self._wait_until(lambda: ctx.send_calls >= 1))
        await self._wait_idle(plugin)
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.send_calls, 1)
        text = self._sent_texts(ctx)[0]
        self.assertIn("聊天", text)
        self.assertIn("1", text)
        self.assertTrue(
            any(
                word in text
                for word in ("停止", "中止", "失败", "上游返回错误", "上游服务异常")
            ),
            "the routing alert does not explain the failed round: " + text,
        )
        self.assertNotIn(SECRET_MARKER, text)
        for secret in (
            support.SEED_CREDENTIALS["accessToken"],
            support.SEED_CREDENTIALS["refreshToken"],
            GATEWAY_KEY,
        ):
            self.assertNotIn(secret, text)


if __name__ == "__main__":
    unittest.main()
