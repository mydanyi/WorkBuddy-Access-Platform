"""Behavioral tests for the admin-QQ notification core and its configuration.

Scope: ``core.notifications.AdminNotifier`` plus the ``notifications`` section
of ``core.config.load_settings``.  This module adds *tests only*; it does not
touch the production implementation or any existing test.

Only the documented external boundaries are replaced:

* ``Context.send_message`` (recording stand-in that returns ``True`` / ``False``
  or raises);
* the platform instance list / metadata (``context.platform_manager
  .platform_insts`` with real ``PlatformMetadata`` objects);
* the balance *network* boundary (the ``client`` constructor argument);
* the clock (an injectable callable).

Everything else is real: the AstrBot ``MessageSession`` / ``MessageType`` /
``MessageChain`` classes, the real ``StateStore``, the real ``ConfigError`` /
``load_settings`` and the real notification logic under test.

The real AstrBot directory is initialized by importing
``tests.test_plugin_integration`` as ``support`` first (its header pins
``ASTRBOT_ROOT`` / ``TMPDIR`` / ``WORKBUDDY_TEST_TMP`` *before* any astrbot
import).  Its test-case classes are intentionally **not** imported, so this
module never re-runs the integration suite's tests.
"""

import asyncio
import shutil
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

# ---------------------------------------------------------------------------
# Make ``tests.test_plugin_integration`` importable in both discovery modes,
# then import it as a support module (this sets up the controlled AstrBot root
# before any astrbot import below).
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
# Real framework classes (framework v4.27.4) used by the notifier.
from astrbot.api.event import MessageChain  # noqa: E402
from astrbot.core.platform.message_session import MessageSession  # noqa: E402

from astrbot_plugin_workbuddy.core.config import ConfigError, load_settings  # noqa: E402
from astrbot_plugin_workbuddy.core.storage import StateStore  # noqa: E402
from astrbot_plugin_workbuddy.core.notifications import AdminNotifier  # noqa: E402


# A module-scoped sandbox, strictly under the task tmp root provided by support.
_MODULE_TMP = Path(
    tempfile.mkdtemp(prefix="wb-admin-notify-", dir=str(support.BASE_TMP))
)

VALID_CATEGORIES = (
    "auth",
    "permission",
    "rate",
    "network",
    "upstream",
    "signin",
    "growth",
    "runtime",
)

_MISSING = object()


# ---------------------------------------------------------------------------
# External boundaries (replaced) and small fixture helpers.
# ---------------------------------------------------------------------------
class _Attrs(dict):
    """A dict that also answers attribute access (result / balance shape)."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name, value):
        self[name] = value


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


class _FakeContext:
    """Only the ``send_message`` + platform-list boundary is replaced."""

    def __init__(self, instances=(), send_result=True, send_error=None):
        self.platform_manager = _PlatformManager(instances)
        self.send_result = send_result
        self.send_error = send_error
        self.sent = []
        self.send_calls = 0

    async def send_message(self, session, message_chain):
        self.send_calls += 1
        if self.send_error is not None:
            raise self.send_error
        self.sent.append((session, message_chain))
        return self.send_result


class _FakeClient:
    """The balance *network* boundary: returns a mapping or raises.

    The implementation must reach the balance through the documented
    ``balance()`` coroutine; any other attribute access is an error.
    """

    def __init__(self, result=_MISSING, error=None):
        self._result = result
        self._error = error
        self.calls = 0

    async def _fetch(self):
        self.calls += 1
        if self._error is not None:
            raise self._error
        return self._result

    async def balance(self):
        return await self._fetch()


class _Clock:
    """Injectable clock: the notifier's ``clock`` contract is a callable."""

    def __init__(self, value=1_000_000.0):
        self.value = float(value)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)


def _signin_result(date, status="signed", verified=True, reward=_MISSING, **extra):
    data = {"date": date, "outcome": status, "verified": verified}
    if reward is not _MISSING:
        data["reward_credits"] = reward
    data.update(extra)
    return _Attrs(data)


def _balance(known=True, remaining=_MISSING, total=_MISSING):
    data = {"known": known}
    if remaining is not _MISSING:
        data["remaining"] = remaining
    if total is not _MISSING:
        data["total"] = total
    return _Attrs(data)


def _settings(
    *,
    enabled=True,
    signin_success=True,
    errors=True,
    admin_qq="10001",
    platform_id="",
    error_cooldown_seconds=3600,
):
    """Build settings through the real ``load_settings`` normalization path."""
    raw = {
        "notifications": {
            "enabled": enabled,
            "signin_success": signin_success,
            "errors": errors,
            "admin_qq": admin_qq,
            "platform_id": platform_id,
            "error_cooldown_seconds": error_cooldown_seconds,
        }
    }
    return load_settings(raw)["notifications"]


class _Harness:
    def __init__(self, notifier, context, client, state, clock, settings, state_dir):
        self.notifier = notifier
        self.context = context
        self.client = client
        self.state = state
        self.clock = clock
        self.settings = settings
        self._state_dir = state_dir

    def new_notifier(self):
        """A second notifier sharing the same on-disk StateStore (rebuild)."""
        return AdminNotifier(
            self.context,
            StateStore(self._state_dir),
            self.settings,
            self.client,
            clock=self.clock,
        )


class _AdminNotificationsTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.assertTrue(callable(getattr(MessageSession, "__init__", None)))
        self.assertTrue(callable(getattr(MessageChain, "__init__", None)))

    # -- fixture factory --------------------------------------------------
    def _make(
        self,
        instances=None,
        *,
        state_dir=None,
        state=None,
        send_result=True,
        send_error=None,
        balance=_balance(known=True, remaining=105, total=150),
        balance_error=None,
        clock=None,
        settings=None,
    ):
        if state_dir is None:
            state_dir = Path(_MODULE_TMP) / ("state-" + uuid.uuid4().hex)
        if state is None:
            state = StateStore(state_dir)
        context = _FakeContext(
            instances if instances is not None else [_Instance("qq-bot-1")],
            send_result=send_result,
            send_error=send_error,
        )
        client = _FakeClient(result=balance, error=balance_error)
        clock = clock or _Clock()
        settings = settings or _settings()
        notifier = AdminNotifier(context, state, settings, client, clock=clock)
        harness = _Harness(notifier, context, client, state, clock, settings, state_dir)
        return harness

    @staticmethod
    def _text(harness, index=0):
        chain = harness.context.sent[index][1]
        return support._result_text(chain)

    # =====================================================================
    # 1. config defaults / normalized types
    # =====================================================================
    def test_notifications_defaults_and_strict_types(self):
        section = load_settings({})["notifications"]
        self.assertIs(section["enabled"], False)
        self.assertIs(section["signin_success"], True)
        self.assertIs(section["errors"], True)
        self.assertEqual(section["admin_qq"], "")
        self.assertEqual(section["platform_id"], "")
        self.assertEqual(section["error_cooldown_seconds"], 1800)
        self.assertIs(type(section["enabled"]), bool)
        self.assertIs(type(section["signin_success"]), bool)
        self.assertIs(type(section["errors"]), bool)
        self.assertIs(type(section["admin_qq"]), str)
        self.assertIs(type(section["platform_id"]), str)
        self.assertIs(type(section["error_cooldown_seconds"]), int)

    # =====================================================================
    # 2. booleans must be strictly bool (ints/strings rejected)
    # =====================================================================
    def test_notifications_require_strict_booleans(self):
        for key in ("enabled", "signin_success", "errors"):
            for bad in (1, 0, "true", "false", [], 1.0):
                with self.subTest(key=key, bad=bad):
                    with self.assertRaises(ConfigError):
                        load_settings({"notifications": {key: bad}})

    # =====================================================================
    # 3. admin_qq: empty OK; else ASCII digits, positive, no suffix/group id
    # =====================================================================
    def test_notifications_admin_qq_validation(self):
        # Accepted (empty and positive ASCII digit strings).
        for good in ("", "1", "10001", "00042", "999999999999"):
            with self.subTest(good=good):
                out = load_settings({"notifications": {"admin_qq": good}})
                self.assertEqual(out["notifications"]["admin_qq"], good)

        # Rejected: empty value, zero, sign, suffix, group id, int type, unicode.
        for bad in (
            "0",
            "00",
            "-1",
            "10001:GroupMessage:",
            "10001:FriendMessage:",
            "1234567890123",  # a plausible group id length must not bypass rules: it is still digits -> accepted
        ):
            if bad.isdigit():
                # Pure digits are valid by contract (no group-id heuristics leak).
                continue
            with self.subTest(bad=bad):
                with self.assertRaises(ConfigError):
                    load_settings({"notifications": {"admin_qq": bad}})

        for bad in (0, 10001, None, ["10001"], "１２３"):
            with self.subTest(bad_type=bad):
                with self.assertRaises(ConfigError):
                    load_settings({"notifications": {"admin_qq": bad}})

    # =====================================================================
    # 4. platform_id is a string; cooldown is an int in 60..86400
    # =====================================================================
    def test_notifications_platform_id_and_cooldown_bounds(self):
        for good in ("", "qq-bot-1"):
            with self.subTest(platform_id=good):
                out = load_settings({"notifications": {"platform_id": good}})
                self.assertEqual(out["notifications"]["platform_id"], good)
        for bad in (0, 123, None, ["x"], True):
            with self.subTest(bad_platform=bad):
                with self.assertRaises(ConfigError):
                    load_settings({"notifications": {"platform_id": bad}})

        for good in (60, 1800, 86400):
            with self.subTest(cooldown=good):
                out = load_settings(
                    {"notifications": {"error_cooldown_seconds": good}}
                )
                self.assertEqual(out["notifications"]["error_cooldown_seconds"], good)
        for bad in (59, 86401, 0, -60, True, False, 1800.5, "1800"):
            with self.subTest(bad_cooldown=bad):
                with self.assertRaises(ConfigError):
                    load_settings(
                        {"notifications": {"error_cooldown_seconds": bad}}
                    )

    # =====================================================================
    # 5. config errors never echo the raw (secret-looking) value
    # =====================================================================
    def test_notifications_config_errors_never_echo_raw(self):
        secret_qq = "SECRETQQ:123456:FriendMessage:"
        with self.assertRaises(ConfigError) as caught:
            load_settings({"notifications": {"admin_qq": secret_qq}})
        message = str(caught.exception)
        self.assertNotIn("SECRETQQ", message)
        self.assertNotIn("FriendMessage", message)

        secret_cd = "SECRET-COOLDOWN"
        with self.assertRaises(ConfigError) as caught:
            load_settings({"notifications": {"error_cooldown_seconds": secret_cd}})
        self.assertNotIn("SECRET-COOLDOWN", str(caught.exception))

    # =====================================================================
    # 6. master switch off: no message, no balance query
    # =====================================================================
    async def test_disabled_master_switch_sends_nothing(self):
        harness = self._make(settings=_settings(enabled=False))
        await harness.notifier.signin(_signin_result("2026-10-04", reward=10))
        await harness.notifier.error("network")
        self.assertEqual(harness.context.send_calls, 0)
        self.assertEqual(harness.client.calls, 0)
        status = harness.notifier.status()
        self.assertIs(status["enabled"], False)

    # =====================================================================
    # 7. signin switch off: no message, no balance query (errors still work)
    # =====================================================================
    async def test_signin_switch_off_sends_nothing(self):
        harness = self._make(settings=_settings(signin_success=False))
        await harness.notifier.signin(_signin_result("2026-10-04", reward=10))
        self.assertEqual(harness.context.send_calls, 0)
        self.assertEqual(harness.client.calls, 0)
        # The error switch is independent and still delivers.
        await harness.notifier.error("auth")
        self.assertEqual(harness.context.send_calls, 1)

    # =====================================================================
    # 8. signin only fires for verified signed / already_signed rounds
    # =====================================================================
    async def test_signin_only_for_verified_real_rounds(self):
        cases = (
            ("signed", True, True),
            ("already_signed", True, True),
            ("signed", False, False),
            ("already_signed", False, False),
            ("not_signed", True, False),
            ("failed", True, False),
            ("skipped", True, False),
        )
        for status, verified, should_send in cases:
            with self.subTest(status=status, verified=verified):
                harness = self._make()
                await harness.notifier.signin(
                    _signin_result("2026-10-04", status=status, verified=verified, reward=1)
                )
                self.assertEqual(harness.context.send_calls, 1 if should_send else 0)

    # =====================================================================
    # 9. real MessageSession / MessageChain routed to the configured QQ
    # =====================================================================
    async def test_signin_routes_real_session_to_admin(self):
        harness = self._make(
            instances=[_Instance("qq-bot-1")],
            settings=_settings(admin_qq="10001", platform_id=""),
            balance=_balance(known=True, remaining=105, total=150),
        )
        await harness.notifier.signin(_signin_result("2026-10-04", reward=0))

        self.assertEqual(harness.context.send_calls, 1)
        session, chain = harness.context.sent[0]
        self.assertIsInstance(session, MessageSession)
        self.assertIsInstance(chain, MessageChain)
        self.assertEqual(session.platform_name, "qq-bot-1")
        self.assertEqual(session.session_id, "10001")
        self.assertEqual(session.message_type, MessageType.FRIEND_MESSAGE)

        status = harness.notifier.status()
        self.assertEqual(status["resolved_platform_id"], "qq-bot-1")
        self.assertEqual(status["category"] if "category" in status else None, None)  # never a synthetic key
        self.assertEqual(status["last_delivery"]["outcome"], "sent")
        self.assertEqual(status["last_delivery"]["category"], "signin")

    # =====================================================================
    # 10. platform candidate selection + no-guess on missing/non-QQ
    # =====================================================================
    async def test_platform_selection_single_multiple_and_missing(self):
        # Single candidate, no platform_id -> auto select.
        harness = self._make(instances=[_Instance("qq-bot-1")])
        await harness.notifier.signin(_signin_result("2026-10-04", reward=1))
        self.assertEqual(harness.context.send_calls, 1)
        self.assertEqual(harness.notifier.status()["resolved_platform_id"], "qq-bot-1")

        # Multiple candidates, unspecified -> must specify, no send, no balance.
        multi = self._make(
            instances=[_Instance("qq-bot-1"), _Instance("qq-bot-2")]
        )
        await multi.notifier.signin(_signin_result("2026-10-04", reward=1))
        self.assertEqual(multi.context.send_calls, 0)
        self.assertEqual(multi.client.calls, 0)
        self.assertIsNone(multi.notifier.status()["resolved_platform_id"])

        # Multiple candidates, explicit -> routes to that exact instance id.
        chosen = self._make(
            instances=[_Instance("qq-bot-1"), _Instance("qq-bot-2")],
            settings=_settings(platform_id="qq-bot-2"),
        )
        await chosen.notifier.signin(_signin_result("2026-10-04", reward=1))
        self.assertEqual(chosen.context.send_calls, 1)
        self.assertEqual(chosen.context.sent[0][0].platform_name, "qq-bot-2")
        self.assertEqual(chosen.notifier.status()["resolved_platform_id"], "qq-bot-2")

        # Unknown platform_id -> no guess, no send, no extra balance query.
        unknown = self._make(
            instances=[_Instance("qq-bot-1")],
            settings=_settings(platform_id="does-not-exist"),
        )
        await unknown.notifier.signin(_signin_result("2026-10-04", reward=1))
        self.assertEqual(unknown.context.send_calls, 0)
        self.assertEqual(unknown.client.calls, 0)
        self.assertIsNone(unknown.notifier.status()["resolved_platform_id"])

        # Only a non-QQ platform -> unsupported, no send, no balance query.
        nonqq = self._make(instances=[_Instance("ding-bot-1", name="dingtalk")])
        await nonqq.notifier.signin(_signin_result("2026-10-04", reward=1))
        self.assertEqual(nonqq.context.send_calls, 0)
        self.assertEqual(nonqq.client.calls, 0)

    # =====================================================================
    # 11. empty admin QQ keeps the API enabled but does not deliver
    # =====================================================================
    async def test_empty_admin_qq_keeps_api_but_skips_delivery(self):
        harness = self._make(settings=_settings(admin_qq="", platform_id=""))
        await harness.notifier.signin(_signin_result("2026-10-04", reward=1))
        self.assertEqual(harness.context.send_calls, 0)
        self.assertEqual(harness.client.calls, 0)
        status = harness.notifier.status()
        self.assertIs(status["enabled"], True)
        self.assertEqual(status["admin_qq"], "")
        self.assertIsNone(status["resolved_platform_id"])

    # =====================================================================
    # 12. per-day dedup: persisted across rebuild and safe under concurrency
    # =====================================================================
    async def test_daily_dedup_rebuild_and_concurrency(self):
        harness = self._make()
        result = _signin_result("2026-10-04", reward=5)
        await harness.notifier.signin(result)
        self.assertEqual(harness.context.send_calls, 1)

        # Same date on the same notifier: no second message, no re-query.
        await harness.notifier.signin(result)
        self.assertEqual(harness.context.send_calls, 1)
        self.assertEqual(harness.client.calls, 1)

        # Rebuild a notifier over the same real StateStore: still deduped.
        rebuilt = harness.new_notifier()
        await rebuilt.signin(_signin_result("2026-10-04", reward=5))
        self.assertEqual(harness.context.send_calls, 1)

        # Persisted under the real StateStore's ``notifications`` section.
        self.assertIsInstance(harness.state.load().get("notifications"), dict)

        # A different day delivers normally.
        await harness.notifier.signin(_signin_result("2026-10-05", reward=5))
        self.assertEqual(harness.context.send_calls, 2)

        # Concurrent identical rounds: at most one delivery.
        concurrent = self._make()
        await asyncio.gather(
            concurrent.notifier.signin(_signin_result("2026-10-06", reward=5)),
            concurrent.notifier.signin(_signin_result("2026-10-06", reward=5)),
        )
        self.assertEqual(concurrent.context.send_calls, 1)

    # =====================================================================
    # 13. reward + balance rendering (0 preserved, invalid/unknown -> 未知)
    # =====================================================================
    async def test_reward_and_balance_rendering(self):
        # reward 0 is preserved (not shown as unknown).
        harness = self._make(balance=_balance(known=True, remaining=7, total=9))
        await harness.notifier.signin(_signin_result("2026-10-04", reward=0))
        text = self._text(harness)
        self.assertIn("0", text)
        self.assertNotIn("未知", text)

        # missing / illegal reward -> 未知.
        for reward in (_MISSING, "abc", None, -1):
            with self.subTest(reward=reward):
                harness = self._make(
                    balance=_balance(known=True, remaining=7, total=9)
                )
                await harness.notifier.signin(
                    _signin_result("2026-10-04", reward=reward)
                )
                self.assertEqual(harness.context.send_calls, 1)
                self.assertIn("未知", self._text(harness))

        # known balance uses ``remaining`` (105), never ``total`` as available.
        harness = self._make(balance=_balance(known=True, remaining=105, total=150))
        await harness.notifier.signin(_signin_result("2026-10-04", reward=5))
        text = self._text(harness)
        self.assertIn("105", text)

        # unknown / non-finite / negative remaining -> 未知.
        for bad_balance in (
            _balance(known=False, remaining=105, total=150),
            _balance(known=True, remaining=float("inf"), total=150),
            _balance(known=True, remaining=float("nan"), total=150),
            _balance(known=True, remaining=-5, total=150),
            _balance(known=True, remaining="x", total=150),
        ):
            with self.subTest(balance=bad_balance):
                harness = self._make(balance=bad_balance)
                await harness.notifier.signin(_signin_result("2026-10-04", reward=5))
                self.assertEqual(harness.context.send_calls, 1)
                self.assertIn("未知", self._text(harness))

        # balance failure still sends the real signin result and never recurses
        # into a second (error) notification.
        harness = self._make(
            balance=_MISSING, balance_error=RuntimeError("RAW-BALANCE-SECRET")
        )
        await harness.notifier.signin(_signin_result("2026-10-04", reward=5))
        self.assertEqual(harness.context.send_calls, 1)
        text = self._text(harness)
        self.assertIn("未知", text)
        self.assertNotIn("RAW-BALANCE-SECRET", text)

    # =====================================================================
    # 14. send False / exception -> failed, never raises, no success record
    # =====================================================================
    async def test_send_false_or_exception_is_failed_not_success(self):
        # False return -> failed outcome, no exception escapes.
        harness = self._make(send_result=False)
        await harness.notifier.signin(_signin_result("2026-10-04", reward=1))
        delivery = harness.notifier.status()["last_delivery"]
        self.assertEqual(delivery["outcome"], "failed")
        self.assertTrue(delivery["reason"])

        # Exception during send -> failed outcome, no exception escapes.
        harness = self._make(send_error=RuntimeError("RAW-SEND-SECRET"))
        await harness.notifier.signin(_signin_result("2026-10-05", reward=1))
        delivery = harness.notifier.status()["last_delivery"]
        self.assertEqual(delivery["outcome"], "failed")
        self.assertTrue(delivery["reason"])
        self.assertNotIn("RAW-SEND-SECRET", repr(harness.notifier.status()))

    # =====================================================================
    # 15. error(): fixed safe text, categories, per-category cooldown
    # =====================================================================
    async def test_error_categories_fixed_text_and_cooldown(self):
        harness = self._make(
            settings=_settings(error_cooldown_seconds=60), clock=_Clock()
        )

        # A confirmed category delivers fixed, safe Chinese text.
        await harness.notifier.error("auth")
        self.assertEqual(harness.context.send_calls, 1)
        first_text = self._text(harness, 0)
        self.assertTrue(first_text.strip())
        delivery = harness.notifier.status()["last_delivery"]
        self.assertEqual(delivery["outcome"], "sent")
        self.assertEqual(delivery["category"], "auth")

        # Immediate repeat of the same category is cooled down: no message.
        await harness.notifier.error("auth")
        self.assertEqual(harness.context.send_calls, 1)

        # A different category is not blocked by another category's cooldown.
        await harness.notifier.error("network")
        self.assertEqual(harness.context.send_calls, 2)

        # After the cooldown elapses the category may fire again, with the
        # exact same fixed text (never a raw error body).
        harness.clock.advance(61)
        await harness.notifier.error("auth")
        self.assertEqual(harness.context.send_calls, 3)
        self.assertEqual(self._text(harness, 2), first_text)

        # Unknown / arbitrary category is rejected without delivering.
        before = harness.context.send_calls
        try:
            await harness.notifier.error("RuntimeError at C:/secret/path")
        except Exception:  # noqa: BLE001 - either a raise or a silent no-op is safe
            pass
        self.assertEqual(harness.context.send_calls, before)

        # Callers of ``error`` for every documented category are accepted.
        for category in VALID_CATEGORIES:
            with self.subTest(category=category):
                fresh = self._make(settings=_settings(error_cooldown_seconds=60))
                await fresh.notifier.error(category)
                self.assertEqual(fresh.context.send_calls, 1)

        # The cooldown is persisted and keyed by each category's *own* last
        # timestamp only.  ``network`` last fired at t0, so 61s later it is
        # allowed again -- the more recent ``auth`` (t61) must not suppress it.
        rebuilt = harness.new_notifier()
        await rebuilt.error("network")
        self.assertEqual(harness.context.send_calls, 4)

        # Concurrent identical errors: at most one delivery.
        concurrent = self._make(settings=_settings(error_cooldown_seconds=60))
        await asyncio.gather(
            concurrent.notifier.error("rate"),
            concurrent.notifier.error("rate"),
        )
        self.assertEqual(concurrent.context.send_calls, 1)

        # A failed send still records the cooldown (anti-spam).
        failing = self._make(
            settings=_settings(error_cooldown_seconds=60),
            send_error=RuntimeError("RAW-ERROR-SECRET"),
        )
        await failing.notifier.error("upstream")
        attempts = failing.context.send_calls
        self.assertEqual(attempts, 1)
        await failing.notifier.error("upstream")
        self.assertEqual(failing.context.send_calls, attempts)
        self.assertEqual(failing.notifier.status()["last_delivery"]["outcome"], "failed")

    # =====================================================================
    # 16. error switch off never sends; status() contract + no raw leak
    # =====================================================================
    async def test_error_switch_off_and_status_contract(self):
        # errors switch off -> no message.
        off = self._make(settings=_settings(errors=False))
        await off.notifier.error("network")
        self.assertEqual(off.context.send_calls, 0)

        # Full status() contract after a real delivery.
        harness = self._make(
            instances=[_Instance("qq-bot-1")],
            settings=_settings(
                admin_qq="10001", platform_id="", error_cooldown_seconds=60
            ),
        )
        await harness.notifier.signin(_signin_result("2026-10-04", reward=3))
        status = harness.notifier.status()
        for key in (
            "enabled",
            "signin_success",
            "errors",
            "admin_qq",
            "platform_id",
            "resolved_platform_id",
            "last_delivery",
        ):
            self.assertIn(key, status)
        self.assertIs(status["enabled"], True)
        self.assertIs(status["signin_success"], True)
        self.assertIs(status["errors"], True)
        self.assertEqual(status["admin_qq"], "10001")
        self.assertEqual(status["platform_id"], "")
        self.assertEqual(status["resolved_platform_id"], "qq-bot-1")
        delivery = status["last_delivery"]
        self.assertIn(delivery["outcome"], ("sent", "failed"))
        self.assertIn("category", delivery)
        self.assertTrue(delivery["at"])

        # No raw response / token / exception text may appear in the status.
        secret_client = self._make(
            balance=_MISSING, balance_error=RuntimeError("TOKEN=RAW-STATUS-SECRET")
        )
        await secret_client.notifier.signin(_signin_result("2026-10-06", reward=1))
        self.assertNotIn("RAW-STATUS-SECRET", repr(secret_client.notifier.status()))

    # =====================================================================
    # 17. error cooldown tracks each category's own timestamp, never global
    # =====================================================================
    async def test_error_cooldown_tracks_each_category_timestamp(self):
        harness = self._make(
            settings=_settings(error_cooldown_seconds=60), clock=_Clock()
        )

        # auth fires at t0.
        await harness.notifier.error("auth")
        self.assertEqual(harness.context.send_calls, 1)

        # network fires at t30 (it has never been notified before).
        harness.clock.advance(30)
        await harness.notifier.error("network")
        self.assertEqual(harness.context.send_calls, 2)

        # auth at t61: its *own* last timestamp is t0 (61s >= 60) so it must be
        # allowed again, even though network fired only 31s ago.  A global
        # cross-category cooldown would wrongly suppress this delivery.
        harness.clock.advance(31)
        await harness.notifier.error("auth")
        self.assertEqual(harness.context.send_calls, 3)

    # =====================================================================
    # 18. last_delivery is persisted across a StateStore reload, not cleared
    # =====================================================================
    async def test_last_delivery_survives_state_store_reload(self):
        # A confirmed delivery's record must survive a rebuilt notifier that
        # shares the same on-disk StateStore.
        delivered = self._make()
        await delivered.notifier.signin(_signin_result("2026-10-04", reward=4))
        original = delivered.notifier.status()["last_delivery"]
        self.assertEqual(original["outcome"], "sent")

        rebuilt = delivered.new_notifier()
        restored = rebuilt.status()["last_delivery"]
        self.assertIsNotNone(restored)
        self.assertEqual(restored["outcome"], "sent")
        self.assertEqual(restored["category"], original["category"])
        self.assertEqual(restored["at"], original["at"])

        # A failed (False-return) delivery persists its fixed reason too.
        failing = self._make(send_result=False)
        await failing.notifier.signin(_signin_result("2026-10-04", reward=4))
        failed = failing.notifier.status()["last_delivery"]
        self.assertEqual(failed["outcome"], "failed")
        self.assertTrue(failed["reason"])

        rebuilt_failed = failing.new_notifier()
        restored_failed = rebuilt_failed.status()["last_delivery"]
        self.assertIsNotNone(restored_failed)
        self.assertEqual(restored_failed["outcome"], "failed")
        self.assertEqual(restored_failed["category"], failed["category"])
        self.assertEqual(restored_failed["at"], failed["at"])
        self.assertEqual(restored_failed["reason"], failed["reason"])

    # =====================================================================
    # 19. unroutable signin records a fixed failure; status resolves solo QQ
    # =====================================================================
    async def test_unroutable_signin_records_fixed_failure_reason(self):
        def failed_reason(harness):
            delivery = harness.notifier.status()["last_delivery"]
            self.assertIsNotNone(delivery)
            self.assertEqual(delivery["outcome"], "failed")
            return delivery["reason"]

        # 1) No receiving QQ configured: enabled API, no send, no balance read,
        #    but a fixed failed reason is recorded.
        no_recipient = self._make(settings=_settings(admin_qq=""))
        await no_recipient.notifier.signin(_signin_result("2026-10-04", reward=1))
        self.assertEqual(no_recipient.context.send_calls, 0)
        self.assertEqual(no_recipient.client.calls, 0)
        reason_no_recipient = failed_reason(no_recipient)

        # 2) Several QQ instances and none specified: still no guess, no send.
        ambiguous = self._make(
            instances=[_Instance("qq-bot-1"), _Instance("qq-bot-2")]
        )
        await ambiguous.notifier.signin(_signin_result("2026-10-04", reward=1))
        self.assertEqual(ambiguous.context.send_calls, 0)
        self.assertEqual(ambiguous.client.calls, 0)
        reason_ambiguous = failed_reason(ambiguous)

        # 3) An explicitly named instance that does not exist: no guess, no send.
        unknown = self._make(
            instances=[_Instance("qq-bot-1")],
            settings=_settings(platform_id="does-not-exist"),
        )
        await unknown.notifier.signin(_signin_result("2026-10-04", reward=1))
        self.assertEqual(unknown.context.send_calls, 0)
        self.assertEqual(unknown.client.calls, 0)
        reason_unknown = failed_reason(unknown)

        # Each route records its *own* fixed, safe reason: "fixed" means a
        # stable per-cause message, not that all failures share one string, so
        # distinct causes may carry distinct fixed Chinese texts.  Every reason
        # is still a non-empty Chinese string that never echoes a raw error /
        # identifier.
        for reason in (reason_no_recipient, reason_ambiguous, reason_unknown):
            self.assertTrue(reason.strip())
            self.assertTrue(any("\u4e00" <= ch <= "\u9fff" for ch in reason))
            self.assertNotIn("does-not-exist", reason)
            self.assertNotIn("qq-bot", reason)

        # With exactly one QQ instance, status() resolves it without any prior
        # delivery attempt.
        solo = self._make(instances=[_Instance("qq-bot-1")])
        self.assertEqual(
            solo.notifier.status()["resolved_platform_id"], "qq-bot-1"
        )


def tearDownModule():
    """Delete only this module's exact sandbox directory."""
    shutil.rmtree(_MODULE_TMP, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
