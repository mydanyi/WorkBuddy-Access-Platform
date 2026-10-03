"""Executable spec for the WorkBuddy model credit / promotion metadata contract.

Scope: the *future* ``UpstreamClient.model_details(now=None)`` method and the
Gateway's use of it.  The production method does not exist yet on purpose: the
tests below assert it is callable (a clear, explicit expected failure) **before**
actually invoking it, so a red result today means exactly "not implemented yet,
and here is what it must return" -- never a false green.  Do not "fix" that here.

Contract pinned for ``model_details(now=None) -> list[dict]`` (one entry per
CLI-callable model, order preserved):

* ``id``               -- the pure upstream model id (e.g. ``"wb-1"``);
* ``name``             -- the native model name;
* ``credits``          -- the raw enterprise multiplier (number) or ``None``
                          when it is missing / not a valid multiplier;
* ``effective_credits``-- the canonical user-facing multiplier string such as
                          ``"0.11x"`` / ``"1x"`` / ``"0x"``, or ``None``;
* ``promo_label``      -- a short label (badge) or ``""``;
* ``promo_note``       -- a longer note (hover text) or ``""``.

Rules exercised below:

* The enterprise ``MODELS_PATH`` document decides the callable set; the existing
  disabled / non-chat filtering is kept.  ``GET /v3/config`` only *supplements*
  same-id metadata and ``modelPromotions`` -- it must never widen the set.
* ``/v3/config`` is fetched with ``User-Agent:
  CodeBuddyIDE/4.12.0 CodeBuddy/4.12.0`` and ``X-Product: SaaS``.
* A failing ``/v3/config`` must not drop the still-usable enterprise models.
* Whole-number ``0`` is a valid multiplier (a free model); ``bool`` / ``NaN`` /
  negative / infinite values are **not** multipliers.
* Promotions are gated by ``enabled`` / ``priority`` / ``schedule.timezone`` /
  ``validFrom`` / ``validUntil`` / daily ``start``/``end`` with cross-midnight
  windows; the highest ``priority`` wins when several apply.
* ``discount.discountedCredits`` wins over ``factor * base`` when present;
  ``factor == 0`` is a real "free" discount (never treated as missing); a bare
  ``hover`` number is never parsed into a discount.

The fake *WorkBuddy upstream* is a real ``aiohttp.web`` server bound to
127.0.0.1:0 that records every request (method/path/query/headers/body).  A real
``UpstreamClient`` (real ``AuthManager`` + encrypted ``CredentialStore``) sits in
front of it -- only the external HTTP responses and the fixed ``now`` are
substituted.  Every credential / token is a FICTIONAL FIXED SAMPLE and is never
read from a live source.  Temporary files use the same NAS-hosted ``tmp`` root as
the other tests (``WORKBUDDY_TEST_TMP``); the system temp dir is never used.
"""

import asyncio
import base64
import collections
import contextlib
import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import web

# --- Make ``astrbot_plugin_workbuddy`` importable when discovered from anywhere.
_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]  # directory that contains astrbot_plugin_workbuddy/
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.auth import AuthManager  # noqa: E402
from astrbot_plugin_workbuddy.core.chat import ChatService  # noqa: E402
from astrbot_plugin_workbuddy.core.client import (  # noqa: E402
    MODELS_PATH,
    UpstreamClient,
)
from astrbot_plugin_workbuddy.core.gateway import Gateway  # noqa: E402
from astrbot_plugin_workbuddy.core.storage import CredentialStore  # noqa: E402
from astrbot_plugin_workbuddy.core.errors import UpstreamError  # noqa: E402


def _resolve_base_tmp() -> Path:
    env = os.environ.get("WORKBUDDY_TEST_TMP")
    if env:
        return Path(env)
    task_root = _HERE.parents[3]
    return task_root / "tmp"


BASE_TMP = _resolve_base_tmp()

# --- FICTIONAL FIXED SAMPLE KEY (standard base64 of exactly 32 bytes). ---
_KEY_BYTES = b"workbuddy-credit-master-key-0001"
if len(_KEY_BYTES) != 32:
    raise AssertionError("sample master key must be exactly 32 bytes")
MASTER_KEY = base64.b64encode(_KEY_BYTES).decode("ascii")

# --- FICTIONAL FIXED SAMPLE TOKENS / ACCOUNT (never real). ---
AT = "FICTIONAL-ACCESS-TOKEN-CREDITS"
RT = "FICTIONAL-REFRESH-TOKEN-CREDITS"
UID = "test-uid-credits"
DOMAIN = "example.test.invalid"
API_KEY = "FICTIONAL-LOCAL-GATEWAY-KEY"

SEED_CREDENTIALS = {
    "accessToken": AT,
    "refreshToken": RT,
    "uid": UID,
    "expiresAt": 4102444800000,  # 2100-01-01 in ms, fictional
    "domain": DOMAIN,
    "realm": "cn",
}

# Pinned upstream paths exercised by ``model_details``.
V3_CONFIG_PATH = "/v3/config"
V3_HEADERS_UA = "CodeBuddyIDE/4.12.0 CodeBuddy/4.12.0"
V3_HEADERS_PRODUCT = "SaaS"

# Pinned gateway / upstream paths used by the consumer tests.
REFRESH_PATH = "/v2/plugin/auth/token/refresh"
CHAT_PATH = "/v2/chat/completions"
GW_MODELS_PATH = "/v1/models"
GW_COMPLETIONS_PATH = "/v1/chat/completions"

# The quota/balance endpoint the plugin's ``client.balance()`` reads.
BALANCE_PATH = "/v2/billing/meter/get-user-resource"

# --- Fixed, explicit UTC+8 times (the model_details ``now`` parameter). ---
TZ = ZoneInfo("Asia/Shanghai")
NOW_DAY = datetime(2026, 10, 3, 12, 0, 0, tzinfo=TZ)
NOW_NIGHT = datetime(2026, 10, 3, 23, 30, 0, tzinfo=TZ)


def _ok(data=None):
    """Envelope success response: HTTP 200 with business code 0."""
    return web.json_response({"code": 0, "data": {} if data is None else data})


def _ok_handler(data):
    """Wrap a fixed payload into an aiohttp GET/POST handler."""

    async def handler(request):
        return _ok(data)

    return handler


class _FakeUpstream:
    """A real aiohttp upstream on 127.0.0.1:0 that records every request."""

    def __init__(self) -> None:
        self.app = web.Application()
        self.handlers = {}
        self.calls = []
        self.hits = collections.Counter()
        self.base = None
        self._runner = None
        self._site = None
        self.app.router.add_route("*", "/{tail:.*}", self._dispatch)

    def on(self, method: str, path: str, handler) -> None:
        self.handlers[(method.upper(), path)] = handler

    async def _dispatch(self, request):
        body = await request.read()
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
        handler = self.handlers.get((request.method, request.path))
        if handler is None:
            return web.json_response({"code": 5999, "msg": "unmapped route"}, status=404)
        return await handler(request)

    async def start(self) -> None:
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await self._site.start()
        host, port = self._runner.addresses[0][:2]
        self.base = "http://{}:{}".format(host, port)

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()

    def calls_for(self, method: str, path: str):
        return [
            c for c in self.calls if c["method"] == method.upper() and c["path"] == path
        ]

    @staticmethod
    def json_body(call):
        return json.loads(call["body"].decode("utf-8")) if call["body"] else None


# ---------------------------------------------------------------------------
# Promotion fixtures (literal values, asserted verbatim below).
# ---------------------------------------------------------------------------

# Nightly window that wraps past midnight (22:00 -> 06:00).
PROMO_NIGHT = {
    "enabled": True,
    "priority": 1,
    "modelIds": ["wb-night"],
    "badge": {"label": "夜间折扣"},
    "discount": {"factor": 0.5},
    "schedule": {
        "timezone": "Asia/Shanghai",
        "daily": [{"start": "22:00", "end": "06:00"}],
    },
}

# A promotion active at any time (no schedule), for the consumer tests.
PROMO_ALWAYS = {
    "enabled": True,
    "priority": 1,
    "modelIds": ["wb-1"],
    "badge": {"label": "夜间折扣"},
}


class _CreditsMetadataTestCase(unittest.IsolatedAsyncioTestCase):
    """Real upstream + real client; only HTTP responses and ``now`` are fixed."""

    async def asyncSetUp(self) -> None:
        BASE_TMP.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(tempfile.mkdtemp(prefix="wb-credit-", dir=str(BASE_TMP)))
        self.up = _FakeUpstream()
        await self.up.start()
        self.session = aiohttp.ClientSession()
        self.store = CredentialStore(self.data_dir, MASTER_KEY)
        self.store.save(dict(SEED_CREDENTIALS))
        self.manager = AuthManager(
            self.session, self.store, chat_base=self.up.base, poll_interval=0.05
        )
        self.client = UpstreamClient(
            self.session,
            self.manager,
            chat_base=self.up.base,
            billing_base=self.up.base,
            web_base=self.up.base,
        )

    async def asyncTearDown(self) -> None:
        await self.session.close()
        await self.up.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)

    # -- upstream fixtures ----------------------------------------------

    def _serve_enterprise(self, models, *, cli_ids=None) -> None:
        if cli_ids is None:
            cli_ids = [m["id"] for m in models]
        payload = {"models": list(models), "agents": [{"name": "cli", "models": cli_ids}]}
        self.up.on("GET", MODELS_PATH, _ok_handler(payload))

    def _serve_v3(self, *, models=None, promotions=None, fail=False) -> None:
        if fail:
            async def v3_fail(request):
                return web.json_response({"code": 5000, "msg": "v3 broken"}, status=500)

            self.up.on("GET", V3_CONFIG_PATH, v3_fail)
            return
        payload = {
            "models": list(models or []),
            "modelPromotions": list(promotions or []),
        }
        self.up.on("GET", V3_CONFIG_PATH, _ok_handler(payload))

    # -- call helper -----------------------------------------------------

    async def _model_details(self, now):
        # Explicit expected failure while ``model_details`` is unimplemented.
        self.assertTrue(
            callable(getattr(self.client, "model_details", None)),
            "UpstreamClient.model_details(now=None) must be implemented",
        )
        return await self.client.model_details(now=now)

    @staticmethod
    def _find(details, model_id):
        for item in details or []:
            if isinstance(item, dict) and item.get("id") == model_id:
                return item
        return None

    # 1 ------------------------------------------------------------------
    async def test_01_enterprise_credits_preserved_and_normalized(self):
        self._serve_enterprise(
            [{"id": "wb-alpha", "name": "Alpha", "credits": "x0.11 credits"}]
        )
        self._serve_v3()

        details = await self._model_details(NOW_DAY)

        item = self._find(details, "wb-alpha")
        self.assertIsNotNone(item, "the CLI-callable enterprise model must be present")
        self.assertEqual(item["name"], "Alpha")
        self.assertEqual(item["credits"], 0.11)  # "x0.11 credits" -> numeric 0.11
        self.assertEqual(item["effective_credits"], "0.11x")  # canonical form

    # 2 ------------------------------------------------------------------
    async def test_02_numeric_zero_credits_is_a_free_model(self):
        self._serve_enterprise([{"id": "wb-free", "name": "Free", "credits": 0}])
        self._serve_v3()

        details = await self._model_details(NOW_DAY)

        item = self._find(details, "wb-free")
        self.assertIsNotNone(item)
        self.assertEqual(item["credits"], 0)
        self.assertEqual(item["effective_credits"], "0x")

    # 3 ------------------------------------------------------------------
    async def test_03_bool_nan_negative_and_infinite_are_not_multipliers(self):
        self._serve_enterprise(
            [
                {"id": "wb-bool", "name": "Bool", "credits": True},
                {"id": "wb-nan", "name": "NaN", "credits": float("nan")},
                {"id": "wb-neg", "name": "Neg", "credits": -1.0},
                {"id": "wb-inf", "name": "Inf", "credits": float("inf")},
            ]
        )
        self._serve_v3()

        details = await self._model_details(NOW_DAY)

        for model_id in ("wb-bool", "wb-nan", "wb-neg", "wb-inf"):
            item = self._find(details, model_id)
            self.assertIsNotNone(item, model_id)
            self.assertIsNone(item["credits"], model_id)
            self.assertIsNone(item["effective_credits"], model_id)

    # 4 ------------------------------------------------------------------
    async def test_04_v3_supplements_same_id_without_widening_the_set(self):
        self._serve_enterprise([{"id": "wb-a", "name": "A"}])
        self._serve_v3(
            models=[
                {"id": "wb-a", "credits": 0.5},
                {"id": "wb-b", "credits": 0.9},  # not CLI-callable -> must not appear
            ]
        )

        details = await self._model_details(NOW_DAY)

        self.assertEqual([i["id"] for i in details], ["wb-a"])
        item = self._find(details, "wb-a")
        self.assertEqual(item["credits"], 0.5)  # price came from v3 metadata
        self.assertEqual(item["effective_credits"], "0.5x")
        self.assertIsNone(self._find(details, "wb-b"))

        # The /v3/config request carries the pinned IDE identity.
        calls = self.up.calls_for("GET", V3_CONFIG_PATH)
        self.assertTrue(calls, "model_details must read /v3/config")
        headers = calls[-1]["headers"]
        self.assertEqual(headers.get("user-agent"), V3_HEADERS_UA)
        self.assertEqual(headers.get("x-product"), V3_HEADERS_PRODUCT)

    # 5 ------------------------------------------------------------------
    async def test_05_v3_failure_still_returns_usable_models(self):
        self._serve_enterprise(
            [{"id": "wb-a", "name": "A", "credits": 0.11}]
        )
        self._serve_v3(fail=True)

        details = await self._model_details(NOW_DAY)

        item = self._find(details, "wb-a")
        self.assertIsNotNone(item, "a v3 failure must not drop the enterprise model")
        self.assertEqual(item["credits"], 0.11)
        self.assertEqual(item["effective_credits"], "0.11x")
        self.assertEqual(self.up.hits[("GET", V3_CONFIG_PATH)], 1)

    # 6 ------------------------------------------------------------------
    async def test_06_cross_midnight_window_is_active_at_night(self):
        self._serve_enterprise(
            [{"id": "wb-night", "name": "Night", "credits": 1.0}]
        )
        self._serve_v3(promotions=[PROMO_NIGHT])

        details = await self._model_details(NOW_NIGHT)

        item = self._find(details, "wb-night")
        self.assertEqual(item["promo_label"], "夜间折扣")
        self.assertEqual(item["effective_credits"], "0.5x")

    # 7 ------------------------------------------------------------------
    async def test_07_cross_midnight_window_is_inactive_by_day(self):
        self._serve_enterprise(
            [{"id": "wb-night", "name": "Night", "credits": 1.0}]
        )
        self._serve_v3(promotions=[PROMO_NIGHT])

        details = await self._model_details(NOW_DAY)

        item = self._find(details, "wb-night")
        self.assertEqual(item["promo_label"], "")
        self.assertEqual(item["effective_credits"], "1x")

    # 8 ------------------------------------------------------------------
    async def test_08_expired_and_not_yet_started_promotions_do_not_apply(self):
        self._serve_enterprise(
            [
                {"id": "wb-expired", "name": "Expired", "credits": 1.0},
                {"id": "wb-future", "name": "Future", "credits": 1.0},
            ]
        )
        self._serve_v3(
            promotions=[
                {
                    "enabled": True,
                    "priority": 1,
                    "modelIds": ["wb-expired"],
                    "badge": {"label": "过期"},
                    "discount": {"factor": 0.5},
                    "schedule": {
                        "timezone": "Asia/Shanghai",
                        "validUntil": "2026-10-01T00:00:00+08:00",
                    },
                },
                {
                    "enabled": True,
                    "priority": 1,
                    "modelIds": ["wb-future"],
                    "badge": {"label": "未开始"},
                    "discount": {"factor": 0.5},
                    "schedule": {
                        "timezone": "Asia/Shanghai",
                        "validFrom": "2026-10-10T00:00:00+08:00",
                    },
                },
            ]
        )

        details = await self._model_details(NOW_DAY)

        for model_id in ("wb-expired", "wb-future"):
            item = self._find(details, model_id)
            self.assertEqual(item["promo_label"], "", model_id)
            self.assertEqual(item["effective_credits"], "1x", model_id)

    # 9 ------------------------------------------------------------------
    async def test_09_highest_priority_promotion_wins(self):
        self._serve_enterprise([{"id": "wb-prio", "name": "Prio", "credits": 1.0}])
        self._serve_v3(
            promotions=[
                {
                    "enabled": True,
                    "priority": 1,
                    "modelIds": ["wb-prio"],
                    "badge": {"label": "低优先"},
                    "discount": {"factor": 0.5},
                },
                {
                    "enabled": True,
                    "priority": 10,
                    "modelIds": ["wb-prio"],
                    "badge": {"label": "高优先"},
                    "discount": {"factor": 0.2},
                },
            ]
        )

        details = await self._model_details(NOW_DAY)

        item = self._find(details, "wb-prio")
        self.assertEqual(item["promo_label"], "高优先")
        self.assertEqual(item["effective_credits"], "0.2x")

    # 10 -----------------------------------------------------------------
    async def test_10_zero_factor_is_a_free_discount_not_missing(self):
        self._serve_enterprise([{"id": "wb-free", "name": "Free", "credits": 1.0}])
        self._serve_v3(
            promotions=[
                {
                    "enabled": True,
                    "priority": 1,
                    "modelIds": ["wb-free"],
                    "badge": {"label": "免费"},
                    "discount": {"factor": 0},
                }
            ]
        )

        details = await self._model_details(NOW_DAY)

        item = self._find(details, "wb-free")
        self.assertEqual(item["promo_label"], "免费")
        self.assertEqual(item["effective_credits"], "0x")

    # 11 -----------------------------------------------------------------
    async def test_11_discounted_credits_beats_factor(self):
        self._serve_enterprise([{"id": "wb-fixed", "name": "Fixed", "credits": 1.0}])
        self._serve_v3(
            promotions=[
                {
                    "enabled": True,
                    "priority": 1,
                    "modelIds": ["wb-fixed"],
                    "badge": {"label": "限时"},
                    "discount": {"factor": 0.7, "discountedCredits": "0.50x"},
                }
            ]
        )

        details = await self._model_details(NOW_DAY)

        item = self._find(details, "wb-fixed")
        # The string "0.50x" wins over factor * base (0.7 * 1.0) and normalizes.
        self.assertEqual(item["effective_credits"], "0.5x")

    # 12 -----------------------------------------------------------------
    async def test_12_badge_only_promotion_never_parses_hover_number(self):
        self._serve_enterprise([{"id": "wb-badge", "name": "Badge", "credits": 1.0}])
        self._serve_v3(
            promotions=[
                {
                    "enabled": True,
                    "priority": 1,
                    "modelIds": ["wb-badge"],
                    "badge": {"label": "badge-only"},
                    "hover": {"textZh": "立省50%"},
                    # no discount block at all
                }
            ]
        )

        details = await self._model_details(NOW_DAY)

        item = self._find(details, "wb-badge")
        self.assertEqual(item["promo_label"], "badge-only")
        self.assertEqual(item["promo_note"], "立省50%")
        # The "50%" in the hover text must NOT become a discount.
        self.assertEqual(item["effective_credits"], "1x")
        self.assertEqual(item["credits"], 1.0)

    # -- balance ----------------------------------------------------------

    def _serve_balance(self, accounts, *, total_count=None):
        data = {
            "Response": {
                "Data": {
                    "Accounts": list(accounts),
                    "TotalCount": len(accounts) if total_count is None else total_count,
                }
            }
        }
        self.up.on("POST", BALANCE_PATH, _ok_handler(data))

    # 13 -----------------------------------------------------------------
    async def test_13_balance_aggregates_packages_without_double_counting(self):
        self._serve_balance(
            [
                # Cycle-quota package; its CapacitySize/CapacityRemain must be ignored.
                {"CycleCapacitySize": 100, "CycleCapacityRemain": 80,
                 "CapacitySize": 1000, "CapacityRemain": 900},
                # Non-cycle package: CapacitySize/CapacityRemain/CapacityUsed apply.
                {"CapacitySize": 50, "CapacityRemain": 25, "CapacityUsed": 25},
            ]
        )

        summary = await self.client.balance()

        self.assertTrue(summary["known"])
        self.assertEqual(summary["remaining"], 105)  # 80 + 25
        self.assertEqual(summary["used"], 45)        # (100-80) + 25
        self.assertEqual(summary["total"], 150)      # 105 + 45

        body = self.up.json_body(self.up.calls_for("POST", BALANCE_PATH)[-1])
        self.assertEqual(body["PageNumber"], 1)
        self.assertEqual(body["PageSize"], 100)
        self.assertEqual(body["ProductCode"], "p_tcaca")
        self.assertEqual(body["Status"], [0, 3])
        self.assertTrue(body["PackageEndTimeRangeBegin"])
        self.assertTrue(body["PackageEndTimeRangeEnd"])

    # 14 -----------------------------------------------------------------
    async def test_14_unknown_shape_is_unknown_and_empty_accounts_is_zero(self):
        self.up.on("POST", BALANCE_PATH, _ok_handler({"unexpected": 1}))
        unknown = await self.client.balance()
        self.assertFalse(unknown["known"])
        self.assertIsNone(unknown["remaining"])
        self.assertIsNone(unknown["used"])
        self.assertIsNone(unknown["total"])

        # An empty Accounts list is a legal, *known* zero (second call).
        self._serve_balance([])
        empty = await self.client.balance()
        self.assertTrue(empty["known"])
        self.assertEqual(empty["remaining"], 0)
        self.assertEqual(empty["used"], 0)
        self.assertEqual(empty["total"], 0)

    # 15 -----------------------------------------------------------------
    async def test_15_zero_negative_overflow_and_bool_are_normalized(self):
        cases = [
            ("zero", {"CapacitySize": 0, "CapacityRemain": 0, "CapacityUsed": 0}, 0, 0, 0, True),
            ("negative_remain",
             {"CapacitySize": 10, "CapacityRemain": -5, "CapacityUsed": 15}, 0, 10, 10, True),
            ("remain_over_size",
             {"CapacitySize": 10, "CapacityRemain": 30, "CapacityUsed": 0}, 10, 0, 10, True),
            ("bool_value",
             {"CapacitySize": True, "CapacityRemain": 1, "CapacityUsed": 0}, None, None, None, False),
            ("missing_remain", {"CapacitySize": 10}, None, None, None, False),
        ]
        for name, account, remaining, used, total, known in cases:
            with self.subTest(case=name):
                self._serve_balance([account])
                summary = await self.client.balance()
                self.assertEqual(summary["known"], known)
                self.assertEqual(summary["remaining"], remaining)
                self.assertEqual(summary["used"], used)
                self.assertEqual(summary["total"], total)

    # 16 -----------------------------------------------------------------
    async def test_16_inconsistent_cycle_used_is_corrected(self):
        self._serve_balance(
            [{"CycleCapacitySize": 100, "CycleCapacityRemain": 20, "CycleCapacityUsed": 90}]
        )

        summary = await self.client.balance()

        # CycleCapacityUsed (90) > size - remain (80): use CycleCapacityUsed as truth.
        self.assertEqual(summary["remaining"], 10)
        self.assertEqual(summary["used"], 90)
        self.assertEqual(summary["total"], 100)

    # 17 -----------------------------------------------------------------
    async def test_17_balance_pages_are_summed_and_unknown_dosage_ignored(self):
        async def handler(request):
            body = json.loads(await request.read())
            if body.get("PageNumber") == 1:
                accounts = [{"CapacitySize": 1, "CapacityRemain": 1} for _ in range(100)]
            else:
                accounts = [{"CapacitySize": 2, "CapacityRemain": 1}]
            return web.json_response(
                {
                    "code": 0,
                    "data": {
                        "Response": {
                            "Data": {
                                "Accounts": accounts,
                                "TotalCount": 101,
                                "TotalDosage": 777,  # unconfirmed semantics: never total
                            }
                        }
                    },
                }
            )

        self.up.on("POST", BALANCE_PATH, handler)
        summary = await self.client.balance()

        self.assertTrue(summary["known"])
        self.assertEqual(summary["remaining"], 101)  # 100 + 1
        self.assertEqual(summary["total"], 102)      # 100 + 2
        self.assertEqual(summary["used"], 1)         # 0 + 1

        calls = self.up.calls_for("POST", BALANCE_PATH)
        self.assertEqual(len(calls), 2)
        first = self.up.json_body(calls[0])
        second = self.up.json_body(calls[1])
        self.assertEqual(first["PageNumber"], 1)
        self.assertEqual(second["PageNumber"], 2)
        self.assertEqual(
            first["PackageEndTimeRangeBegin"], second["PackageEndTimeRangeBegin"]
        )
        self.assertEqual(
            first["PackageEndTimeRangeEnd"], second["PackageEndTimeRangeEnd"]
        )

    # 18 -----------------------------------------------------------------
    async def test_18_second_page_failure_never_yields_a_known_total(self):
        async def handler(request):
            body = json.loads(await request.read())
            if body.get("PageNumber") == 1:
                return web.json_response(
                    {
                        "code": 0,
                        "data": {
                            "Response": {
                                "Data": {
                                    "Accounts": [{"CapacitySize": 1, "CapacityRemain": 1}],
                                    "TotalCount": 101,
                                }
                            }
                        },
                    }
                )
            return web.json_response({"code": 5000, "msg": "page2 boom"}, status=500)

        self.up.on("POST", BALANCE_PATH, handler)
        try:
            summary = await self.client.balance()
        except UpstreamError:
            return
        # Never a known, complete total from a partial (page-1-only) read.
        self.assertFalse(summary["known"])

    # 19 -----------------------------------------------------------------
    async def test_19_invalid_daily_type_does_not_activate_free_promo(self):
        # ``schedule.daily`` must be a list of {start,end} windows.  A wrong
        # *type* (string / bool / number) is not an all-day default, so the
        # free promo must NOT activate; only None / [] mean "no window".
        cases = [
            ("string", "broken", False),
            ("bool", False, False),
            ("number", 123, False),
            ("none", None, True),
            ("empty_list", [], True),
        ]
        for name, daily, active in cases:
            with self.subTest(daily=name):
                self._serve_enterprise(
                    [{"id": "wb-daily", "name": "Daily", "credits": 1.0}]
                )
                self._serve_v3(
                    promotions=[
                        {
                            "enabled": True,
                            "priority": 1,
                            "modelIds": ["wb-daily"],
                            "badge": {"label": "免费"},
                            "discount": {"factor": 0},
                            "schedule": {
                                "timezone": "Asia/Shanghai",
                                "daily": daily,
                            },
                        }
                    ]
                )

                details = await self._model_details(NOW_DAY)

                item = self._find(details, "wb-daily")
                self.assertIsNotNone(item)
                if active:
                    # All-day default: the free promo really applies.
                    self.assertEqual(item["promo_label"], "免费")
                    self.assertEqual(item["effective_credits"], "0x")
                else:
                    # Invalid daily type: no activation, base multiplier kept.
                    self.assertEqual(item["promo_label"], "")
                    self.assertEqual(item["effective_credits"], "1x")


# ---------------------------------------------------------------------------
# Optional consumers: the Gateway surfaces the credit metadata and maps the
# display alias back to the pure upstream id.
# ---------------------------------------------------------------------------

DONE = b"data: [DONE]\n\n"


def _frame(obj) -> bytes:
    return b"data: " + json.dumps(obj, ensure_ascii=False).encode("utf-8") + b"\n\n"


def _basic_sse(text="ok"):
    return [
        _frame({"choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]}),
        _frame({"choices": [{"index": 0, "delta": {"content": text}, "finish_reason": None}]}),
        _frame({"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}),
        DONE,
    ]


ALIAS_MODEL = "wb-1 [0.11x · 夜间折扣]"


class _GatewayCreditsConsumerTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        BASE_TMP.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(tempfile.mkdtemp(prefix="wb-credit-gw-", dir=str(BASE_TMP)))
        self.up = _FakeUpstream()
        await self.up.start()
        self.session = aiohttp.ClientSession()
        self.store = CredentialStore(self.data_dir, MASTER_KEY)
        self.store.save(dict(SEED_CREDENTIALS))
        self.manager = AuthManager(
            self.session, self.store, chat_base=self.up.base, poll_interval=0.05
        )
        self.client = UpstreamClient(
            self.session,
            self.manager,
            chat_base=self.up.base,
            billing_base=self.up.base,
            web_base=self.up.base,
        )
        self.chat = ChatService(self.client)

        # Real upstream fixtures: one CLI model priced 0.11 with an always-on promo.
        enterprise = {
            "models": [{"id": "wb-1", "name": "WB One", "credits": 0.11}],
            "agents": [{"name": "cli", "models": ["wb-1"]}],
        }
        v3 = {"models": [], "modelPromotions": [PROMO_ALWAYS]}
        self.up.on("GET", MODELS_PATH, _ok_handler(enterprise))
        self.up.on("GET", V3_CONFIG_PATH, _ok_handler(v3))
        self.up.on("POST", CHAT_PATH, self._chat_handler)

        self.gateway = Gateway(
            self.chat,
            self.client,
            api_key=API_KEY,
            host="127.0.0.1",
            port=0,
            concurrency=4,
            rpm=600,
            queue_size=8,
            queue_timeout=5,
        )
        await self.gateway.start()
        self.base_url = "http://127.0.0.1:{}".format(self.gateway.bound_port)

    async def asyncTearDown(self) -> None:
        with contextlib.suppress(Exception):
            await self.gateway.stop()
        await self.session.close()
        await self.up.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)

    async def _chat_handler(self, request):
        await request.read()
        resp = web.StreamResponse(
            status=200, headers={"Content-Type": "text/event-stream"}
        )
        await resp.prepare(request)
        for chunk in _basic_sse():
            await resp.write(chunk)
        with contextlib.suppress(Exception):
            await resp.write_eof()
        return resp

    def _headers(self):
        return {"Authorization": "Bearer " + API_KEY}

    async def test_gateway_models_expose_display_id_and_pure_upstream_id(self):
        async with self.session.get(
            self.base_url + GW_MODELS_PATH, headers=self._headers()
        ) as resp:
            self.assertEqual(resp.status, 200)
            body = await resp.json()

        data = body.get("data")
        self.assertIsInstance(data, list)
        item = next(
            (
                i
                for i in data
                if isinstance(i, dict) and i.get("upstream_id") == "wb-1"
            ),
            None,
        )
        self.assertIsNotNone(item, "the upstream catalog model must be advertised")
        self.assertEqual(item["id"], ALIAS_MODEL)
        self.assertEqual(item["upstream_id"], "wb-1")
        # Metadata is projected alongside the display id.
        self.assertEqual(item.get("effective_credits"), "0.11x")
        self.assertEqual(item.get("promo_label"), "夜间折扣")

    async def test_gateway_chat_alias_maps_back_to_pure_upstream_id(self):
        for model in (
            ALIAS_MODEL,
            "wb-1 [9x · 旧活动]",
            "wb-1 [倍率未知]",
            "wb-1",
        ):
            for stream in (False, True):
                with self.subTest(model=model, stream=stream):
                    before = len(self.up.calls_for("POST", CHAT_PATH))
                    async with self.session.post(
                        self.base_url + GW_COMPLETIONS_PATH,
                        json={
                            "model": model,
                            "messages": [{"role": "user", "content": "hi"}],
                            "stream": stream,
                        },
                        headers=self._headers(),
                    ) as resp:
                        self.assertEqual(resp.status, 200)
                        await resp.read()

                    calls = self.up.calls_for("POST", CHAT_PATH)
                    # Each alias/stream round really issues one new upstream
                    # request (count grows, and the *last* body carries the id).
                    self.assertEqual(len(calls), before + 1)
                    sent = self.up.json_body(calls[-1])
                    self.assertEqual(sent["model"], "wb-1")  # pure id, never the alias


if __name__ == "__main__":
    unittest.main()
