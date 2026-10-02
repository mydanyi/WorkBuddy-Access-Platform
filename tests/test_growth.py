"""Executable contract tests for the growth task runner.

Scope: only the documented public contract of ``core.growth.GrowthService`` and
``core.growth_tasks.TaskActions``.  The production modules do not exist yet on
purpose: the top-level imports below are expected to fail until ``core/growth.py``
and ``core/growth_tasks.py`` are implemented and verified remotely in the Docker
host.  Do not "fix" that here.

The fake *WorkBuddy upstream* is a real ``aiohttp.web`` server; a real
``CredentialStore`` / ``AuthManager`` / ``UpstreamClient`` / ``ChatService`` and a
real ``StateStore`` sit behind a real ``GrowthService``.  No production class is
mocked.  Every credential / token below is a FICTIONAL FIXED SAMPLE.

Pinned growth endpoints (chat / billing domain)::

    GET  /v2/activity/growth/tasks                    -> data.tasks[]
    POST /v2/activity/growth/tasks/accept             {"task_codes":[code]}
    POST /activity/growth/tasks/{urlencoded}/claim    empty body / {}, x-client-platform: web
    POST /v2/report                                   [ {eventCode:"chat_request_send", ...} ]
    GET  /console/enterprises/personal/models         dynamic CLI-chat catalog

``data.tasks[]`` entries are parsed loosely: progress may be a
``{current,target}`` object *or* flat ``current``/``target`` fields; a task may
carry ``accept_status`` (``not_accepted`` / ``accepted`` / ``claimed``),
``status`` (``complete``), ``locked`` and ``expired`` (bool or ``expires_at``).
Claiming requires a completed task and a read-back that confirms the ``claimed``
flag -- a business ``code: 0`` on the claim itself is never treated as the score.

Pinned result shape (``run`` / ``start`` / ``cancel`` return a dict)::

    {
      "claim_only": bool,
      "listed": int,
      "actions": [ {"task_code", "action", "outcome"} ],
      "progress": {task_code: {"current": int, "target": int}},
      "rewards": [ {"task_code", "credit", "energy"} ],
      "pending": [ {"task_code", "reason"} ],   # locked/expired/claimed/unknown/manual
      "unconfirmed": [task_code, ...],          # progress_unconfirmed / claim_unconfirmed
      "needs_attention": bool,
    }

``status()`` returns::

    {
      "running": bool, "paused": bool, "cancelled": bool, "date": str,
      "chat_requests_today": int, "max_chat_requests": int,
      "pending": [ {"task_code", "reason"} ], "last_result": dict | None,
    }

``core.growth_tasks.TaskActions`` is the automated-action whitelist::

    TaskActions.allowed(task_code) -> bool        # may the runner act automatically?
    TaskActions.action_for(task_code) -> str|None # the action kind, or None if manual/unknown

Unknown and human-only tasks (skills / real donations / external identity
grants / theme changes when ``allow_theme_change`` is false) are excluded.

Temporary files use the NAS-hosted ``tmp`` root and honour
``WORKBUDDY_TEST_TMP``; the system temp dir is never used as a fallback.
"""

import asyncio
import collections
import contextlib
import json
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
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
from astrbot_plugin_workbuddy.core.client import UpstreamClient  # noqa: E402
from astrbot_plugin_workbuddy.core.growth import GrowthService  # noqa: E402
from astrbot_plugin_workbuddy.core.growth_tasks import TaskActions  # noqa: E402
from astrbot_plugin_workbuddy.core.limits import ConcurrencyGate  # noqa: E402
from astrbot_plugin_workbuddy.core.storage import CredentialStore, StateStore  # noqa: E402

# Reuse the chat suite's real frame helpers and fixed sample credentials, but NOT
# its test case (subclassing it would re-run every gateway contract test).
try:  # package discovery
    from tests.test_chat_gateway import (  # noqa: E402
        _frame,
        _chunk,
        _basic_sse,
        _DONE,
        MASTER_KEY,
        SEED_CREDENTIALS,
        UID,
        MODEL,
        BASE_TMP,
    )
except ImportError:  # top-level discovery
    from test_chat_gateway import (  # noqa: E402
        _frame,
        _chunk,
        _basic_sse,
        _DONE,
        MASTER_KEY,
        SEED_CREDENTIALS,
        UID,
        MODEL,
        BASE_TMP,
    )

TZ = ZoneInfo("Asia/Shanghai")
UTC = timezone.utc

GROWTH_LIST_PATH = "/v2/activity/growth/tasks"
ACCEPT_PATH = "/v2/activity/growth/tasks/accept"
CLAIM_ROUTE = "/activity/growth/tasks/{code}/claim"
REPORT_PATH = "/v2/report"
MODELS_CATALOG_PATH = "/console/enterprises/personal/models"
CHAT_PATH = "/v2/chat/completions"
CHECKIN_CLAIM_PATH = "/v2/billing/meter/daily-checkin"
CHECKIN_STATUS_PATH = "/v2/billing/meter/checkin-activity-status"


def at(year, month, day, hour, minute, second=0) -> datetime:
    """A Shanghai-local wall-clock instant."""
    return datetime(year, month, day, hour, minute, second, tzinfo=TZ)


class _Clock:
    """Controllable clock returning POSIX seconds (UTC), same boundary as the
    scheduler suite."""

    def __init__(self, moment_utc: datetime):
        self.set(moment_utc)

    def set(self, moment_utc: datetime) -> None:
        self._moment = moment_utc.astimezone(UTC)

    def advance(self, seconds: float = 0) -> None:
        self._moment = self._moment + timedelta(seconds=seconds)

    def __call__(self) -> float:
        return self._moment.timestamp()


def _chat_sse(text="ok", cid="srv-resp-1", delay=0.0):
    program = {
        "kind": "sse",
        "frames": [
            _frame(_chunk({"role": "assistant"}, cid=cid)),
            _frame(_chunk({"content": text}, cid=cid)),
            _frame(_chunk({}, finish="stop", cid=cid)),
            _DONE,
        ],
    }
    if delay:
        program["sleep_before"] = delay
    return program


def _nested(code, current, target, **extra):
    task = {"task_code": code, "progress": {"current": current, "target": target}}
    task.update(extra)
    return task


def _flat(code, current, target, **extra):
    task = {"task_code": code, "current": current, "target": target}
    task.update(extra)
    return task


class _GrowthUpstream:
    """Real aiohttp growth upstream: dynamic task list + accept/claim/report/chat."""

    def __init__(self) -> None:
        self.app = web.Application()
        self.hits = collections.Counter()
        self.tasks = []
        self.models = []
        self.accept_calls = []
        self.claim_calls = []
        self.report_calls = []
        self.chat_bodies = []
        self.chat_scripts = collections.deque()
        self.on_accept = None
        self.on_claim = None
        self.on_report = None
        self.on_chat = None
        self.claim_responses = {}
        self.default_claim = (
            200,
            {"code": 0, "data": {"already_claimed": False, "credit": 100, "energy": 5}},
        )
        self._runner = None
        self._site = None
        self.base = None
        self._routes()

    def _routes(self) -> None:
        r = self.app.router
        r.add_get(GROWTH_LIST_PATH, self._list)
        r.add_post(ACCEPT_PATH, self._accept)
        r.add_post(CLAIM_ROUTE, self._claim)
        r.add_post(REPORT_PATH, self._report)
        r.add_get(MODELS_CATALOG_PATH, self._models)
        r.add_post(CHAT_PATH, self._chat)

    # -- helpers -------------------------------------------------------------
    def task(self, code):
        for item in self.tasks:
            if item.get("task_code") == code:
                return item
        return None

    def current(self, code):
        item = self.task(code)
        if item is None:
            return 0
        value = item.get("current")
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        progress = item.get("progress")
        if isinstance(progress, dict) and isinstance(progress.get("current"), int):
            return progress["current"]
        return 0

    def set_progress(self, code, current):
        item = self.task(code)
        if item is None:
            return
        item["current"] = current
        progress = item.get("progress")
        if isinstance(progress, dict):
            progress["current"] = current

    def set_accept(self, code, status):
        item = self.task(code)
        if item is not None:
            item["accept_status"] = status

    # -- handlers ------------------------------------------------------------
    async def _list(self, request):
        self.hits[("GET", GROWTH_LIST_PATH)] += 1
        payload = [{k: v for k, v in t.items()} for t in self.tasks]
        return web.json_response({"code": 0, "data": {"tasks": payload}})

    async def _accept(self, request):
        raw = await request.read()
        try:
            body = json.loads(raw) if raw else {}
        except Exception:
            body = {}
        codes = body.get("task_codes") or []
        self.accept_calls.append(list(codes))
        self.hits[("POST", ACCEPT_PATH)] += 1
        if self.on_accept:
            self.on_accept(codes)
        return web.json_response({"code": 0, "data": {}})

    async def _claim(self, request):
        code = request.match_info["code"]
        raw = await request.read()
        self.claim_calls.append(
            {"task_code": code, "platform": request.headers.get("x-client-platform"), "body": raw}
        )
        self.hits[("POST", "claim")] += 1
        if self.on_claim:
            self.on_claim(code)
        status, payload = self.claim_responses.get(code, self.default_claim)
        return web.json_response(payload, status=status)

    async def _report(self, request):
        raw = await request.read()
        try:
            events = json.loads(raw) if raw else []
        except Exception:
            events = None
        self.report_calls.append(events)
        self.hits[("POST", REPORT_PATH)] += 1
        if self.on_report:
            self.on_report(events)
        return web.json_response({"code": 0, "data": {}})

    async def _models(self, request):
        self.hits[("GET", MODELS_CATALOG_PATH)] += 1
        data = {
            "models": [{"id": m, "name": m, "maxOutputTokens": 128000} for m in self.models],
            "agents": [{"name": "cli", "models": list(self.models)}],
        }
        return web.json_response({"code": 0, "data": data})

    async def _chat(self, request):
        raw = await request.read()
        try:
            body = json.loads(raw) if raw else {}
        except Exception:
            body = {}
        self.chat_bodies.append(body)
        self.hits[("POST", CHAT_PATH)] += 1
        program = self.chat_scripts.popleft() if self.chat_scripts else {"kind": "sse", "frames": _basic_sse("ok")}
        if program.get("sleep_before"):
            await asyncio.sleep(program["sleep_before"])
        kind = program.get("kind", "sse")
        if kind == "status":
            if self.on_chat:
                self.on_chat(body)
            return web.json_response(
                program.get("body", {}),
                status=program["status"],
                headers=program.get("headers", {}),
            )
        if kind == "json":
            if self.on_chat:
                self.on_chat(body)
            return web.json_response(program.get("body", {}), status=program.get("status", 200))
        resp = web.StreamResponse(status=200, headers={"Content-Type": "text/event-stream"})
        await resp.prepare(request)
        for frame in program.get("frames") or []:
            await resp.write(frame)
            await asyncio.sleep(0)
        if self.on_chat:
            self.on_chat(body)
        with contextlib.suppress(Exception):
            await resp.write_eof()
        return resp

    # -- lifecycle -----------------------------------------------------------
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


class _GrowthTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        BASE_TMP.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(tempfile.mkdtemp(prefix="wb-growth-", dir=str(BASE_TMP)))
        self.up = _GrowthUpstream()
        self.up.models = [MODEL]
        await self.up.start()
        self.session = aiohttp.ClientSession()
        self.store = CredentialStore(self.data_dir, MASTER_KEY)
        self.store.save(dict(SEED_CREDENTIALS))
        self.manager = AuthManager(
            self.session, self.store, chat_base=self.up.base, poll_interval=0.01
        )
        self.client = UpstreamClient(
            self.session,
            self.manager,
            chat_base=self.up.base,
            billing_base=self.up.base,
            web_base=self.up.base,
        )
        self.chat = ChatService(self.client)
        self.clock = _Clock(at(2026, 10, 4, 10, 0))
        self._growths = []
        self.growth = None

    async def asyncTearDown(self) -> None:
        for growth in self._growths:
            with contextlib.suppress(Exception):
                await growth.cancel()
        await self.session.close()
        await self.up.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)

    # -- factories -----------------------------------------------------------
    def _make_growth(self, **overrides):
        params = {
            "task_interval": 0.05,
            "poll_interval": 0.05,
            "progress_timeout": 2.0,
            "max_retries": 2,
            "max_chat_requests": 20,
            "auto_claim": True,
            "clock": self.clock,
        }
        params.update(overrides)
        return self._new_growth(**params)

    def _new_growth(self, **params):
        params.setdefault("clock", self.clock)
        growth = GrowthService(self.client, self.chat, StateStore(self.data_dir), **params)
        self._growths.append(growth)
        self.growth = growth
        return growth

    @staticmethod
    def _pending_codes(result) -> set:
        return {item.get("task_code") for item in result.get("pending", [])}

    @staticmethod
    def _action_outcome(result, code):
        for item in result.get("actions", []):
            if item.get("task_code") == code:
                return item.get("outcome")
        return None

    # -- 1. list parsing + pending classification ---------------------------
    async def test_list_parses_both_progress_shapes_and_skips_pending(self):
        self.up.tasks = [
            _nested("chat_5", 5, 5, accept_status="accepted", status="complete"),
            _flat("locked_task", 0, 1, locked=True, accept_status="not_accepted"),
            _nested("expired_task", 0, 1, expired=True, accept_status="not_accepted"),
            _flat("mystery_task", 0, 1, accept_status="not_accepted"),
        ]
        growth = self._make_growth()
        result = await growth.run()

        self.assertEqual(result["progress"]["chat_5"], {"current": 5, "target": 5})
        self.assertEqual(result["progress"]["mystery_task"], {"current": 0, "target": 1})
        pending = self._pending_codes(result)
        self.assertTrue({"locked_task", "expired_task", "mystery_task"} <= pending)
        for item in result["pending"]:
            self.assertIsInstance(item.get("reason"), str)
            self.assertTrue(item["reason"])

        accepted = {code for batch in self.up.accept_calls for code in batch}
        for code in ("locked_task", "expired_task", "mystery_task"):
            self.assertNotIn(code, accepted)
        claimed = [c["task_code"] for c in self.up.claim_calls]
        for code in ("locked_task", "expired_task", "mystery_task"):
            self.assertNotIn(code, claimed)

    # -- 2. claim_only ------------------------------------------------------
    async def test_claim_only_claims_completed_and_never_incomplete(self):
        self.up.tasks = [
            _nested("done_task", 3, 3, accept_status="accepted", status="complete"),
            _flat("partial_task", 1, 3, accept_status="accepted"),
        ]
        self.up.on_claim = lambda code: self.up.set_accept(code, "claimed")

        growth = self._make_growth()
        result = await growth.run(claim_only=True)

        claimed = [c["task_code"] for c in self.up.claim_calls]
        self.assertEqual(claimed, ["done_task"])  # incomplete never claimed
        self.assertEqual(self.up.accept_calls, [])  # claim_only never re-does actions
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 0)
        # The claim uses the pinned web endpoint shape: x-client-platform: web and
        # an empty / `{}` body (never the old /tasks/reward/claim form).
        self.assertEqual(self.up.claim_calls[0]["platform"], "web")
        self.assertIn(self.up.claim_calls[0]["body"], (b"", b"{}", b"null"))
        self.assertIn("done_task", {r["task_code"] for r in result["rewards"]})

    # -- 3. chat delta + report + claim -------------------------------------
    async def test_chat5_uploads_only_the_delta_then_claims(self):
        self.up.tasks = [_nested("chat_5", 1, 3, accept_status="accepted")]

        def on_report(events):
            self.up.set_progress("chat_5", min(3, self.up.current("chat_5") + len(events)))

        self.up.on_report = on_report
        self.up.on_claim = lambda code: self.up.set_accept(code, "claimed")
        self.up.chat_scripts.extend([_chat_sse("a", "srv-1"), _chat_sse("b", "srv-2")])

        growth = self._make_growth()
        result = await growth.run()

        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 2)  # delta = 3 - 1
        self.assertEqual(len(self.up.report_calls), 2)
        seen_ids = set()
        for events in self.up.report_calls:
            self.assertEqual(len(events), 1)
            event = events[0]
            self.assertEqual(event.get("eventCode"), "chat_request_send")
            self.assertEqual(event.get("userId"), UID)
            self.assertEqual(event.get("requestModelId"), MODEL)
            seen_ids.add(event.get("requestId"))
        self.assertEqual(seen_ids, {"srv-1", "srv-2"})  # real server response ids
        self.assertIn("chat_5", [c["task_code"] for c in self.up.claim_calls])
        self.assertIn("chat_5", {r["task_code"] for r in result["rewards"]})

    # -- 4. HTTP 200 without progress is not progress -----------------------
    async def test_http200_without_progress_is_unconfirmed_and_bounded(self):
        self.up.tasks = [_nested("chat_5", 0, 2, accept_status="accepted")]
        # Reports arrive but the server-side progress never advances.
        self.up.chat_scripts.extend([_chat_sse("a", "s1"), _chat_sse("b", "s2"), _chat_sse("c", "s3")])

        growth = self._make_growth(progress_timeout=0.5, poll_interval=0.05, max_retries=2)
        result = await growth.run()

        self.assertIn("chat_5", result["unconfirmed"])
        self.assertEqual(self._action_outcome(result, "chat_5"), "progress_unconfirmed")
        self.assertEqual(self.up.claim_calls, [])  # never claim on an unconfirmed action
        self.assertEqual(result["rewards"], [])
        # Bounded: the action is retried at most max_retries+1 times, never forever.
        self.assertLessEqual(self.up.hits[("POST", CHAT_PATH)], 3)

    # -- 5. claim read-back still unclaimed ---------------------------------
    async def test_claim_readback_still_unclaimed_is_unconfirmed(self):
        self.up.tasks = [_nested("done_task", 3, 3, accept_status="accepted", status="complete")]
        # Server answers code:0 but never flips accept_status to claimed.
        growth = self._make_growth()
        result = await growth.run()

        self.assertEqual([c["task_code"] for c in self.up.claim_calls], ["done_task"])
        self.assertIn("done_task", result["unconfirmed"])
        self.assertEqual(result["rewards"], [])  # no reward is credited without confirmation

    # -- 6. restart: list first, never redo ---------------------------------
    async def test_restart_lists_first_and_never_redoes_claimed(self):
        self.up.tasks = [_nested("done_task", 3, 3, accept_status="accepted", status="complete")]
        self.up.on_claim = lambda code: self.up.set_accept(code, "claimed")

        growth = self._make_growth()
        await growth.run()
        self.assertEqual([c["task_code"] for c in self.up.claim_calls], ["done_task"])
        self.assertTrue((self.data_dir / "state.json").is_file())

        # A second runner on the same state directory lists the (now claimed)
        # task first and must not redo the accept/claim.
        self._new_growth(
            task_interval=0.05, poll_interval=0.05, progress_timeout=1.0
        )
        await self.growth.run()
        self.assertEqual(len(self.up.claim_calls), 1)
        self.assertEqual(self.up.accept_calls, [])
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 0)

    # -- 7. chat budget -----------------------------------------------------
    async def test_max_chat_requests_budget_persists_and_resets_next_day(self):
        self.up.tasks = [_nested("chat_5", 0, 5, accept_status="accepted")]

        def on_report(events):
            self.up.set_progress("chat_5", min(5, self.up.current("chat_5") + 1))

        self.up.on_report = on_report
        for _ in range(10):
            self.up.chat_scripts.append(_chat_sse("x", "s"))

        growth = self._make_growth(max_chat_requests=3)
        await growth.run()
        self.assertLessEqual(self.up.hits[("POST", CHAT_PATH)], 3)
        used = self.up.hits[("POST", CHAT_PATH)]
        self.assertEqual(growth.status()["max_chat_requests"], 3)
        self.assertEqual(growth.status()["chat_requests_today"], used)

        # Same day, new instance: the persisted budget is not reset.
        self._new_growth(task_interval=0.05, poll_interval=0.05, progress_timeout=0.5, max_chat_requests=3)
        await self.growth.run()
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], used)

        # Next day the budget resets and chatting resumes.
        self.clock.advance(24 * 3600)
        for _ in range(10):
            self.up.chat_scripts.append(_chat_sse("z", "s"))
        self._new_growth(task_interval=0.05, poll_interval=0.05, progress_timeout=0.5, max_chat_requests=3)
        await self.growth.run()
        self.assertGreater(self.up.hits[("POST", CHAT_PATH)], used)

        # An empty / unknown model catalog skips chat tasks instead of inventing one.
        self.up.models = []
        before_chat = self.up.hits[("POST", CHAT_PATH)]
        before_report = self.up.hits[("POST", REPORT_PATH)]
        self._new_growth(task_interval=0.05, poll_interval=0.05, progress_timeout=0.5, max_chat_requests=3)
        await self.growth.run()
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], before_chat)
        self.assertEqual(self.up.hits[("POST", REPORT_PATH)], before_report)

    # -- 8. single flight + cancel ------------------------------------------
    async def test_concurrent_run_is_single_flight_and_cancel_is_clean(self):
        self.up.tasks = [_nested("chat_5", 0, 5, accept_status="accepted")]
        self.up.chat_scripts.append(_chat_sse("slow", "s1", delay=0.5))

        growth = self._make_growth(progress_timeout=5.0)
        first = asyncio.ensure_future(growth.run())
        await asyncio.sleep(0.1)

        second = await growth.run()
        self.assertTrue(second.get("busy"))  # one run per account at a time

        returned = await growth.cancel()
        self.assertIsInstance(returned, dict)
        with contextlib.suppress(BaseException):
            await first
        self.assertTrue(first.done())
        status = growth.status()
        self.assertTrue(status["cancelled"])
        self.assertFalse(status["running"])

        # start() is the same background entry point and returns a dict.
        other = self._make_growth()
        started = other.start()
        self.assertIsInstance(started, dict)
        await other.cancel()

    # -- 9. 429 -------------------------------------------------------------
    async def test_429_pauses_round_and_resume_clears(self):
        self.up.tasks = [_nested("chat_5", 0, 2, accept_status="accepted")]
        self.up.chat_scripts.append(
            {"kind": "status", "status": 429, "body": {"code": 4290, "msg": "slow"}, "headers": {"Retry-After": "9"}}
        )
        growth = self._make_growth()
        await growth.run()

        status = growth.status()
        self.assertTrue(status["paused"])
        self.assertEqual(self.up.claim_calls, [])
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 1)  # no blind retry

        growth.resume()
        self.assertFalse(growth.status()["paused"])

    # -- 10. 401 / 403 ------------------------------------------------------
    async def test_401_403_pause_until_resume_without_retry(self):
        self.up.tasks = [_nested("chat_5", 0, 2, accept_status="accepted")]
        self.up.chat_scripts.append({"kind": "status", "status": 403, "body": {"code": 4030, "msg": "nope"}})
        growth = self._make_growth()
        await growth.run()

        self.assertTrue(growth.status()["paused"])
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 1)
        self.assertEqual(self.up.claim_calls, [])

        growth.resume()
        self.assertFalse(growth.status()["paused"])

    # -- 11. network / 5xx re-read then bounded redo ------------------------
    async def test_network_error_rereads_then_retries_bounded(self):
        self.up.tasks = [_nested("chat_5", 0, 1, accept_status="accepted")]
        self.up.chat_scripts.append({"kind": "status", "status": 503, "body": {"code": 9001, "msg": "down"}})
        self.up.chat_scripts.append({"kind": "status", "status": 503, "body": {"code": 9001, "msg": "down"}})
        self.up.chat_scripts.append({"kind": "status", "status": 503, "body": {"code": 9001, "msg": "down"}})

        growth = self._make_growth(max_retries=1, progress_timeout=0.5, poll_interval=0.05)
        await growth.run()

        # The unconfirmed action is retried but bounded (never an unbounded loop).
        self.assertGreaterEqual(self.up.hits[("GET", GROWTH_LIST_PATH)], 1)  # re-read first
        self.assertLessEqual(self.up.hits[("POST", CHAT_PATH)], 2)
        self.assertEqual(self.up.claim_calls, [])

    # -- 12. shared chat gate + no signin side effects ----------------------
    async def test_growth_chat_reuses_shared_gate_and_never_signs_in(self):
        # A fresh service reports the default chat budget.
        default = GrowthService(self.client, self.chat, StateStore(self.data_dir))
        self._growths.append(default)
        self.assertEqual(default.status()["max_chat_requests"], 20)

        # Saturate the shared ChatService gate with one direct call: growth must
        # draw on the *same* gate (not open an unlimited chat object), so its own
        # chat cannot slip through against a fresh budget.
        self.chat.limits = ConcurrencyGate(concurrency=1, queue_size=0, rpm=1)
        self.up.chat_scripts.append(_chat_sse("direct", "s0"))
        await self.chat.complete({"model": MODEL, "messages": [{"role": "user", "content": "hi"}]})
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 1)

        self.up.tasks = [_nested("chat_5", 0, 1, accept_status="accepted")]
        self.up.chat_scripts.append(_chat_sse("growth", "s1"))
        growth = self._make_growth()
        with contextlib.suppress(Exception):
            await growth.run()
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 1)  # gate refused, no bypass

        # Growth never performs a check-in of its own.
        self.assertEqual(self.up.hits[("POST", CHECKIN_CLAIM_PATH)], 0)
        self.assertEqual(self.up.hits[("POST", CHECKIN_STATUS_PATH)], 0)

    # -- 13. Model_chat_GLM5.2 needs the dynamic model ----------------------
    async def test_model_chat_glm52_is_skipped_when_the_model_is_absent(self):
        self.up.tasks = [
            _flat("Model_chat_GLM5.2", 0, 1, accept_status="not_accepted", task_type="single")
        ]
        self.up.on_accept = lambda codes: [self.up.set_accept(c, "accepted") for c in codes]
        self.up.models = [MODEL]  # glm-5.2 absent from the live catalog
        growth = self._make_growth()
        result = await growth.run()
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 0)  # never invent the model
        self.assertIn("Model_chat_GLM5.2", self._pending_codes(result))

    async def test_model_chat_glm52_runs_real_chat_when_the_model_is_listed(self):
        self.up.tasks = [
            _flat("Model_chat_GLM5.2", 0, 1, accept_status="not_accepted", task_type="single")
        ]
        self.up.on_accept = lambda codes: [self.up.set_accept(c, "accepted") for c in codes]
        self.up.models = [MODEL, "glm-5.2"]
        self.up.chat_scripts.append(_chat_sse("hello", "glm-resp"))
        growth = self._make_growth()
        await growth.run()
        self.assertGreaterEqual(self.up.hits[("POST", CHAT_PATH)], 1)
        using = [b.get("model") for b in self.up.chat_bodies]
        self.assertIn("glm-5.2", using)

    # -- 14. RichMeow + permission-gated manual tasks -----------------------
    async def test_richmeow_real_chat_and_permission_gated_tasks_are_manual(self):
        self.up.tasks = [
            _flat("RichMeow_Chat", 0, 1, accept_status="not_accepted"),
            _flat("skill_tool_task", 0, 1, accept_status="not_accepted"),
            _flat("real_donation_task", 0, 1, accept_status="not_accepted"),
            _flat("external_identity_task", 0, 1, accept_status="not_accepted"),
            _flat("Hp_Appearance", 0, 1, accept_status="not_accepted"),
        ]
        self.up.on_accept = lambda codes: [self.up.set_accept(c, "accepted") for c in codes]
        self.up.chat_scripts.append(_chat_sse("meow", "rich-cid"))

        growth = self._make_growth(allow_theme_change=False)
        result = await growth.run()

        # RichMeow performs a real, successful chat and reports the server id.
        self.assertGreaterEqual(self.up.hits[("POST", CHAT_PATH)], 1)
        report_ids = {e.get("requestId") for events in self.up.report_calls if events for e in events}
        self.assertIn("rich-cid", report_ids)

        # The gated tasks stay pending/manual and never touch accept/chat/claim.
        accepted = {code for batch in self.up.accept_calls for code in batch}
        claim_codes = {c["task_code"] for c in self.up.claim_calls}
        for code in ("skill_tool_task", "real_donation_task", "external_identity_task", "Hp_Appearance"):
            self.assertIn(code, self._pending_codes(result))
            self.assertNotIn(code, accepted)
            self.assertNotIn(code, claim_codes)

    # -- 15. black_cat night window -----------------------------------------
    async def test_black_cat_skips_outside_the_night_window(self):
        self.up.tasks = [_flat("black_cat", 0, 3, accept_status="accepted")]
        self.up.models = [MODEL, "glm-5.2"]
        growth = self._make_growth()

        self.clock.set(at(2026, 10, 4, 10, 0))  # daytime: outside 23:00-08:00
        await growth.run()
        self.assertEqual(self.up.hits[("POST", CHAT_PATH)], 0)

        self.clock.set(at(2026, 10, 4, 23, 30))  # inside the window
        self.up.chat_scripts.append(_chat_sse("night", "night-cid"))
        await growth.run()
        self.assertGreaterEqual(self.up.hits[("POST", CHAT_PATH)], 1)

    # -- 16. no secrets, separated fields, status, whitelist ----------------
    async def test_result_fields_are_separated_and_status_lists_pending(self):
        self.up.tasks = [
            _nested("chat_5", 5, 5, accept_status="accepted", status="complete"),
            _flat("mystery_task", 0, 1, accept_status="not_accepted"),
        ]
        self.up.on_claim = lambda code: self.up.set_accept(code, "claimed")

        growth = self._make_growth()
        result = await growth.run()

        # Actions, progress and rewards are separate buckets.
        for key in ("actions", "progress", "rewards", "pending", "unconfirmed"):
            self.assertIn(key, result)
        self.assertIsInstance(result["actions"], list)
        self.assertIsInstance(result["progress"], dict)
        self.assertIsInstance(result["rewards"], list)

        # No raw credential / token ever leaks through the result or status.
        blob = json.dumps(result, default=str) + json.dumps(growth.status(), default=str)
        for secret in (SEED_CREDENTIALS["accessToken"], SEED_CREDENTIALS["refreshToken"]):
            self.assertNotIn(secret, blob)

        # status lists the task awaiting a human / unknown decision.
        status = growth.status()
        self.assertIn("mystery_task", {p.get("task_code") for p in status["pending"]})
        self.assertIn("date", status)

        # TaskActions is the automated-action whitelist; gated tasks are excluded.
        self.assertTrue(TaskActions.allowed("chat_5"))
        self.assertFalse(TaskActions.allowed("mystery_task"))
        self.assertFalse(TaskActions.allowed("skill_tool_task"))
        self.assertIsNone(TaskActions.action_for("mystery_task"))


if __name__ == "__main__":
    unittest.main()
