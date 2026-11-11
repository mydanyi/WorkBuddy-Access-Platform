"""CLI 凭据读取端的契约测试。

不 spawn 进程：用一个假运行时给出"凭据文件在哪"，并模拟"叫醒 CLI 会刷新"。
真实 CLI 的落盘格式见 ``evidence/credential-store.md``。

运行::

    python -m unittest discover -s astrbot_plugin_workbuddy/tests -t astrbot_plugin_workbuddy/tests -p test_acp_credentials.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.acp import (  # noqa: E402
    CREDENTIAL_FILE,
    CREDENTIAL_LOGGED_OUT,
    CliCredentials,
    map_credentials,
)
from astrbot_plugin_workbuddy.core.acp.credentials import (  # noqa: E402
    _expiry_seconds,
)
from astrbot_plugin_workbuddy.core.errors import UpstreamError  # noqa: E402


class _FakeRuntime:
    """只提供凭据路径与 ``ensure_started``。"""

    def __init__(self, home, *, on_start=None, start_error=None):
        self.home = home
        self.starts = 0
        self.on_start = on_start
        self.start_error = start_error

    @property
    def credential_dir(self):
        return os.path.join(
            self.home, ".local", "share", "CodeBuddyExtension", "Data", "Public", "auth"
        )

    @property
    def credential_path(self):
        return os.path.join(self.credential_dir, CREDENTIAL_FILE)

    @property
    def logged_out_path(self):
        return os.path.join(self.credential_dir, CREDENTIAL_LOGGED_OUT)

    async def ensure_started(self):
        self.starts += 1
        if self.start_error is not None:
            raise self.start_error
        if self.on_start is not None:
            self.on_start()
        return object()


def _write(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return path


def _record(token="tok-1", *, expires_at=None, uid="42", domain="www.example.com"):
    auth = {"accessToken": token, "refreshToken": "refresh-should-not-leak"}
    if expires_at is not None:
        auth["expiresAt"] = expires_at
    if domain is not None:
        auth["domain"] = domain
    return {"account": {"uid": uid}, "auth": auth}


class MapCredentialsTests(unittest.TestCase):
    def test_maps_the_fields_the_client_needs(self):
        mapped = map_credentials(_record(expires_at=1767225600000))
        self.assertEqual(mapped["accessToken"], "tok-1")
        self.assertEqual(mapped["uid"], "42")
        self.assertEqual(mapped["domain"], "www.example.com")
        self.assertEqual(mapped["realm"], "cn")
        # 毫秒时间戳归一到秒
        self.assertEqual(mapped["expiresAt"], 1767225600.0)

    def test_refresh_token_is_never_carried_out(self):
        # 插件不刷新，带出去只会诱使后来者再写一套 refresh。
        self.assertNotIn("refreshToken", map_credentials(_record()))

    def test_missing_or_broken_shapes_map_to_empty(self):
        for payload in (
            None,
            [],
            {},
            {"auth": None},
            {"auth": {}},
            {"auth": {"accessToken": ""}},
            {"auth": {"accessToken": 123}},
        ):
            with self.subTest(payload=payload):
                self.assertEqual(map_credentials(payload), {})

    def test_optional_fields_are_skipped_instead_of_faked(self):
        mapped = map_credentials(
            {"account": {}, "auth": {"accessToken": "tok", "domain": ""}}
        )
        self.assertEqual(mapped, {"accessToken": "tok", "realm": "cn"})

    def test_uid_is_normalised_to_text(self):
        mapped = map_credentials({"account": {"uid": 42}, "auth": {"accessToken": "t"}})
        self.assertEqual(mapped["uid"], "42")

    def test_expiry_accepts_seconds_and_milliseconds(self):
        self.assertEqual(_expiry_seconds(1767225600), 1767225600.0)
        self.assertEqual(_expiry_seconds(1767225600000), 1767225600.0)
        # json 默认接受 NaN / Infinity 字面量，放进去会让比较全部失真。
        junk = (
            None,
            True,
            "1767225600",
            0,
            -1,
            [],
            float("nan"),
            float("inf"),
            float("-inf"),
        )
        for value in junk:
            with self.subTest(value=value):
                self.assertIsNone(_expiry_seconds(value))


class WiringGuardTests(unittest.TestCase):
    def test_a_runtime_without_a_credential_path_is_rejected(self):
        # 没配 home_dir 时读不到凭据，表现是"永远未登录"——把配置错误伪装成
        # 登录状态。构造期就该炸，别留到凌晨三点。
        class _NoHome:
            credential_path = None
            logged_out_path = None

        with self.assertRaises(ValueError):
            CliCredentials(_NoHome())


class ReadTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.home = self._tmp.name
        self.runtime = _FakeRuntime(self.home)
        self.source = CliCredentials(self.runtime)

    def test_no_file_means_no_credentials(self):
        self.assertEqual(self.source.credentials, {})
        self.assertFalse(self.source.logged_out)

    def test_reads_the_token_from_the_cli_file(self):
        _write(self.runtime.credential_path, _record("tok-live"))
        self.assertEqual(self.source.credentials["accessToken"], "tok-live")

    def test_logged_out_marker_beats_a_leftover_file(self):
        # 删了一半的登录态：.info 还在，但 CLI 说已登出。
        _write(self.runtime.credential_path, _record("tok-stale"))
        Path(self.runtime.logged_out_path).write_text("", encoding="utf-8")
        self.assertTrue(self.source.logged_out)
        self.assertEqual(self.source.credentials, {})

    def test_a_login_after_logout_wins_over_the_stale_marker(self):
        # 万一 CLI 重新登录时不删标记，只看存在性会把账号永久锁死：连 401 →
        # refresh 都会被短路。凭据文件比标记新，就说明登录发生在登出之后。
        _write(self.runtime.credential_path, _record("tok-1"))
        marker = Path(self.runtime.logged_out_path)
        marker.write_text("", encoding="utf-8")
        self.assertTrue(self.source.logged_out)
        os.utime(marker, ns=(1, 1))
        self.assertFalse(self.source.logged_out)
        self.assertEqual(self.source.credentials["accessToken"], "tok-1")

    def test_cache_is_reused_until_the_file_changes(self):
        _write(self.runtime.credential_path, _record("tok-1"))
        first = self.source.credentials
        self.assertIs(self.source.credentials, first)
        # 内容长度不同 → mtime 与 size 都变，必须重读。
        _write(self.runtime.credential_path, _record("tok-2-longer"))
        self.assertEqual(self.source.credentials["accessToken"], "tok-2-longer")

    def test_half_written_file_keeps_the_last_known_good(self):
        _write(self.runtime.credential_path, _record("tok-1"))
        self.assertEqual(self.source.credentials["accessToken"], "tok-1")
        # CLI 正在落盘：长度不同的半截 JSON。
        Path(self.runtime.credential_path).write_text('{"auth": {"access', encoding="utf-8")
        # 瞬时解析失败不能报成"未登录"，否则签到与聊天会一起停摆。
        self.assertEqual(self.source.credentials["accessToken"], "tok-1")

    def test_deleting_the_file_clears_the_cache(self):
        _write(self.runtime.credential_path, _record("tok-1"))
        self.assertEqual(self.source.credentials["accessToken"], "tok-1")
        os.remove(self.runtime.credential_path)
        self.assertEqual(self.source.credentials, {})

    async def test_logout_only_drops_the_cache(self):
        # 真正的删除归 AcpRuntime.logout(purge_config=True)；这里删文件等于
        # 另开一个删除者，迟早和它打架。
        _write(self.runtime.credential_path, _record("tok-1"))
        self.assertEqual(self.source.credentials["accessToken"], "tok-1")
        await self.source.logout()
        self.assertTrue(os.path.exists(self.runtime.credential_path))
        self.assertEqual(self.source.credentials["accessToken"], "tok-1")


class RefreshTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.home = self._tmp.name
        self.runtime = _FakeRuntime(self.home)
        self.source = CliCredentials(
            self.runtime, refresh_wait=2.0, poll_interval=0.02
        )

    async def test_a_token_that_already_changed_needs_no_cli(self):
        _write(self.runtime.credential_path, _record("tok-new"))
        creds = await self.source.refresh(stale_token="tok-old")
        self.assertEqual(creds["accessToken"], "tok-new")
        self.assertEqual(self.runtime.starts, 0)

    async def test_the_cli_refresh_is_picked_up(self):
        _write(self.runtime.credential_path, _record("tok-old"))
        loop = asyncio.get_running_loop()

        # 写入必须落在第一次 sleep 之后，否则测的只是"读完文件就返回"，
        # 轮询的时序写错了也照样绿。
        def _refresh():
            loop.call_later(
                0.05, _write, self.runtime.credential_path, _record("tok-served")
            )

        self.runtime.on_start = _refresh
        creds = await self.source.refresh(stale_token="tok-old")
        self.assertEqual(creds["accessToken"], "tok-served")
        self.assertEqual(self.runtime.starts, 1)

    async def test_a_same_size_rewrite_is_still_picked_up(self):
        # JWT 长度基本恒定，文件系统时间戳粒度可能只有秒级：mtime + size 会相等。
        # 轮询若走节流缓存就永远看不见新 token，把"已刷好"误判成 401。
        path = _write(self.runtime.credential_path, _record("tok-aaaa"))
        before = os.stat(path)
        self.assertEqual(self.source.credentials["accessToken"], "tok-aaaa")

        def _rewrite():
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(_record("tok-bbbb"), handle)
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))

        self.runtime.on_start = _rewrite
        creds = await self.source.refresh(stale_token="tok-aaaa")
        self.assertEqual(creds["accessToken"], "tok-bbbb")

    async def test_no_new_token_is_an_auth_failure(self):
        _write(self.runtime.credential_path, _record("tok-old"))
        with self.assertRaises(UpstreamError) as ctx:
            await self.source.refresh(stale_token="tok-old")
        self.assertEqual(ctx.exception.status, 401)
        # 叫醒过 CLI，只是它没换 token。
        self.assertEqual(self.runtime.starts, 1)

    async def test_a_cli_that_cannot_start_fails_fast(self):
        _write(self.runtime.credential_path, _record("tok-old"))
        self.runtime.start_error = RuntimeError("boom")
        loop = asyncio.get_running_loop()
        started = loop.time()
        with self.assertRaises(UpstreamError) as ctx:
            await self.source.refresh(stale_token="tok-old")
        # 起不来就没得刷，不该干等满一个窗口（这里是 2 秒）。
        self.assertLess(loop.time() - started, 1.0)
        # 原始异常仍挂在因果链上：别把"没装 CLI"表现成"请重新登录"。
        self.assertIsInstance(ctx.exception.__cause__, RuntimeError)

    async def test_concurrent_refreshes_start_the_cli_once(self):
        _write(self.runtime.credential_path, _record("tok-old"))

        def _refresh():
            _write(self.runtime.credential_path, _record("tok-fresh"))

        self.runtime.on_start = _refresh
        results = await asyncio.gather(
            *(self.source.refresh(stale_token="tok-old") for _ in range(4))
        )
        self.assertTrue(all(r["accessToken"] == "tok-fresh" for r in results))
        self.assertEqual(self.runtime.starts, 1)

    async def test_refresh_returns_failure_when_logged_out(self):
        _write(self.runtime.credential_path, _record("tok-old"))
        Path(self.runtime.logged_out_path).write_text("", encoding="utf-8")
        loop = asyncio.get_running_loop()
        started = loop.time()
        with self.assertRaises(UpstreamError) as ctx:
            await self.source.refresh(stale_token="tok-old")
        self.assertEqual(ctx.exception.status, 401)
        # 明确登出就别白等一个窗口，也别去拉起 CLI。
        self.assertLess(loop.time() - started, 0.5)
        self.assertEqual(self.runtime.starts, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
