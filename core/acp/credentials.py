"""CLI 登录态的读取端：把 CLI 自己落盘的凭据拿给插件用。

为什么不再自己跑一套 OAuth：CLI 已经把 token 写在明处（明文 JSON），插件再跑
一套 refresh 只会造出两个刷新者，两边迟早互相把对方的 token 作废。现在只有一个
负责人——CLI；插件只读。

契约很小，``UpstreamClient`` 只用这两样：

* :attr:`credentials` —— 同步读，返回 ``accessToken`` / ``uid`` / ``domain`` 等；
* :meth:`refresh` —— 叫醒 CLI 让它刷一次，再重读；刷不出来就认输。

三条边界：

* **读失败不等于登出。** CLI 正在写盘时文件可能只写了一半，解析失败就退回上一次
  成功读到的凭据；把瞬时故障报成"未登录"会让签到和聊天一起停摆。
* **不自己造 token。** 刷新只能由 CLI 发生，等不到新 token 就抛
  ``UpstreamError(401)``，由上层提示重新登录。
* **``.logged-out`` 是权威标记。** 它存在时，即使 ``.info`` 还在（删了一半）也按
  未登录处理。

**接线状态：已接进生产链路。** ``main.py`` 把 ``CliCredentials(self.acp)`` 交给
``UpstreamClient``，凭据只剩 CLI 这一个来源。``core/auth.py``（旧 OAuth）与
``CredentialStore`` 里的登录态读取已经不在装配路径上。
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import time

from ..errors import UpstreamError

# 等 CLI 把新 token 写回来的窗口。刷新不是插件能直接触发的事，只能等；等不到
# 就如实报未登录，不猜也不伪造。
DEFAULT_REFRESH_WAIT = 5.0
DEFAULT_POLL_INTERVAL = 0.2
# 只给"拉起进程"的预算。它和上面那个窗口是两笔账：CLI 冷启动要握一次手，
# 不该让调用方在一把锁上无限期排队。
DEFAULT_START_TIMEOUT = 10.0

# 毫秒时间戳约 1e12，秒级约 1e9；1e11 是两者的分界，留足余量。
_MS_THRESHOLD = 1e11


def _expiry_seconds(value):
    """把 ``expiresAt`` 归一到秒级时间戳；认不出来就返回 ``None``。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    if not math.isfinite(value) or value <= 0:
        # json.load 默认接受 NaN / Infinity 字面量，放进去会让比较全部失真。
        return None
    return float(value) / 1000.0 if value > _MS_THRESHOLD else float(value)


def map_credentials(raw):
    """把 CLI 的凭据文件映射成 ``UpstreamClient`` 认的形状。

    只取用得上的字段。``refreshToken`` **不**往外带：插件不刷新，拿着它只会诱使
    后来者再写一套 refresh，把"单一刷新者"这条又破掉。
    """
    if not isinstance(raw, dict):
        return {}
    auth = raw.get("auth")
    if not isinstance(auth, dict):
        return {}
    token = auth.get("accessToken")
    if not isinstance(token, str) or not token:
        return {}

    mapped = {"accessToken": token, "realm": "cn"}
    domain = auth.get("domain")
    if isinstance(domain, str) and domain:
        mapped["domain"] = domain
    expires_at = _expiry_seconds(auth.get("expiresAt"))
    if expires_at is not None:
        mapped["expiresAt"] = expires_at

    account = raw.get("account")
    if isinstance(account, dict):
        uid = account.get("uid")
        if isinstance(uid, (str, int)) and not isinstance(uid, bool) and uid != "":
            mapped["uid"] = str(uid)
        enterprise = account.get("enterpriseId")
        # 数字型的 ID 也要接住：只认 str 会让"企业账号"静默退化成个人账号。
        if (
            isinstance(enterprise, (str, int))
            and not isinstance(enterprise, bool)
            and enterprise != ""
        ):
            mapped["enterpriseId"] = str(enterprise)
    return mapped


class CliCredentials:
    """读 CLI 凭据文件；``UpstreamClient`` 拿它当"凭据来源"用。"""

    def __init__(
        self,
        runtime,
        *,
        refresh_wait=DEFAULT_REFRESH_WAIT,
        poll_interval=DEFAULT_POLL_INTERVAL,
        start_timeout=DEFAULT_START_TIMEOUT,
    ):
        if runtime.credential_path is None:
            # 没配 home_dir 就读不到凭据，表现是"永远未登录"——把配置错误伪装成
            # 登录状态，比启动失败难查一百倍。构造期就炸。
            raise ValueError("credential path is not configured")
        self._runtime = runtime
        self._refresh_wait = refresh_wait
        self._poll_interval = poll_interval
        self._start_timeout = start_timeout
        self._cached = {}
        # 上次成功解析时文件的 (mtime_ns, size)。命中就不重复解析。
        self._stamp = None
        self._refresh_lock = asyncio.Lock()
        # 上一次"等文件"失败时的基线 token 与时刻。同一份 token 刚刷失败过，
        # 排在后头的调用方等到的必然还是同一个结论——失败也要共享，不然 4 个并发
        # 401 会排成 5 / 10 / 15 / 20 秒四段，把同一个窗口重复等四遍。
        self._failed_token = None
        self._failed_at = 0.0

    # -- 只读视图 -----------------------------------------------------------

    @property
    def path(self):
        return self._runtime.credential_path

    @property
    def logged_out(self) -> bool:
        """CLI 是否明确登出过（``.info.logged-out`` 存在）。

        只看标记是否存在会把账号**永久**锁死在未登录：万一 CLI 重新登录时不删这个
        标记，连 401 → refresh 都短路掉，没有任何自愈路径。所以补一条时间判断——
        凭据文件比标记新，说明登录发生在登出之后，标记已经过期。

        这条规则只能靠"登出写标记、登录写文件"这个顺序成立；真实 CLI 会不会这么
        做尚未实测（见 ``evidence/credential-store.md``）。
        """
        marker = self._runtime.logged_out_path
        if marker is None or not os.path.exists(marker):
            return False
        path = self.path
        if path is None:
            return True
        try:
            info_mtime = os.stat(path).st_mtime_ns
            marker_mtime = os.stat(marker).st_mtime_ns
        except OSError:
            # 拿不到时间就按标记说了算：宁可说未登录，也不要说已登录。
            return True
        return info_mtime <= marker_mtime

    @property
    def credentials(self) -> dict:
        """当前登录态；未登录（或没有凭据文件）时返回 ``{}``。"""
        if self.logged_out:
            return {}
        return self._read()

    # -- 刷新 ---------------------------------------------------------------

    async def refresh(self, stale_token=None) -> dict:
        """叫醒 CLI 刷新，然后重读。

        ``stale_token`` 是调用方刚才用失败的那个 token。先比一遍：如果文件里的
        token 已经变了，说明别人刷过了，直接返回，绝不重复折腾 CLI。整个过程串在
        一把锁上，一批并发 401 只会触发一次等待。

        等不到新 token 就抛 ``UpstreamError(401)``——让上层去提示重新登录，而不是
        拿旧 token 重试一轮，白烧一次配额。

        总耗时上限是 ``start_timeout + refresh_wait``：拉起进程和等文件是两笔账，
        前者不该让排队的调用方无限期等着。
        """
        async with self._refresh_lock:
            if self.logged_out:
                # 明确登出过就没有"刷新"这回事，别让调用方等满一个窗口。
                raise UpstreamError(401, None)
            current = self.credentials
            token_now = current.get("accessToken")
            if stale_token is not None and token_now and token_now != stale_token:
                # 别人已经换过了，别再折腾 CLI。这里必须要求 token_now 为真：
                # 文件被删掉时 None != stale_token 也成立，那会把"没凭据"当成
                # "已经换好了"直接返回空字典。
                self._failed_token = None
                return current
            # 基线是"调用方用失败的那个 token"，不是"我这次读到了什么"。读失败
            # 时后者是 None，于是第一轮读到任何 token 都会被判成"变了"。
            before = stale_token if stale_token is not None else token_now

            # 同一份 token 刚在这一个窗口里失败过：直接给同一个结论。等下去只会
            # 再等满一个注定失败的窗口，而结论不会变。
            if (
                before is not None
                and before == self._failed_token
                and time.monotonic() - self._failed_at < self._refresh_wait
            ):
                raise UpstreamError(401, None)

            # 把刷新这件事的负责人叫起来。CLI 起不来就没得刷，直接认输，不要
            # 干等满一个窗口。
            try:
                await asyncio.wait_for(
                    self._runtime.ensure_started(), timeout=self._start_timeout
                )
            except Exception as exc:
                # 保留因果链（没装、argv 错、权限不足…），别让它表现成"请重新登录"。
                raise UpstreamError(401, None) from exc

            loop = asyncio.get_running_loop()
            deadline = loop.time() + self._refresh_wait
            while True:
                await asyncio.sleep(self._poll_interval)
                latest = self._poll_read()
                token = latest.get("accessToken")
                if token and token != before:
                    self._failed_token = None
                    return latest
                if loop.time() >= deadline:
                    break
            self._failed_token = before
            self._failed_at = time.monotonic()
            raise UpstreamError(401, None)

    async def logout(self) -> None:
        """丢掉内存里的那份缓存。

        真正的删除归 ``AcpRuntime.logout(purge_config=True)``——它要先确认进程
        真的死了才动手。这里再删一次没有任何好处，只会让"谁负责删"变成两个答案。
        """
        self._cached = {}
        self._stamp = None

    # -- 内部 ---------------------------------------------------------------

    def _read(self, *, force=False) -> dict:
        """读一次；``force`` 跳过节流缓存。

        只在事件循环线程调用：``_cached`` / ``_stamp`` 是普通属性，不加锁的前提
        是这里全程没有 ``await`` 打断。换个线程读最多多解析一次，不会永久卡住。
        """
        path = self.path
        if path is None:
            return {}
        try:
            stat = os.stat(path)
        except FileNotFoundError:
            self._cached = {}
            self._stamp = None
            return {}
        except OSError:
            # EACCES / EMFILE / EBUSY：文件可能根本还在，别把验证过的好凭据丢掉。
            return self._cached
        stamp = (stat.st_mtime_ns, stat.st_size)
        if not force and stamp == self._stamp:
            return self._cached
        try:
            with open(path, "r", encoding="utf-8") as handle:
                raw = json.load(handle)
        except (OSError, ValueError):
            # 半写状态或文件损坏：保留上一次读到的凭据，下次 stat 变了再试。
            return self._cached
        mapped = map_credentials(raw)
        self._cached = mapped
        self._stamp = stamp
        return mapped

    def _poll_read(self) -> dict:
        """轮询专用：绕过节流。

        同一账号刷出来的 JWT 长度基本恒定，而落盘文件系统的时间戳粒度可能只有
        秒级（bind mount / SMB / exFAT 都常见）。mtime + size 这一对会相等，走
        缓存就永远看不见新 token，把"CLI 已经刷好"误判成 401。
        """
        if self.logged_out:
            return {}
        return self._read(force=True)


__all__ = [
    "CliCredentials",
    "map_credentials",
    "DEFAULT_REFRESH_WAIT",
    "DEFAULT_POLL_INTERVAL",
]
