"""ACP 运行时：按需拉起 CLI、持有唯一的连接、负责登录与关停。

四条取舍：

* **懒启动。** 插件初始化不 spawn 子进程；第一次真要登录/聊天才拉起。CLI 是
  Node 进程，常驻但没人用时启动它只是白占内存。
* **配置与凭据都要圈进数据目录。** ``CODEBUDDY_CONFIG_DIR`` 只圈配置；登录凭据
  在 ``~/.local/share/CodeBuddyExtension/Data/Public/auth/``（实测：CLI 用
  ``os.homedir()`` 解析，**不看** ``XDG_DATA_HOME``）。所以真正管用的杠杆是给子
  进程设 ``HOME``。不设的话凭据落在镜像层：容器重建就丢，退出登录也删不干净
  （"已断开连接"会变成谎话）。
* **一条连接 = 一个模型。** 本类只守一条进程；模型是进程启动参数（``--model``），
  所以"多个模型"就是"多条进程"。谁负责按模型挑、按上限回收，是
  :class:`core.acp.pool.AcpRuntimePool` 的事，本类不掺和。
* **登录态不猜。** 只有真的收到 auth 错误才记为未登录。为了"看一眼状态"去建一个
  会话，会在 CLI 侧留下垃圾会话，也会让首轮系统提示的判断失真。
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shutil

from .client import AcpClient
from .protocol import AcpError, AcpTimeoutError
from .transport import AcpTransport

# 最多等用户扫码 5 分钟：够慢的人扫完，也不会把任务永远挂着。
DEFAULT_LOGIN_TIMEOUT = 300.0

# 配置目录的目录名。退出登录时只删这一层，避免误删数据目录里的其它东西。
CONFIG_DIR_NAME = "codebuddy"

# 凭据目录相对 HOME 的路径（POSIX）。实测（CLI 2.182.0，Linux 容器）CLI 启动时
# 只对这个路径做两次 access(F_OK)：
#   Tencent-Cloud.coding-copilot.info             有 = 已登录
#   Tencent-Cloud.coding-copilot.info.logged-out  有 = 明确登出过
# 文件是明文 JSON，``auth.accessToken`` 就是给 API 用的 Bearer。
#
# 注意：``XDG_DATA_HOME`` 对这个路径**无效**（实测设了仍然读 ``~/.local/share``），
# 能重定向它的只有 ``HOME``。Windows 上 CLI 走 ``%LOCALAPPDATA%``，与 POSIX 不同：
# 本模块按平台分支计算，但只在 Linux 容器里实测过。
PRODUCT_DATA_DIR = "CodeBuddyExtension"
AUTH_SUBPATH = (PRODUCT_DATA_DIR, "Data", "Public", "auth")
AUTH_PARENT = (".local", "share")

# 凭据文件名与"明确登出"标记。CLI 只对这两个路径做存在性探测：
#   <name>           有 = 已登录（明文 JSON，auth.accessToken 就是 Bearer）
#   <name>.logged-out 有 = 明确登出过
CREDENTIAL_FILE = "Tencent-Cloud.coding-copilot.info"
CREDENTIAL_LOGGED_OUT = CREDENTIAL_FILE + ".logged-out"


def default_argv(cli_path="codebuddy", *, tools="", model=None):
    """ACP 模式的命令行。

    ``--tools ""`` 从源头关掉全部内置工具：传输层已经拒掉所有权限请求，这里再
    把工具表清空，两道闸。真进程实测接受空字符串且正常常驻。

    ``--model`` 是**进程级**参数，不是会话参数：ACP 没有"换模型"的请求，一条进程
    从头到尾只用启动时这一个模型。所以选不同模型只能换进程，这正是一模型一进程的
    由来（见 :mod:`core.acp.pool`）。``model`` 为空表示"用 CLI 自己的默认模型"。
    """
    argv = [cli_path, "--acp", "--acp-transport", "stdio"]
    if isinstance(model, str) and model.strip():
        argv += ["--model", model.strip()]
    if tools is not None:
        argv += ["--tools", tools]
    return argv


def default_env(*, config_dir=None, home_dir=None, base=None):
    """子进程的环境：PATH、配置目录、以及被重定向的 HOME 全家。

    抽成公开函数是因为**模型清单的探针进程也要用同一份环境**。清单是
    ``codebuddy --help`` 打出来的，而 ``--help`` 里那串 ``custom-local:`` 候选来自
    CLI 的配置目录；聊天进程跑在 ``CODEBUDDY_CONFIG_DIR`` + 重定向后的 ``HOME`` 下，
    探针却继承宿主的 env，两边就可能报出不同的模型集合——命令说"有 A、B"，聊天却
    说"A 不认识"。

    ``create_subprocess_exec`` 拿到 ``env`` 就整份替换，不继承父进程，所以 ``base``
    默认是空的：只给 PATH 兜底，别的一概不带。
    """
    env = dict(base) if base else {}
    env.setdefault("PATH", os.environ.get("PATH", os.defpath))
    if config_dir:
        env["CODEBUDDY_CONFIG_DIR"] = config_dir
    if home_dir:
        # 凭据不在 CODEBUDDY_CONFIG_DIR 里，而在 ~/.local/share 下，且
        # XDG_DATA_HOME 对它无效——CLI 直接读 os.homedir()。所以必须改 HOME，
        # 否则凭据落在镜像层：容器重建就丢，退出登录也删不干净。
        env["HOME"] = home_dir
        # Windows 的 os.homedir() 认的是 USERPROFILE，一并给上。
        env["USERPROFILE"] = home_dir
        # XDG 那几层跟着走，免得缓存散落到别处（与凭据路径无关）。
        env["XDG_DATA_HOME"] = os.path.join(home_dir, ".local", "share")
        env["XDG_CONFIG_HOME"] = os.path.join(home_dir, ".config")
        env["XDG_CACHE_HOME"] = os.path.join(home_dir, ".cache")
    return env


class AcpRuntime:
    """一条 CLI 进程 + 一个 :class:`AcpClient`，外加登录态与配置目录。"""

    def __init__(
        self,
        *,
        argv,
        cwd,
        model=None,
        config_dir=None,
        home_dir=None,
        env=None,
        request_timeout=60.0,
        login_timeout=DEFAULT_LOGIN_TIMEOUT,
    ):
        self._argv = list(argv)
        self._cwd = cwd
        # 记录这条进程绑的模型，供状态页与淘汰策略读取；真正生效的是 argv 里那个
        # ``--model``，这里只是同一件事的只读视图，绝不用来"切换"模型。
        self._model = model
        self._config_dir = config_dir
        self._home_dir = home_dir

        if home_dir and not os.path.isabs(home_dir):
            # 相对路径会让 realpath 按父进程 cwd 解析，与子进程实际写入的位置分叉：
            # 删的时候找错地方，还报"已退出登录"。宁可构造失败。
            raise ValueError("home_dir must be an absolute path")

        # 这几个键不用 setdefault：下面拿它们推导"子进程把凭据写在哪"，退出登录
        # 也照着删。调用方再塞一份同名值，就会变成"写的是一处、删的是另一处"。
        self._env = default_env(
            config_dir=config_dir, home_dir=home_dir, base=env
        )
        self._request_timeout = request_timeout
        self._login_timeout = login_timeout

        self._transport = None
        self._client = None
        self._start_lock = asyncio.Lock()
        self._login_lock = asyncio.Lock()
        # None 表示"还没验证过"，不是"未登录"。
        self._auth_state = None
        # 永久关闭后，任何旧引用都不许再把进程拉起来。
        self._closed = False

    # -- 只读视图 -----------------------------------------------------------

    @property
    def client(self):
        return self._client

    @property
    def transport(self):
        return self._transport

    @property
    def model(self):
        """这条进程绑定的模型 ID（``None`` 表示走 CLI 自己的默认）。"""
        return self._model

    @property
    def argv(self):
        """启动参数的一份拷贝——排查"到底传了什么"时的唯一依据。"""
        return list(self._argv)

    @property
    def started(self) -> bool:
        return self._transport is not None and self._transport.running

    @property
    def auth_state(self):
        """``True`` 已登录 / ``False`` 未登录 / ``None`` 未知。"""
        return self._auth_state

    @property
    def config_dir(self):
        return self._config_dir

    @property
    def credential_dir(self):
        """CLI 真正放登录凭据的目录（不在 config_dir 里，也不受 XDG 控制）。"""
        if not self._home_dir:
            return None
        if os.name == "nt":
            return os.path.join(self._home_dir, "AppData", "Local", *AUTH_SUBPATH)
        return os.path.join(self._home_dir, *AUTH_PARENT, *AUTH_SUBPATH)

    @property
    def credential_path(self):
        """凭据文件本身的路径；没配 ``home_dir`` 时为 ``None``。"""
        directory = self.credential_dir
        return None if directory is None else os.path.join(directory, CREDENTIAL_FILE)

    @property
    def logged_out_path(self):
        """CLI 的"明确登出"标记路径；没配 ``home_dir`` 时为 ``None``。"""
        directory = self.credential_dir
        return (
            None
            if directory is None
            else os.path.join(directory, CREDENTIAL_LOGGED_OUT)
        )

    def status(self) -> dict:
        return {
            "started": self.started,
            "auth": self._auth_state,
            "has_config_dir": bool(self._config_dir),
            "has_home_dir": bool(self._home_dir),
        }

    # -- 生命周期 -----------------------------------------------------------

    async def ensure_started(self) -> AcpClient:
        """拉起并握手；已经活着就复用。并发调用只拉起一次。"""
        async with self._start_lock:
            if self._closed:
                raise AcpError("acp runtime is closed")
            transport = self._transport
            if (
                self._client is not None
                and transport is not None
                and transport.running
            ):
                return self._client
            await self._discard()
            new_transport = AcpTransport(
                self._argv,
                env=self._env,
                cwd=self._cwd,
                request_timeout=self._request_timeout,
            )
            client = AcpClient(new_transport, cwd=self._cwd)
            try:
                # start() 内部有 await，取消可能落在"进程已 spawn、读循环未起"
                # 的窗口里；这时 new_transport 只是局部变量，不在这里关掉就
                # 再也没人认领这个子进程了。
                await new_transport.start()
                await client.initialize()
            except BaseException:
                with contextlib.suppress(Exception):
                    await new_transport.close(timeout=2.0)
                raise
            self._transport = new_transport
            self._client = client
            return client

    async def close(self) -> None:
        """关掉子进程；可以重复调用，之后还能重新拉起。"""
        async with self._start_lock:
            await self._discard()

    async def shutdown(self) -> None:
        """永久关闭：之后 :meth:`ensure_started` 一律失败。

        插件卸载/重载时用它，防止某个仍持有旧引用的协程把 CLI 再拉起来——
        那种进程活过了插件生命周期，再也没人回收。
        """
        self._closed = True
        await self.close()

    async def _discard(self) -> None:
        """关掉当前连接；**关干净了才摘引用**。

        顺序不能反。先把 ``self._transport`` 置空再去 ``close()``，一旦关闭过程被
        取消（淘汰空闲进程时正好撞上请求的截止时间就是这条路），调用方手里已经没有
        这条传输层了，而子进程还活着——没人认领，也没人再试一次。取消时引用留在
        身上，下一次 ``close()`` / ``ensure_started()`` 会接着把它收掉。
        """
        transport = self._transport
        if transport is None:
            self._client = None
            return
        try:
            # 取消不吞：清理没做完就该让调用方知道这一轮被打断了。
            with contextlib.suppress(Exception):
                await transport.close(timeout=3.0)
        finally:
            # ``alive`` 为真说明没杀干净（关不掉，或取消落在 terminate 之前）。
            # 这时保留引用，调用方还能看见"进程还在"，下一轮也还能再试一次。
            if not transport.alive:
                self._transport = None
                self._client = None

    # -- 登录 ---------------------------------------------------------------

    async def login(self, *, on_auth_url=None, timeout=None, preferred="internal"):
        """发起登录，等用户扫码完成。

        ``on_auth_url`` 在链接到达时被调用（同步回调）。返回 ``authenticate``
        的结果与捕获到的链接。失败时抛 ACP 层的固定文案异常，调用方不需要
        判断上游原文。
        """
        limit = self._login_timeout if timeout is None else timeout
        async with self._login_lock:
            client = await self.ensure_started()
            method = client.pick_auth_method(preferred)
            if method is None:
                raise AcpError("acp cli offered no authentication method")
            try:
                result = await client.authenticate(
                    method, on_auth_url=on_auth_url, timeout=limit
                )
            except (AcpTimeoutError, asyncio.CancelledError):
                # 超时或被打断时，确实还是没登上：不能停在"未知"，否则状态栏
                # 会退回显示旧凭据的登录态。
                self._auth_state = False
                raise
            self._auth_state = True
            return result

    async def logout(self, *, purge_config=False) -> None:
        """关闭连接；``purge_config`` 时连带删掉登录凭据。

        删除前必须确认进程真的死了：活着的 CLI 手里还有 token，会把配置写回去，
        凭据删了又出现，而命令已经回复用户"已退出"。

        删两处，**凭据先删**：``credential_dir`` 才是登录闸门，配置残留是无害的，
        反过来不是。只删 ``CODEBUDDY_CONFIG_DIR`` 更不够——凭据在
        ``~/.local/share`` 下，删了配置目录"已断开连接"就成了谎话。
        """
        # 先等登录那一段退出。main 在调用前会 cancel 登录任务，正常情况下立即
        # 通过；直接调用本方法的运维脚本要注意这一点。
        async with self._login_lock:
            async with self._start_lock:
                transport = self._transport
                if transport is not None:
                    # 这里不吞异常：关不掉就不能往下删凭据。
                    await transport.close(timeout=3.0)
                    if transport.alive:
                        # 进程没死透就别删。它握着内存里的 token，下次刷新会把
                        # 凭据原样写回来，而用户已经看到"已退出登录"。
                        raise AcpError("acp process is still running")
                # 关失败时保留引用：那个进程还占着 pid，丢掉就再也杀不掉了。
                self._transport = None
                self._client = None
            if purge_config:
                self._purge_credential_dir()
                self._purge_config_dir()
                # 只有真删干净了才敢说未登录。状态字段记的是"结果"，不是"意图"；
                # 删失败时保留原状态，免得状态栏说未登录、CLI 却照样能出字。
                self._auth_state = False

    # 供上层在真实请求里同步登录态，不再额外探测。
    #
    # 调用方是 ``AcpChatBackend``：``UpstreamError(401, "acp_auth")`` 处报失败、
    # 正常收尾处报成功（见 ``core/acp/backend.py`` 的 ``_stream``）。后端只有在
    # 构造时拿到 ``runtime=`` 才会调它们——只注入 ``client=``（测试里常见）时为
    # ``None``，``auth_state`` 就只反映登录/退出动作本身。
    def note_auth_failure(self) -> None:
        self._auth_state = False

    def note_session_ok(self) -> None:
        self._auth_state = True

    def note_logged_out(self) -> None:
        """登出成功后的统一收口。

        和 :meth:`note_auth_failure` 的状态转换一模一样，但**名字必须分开**：那个
        说的是"这一轮撞了 401"，这个说的是"凭据已经删掉了"。池子要靠它把**每一条**
        运行时的登录态一起压成 False——只压主运行时的话，别的模型跑成功过一轮就仍然
        报 True，``/wb status`` 会在凭据已经删干净之后继续显示"已登录（CLI）"。
        """
        self._auth_state = False

    # -- 内部 ---------------------------------------------------------------

    def _purge_config_dir(self) -> None:
        """删掉 CLI 的配置目录。

        只删我们自己那一层：目录名必须恰好是 ``codebuddy``、不能是符号链接、
        不能是家目录或根目录。删不掉就抛异常——静默失败会让"已退出登录"变成谎话。
        """
        if not self._config_dir:
            return
        self._rmtree_guarded(
            self._config_dir, CONFIG_DIR_NAME, "acp config dir could not be removed"
        )

    def _purge_credential_dir(self) -> None:
        """删掉 CLI 真正的登录凭据目录。

        这个目录不在 ``config_dir`` 里，删了配置不等于退出登录。没配 ``home_dir``
        时**必须报错**：拿不准凭据在哪却回复"已断开连接"，正是这次修掉的那个谎话。
        """
        if not self._home_dir:
            raise AcpError("acp credential dir is not configured")
        self._rmtree_guarded(
            self.credential_dir,
            os.path.basename(os.path.join(*AUTH_SUBPATH)),
            "acp credential dir could not be removed",
            # 信任根是 home_dir：CLI 只会往它下面写。跑到外面去就说明路径算错了，
            # 那一刀不能落——删错目录是不可逆的。
            root=self._home_dir,
        )

    def _rmtree_guarded(self, path, expected_basename, failure_message, *, root=None):
        """删一层目录；任何一条守卫不通过都**报错**，只有"本来就不存在"才静默。

        "拒绝"与"删成功"必须能被调用方区分。静默返回会让 ``logout`` 回复"已退出
        登录"，而目录还原封不动躺着——那正是这次要根除的谎话。
        """
        if os.path.islink(path):
            # 指向别处的同名目录：底下可能是共享卷或用户自己的数据，顺着删下去
            # 就是删别人的东西。
            raise AcpError(failure_message + " (target is a symlink)")
        real = os.path.realpath(path)
        if root is not None:
            real_root = os.path.realpath(root)
            try:
                inside = os.path.commonpath([real_root, real]) == real_root
            except ValueError:
                # Windows 跨盘符时 commonpath 直接抛异常，按"不在里面"处理。
                inside = False
            if not inside or real == real_root:
                raise AcpError(failure_message + " (target is outside the data directory)")
        home = os.path.realpath(os.path.expanduser("~"))
        if real == home or real == os.sep:
            raise AcpError(failure_message + " (target is not a dedicated directory)")
        if os.path.basename(real) != expected_basename:
            raise AcpError(failure_message + " (unexpected directory name)")
        if not os.path.exists(real):
            return
        shutil.rmtree(real, ignore_errors=True)
        if os.path.exists(real):
            raise AcpError(failure_message)


__all__ = [
    "AcpRuntime",
    "default_argv",
    "DEFAULT_LOGIN_TIMEOUT",
    "CONFIG_DIR_NAME",
    "PRODUCT_DATA_DIR",
    "AUTH_SUBPATH",
    "CREDENTIAL_FILE",
    "CREDENTIAL_LOGGED_OUT",
]
