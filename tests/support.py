"""测试共用的底层夹具（不是测试用例本身）。

这里的每一样东西都只做两件事之一：给测试提供一个**真实**边界（一个真的 aiohttp
服务端、一份真的 CLI 凭据文件），或者提供一个**脚本化**的对端（按脚本产 chunk 的
聊天后端）。凡是能跑生产代码的地方都不用 mock —— 上一版的 ``test_chat_gateway``
既是夹具又是用例，导致改一处架构就要连累四个文件；夹具和用例分开放之后，改夹具就
只改这一个文件。
"""

from __future__ import annotations

import collections
import inspect
import json
import os
import sys
import tempfile
from pathlib import Path

import aiohttp
from aiohttp import web

# --- 让 ``astrbot_plugin_workbuddy`` 在任何发现方式下都能导入 ----------------
_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]  # 含 astrbot_plugin_workbuddy/ 的那一层
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.errors import (  # noqa: E402
    ChatStreamError,
    UpstreamError,
)


def _resolve_base_tmp() -> Path:
    """临时根目录：环境变量优先，否则用任务目录下的 ``tmp``。

    刻意优先落在任务目录：构建机上的测试目录要能被人找到、清掉。但
    ``_HERE.parents[3]`` 只在"仓库就在任务目录里"这一种布局下可写——裸 clone 到
    ``/home/alice/...`` 时它算出来是 ``/home/tmp``，普通用户建不了，而这里是
    **导入期**，一抛就把整个测试套在收集阶段打死。所以先试着建；建不出来再退到
    系统临时目录下的固定子目录，宁可换个位置，也不让收集阶段失败。
    """
    env = os.environ.get("WORKBUDDY_TEST_TMP")
    if env:
        return Path(env)
    preferred = _HERE.parents[3] / "tmp"
    try:
        preferred.mkdir(parents=True, exist_ok=True)
        # 目录已存在但不可写时 mkdir 不会报错，所以真写一个探针文件再删掉。
        probe = preferred / ".write-probe"
        probe.write_text("", encoding="utf-8")
        probe.unlink()
    except OSError:
        fallback = Path(tempfile.gettempdir()) / "astrbot_plugin_workbuddy-tmp"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback
    return preferred


BASE_TMP = _resolve_base_tmp()


def make_temp_dir(prefix: str) -> Path:
    """在 ``BASE_TMP`` 下建一个本次用例独占的目录。"""
    BASE_TMP.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=prefix, dir=str(BASE_TMP)))


# --- 固定的假凭据（全部是虚构样本，从不读真实账号） -------------------------
API_KEY = "FICTIONAL-LOCAL-GATEWAY-KEY"
GATEWAY_KEY = "FICTIONAL-PLUGIN-GATEWAY-KEY"
#: 32 字节的 base64 主密钥；``CredentialStore`` 拿它加解密，长度差一点就抛。
MASTER_KEY = "RklDVElPTkFMLU1BU1RFUi1LRVktMzJCWVRFUyEhISE="

ACCESS_TOKEN = "FICTIONAL-ACCESS-TOKEN"
REFRESH_TOKEN = "FICTIONAL-REFRESH-TOKEN"
UID = "test-uid-support"
DOMAIN = "example.test.invalid"
MODEL = "wb-1"

SEED_CREDENTIALS = {
    "accessToken": ACCESS_TOKEN,
    "refreshToken": REFRESH_TOKEN,
    "uid": UID,
    "expiresAt": 4102444800000,
    "domain": DOMAIN,
    "realm": "cn",
}

#: CLI 把凭据写在哪：``<home_dir>/.local/share/CodeBuddyExtension/Data/Public/auth``。
#: ``home_dir`` 由插件数据目录决定，所以测试要模拟"已登录"就得往这儿写。
CLI_HOME_DIRNAME = "cli-home"
CLI_CREDENTIAL_SUBPATH = (
    ".local",
    "share",
    "CodeBuddyExtension",
    "Data",
    "Public",
    "auth",
)
CLI_CREDENTIAL_FILE = "Tencent-Cloud.coding-copilot.info"


def cli_credential_path(data_dir) -> Path:
    """插件数据目录 → CLI 凭据文件的实际落点。"""
    return Path(
        data_dir, CLI_HOME_DIRNAME, *CLI_CREDENTIAL_SUBPATH, CLI_CREDENTIAL_FILE
    )


def seed_cli_credentials(data_dir, **overrides) -> Path:
    """写一份"CLI 已登录"的凭据文件，形状与真实落盘一致。"""
    creds = dict(SEED_CREDENTIALS)
    creds.update(overrides)
    path = cli_credential_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "account": {"uid": creds["uid"]},
                "auth": {
                    "accessToken": creds["accessToken"],
                    "refreshToken": creds["refreshToken"],
                    "expiresAt": creds["expiresAt"],
                    "domain": creds["domain"],
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def clear_cli_credentials(data_dir) -> None:
    """删掉凭据文件与登出标记，回到"没登录过"的状态。"""
    path = cli_credential_path(data_dir)
    for target in (path, path.with_name(path.name + ".logged-out")):
        try:
            target.unlink()
        except FileNotFoundError:
            pass


class FakeUpstream:
    """真实跑在 127.0.0.1:0 上的 aiohttp 假上游，记录每一次请求。

    用真服务端而不是 mock：签到这条链要验的是"请求真的发出去、带的头对不对、
    401 之后真的只重发一次"，mock 掉传输层就什么也验不到。
    """

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
        # ``web.json_response`` 是同步的，所以一个路由完全可以写成普通函数。这里
        # 两种都接住：只 await 真正可等待的返回值，否则 aiohttp 会拿一个 Response
        # 去 await，抛 TypeError 之后悄悄变成 500——测试仍"通过"，但验的是别的东西。
        result = handler(request)
        # ``inspect.isawaitable`` 而不是 ``hasattr(result, "__await__")``：前者问的
        # 正是"这是不是该等的东西"，也接得住 __await__ 之外的 awaitable。
        if inspect.isawaitable(result):
            result = await result
        if result is None:
            # 处理器忘了 return：aiohttp 会把它变成 500，测试于是以"上游挂了"的样子
            # 红掉，跟真的上游故障分不开。当场炸出来更好查。
            raise AssertionError("handler for {} returned None".format(request.path))
        return result

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
            self._runner = None

    def calls_for(self, method: str, path: str):
        return [
            call
            for call in self.calls
            if call["method"] == method.upper() and call["path"] == path
        ]

    @staticmethod
    def json_body(call):
        return json.loads(call["body"].decode("utf-8")) if call["body"] else None


def ok(data=None):
    """成功信封。"""
    return web.json_response({"code": 0, "data": {} if data is None else data})


def biz(code, msg, status=200):
    """业务错误信封（HTTP 状态可以与业务码不一致）。"""
    return web.json_response({"code": code, "msg": msg}, status=status)


# --------------------------------------------------------------------------- #
# 脚本化聊天后端：按模型排队一条"这一轮会怎样"的剧本
# --------------------------------------------------------------------------- #

#: 剧本种类。``text`` 正常出字；``raise`` 抛一个给定的失败；``abort`` 出半句就断；
#: ``wait`` 在吐第一帧之前卡住，直到测试放行（模拟"流开了但首帧迟迟不来"）。
KIND_TEXT = "text"
KIND_RAISE = "raise"
KIND_ABORT = "abort"
KIND_WAIT = "wait"


class ScriptedChat:
    """按模型排队剧本的聊天后端，接口与 ``AcpChatBackend`` 一致。

    为什么不用真的 ACP 后端 + 假 CLI：网关与路由这层要验的是"失败怎么分类、备用
    怎么走、chunk 怎么转发"，让一个真 Node 进程参与只会把测试变慢、变脆。适配层
    自己的错误映射由 ``test_acp_backend`` 负责，这里只负责扮演一个**已经按契约
    出错**的后端。
    """

    def __init__(self, *, default_text="ok", on_error=None) -> None:
        self.scripts = {}
        self.attempts = []  # 每次真正尝试的 (model, observe_errors)
        self.payloads = []  # 每次真正尝试收到的 payload（验请求体用）
        self.default_text = default_text
        self.on_error = on_error
        self.limits = None

    @property
    def model(self):
        return MODEL

    def queue(self, model, kind, **kwargs) -> None:
        self.scripts.setdefault(model, collections.deque()).append((kind, kwargs))

    def queue_text(self, model, text) -> None:
        self.queue(model, KIND_TEXT, text=text)

    def queue_error(self, model, exc) -> None:
        self.queue(model, KIND_RAISE, exc=exc)

    def queue_abort(self, model, text="partial") -> None:
        self.queue(model, KIND_ABORT, text=text)

    def queue_wait(self, model, gate, *, text="", started=None) -> None:
        """卡在首帧之前，直到 ``gate`` 被 set。

        ``started`` 是给测试用的信号：它被 set 说明这一轮**已经被迭代到首帧之前**
        （async generator 的函数体只在第一次 ``__anext__`` 时才跑，不是构造时）。
        所以它证明的是"适配层真的消费了这个生成器"，不是"生成器对象被创建了"——
        若哪天适配层改成先建流不消费，这个信号就不会亮。
        不设它就只能靠 sleep 猜，测试会变脆。
        """
        self.queue(model, KIND_WAIT, gate=gate, text=text, started=started)

    @property
    def sent_models(self):
        return [model for model, _ in self.attempts]

    def chat_bodies(self):
        """每次真正尝试收到的 payload，按顺序。"""
        return list(self.payloads)

    async def stream(self, payload, *, observe_errors=True):
        model = payload.get("model") if isinstance(payload, dict) else None
        # 额度由后端花，不由网关判——真适配层就是这么做的（见 AcpChatBackend._stream）。
        # 少了这一句，网关的 rpm 用例全部失效而不会报错。但这也意味着**这个夹具
        # 替被测代码兑现了"适配层确实花额度"这个承诺**：那条契约不能靠它证明，
        # 由 ``test_acp_backend`` 的 ``test_every_attempt_spends_one_rpm_permit``
        # 用真后端 + 假 CLI 单独守门。改这里之前先看那条。
        if self.limits is not None:
            await self.limits.consume_rpm()
        self.attempts.append((model, observe_errors))
        self.payloads.append(payload)
        queue = self.scripts.get(model)
        kind, kwargs = queue.popleft() if queue else (KIND_TEXT, {"text": self.default_text})
        if kind == KIND_RAISE:
            exc = kwargs["exc"]
            if observe_errors and self.on_error is not None:
                # 分类本身由 ``AcpChatBackend`` 负责（见 test_acp_backend）；这里只要
                # 复现"观察者被调用"这一件事实，好让网关那层判"备用候选失败不上报"。
                self.on_error("upstream")
            raise exc
        if kind == KIND_WAIT:
            # 先报"已经开到这一步"，再卡在首帧之前：路由层看到的仍然是"一个字节
            # 都没吐"，所以它还可能换备用；而对调用方来说这条请求确实挂着。
            started = kwargs.get("started")
            if started is not None:
                started.set()
            await kwargs["gate"].wait()
        yield self._chunk({"role": "assistant"}, model=model)
        if kind == KIND_ABORT:
            yield self._chunk({"content": kwargs.get("text", "partial")}, model=model)
            raise ChatStreamError("upstream stream ended without a terminator")
        yield self._chunk({"content": kwargs.get("text", "")}, model=model)
        yield self._chunk({}, model=model, finish="stop")

    @staticmethod
    def _chunk(delta, *, model=None, finish=None):
        return {
            "id": "chatcmpl-scripted",
            "object": "chat.completion.chunk",
            "created": 111,
            "model": model or MODEL,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
        }


class StubCatalog:
    """只回答"清单是什么"的模型目录，用于网关的 ``/v1/models`` 与校验。

    形状必须与 ``core.models.ModelCatalog`` 对齐：状态页会读 ``age`` / ``stale``
    来决定要不要说"这份清单可能过期了"，替身少了这两个属性就会在命令里炸掉。
    """

    def __init__(self, models, *, source="help", age=0.0, stale=False) -> None:
        self.models = list(models)
        self.source = source
        self.reads = 0
        # 刚读过、没过期：替身默认站在"一切都新鲜"这一边上，要验过期的用例自己传。
        self.age = age
        self.stale = stale

    async def ensure(self):
        self.reads += 1
        return list(self.models)

    async def refresh(self):
        self.reads += 1
        return list(self.models)

    @property
    def authoritative(self) -> bool:
        """跟真目录同一条规矩：只有 ``--help`` 读来的那份能当允许列表用。"""
        return self.source == "help"


def upstream_error(status, *, code=None, retry_after=None, limit_scope=None):
    """构造一个形状正确的 ``UpstreamError``（脚本化后端用）。"""
    exc = UpstreamError(status, code, retry_after, limit_scope=limit_scope)
    return exc


__all__ = [
    "API_KEY",
    "BASE_TMP",
    "CLI_CREDENTIAL_FILE",
    "CLI_CREDENTIAL_SUBPATH",
    "CLI_HOME_DIRNAME",
    "DOMAIN",
    "FakeUpstream",
    "GATEWAY_KEY",
    "MASTER_KEY",
    "MODEL",
    "SEED_CREDENTIALS",
    "ScriptedChat",
    "StubCatalog",
    "UID",
    "biz",
    "clear_cli_credentials",
    "cli_credential_path",
    "make_temp_dir",
    "ok",
    "seed_cli_credentials",
    "upstream_error",
]
