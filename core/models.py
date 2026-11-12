"""聊天模型清单：从本机装好的 CLI 里读，不联网、不要登录。

为什么改成读本地：插件现在通过 ACP 跟 CLI 说话，而**只有 CLI 自己知道这次安装
到底认哪些模型 ID**。它的帮助文本里那行 ``--model ... Currently supported: (...)``
就是这份集合，末尾那串 ``custom-local:<id>`` 是从本地 ``models.json`` 推出来的
（运维自己接的网关）。读它只花一个很短的本地进程，既不需要登录态，也不需要网络。

回退：换了个构建、帮助文本里没有那一行时，直接读 ``models.json``，给每个 id 加上
``custom-local:`` 前缀——那正是 ``--model`` 会接受的写法。

刻意不做的两件事：

* **不缓存到磁盘。** 这份清单由 CLI 安装和 ``models.json`` 决定，两者都可能被改动；
  缓存在内存里、由 ``/wb models`` 显式刷新，比留一份会过期的落盘副本更省心。
* **不猜。** 读不到就是空列表，绝不用一份写死的模型名兜底——那会让用户以为某个
  根本没装的模型可用。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time

# 一次 ``--help`` 的上限。它不联网，正常几十毫秒；给足余量只为防卡死。
_HELP_TIMEOUT = 20.0

# 清单的保鲜期（秒）。见 :meth:`ModelCatalog.ensure`。
DEFAULT_CATALOG_AGE = 60.0

# 读失败之后的最短重试间隔（秒）。没有它，``ensure()`` 在"CLI 被卸载"和"CLI 这一
# 瞬间读不出来"两种情况下都会每次调用 spawn 一个进程——``/v1/models`` 是给客户端
# 轮询的，那就等于把自己变成了进程制造机，而且每次都吃满 ``_HELP_TIMEOUT``。
DEFAULT_RETRY_AFTER = 5.0

# 收尸时等进程真正退出的上限（秒）。``kill()`` 是信号，``wait()`` 才是回收。
_REAP_TIMEOUT = 5.0

# 帮助文本里的锚点，取自 ``--model <model>`` 那行。
_MARKER = "Currently supported:"

# 图片模型也在 ``--model`` 的清单里，但它们不接 chat completions，选进来只会
# 在真正发 prompt 时失败。宁可不出现在候选里。
#
# 为什么用子串匹配而不是白名单：白名单意味着"我猜这份 CLI 装了什么"，猜错的代价
# 比子串误伤大得多——白名单过期的表现是**新装的图片模型漏进来**（选了必失败），
# 或者**把某个真模型挡在外面**（用户根本选不到，但至少不会烧一次配额）。子串的
# 误伤只发生在"名字里恰好含 image 的聊天模型"，那种模型在候选里消失，用户看不到、
# 也发不出必失败的请求。两害相权，选误伤面更小的那个。
_IMAGE_HINT = "image"

# 自定义网关模型在 ``--model`` 里的前缀。
CUSTOM_PREFIX = "custom-local:"

# 模型 ID 的字符上限。聊天入口必须拒绝更长的 ``model``：它会原样进 ``--model``
# 的 argv 和日志行，不掐住就是一条能顶满请求体上限的字符串。这条线对这份清单同样
# 成立——清单是那个入口的**广告**，广告里出现一个入口收不下的 ID，客户端照着选，
# 每一次都是 400，而用户看不出为什么。
MAX_MODEL_ID_CHARS = 128

# 旧版本在模型 ID 后面拼的显示后缀：``<id> [0.11x]`` / ``<id> [0.11x · 夜间折扣]``
# / ``<id> [倍率未知]``。倍率下线后这个后缀不再产生，但运维存下来的配置里可能还
# 有，所以保留这条精确、锚定在末尾的剥除规则；自由格式的方括号一概不动。
_ALIAS_MULTIPLIER = r"[0-9]+(?:\.[0-9]+)?x|倍率未知"
_ALIAS_SUFFIX_RE = re.compile(
    r" \[(?:" + _ALIAS_MULTIPLIER + r")(?: · [^\[\]\r\n]+)?\]$"
)


def strip_model_alias(value):
    """剥掉本插件旧版拼的显示后缀，返回纯模型 ID。

    ``"wb-1 [0.11x · 夜间折扣]"`` → ``"wb-1"``；纯 ID 或末尾是非标准方括号的值
    原样返回，非字符串也原样返回（不喂给正则）。
    """
    if not isinstance(value, str):
        return value
    return _ALIAS_SUFFIX_RE.sub("", value)


def _split_ids(blob):
    """把 ``"a, b, c"`` 拆成去重后的 id 列表，顺手丢掉图片模型。"""
    result = []
    seen = set()
    for raw in blob.split(","):
        model_id = raw.strip().strip("`'\"")
        if not model_id or model_id in seen:
            continue
        if _IMAGE_HINT in model_id.lower():
            continue
        seen.add(model_id)
        result.append(model_id)
    return result


def parse_help_models(text):
    """从 ``--help`` 文本里取出 ``--model`` 支持的全部 id；取不到返回 ``[]``。

    形状是 ``... Currently supported: (a, b, c)``，整段在一行里；这里不按行切，
    只认 ``(`` 与它后面第一个 ``)``，免得被换行或前置说明干扰。
    """
    if not isinstance(text, str):
        return []
    marker = text.find(_MARKER)
    if marker < 0:
        return []
    opened = text.find("(", marker + len(_MARKER))
    if opened < 0:
        return []
    closed = text.find(")", opened + 1)
    if closed < 0:
        return []
    return _split_ids(text[opened + 1 : closed])


def parse_config_models(document):
    """从 ``models.json`` 的解析结果里取 id，补上 ``custom-local:`` 前缀。

    接受两种形状：``{"models": [...]}``（现行）与裸列表；每项可以是对象（读 ``id``）
    或纯字符串。任何一项不合法就跳过，绝不整份放弃。

    带 ``custom-local:`` 前缀的条目会先剥一层再看：剩下的是空串、或者还叠着一层
    前缀，整条丢掉（那种 id CLI 不认，留着只会让人选到必然失败的模型）；前缀写成
    别的的大小写则归一成小写，因为 CLI 只认小写那一种写法。
    """
    if isinstance(document, dict):
        items = document.get("models")
    elif isinstance(document, list):
        items = document
    else:
        return []
    if not isinstance(items, list):
        return []

    result = []
    seen = set()
    for item in items:
        if isinstance(item, dict):
            raw = item.get("id")
        elif isinstance(item, str):
            raw = item
        else:
            continue
        if not isinstance(raw, str):
            continue
        model_id = raw.strip()
        if not model_id:
            continue
        if model_id[: len(CUSTOM_PREFIX)].lower() == CUSTOM_PREFIX:
            # 已经带前缀：先剥掉这一层，再看里面剩什么。剥完是空的、或者里面还叠着
            # 一层（``custom-local:custom-local:a``），都只能来自脏数据——那个 id
            # CLI 不认，放进候选就会被运维当成"这台机器真有的模型"选进去，选中必失败。
            rest = model_id[len(CUSTOM_PREFIX) :].strip()
            if not rest or rest[: len(CUSTOM_PREFIX)].lower() == CUSTOM_PREFIX:
                # 丢掉，但**不记日志**：这条脏数据会在每次刷新时重复出现，记下来
                # 就是刷屏；而它要传达的信息（"你手写的配置里有条没用的"）已经在
                # 候选列表里如实体现了——那条模型不在候选里。
                continue
            # 前缀大小写归一：``Custom-Local:x`` 同样只有脏数据一个来源，但把它改写成
            # CLI 认的那个写法，比整条丢掉对运维更友好，也没有凭空造出模型。
            full = CUSTOM_PREFIX + rest
        else:
            full = CUSTOM_PREFIX + model_id
        if full in seen:
            continue
        if _IMAGE_HINT in model_id.lower():
            continue
        seen.add(full)
        result.append(full)
    return result


def _publishable(models):
    """去掉聊天入口收不下的 ID，别把用不了的东西写进清单。

    ``--help`` 打出来的 ID 都是短的，超长的只可能来自手写的 ``models.json``。丢掉
    而不是截断：截出来的 ID 是**另一个**模型，CLI 那边根本不认，那比不发布更糟。
    也不记日志——脏配置每次刷新都会被读到，记下来就是刷屏，而它要传达的信息（"这条
    选不了"）已经体现在清单里了。
    """
    return [model for model in models if len(model) <= MAX_MODEL_ID_CHARS]


class ModelCatalog:
    """内存里的模型清单，来源是本地 CLI 的帮助文本，回退到 ``models.json``。"""
    def __init__(
        self,
        cli_path,
        *,
        config_path=None,
        env=None,
        timeout=_HELP_TIMEOUT,
        clock=None,
        max_age=DEFAULT_CATALOG_AGE,
        retry_after=DEFAULT_RETRY_AFTER,
    ):
        self._cli_path = cli_path
        self._config_path = config_path
        self._env = dict(env) if env else None
        self._timeout = timeout
        self._clock = time.monotonic if clock is None else clock
        self._max_age = max_age
        # 失败后的最短重试间隔不能比保鲜期还长，否则"清单过期了却总也不重读"。
        self._retry_after = min(float(retry_after), max_age)
        self._models = []
        self._source = "none"
        # 上次**成功**读取的时刻；``None`` 表示还没读成过。
        self._read_at = None
        # 上次**尝试**读取的时刻，成败都记。只有它能把"一直失败"与"还没试过"
        # 区分开，也才有地方挡住失败重试。
        self._last_attempt = None
        # 让并发的轮询只 spawn 一次。显式刷新（``refresh``）不持锁：它是"我就要重读"，
        # 而 ``ensure`` 是"能不读就不读"，两件事的语义不一样。
        self._refresh_lock = asyncio.Lock()

    @property
    def models(self) -> list:
        return list(self._models)

    @property
    def source(self) -> str:
        """``"help"`` / ``"config"`` / ``"none"``——清单是从哪读到的。

        这是**上次成功**读取的来源。读失败时它不变，所以单看它分不出"刚读到"与
        "多少小时前读到的"；要分辨就看 :attr:`age` / :attr:`stale`。
        """
        return self._source

    @property
    def age(self):
        """距上次成功读取过了多少秒；从未读成过是 ``None``。"""
        if self._read_at is None:
            return None
        return max(0.0, self._clock() - self._read_at)

    @property
    def stale(self) -> bool:
        """手上这份清单是否已经过了保鲜期（读失败被保留下来时就是这种状态）。"""
        if not self._models:
            # 空 ≠ 过期：从来没读到过就是"没有"，不是"有一份旧的"。
            return False
        return self.age >= self._max_age

    async def refresh(self) -> list:
        """重读一次。失败时保留上一次成功的结果，绝不清空成"没有模型"。

        **尝试时刻在读之前就记。** 读到一半被取消（上层超时、客户端断开）时，函数
        其余部分一步都走不到；若把盖章放在读之后，一次取消就会让退避闸门失效，
        之后每次 ``ensure()`` 都重新 spawn、重新吃满一次超时——正是那个闸门要堵的
        事。``_read_at`` 仍然记在成功之后，因为它回答的是"这份清单有多新"。

        **失败之后再盖一次。** 读一次可能要吃掉 ``--help`` 那 20 秒超时。只在读之前
        盖章的话，等它失败返回，5 秒的退避期早过了；排在 ``_refresh_lock`` 上的调用方
        会一个接一个地再吃满一次超时。从"这次失败结束"开始算，闸门才真的挡得住。
        """
        self._last_attempt = self._clock()
        models, source = await self._discover()
        if models:
            self._models = models
            self._source = source
            self._read_at = self._clock()
        else:
            self._last_attempt = self._clock()
        return self.models

    async def ensure(self) -> list:
        """按需重读：清单还空着、或上次成功读取已超过 ``max_age`` 秒。

        为什么不是"每次都读"：读一次要 spawn 一个短命进程。``/v1/models`` 是给
        客户端轮询的接口，每来一次就 spawn 一次等于把插件变成进程制造机。为什么
        也不是"只在启动时读一次"：CLI 可能装晚了一步、``models.json`` 也可能刚被
        改过，那份清单就永远停在空列表或旧值上——所以给一个短的保鲜期。

        失败也要挡：只按"上次成功"计时的话，CLI 被卸载或卡死时每次轮询都会再 spawn
        一个进程、再吃满一次超时。"上次尝试"距今不到 ``retry_after`` 就直接放弃，
        把下一次尝试推后。显式刷新（``/wb models``）走 :meth:`refresh`，不受它限制。

        整个过程持锁：两个并发的轮询都判断成"过期了"，就会各 spawn 一个 CLI、各吃
        满一次超时。锁只保护"判断 + 读"，不让第二个请求重复第一次的等待以外的东西。
        """
        async with self._refresh_lock:
            now = self._clock()
            if self._models and self.age < self._max_age:
                return self.models
            if (
                self._last_attempt is not None
                and (now - self._last_attempt) < self._retry_after
            ):
                return self.models
            return await self.refresh()

    async def _discover(self):
        # **先滤再判来源。** 判在滤之前的话，``--help`` 只报出一串超长 ID 时
        # ``models`` 非空、来源就定成 help，而真正发布出去的是滤完的空清单——配置
        # 那份回退再也轮不到，清单从此一直空着。过滤之后的"空"才算"这条来源没货"。
        models = _publishable(await self._from_help())
        if models:
            return models, "help"
        # 读一个小 JSON 也要开线程：本方法在事件循环里被调，卡住的是整个插件的
        # 所有请求，不值得为省一次线程池调度去赌那个文件永远在本地磁盘上。
        models = _publishable(await asyncio.to_thread(self._from_config))
        if models:
            return models, "config"
        return [], "none"

    async def _from_help(self):
        if not self._cli_path:
            return []
        try:
            process = await asyncio.create_subprocess_exec(
                self._cli_path,
                "--help",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=self._env,
            )
        except (OSError, ValueError):
            # 没装、路径错、不是可执行文件：回退到读配置，不抛给调用方。
            return []
        try:
            stdout, _ = await asyncio.wait_for(
                process.communicate(), timeout=self._timeout
            )
        except asyncio.TimeoutError:
            # 收尸期间可能刚好来了一次取消；它被 _reap 吞掉了，这里补抛——
            # cancel 只投递一次，不补就真丢了（下一次自动抛不会发生）。
            if await _reap(process):
                raise asyncio.CancelledError
            return []
        except asyncio.CancelledError:
            # 取消也必须收尸：卡住的 CLI 若留在那儿，管道与 pid 都算泄漏。
            await _reap(process)
            raise
        return parse_help_models(stdout.decode("utf-8", "replace"))

    def _from_config(self):
        if not self._config_path or not os.path.isfile(self._config_path):
            return []
        try:
            with open(self._config_path, "r", encoding="utf-8") as handle:
                document = json.load(handle)
        except (OSError, ValueError):
            return []
        return parse_config_models(document)


async def _reap(process) -> bool:
    """杀掉一个卡住的子进程并**等它真的退出**；返回"收尸期间是否被取消过"。

    ``kill()`` 只是发信号，``wait()`` 才是回收。少了后者，进程对象停在
    "killed but not waited"，stdout 的管道传输没人关，事件循环关闭或插件热重载时
    会冒出 "Transport closing" 与僵尸告警——而超时路径恰恰是最容易走到的那条。

    **调用方契约**：本函数会吞掉 ``CancelledError``（收尸不能半路被打断），并把
    "吞掉过"通过返回值告诉调用方——取消路径必须自己 ``raise``，超时路径也要照着
    补一次 ``raise``：取消只投递一次，不补就真的丢了。改这个函数之前先看
    ``_from_help`` 的两个 except 分支。
    """
    cancelled = False
    try:
        process.kill()
    except (ProcessLookupError, OSError):
        pass
    try:
        # shield：外层被取消时别把"等它退出"这一步一起取消掉，否则又是一条
        # killed 但没 wait 的进程。
        await asyncio.wait_for(
            asyncio.shield(process.wait()), timeout=_REAP_TIMEOUT
        )
    except asyncio.CancelledError:
        cancelled = True
    except (asyncio.TimeoutError, OSError, ProcessLookupError):
        pass
    return cancelled


__all__ = [
    "CUSTOM_PREFIX",
    "DEFAULT_CATALOG_AGE",
    "DEFAULT_RETRY_AFTER",
    "MAX_MODEL_ID_CHARS",
    "ModelCatalog",
    "parse_config_models",
    "parse_help_models",
    "strip_model_alias",
]
