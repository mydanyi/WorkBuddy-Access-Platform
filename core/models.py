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
import contextlib
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

# 连败退避的上限（秒）。只按固定的 ``retry_after`` 限速时，"CLI 永久坏掉"这件事在
# 时间轴上没有成本：每 5 秒一次、每次吃满 20 秒超时，等于每 25 秒就 spawn 一个注定
# 失败的进程，永不停止。失败越连续、退避越长，恢复靠显式刷新（``/wb models``，不受
# 它限制）或下一轮定时重读，而不是靠更快地重试。
MAX_RETRY_AFTER = 300.0

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

    **形状不对返回 ``None``，不返回空表。** 空表是"这份文件里一条都没有"，调用方要
    照它把候选清空；``{"models": {}}``、``null``、裸字符串这些是"这份文件长得不对"，
    跟"读不到"一样，只能保留上一份。两者混成一个 ``[]`` 的话，运维手滑把 ``models``
    写成对象、恰好又赶上 ``--help`` 读不出货，候选里的自定义模型会一次性消失。

    带 ``custom-local:`` 前缀的条目会先剥一层再看：剩下的是空串、或者还叠着一层
    前缀，整条丢掉（那种 id CLI 不认，留着只会让人选到必然失败的模型）；前缀写成
    别的的大小写则归一成小写，因为 CLI 只认小写那一种写法。
    """
    if isinstance(document, dict):
        items = document.get("models")
    elif isinstance(document, list):
        items = document
    else:
        return None
    if not isinstance(items, list):
        return None

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
        # 两个来源**分开记账**，发布出去的是它们的并集：
        #
        # * ``_help_models`` 只在 ``--help`` 读成时整份替换。那份是全集，末尾本来就
        #   带着 ``models.json`` 推出来的 ``custom-local:`` 条目。
        # * ``_config_models`` 每次读到 ``models.json`` 都整份替换。
        #
        # 为什么分开：只做"并集"的话，删除不可表达——运维从 ``models.json`` 里删掉
        # 一个网关，在 help 恢复之前它会一直挂在清单上（也就一直挂在聊天入口的允许
        # 列表里）。分开记账之后，config 那份每次都是整体换掉，删除照样生效。
        self._help_models = []
        self._config_models = []
        # 上次**成功**读取的时刻；``None`` 表示还没读成过。
        self._read_at = None
        # 上次**尝试**读取的时刻，成败都记。只有它能把"一直失败"与"还没试过"
        # 区分开，也才有地方挡住失败重试。
        self._last_attempt = None
        # 连续失败的次数：``--help`` 没读成就加一，读成清零。**配置清单命中不算成功**
        # ——这一趟 ``--help`` 还是没读到，只是从 ``models.json`` 捞到一部分。
        self._failures = 0
        # 一次只读一趟：并发的轮询不必各 spawn 一个 CLI，显式刷新（``/wb models``）
        # 也不该在别人正读着的时候再拉一个进程出来。
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

    @property
    def authoritative(self) -> bool:
        """这份清单够不够格当聊天入口的**允许列表**。

        只有手里真有一份 ``--help`` 全集时才够格。判据直接看 ``_help_models`` 而不是
        ``_source`` 字符串：字符串只说"上次成功是从哪读的"，说得出一份从来没拿到过的
        全集。

        ``models.json`` 那条回退永远只是**子集**（只含运维手写的 ``custom-local:``）。
        拿子集当允许列表，会让所有内置模型变成 400——正是"子集不许顶替全集"要防的
        事。拒收替换只挡得住"手里原来有全集"那一路；**首次读取就没读成**时手里本来
        没有全集，拒收无从谈起，只能靠这个标志告诉调用方"别拿它拦人"。
        """
        return bool(self._help_models)

    @property
    def retry_wait(self) -> float:
        """距上次失败的尝试要隔多久才允许再试一次（秒），随连败次数翻倍。

        逐次翻倍、到了上限就停：不写 ``retry_after * 2 ** (n - 1)``，那个式子在连败
        上千次之后会抛 ``OverflowError``（浮点指数越界）。上限之外再乘也没有意义。
        """
        wait = self._retry_after
        for _ in range(self._failures - 1):
            if wait >= MAX_RETRY_AFTER:
                break
            wait *= 2.0
        return min(wait, MAX_RETRY_AFTER)

    def _merged(self) -> list:
        """拼出要发布的清单：``--help`` 的内置条目在前，自定义网关在后。

        自定义那条**跟配置走，不跟 help 走**：help 快照里末尾那串 ``custom-local:``
        是上一次读它时 ``models.json`` 的样子，运维删掉一个网关，它会在 help 下一次
        读成之前一直挂着——而 "help 一直读不成" 恰恰是这段代码最可能被用到的场景。
        所以 help 那一半只留内置的，自定义的永远取 ``_config_models``（每次整份替换）。
        """
        merged = [
            model for model in self._help_models if not model.startswith(CUSTOM_PREFIX)
        ]
        for model in self._config_models:
            if model not in merged:
                merged.append(model)
        return merged

    async def refresh(self) -> list:
        """重读一次（持锁）。失败时保留上一次成功的结果，绝不清空成"没有模型"。

        **尝试时刻在读之前就记。** 读到一半被取消（上层超时、客户端断开）时，函数
        其余部分一步都走不到；若把盖章放在读之后，一次取消就会让退避闸门失效，
        之后每次 ``ensure()`` 都重新 spawn、重新吃满一次超时——正是那个闸门要堵的
        事。``_read_at`` 仍然记在成功之后，因为它回答的是"这份清单有多新"。

        **失败之后再盖一次。** 读一次可能要吃掉 ``--help`` 那 20 秒超时。只在读之前
        盖章的话，等它失败返回，5 秒的退避期早过了；排在 ``_refresh_lock`` 上的调用方
        会一个接一个地再吃满一次超时。从"这次失败结束"开始算，闸门才真的挡得住。

        **不拿子集顶替全集。** 手里那份是从 ``--help`` 读来的，这一趟只回退到
        ``models.json``（必然更小、只含 ``custom-local:``），那就不拿它替换 help 那份
        ——见下面那段注释。判据是"来源变差了"，不是"新清单更短"：两个来源本来就不可比。

        **但也不把这一趟白读。** 拒收的是"替换全集"，不是"内容"：``models.json`` 里
        新加的网关照样发布出去。CLI 坏掉时"临时挂一个本地网关顶上"正是最可能发生的
        运维动作，整份丢掉会让它在 help 恢复前一直看不见。

        **持锁**：显式刷新不受退避闸门限制（"我就要重读"），但也不该与一次正在飞的
        读并发出第二个 CLI 进程。
        """
        async with self._refresh_lock:
            return await self._read_once()

    async def _read_once(self) -> list:
        """:meth:`refresh` 的正文；只由已持锁的调用方进来。"""
        self._last_attempt = self._clock()
        try:
            models, source = await self._discover()
        except asyncio.CancelledError:
            # 取消也记一次失败：这一趟已经花掉一次 spawn 了。不记的话，反复被早取消的
            # 调用方（上层超时、客户端断开）会让退避永远停在最短那一档，比修前还密。
            self._failures += 1
            self._last_attempt = self._clock()
            raise
        if models and source == "help":
            self._help_models = models
            # help 那份末尾本来就带着 ``models.json`` 推出来的 ``custom-local:`` 条目。
            # 把它们挪进 config 那本账（见 :meth:`_merged`），这样"自定义网关"永远只有
            # 一个来源：help 里那一份只是**上一次**读配置时的样子，照着它过日子，运维
            # 删掉的网关就删不掉了。
            self._config_models = [
                model for model in models if model.startswith(CUSTOM_PREFIX)
            ]
            self._source = "help"
            self._read_at = self._clock()
            self._failures = 0
        else:
            # 这一趟 ``--help`` 没读到（超时、CLI 被换掉，或者干脆没有那一行），
            # 配置清单兜底。**不算成功**：``_failures`` 照加，退避照升级——否则
            # "CLI 永久坏掉 + ``models.json`` 有货"会变成每 ``max_age`` 秒 spawn 一个
            # 注定失败的进程，一次都不升级。
            self._failures += 1
            self._last_attempt = self._clock()
            if source == "config":
                # 读到就是读到，**空的也算**：文件被清空等于"没有自定义网关了"，
                # 留着上一份等于把已经删掉的网关又挂回去。读失败（文件没了、JSON 坏
                # 了）走的是 ``source == "none"``，那才该保留旧值。
                self._config_models = models
                if not self._help_models:
                    self._source = "config"
                    self._read_at = self._clock()
        self._models = self._merged()
        return self.models

    async def ensure(self) -> list:
        """按需重读：清单还空着、或上次成功读取已超过 ``max_age`` 秒。

        为什么不是"每次都读"：读一次要 spawn 一个短命进程。``/v1/models`` 是给
        客户端轮询的接口，每来一次就 spawn 一次等于把插件变成进程制造机。为什么
        也不是"只在启动时读一次"：CLI 可能装晚了一步、``models.json`` 也可能刚被
        改过，那份清单就永远停在空列表或旧值上——所以给一个短的保鲜期。

        失败也要挡：只按"上次成功"计时的话，CLI 被卸载或卡死时每次轮询都会再 spawn
        一个进程、再吃满一次超时。"上次尝试"距今不到 :attr:`retry_wait` 就直接放弃，
        把下一次尝试推后。显式刷新（``/wb models``）走 :meth:`refresh`，不受它限制。

        整个过程持锁：两个并发的轮询都判断成"过期了"，就会各 spawn 一个 CLI、各吃
        满一次超时。锁只保护"判断 + 读"，不让第二个请求重复第一次的等待以外的东西。
        :meth:`refresh` 拿的是同一把锁——"我就要重读"不该再拉第二个进程出来。

        **手里有旧清单时不等锁。** 一轮读要吃掉整个 ``_HELP_TIMEOUT``，全程持锁；
        help 坏掉时 ``stale`` 永真、每个调用方都进不了 fast-path，于是全部在锁上排队，
        而其中不少是消息路径上"这个模型认不认"的校验——白等半个超时窗。旧清单就在手
        边，先给它便是。空清单不这么办：那说明还没读成过，让它等那唯一的一次真读。
        """
        if self._models and self._refresh_lock.locked():
            return self.models
        async with self._refresh_lock:
            now = self._clock()
            if self._models and self.age < self._max_age:
                return self.models
            if (
                self._last_attempt is not None
                and (now - self._last_attempt) < self.retry_wait
            ):
                return self.models
            return await self._read_once()

    async def _discover(self):
        # **先滤再判来源。** 判在滤之前的话，``--help`` 只报出一串超长 ID 时
        # ``models`` 非空、来源就定成 help，而真正发布出去的是滤完的空清单——配置
        # 那份回退再也轮不到，清单从此一直空着。过滤之后的"空"才算"这条来源没货"。
        models = _publishable(await self._from_help())
        if models:
            return models, "help"
        # 读一个小 JSON 也要开线程：本方法在事件循环里被调，卡住的是整个插件的
        # 所有请求，不值得为省一次线程池调度去赌那个文件永远在本地磁盘上。
        config = await asyncio.to_thread(self._from_config)
        if config is not None:
            # **读到就算数，空的也算。** 空表说的是"文件里就是没有条目"——可能是运维
            # 刚清空，也可能是这一版 CLI 的自定义网关都配在别处。把它跟"读不到"
            # （文件没了、JSON 坏了）分开：前者该照它清账，后者才该保留上一份。
            return _publishable(config), "config"
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
        """读 ``models.json``：读到就返回列表（**可能是空表**），没读到返回 ``None``。

        两者必须分开。空表是"这份文件里没有条目"，调用方要照它把旧的自定义网关清掉；
        ``None`` 是"没读到"（没配路径、文件不在、JSON 坏了、**或者形状不对**），
        那种情况下保留上一份比清空安全——一次手滑的编辑不该让整台机器上的网关消失。
        形状判断在 :func:`parse_config_models` 里：它给 ``None``，这里就照没读到处理。
        """
        if not self._config_path or not os.path.isfile(self._config_path):
            return None
        try:
            with open(self._config_path, "r", encoding="utf-8") as handle:
                document = json.load(handle)
        except (OSError, ValueError):
            return None
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
    # 管道先关掉。``communicate()`` 被取消之后，stdout 的读传输可能停在"暂停"上：
    # 没人再读，EOF 到不了，而 ``Process.wait()`` 等的是子进程传输——管道不关它
    # 就永远不醒。收尸这一步不读内容，关掉最省事。
    _close_pipes(process)
    # 先把"等它退出"这条任务抓在手里。直接 ``await asyncio.shield(process.wait())``
    # 是挡不住取消的：``CancelledError`` 会在**这一层**立刻抛出，shield 里的等待还
    # 在跑，我们却已经返回了——调用方照着 ``cancelled`` 马上 ``raise``、目录的锁也
    # 放掉，而进程还没真退，函数名与契约里的"等它真的退出"当场落空。留着这条任务，
    # 取消之后接着等（总共仍以 ``_REAP_TIMEOUT`` 为限），最后用返回值交差。
    waiter = asyncio.ensure_future(process.wait())
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _REAP_TIMEOUT
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        try:
            await asyncio.wait_for(asyncio.shield(waiter), timeout=remaining)
            break
        except asyncio.CancelledError:
            # 收尸不能半路被打断：记下这一次取消，接着等。
            cancelled = True
            continue
        except (asyncio.TimeoutError, OSError, ProcessLookupError):
            break
    if not waiter.done():
        # 到点了还没退。这条等待不能留在这儿：它攥着进程对象，事件循环关掉时会冒出
        # "Task was destroyed but it is pending"。收尸的承诺是"等它退出"，做不到
        # 就当场撤掉这条等待，别留个半开着的东西在后面。
        waiter.cancel()
        try:
            await asyncio.gather(waiter, return_exceptions=True)
        except asyncio.CancelledError:
            cancelled = True
    return cancelled


def _close_pipes(process) -> None:
    """把子进程的管道关掉；替身进程没有 ``_transport`` 时什么也不做。"""
    transport = getattr(process, "_transport", None)
    if transport is None:
        return
    with contextlib.suppress(Exception):
        transport.close()


__all__ = [
    "CUSTOM_PREFIX",
    "DEFAULT_CATALOG_AGE",
    "DEFAULT_RETRY_AFTER",
    "MAX_MODEL_ID_CHARS",
    "MAX_RETRY_AFTER",
    "ModelCatalog",
    "parse_config_models",
    "parse_help_models",
    "strip_model_alias",
]
