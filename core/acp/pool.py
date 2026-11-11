"""按模型分进程的 CLI 池。

**为什么必须是多个进程。** ``--model`` 是进程启动参数：ACP 没有"换模型"的请求，
``session/new`` 也不收模型，一条进程从头到尾只用启动时那一个模型。想让调用方
指定哪个模型就用哪个，只能按模型各起一条。另一个看似更省事的做法是
``session/set_config_option``——但那是 CLI 自己声明的可选项，不同账号下内容不同
（有的账号只有 ``mode``），拿它当模型开关会在部分账号上静默失效。宁可多一个进程，
也不要一个"看起来切了其实没切"的开关。

**为什么要设上限。** 每条进程是一个 Node 运行时，几十 MB 起。模型清单可能有十几
个，全都常驻就是把插件变成内存黑洞。上限到顶时淘汰**最久未用的空闲进程**：正在
出字的一律不碰——杀掉一个正在回答的进程，比多占几十 MB 严重得多。

**为什么淘汰用 ``shutdown`` 而不是 ``close``。** 淘汰掉的运行时必须彻底作废：它
从池里被摘掉后不会再有人拿到它，但万一有旧引用还捏着，``close`` 之后
``ensure_started`` 还能把进程再拉起来——那条进程活过了插件生命周期，再也没人回收。
``shutdown`` 让它永久失效。

**默认模型那条是"主运行时"。** 登录、凭据读取、token 刷新都只要有一条能说话的
进程，跟聊天用的是不是同一个模型无关。池把默认模型那条标成 primary：它不会被淘汰，
``main`` 通过它登录与刷新，聊天请求打到同一个模型时复用同一条进程，不会多起一条。
"""

from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager

from .protocol import AcpError


def _sort_key(model):
    """``None``（默认模型）排在最前，其余按字典序；``None`` 不能直接参与比较。"""
    return (0, "") if model is None else (1, model)


def _normalize(model):
    """把调用方给的模型名归一成池里的键：空串与非字符串都是"CLI 自己的默认模型"。

    ``get`` 与 ``lease`` 都走它。两份各写一遍的结果今天一样，但只要归一规则改一次
    （比如允许去空白），``_runtimes`` 与 ``_busy`` 就会用上两个不同的键，占用计数
    从此对不上号——所以只留一份实现。
    """
    return model if isinstance(model, str) and model.strip() else None


class AcpRuntimePool:
    """按模型懒创建 :class:`core.acp.runtime.AcpRuntime`，并保证进程数有上限。"""

    def __init__(self, factory, *, max_processes=2, primary_model=None):
        """
        ``factory`` 是 ``(model) -> AcpRuntime``。池自己不认识命令行、目录或超时：
        那些都是装配参数，由调用方一次配好。
        """
        if (
            isinstance(max_processes, bool)
            or not isinstance(max_processes, int)
            or max_processes < 1
        ):
            raise ValueError("max_processes must be a positive integer")
        if not callable(factory):
            raise ValueError("factory must be callable")
        self._factory = factory
        self._max = max_processes
        self._primary_model = primary_model if isinstance(primary_model, str) else None
        # 模型 ID → 运行时。只放"当前有效"的；被淘汰的会从这里摘掉。
        self._runtimes = {}
        # 模型 ID → 正在它上面跑的请求数。只有 0 才允许淘汰。
        self._busy = {}
        # 模型 ID → 最近一次领取的序号，用来挑最久未用的那个。
        self._last_used = {}
        self._sequence = 0
        self._trim_lock = asyncio.Lock()
        self._closed = False
        # 登出期间为真。见 :meth:`logout`：光把进程关掉不够，这中间进来的请求会
        # 把进程重新拉起来、握着 token，然后凭据被删——用户看到"已退出"而账号还在。
        self._logging_out = False

    # -- 只读视图 -----------------------------------------------------------

    @property
    def primary(self):
        """默认模型那条运行时（登录 / 凭据 / 刷新都走它）。"""
        return self.get(self._primary_model)

    @property
    def max_processes(self) -> int:
        return self._max

    @property
    def auth_state(self):
        """整池的登录态：``True`` / ``False`` / ``None``（未知）。

        凭据文件是**所有**进程共用的那一份，所以只要有一条进程真跑通过，整池就是
        已登录——别的进程说"未登录"只是它启动得比登录早，不代表账号没了。反过来
        全都明确失败才算未登录；一条都没验证过就是未知，不猜。

        "共用一份凭据"也意味着换账号**必须走** :meth:`logout`：直接覆盖登录会让
        旧进程带着旧账号的 token 继续活着，而它报的 ``True`` 会把整池说成已登录。
        """
        states = [runtime.auth_state for runtime in self._runtimes.values()]
        if any(state is True for state in states):
            return True
        if states and all(state is False for state in states):
            return False
        return None

    def status(self) -> dict:
        """状态页要的只读快照：哪些模型有进程、哪条在跑。

        模型 ID 原样给出（``None`` 表示"用 CLI 自己的默认模型"）；排序时把 ``None``
        排在最前，免得它和字符串比较时把整个状态页拖崩。
        """
        return {
            "max_processes": self._max,
            "primary_model": self._primary_model,
            "models": sorted(self._runtimes, key=_sort_key),
            "running": sorted(
                (model for model, runtime in self._runtimes.items() if runtime.started),
                key=_sort_key,
            ),
        }

    # -- 领取 ---------------------------------------------------------------

    def get(self, model=None):
        """取该模型的运行时（首次只创建对象，不 spawn）。

        同步方法：它不 await，于是不可能在"我刚检查完就被人换掉"的窗口里出错。
        真正的进程拉起在 :meth:`core.acp.runtime.AcpRuntime.ensure_started`。

        **上限只在 :meth:`lease` 上成立。** ``get`` 不 trim，所以拿它的返回值直接
        ``ensure_started`` 就能绕过 ``max_processes`` 堆进程。除了登录/刷新那条
        "只要一条能说话的进程"的路径（它走 :attr:`primary`），聊天一律走 ``lease``。
        """
        if self._closed:
            raise AcpError("acp runtime pool is closed")
        key = _normalize(model)
        runtime = self._runtimes.get(key)
        if runtime is None:
            runtime = self._factory(key)
            self._runtimes[key] = runtime
        self._sequence += 1
        self._last_used[key] = self._sequence
        return runtime

    def busy(self, model=None) -> int:
        """该模型上正在跑的请求数；0 意味着它随时可以被淘汰。

        占用计数是"淘汰谁"的唯一依据，靠 LRU 顺序去反推它会把用例钉死在当前淘汰
        策略上，所以测试从这里读。状态页**没有**接这个接口（它要的是
        :meth:`status` 那几个列表）——别因为这句注释以为接线已经做了。
        """
        return self._busy.get(_normalize(model), 0)

    @asynccontextmanager
    async def lease(self, model=None):
        """占用该模型的运行时直到本轮结束：占用期间它不会被淘汰。"""
        if self._closed:
            raise AcpError("acp runtime pool is closed")
        if self._logging_out:
            # 登出正在关进程、删凭据。这一刻放一条请求进去，它会把 CLI 重新拉起、
            # 握着还没被删的 token，然后凭据被删掉——命令回复"已退出登录"，而这条
            # 进程还活着，下次刷新会把凭据原样写回来。
            raise AcpError("acp runtime pool is logging out")
        key = _normalize(model)
        runtime = self.get(key)
        self._busy[key] = self._busy.get(key, 0) + 1
        try:
            # 先淘汰再交出去：这样"新起一条"与"收掉一条空闲"在同一轮里完成，
            # 稳态下进程数不会越过上限。
            await self.trim()
            yield runtime
        finally:
            remaining = self._busy.get(key, 1) - 1
            if remaining < 0:
                # 只有 lease 自己写这个计数，减穿只可能是本模块的 bug。静默 pop 会
                # 把它藏成"淘汰策略随机失灵"，当场炸出来反而好定位。带上模型键，
                # 否则看到这条只能靠栈去猜是哪一条。
                self._busy.pop(key, None)
                raise AcpError(
                    "acp runtime pool busy counter went negative for model "
                    "{!r}".format(key)
                )
            if remaining == 0:
                self._busy.pop(key, None)
                # 释放之后再收一次：并发冷启动会让两条进程同时越过限额（谁都不空闲，
                # trim 当时挑不出牺牲者），等它们陆续结束，这里才是真正能收的时候。
                # 没超额时这一步只看一眼列表就返回，不 spawn、不关任何东西。
                with contextlib.suppress(Exception):
                    await self.trim()
            else:
                self._busy[key] = remaining

    # -- 登录 / 登出 --------------------------------------------------------

    async def logout(self, *, purge_config=False) -> None:
        """退出登录：先关掉**全部**进程，再删凭据与配置。

        只关主运行时不够。别的模型进程还活着，手里握着 token，会把凭据原样写回去，
        而命令已经回复用户"已退出登录"——那正是 ``AcpRuntime.logout`` 的注释里要
        根除的那个谎话，只是换了一条进程来犯。

        光"先关再删"也不够：``close_all`` 只调 ``close()``，而 ``close()`` 之后
        ``ensure_started()`` 能把进程再拉起来。登出这两步之间只要进来一条聊天请求，
        它就会拉起一条新进程、握着还没删的 token，然后凭据被删掉。所以整个期间置位
        :attr:`_logging_out`，让 :meth:`lease` 直接拒掉新请求——宁可让这一刻的聊天
        拿到 503，也不要留一条登不掉的进程。

        ``AcpRuntime.logout`` 自己不需要活进程：它拿 ``self._transport``，为 ``None``
        就只删目录。所以 ``close_all`` 之后直接调它是安全的，不会反过来又 spawn 一条。
        """
        self._logging_out = True
        try:
            await self.close_all()
            await self.primary.logout(purge_config=purge_config)
            if purge_config:
                # 登录态是**每条**运行时各记一份的，成功聊过一轮的备用模型会是 True。
                # 只更新主运行时的话，凭据已经删干净、网关也停了，而
                # ``auth_state`` 因为"有一条 True"仍然报告已登录。
                for runtime in list(self._runtimes.values()):
                    runtime.note_logged_out()
        finally:
            self._logging_out = False

    # -- 回收 ---------------------------------------------------------------

    async def trim(self) -> int:
        """把空闲进程收到上限以内，返回真正关掉的条数。

        选"最久未用的空闲进程"淘汰。挑不到（全在忙）就停手：宁可暂时超额，也不动
        正在出字的进程。主运行时永不淘汰。

        **占位算的是"起来的 + 正在被领取的"。** 只数 ``runtime.started`` 会漏掉窗口：
        ``lease`` 里先 ``trim`` 再交给调用方，而进程是调用方稍后
        ``ensure_started()`` 才拉起来的。只数已启动的话，"先领 A 再领 B"两次都能通过
        限额检查，最后两条进程一起活着；释放时又没人收，上限形同虚设。

        返回值只数**关成功**的：``shutdown()`` 抛异常的那条算没关掉，少报一条总比
        报一条假的强。
        """
        victims = []
        async with self._trim_lock:
            while True:
                occupied = [
                    model
                    for model, runtime in self._runtimes.items()
                    if runtime.started or self._busy.get(model)
                ]
                if len(occupied) <= self._max:
                    break
                idle = [
                    model
                    for model in occupied
                    if self._runtimes[model].started
                    and not self._busy.get(model)
                    and model != self._primary_model
                ]
                if not idle:
                    break
                victim = min(idle, key=lambda model: self._last_used.get(model, 0))
                # 先从映射里摘掉再关：这样并发的 ``get`` 不会拿回一条正在关的运行时。
                victims.append(self._runtimes.pop(victim))
                self._busy.pop(victim, None)
                self._last_used.pop(victim, None)
        closed = 0
        for runtime in victims:
            with contextlib.suppress(Exception):
                await runtime.shutdown()
                closed += 1
        return closed

    async def close_all(self) -> None:
        """关掉全部进程，但池仍然可用（下次领取会重新拉起）。

        登录成功后必须调它：登录发生在某一条进程上，其它进程在登录前就启动了，手里
        还记着"未登录"。不重启它们，那些模型会一直报 401，而用户明明已经登上了。

        映射**不**清空：``CliCredentials`` 手里握着某条运行时的引用，清空会让它下次
        刷新时拉起一条池子不认识的进程——那条进程活过插件生命周期，再也没人回收。

        后果要记住：这之后再 ``get(同一个 model)`` 拿回的是**同一条对象**，而它已经
        关了。想用它必须再过一次 ``ensure_started()``，直接 ``stream()`` 会失败。
        """
        async with self._trim_lock:
            runtimes = list(self._runtimes.values())
        for runtime in runtimes:
            with contextlib.suppress(Exception):
                await runtime.close()

    async def shutdown(self) -> None:
        """永久关闭：之后 :meth:`get` 一律抛 :class:`AcpError`。"""
        self._closed = True
        async with self._trim_lock:
            runtimes = list(self._runtimes.values())
            self._runtimes.clear()
            self._last_used.clear()
            self._busy.clear()
        for runtime in runtimes:
            with contextlib.suppress(Exception):
                await runtime.shutdown()


__all__ = ["AcpRuntimePool"]
