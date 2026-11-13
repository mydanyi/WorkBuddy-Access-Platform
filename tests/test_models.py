"""模型清单：从本机 CLI 的帮助文本里读，读不到才回退到 ``models.json``。

这一层的要求是"绝不猜"：读不到就是空列表，不用一份写死的模型名兜底。所以这里既验
解析（纯函数，无进程），也验真的 spawn 一个假 CLI 去读——假 CLI 是一个**真的可执行
脚本**，因为要证明的正是"插件自己去跑 ``--help``、自己从输出里抠清单"。Windows 没有
POSIX 可执行位、``create_subprocess_exec`` 还得靠 ``.cmd``，所以涉及进程的用例在那里
跳过（部署目标是 Linux）。

模型 ID 全是虚构样本。
"""

import asyncio
import json
import shutil
import sys
import unittest
from pathlib import Path

_HERE = Path(__file__).resolve()
_PLUGIN_ROOT = _HERE.parents[1]
_SOURCE_ROOT = _HERE.parents[2]
for _path in (str(_PLUGIN_ROOT), str(_SOURCE_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from astrbot_plugin_workbuddy.core.models import (  # noqa: E402
    CUSTOM_PREFIX,
    DEFAULT_CATALOG_AGE,
    DEFAULT_RETRY_AFTER,
    MAX_RETRY_AFTER,
    ModelCatalog,
    parse_config_models,
    parse_help_models,
    strip_model_alias,
)

try:  # 包内发现
    from tests import support as fixtures
except ImportError:  # 顶层发现
    import support as fixtures  # type: ignore

MODULE_TMP = fixtures.make_temp_dir("wb-models-")

ALPHA = "wb-alpha"
BETA = "wb-beta"
GAMMA = "wb-gamma"
IMAGE_MODEL = "wb-image-preview"


# --------------------------------------------------------------------------- #
# parse_help_models
# --------------------------------------------------------------------------- #

class ParseHelpTests(unittest.TestCase):
    def test_reads_the_currently_supported_parens(self):
        text = (
            "Usage: codebuddy [options]\n"
            "  --model <model>  Model to use. Currently supported: ({}, {}, {})\n"
            "  --tools <list>   enable tools\n".format(ALPHA, BETA, GAMMA)
        )
        self.assertEqual(parse_help_models(text), [ALPHA, BETA, GAMMA])

    def test_keeps_the_cli_order_and_drops_duplicates(self):
        text = "Currently supported: ({}, {}, {})".format(BETA, ALPHA, BETA)
        self.assertEqual(parse_help_models(text), [BETA, ALPHA])

    def test_drops_image_models_which_cannot_serve_chat(self):
        text = "Currently supported: ({}, {}, {})".format(ALPHA, IMAGE_MODEL, BETA)
        self.assertEqual(parse_help_models(text), [ALPHA, BETA])

    def test_tolerates_quotes_and_backticks_around_an_id(self):
        text = "Currently supported: (`{}`, \"{}\", '{}')".format(ALPHA, BETA, GAMMA)
        self.assertEqual(parse_help_models(text), [ALPHA, BETA, GAMMA])

    def test_the_list_may_wrap_across_lines(self):
        # 只认 "(" 与它后面第一个 ")"，不按行切——不同版本的 CLI 换行位置不一样。
        text = "  --model <model>\n    Currently supported: (\n      {},\n      {}\n    )\n".format(
            ALPHA, BETA
        )
        self.assertEqual(parse_help_models(text), [ALPHA, BETA])

    def test_a_missing_or_malformed_marker_yields_nothing(self):
        for text in (
            "",
            "--model <model>  Model to use.",
            "Currently supported:",
            "Currently supported: (no closing paren",
            "Currently supported: )",
            None,
            123,
            [],
        ):
            with self.subTest(text=text):
                self.assertEqual(parse_help_models(text), [])

    def test_an_empty_parens_is_an_empty_list(self):
        self.assertEqual(parse_help_models("Currently supported: ()"), [])
        self.assertEqual(parse_help_models("Currently supported: ( , , )"), [])


# --------------------------------------------------------------------------- #
# parse_config_models
# --------------------------------------------------------------------------- #

class ParseConfigTests(unittest.TestCase):
    def test_reads_the_object_shape_and_prefixes_custom_local(self):
        document = {"models": [{"id": ALPHA}, {"id": BETA}]}
        self.assertEqual(
            parse_config_models(document),
            [CUSTOM_PREFIX + ALPHA, CUSTOM_PREFIX + BETA],
        )

    def test_reads_a_bare_list_of_strings_and_objects(self):
        self.assertEqual(
            parse_config_models([ALPHA, {"id": BETA}]),
            [CUSTOM_PREFIX + ALPHA, CUSTOM_PREFIX + BETA],
        )

    def test_an_already_prefixed_id_is_not_prefixed_twice(self):
        document = {"models": [CUSTOM_PREFIX + ALPHA]}
        self.assertEqual(parse_config_models(document), [CUSTOM_PREFIX + ALPHA])

    def test_deduplicates_and_skips_junk_entries(self):
        document = {
            "models": [
                {"id": ALPHA},
                {"id": "  "},
                {"no_id": True},
                {"id": 42},
                None,
                17,
                [ALPHA],
                {"id": "  " + BETA + "  "},
                {"id": ALPHA},
            ]
        }
        self.assertEqual(
            parse_config_models(document),
            [CUSTOM_PREFIX + ALPHA, CUSTOM_PREFIX + BETA],
        )

    def test_drops_image_models(self):
        document = {"models": [{"id": ALPHA}, {"id": IMAGE_MODEL}]}
        self.assertEqual(parse_config_models(document), [CUSTOM_PREFIX + ALPHA])

    def test_drops_a_doubly_prefixed_id_instead_of_keeping_junk(self):
        # ``custom-local:custom-local:a`` 与裸 ``custom-local:`` 都只可能来自脏数据：
        # CLI 不认那个 id，放进候选就会被运维当成"这台机器真有的模型"选进去。
        document = {
            "models": [
                {"id": CUSTOM_PREFIX + CUSTOM_PREFIX + ALPHA},
                {"id": CUSTOM_PREFIX},
                {"id": CUSTOM_PREFIX + "   "},
                {"id": BETA},
            ]
        }
        self.assertEqual(parse_config_models(document), [CUSTOM_PREFIX + BETA])

    def test_normalizes_a_differently_cased_prefix(self):
        # CLI 只认小写的 custom-local:。别的写法同样只有脏数据一个来源，但改写成
        # CLI 认的那个，比整条丢掉对运维更友好，也没有凭空造出模型。
        document = {"models": [{"id": "Custom-Local:" + ALPHA}]}
        self.assertEqual(parse_config_models(document), [CUSTOM_PREFIX + ALPHA])

    def test_a_shape_that_is_not_a_model_list_yields_nothing(self):
        for document in (None, 1, "models", {"models": {}}, {"models": "abc"}, [], {}):
            with self.subTest(document=document):
                self.assertEqual(parse_config_models(document), [])


# --------------------------------------------------------------------------- #
# strip_model_alias
# --------------------------------------------------------------------------- #

class StripAliasTests(unittest.TestCase):
    def test_strips_the_legacy_multiplier_suffix(self):
        for suffix in (
            " [0.11x]",
            " [1x]",
            " [2.5x]",
            " [倍率未知]",
            " [0.11x · 夜间折扣]",
            " [倍率未知 · 试用]",
        ):
            with self.subTest(suffix=suffix):
                self.assertEqual(strip_model_alias(ALPHA + suffix), ALPHA)

    def test_leaves_a_plain_id_untouched(self):
        self.assertEqual(strip_model_alias(ALPHA), ALPHA)
        self.assertEqual(strip_model_alias(CUSTOM_PREFIX + ALPHA), CUSTOM_PREFIX + ALPHA)

    def test_leaves_a_free_form_bracket_suffix_untouched(self):
        # 只有插件旧版拼的那两种后缀才剥；运维自己写的方括号一概不动。
        for value in (
            ALPHA + " [beta]",
            ALPHA + " [0.11]",
            ALPHA + " [0.11x] ",
            ALPHA + "  [0.11x] extra",
            "[" + ALPHA + "]",
        ):
            with self.subTest(value=value):
                self.assertEqual(strip_model_alias(value), value)

    def test_leaves_a_non_string_untouched(self):
        for value in (None, 1, 1.5, True, [], {}):
            with self.subTest(value=value):
                self.assertIs(strip_model_alias(value), value)


# --------------------------------------------------------------------------- #
# ModelCatalog（需要真的 spawn 一个假 CLI）
# --------------------------------------------------------------------------- #

def _case_dir(case) -> Path:
    """给每个用例一个独立目录。

    同一个 ``MODULE_TMP`` 下用固定文件名会让用例互相踩磁盘：谁先写谁赢，并行 runner
    下还会变成假红假绿。目录名取用例名，跑多少次都是干净的一份。
    """
    path = Path(MODULE_TMP) / case.id().rsplit(".", 1)[-1]
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_cli(directory, models, *, name="codebuddy-fake", marker=True, extra=""):
    """写一个只在 ``--help`` 时打印清单的可执行脚本。"""
    if marker:
        line = "  --model <model>  Currently supported: ({})\n".format(", ".join(models))
    else:
        line = "  --model <model>  Model to use.\n"
    path = Path(directory) / name
    path.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        "sys.stdout.write(" + repr(line) + ")\n" + extra,
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def _write_config(directory, document, *, name="models.json"):
    path = Path(directory) / name
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def _counting(directory, *, name="runs"):
    """返回一段脚本尾巴：每跑一次就往 ``name`` 里追加一个字符。

    用来数"到底 spawn 了几次"——比断言耗时可靠，也比偷看私有属性干净。
    """
    return "open({!r}, 'a').write('x')\n".format(str(Path(directory) / name))


def _runs(directory, *, name="runs") -> int:
    path = Path(directory) / name
    return len(path.read_text()) if path.exists() else 0


@unittest.skipIf(
    sys.platform == "win32",
    "ModelCatalog spawns the CLI with create_subprocess_exec; Windows has no "
    "POSIX executable bit and needs a .cmd shim, so the deployed Linux target is "
    "the only place this path can be exercised",
)
class CatalogProcessTests(unittest.IsolatedAsyncioTestCase):
    async def test_reads_the_help_text_and_reports_the_source(self):
        tmp = _case_dir(self)
        cli = _write_cli(tmp, [ALPHA, BETA, IMAGE_MODEL])
        catalog = ModelCatalog(str(cli), timeout=15)
        self.assertEqual(catalog.models, [], "还没读过就是空，绝不预填")
        self.assertEqual(catalog.source, "none")
        self.assertIsNone(catalog.age, "从未读成过就没有年龄")
        self.assertFalse(catalog.stale, "空清单不算过期，算没有")
        self.assertFalse(catalog.authoritative, "没读到过当然不够格当允许列表")

        self.assertEqual(await catalog.refresh(), [ALPHA, BETA])
        self.assertEqual(catalog.source, "help")
        self.assertIsNotNone(catalog.age)
        self.assertFalse(catalog.stale)
        self.assertTrue(catalog.authoritative, "help 读来的才是全集")

    async def test_a_cli_without_the_marker_falls_back_to_the_config_file(self):
        tmp = _case_dir(self)
        cli = _write_cli(tmp, [], marker=False)
        config = _write_config(tmp, {"models": [{"id": BETA}]})
        catalog = ModelCatalog(str(cli), config_path=str(config), timeout=15)

        self.assertEqual(await catalog.refresh(), [CUSTOM_PREFIX + BETA])
        self.assertEqual(catalog.source, "config")
        # 子集：聊天入口若拿它拦人，内置模型会全部 400。
        self.assertFalse(catalog.authoritative)

    async def test_an_over_long_help_entry_does_not_hide_the_config_fallback(self):
        # ``--help`` 只报出一串入口收不下的 ID 时，过滤之后等于"这条来源没货"。
        # 要是先判"有没有读到"、后过滤，来源就定成 help、发出去的是空清单，配置那份
        # 回退再也轮不到——清单从此一直空着，而文件里明明有能用的条目。
        tmp = _case_dir(self)
        cli = _write_cli(tmp, ["wb-" + "x" * 200])
        config = _write_config(tmp, {"models": [{"id": BETA}]})
        catalog = ModelCatalog(str(cli), config_path=str(config), timeout=15)

        self.assertEqual(await catalog.refresh(), [CUSTOM_PREFIX + BETA])
        self.assertEqual(catalog.source, "config")

    async def test_an_id_the_chat_endpoint_would_reject_is_not_published(self):
        # 清单是聊天入口的**广告**。入口对 ``model`` 卡 128 个字符（见
        # ``test_routing_gateway`` 里那条 4096 字符的用例），广告里出现更长的 ID，
        # 客户端照着选，每一次都是 400，而用户看不出为什么。截断更糟：截出来的是
        # 另一个 CLI 根本不认的模型。
        tmp = _case_dir(self)
        cli = _write_cli(tmp, [], marker=False)
        # 发布出去的是**带前缀**的那一整条，所以边界要按全长算。
        at_limit = CUSTOM_PREFIX + "wb-" + "y" * (128 - len(CUSTOM_PREFIX) - 3)
        self.assertEqual(len(at_limit), 128)
        one_over = at_limit + "z"
        config = _write_config(
            tmp,
            {
                "models": [
                    {"id": "wb-" + "x" * 200},
                    {"id": BETA},
                    {"id": one_over},
                    {"id": at_limit},
                ]
            },
        )
        catalog = ModelCatalog(str(cli), config_path=str(config), timeout=15)

        self.assertEqual(
            await catalog.refresh(),
            [CUSTOM_PREFIX + BETA, at_limit],
            "只发布入口收得下的：正好 128 的留着，129 和 203 的丢掉",
        )

    async def test_neither_source_yields_an_empty_list_and_says_so(self):
        tmp = _case_dir(self)
        cli = _write_cli(tmp, [], marker=False, name="codebuddy-empty")
        catalog = ModelCatalog(
            str(cli), config_path=str(tmp / "nope.json"), timeout=15
        )
        self.assertEqual(await catalog.refresh(), [])
        self.assertEqual(catalog.source, "none")
        self.assertEqual(catalog.models, [])

    async def test_a_missing_executable_is_not_an_error(self):
        tmp = _case_dir(self)
        config = _write_config(tmp, {"models": [{"id": GAMMA}]}, name="fallback.json")
        catalog = ModelCatalog(
            str(tmp / "no-such-cli"), config_path=str(config), timeout=15
        )
        self.assertEqual(await catalog.refresh(), [CUSTOM_PREFIX + GAMMA])
        self.assertEqual(catalog.source, "config")

    async def test_a_slow_failed_read_restarts_the_backoff_from_its_end(self):
        # 读一次要吃满 20 秒超时。只在读之前盖章的话，等它失败返回，5 秒退避期早过
        # 了，排在锁上的调用方会一个接一个地再吃满一次——正是退避闸门要堵的事。
        now = [0.0]
        catalog = ModelCatalog(
            "codebuddy", clock=lambda: now[0], max_age=60, retry_after=5
        )
        calls = []

        async def _slow_empty():
            calls.append(1)
            now[0] += 20.0
            return [], "none"

        catalog._discover = _slow_empty

        self.assertEqual(await catalog.refresh(), [])
        now[0] += 1.0  # 距这次失败结束才 1 秒，退避期还没过
        self.assertEqual(await catalog.ensure(), [])
        self.assertEqual(len(calls), 1, "退避期没过就重读了")

    async def test_a_hanging_cli_is_killed_and_the_list_stays_empty(self):
        tmp = _case_dir(self)
        cli = tmp / "codebuddy-slow"
        cli.write_text(
            "#!/usr/bin/env python3\nimport time\ntime.sleep(30)\n", encoding="utf-8"
        )
        cli.chmod(0o755)
        catalog = ModelCatalog(str(cli), timeout=0.3)

        loop = asyncio.get_running_loop()
        started = loop.time()
        self.assertEqual(await catalog.refresh(), [])
        # 假 CLI 睡 30 秒。上限给 2 秒：0.3 秒超时之后必须立刻收工，慢于这个数就
        # 说明"没真掐掉、只是没等它"——那种回归在 10 秒上限下也能混过去。
        self.assertLess(loop.time() - started, 2.0, "超时必须真的把它掐掉，不能干等")

    async def test_cancelling_a_hanging_read_still_reaps_the_child(self):
        tmp = _case_dir(self)
        cli = tmp / "codebuddy-forever"
        cli.write_text(
            "#!/usr/bin/env python3\nimport time\ntime.sleep(30)\n", encoding="utf-8"
        )
        cli.chmod(0o755)
        catalog = ModelCatalog(str(cli), timeout=15)

        task = asyncio.create_task(catalog.refresh())
        await asyncio.sleep(0.2)          # 让它真的把子进程拉起来
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        # 清单没变，来源没变：取消不是"读到了空清单"。
        self.assertEqual(catalog.models, [])
        self.assertEqual(catalog.source, "none")

    async def test_a_failed_read_keeps_the_last_good_list(self):
        tmp = _case_dir(self)
        clock = [0.0]
        cli = _write_cli(tmp, [ALPHA, BETA], name="codebuddy-flaky")
        catalog = ModelCatalog(str(cli), timeout=15, clock=lambda: clock[0])
        self.assertEqual(await catalog.refresh(), [ALPHA, BETA])
        self.assertEqual(catalog.source, "help")

        # 这台机器上的 CLI 突然读不出清单了：旧清单必须留着，不能一夜之间变空。
        _write_cli(tmp, [], marker=False, name="codebuddy-flaky")
        clock[0] = 5.0
        self.assertEqual(await catalog.refresh(), [ALPHA, BETA])
        self.assertEqual(catalog.source, "help", "读失败不改来源")

        # 但年龄照走：清单没变，而"多久以前读到的"变了。状态页靠这个说清"这是旧数据"。
        self.assertEqual(catalog.age, 5.0)
        clock[0] = 300.0
        self.assertTrue(catalog.stale, "过了保鲜期的旧清单必须自报过期")

    async def test_a_partial_config_fallback_does_not_replace_a_full_catalog(self):
        # 上一版的坑：``--help`` 失败时回退到 ``models.json``，那份**必然更小**、且
        # 全是 ``custom-local:`` 前缀。拿它顶掉手里的完整清单，等于把这台机器"认哪些
        # id"悄悄缩成运维手写的那几个：内置模型从候选里消失，聊天入口的允许列表跟着
        # 缩，一个本来好好的请求就变成 400——而它的进程还好好的。
        tmp = _case_dir(self)
        marker = tmp / "probed-once"
        good = "  --model <model>  Currently supported: ({}, {})\n".format(ALPHA, BETA)
        cli = _write_cli(
            tmp,
            [],
            name="codebuddy-half-dead",
            marker=False,
            extra=(
                "import os\n"
                "marker = " + repr(str(marker)) + "\n"
                "if not os.path.exists(marker):\n"
                "    open(marker, 'w').write('x')\n"
                "    sys.stdout.write(" + repr(good) + ")\n"
            ),
        )
        config = _write_config(tmp, {"models": [{"id": GAMMA}]})
        catalog = ModelCatalog(str(cli), config_path=str(config), timeout=15)

        self.assertEqual(await catalog.refresh(), [ALPHA, BETA])
        self.assertEqual(catalog.source, "help")
        self.assertTrue(catalog.authoritative, "help 读来的才算权威清单")
        read_at = catalog._read_at
        # 第二次 ``--help`` 不再报清单，只剩配置里那一条——不许顶替，但也不白读：
        # 并进清单，老的一条都不能少。
        self.assertEqual(
            await catalog.refresh(),
            [ALPHA, BETA, CUSTOM_PREFIX + GAMMA],
            "子集不许顶掉完整清单",
        )
        self.assertEqual(catalog.source, "help", "来源也不能被改写成 config")
        self.assertTrue(catalog.authoritative, "并集之后照样是权威的")
        self.assertEqual(catalog._read_at, read_at, "help 那部分没刷新，年龄就该照走")

        # 反过来：手里本来就没有全集时，配置那份回退照收——那正是它存在的意义。
        fresh = ModelCatalog(str(cli), config_path=str(config), timeout=15)
        self.assertEqual(await fresh.refresh(), [CUSTOM_PREFIX + GAMMA])
        self.assertEqual(fresh.source, "config")
        self.assertFalse(fresh.authoritative, "只有配置清单，当不了允许列表")

    async def test_refresh_really_re_reads_after_the_cli_changes(self):
        tmp = _case_dir(self)
        cli = _write_cli(tmp, [ALPHA, BETA], name="codebuddy-upgrade")
        catalog = ModelCatalog(str(cli), timeout=15)
        self.assertEqual(await catalog.refresh(), [ALPHA, BETA])

        _write_cli(tmp, [ALPHA, BETA, GAMMA], name="codebuddy-upgrade")
        self.assertEqual(await catalog.refresh(), [ALPHA, BETA, GAMMA])

    async def test_ensure_only_re_reads_once_the_list_goes_stale(self):
        tmp = _case_dir(self)
        cli = _write_cli(tmp, [ALPHA], name="codebuddy-fresh")
        clock = [0.0]
        catalog = ModelCatalog(
            str(cli), timeout=15, clock=lambda: clock[0], max_age=60
        )

        self.assertEqual(await catalog.ensure(), [ALPHA])
        # 保鲜期内不重读：/v1/models 会被客户端轮询，每次都 spawn 一个进程等于
        # 把插件变成进程制造机。
        _write_cli(tmp, [ALPHA, BETA], name="codebuddy-fresh")
        self.assertEqual(await catalog.ensure(), [ALPHA])

        clock[0] = 60.5
        self.assertEqual(await catalog.ensure(), [ALPHA, BETA], "过期了就该重读")

    async def test_a_failed_read_is_not_retried_on_every_poll(self):
        # 只按"上次成功"计时的话，CLI 被卸载/卡死时每次轮询都会再 spawn 一个进程、
        # 再吃满一次超时。这里用假 CLI 自己数的运行次数来钉住这件事。
        tmp = _case_dir(self)
        clock = [0.0]
        cli = _write_cli(
            tmp, [], marker=False, name="codebuddy-dead", extra=_counting(tmp)
        )
        catalog = ModelCatalog(
            str(cli),
            timeout=15,
            clock=lambda: clock[0],
            max_age=60,
            retry_after=5,
        )

        self.assertEqual(await catalog.ensure(), [])
        self.assertEqual(_runs(tmp), 1)

        clock[0] = 1.0
        self.assertEqual(await catalog.ensure(), [], "刚失败过，别又 spawn 一个")
        self.assertEqual(_runs(tmp), 1, "重试间隔内不许再拉进程")

        clock[0] = 6.0
        await catalog.ensure()
        self.assertEqual(_runs(tmp), 2, "过了重试间隔就该再试一次")

    async def test_repeated_failures_back_off_further_each_time(self):
        # 固定 5 秒退避在"CLI 永久坏掉"面前等于没有：每 5 秒一次、每次吃满 20 秒
        # 超时，也就是每 25 秒 spawn 一个注定失败的进程，永不停止。连败要越退越远。
        tmp = _case_dir(self)
        clock = [0.0]
        cli = _write_cli(
            tmp, [], marker=False, name="codebuddy-dead", extra=_counting(tmp)
        )
        catalog = ModelCatalog(
            str(cli), timeout=15, clock=lambda: clock[0], max_age=60, retry_after=5
        )

        self.assertEqual(await catalog.ensure(), [])
        self.assertEqual(_runs(tmp), 1)
        clock[0] = 6.0  # 第一次退避 5 秒，过了
        await catalog.ensure()
        self.assertEqual(_runs(tmp), 2)
        self.assertEqual(catalog._failures, 2, "两次失败都记上")

        clock[0] = 15.0  # 距上次失败 9 秒：第二次退避已经是 10 秒，还不够
        await catalog.ensure()
        self.assertEqual(_runs(tmp), 2, "连败之后退避要翻倍")

        clock[0] = 17.0
        await catalog.ensure()
        self.assertEqual(_runs(tmp), 3, "过了 10 秒就该再试")

        # 封顶：再怎么连败也不会退到"永远不重试"。
        catalog._failures = 40
        self.assertEqual(catalog.retry_wait, MAX_RETRY_AFTER, "退避有上限")

        # 一旦读成，连败清零、退避回到最短。
        _write_cli(tmp, [ALPHA], name="codebuddy-dead")
        clock[0] = 17.0 + MAX_RETRY_AFTER
        self.assertEqual(await catalog.ensure(), [ALPHA])
        self.assertEqual(catalog._failures, 0, "读成了就把连败清零")
        self.assertEqual(catalog.retry_wait, 5.0)

    async def test_a_poll_does_not_queue_behind_an_in_flight_read(self):
        # 一轮读吃满 20 秒超时并全程持锁。help 坏掉时 ``stale`` 永真，每个调用方都进
        # 不了 fast-path，于是一起在锁上排队——而其中不少是消息路径上"这个模型认不
        # 认"的校验。手里有旧清单就先给它，别让它白等半个超时窗。
        tmp = _case_dir(self)
        clock = [120.0]
        cli = _write_cli(tmp, [ALPHA], name="codebuddy-slow")
        catalog = ModelCatalog(
            str(cli), timeout=15, clock=lambda: clock[0], max_age=60, retry_after=0
        )
        catalog._models = [ALPHA]
        catalog._source = "help"
        catalog._read_at = 0.0          # 已经过期

        started = asyncio.Event()
        gate = asyncio.Event()

        async def _blocked_read():
            started.set()
            await gate.wait()
            return [], "none"

        catalog._discover = _blocked_read
        first = asyncio.create_task(catalog.ensure())
        await asyncio.wait_for(started.wait(), 5.0)
        try:
            # 读还卡着。这一次必须立刻拿到旧清单，而不是排在锁上。
            self.assertEqual(
                await asyncio.wait_for(catalog.ensure(), 2.0),
                [ALPHA],
                "手里有旧清单就别排队",
            )
        finally:
            gate.set()
            await asyncio.wait_for(first, 5.0)

        # 空清单不这么办：那说明还没读成过，该等那唯一的一次真读。
        cold = ModelCatalog("codebuddy", timeout=15)
        started.clear()
        gate.clear()

        async def _blocked_again():
            started.set()
            await gate.wait()
            return [BETA], "help"

        cold._discover = _blocked_again
        holder = asyncio.create_task(cold.ensure())
        await asyncio.wait_for(started.wait(), 5.0)
        self.assertTrue(cold._refresh_lock.locked(), "这条用例的前提是锁真的被占着")
        gate.set()
        self.assertEqual(await asyncio.wait_for(holder, 5.0), [BETA])
        self.assertEqual(await asyncio.wait_for(cold.ensure(), 5.0), [BETA])

    async def test_a_config_fallback_does_not_reset_the_backoff(self):
        # "``--help`` 永久坏掉 + ``models.json`` 有货"：每趟都靠配置清单兜底。要是那
        # 算成功、连败清零，退避就一次都不会升级——节奏从"每 25 秒"变成"每 60 秒"，
        # 照样是一个注定失败的进程接着一个。
        tmp = _case_dir(self)
        clock = [0.0]
        cli = _write_cli(
            tmp, [], marker=False, name="codebuddy-dead", extra=_counting(tmp)
        )
        config = _write_config(tmp, {"models": [{"id": GAMMA}]})
        catalog = ModelCatalog(
            str(cli),
            config_path=str(config),
            timeout=15,
            clock=lambda: clock[0],
            max_age=60,
            retry_after=5,
        )

        self.assertEqual(await catalog.ensure(), [CUSTOM_PREFIX + GAMMA])
        self.assertEqual(_runs(tmp), 1)
        self.assertEqual(catalog._failures, 1, "配置兜底不算 help 读成")

        clock[0] = 61.0  # 过了保鲜期，再读一趟
        await catalog.ensure()
        self.assertEqual(_runs(tmp), 2)
        self.assertEqual(catalog._failures, 2)
        self.assertEqual(catalog.retry_wait, 10.0, "连败要升级")

        clock[0] = 66.0  # 距上次失败 5 秒：升级后是 10 秒，还不够
        await catalog.ensure()
        self.assertEqual(_runs(tmp), 2, "配置兜底不许把退避打回最短")

    async def test_a_gateway_removed_from_the_config_disappears(self):
        # 并集只能加不能减。运维从 ``models.json`` 里删掉一个网关，在 help 恢复之前
        # 它会一直挂在清单上（也就一直挂在聊天入口的允许列表里）。两个来源分开记账
        # 之后，config 那份整体换新，删除照样生效。
        tmp = _case_dir(self)
        marker = tmp / "probed-once"
        good = "  --model <model>  Currently supported: ({})\n".format(ALPHA)
        cli = _write_cli(
            tmp,
            [],
            name="codebuddy-half-dead",
            marker=False,
            extra=(
                "import os\n"
                "marker = " + repr(str(marker)) + "\n"
                "if not os.path.exists(marker):\n"
                "    open(marker, 'w').write('x')\n"
                "    sys.stdout.write(" + repr(good) + ")\n"
            ),
        )
        config = _write_config(tmp, {"models": [{"id": GAMMA}]})
        catalog = ModelCatalog(str(cli), config_path=str(config), timeout=15)

        self.assertEqual(await catalog.refresh(), [ALPHA])
        self.assertEqual(await catalog.refresh(), [ALPHA, CUSTOM_PREFIX + GAMMA])

        _write_config(tmp, {"models": [{"id": BETA}]})
        self.assertEqual(
            await catalog.refresh(),
            [ALPHA, CUSTOM_PREFIX + BETA],
            "删掉的那条必须真的消失",
        )

    async def test_an_explicit_refresh_waits_for_the_read_in_flight(self):
        # ``/wb models`` 直调 ``refresh``。锁只由 ``ensure`` 拿的话，help 卡死那 20 秒
        # 里管理员再敲一次就并发出第二个 CLI 进程——持锁要防的正是这个。
        catalog = ModelCatalog("codebuddy", timeout=15)
        started = asyncio.Event()
        gate = asyncio.Event()
        calls = []

        async def _blocked_read():
            calls.append(1)
            started.set()
            await gate.wait()
            return [ALPHA], "help"

        catalog._discover = _blocked_read
        first = asyncio.create_task(catalog.refresh())
        await asyncio.wait_for(started.wait(), 5.0)
        second = asyncio.create_task(catalog.refresh())
        for _ in range(20):
            await asyncio.sleep(0.01)
        self.assertEqual(len(calls), 1, "别人正读着的时候不许再拉一个进程")

        gate.set()
        self.assertEqual(await asyncio.wait_for(first, 5.0), [ALPHA])
        self.assertEqual(await asyncio.wait_for(second, 5.0), [ALPHA])

    async def test_a_cancelled_read_still_counts_as_an_attempt(self):
        # 读到一半被取消时，"尝试时刻"必须在读之前就盖章。否则一次取消就让退避
        # 闸门失效，之后每次轮询都重新 spawn、重新吃满一次超时。
        tmp = _case_dir(self)
        clock = [0.0]
        cli = _write_cli(
            tmp,
            [],
            marker=False,
            name="codebuddy-forever",
            extra=_counting(tmp) + "import time\ntime.sleep(30)\n",
        )
        catalog = ModelCatalog(
            str(cli),
            timeout=15,
            clock=lambda: clock[0],
            max_age=60,
            retry_after=5,
        )

        task = asyncio.create_task(catalog.ensure())
        for _ in range(200):  # 等假 CLI 真的跑起来（它一起来就先记一笔）
            if _runs(tmp):
                break
            await asyncio.sleep(0.01)
        self.assertEqual(_runs(tmp), 1, "假 CLI 没起来，这条用例没验到东西")

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        clock[0] = 1.0
        self.assertEqual(await catalog.ensure(), [])
        self.assertEqual(_runs(tmp), 1, "取消也算一次尝试，退避闸门必须生效")
        self.assertEqual(catalog.retry_wait, 5.0, "一次失败还是基础宽度")

        # 再被早取消一次：退避必须跟着升级，否则反复早取消的调用方能让它停在最短
        # 那一档——比修前还密。
        clock[0] = 6.0
        second = asyncio.create_task(catalog.ensure())
        for _ in range(200):
            if _runs(tmp) == 2:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(_runs(tmp), 2, "假 CLI 没起来，这条用例没验到东西")
        second.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await second
        self.assertEqual(catalog._failures, 2, "取消也要计数")
        self.assertEqual(catalog.retry_wait, 10.0, "连着被取消，退避要升级")

    async def test_concurrent_polls_spawn_only_one_cli(self):
        # 两个并发的 /v1/models 都判断成"过期了"，会各拉一个 CLI、各吃满一次超时。
        tmp = _case_dir(self)
        clock = [0.0]
        cli = _write_cli(
            tmp, [ALPHA], name="codebuddy-concurrent", extra=_counting(tmp)
        )
        catalog = ModelCatalog(
            str(cli), timeout=15, clock=lambda: clock[0], max_age=60
        )

        self.assertEqual(await catalog.ensure(), [ALPHA])
        self.assertEqual(_runs(tmp), 1)

        clock[0] = 120.0  # 过期了，两个请求同时来
        results = await asyncio.gather(catalog.ensure(), catalog.ensure())
        self.assertEqual(results, [[ALPHA], [ALPHA]])
        self.assertEqual(_runs(tmp), 2, "第二次轮询要复用第一次的结果，不能再拉一个")

    async def test_ensure_reads_immediately_when_nothing_was_ever_read(self):
        tmp = _case_dir(self)
        cli = _write_cli(tmp, [ALPHA], name="codebuddy-cold")
        catalog = ModelCatalog(str(cli), timeout=15, max_age=60)
        self.assertEqual(await catalog.ensure(), [ALPHA])


class CatalogDefaultsTests(unittest.TestCase):
    def test_the_documented_freshness_window(self):
        self.assertEqual(DEFAULT_CATALOG_AGE, 60.0)

    def test_the_documented_retry_window(self):
        self.assertEqual(DEFAULT_RETRY_AFTER, 5.0)

    def test_a_retry_window_longer_than_the_freshness_window_is_clamped(self):
        # 否则会出现"清单过期了，却因为重试间隔太长而总也不重读"。
        catalog = ModelCatalog("", max_age=3, retry_after=600)
        self.assertEqual(catalog._retry_after, 3)

    def test_the_custom_prefix_is_the_one_the_cli_accepts(self):
        self.assertEqual(CUSTOM_PREFIX, "custom-local:")

    def test_a_catalog_without_a_cli_never_spawns_anything(self):
        catalog = ModelCatalog("")
        self.assertEqual(catalog.models, [])
        self.assertEqual(catalog.source, "none")


def tearDownModule() -> None:
    """只删本模块自己那个临时根目录，别碰别人的沙箱。"""
    shutil.rmtree(MODULE_TMP, ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
