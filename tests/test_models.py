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

        self.assertEqual(await catalog.refresh(), [ALPHA, BETA])
        self.assertEqual(catalog.source, "help")
        self.assertIsNotNone(catalog.age)
        self.assertFalse(catalog.stale)

    async def test_a_cli_without_the_marker_falls_back_to_the_config_file(self):
        tmp = _case_dir(self)
        cli = _write_cli(tmp, [], marker=False)
        config = _write_config(tmp, {"models": [{"id": BETA}]})
        catalog = ModelCatalog(str(cli), config_path=str(config), timeout=15)

        self.assertEqual(await catalog.refresh(), [CUSTOM_PREFIX + BETA])
        self.assertEqual(catalog.source, "config")

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
