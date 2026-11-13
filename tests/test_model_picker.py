"""Real-AstrBot regression tests for the WorkBuddy model picker.

Goal: ``routing.models`` must stay a **native AstrBot list+options multiselect**
whose candidates come from **this machine's CLI** -- no login, no network, no
account API.  The plugin runs inside the real AstrBot framework with the real
``AstrBotConfig`` built from the plugin's real ``_conf_schema.json``, the real
``WorkBuddyPlugin`` / ``ModelCatalog`` / ``ModelRouter`` / ``load_settings``.

How the local CLI is faked: ``ModelCatalog`` shells out to ``<cli_path> --help``
and reads the ``--model ... Currently supported: (...)`` line.  So the fixture is
a real, tiny, executable script in the test sandbox that prints exactly that
line.  Nothing inside the plugin is replaced -- the whole "read the list off this
machine" path really runs.  Platforms without a POSIX executable bit cannot host
such a script, so the whole module skips there (the deployed target is Linux).

What changed with the ACP migration, and why the old sections are gone:

* the candidate list used to be fetched from ``/console/enterprises/personal/
  models`` with the account's token and carried credit multipliers in the labels.
  Both the endpoint and the multipliers are gone; labels are now plain ids.
* the periodic re-read is back, on the local source: ``model_refresh`` switches a
  timer that re-reads ``codebuddy --help`` and republishes the candidates.  It
  never touches the network, so there is no quota, no multiplier and no account
  involved -- the sections at the bottom cover the timer itself (one task only,
  off when switched off, reaped before the catalog is dropped).

Every model id and token below is a FICTIONAL FIXED SAMPLE.
"""

import asyncio
import contextlib
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path

# ---------------------------------------------------------------------------
# ``support`` pins ASTRBOT_ROOT / TMPDIR / WORKBUDDY_TEST_TMP before astrbot is
# imported, so it must be imported first.  Both discovery modes are supported.
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve()
_PLUGIN_ROOT = _HERE.parents[1]          # .../astrbot_plugin_workbuddy
_TESTS_DIR = _HERE.parent                # .../astrbot_plugin_workbuddy/tests
_SOURCE_ROOT = _HERE.parents[2]          # directory containing the plugin pkg
for _path in (str(_PLUGIN_ROOT), str(_TESTS_DIR), str(_SOURCE_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

try:  # package discovery (``tests`` is a real package)
    import tests.test_plugin_integration as support
except ImportError:  # top-level discovery
    import test_plugin_integration as support  # type: ignore

#: 底层夹具（临时目录、假上游、脚本化后端）。``support`` 是"框架沙箱 + 插件装配"
#: 那一层，它自己也是从这里拿夹具的，所以这里直接用同一份源头，不隔着它转手。
try:
    from tests import support as fixtures
except ImportError:  # top-level discovery
    import support as fixtures  # type: ignore

# --- Real framework / plugin imports (sandbox already pinned by ``support``). ---
import astrbot_plugin_workbuddy.main as wb_main  # noqa: E402
from astrbot_plugin_workbuddy.core.acp import default_argv  # noqa: E402
from astrbot_plugin_workbuddy.core.config import ConfigError, load_settings  # noqa: E402
from astrbot_plugin_workbuddy.core.models import strip_model_alias  # noqa: E402
from astrbot_plugin_workbuddy.core.routing import ORDERED_MODEL_ID  # noqa: E402

# --- FICTIONAL FIXED SAMPLE model ids (never production ids). ---
ALPHA = "wb-alpha"
BETA = "wb-beta"
GAMMA = "wb-gamma"
DELTA = "wb-delta"
CUSTOM = "wb-self-hosted"

#: ``--model`` 清单里出现图片模型时，它不能被选进聊天候选。
IMAGE_MODEL = "wb-image-preview"

MODULE_TMP = fixtures.make_temp_dir("wb-picker-")


def _write_fake_cli(help_models) -> Path:
    """写一个只会打印 ``--help`` 的假 CLI；返回可执行路径。

    它替的是"这台机器上装好的 codebuddy"：真正的证据是插件自己 spawn 它、自己
    从输出里抠出模型清单。因此这个脚本必须是真可执行的，不能是 mock。
    """
    path = Path(MODULE_TMP) / "codebuddy-fake"
    line = "  --model <model>  Currently supported: ({})\n".format(
        ", ".join(help_models)
    )
    path.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        "sys.stdout.write(" + repr(line) + ")\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


@unittest.skipIf(
    sys.platform == "win32",
    "the local CLI is invoked with create_subprocess_exec; Windows needs a "
    "codebuddy.cmd and has no POSIX executable bit, so this path cannot be "
    "exercised here (the deployed target is Linux)",
)
class _ModelPickerTestCase(unittest.IsolatedAsyncioTestCase):
    """Drives the real plugin; the shared fixtures are bound, not inherited."""

    async def asyncSetUp(self) -> None:
        await support._PluginIntegrationTestCase.asyncSetUp(self)
        #: 这台"机器"上 CLI 报的模型清单。子类/用例可以改它再重建插件。
        self.help_models = [ALPHA, BETA, GAMMA, IMAGE_MODEL]
        self.cli_path = _write_fake_cli(self.help_models)

    async def asyncTearDown(self) -> None:
        await support._PluginIntegrationTestCase.asyncTearDown(self)

    def _rebuild_cli(self, help_models) -> None:
        """换一份 CLI 清单并重建那个可执行文件。"""
        self.help_models = list(help_models)
        self.cli_path = _write_fake_cli(self.help_models)

    # -- bound support fixtures --------------------------------------------
    def _build_config(self, config_path, **overrides):
        return support._PluginIntegrationTestCase._build_config(
            self, config_path, **overrides
        )

    def _ensure_metadata_name(self):
        return support._PluginIntegrationTestCase._ensure_metadata_name(self)

    def _seed_credentials(self):
        return support._PluginIntegrationTestCase._seed_credentials(self)

    # -- config / plugin factories -----------------------------------------
    def _picker_config(self, **overrides):
        base = {
            "gateway": {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 0,
                "api_key": support.GATEWAY_KEY,
                # Fixed, non-default values the model work must never disturb.
                "concurrency": 3,
                "queue_size": 10,
            },
            # No background producer: this module is about the picker only.
            "signin": {"enabled": False},
            # 定时重读默认关掉：绝大多数用例只关心"候选是什么"，让一条 60 分钟计时
            # 在背后跑着毫无意义。定时那几项（底部）自己打开它。
            "model_refresh": {"enabled": False},
            # 本机 CLI 用那个假可执行文件；别的一律不拉起。
            "acp": {"cli_path": str(self.cli_path)},
            "routing": {
                "enabled": True,
                "models": [ALPHA, GAMMA],
                "cooldown_seconds": 60,
                "first_frame_timeout": 30,
            },
        }
        return support._deep_merge(base, overrides)

    async def _make_plugin(self, *, initialize=True, **overrides):
        """Build the real plugin on a real ``AstrBotConfig``; keep both objects."""
        cfg_path = support._SESSION_ROOT / ("cfg-" + uuid.uuid4().hex + ".json")
        config = self._build_config(cfg_path, **overrides)
        context = support._Context(config)
        self._ensure_metadata_name()
        plugin = wb_main.WorkBuddyPlugin(context, config)
        self._plugins.append(plugin)
        if initialize:
            await plugin.initialize()
        return plugin, config, cfg_path

    # -- schema readers (the values the config service really delivers) -----
    def _schema_models(self, config):
        schema = getattr(config, "schema", None)
        self.assertIsInstance(
            schema,
            dict,
            "the real AstrBotConfig must expose the delivered schema",
        )
        section = schema.get("routing")
        self.assertIsInstance(section, dict, "routing section missing from schema")
        items = section.get("items")
        self.assertIsInstance(items, dict, "routing.items missing from schema")
        field = items.get("models")
        self.assertIsInstance(field, dict, "routing.models field missing from schema")
        return field

    def _options(self, config):
        field = self._schema_models(config)
        options = field.get("options")
        self.assertIsInstance(
            options,
            list,
            "routing.models must be served as a native list+options multiselect, "
            "not a free-text field: {!r}".format(field),
        )
        return list(options)

    def _labels(self, config):
        field = self._schema_models(config)
        labels = field.get("labels")
        self.assertIsInstance(
            labels,
            list,
            "routing.models options must carry parallel labels: {!r}".format(field),
        )
        return list(labels)

    def _options_map(self, config):
        options = self._options(config)
        labels = self._labels(config)
        self.assertEqual(
            len(labels), len(options), "labels must be parallel to the option values"
        )
        for value, label in zip(options, labels):
            self.assertIsInstance(value, str)
            self.assertIsInstance(label, str)
            self.assertTrue(
                label.startswith(value),
                "label {!r} does not name its option {!r}".format(label, value),
            )
        self.assertEqual(
            len(set(options)), len(options), "option values must be unique"
        )
        return dict(zip(options, labels))

    # -- helpers -----------------------------------------------------------
    def _export_picker_fixture(self, config) -> None:
        """Optionally write the config-service payload for the real-SPA harness.

        Only when ``WORKBUDDY_PICKER_EVIDENCE`` names the manager's evidence
        directory; nothing is written otherwise.  The payload is the *delivered*
        ``routing`` schema metadata (with the real, freshly read ``options`` /
        ``labels``) plus a copy of this test's fictional routing config whose
        ``models`` list is empty, so a separate harness can drive the deployed
        frontend through a pure-ID selection / save / reopen.  No production
        config, credential or data file is read or written here.
        """
        target = os.environ.get("WORKBUDDY_PICKER_EVIDENCE")
        if not target:
            return
        schema = getattr(config, "schema", None)
        self.assertIsInstance(schema, dict, "no delivered schema to export")
        routing_schema = schema.get("routing")
        self.assertIsInstance(routing_schema, dict, "routing schema missing")
        routing_config = json.loads(json.dumps(self._picker_config()["routing"]))
        routing_config["models"] = []
        payload = {
            "plugin_name": support.PLUGIN_NAME,
            "log_level": None,
            "i18n": {},
            "metadata": {
                support.PLUGIN_NAME: {
                    "type": "object",
                    "items": {"routing": routing_schema},
                }
            },
            "config": {"routing": routing_config},
        }
        directory = Path(target)
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "picker-api-fixture.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    # =====================================================================
    # 1. Startup delivers this machine's CLI list as the native option values
    #    with plain-id labels -- never the virtual ordered entry, never an
    #    image model -- and it needs no credentials and no network.
    # =====================================================================
    async def test_startup_delivers_the_local_cli_list_as_plain_ids(self):
        plugin, config, _ = await self._make_plugin(**self._picker_config())

        self.assertTrue(plugin.ready, plugin.error_reason)
        self.assertEqual(plugin.error_reason, "")
        # 清单一事与账号无关：没登录也要读得到，而且一次 HTTP 都不发。
        self.assertFalse(plugin.account.credentials)
        self.assertEqual(
            self.up.total_hits(), 0, "reading the local list must not go online"
        )
        self.assertEqual(plugin.catalog.source, "help")

        field = self._schema_models(config)
        self.assertEqual(
            field.get("type"),
            "list",
            "routing.models must stay the native list type the options branch "
            "belongs to",
        )

        options = self._options(config)
        # 图片模型不进候选：它不接 chat completions，选进来只会在真发 prompt 时失败。
        self.assertEqual(set(options), {ALPHA, BETA, GAMMA})
        self.assertNotIn(IMAGE_MODEL, options)
        self.assertNotIn(ORDERED_MODEL_ID, options)
        # 发布顺序是"运维存下来的先排"，其余按清单补在后面——这样配置页打开时，
        # 已经选中的那一串不会因为清单顺序变了就看起来像被重排过。
        self.assertEqual(options[:2], [ALPHA, GAMMA])

        # 标签就是 id 本身：倍率下线后不再有 " [0.11x]" 这种后缀。
        self.assertEqual(self._labels(config), options)
        for option in options:
            self.assertEqual(strip_model_alias(option), option)

        # 读一次清单不会改写运维存下来的选择。
        self.assertEqual(config["routing"]["models"], [ALPHA, GAMMA])

        self._export_picker_fixture(config)

    # =====================================================================
    # 2. A CLI that cannot be read leaves the plugin ready, offers the saved
    #    values as-is, and invents nothing.
    # =====================================================================
    async def test_unreadable_cli_keeps_plugin_ready_and_saved_selection(self):
        saved = [GAMMA, BETA]
        self._rebuild_cli([])  # 输不出那一行 = 读不到清单
        plugin, config, cfg_path = await self._make_plugin(
            **self._picker_config(routing={"models": list(saved)})
        )

        self.assertTrue(plugin.ready, "a missing model list must not disable the plugin")
        self.assertEqual(plugin.error_reason, "")
        self.assertEqual(plugin.catalog.source, "none")
        self.assertEqual(self.up.total_hits(), 0)

        # 清单是空的：已经选过的值必须还在候选里，否则配置页一打开就像被清空了。
        self.assertEqual(self._options(config), saved)
        self.assertEqual(self._labels(config), saved)
        self.assertNotIn(ORDERED_MODEL_ID, self._options(config))

        on_disk = json.loads(Path(cfg_path).read_text(encoding="utf-8-sig"))
        self.assertEqual(on_disk["routing"]["models"], saved)
        self.assertEqual(load_settings(config)["routing"]["models"], saved)
        self.assertEqual(plugin.router.status()["models"], saved)

        # 与模型无关的网关设置一个字都没动。
        self.assertEqual(config["gateway"]["concurrency"], 3)
        self.assertEqual(config["gateway"]["queue_size"], 10)
        self.assertEqual(plugin._settings["gateway"]["concurrency"], 3)
        self.assertEqual(plugin._settings["gateway"]["queue_size"], 10)

    # =====================================================================
    # 3. A model that the operator saved but this machine no longer offers is
    #    kept and marked unconfirmed -- never dropped, never shown as available.
    # =====================================================================
    async def test_saved_model_missing_from_the_list_is_kept_and_marked(self):
        saved = [DELTA, ALPHA]
        plugin, config, _ = await self._make_plugin(
            **self._picker_config(routing={"models": list(saved)})
        )
        self.assertTrue(plugin.ready, plugin.error_reason)

        options = self._options(config)
        labels = dict(zip(options, self._labels(config)))
        self.assertIn(DELTA, options, "a saved-but-unknown value must never be dropped")
        self.assertIn("当前目录无此模型", labels[DELTA])
        # 清单里真有的那个不标记。
        self.assertEqual(labels[ALPHA], ALPHA)
        # 保存的数组本身不动。
        self.assertEqual(config["routing"]["models"], saved)

    # =====================================================================
    # 4. ``/wb models`` re-reads the local list, and the picker follows -- the
    #    only refresh trigger that exists now that the periodic loop is gone.
    # =====================================================================
    async def test_commands_refresh_the_local_catalog(self):
        saved = [ALPHA, GAMMA]
        plugin, config, _ = await self._make_plugin(
            **self._picker_config(routing={"models": list(saved)})
        )
        self.assertTrue(plugin.ready, plugin.error_reason)
        self.assertEqual(set(self._options(config)), {ALPHA, BETA, GAMMA})

        # 这台机器上换了 CLI 版本：GAMMA 没了，DELTA 和一张图片出现了。
        self._rebuild_cli([ALPHA, BETA, DELTA, IMAGE_MODEL])

        reply = support._text_of(
            await support._invoke(
                plugin.wb, support._make_event("wb models"), "models", ""
            )
        )
        # 命令回的是同一份清单，带来源说明，不联网。
        self.assertIn("CLI 帮助文本", reply)
        for model in (ALPHA, BETA, DELTA):
            self.assertIn(model, reply)
        self.assertNotIn(GAMMA, reply)
        self.assertNotIn(IMAGE_MODEL, reply)
        self.assertIn(ORDERED_MODEL_ID, reply)  # 路由开着，虚拟入口也列出来
        self.assertEqual(self.up.total_hits(), 0)

        # 配置页的候选也换成了新清单：命令说的和配置页看到的必须是同一份。
        mapping = self._options_map(config)
        self.assertEqual(set(mapping), {ALPHA, BETA, DELTA, GAMMA})
        # 已选但新清单里没有的 GAMMA 保留并标记为未确认，不悄悄丢掉。
        self.assertIn("当前目录无此模型", mapping[GAMMA])
        self.assertEqual(mapping[DELTA], DELTA)
        self.assertEqual(mapping[ALPHA], ALPHA)
        # 运维的选择与路由顺序都不受影响。
        self.assertEqual(config["routing"]["models"], saved)
        self.assertEqual(plugin.router.status()["models"], saved)

    # =====================================================================
    # 4b. 清单读失败时旧值会被保留，所以命令必须自己说清"这份清单有多旧"。
    #     少了这一句，状态页会在清单早已过期时仍然宣称"来源：CLI 帮助文本"。
    # =====================================================================
    async def test_a_stale_catalog_says_so_instead_of_claiming_freshness(self):
        plugin, _config, _ = await self._make_plugin(**self._picker_config())
        self.assertTrue(plugin.ready, plugin.error_reason)

        plugin.catalog = fixtures.StubCatalog(
            [ALPHA, BETA], source="help", age=7200.0, stale=True
        )
        status = support._text_of(
            await support._invoke(
                plugin.wb, support._make_event("wb status"), "status", ""
            )
        )
        self.assertIn("CLI 帮助文本", status)      # 来源还是那个来源
        self.assertIn("2 小时前", status)          # 但年龄要说出来
        self.assertIn("保鲜期", status)            # 以及"可能已经不是这样了"

        # 新鲜时不许吓人：没有那条提示。
        plugin.catalog = fixtures.StubCatalog([ALPHA, BETA], age=3.0, stale=False)
        fresh = support._text_of(
            await support._invoke(
                plugin.wb, support._make_event("wb status"), "status", ""
            )
        )
        self.assertIn("3 秒前", fresh)
        self.assertNotIn("保鲜期", fresh)

    # =====================================================================
    # 5. A selected [B, A] array of pure ids saves through AstrBotConfig, loads
    #    back in the same order, drives router.status in that order, and the
    #    normal strict validation still rejects duplicates / the virtual entry.
    # =====================================================================
    async def test_selected_order_round_trips_and_strict_validation_holds(self):
        saved = [BETA, ALPHA]
        plugin, config, cfg_path = await self._make_plugin(
            **self._picker_config(routing={"models": saved})
        )
        self.assertTrue(plugin.ready, plugin.error_reason)

        # The UI-saved array (a real AstrBotConfig save) holds pure ids in order.
        config["routing"]["models"] = list(saved)
        config.save_config()
        on_disk = json.loads(Path(cfg_path).read_text(encoding="utf-8-sig"))
        self.assertEqual(on_disk["routing"]["models"], saved)
        self.assertTrue(
            all(isinstance(item, str) for item in on_disk["routing"]["models"])
        )

        # Reload: the order survives validation and the live route.
        self.assertEqual(load_settings(on_disk)["routing"]["models"], saved)
        self.assertEqual(load_settings(config)["routing"]["models"], saved)
        self.assertEqual(plugin.router.status()["models"], saved)
        # The picker offers the catalog, but the catalog order never reorders
        # the operator's saved selection (B first, even though A comes first).
        self.assertEqual(set(self._options(config)), {ALPHA, BETA, GAMMA})
        self.assertEqual(plugin.router.status()["models"], [BETA, ALPHA])

        # Normal strict validation is preserved, not loosened for the picker.
        with self.assertRaises(ConfigError):
            load_settings({"routing": {"models": [BETA, BETA]}})
        with self.assertRaises(ConfigError):
            load_settings({"routing": {"models": [ORDERED_MODEL_ID]}})
        with self.assertRaises(ConfigError):
            load_settings({"routing": {"models": [1, 2]}})

    # =====================================================================
    # 6. A *saved legacy multiplier alias* (exactly what an older picker stored,
    #    ``wb-alpha [0.11x]``) must normalize to the pure id everywhere, while
    #    the saved array stays byte-for-byte untouched until the operator saves.
    # =====================================================================
    async def test_saved_legacy_alias_normalizes_to_the_pure_id(self):
        legacy = ALPHA + " [0.11x]"
        saved = [legacy, BETA]
        plugin, config, cfg_path = await self._make_plugin(
            **self._picker_config(routing={"models": list(saved)})
        )
        self.assertTrue(plugin.ready, plugin.error_reason)

        # Publishing options never rewrites the operator's own array: the alias
        # survives in memory and on disk, and is only *normalized* for the route.
        self.assertEqual(config["routing"]["models"], saved)
        on_disk = json.loads(Path(cfg_path).read_text(encoding="utf-8-sig"))
        self.assertEqual(on_disk["routing"]["models"], saved)
        self.assertEqual(load_settings(config)["routing"]["models"], [ALPHA, BETA])
        self.assertEqual(plugin.router.status()["models"], [ALPHA, BETA])

        option_map = self._options_map(config)
        # 候选里给的是纯 id，别名不再是一个能选中的独立项——否则运维会永远选着
        # 一个 CLI 根本不认识的字符串。
        self.assertNotIn(legacy, option_map)
        self.assertIn(ALPHA, option_map)
        self.assertEqual(option_map[ALPHA], ALPHA)
        self.assertEqual(set(option_map), {ALPHA, BETA, GAMMA})

        # 运维重新保存纯 id 之后，别名从文件里消失。
        selection = [ALPHA, BETA]
        config["routing"]["models"] = list(selection)
        config.save_config()
        migrated = json.loads(Path(cfg_path).read_text(encoding="utf-8-sig"))
        self.assertEqual(migrated["routing"]["models"], selection)
        self.assertEqual(load_settings(migrated)["routing"]["models"], selection)

        # A real reinitialize on the saved file keeps that order and the list.
        await plugin.initialize()
        self.assertTrue(plugin.ready, plugin.error_reason)
        self.assertEqual(plugin.router.status()["models"], selection)
        self.assertEqual(plugin._settings["routing"]["models"], selection)
        self.assertEqual(set(self._options(config)), {ALPHA, BETA, GAMMA})
        self.assertEqual(plugin.catalog.source, "help")

    # =====================================================================
    # 7. Model ids really become each CLI process's ``--model`` argument: the
    #    catalog is a process-level switch, and that is why the pool is keyed by
    #    model (see ``tests/test_acp_pool.py``).
    # =====================================================================
    async def test_catalog_ids_are_the_argv_model_switch(self):
        plugin, config, _ = await self._make_plugin(**self._picker_config())
        self.assertTrue(plugin.ready, plugin.error_reason)

        for model in self._options(config):
            argv = default_argv(str(self.cli_path), tools="", model=model)
            self.assertIn("--model", argv)
            self.assertEqual(argv[argv.index("--model") + 1], model)
            # 内置工具始终关掉：群聊消息永远不能驱动服务器执行命令。
            self.assertEqual(argv[argv.index("--tools") + 1], "")

        # 清单里读不到的空 id 不能变成一条没有 --model 的进程。
        argv = default_argv(str(self.cli_path), tools="", model="")
        self.assertNotIn("--model", argv)

    # =====================================================================
    # 8. ``/wb models`` never touches the account or the network, even when the
    #    list is empty -- it explains what to check instead.
    # =====================================================================
    async def test_models_command_without_a_cli_explains_and_stays_offline(self):
        self._rebuild_cli([])
        # 直接把 CLI 指向不存在的路径：连 spawn 都做不到。
        plugin, config, _ = await self._make_plugin(
            **self._picker_config(acp={"cli_path": "/nonexistent/codebuddy-for-tests"})
        )
        self.assertTrue(plugin.ready, plugin.error_reason)

        reply = support._text_of(
            await support._invoke(
                plugin.wb, support._make_event("wb models"), "models", ""
            )
        )
        self.assertIn("当前没有可用模型", reply)
        self.assertIn("codebuddy --help", reply)
        self.assertEqual(self.up.total_hits(), 0)
        # 状态页也照常渲染，不因为清单读不到就崩。
        status = support._text_of(
            await support._invoke(
                plugin.wb, support._make_event("wb status"), "status", ""
            )
        )
        self.assertIn("当前没有可用模型", status)
        self.assertEqual(self.up.total_hits(), 0)

    # =====================================================================
    # 9. 定时重读：开关、单条任务、每轮重读并重发候选、退场时收干净。
    #    读的是本机 CLI，所以整节不发一个请求；``self.up`` 的计数一直是 0。
    # =====================================================================
    async def test_the_timer_starts_once_and_the_overview_says_so(self):
        plugin, _config, _ = await self._make_plugin(
            **self._picker_config(
                model_refresh={"enabled": True, "interval_minutes": 15}
            )
        )
        self.assertTrue(plugin.ready, plugin.error_reason)

        task = plugin._model_refresh_task
        self.assertIsNotNone(task, "开关打开却没有起定时任务")
        self.assertFalse(task.done())
        self.assertEqual(task.get_name(), "wb-model-refresh")

        # 重复调用（重载、重复 initialize）不能叠第二条：两条循环读的是同一份
        # 清单，白白多 spawn 一倍进程。
        plugin._start_model_refresh()
        plugin._start_model_refresh()
        self.assertIs(plugin._model_refresh_task, task)
        # 字段同一性只说明"入口还指着它"；按名字数一遍，才算真的只有一条。
        self.assertEqual(_pending_refresh_tasks(), [task])
        # 而且跑的是这个协程本体，不是包了一层取消/异常语义的包装。
        self.assertIs(
            task.get_coro().cr_code, plugin._model_refresh_loop.__func__.__code__
        )

        status = support._text_of(
            await support._invoke(
                plugin.wb, support._make_event("wb status"), "status", ""
            )
        )
        self.assertIn("模型自动刷新：已启用（间隔 15 分钟", status)
        self.assertIn("刷新范围", status)
        self.assertEqual(self.up.total_hits(), 0)
        # 分钟→秒这一步有没有真的接到任务上：任务协程拿到的第一个参数就是它要睡的
        # 秒数。只单测换算函数挡不住"入口把分钟当秒传"这一类的接线错。
        self.assertEqual(
            plugin._model_refresh_task.get_coro().cr_frame.f_locals["interval_seconds"],
            wb_main._refresh_interval_seconds(15),
        )

    async def test_the_switch_off_starts_no_timer_and_manual_reads_still_work(self):
        plugin, _config, _ = await self._make_plugin(**self._picker_config())
        self.assertTrue(plugin.ready, plugin.error_reason)
        self.assertIsNone(plugin._model_refresh_task)

        status = support._text_of(
            await support._invoke(
                plugin.wb, support._make_event("wb status"), "status", ""
            )
        )
        self.assertIn("模型自动刷新：未启用", status)
        self.assertNotIn("刷新范围", status)

        # 关的只是定时那一份：手动重读照旧生效（配置页的 hint 就是这么写的）。
        self._rebuild_cli([ALPHA, BETA, DELTA])
        reply = support._text_of(
            await support._invoke(
                plugin.wb, support._make_event("wb models"), "models", ""
            )
        )
        self.assertIn(DELTA, reply)
        self.assertIn(DELTA, self._options(_config))
        # 手动重读不许顺手把定时那条拉起来。
        self.assertIsNone(plugin._model_refresh_task)
        self.assertEqual(_pending_refresh_tasks(), [])
        self.assertEqual(self.up.total_hits(), 0)

    async def test_a_reload_makes_a_changed_switch_take_effect(self):
        """改开关走的是重载：``initialize()`` 开头先拆，旧任务在那里被收掉。

        这条同时回答"运行中关掉怎么办"：插件没有第二套活配置通道，配置页保存后
        AstrBot 重载插件，而重载的第一个动作就是 ``_release_runtime()``。
        """
        plugin, _config, _ = await self._make_plugin(
            **self._picker_config(
                model_refresh={"enabled": True, "interval_minutes": 60}
            )
        )
        self.assertTrue(plugin.ready, plugin.error_reason)
        first = plugin._model_refresh_task
        self.assertIsNotNone(first)

        plugin._raw_config["model_refresh"]["enabled"] = False
        await plugin.initialize()
        self.assertIsNone(plugin._model_refresh_task)
        self.assertTrue(first.cancelled(), "重载之后旧任务不是被取消收掉的")
        self.assertEqual(_pending_refresh_tasks(), [])

        # 再打开、换个间隔：重载后是一条新任务，而且按新间隔睡。
        plugin._raw_config["model_refresh"]["enabled"] = True
        plugin._raw_config["model_refresh"]["interval_minutes"] = 5
        await plugin.initialize()
        second = plugin._model_refresh_task
        self.assertIsNotNone(second)
        self.assertIsNot(second, first)
        self.assertEqual(_pending_refresh_tasks(), [second])
        self.assertEqual(
            second.get_coro().cr_frame.f_locals["interval_seconds"],
            wb_main._refresh_interval_seconds(5),
        )

    async def test_the_timer_re_reads_and_republishes_the_candidates(self):
        plugin, config, _ = await self._make_plugin(
            **self._picker_config(
                model_refresh={"enabled": True, "interval_minutes": 60}
            )
        )
        self.assertTrue(plugin.ready, plugin.error_reason)
        self.assertNotIn(DELTA, self._options(config))

        # 插件自己那条按分钟走，用例等不起：换一条同形状的任务按秒跑，登记进同一个
        # 字段。被验的仍是"每轮重读、再把候选重发"，计时器本身见下面的换算用例。
        own = plugin._model_refresh_task
        own.cancel()
        await asyncio.gather(own, return_exceptions=True)
        stub = fixtures.StubCatalog([ALPHA])
        plugin.catalog = stub
        task = asyncio.create_task(
            plugin._model_refresh_loop(0.01), name="wb-model-refresh"
        )
        plugin._model_refresh_task = task
        try:
            # 这台机器上 CLI 换了版本：清单只剩 DELTA。
            stub.models = [DELTA]
            self.assertTrue(
                await _wait_for(lambda: DELTA in self._options(config)),
                "定时那一轮没有把新清单发布出来",
            )
            first = stub.reads
            # "定时"的含义是还会再来一轮：再换一次，第二轮必须跟着换。
            stub.models = [BETA]
            self.assertTrue(
                await _wait_for(lambda: BETA in self._options(config)),
                "只跑了一轮就停了，那就不是定时的",
            )
            self.assertGreater(stub.reads, first)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            plugin._model_refresh_task = None

        # 已保存但新清单里没有的项保留并标记；候选整份换掉，不是越积越多。
        mapping = self._options_map(config)
        self.assertEqual(mapping[BETA], BETA)
        self.assertIn("当前目录无此模型", mapping[ALPHA])
        self.assertIn("当前目录无此模型", mapping[GAMMA])
        self.assertNotIn(DELTA, mapping)
        # 定时重读不碰运维的选择，也不碰路由顺序。
        self.assertEqual(config["routing"]["models"], [ALPHA, GAMMA])
        self.assertEqual(plugin.router.status()["models"], [ALPHA, GAMMA])
        self.assertEqual(self.up.total_hits(), 0)

    async def test_the_timer_re_reads_the_real_cli_not_a_cached_copy(self):
        """同一个 catalog 实例、真的假 CLI：两轮之间换掉 CLI 输出，定时必须跟着换。

        上面那条用的是清单替身（它永远返回最新的 ``models``），所以它证明的是"每轮
        都发布"，证明不了"每轮都真的读了一次 CLI"。这条不换 catalog，只换 CLI 的打
        印内容，落到的是沙箱里那个真可执行脚本。
        """
        plugin, config, _ = await self._make_plugin(
            **self._picker_config(
                model_refresh={"enabled": True, "interval_minutes": 60}
            )
        )
        self.assertTrue(plugin.ready, plugin.error_reason)
        own = plugin._model_refresh_task
        own.cancel()
        await asyncio.gather(own, return_exceptions=True)

        task = asyncio.create_task(
            plugin._model_refresh_loop(0.05), name="wb-model-refresh"
        )
        plugin._model_refresh_task = task
        try:
            self._rebuild_cli([ALPHA, DELTA])
            self.assertTrue(
                await _wait_for(lambda: DELTA in self._options(config)),
                "定时那一拍没有读到换过的 CLI 清单",
            )
            self._rebuild_cli([ALPHA, BETA])
            self.assertTrue(
                await _wait_for(lambda: BETA in self._options(config)),
                "第二轮没有重读 CLI",
            )
            self.assertNotIn(DELTA, self._options(config))
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            plugin._model_refresh_task = None
        self.assertEqual(self.up.total_hits(), 0)

    async def test_a_tick_nobody_owns_stops_before_touching_anything(self):
        plugin, config, _ = await self._make_plugin(
            **self._picker_config(
                model_refresh={"enabled": True, "interval_minutes": 60}
            )
        )
        self.assertTrue(plugin.ready, plugin.error_reason)
        before = self._options(config)

        # 交接窗口的形状：字段已经被清掉，之后才起来的那条任务没人认领。
        own = plugin._model_refresh_task
        own.cancel()
        await asyncio.gather(own, return_exceptions=True)
        plugin._model_refresh_task = None
        stub = fixtures.StubCatalog([DELTA])
        plugin.catalog = stub
        orphan = asyncio.create_task(
            plugin._model_refresh_loop(0.01), name="wb-model-refresh"
        )
        await asyncio.wait({orphan}, timeout=5)

        self.assertTrue(orphan.done(), "没人认领的任务没有自己退出")
        self.assertIsNone(orphan.exception())
        self.assertEqual(stub.reads, 0, "没人认领的任务还去读了清单")
        self.assertEqual(self._options(config), before, "没人认领的任务重发了一遍候选")

    async def test_a_tick_whose_catalog_is_pulled_mid_read_publishes_nothing(self):
        plugin, config, _ = await self._make_plugin(
            **self._picker_config(
                model_refresh={"enabled": True, "interval_minutes": 60}
            )
        )
        self.assertTrue(plugin.ready, plugin.error_reason)
        before = self._options(config)

        # 读清单那一步是真让出点：运行时正好能在这段时间里被拆掉。等这一轮读回来，
        # 手里的清单已经不代表任何人，发布出去就是把候选刷成"只剩已保存项"。
        own = plugin._model_refresh_task
        own.cancel()
        await asyncio.gather(own, return_exceptions=True)
        vanishing = _VanishingCatalog(plugin)
        plugin.catalog = vanishing
        task = asyncio.create_task(
            plugin._model_refresh_loop(0.01), name="wb-model-refresh"
        )
        plugin._model_refresh_task = task
        await asyncio.wait({task}, timeout=5)

        self.assertTrue(vanishing.read, "那一轮没读到清单，用例没验到东西")
        self.assertTrue(task.done(), "清单被拆了还在空转")
        self.assertIsNone(task.exception())
        self.assertEqual(self._options(config), before, "读回来还是发了一次空候选")

    async def test_a_tick_whose_catalog_is_swapped_mid_read_keeps_running(self):
        """换了一份清单（不是拆掉）：跳过这一轮，下轮按新的接着来，不许自己退出。

        交叠的 ``initialize()`` 会重建清单，而它收尾的 ``_start_model_refresh()``
        看见这条任务还活着就不再起新的——这时候退出去，定时就永远停在那儿，配置页
        还写着"已启用"。
        """
        plugin, config, _ = await self._make_plugin(
            **self._picker_config(
                model_refresh={"enabled": True, "interval_minutes": 60}
            )
        )
        self.assertTrue(plugin.ready, plugin.error_reason)

        own = plugin._model_refresh_task
        own.cancel()
        await asyncio.gather(own, return_exceptions=True)
        # 换上的那份必须装一个启动清单里没有的 id：拿 ALPHA/BETA/GAMMA 当判据的话，
        # "已经在那儿了"会让等待立刻成立，这条用例就白等了。
        replacement = fixtures.StubCatalog([CUSTOM])
        swapping = _SwappingCatalog(plugin, replacement)
        plugin.catalog = swapping
        task = asyncio.create_task(
            plugin._model_refresh_loop(0.01), name="wb-model-refresh"
        )
        plugin._model_refresh_task = task
        try:
            self.assertTrue(
                await _wait_for(lambda: CUSTOM in self._options(config)),
                "换了清单之后没有按新的发出来（大概是自己退出了）",
            )
            self.assertFalse(task.done(), "只是换了清单，定时却停了")
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            plugin._model_refresh_task = None
        # 换的那一轮读到的 DELTA 一次都不该发出去；先确认它真的读过。
        self.assertTrue(swapping.read, "那一轮没读到清单，用例没验到东西")
        self.assertNotIn(DELTA, self._options(config))

    async def test_a_tick_without_a_catalog_stops_instead_of_an_empty_publish(self):
        plugin, config, _ = await self._make_plugin(
            **self._picker_config(
                model_refresh={"enabled": True, "interval_minutes": 60}
            )
        )
        self.assertTrue(plugin.ready, plugin.error_reason)
        before = self._options(config)

        own = plugin._model_refresh_task
        own.cancel()
        await asyncio.gather(own, return_exceptions=True)
        # 运行时的清单已经拆了，只剩这条任务还活着。
        plugin.catalog = None
        task = asyncio.create_task(
            plugin._model_refresh_loop(0.01), name="wb-model-refresh"
        )
        plugin._model_refresh_task = task
        await asyncio.wait({task}, timeout=5)

        self.assertTrue(task.done(), "清单没了还在空转")
        self.assertIsNone(task.exception())
        # 发布一次就等于把候选刷成"只剩已保存的那几项"，这里一处都不许动。
        self.assertEqual(self._options(config), before, "清单没了还发了一次空候选")

    async def test_terminate_stops_the_timer_before_the_catalog_is_dropped(self):
        plugin, config, _ = await self._make_plugin(
            **self._picker_config(
                model_refresh={"enabled": True, "interval_minutes": 60}
            )
        )
        self.assertTrue(plugin.ready, plugin.error_reason)

        # 让某一轮卡在"读清单"里面，模拟退场时正好有一轮在跑。真正那个一分钟
        # 起步的间隔在这里等不起，所以按 ``_start_model_refresh`` 的写法手工登记
        # 同一条任务——被验证的仍是 terminate 的收尸，不是造任务那一步。
        blocking = _BlockingCatalog(plugin)
        plugin.catalog = blocking
        plugin._model_refresh_task.cancel()
        await asyncio.gather(plugin._model_refresh_task, return_exceptions=True)
        task = asyncio.create_task(plugin._model_refresh_loop(0), name="wb-model-refresh")
        plugin._model_refresh_task = task
        self.assertTrue(await _wait_for(blocking.entered.is_set), "那一轮没进到读清单")
        # 先钉住"按名字确实数得到它"，否则后面那条"为空"对空集恒真。
        self.assertEqual(_pending_refresh_tasks(), [task])

        # terminate 单独起一条任务：那一轮被取消后还挂在 ``blocking.release`` 上，
        # 所以真等它的 terminate 不可能先完成；只 cancel 不等的实现在这个窗口里
        # 早就返回了。窗口给到 1 秒：再宽限的"有界等待"（比如 wait_for(task, 0.5)）
        # 也得落在这条线内被抓；正确实现在放行之前是确定挂着的，放宽不会误红。
        term = asyncio.create_task(plugin.terminate())
        try:
            finished_early = await _wait_for(term.done, timeout=1.0)
            self.assertFalse(finished_early, "terminate 没等在飞的那一轮就返回了")
        finally:
            # 断言先炸也要放行：不然收尾时那条任务没人放，整个测试进程会挂在这里。
            blocking.release.set()
        await asyncio.wait({term}, timeout=10)
        self.assertTrue(term.done(), "放行之后 terminate 还没返回")

        # 停干净：字段清空、任务真结束、事件循环里不再留着它。
        self.assertIsNone(plugin._model_refresh_task)
        self.assertIsNone(plugin.catalog)
        self.assertTrue(task.done(), "终止后定时任务还在跑")
        self.assertEqual(_pending_refresh_tasks(), [])
        self.assertTrue(blocking.exited.is_set(), "那一轮没退场")
        # 顺序：那一轮退场时清单还挂着 —— 先收任务、后丢清单，反过来这里就是 False。
        self.assertTrue(blocking.attached_at_exit, "清单在那一轮退场之前就被拆了")
        # 收尾没有把候选动过：那一轮读回来的是 DELTA，真发出去过就一定看得见。
        self.assertNotIn(DELTA, self._options(config))
        self.assertEqual(set(self._options(config)), {ALPHA, BETA, GAMMA})

    async def test_a_read_never_replaces_the_catalog_instance(self):
        """重读只在同一个 ``ModelCatalog`` 实例上做，不换对象。

        定时循环每轮记下 ``self.catalog``、读完比对还是不是它，用来判断"这一轮读的
        世界还算不算数"。要是某次重读把实例换掉，那一轮就会被当成"世界变了"而跳过，
        而且每轮都这样——定时从此只读不发，还不报错。所以这条不变量得自己站住。
        """
        plugin, _config, _ = await self._make_plugin(**self._picker_config())
        self.assertTrue(plugin.ready, plugin.error_reason)
        before = plugin.catalog
        self.assertIsNotNone(before)

        self._rebuild_cli([ALPHA, DELTA])
        models = await plugin._refresh_catalog()
        self.assertIn(DELTA, models, "重读没真的读")
        self.assertIs(plugin.catalog, before, "重读把清单实例换掉了")

    async def test_an_unusable_interval_starts_no_timer(self):
        plugin, _config, _ = await self._make_plugin(
            **self._picker_config(
                model_refresh={"enabled": True, "interval_minutes": 60}
            )
        )
        self.assertTrue(plugin.ready, plugin.error_reason)
        first = plugin._model_refresh_task
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        plugin._model_refresh_task = None

        # 校验层已经挡掉了这些值；这里守的是"设置被改坏之后别再起任务"。
        for bad in (0, -3, True, 2.5, "60", None):
            plugin._settings["model_refresh"]["interval_minutes"] = bad
            plugin._start_model_refresh()
            self.assertIsNone(
                plugin._model_refresh_task, "间隔 {!r} 竟然起了任务".format(bad)
            )

        plugin._settings["model_refresh"] = "on"
        plugin._start_model_refresh()
        self.assertIsNone(plugin._model_refresh_task)


class _BlockingCatalog:
    """读不完的清单；被取消后还要等用例放行，才真的退场。

    两件事因此变成可观测的：取消有没有被送达（``entered`` / ``exited``），以及
    terminate 是不是**等到了**那一轮——只要 ``release`` 没放行，真正 gather 这条
    任务的 terminate 就不可能返回；只 cancel 不等的实现会在这段时间里跑完。
    退场时还顺手记下"清单当时还在不在"，用来钉"先收任务、后丢清单"的顺序。

    **等放行是有上限的。** 用例万一在放行之前就失败了，没人会来 ``set``；无限等
    下去，事件循环收尾时这条任务永远收不掉，整个测试进程就挂在这儿。超过
    :attr:`GRACE` 或再挨一次取消，一律认输退场。
    """

    #: 等用例放行的上限（秒）。正常的用例几百毫秒内就放行。
    GRACE = 10.0

    def __init__(self, plugin) -> None:
        self.plugin = plugin
        # 故意不是候选里已经有的那几个：万一被发布出去，用例看得见。
        self.models = [DELTA]
        self.source = "help"
        self.age = 0.0
        self.stale = False
        self.entered = asyncio.Event()
        self.exited = asyncio.Event()
        #: 由用例放行；不放行，被取消的那一轮就一直挂着。
        self.release = asyncio.Event()
        #: 那一轮退场时，插件手上还是不是这份清单。
        self.attached_at_exit = None

    async def refresh(self):
        self.entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            # 放行之前不退场：terminate 到底等没等，全靠这一点。
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await asyncio.wait_for(self.release.wait(), timeout=self.GRACE)
            raise
        finally:
            self.attached_at_exit = self.plugin.catalog is self
            self.exited.set()
        return list(self.models)


class _VanishingCatalog:
    """读清单的中途运行时被拆掉：``refresh`` 返回时插件手上的清单已经没了。

    真实形状是"读一次 ``--help`` 要几十到上百毫秒，这期间 ``_release_runtime``
    把 ``self.catalog`` 置空"。这里用一次读把现场摆出来，不依赖时序。
    """

    def __init__(self, plugin) -> None:
        self.plugin = plugin
        self.models = [ALPHA]
        self.source = "help"
        self.age = 0.0
        self.stale = False
        self.read = False

    async def refresh(self):
        self.read = True
        self.plugin.catalog = None
        await asyncio.sleep(0)
        return [DELTA]


class _SwappingCatalog:
    """读清单的中途运行时被换成了另一份（不是拆掉）：这一轮读到的东西已经过时了。

    真实形状是交叠的 ``initialize()`` 重建了清单。返回的 ``models`` 故意是旧的那份，
    用例据此区分"跳过这一轮"与"照着旧清单发出去"。
    """

    def __init__(self, plugin, replacement) -> None:
        self.plugin = plugin
        self.replacement = replacement
        self.models = [ALPHA]
        self.source = "help"
        self.age = 0.0
        self.stale = False
        self.read = False

    async def refresh(self):
        self.read = True
        self.plugin.catalog = self.replacement
        await asyncio.sleep(0)
        return [DELTA]


def _pending_refresh_tasks():
    """事件循环里还挂着的定时任务（只算这个名字的）。"""
    return [task for task in asyncio.all_tasks() if task.get_name() == "wb-model-refresh"]


async def _wait_for(predicate, timeout=10.0, interval=0.01) -> bool:
    """轮询等一个条件成立；超时返回 ``False``，让用例自己报清楚哪一步没等到。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


class ModelRefreshConfigTests(unittest.TestCase):
    """``model_refresh`` 这一组配置的默认值与边界（不走插件，只要校验结果）。"""

    def test_the_defaults_are_switched_on_and_hourly(self):
        self.assertEqual(
            load_settings({})["model_refresh"],
            {"enabled": True, "interval_minutes": 60},
        )
        # 旧配置文件里没有这一组时同样落到默认值，不会读成"关"。
        self.assertEqual(
            load_settings({"routing": {}})["model_refresh"],
            {"enabled": True, "interval_minutes": 60},
        )
        # ``null`` 在这个配置里一律当"没写"（``_bool`` / ``_int`` 都是这个约定，
        # 别的每一组也一样），所以它落到默认值而不是报错。
        self.assertEqual(
            load_settings(
                {"model_refresh": {"enabled": None, "interval_minutes": None}}
            )["model_refresh"],
            {"enabled": True, "interval_minutes": 60},
        )

    def test_the_switch_accepts_only_a_bool(self):
        for good in (True, False):
            self.assertEqual(
                load_settings({"model_refresh": {"enabled": good}})["model_refresh"][
                    "enabled"
                ],
                good,
            )
        for bad in ("true", 1, [], {}):
            with self.assertRaises(ConfigError):
                load_settings({"model_refresh": {"enabled": bad}})

    def test_the_interval_must_be_a_positive_integer(self):
        for good in (1, 60, 1440):
            self.assertEqual(
                load_settings(
                    {"model_refresh": {"interval_minutes": good}}
                )["model_refresh"]["interval_minutes"],
                good,
            )
        # ``True`` 在 Python 里是 1：不许它冒充一分钟的间隔。
        for bad in (0, -1, True, 60.0, "60", [60]):
            with self.assertRaises(ConfigError):
                load_settings({"model_refresh": {"interval_minutes": bad}})

    def test_minutes_become_seconds(self):
        """整条定时链上唯一的单位换算：错成 ``* 3600`` 或当秒睡，跑起来都"正常"。"""
        for minutes, seconds in ((1, 60), (15, 900), (60, 3600), (1440, 86400)):
            self.assertEqual(wb_main._refresh_interval_seconds(minutes), seconds)
        # 与校验层同一套边界：不是正整数就不该有秒数出来。
        for bad in (0, -3, True, 2.5, "60", None):
            self.assertIsNone(wb_main._refresh_interval_seconds(bad))


def tearDownModule() -> None:
    """Delete only this module's exact sandbox directory."""
    shutil.rmtree(MODULE_TMP, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
