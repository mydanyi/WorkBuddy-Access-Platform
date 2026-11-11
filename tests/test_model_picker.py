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
* ``config.model_refresh`` + ``_model_refresh_loop`` are gone.  The list is read
  once at startup and re-read on demand (``/wb models``), so there is no periodic
  task to test.  The refresh *policy* that replaced it is covered by
  ``tests/test_models.py`` (``ensure()`` freshness) and by
  ``test_commands_refresh_the_local_catalog`` below.

Every model id and token below is a FICTIONAL FIXED SAMPLE.
"""

import json
import os
import shutil
import sys
import tempfile
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


def tearDownModule() -> None:
    """Delete only this module's exact sandbox directory."""
    shutil.rmtree(MODULE_TMP, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
