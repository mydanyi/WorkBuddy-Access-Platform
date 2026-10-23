"""Real-AstrBot regression tests for the WorkBuddy model picker (P01, red).

Goal: ``routing.models`` must be a **native AstrBot list+options multiselect**
fed by the *real* WorkBuddy catalog -- pure upstream model ids as the values and
the existing ``display_model_id`` metadata as parallel labels -- instead of a
free-text field the operator has to type by hand.

Nothing here is a source-string assertion.  The plugin runs inside the real
AstrBot framework with the real ``AstrBotConfig`` built from the plugin's real
``_conf_schema.json``, the real ``WorkBuddyPlugin`` / ``UpstreamClient`` /
``ModelRouter`` / ``load_settings``, and the existing real ``aiohttp`` fake
upstream supplied by ``tests.test_plugin_integration``.  Only the external
framework boundary (the ``Context`` shim) and the external upstream responses
are fixtures; every behaviour asserted below is production code.

The ``tests.test_plugin_integration`` fixtures are *bound* method by method (its
``TestCase`` is never inherited or instantiated as a base), so that suite is not
re-run from here.

Red on purpose: today ``initialize`` performs **no** catalog read at all, so an
authenticated plugin never delivers any ``options`` / ``labels`` (and does not
know a single real model id).  These tests therefore fail until ``main`` updates
the in-memory ``AstrBotConfig.schema`` from the model catalog.  Do not pre-fix
``main`` / ``_conf_schema.json`` here; the red light is the point.

Every model id, multiplier, token and master key below is a FICTIONAL FIXED
SAMPLE -- no production model id is hard-coded.

Sections 8-14 add the *model auto-refresh* behaviour in the same style: the
real ``config.model_refresh`` settings, the real plugin lifecycle and the real
``_model_refresh_loop`` driven at a short **real** interval (the production loop
is called directly, so ``asyncio.sleep``, ``model_details`` and the public config
validation are never patched), all against the real fake upstream.  They fail
until the plugin owns that task, its settings and its status line.
"""

import asyncio
import json
import os
import sys
import unittest
import uuid
from pathlib import Path

from aiohttp import web

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

# --- Real framework / plugin imports (sandbox already pinned by ``support``). ---
import astrbot_plugin_workbuddy.main as wb_main  # noqa: E402
from astrbot_plugin_workbuddy.core.config import ConfigError, load_settings  # noqa: E402
from astrbot_plugin_workbuddy.core.credits import (  # noqa: E402
    display_model_id,
    format_credits,
    upstream_model_id,
)
from astrbot_plugin_workbuddy.core.errors import UpstreamError  # noqa: E402
from astrbot_plugin_workbuddy.core.routing import ORDERED_MODEL_ID  # noqa: E402

MODELS_PATH = support.MODELS_CATALOG_PATH
V3_CONFIG_PATH = support.V3_CONFIG_PATH

# --- FICTIONAL FIXED SAMPLE model ids (never production ids). ---
ALPHA = "wb-alpha"        # priced + promotional
BETA = "wb-beta"          # zero multiplier (a real, free model)
GAMMA = "wb-gamma"        # no multiplier anywhere -> unknown
UNLISTED = "other-provider-model"   # catalogued, but not in the cli agent
DISABLED = "wb-disabled"  # cli-listed but disabled upstream

# One active promotion for ALPHA: a fixed discounted multiplier and a badge.
PROMO = {
    "enabled": True,
    "priority": 1,
    "modelIds": [ALPHA],
    "discount": {"discountedCredits": "0.03"},
    "badge": {"label": "夜间折扣"},
    "hover": {"textZh": "FICTIONAL-PROMO-NOTE"},
}


def _label(model_id, effective, promo_label=""):
    """The label the real ``display_model_id`` derives from these metadata."""
    return display_model_id(
        {
            "id": model_id,
            "name": model_id,
            "credits": None,
            "effective_credits": effective,
            "promo_label": promo_label,
            "promo_note": "",
        }
    )


# Expected picker labels, derived with the *existing* production helper (never a
# hand-written string), covering the promoted, zero and unknown multiplier cases.
ALPHA_PROMO_LABEL = _label(ALPHA, format_credits(0.03), "夜间折扣")
ALPHA_PLAIN_LABEL = _label(ALPHA, format_credits(0.11))
BETA_ZERO_LABEL = _label(BETA, format_credits(0))
BETA_PROMOTED_LABEL = _label(BETA, format_credits(0.25))
GAMMA_UNKNOWN_LABEL = _label(GAMMA, None)


class _ModelPickerTestCase(unittest.IsolatedAsyncioTestCase):
    """Drives the real plugin; the shared fixtures are bound, not inherited."""

    async def asyncSetUp(self) -> None:
        await support._PluginIntegrationTestCase.asyncSetUp(self)
        # The real fake upstream, with its model catalog route replaced by a
        # registrable external fixture (still a real aiohttp handler).
        self.catalog_entries = [
            {"id": ALPHA, "name": ALPHA, "credits": 0.11, "cli": True},
            {"id": BETA, "name": BETA, "credits": 0, "cli": True},
            {"id": GAMMA, "name": GAMMA, "cli": True},
            {"id": UNLISTED, "name": UNLISTED, "credits": 1.0, "cli": False},
            {"id": DISABLED, "name": DISABLED, "credits": 2.0, "cli": True,
             "disabled": True},
        ]
        # ``None`` means "answer the catalog"; a tuple is a real failure answer.
        self.models_failure = None
        self.up.v3 = {"models": [], "modelPromotions": []}
        self.up.handlers[("GET", MODELS_PATH)] = self._models_handler

    async def asyncTearDown(self) -> None:
        await support._PluginIntegrationTestCase.asyncTearDown(self)

    # -- external upstream fixture -----------------------------------------
    def _models_handler(self, request):
        """A real ``aiohttp`` response: the enterprise catalog or a failure."""
        if self.models_failure is not None:
            status, body, headers = self.models_failure
            return web.json_response(body, status=status, headers=dict(headers))
        models = [
            {key: value for key, value in entry.items() if key != "cli"}
            for entry in self.catalog_entries
        ]
        cli_ids = [
            entry["id"] for entry in self.catalog_entries if entry.get("cli")
        ]
        return web.json_response(
            {
                "code": 0,
                "data": {
                    "models": models,
                    "agents": [{"name": "cli", "models": cli_ids}],
                },
            }
        )

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
            "growth": {"auto_after_signin": False, "auto_claim": False, "task_interval": 0},
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
        ``routing`` schema metadata (with the real, freshly verified
        ``options`` / ``labels``) plus a copy of this test's fictional routing
        config whose ``models`` list is empty, so a separate harness can drive
        the deployed frontend through a pure-ID selection / save / reopen.  No
        production config, credential or data file is read or written here.
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

    async def _wait_until(self, predicate, timeout=3.0):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not predicate():
            if loop.time() >= deadline:
                return False
            await asyncio.sleep(0.01)
        return True

    async def _wait_models_read(self, count=1, timeout=3.0):
        """An authenticated startup must attempt exactly one catalog read."""
        key = ("GET", MODELS_PATH)
        ok = await self._wait_until(lambda: self.up.hits[key] >= count, timeout)
        self.assertTrue(
            ok,
            "an authenticated initialize() never attempted the model catalog "
            "read, so the picker can never offer real model ids",
        )

    # =====================================================================
    # 1. An authenticated startup delivers the *real* eligible catalog as the
    #    native option values, with display_model_id labels in parallel --
    #    including the zero and the unknown multiplier cases -- and never the
    #    virtual ordered entry or an unrelated provider's model.
    # =====================================================================
    async def test_authenticated_startup_delivers_real_ids_and_metadata_labels(self):
        self._seed_credentials()
        self.up.v3 = {"models": [], "modelPromotions": [PROMO]}
        plugin, config, _ = await self._make_plugin(**self._picker_config())

        self.assertTrue(plugin.ready, plugin.error_reason)
        self.assertEqual(plugin.error_reason, "")

        # Exactly one catalog read (+ its /v3/config supplement); no extra probe.
        await self._wait_models_read(1)
        self.assertEqual(self.up.hits[("GET", MODELS_PATH)], 1)
        self.assertEqual(self.up.hits[("GET", V3_CONFIG_PATH)], 1)
        self.assertEqual(
            self.up.total_hits(),
            2,
            "authenticated startup must read the catalog once, not once per "
            "model and not twice",
        )

        field = self._schema_models(config)
        self.assertEqual(
            field.get("type"),
            "list",
            "routing.models must stay the native list type the options branch "
            "belongs to",
        )

        options = self._options(config)
        labels = self._labels(config)
        # Only the cli-callable, enabled, chat entries of the live catalog.
        self.assertEqual(set(options), {ALPHA, BETA, GAMMA})
        self.assertEqual(len(options), 3, "a duplicated option value is unusable")
        self.assertNotIn(ORDERED_MODEL_ID, options)
        self.assertNotIn(UNLISTED, options)
        self.assertNotIn(DISABLED, options)
        for option in options:
            # The saved value must be a pure upstream id the router can call.
            self.assertEqual(upstream_model_id(option), option)

        # Labels are the existing display metadata, in parallel with the values.
        self.assertEqual(len(labels), len(options))
        self.assertEqual(
            dict(zip(options, labels)),
            {
                ALPHA: ALPHA_PROMO_LABEL,       # promotional discounted multiplier
                BETA: BETA_ZERO_LABEL,          # zero multiplier (free model)
                GAMMA: GAMMA_UNKNOWN_LABEL,     # no multiplier anywhere
            },
        )

        # A catalog refresh never rewrites the operator's saved selection.
        self.assertEqual(config["routing"]["models"], [ALPHA, GAMMA])

        # Optional: hand the *delivered* metadata to the lightweight real-SPA
        # harness (only when the manager named an evidence directory).
        self._export_picker_fixture(config)

    # =====================================================================
    # 2. Without credentials nothing is fetched from upstream, yet the saved
    #    selection stays visible as options (in its configured order) and the
    #    unrelated gateway settings are untouched.
    # =====================================================================
    async def test_no_credentials_keep_saved_options_without_any_model_request(self):
        saved = [GAMMA, BETA]
        plugin, config, cfg_path = await self._make_plugin(
            **self._picker_config(routing={"models": saved})
        )

        self.assertTrue(plugin.ready, plugin.error_reason)
        # Really the unauthenticated path: no stored account at all.
        self.assertFalse(plugin.auth.credentials)
        self.assertEqual(
            self.up.total_hits(),
            0,
            "an unauthenticated plugin must never call the model catalog",
        )

        options = self._options(config)
        self.assertEqual(
            set(options),
            set(saved),
            "saved model values must remain selectable, not be erased",
        )
        self.assertNotIn(ORDERED_MODEL_ID, options)
        # Configured order is kept (no re-sorting, no catalog substitution).
        self.assertEqual([item for item in options if item in saved], saved)

        labels = self._labels(config)
        self.assertEqual(len(labels), len(options))
        for value, label in zip(options, labels):
            self.assertTrue(label.startswith(value))

        # The saved array itself is unchanged on disk and through load_settings.
        on_disk = json.loads(Path(cfg_path).read_text(encoding="utf-8-sig"))
        self.assertEqual(on_disk["routing"]["models"], saved)
        self.assertEqual(load_settings(config)["routing"]["models"], saved)
        self.assertEqual(plugin.router.status()["models"], saved)

        # Unrelated gateway tuning is not touched by the model work.
        self.assertEqual(config["gateway"]["concurrency"], 3)
        self.assertEqual(config["gateway"]["queue_size"], 10)
        self.assertEqual(plugin._settings["gateway"]["concurrency"], 3)
        self.assertEqual(plugin._settings["gateway"]["queue_size"], 10)

    # =====================================================================
    # 3. A 429 on the startup catalog read leaves the plugin ready with the
    #    saved selection intact and offers no fabricated catalog.
    # =====================================================================
    async def test_startup_catalog_failure_keeps_plugin_ready_and_saved_selection(self):
        saved = [ALPHA, GAMMA]
        self.models_failure = (
            429,
            {"code": 4290, "msg": "FICTIONAL-MODELS-RATE-LIMIT"},
            {"Retry-After": "30"},
        )
        self._seed_credentials()
        plugin, config, _ = await self._make_plugin(
            **self._picker_config(routing={"models": saved})
        )

        self.assertTrue(plugin.ready, "a catalog failure must not disable the plugin")
        self.assertEqual(plugin.error_reason, "")
        await self._wait_models_read(1)

        # The failure was observed upstream (one real attempt) ...
        self.assertEqual(self.up.hits[("GET", MODELS_PATH)], 1)

        options = self._options(config)
        self.assertEqual(
            set(options),
            set(saved),
            "a failed catalog read must not drop or invent model options",
        )
        self.assertEqual(len(options), len(saved))
        labels = self._labels(config)
        self.assertEqual(len(labels), len(options))
        for value, label in zip(options, labels):
            self.assertTrue(label.startswith(value))
            self.assertNotIn("FICTIONAL-MODELS-RATE-LIMIT", label)

        # Neither the delivered schema nor the saved array is rewritten.
        self.assertNotIn("FICTIONAL-MODELS-RATE-LIMIT", json.dumps(config.schema))
        self.assertEqual(config["routing"]["models"], saved)
        self.assertEqual(load_settings(config)["routing"]["models"], saved)
        self.assertEqual(plugin.router.status()["models"], saved)

    # =====================================================================
    # 4. A later *successful* real ``client.model_details`` (the /wb models and
    #    API shared path) refreshes the picker labels from changed metadata
    #    without issuing an additional catalog GET or any per-model query.
    # =====================================================================
    async def test_models_command_refresh_updates_labels_from_changed_metadata(self):
        self._seed_credentials()
        self.up.v3 = {"models": [], "modelPromotions": [PROMO]}
        plugin, config, _ = await self._make_plugin(**self._picker_config())
        await self._wait_models_read(1)

        first = self._options_map(config)
        self.assertEqual(
            first,
            {
                ALPHA: ALPHA_PROMO_LABEL,
                BETA: BETA_ZERO_LABEL,
                GAMMA: GAMMA_UNKNOWN_LABEL,
            },
        )
        options_before = self._options(config)
        self.assertEqual(set(options_before), {ALPHA, BETA, GAMMA})

        # The upstream metadata really changes: BETA is no longer free and the
        # promotion on ALPHA has ended.
        for entry in self.catalog_entries:
            if entry["id"] == BETA:
                entry["credits"] = 0.25
        self.up.v3 = {"models": [], "modelPromotions": []}

        before_total = self.up.total_hits()
        reply = support._text_of(
            await support._invoke(
                plugin.wb, support._make_event("wb models"), "models", ""
            )
        )
        # One catalog GET plus its /v3/config supplement -- nothing per model.
        self.assertEqual(
            self.up.total_hits(),
            before_total + 2,
            "the shared model_details path must reuse one catalog read "
            "(plus /v3/config) and never query per model",
        )
        self.assertEqual(self.up.hits[("GET", MODELS_PATH)], 2)
        self.assertEqual(self.up.hits[("GET", V3_CONFIG_PATH)], 2)

        second = self._options_map(config)
        self.assertEqual(set(second), {ALPHA, BETA, GAMMA})
        self.assertEqual(second[ALPHA], ALPHA_PLAIN_LABEL)
        self.assertEqual(second[BETA], BETA_PROMOTED_LABEL)
        self.assertEqual(second[GAMMA], GAMMA_UNKNOWN_LABEL)
        self.assertNotIn(ALPHA_PROMO_LABEL, self._labels(config))
        self.assertNotIn(BETA_ZERO_LABEL, self._labels(config))
        # The command output reflects the same new metadata.
        self.assertIn(BETA_PROMOTED_LABEL, reply)

        # The saved selection is still the operator's, untouched.
        self.assertEqual(config["routing"]["models"], [ALPHA, GAMMA])

    # =====================================================================
    # 5. A failed later fetch must not replace a successful catalog or the
    #    saved selection.
    # =====================================================================
    async def test_failed_later_fetch_keeps_previous_catalog_and_selection(self):
        self._seed_credentials()
        plugin, config, _ = await self._make_plugin(**self._picker_config())
        await self._wait_models_read(1)
        options_before = self._options(config)
        labels_before = self._labels(config)
        self.assertEqual(set(options_before), {ALPHA, BETA, GAMMA})

        self.models_failure = (
            500,
            {"code": 5000, "msg": "FICTIONAL-MODELS-ERROR"},
            {},
        )
        before = self.up.hits[("GET", MODELS_PATH)]
        with self.assertRaises(UpstreamError):
            await plugin.client.model_details()
        self.assertEqual(
            self.up.hits[("GET", MODELS_PATH)],
            before + 1,
            "a failed refresh must be a real attempt, not a cached no-op",
        )

        self.assertEqual(
            self._options(config),
            options_before,
            "a failed fetch must not replace a successful catalog",
        )
        self.assertEqual(self._labels(config), labels_before)
        self.assertEqual(config["routing"]["models"], [ALPHA, GAMMA])
        self.assertEqual(load_settings(config)["routing"]["models"], [ALPHA, GAMMA])

    # =====================================================================
    # 6. A selected [B, A] array of real pure ids saves through AstrBotConfig,
    #    loads back in the same order, drives router.status in that order, and
    #    normal strict validation still rejects duplicates / the virtual entry.
    # =====================================================================
    async def test_selected_order_round_trips_and_strict_validation_holds(self):
        saved = [BETA, ALPHA]
        self._seed_credentials()
        plugin, config, cfg_path = await self._make_plugin(
            **self._picker_config(routing={"models": saved})
        )
        self.assertTrue(plugin.ready, plugin.error_reason)
        await self._wait_models_read(1)

        # The UI-saved array (a real AstrBotConfig save) holds pure ids in order.
        config["routing"]["models"] = list(saved)
        config.save_config()
        on_disk = json.loads(Path(cfg_path).read_text(encoding="utf-8-sig"))
        self.assertEqual(on_disk["routing"]["models"], saved)
        self.assertTrue(all(isinstance(item, str) for item in on_disk["routing"]["models"]))
        for item in on_disk["routing"]["models"]:
            self.assertEqual(upstream_model_id(item), item)

        # Reload: the order survives validation and the live route.
        self.assertEqual(load_settings(on_disk)["routing"]["models"], saved)
        self.assertEqual(load_settings(config)["routing"]["models"], saved)
        self.assertEqual(plugin.router.status()["models"], saved)
        self.assertEqual(config["routing"]["models"], saved)
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
        # A legacy alias value still normalizes to its pure upstream id.
        self.assertEqual(
            load_settings(
                {"routing": {"models": [ALPHA + " [0.11x]", BETA + " [0x]"]}}
            )["routing"]["models"],
            [ALPHA, BETA],
        )

    # =====================================================================
    # 7. A *saved legacy multiplier alias* (exactly the label an older picker
    #    stored, i.e. the display string ``wb-alpha [0.11x]``) must not shadow
    #    the same model's pure catalog id: the alias stays a selectable option
    #    and the saved array stays byte-for-byte untouched, while the real pure
    #    id is still offered, so the operator can deselect the alias and
    #    re-select the pure id.  That pure reselection then round-trips through
    #    the real ``AstrBotConfig`` save into load_settings and the live route.
    # =====================================================================
    async def test_saved_legacy_alias_does_not_shadow_the_pure_catalog_option(self):
        # The legacy value an older picker saved is the display label itself.
        legacy = ALPHA + " [0.11x]"
        self.assertEqual(legacy, ALPHA_PLAIN_LABEL)
        saved = [legacy, BETA]
        self._seed_credentials()
        plugin, config, cfg_path = await self._make_plugin(
            **self._picker_config(routing={"models": list(saved)})
        )
        self.assertTrue(plugin.ready, plugin.error_reason)
        await self._wait_models_read(1)

        # Publishing options never rewrites the operator's own array: the alias
        # survives in memory, on disk, and is only *normalized* for the route.
        self.assertEqual(config["routing"]["models"], saved)
        on_disk = json.loads(Path(cfg_path).read_text(encoding="utf-8-sig"))
        self.assertEqual(on_disk["routing"]["models"], saved)
        self.assertEqual(load_settings(config)["routing"]["models"], [ALPHA, BETA])
        self.assertEqual(plugin.router.status()["models"], [ALPHA, BETA])

        option_map = self._options_map(config)
        options = list(option_map)
        # The saved alias is still delivered, never dropped.
        self.assertIn(legacy, options)

        # ... but the real catalog entry for that *same* model must not be
        # consumed by it.  Today ``seen`` tracks the normalized pure id, so the
        # saved alias adds ``wb-alpha`` to ``seen`` and the catalog's
        # ``wb-alpha`` option is skipped: the alias is then unpairable and the
        # promised migration to the pure id is impossible.
        self.assertIn(
            ALPHA,
            options,
            "the saved legacy alias {!r} consumed the pure id {!r}, so the real "
            "catalog option is missing and the operator can never migrate to "
            "{!r}".format(legacy, ALPHA, ALPHA),
        )
        # The pure option carries the actual catalog label (no promotion in this
        # fixture), while the alias label says it is a legacy value and names
        # that same live label.
        self.assertEqual(option_map[ALPHA], ALPHA_PLAIN_LABEL)
        alias_label = option_map[legacy]
        self.assertTrue(alias_label.startswith(legacy))
        self.assertNotEqual(alias_label, ALPHA_PLAIN_LABEL)
        self.assertIn(ALPHA_PLAIN_LABEL, alias_label[len(legacy):])

        # The operator now deselects the alias and selects the two pure ids the
        # picker offers; the real config service saves that selection as-is.
        selection = [ALPHA, BETA]
        config["routing"]["models"] = list(selection)
        config.save_config()
        migrated = json.loads(Path(cfg_path).read_text(encoding="utf-8-sig"))
        self.assertEqual(migrated["routing"]["models"], selection)
        for item in migrated["routing"]["models"]:
            self.assertEqual(upstream_model_id(item), item)
        self.assertEqual(load_settings(migrated)["routing"]["models"], selection)

        # A real reinitialize on the saved file keeps that order, issues exactly
        # one catalog read again, and no longer offers the legacy alias at all.
        await plugin.initialize()
        self.assertTrue(plugin.ready, plugin.error_reason)
        self.assertEqual(self.up.hits[("GET", MODELS_PATH)], 2)
        self.assertEqual(plugin.router.status()["models"], selection)
        self.assertEqual(plugin._settings["routing"]["models"], selection)
        self.assertEqual(config["routing"]["models"], selection)
        after = self._options_map(config)
        self.assertEqual(set(after), {ALPHA, BETA, GAMMA})
        self.assertNotIn(legacy, after)
        self.assertEqual([value for value in after if value in selection], selection)
        self.assertEqual(after[ALPHA], ALPHA_PLAIN_LABEL)
        self.assertTrue(after[ALPHA].startswith(ALPHA))

    # =====================================================================
    # model auto-refresh helpers
    #
    # The periodic loop is production code: the tests never replace
    # ``asyncio.sleep`` / ``model_details`` and never touch the public config
    # validation.  The only thing they do is cancel the ordinary (one hour)
    # task and start the *same* production loop at a short real interval,
    # attached to the plugin's own task slot so ``terminate`` owns it.
    # =====================================================================
    def _new_config(self, **overrides):
        """A real ``AstrBotConfig`` from the plugin's real schema."""
        cfg_path = support._SESSION_ROOT / ("cfg-" + uuid.uuid4().hex + ".json")
        return self._build_config(cfg_path, **overrides)

    def _assert_documented_settings(self, settings, label):
        """Every pre-existing validated default is unchanged by the refresh."""
        for section, values in support.DOCUMENTED_CONFIG.items():
            self.assertIn(
                section, settings, "{}: section {} vanished".format(label, section)
            )
            for key, expected in values.items():
                self.assertEqual(
                    settings[section][key],
                    expected,
                    "{}: unrelated setting {}.{} changed".format(label, section, key),
                )

    async def _attach_short_loop(self, plugin, interval=0.02):
        """Run the *real* ``_model_refresh_loop`` at a short real interval."""
        ordinary = getattr(plugin, "_model_refresh_task", None)
        if ordinary is not None and not ordinary.done():
            ordinary.cancel()
            await asyncio.gather(ordinary, return_exceptions=True)
        loop = getattr(plugin, "_model_refresh_loop", None)
        self.assertTrue(
            callable(loop),
            "the plugin owns no model refresh loop, so nothing can refresh the "
            "catalog periodically",
        )
        short = asyncio.create_task(loop(interval), name="wb-model-refresh-short")
        plugin._model_refresh_task = short
        return short

    async def _stop_short_loop(self, plugin, task):
        """Cancel and await the short loop, leaving no running task behind."""
        pending = []
        current = getattr(plugin, "_model_refresh_task", None)
        if current is not None and current is not task and not current.done():
            pending.append(current)
        if task is not None and not task.done():
            pending.append(task)
        for item in pending:
            item.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    def _live_refresh_tasks(self):
        """Live tasks running this plugin's refresh loop (name or coroutine)."""
        found = []
        for candidate in asyncio.all_tasks():
            if candidate is asyncio.current_task() or candidate.done():
                continue
            marker = "{} {}".format(
                getattr(candidate.get_coro(), "__qualname__", ""),
                candidate.get_name(),
            )
            if "model_refresh" in marker:
                found.append(candidate)
        return found

    def _refresh_status_block(self, text):
        """The status lines that report the model auto-refresh."""
        lines = [
            line for line in text.splitlines() if "刷新" in line or "更新" in line
        ]
        self.assertTrue(
            lines,
            "wb status must report the model auto-refresh state: {!r}".format(text),
        )
        return "\n".join(lines)

    # =====================================================================
    # 8. The refresh policy really comes from the plugin's own schema and
    #    real ``load_settings``: default on / 60 minutes, explicit values
    #    honoured, and a bad interval rejected with a fixed message.
    # =====================================================================
    async def test_model_refresh_settings_defaults_overrides_and_rejections(self):
        config = self._new_config()
        delivered = config.get("model_refresh")
        self.assertIsInstance(
            delivered,
            dict,
            "the delivered config must carry a model_refresh group",
        )
        self.assertIs(delivered.get("enabled"), True)
        self.assertEqual(delivered.get("interval_minutes"), 60)

        defaults = load_settings(config)
        refresh = defaults.get("model_refresh")
        self.assertIsInstance(
            refresh, dict, "load_settings must expose model_refresh"
        )
        self.assertIs(refresh.get("enabled"), True)
        self.assertEqual(refresh.get("interval_minutes"), 60)
        self._assert_documented_settings(defaults, "the refresh defaults")

        explicit = self._new_config(
            model_refresh={"enabled": False, "interval_minutes": 2}
        )
        self.assertEqual(explicit["model_refresh"]["enabled"], False)
        self.assertEqual(explicit["model_refresh"]["interval_minutes"], 2)
        settings = load_settings(explicit)
        self.assertIs(settings["model_refresh"]["enabled"], False)
        self.assertEqual(settings["model_refresh"]["interval_minutes"], 2)
        self._assert_documented_settings(settings, "an explicit refresh group")

        # Zero, negative, boolean, float and string intervals are all invalid.
        for bad in (0, -1, True, 1.5, "FICTIONAL-SECRET-LOOKING-INTERVAL"):
            with self.assertRaises(ConfigError) as caught:
                load_settings({"model_refresh": {"interval_minutes": bad}})
            self.assertNotIn(
                "FICTIONAL-SECRET-LOOKING-INTERVAL",
                str(caught.exception),
                "a rejected interval must never echo the raw value",
            )
            self.assertNotIn(repr(bad), str(caught.exception))

        # A non-boolean switch is rejected just as strictly.
        with self.assertRaises(ConfigError) as caught:
            load_settings({"model_refresh": {"enabled": "yes"}})
        self.assertNotIn("yes", str(caught.exception))

    # =====================================================================
    # 9. Lifecycle: one owned task when enabled, none when disabled, and a
    #    repeated initialize / terminate that never stacks or leaks tasks.
    # =====================================================================
    async def test_refresh_task_lifecycle_is_single_and_idempotent(self):
        self._seed_credentials()
        plugin, config, _ = await self._make_plugin(**self._picker_config())
        self.assertTrue(plugin.ready, plugin.error_reason)
        await self._wait_models_read(1)

        task = getattr(plugin, "_model_refresh_task", None)
        self.assertIsNotNone(
            task, "an enabled model refresh must own a background task"
        )
        self.assertIsInstance(task, asyncio.Task)
        self.assertFalse(task.done(), "the periodic task must stay alive after startup")

        # The loop waits one whole production interval first: the startup prime
        # stays the only catalog read (no immediate duplicate, no short period).
        await asyncio.sleep(0.05)
        self.assertEqual(
            self.up.hits[("GET", MODELS_PATH)],
            1,
            "the periodic loop must wait one interval before its first refresh",
        )

        disabled, _, _ = await self._make_plugin(
            **self._picker_config(model_refresh={"enabled": False})
        )
        self.assertTrue(disabled.ready, disabled.error_reason)
        self.assertIsNone(
            getattr(disabled, "_model_refresh_task", None),
            "a disabled model refresh must not start a background task",
        )
        quiet = dict(self.up.hits)
        await asyncio.sleep(0.05)
        self.assertEqual(
            dict(self.up.hits),
            quiet,
            "a disabled refresh must not query the catalog periodically",
        )

        # A repeated initialize replaces the task instead of stacking a second.
        reads_before = self.up.hits[("GET", MODELS_PATH)]
        await plugin.initialize()
        self.assertTrue(plugin.ready, plugin.error_reason)
        self.assertEqual(plugin.error_reason, "")
        self.assertTrue(
            getattr(plugin.gateway, "bound_port", None),
            "the gateway must still be listening after a repeated initialize",
        )
        second = getattr(plugin, "_model_refresh_task", None)
        self.assertIsNotNone(second, "the re-initialized plugin must own a task again")
        self.assertIsNot(second, task)
        self.assertTrue(
            task.done(), "the previous refresh task must be cancelled and awaited"
        )
        self.assertEqual(
            self.up.hits[("GET", MODELS_PATH)],
            reads_before + 1,
            "each initialize primes the picker exactly once",
        )
        live = self._live_refresh_tasks()
        self.assertEqual(
            len(live), 1, "exactly one refresh task may survive: {!r}".format(live)
        )
        self.assertIs(live[0], second)

        before = {t for t in asyncio.all_tasks() if t is not asyncio.current_task()}
        await plugin.terminate()
        await plugin.terminate()
        await asyncio.sleep(0)
        self.assertFalse(plugin.ready)
        self.assertTrue(
            second.done(), "terminate must cancel and await the refresh task"
        )
        self.assertIsNone(plugin.session, "terminate must close the HTTP session")
        leaked = {
            t
            for t in asyncio.all_tasks()
            if t is not asyncio.current_task() and not t.done() and t not in before
        }
        self.assertFalse(leaked, "terminate leaked tasks: {!r}".format(leaked))

        # The disabled plugin tears down just as safely and never spawned one.
        await disabled.terminate()
        await disabled.terminate()
        self.assertIsNone(getattr(disabled, "_model_refresh_task", None))

    # =====================================================================
    # 10. After the baseline initialize the live catalog really changes and
    #     one short real interval must deliver the new options / labels while
    #     the operator's routing decision stays untouched everywhere.
    # =====================================================================
    async def test_short_loop_delivers_refreshed_catalog_and_keeps_routing(self):
        delta_model = "wb-delta"
        saved = [ALPHA, BETA]
        self._seed_credentials()
        plugin, config, cfg_path = await self._make_plugin(
            **self._picker_config(routing={"models": list(saved)})
        )
        self.assertTrue(plugin.ready, plugin.error_reason)
        await self._wait_models_read(1)
        baseline = self._options_map(config)
        self.assertEqual(set(baseline), {ALPHA, BETA, GAMMA})
        self.assertEqual(baseline[BETA], BETA_ZERO_LABEL)

        # A new fictional model appears, ALPHA's multiplier moves, and BETA
        # (selected) plus GAMMA (not selected) leave the catalog.
        self.catalog_entries.append(
            {"id": delta_model, "name": delta_model, "credits": 0.37, "cli": True}
        )
        for entry in self.catalog_entries:
            if entry["id"] == ALPHA:
                entry["credits"] = 0.42
        self.catalog_entries = [
            entry for entry in self.catalog_entries if entry["id"] not in (BETA, GAMMA)
        ]

        expected_alpha = _label(ALPHA, format_credits(0.42))

        def refreshed():
            options = self._options(config)
            labels = self._labels(config)
            if delta_model not in options or ALPHA not in options:
                return False
            return labels[options.index(ALPHA)] == expected_alpha

        before = dict(self.up.hits)
        short = await self._attach_short_loop(plugin)
        try:
            ok = await self._wait_until(refreshed, 5.0)
            self.assertTrue(
                ok, "the periodic loop never delivered the refreshed catalog/labels"
            )
            mapping = self._options_map(config)
            self.assertEqual(
                mapping.get(delta_model), _label(delta_model, format_credits(0.37))
            )
            self.assertEqual(mapping.get(ALPHA), expected_alpha)
            self.assertNotIn(GAMMA, mapping)
            self.assertIn(
                BETA,
                mapping,
                "a selected model missing from the refreshed catalog must be kept",
            )
            self.assertTrue(mapping[BETA].startswith(BETA))
            self.assertNotEqual(mapping[BETA], BETA_ZERO_LABEL)
            self.assertIn("未确认", mapping[BETA])
            self.assertFalse(short.done(), "the loop must keep running after a refresh")
        finally:
            await self._stop_short_loop(plugin, short)

        # The operator's own routing decision is untouched everywhere.
        self.assertEqual(config["routing"]["models"], saved)
        on_disk = json.loads(Path(cfg_path).read_text(encoding="utf-8-sig"))
        self.assertEqual(on_disk["routing"]["models"], saved)
        self.assertEqual(load_settings(config)["routing"]["models"], saved)
        self.assertEqual(plugin.router.status()["models"], saved)
        self.assertEqual(
            [value for value in self._options(config) if value in saved], saved
        )

        # Each interval is exactly one catalog read plus its /v3/config
        # supplement: no sign-in, chat or balance endpoint was touched.
        delta = {
            key: count - before.get(key, 0)
            for key, count in self.up.hits.items()
            if count != before.get(key, 0)
        }
        self.assertLessEqual(
            set(delta),
            {("GET", MODELS_PATH), ("GET", V3_CONFIG_PATH)},
            "the refresh must not touch another endpoint: {!r}".format(delta),
        )
        self.assertGreaterEqual(delta.get(("GET", MODELS_PATH), 0), 1)
        self.assertIn(
            delta.get(("GET", MODELS_PATH), 0)
            - delta.get(("GET", V3_CONFIG_PATH), 0),
            (0, 1),
            "each interval must reuse one catalog read plus its /v3/config "
            "supplement: {!r}".format(delta),
        )

    # =====================================================================
    # 11. A failed interval keeps the last good catalog and the plugin ready,
    #     keeps waiting (no retry storm) and recovers on a later interval.
    # =====================================================================
    async def test_failed_interval_keeps_catalog_and_recovers_on_success(self):
        delta_model = "wb-delta"
        self._seed_credentials()
        plugin, config, _ = await self._make_plugin(**self._picker_config())
        self.assertTrue(plugin.ready, plugin.error_reason)
        await self._wait_models_read(1)
        options_before = self._options(config)
        labels_before = self._labels(config)
        self.assertEqual(set(options_before), {ALPHA, BETA, GAMMA})

        self.models_failure = (
            429,
            {"code": 4290, "msg": "FICTIONAL-MODELS-RATE-LIMIT"},
            {"Retry-After": "30"},
        )

        short = await self._attach_short_loop(plugin, interval=0.1)
        try:
            attempts = self.up.hits[("GET", MODELS_PATH)]
            ok = await self._wait_until(
                lambda: self.up.hits[("GET", MODELS_PATH)] > attempts, 5.0
            )
            self.assertTrue(ok, "the periodic loop never attempted the catalog again")
            self.assertEqual(
                self._options(config),
                options_before,
                "a failed interval must keep the last good options",
            )
            self.assertEqual(self._labels(config), labels_before)
            self.assertTrue(
                plugin.ready, "a failed interval must not disable the plugin"
            )
            self.assertEqual(plugin.error_reason, "")
            self.assertFalse(short.done(), "a failed interval must not stop the loop")

            # No busy retry: about one attempt per interval, never a storm.
            attempts = self.up.hits[("GET", MODELS_PATH)]
            await asyncio.sleep(0.25)
            storm = self.up.hits[("GET", MODELS_PATH)] - attempts
            self.assertLessEqual(
                storm,
                4,
                "a failed interval must not retry immediately ({} attempts)".format(
                    storm
                ),
            )
            self.assertEqual(self._options(config), options_before)

            # The next successful interval publishes the real new catalog.
            self.catalog_entries.append(
                {"id": delta_model, "name": delta_model, "credits": 0.37, "cli": True}
            )
            self.models_failure = None
            ok = await self._wait_until(
                lambda: delta_model in self._options(config), 5.0
            )
            self.assertTrue(ok, "the loop never recovered on a successful interval")
            self.assertEqual(
                self._options_map(config).get(delta_model),
                _label(delta_model, format_credits(0.37)),
            )
            self.assertFalse(short.done())
            self.assertTrue(plugin.ready)
        finally:
            await self._stop_short_loop(plugin, short)

    # =====================================================================
    # 12. Without credentials several short real intervals pass with zero
    #     upstream work and the saved choices stay exactly as saved.
    # =====================================================================
    async def test_short_cycles_without_credentials_touch_no_upstream(self):
        saved = [GAMMA, BETA]
        plugin, config, cfg_path = await self._make_plugin(
            **self._picker_config(routing={"models": list(saved)})
        )
        self.assertTrue(plugin.ready, plugin.error_reason)
        self.assertFalse(
            plugin.auth.credentials, "this test needs the unauthenticated path"
        )
        options_before = self._options(config)
        labels_before = self._labels(config)
        self.assertEqual([value for value in options_before if value in saved], saved)

        short = await self._attach_short_loop(plugin)
        try:
            # Many real 0.02 s intervals, so more than two cycles really ran.
            await asyncio.sleep(0.2)
            self.assertFalse(
                short.done(), "the loop must keep running without credentials"
            )
        finally:
            await self._stop_short_loop(plugin, short)

        self.assertEqual(
            self.up.total_hits(),
            0,
            "an unauthenticated periodic refresh must never call upstream",
        )
        self.assertEqual(self._options(config), options_before)
        self.assertEqual(self._labels(config), labels_before)
        on_disk = json.loads(Path(cfg_path).read_text(encoding="utf-8-sig"))
        self.assertEqual(on_disk["routing"]["models"], saved)
        self.assertEqual(load_settings(config)["routing"]["models"], saved)
        self.assertEqual(plugin.router.status()["models"], saved)

    # =====================================================================
    # 13. Terminating while an interval is blocked inside a real catalog
    #     request must cancel promptly, close the session and ask no more.
    # =====================================================================
    async def test_terminate_cancels_a_blocked_refresh_and_closes_the_session(self):
        self._seed_credentials()
        plugin, config, _ = await self._make_plugin(**self._picker_config())
        self.assertTrue(plugin.ready, plugin.error_reason)
        await self._wait_models_read(1)

        gate = asyncio.Event()
        blocked = []

        async def _blocked_catalog(request):
            blocked.append(request.path)
            await gate.wait()
            return self._models_handler(request)

        clock = asyncio.get_running_loop()
        short = await self._attach_short_loop(plugin)
        self.up.handlers[("GET", MODELS_PATH)] = _blocked_catalog
        try:
            ok = await self._wait_until(lambda: bool(blocked), 5.0)
            self.assertTrue(ok, "the periodic loop never issued the catalog read")
            started = clock.time()
            await plugin.terminate()
            elapsed = clock.time() - started
            self.assertLess(
                elapsed, 2.0, "terminate must not wait for a blocked catalog read"
            )
            self.assertTrue(
                short.done(), "the blocked refresh task must be cancelled"
            )
            self.assertFalse(plugin.ready)
            self.assertIsNone(
                plugin.session, "the HTTP session must be closed on release"
            )
            attempts = len(blocked)
            reads = self.up.hits[("GET", MODELS_PATH)]
            await asyncio.sleep(0.15)
            self.assertEqual(
                self.up.hits[("GET", MODELS_PATH)],
                reads,
                "no further catalog request may follow terminate",
            )
            self.assertEqual(len(blocked), attempts)
        finally:
            gate.set()
            await self._stop_short_loop(plugin, short)
            self.up.handlers[("GET", MODELS_PATH)] = self._models_handler

    # =====================================================================
    # 14. ``wb status`` reports the real switch and the configured interval,
    #     without adding one upstream read to the normal overview.
    # =====================================================================
    async def test_status_reports_refresh_switch_and_interval(self):
        self._seed_credentials()
        plugin, config, _ = await self._make_plugin(
            **self._picker_config(
                model_refresh={"enabled": True, "interval_minutes": 90}
            )
        )
        self.assertTrue(plugin.ready, plugin.error_reason)
        await self._wait_models_read(1)

        before = dict(self.up.hits)
        text = support._text_of(
            await support._invoke(
                plugin.wb, support._make_event("wb status"), "status", ""
            )
        )
        delta = {
            key: count - before.get(key, 0)
            for key, count in self.up.hits.items()
            if count != before.get(key, 0)
        }
        self.assertEqual(
            delta,
            {
                ("GET", MODELS_PATH): 1,
                ("GET", V3_CONFIG_PATH): 1,
                ("POST", support.BALANCE_PATH): 1,
                ("GET", support.GROWTH_LIST_PATH): 1,
            },
            "wb status must not query anything beyond its normal model fetch: "
            "{!r}".format(delta),
        )

        enabled_block = self._refresh_status_block(text)
        self.assertIn(
            "90", enabled_block, "the configured interval must be displayed"
        )
        self.assertIn("启用", enabled_block)
        self.assertNotIn("未启用", enabled_block)

        disabled, _, _ = await self._make_plugin(
            **self._picker_config(
                model_refresh={"enabled": False, "interval_minutes": 2}
            )
        )
        self.assertTrue(disabled.ready, disabled.error_reason)
        disabled_text = support._text_of(
            await support._invoke(
                disabled.wb, support._make_event("wb status"), "status", ""
            )
        )
        disabled_block = self._refresh_status_block(disabled_text)
        self.assertIn("未启用", disabled_block)
        self.assertIn("2", disabled_block, "the configured interval must be displayed")
        self.assertNotIn(
            "60", disabled_block, "the configured interval must not be replaced"
        )


if __name__ == "__main__":
    unittest.main()
