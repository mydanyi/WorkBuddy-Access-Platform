"""Behavior tests for WorkBuddy credential/state storage.

Scope: only the documented public contract of
``astrbot_plugin_workbuddy.core.storage`` (``CredentialStore`` / ``StateStore``).

All credentials and master keys below are explicitly FICTIONAL FIXED SAMPLES.
They are not real account data and are never read from any live source
(no real accounts, no ``.codebuddy/models.json``).

The production module does not exist yet on purpose: the top-level import is
expected to fail until ``core/storage`` is implemented and verified remotely
in the Docker host. Do not "fix" that here.
"""

import base64
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

# --- Make ``astrbot_plugin_workbuddy`` importable when discovered from anywhere.
_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]  # directory that contains astrbot_plugin_workbuddy/
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.core.storage import CredentialStore, StateStore  # noqa: E402


def _resolve_base_tmp() -> Path:
    """Temp root: env override, else the current task's NAS tmp dir.

    The system temp dir is deliberately NOT used as a fallback.
    """
    env = os.environ.get("WORKBUDDY_TEST_TMP")
    if env:
        return Path(env)
    # _HERE = <task>/source/astrbot_plugin_workbuddy/tests/test_storage.py
    task_root = _HERE.parents[3]
    return task_root / "tmp"


BASE_TMP = _resolve_base_tmp()

# --- FICTIONAL FIXED SAMPLE KEYS (standard base64 of exactly 32 bytes). ---
_KEY_A_BYTES = b"workbuddy-test-master-key-000001"
_KEY_B_BYTES = b"workbuddy-test-master-key-000002"
if len(_KEY_A_BYTES) != 32 or len(_KEY_B_BYTES) != 32:
    raise AssertionError("sample master keys must be exactly 32 bytes")

MASTER_KEY_A = base64.b64encode(_KEY_A_BYTES).decode("ascii")
MASTER_KEY_B = base64.b64encode(_KEY_B_BYTES).decode("ascii")

# --- FICTIONAL FIXED SAMPLE CREDENTIALS (never real tokens). ---
ACCESS_TOKEN_LITERAL = "FICTIONAL-ACCESS-TOKEN-a1b2c3d4e5f6"
REFRESH_TOKEN_LITERAL = "FICTIONAL-REFRESH-TOKEN-f6e5d4c3b2a1"
UID_LITERAL = "test-uid-0000000000"
DOMAIN_LITERAL = "example.test.invalid"
REALM_LITERAL = "workbuddy-test-realm"

SAMPLE_CREDENTIALS = {
    "accessToken": ACCESS_TOKEN_LITERAL,
    "refreshToken": REFRESH_TOKEN_LITERAL,
    "uid": UID_LITERAL,
    "expiresAt": 4102444800000,  # 2100-01-01 in ms, fictional
    "domain": DOMAIN_LITERAL,
    "realm": REALM_LITERAL,
}

# Plaintext that must never appear in on-disk ciphertext or in error messages.
PLAINTEXT_LITERALS = (
    ACCESS_TOKEN_LITERAL,
    REFRESH_TOKEN_LITERAL,
    UID_LITERAL,
    DOMAIN_LITERAL,
    REALM_LITERAL,
)


class _StorageTestCase(unittest.TestCase):
    """Provides an isolated, NAS-hosted temporary data dir per test."""

    def setUp(self) -> None:
        BASE_TMP.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(
            tempfile.mkdtemp(prefix="wb-test-", dir=str(BASE_TMP))
        )
        self.addCleanup(shutil.rmtree, self.data_dir, ignore_errors=True)

    def credential_file(self) -> Path:
        return self.data_dir / "credentials.enc"

    def state_file(self) -> Path:
        return self.data_dir / "state.json"

    def assertNoSecretLeak(self, exc: BaseException) -> None:
        """The exception must not carry any plaintext credential fragment."""
        text = "{}\n{}".format(exc, repr(exc))
        for secret in PLAINTEXT_LITERALS:
            self.assertNotIn(secret, text)


class CredentialStoreTests(_StorageTestCase):
    def test_credentials_roundtrip_full(self) -> None:
        """1. Full credential set saves and reads back unchanged."""
        store = CredentialStore(self.data_dir, MASTER_KEY_A)
        store.save(dict(SAMPLE_CREDENTIALS))

        loaded = store.load()
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded, SAMPLE_CREDENTIALS)
        for field in (
            "accessToken",
            "refreshToken",
            "uid",
            "expiresAt",
            "domain",
            "realm",
        ):
            self.assertIn(field, loaded)
        self.assertTrue(self.credential_file().is_file())

    def test_ciphertext_does_not_contain_plaintext(self) -> None:
        """2. On-disk ciphertext hides the plaintext tokens (literal check)."""
        store = CredentialStore(self.data_dir, MASTER_KEY_A)
        store.save(dict(SAMPLE_CREDENTIALS))

        raw = self.credential_file().read_bytes()
        for secret in PLAINTEXT_LITERALS:
            self.assertNotIn(secret.encode("utf-8"), raw)

    def test_second_save_changes_ciphertext_and_roundtrips(self) -> None:
        """3. Re-saving identical creds yields different ciphertext, still reads."""
        store = CredentialStore(self.data_dir, MASTER_KEY_A)
        store.save(dict(SAMPLE_CREDENTIALS))
        first = self.credential_file().read_bytes()

        store.save(dict(SAMPLE_CREDENTIALS))
        second = self.credential_file().read_bytes()

        self.assertNotEqual(first, second)
        self.assertEqual(store.load(), SAMPLE_CREDENTIALS)

    def test_wrong_master_key_fails_without_leaking(self) -> None:
        """4. A different valid key cannot decrypt, and leaks nothing."""
        CredentialStore(self.data_dir, MASTER_KEY_A).save(dict(SAMPLE_CREDENTIALS))

        wrong = CredentialStore(self.data_dir, MASTER_KEY_B)
        with self.assertRaises(ValueError) as ctx:
            wrong.load()
        self.assertNoSecretLeak(ctx.exception)

    def test_tampered_ciphertext_fails_without_leaking(self) -> None:
        """5. Flipping one ciphertext byte makes load fail, without leaks."""
        store = CredentialStore(self.data_dir, MASTER_KEY_A)
        store.save(dict(SAMPLE_CREDENTIALS))

        path = self.credential_file()
        data = bytearray(path.read_bytes())
        self.assertTrue(data, "credential file must not be empty")
        data[-1] ^= 0x01
        path.write_bytes(bytes(data))

        with self.assertRaises(ValueError) as ctx:
            CredentialStore(self.data_dir, MASTER_KEY_A).load()
        self.assertNoSecretLeak(ctx.exception)

    def test_invalid_master_key_rejected_without_file(self) -> None:
        """6. Invalid base64 / non-32-byte keys raise and write no file."""
        bad_keys = (
            "not valid base64 !!!",  # invalid base64
            base64.b64encode(b"too-short-16byte").decode("ascii"),  # 16 bytes
            base64.b64encode(b"x" * 64).decode("ascii"),  # 64 bytes
        )
        for bad_key in bad_keys:
            with self.assertRaises(ValueError):
                CredentialStore(self.data_dir, bad_key)
            self.assertFalse(
                self.credential_file().exists(),
                msg="invalid key must not create credentials.enc",
            )

    def test_new_store_reads_and_delete_returns_none(self) -> None:
        """7. Files are portable across instances; delete() clears to None."""
        CredentialStore(self.data_dir, MASTER_KEY_A).save(dict(SAMPLE_CREDENTIALS))

        reopened = CredentialStore(self.data_dir, MASTER_KEY_A)
        self.assertEqual(reopened.load(), SAMPLE_CREDENTIALS)

        reopened.delete()
        self.assertIsNone(reopened.load())

    def test_failed_save_preserves_original_file(self) -> None:
        """10. A failed atomic replace keeps the previous valid file intact."""
        store = CredentialStore(self.data_dir, MASTER_KEY_A)
        store.save(dict(SAMPLE_CREDENTIALS))
        path = self.credential_file()
        original = path.read_bytes()

        mutated = dict(SAMPLE_CREDENTIALS)
        mutated["accessToken"] = "FICTIONAL-ACCESS-TOKEN-changed-9999"

        # Patch only the filesystem boundary (os.replace), not store internals.
        with mock.patch("os.replace", side_effect=OSError("simulated replace failure")):
            with self.assertRaises((OSError, ValueError)):
                store.save(mutated)

        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(
            CredentialStore(self.data_dir, MASTER_KEY_A).load(),
            SAMPLE_CREDENTIALS,
        )


class StateStoreTests(_StorageTestCase):
    def test_state_sections_preserved_across_updates_and_instances(self) -> None:
        """8. Updating one section keeps the others and survives a new instance."""
        store = StateStore(self.data_dir)
        self.assertEqual(store.load(), {})  # first load -> empty dict

        signin = {"days": 3, "lastDate": "2026-10-01"}
        growth = {"points": 42, "level": 5}

        store.update("signin", dict(signin))
        store.update("growth", dict(growth))

        state = store.load()
        self.assertEqual(state.get("signin"), signin)
        self.assertEqual(state.get("growth"), growth)

        restored = StateStore(self.data_dir).load()
        self.assertEqual(restored.get("signin"), signin)
        self.assertEqual(restored.get("growth"), growth)

    def test_corrupted_state_json_raises(self) -> None:
        """9. Corrupt state JSON must raise, never silently become {}."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_file().write_bytes(b"{ this is not valid json")

        store = StateStore(self.data_dir)
        with self.assertRaises(ValueError):
            store.load()


if __name__ == "__main__":
    unittest.main()
