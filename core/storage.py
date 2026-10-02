"""Credential and state persistence for the WorkBuddy AstrBot plugin.

``CredentialStore`` encrypts the whole credential dict with AES-256-GCM using a
random 12-byte nonce and writes it atomically (same-directory temp file plus
``os.replace``).  ``StateStore`` keeps a small JSON document that is updated one
section at a time, also atomically.

Security / error contract:

* The master key must be standard base64 that decodes to exactly 32 bytes.
  An invalid key raises ``ValueError`` *before* any file is written.
* Every decryption or parse failure raises ``ValueError`` with a fixed message
  that never contains credential data.
* A failed atomic replace never removes the previously stored file.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_MAGIC = b"WBUD"
_VERSION = b"\x01"
_PREFIX = _MAGIC + _VERSION
_NONCE_BYTES = 12
_KEY_BYTES = 32

_ERR_KEY = "credential master key must be base64 of exactly 32 bytes"
_ERR_DECRYPT = "stored credentials could not be decrypted"
_ERR_PARSE = "stored credentials could not be parsed"
_ERR_STATE = "state file could not be parsed"

_CREDENTIAL_FILENAME = "credentials.enc"
_STATE_FILENAME = "state.json"


def _decode_master_key(master_key: str) -> bytes:
    """Decode and validate a base64 master key (exactly 32 bytes)."""
    try:
        raw = base64.b64decode(master_key, validate=True)
    except (binascii.Error, ValueError, TypeError):
        raise ValueError(_ERR_KEY) from None
    if len(raw) != _KEY_BYTES:
        raise ValueError(_ERR_KEY)
    return raw


def _atomic_write(directory: Path, target: Path, blob: bytes, prefix: str) -> None:
    """Write ``blob`` to ``target`` via a temp file + ``os.replace``.

    The temp file lives in the same directory (same filesystem) so the replace
    is atomic.  On any failure the temp file is removed and the original target
    is left untouched.
    """
    directory.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=prefix, suffix=".tmp", dir=str(directory))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(blob)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.chmod(tmp_name, 0o600)
        except OSError:
            pass
        os.replace(tmp_name, target)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


class CredentialStore:
    """Encrypted, atomically written credential store."""

    def __init__(self, data_dir: str | os.PathLike[str], master_key: str) -> None:
        self._dir = Path(data_dir)
        # Validate the key before touching the filesystem.
        self._key = _decode_master_key(master_key)
        self._aad = _PREFIX
        self._path = self._dir / _CREDENTIAL_FILENAME

    @property
    def path(self) -> Path:
        return self._path

    def save(self, credentials: dict) -> None:
        """Encrypt and atomically persist ``credentials``."""
        payload = json.dumps(
            credentials, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        nonce = os.urandom(_NONCE_BYTES)
        ciphertext = AESGCM(self._key).encrypt(nonce, payload, self._aad)
        blob = _PREFIX + nonce + ciphertext
        _atomic_write(self._dir, self._path, blob, ".wb-cred-")

    def load(self) -> dict | None:
        """Return the stored credentials, or ``None`` if none are stored."""
        try:
            raw = self._path.read_bytes()
        except FileNotFoundError:
            return None
        if not raw.startswith(_PREFIX) or len(raw) < len(_PREFIX) + _NONCE_BYTES:
            raise ValueError(_ERR_DECRYPT)
        nonce = raw[len(_PREFIX):len(_PREFIX) + _NONCE_BYTES]
        ciphertext = raw[len(_PREFIX) + _NONCE_BYTES:]
        try:
            plaintext = AESGCM(self._key).decrypt(nonce, ciphertext, self._aad)
        except Exception:
            raise ValueError(_ERR_DECRYPT) from None
        try:
            obj = json.loads(plaintext.decode("utf-8"))
        except Exception:
            raise ValueError(_ERR_PARSE) from None
        if not isinstance(obj, dict):
            raise ValueError(_ERR_PARSE)
        return obj

    def delete(self) -> None:
        """Remove stored credentials; a later ``load`` returns ``None``."""
        try:
            self._path.unlink()
        except FileNotFoundError:
            pass


class StateStore:
    """Small JSON document updated one section at a time, atomically."""

    def __init__(self, data_dir: str | os.PathLike[str]) -> None:
        self._dir = Path(data_dir)
        self._path = self._dir / _STATE_FILENAME

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> dict:
        """Return the state dict; an empty dict if nothing is stored yet."""
        try:
            raw = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return {}
        try:
            obj = json.loads(raw)
        except ValueError:
            raise ValueError(_ERR_STATE) from None
        if not isinstance(obj, dict):
            raise ValueError(_ERR_STATE)
        return obj

    def update(self, section: str, value: Any) -> None:
        """Set ``state[section] = value`` and persist atomically."""
        state = self.load()
        state[section] = value
        blob = json.dumps(
            state, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        _atomic_write(self._dir, self._path, blob, ".wb-state-")
