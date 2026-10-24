"""Object storage behind one interface, held by the agent and the runner.

``LocalStore`` keeps objects as files under a root directory (``file://`` URIs).
Cloud stores (S3, Blob Storage, Cloud Storage) implement the same interface;
their grants are presigned URLs the agent mints with the organization's own
identity.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol
from urllib.parse import quote, unquote, urlparse

from .contracts.agent import Grant
from .contracts.manifests import check_safe_path


class ObjectExists(Exception):
    """A conditional create found the object already present."""


class StorageBackend(Protocol):
    def put_file(self, key: str, source: Path, *, if_absent: bool = False) -> None: ...
    def get_file(self, key: str, destination: Path) -> None: ...
    def stat(self, key: str) -> tuple[int, str] | None: ...
    def exists(self, key: str) -> bool: ...
    def delete(self, key: str) -> None: ...
    def grant(self, key: str, method: str, expires_in: int) -> Grant: ...


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


class LocalStore:
    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_uri(cls, uri: str) -> LocalStore:
        parsed = urlparse(uri)
        if parsed.scheme != "file":
            raise ValueError(f"LocalStore needs a file:// URI, got {uri!r}")
        return cls(unquote(parsed.path))

    def path(self, key: str) -> Path:
        check_safe_path(key)
        target = (self.root / key).resolve()
        if self.root not in target.parents:
            raise ValueError(f"object key {key!r} escapes the store")
        return target

    def put_file(self, key: str, source: Path, *, if_absent: bool = False) -> None:
        """Write atomically; with ``if_absent`` an existing object is never replaced."""
        target = self.path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=".put-")
        os.close(fd)
        try:
            shutil.copyfile(source, tmp)
            if if_absent:
                _create_if_absent(tmp, target, key)
            else:
                os.replace(tmp, target)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def get_file(self, key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self.path(key), destination)

    def stat(self, key: str) -> tuple[int, str] | None:
        target = self.path(key)
        if not target.is_file():
            return None
        return target.stat().st_size, sha256_file(target)

    def exists(self, key: str) -> bool:
        return self.path(key).is_file()

    def delete(self, key: str) -> None:
        target = self.path(key)
        if target.exists():
            target.unlink()

    def grant(self, key: str, method: str, expires_in: int) -> Grant:
        # Local grants are paths: they work only where the store's directory is
        # reachable, which is the local-pool deployment's premise.
        target = self.path(key)
        if method == "PUT":
            target.parent.mkdir(parents=True, exist_ok=True)
        return Grant(key=key, method="file", url="file://" + quote(str(target)),
                     expires_at=datetime.now(timezone.utc) + timedelta(seconds=expires_in))


def _create_if_absent(tmp: str, target: Path, key: str) -> None:
    """Publish ``tmp`` at ``target`` only if nothing is there, atomically."""
    try:
        os.link(tmp, target)
        return
    except FileExistsError:
        raise ObjectExists(key) from None
    except OSError:
        pass  # this filesystem has no hard links; claim the name instead
    try:
        fd = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        raise ObjectExists(key) from None
    os.close(fd)
    # The empty placeholder is replaced in one step; readers check checksums anyway.
    os.replace(tmp, target)


def open_store(uri: str) -> StorageBackend:
    if uri.startswith("file://"):
        return LocalStore.from_uri(uri)
    raise ValueError(f"no storage backend for {uri!r} in this release; file:// is supported")
