"""Object storage uniforme para desenvolvimento local e cluster."""

from __future__ import annotations

import os
import posixpath
import tempfile
from pathlib import Path
from typing import Union

import fsspec


StorageLocation = Union[Path, str]


def artifact_root(namespace: str, local_fallback: str | Path) -> StorageLocation:
    """Resolve um namespace sem acoplar o código ao bucket definitivo."""
    base = os.environ.get("PGFN_ARTIFACT_STORE_URI", "").strip().rstrip("/")
    if not base:
        return Path(local_fallback)
    return f"{base}/{namespace.strip('/')}"


def storage_child(root: StorageLocation, *parts: str) -> StorageLocation:
    if isinstance(root, Path):
        return root.joinpath(*parts)
    suffix = "/".join(part.strip("/") for part in parts)
    return f"{root.rstrip('/')}/{suffix}"


class ObjectStorage:
    """Pequeno adaptador de bytes sobre qualquer filesystem do fsspec."""

    def __init__(self, root: StorageLocation) -> None:
        raw_root = str(root)
        self._fs, self._root = fsspec.core.url_to_fs(raw_root)
        protocol = self._fs.protocol
        if isinstance(protocol, (tuple, list)):
            protocol = protocol[0]
        self._is_local = protocol in {None, "", "file", "local"}

    def _path(self, key: str) -> str:
        clean = key.replace("\\", "/").lstrip("/")
        return posixpath.join(self._root.rstrip("/"), clean)

    def location(self, key: str) -> StorageLocation:
        path = self._path(key)
        if self._is_local:
            return Path(path)
        return str(self._fs.unstrip_protocol(path))

    def exists(self, key: str) -> bool:
        return bool(self._fs.isfile(self._path(key)))

    def read_bytes(self, key: str) -> bytes:
        with self._fs.open(self._path(key), "rb") as handle:
            return handle.read()

    def write_bytes(self, key: str, content: bytes) -> StorageLocation:
        destination = self._path(key)
        parent = posixpath.dirname(destination)
        self._fs.makedirs(parent, exist_ok=True)
        if self._is_local:
            self._atomic_local_write(Path(destination), content)
        else:
            # Upload de objeto é publicado pelo backend como uma nova geração;
            # não removemos a versão anterior antes de concluir a escrita.
            self._fs.pipe_file(destination, content)
        return self.location(key)

    @staticmethod
    def _atomic_local_write(destination: Path, content: bytes) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary_name = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix=f".{destination.name}.",
                suffix=".tmp",
                dir=destination.parent,
                delete=False,
            ) as temporary:
                temporary.write(content)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_name = temporary.name
            os.replace(temporary_name, destination)
        finally:
            if temporary_name and os.path.exists(temporary_name):
                os.unlink(temporary_name)
