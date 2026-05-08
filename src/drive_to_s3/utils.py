from __future__ import annotations

import hashlib
from dataclasses import dataclass


def sha256_file(path: str, chunk_size: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def normalize_s3_prefix(prefix: str) -> str:
    if not prefix:
        return ""
    p = prefix.strip("/")
    return f"{p}/" if p else ""


def safe_join_s3_key(prefix: str, key: str) -> str:
    prefix = normalize_s3_prefix(prefix)
    key = key.lstrip("/")
    return f"{prefix}{key}" if prefix else key


@dataclass(frozen=True)
class DriveFileFingerprint:
    file_id: str
    modified_time: str | None
    size: int | None

    def stable_key(self) -> str:
        # usable for state map keys
        return self.file_id
