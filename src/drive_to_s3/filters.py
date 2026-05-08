from __future__ import annotations

import fnmatch
import os


def included(name: str, include_patterns: list[str], exclude_patterns: list[str]) -> bool:
    inc = any(fnmatch.fnmatch(name, p) for p in include_patterns) if include_patterns else True
    exc = any(fnmatch.fnmatch(name, p) for p in exclude_patterns) if exclude_patterns else False
    return inc and not exc


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)
