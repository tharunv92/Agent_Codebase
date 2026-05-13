from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

FROM_ENV_KEY = "DRIVE_TO_S3_MANIFEST"


@dataclass
class ManifestEntry:
    name: str
    config_path: str

    @staticmethod
    def from_dict(d: dict) -> "ManifestEntry":
        return ManifestEntry(name=str(d["name"]), config_path=str(d["config_path"]))


def load_manifest(manifest_path: str | None = None) -> list[ManifestEntry]:
    """Load a manifest of pipeline runs (configs).

    Manifest is metadata-driven: a single file can define multiple Drive->S3 pipelines
    (different folders/buckets/prefixes).

    Path precedence:
      1) function arg
      2) env var DRIVE_TO_S3_MANIFEST
      3) default config/manifest.yml
    """

    p = manifest_path or os.getenv(FROM_ENV_KEY) or "config/manifest.yml"
    path = Path(p)
    if not path.exists():
        raise FileNotFoundError(f"Manifest not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    entries = data.get("pipelines", [])
    return [ManifestEntry.from_dict(e) for e in entries]
