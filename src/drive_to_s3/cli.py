from __future__ import annotations

import json

import click

from .manifest import load_manifest
from .pipeline import run


@click.group()
def main() -> None:
    """Google Drive -> S3 ingestion utilities."""


@main.command("run")
@click.option(
    "--config",
    "config_path",
    default="config/example.yml",
    show_default=True,
    help="Path to YAML config file",
)
def run_one(config_path: str) -> None:
    """Run a single ingestion pipeline defined by a YAML config."""
    summary = run(config_path)
    click.echo(json.dumps({k: v for k, v in summary.items() if k != "results"}, indent=2))


@main.command("run-manifest")
@click.option(
    "--manifest",
    "manifest_path",
    default="config/manifest.yml",
    show_default=True,
    help="Path to manifest YAML listing multiple pipeline configs",
)
def run_manifest(manifest_path: str) -> None:
    """Run multiple ingestion pipelines from a single manifest file."""
    entries = load_manifest(manifest_path)
    results = []
    failures = 0

    for e in entries:
        try:
            summary = run(e.config_path)
            results.append({"name": e.name, "config_path": e.config_path, "summary": summary})
        except Exception as ex:
            failures += 1
            results.append(
                {"name": e.name, "config_path": e.config_path, "error": str(ex), "status": "failed"}
            )

    click.echo(json.dumps({"pipelines": results, "failures": failures}, indent=2))


if __name__ == "__main__":
    main()
