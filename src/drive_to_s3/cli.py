from __future__ import annotations

import json

import click

from .pipeline import run


@click.command()
@click.option(
    "--config",
    "config_path",
    default="config/example.yml",
    show_default=True,
    help="Path to YAML config file",
)
def main(config_path: str) -> None:
    """Ingest files from Google Drive folder into S3."""
    summary = run(config_path)
    click.echo(json.dumps({k: v for k, v in summary.items() if k != "results"}, indent=2))


if __name__ == "__main__":
    main()
