"""octop router-obs — read-only Router operations telemetry."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click

from octop.cli.support.db import open_cli_services
from octop.infra.conversation.observability import RouterObservabilityService


@click.group("router-obs")
def router_obs_group() -> None:
    """Router operations telemetry (summary and health, read-only)."""


def _emit(payload: dict[str, Any]) -> None:
    click.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@router_obs_group.command("summary")
@click.option(
    "--window-minutes",
    type=int,
    default=1_440,
    show_default=True,
    help="Recent window in minutes.",
)
@click.option(
    "--home",
    type=click.Path(path_type=Path),
    default=None,
    help="Octop home (default ~/.octop).",
)
def router_summary(window_minutes: int, home: Path | None) -> None:
    """Print aggregated Router counts, Context Pack tokens and budget totals."""
    with open_cli_services(home) as services:
        payload = RouterObservabilityService(services.db).summary(
            window_minutes=window_minutes,
        )
    _emit(payload)


@router_obs_group.command("health")
@click.option(
    "--home",
    type=click.Path(path_type=Path),
    default=None,
    help="Octop home (default ~/.octop).",
)
def router_health(home: Path | None) -> None:
    """Print Router health alerts for stale leases, writes and budgets."""
    with open_cli_services(home) as services:
        payload = RouterObservabilityService(services.db).health()
    _emit(payload)
