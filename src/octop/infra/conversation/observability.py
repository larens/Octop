"""Read-only Router operations telemetry for Phase 5 hardening.

This module aggregates the Router governance tables (events, Context Packs,
budget ledgers, tasks/runs/approvals/receipts) into a bounded summary and an
operator health report.  It never writes to the database and never flips any
production switch; its job is to make the existing trace, budget and recovery
state observable from a single admin/CLI surface.
"""

from __future__ import annotations

import time
from typing import Any

from octop.infra.conversation.retention import DEFAULT_RETENTION_DAYS
from octop.infra.db.pool import DatabasePool

CONTEXT_PACK_P95_WARNING = 6_000
HEALTH_WINDOW_MINUTES = 60

BUDGET_BLOCKED_EVENT = "budget.blocked"
BUDGET_SHADOW_CHECKED_EVENT = "budget.shadow_checked"


def _percentile(values: list[int], fraction: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(len(ordered) - 1, lower + 1)
    weight = position - lower
    return int(ordered[lower] + (ordered[upper] - ordered[lower]) * weight)


def _enabled(raw: str | None) -> bool:
    if raw is None or not str(raw).strip():
        return False
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


class RouterObservabilityService:
    """Aggregate Router state for the ops dashboard / CLI.

    ``window_minutes`` controls the recent-time slices; timezone only affects
    how calendar day periods are presented, not the Unix-second math used by
    the ledger tables.
    """

    def __init__(
        self,
        db: DatabasePool,
        *,
        timezone_name: str = "Asia/Shanghai",
    ) -> None:
        self._db = db
        self._timezone = timezone_name or "Asia/Shanghai"

    def summary(
        self,
        *,
        window_minutes: int = 1_440,
        now: int | None = None,
    ) -> dict[str, Any]:
        timestamp = int(time.time()) if now is None else int(now)
        minutes = max(1, int(window_minutes))
        cutoff = timestamp - minutes * 60
        with self._db.connect() as conn:
            counts: dict[str, int] = {}
            for table, column in (
                ("conversation_events", "created_at"),
                ("conversation_tasks", "created_at"),
                ("task_runs", "created_at"),
                ("task_approvals", "created_at"),
                ("write_receipts", "created_at"),
                ("dispatch_plans", "created_at"),
                ("context_pack_manifests", "created_at"),
                ("budget_ledgers", "updated_at"),
            ):
                label = table.replace("conversation_events", "events").replace(
                    "context_pack_manifests", "context_packs"
                ).replace("conversation_tasks", "tasks").replace(
                    "task_runs", "runs"
                ).replace("task_approvals", "approvals").replace(
                    "write_receipts", "receipts"
                ).replace("dispatch_plans", "plans").replace(
                    "budget_ledgers", "budget_ledgers"
                )
                counts[f"{label}_window"] = conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {column} >= ?",
                    (cutoff,),
                ).fetchone()[0]
                counts[f"{label}_total"] = conn.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
            counts["main_conversations"] = conn.execute(
                "SELECT COUNT(*) FROM main_conversations"
            ).fetchone()[0]
            counts["usage_tokens_window"] = conn.execute(
                "SELECT COALESCE(SUM(total_tokens), 0) FROM usage_log WHERE ts >= ?",
                (cutoff,),
            ).fetchone()[0]
            context_tokens = [
                int(row[0])
                for row in conn.execute(
                    "SELECT used_tokens FROM context_pack_manifests "
                    "WHERE created_at >= ?",
                    (cutoff,),
                ).fetchall()
            ]
            schema_version = conn.execute(
                "SELECT MAX(version) FROM _schema_version"
            ).fetchone()[0]
            retention_days = {
                key: int(value)
                for key, value in self._retention_policy(
                    conn,
                ).items()
            }
            last_event_at = conn.execute(
                "SELECT MAX(created_at) FROM conversation_events"
            ).fetchone()[0]
            budget = self._budget_totals(conn, now=timestamp)
        return {
            "schema_version": schema_version,
            "timezone": self._timezone,
            "window_minutes": minutes,
            "now": timestamp,
            "cutoff_at": cutoff,
            "counts": counts,
            "context_pack_tokens": {
                "count": len(context_tokens),
                "p50": _percentile(context_tokens, 0.50),
                "p95": _percentile(context_tokens, 0.95),
                "max": max(context_tokens) if context_tokens else 0,
                "sum": sum(context_tokens),
            },
            "retention_days": retention_days,
            "last_event_at": last_event_at,
            "budget": budget,
        }

    def health(
        self,
        *,
        now: int | None = None,
    ) -> dict[str, Any]:
        timestamp = int(time.time()) if now is None else int(now)
        summary = self.summary(
            window_minutes=HEALTH_WINDOW_MINUTES,
            now=timestamp,
        )
        cutoff = summary["cutoff_at"]
        reasons: list[str] = []
        with self._db.connect() as conn:
            stale_runs = conn.execute(
                "SELECT COUNT(*) FROM task_runs "
                "WHERE status = 'running' AND lease_until IS NOT NULL "
                "AND lease_until <= ?",
                (timestamp,),
            ).fetchone()[0]
            abandoned_receipts = conn.execute(
                "SELECT COUNT(*) FROM write_receipts WHERE status = 'processing'"
            ).fetchone()[0]
            budget_over_limit = self._budget_totals(
                conn,
                now=timestamp,
            )["over_limit"]
            shadow_would_block = conn.execute(
                "SELECT COUNT(*) FROM conversation_events "
                "WHERE event_type = ? AND status = 'would_block' AND created_at >= ?",
                (BUDGET_SHADOW_CHECKED_EVENT, cutoff),
            ).fetchone()[0]
            budget_blocked = conn.execute(
                "SELECT COUNT(*) FROM conversation_events "
                "WHERE event_type = ? AND created_at >= ?",
                (BUDGET_BLOCKED_EVENT, cutoff),
            ).fetchone()[0]
            settings = {
                "worker_dispatch_enabled": self._setting(
                    conn,
                    "router.worker_dispatch.enabled",
                ),
                "side_effect_writes_enabled": self._setting(
                    conn,
                    "router.side_effect_writes.enabled",
                ),
                "budget_mode": self._setting(conn, "router.budget.mode")
                or "shadow",
            }
            for enabled_key in (
                "worker_dispatch_enabled",
                "side_effect_writes_enabled",
            ):
                if _enabled(settings[enabled_key]):
                    reasons.append(enabled_key.replace("_", ":"))
            if settings["budget_mode"] == "enforce":
                reasons.append("router:budget:enforce")
        if stale_runs:
            reasons.append("task_runs:stale_leases")
        if abandoned_receipts:
            reasons.append("write_receipts:processing")
        if budget_over_limit:
            reasons.append("budget_ledgers:over_limit")
        if shadow_would_block or budget_blocked:
            reasons.append("budget:blocked_recent")
        p95 = int(summary["context_pack_tokens"]["p95"])
        if p95 > CONTEXT_PACK_P95_WARNING:
            reasons.append("context_pack:p95_high")
        return {
            "status": "healthy" if not reasons else "warning",
            "reasons": reasons,
            "checked_at": timestamp,
            "window_minutes": HEALTH_WINDOW_MINUTES,
            "counts": {
                "stale_running_runs": stale_runs,
                "processing_write_receipts": abandoned_receipts,
                "budget_ledgers_over_limit": budget_over_limit,
                "shadow_would_block_recent": shadow_would_block,
                "budget_blocked_recent": budget_blocked,
                "context_pack_p95": p95,
                "context_pack_p95_warning_limit": CONTEXT_PACK_P95_WARNING,
            },
            "settings": settings,
            "schema_version": summary["schema_version"],
        }

    def _budget_totals(
        self,
        conn: Any,
        *,
        now: int,
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT "
            "  SUM(reserved_tokens) AS reserved_tokens, "
            "  SUM(spent_tokens) AS spent_tokens, "
            "  SUM(reserved_cost_micros) AS reserved_cost_micros, "
            "  SUM(spent_cost_micros) AS spent_cost_micros, "
            "  COUNT(*) AS ledger_rows "
            "FROM budget_ledgers WHERE period_start <= ? AND ? < period_end",
            (now, now),
        ).fetchone()
        reserved_tokens = int(row[0] or 0)
        spent_tokens = int(row[1] or 0)
        reserved_cost = int(row[2] or 0)
        spent_cost = int(row[3] or 0)
        over_limit = conn.execute(
            "SELECT COUNT(*) FROM budget_ledgers "
            "WHERE spent_tokens > limit_tokens OR "
            "spent_cost_micros > limit_cost_micros OR "
            "reserved_tokens < 0 OR spent_tokens < 0"
        ).fetchone()[0]
        return {
            "active_ledgers": int(row[4] or 0),
            "reserved_tokens": reserved_tokens,
            "spent_tokens": spent_tokens,
            "reserved_cost_micros": reserved_cost,
            "spent_cost_micros": spent_cost,
            "over_limit": int(over_limit),
        }

    def _retention_policy(self, conn: Any) -> dict[str, int]:
        stored = {
            str(r[0]).removeprefix("router.retention."): r[1]
            for r in conn.execute(
                "SELECT key, value FROM settings WHERE key LIKE 'router.retention.%'"
            ).fetchall()
        }
        policy = dict(DEFAULT_RETENTION_DAYS)
        policy.update(
            {
                key: int(value)
                for key, value in stored.items()
                if key in DEFAULT_RETENTION_DAYS
                and str(value).strip().lstrip("-").isdigit()
                and int(value) > 0
            }
        )
        return policy

    def _setting(self, conn: Any, key: str) -> str | None:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        if row is None:
            return None
        value = row[0]
        return str(value) if value is not None else None
