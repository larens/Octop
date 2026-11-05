#!/usr/bin/env python3
"""Tests for the Phase 5 read-only Router operations telemetry service."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from octop.infra.conversation.observability import RouterObservabilityService
from octop.infra.conversation.service import ConversationService
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.users import UserRepo

BASE = 1_800_000_000


class RouterObservabilityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "octop.db"
        self.db = SqlitePool(self.db_path)
        run_migrations(self.db)
        UserRepo(self.db).create(username="alice", password_hash="h", role="user")
        AgentRepo(self.db).create(agent_id="agent-a", user_id=1, name="Agent A")
        self.conversation = ConversationService(self.db)
        self.main = self.conversation.ensure_main_conversation(
            agent_id="agent-a",
            user_id=1,
        )
        self.conv_id = str(self.main["conversation_id"])
        self.service = RouterObservabilityService(self.db, timezone_name="Asia/Shanghai")

    def tearDown(self) -> None:
        self.db.close()
        self.temp.cleanup()

    def _insert_task_and_run(self, *, run_status: str = "succeeded") -> None:
        with self.db.connect() as conn:
            conn.execute(
                "INSERT INTO conversation_tasks("
                "task_id, conversation_id, task_type, title, subject_key, "
                "objective, status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "task-1",
                    self.conv_id,
                    "research",
                    "title",
                    "subject",
                    "objective",
                    "done",
                    BASE - 100,
                    BASE - 100,
                ),
            )
            conn.execute(
                "INSERT INTO task_runs("
                "run_id, task_id, status, attempt_no, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                ("run-1", "task-1", run_status, 1, BASE - 100),
            )

    def _insert_approval_receipt(self) -> None:
        with self.db.connect() as conn:
            conn.execute(
                "INSERT INTO task_approvals("
                "approval_id, conversation_id, task_id, trace_id, action_name, "
                "action_json, action_hash, idempotency_key, status, token_limit, "
                "expires_at, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "approval-1",
                    self.conv_id,
                    "task-1",
                    "trace-1",
                    "email.send",
                    '{"to":"owner@example.com"}',
                    "a" * 64,
                    "idem-1",
                    "consumed",
                    8_000,
                    BASE + 1_000,
                    BASE - 100,
                    BASE - 100,
                ),
            )
            conn.execute(
                "INSERT INTO write_receipts("
                "receipt_id, conversation_id, task_id, run_id, trace_id, attempt_no, "
                "approval_id, action_name, action_hash, idempotency_key, status, "
                "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "receipt-1",
                    self.conv_id,
                    "task-1",
                    "run-1",
                    "trace-1",
                    1,
                    "approval-1",
                    "email.send",
                    "a" * 64,
                    "idem-receipt-1",
                    "succeeded",
                    BASE - 100,
                    BASE - 100,
                ),
            )

    def _insert_dispatch_plan(self) -> None:
        with self.db.connect() as conn:
            conn.execute(
                "INSERT INTO dispatch_plans("
                "plan_id, conversation_id, trace_id, objective, status, plan_hash, "
                "estimated_tokens, hard_token_limit, hard_cost_limit_micros, "
                "cost_per_1k_tokens_micros, created_at, proposed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "plan-1",
                    self.conv_id,
                    "trace-1",
                    "objective",
                    "draft",
                    "h" * 64,
                    1_000,
                    8_000,
                    0,
                    0,
                    BASE - 100,
                    BASE - 100,
                ),
            )

    def _insert_context_packs(self, tokens: list[int]) -> None:
        with self.db.connect() as conn:
            for i, used in enumerate(tokens):
                conn.execute(
                    "INSERT INTO context_pack_manifests("
                    "pack_id, conversation_id, trace_id, policy_version, model, "
                    "used_tokens, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        f"pack-{i}",
                        self.conv_id,
                        f"trace-pack-{i}",
                        "v1",
                        "test-model",
                        used,
                        BASE - (i * 10) - 50,
                    ),
                )

    def _insert_ledger(
        self,
        *,
        reserved: int = 0,
        spent: int = 0,
        limit: int = 8_000,
        spent_cost: int = 0,
        limit_cost: int = 100_000,
    ) -> None:
        with self.db.connect() as conn:
            conn.execute(
                "INSERT INTO budget_ledgers("
                "scope_type, scope_id, period_start, period_end, reserved_tokens, "
                "spent_tokens, reserved_cost_micros, spent_cost_micros, limit_tokens, "
                "limit_cost_micros, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "task_daily",
                    "task-1",
                    BASE - 1_000,
                    BASE + 1_000,
                    reserved,
                    spent,
                    0,
                    spent_cost,
                    limit,
                    limit_cost,
                    BASE - 100,
                ),
            )

    def _insert_usage(self, total: int) -> None:
        with self.db.connect() as conn:
            conn.execute(
                "INSERT INTO usage_log(ts, agent_id, user_id, total_tokens) "
                "VALUES (?, ?, ?, ?)",
                (BASE - 100, "agent-a", 1, total),
            )

    def _insert_event(
        self,
        *,
        event_id: str,
        event_type: str,
        status: str | None,
        trace_id: str = "trace-1",
        seq: int,
        created_at: int,
    ) -> None:
        with self.db.connect() as conn:
            conn.execute(
                "INSERT INTO conversation_events("
                "event_id, conversation_id, trace_id, seq, event_type, status, "
                "created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    event_id,
                    self.conv_id,
                    trace_id,
                    seq,
                    event_type,
                    status,
                    created_at,
                ),
            )

    def _set_setting(self, key: str, value: str) -> None:
        with self.db.connect() as conn:
            conn.execute(
                "INSERT INTO settings(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def test_summary_aggregates_counts_budget_and_context_packs(self) -> None:
        self._insert_task_and_run()
        self._insert_approval_receipt()
        self._insert_dispatch_plan()
        self._insert_context_packs([3_000, 9_000])
        self._insert_ledger(reserved=5_000, spent=1_000, limit=8_000)
        self._insert_usage(total=42)
        self._insert_event(
            event_id="evt-received",
            event_type="router.received",
            status="ok",
            seq=1,
            created_at=BASE - 100,
        )

        summary = self.service.summary(window_minutes=1_440, now=BASE)
        counts = summary["counts"]
        self.assertEqual(counts["events_window"], 1)
        self.assertEqual(counts["events_total"], 1)
        self.assertEqual(counts["tasks_window"], 1)
        self.assertEqual(counts["runs_window"], 1)
        self.assertEqual(counts["approvals_window"], 1)
        self.assertEqual(counts["receipts_window"], 1)
        self.assertEqual(counts["plans_window"], 1)
        self.assertEqual(counts["context_packs_window"], 2)
        self.assertEqual(counts["main_conversations"], 1)
        self.assertEqual(counts["usage_tokens_window"], 42)

        # Migrations are numbered per-environment (repo 025-028, blue-green 029-034).
        self.assertGreaterEqual(summary["schema_version"], 28)
        self.assertEqual(summary["context_pack_tokens"]["count"], 2)
        self.assertEqual(summary["context_pack_tokens"]["p50"], 6_000)
        self.assertEqual(summary["context_pack_tokens"]["p95"], 8_700)
        self.assertEqual(summary["context_pack_tokens"]["max"], 9_000)
        self.assertEqual(summary["context_pack_tokens"]["sum"], 12_000)

        self.assertEqual(summary["budget"]["active_ledgers"], 1)
        self.assertEqual(summary["budget"]["reserved_tokens"], 5_000)
        self.assertEqual(summary["budget"]["spent_tokens"], 1_000)
        self.assertEqual(summary["budget"]["reserved_cost_micros"], 0)
        self.assertEqual(summary["budget"]["spent_cost_micros"], 0)
        self.assertEqual(summary["budget"]["over_limit"], 0)

    def test_budget_totals_span_active_and_over_limit_ledgers(self) -> None:
        self._insert_ledger(spent=9_000, limit=8_000)
        budget = self.service.summary(window_minutes=1_440, now=BASE)["budget"]
        self.assertEqual(budget["active_ledgers"], 1)
        self.assertEqual(budget["over_limit"], 1)
        self.assertEqual(budget["spent_tokens"], 9_000)

    def test_health_detects_stale_leases_and_processing_receipts(self) -> None:
        self._insert_task_and_run(run_status="running")
        self._insert_approval_receipt()
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE task_runs SET lease_until = ? WHERE run_id = 'run-1'",
                (BASE - 60,),
            )
            conn.execute(
                "UPDATE write_receipts SET status = 'processing' "
                "WHERE receipt_id = 'receipt-1'"
            )

        health = self.service.health(now=BASE)
        self.assertEqual(health["status"], "warning")
        self.assertIn("task_runs:stale_leases", health["reasons"])
        self.assertIn("write_receipts:processing", health["reasons"])
        self.assertEqual(health["counts"]["stale_running_runs"], 1)
        self.assertEqual(health["counts"]["processing_write_receipts"], 1)

    def test_health_flags_budget_over_limit(self) -> None:
        self._insert_ledger(spent=9_000, limit=8_000)
        health = self.service.health(now=BASE)
        self.assertEqual(health["status"], "warning")
        self.assertIn("budget_ledgers:over_limit", health["reasons"])
        self.assertEqual(health["counts"]["budget_ledgers_over_limit"], 1)

    def test_health_flags_recent_shadow_would_block_and_budget_blocked(self) -> None:
        self._insert_event(
            event_id="evt-shadow",
            event_type="budget.shadow_checked",
            status="would_block",
            seq=1,
            created_at=BASE - 50,
        )
        self._insert_event(
            event_id="evt-blocked",
            event_type="budget.blocked",
            status="blocked",
            seq=2,
            created_at=BASE - 40,
        )
        health = self.service.health(now=BASE)
        self.assertEqual(health["status"], "warning")
        self.assertIn("budget:blocked_recent", health["reasons"])
        self.assertEqual(health["counts"]["shadow_would_block_recent"], 1)
        self.assertEqual(health["counts"]["budget_blocked_recent"], 1)

    def test_health_flags_production_switches(self) -> None:
        self._set_setting("router.worker_dispatch.enabled", "1")
        self._set_setting("router.side_effect_writes.enabled", "1")
        self._set_setting("router.budget.mode", "enforce")
        health = self.service.health(now=BASE)
        self.assertEqual(health["status"], "warning")
        self.assertIn("worker:dispatch:enabled", health["reasons"])
        self.assertIn("side:effect:writes:enabled", health["reasons"])
        self.assertIn("router:budget:enforce", health["reasons"])
        self.assertEqual(health["settings"]["budget_mode"], "enforce")

    def test_health_warns_on_context_pack_p95_over_limit(self) -> None:
        self._insert_context_packs([6_000, 7_000, 8_000])
        health = self.service.health(now=BASE)
        self.assertEqual(health["status"], "warning")
        self.assertIn("context_pack:p95_high", health["reasons"])
        self.assertGreater(health["counts"]["context_pack_p95"], 6_000)

    def test_health_is_healthy_with_no_issues(self) -> None:
        self._insert_task_and_run()
        health = self.service.health(now=BASE)
        self.assertEqual(health["status"], "healthy")
        self.assertEqual(health["reasons"], [])

    def test_retention_policy_merges_settings_overrides(self) -> None:
        self._set_setting("router.retention.event_days", "10")
        self._set_setting("router.retention.task_run_days", "20")
        summary = self.service.summary(window_minutes=1_440, now=BASE)
        self.assertEqual(summary["retention_days"]["event_days"], 10)
        self.assertEqual(summary["retention_days"]["task_run_days"], 20)
        # Non-overridden defaults are preserved.
        self.assertEqual(summary["retention_days"]["context_pack_days"], 90)

    def test_settings_reflect_default_budget_mode(self) -> None:
        self._set_setting("router.budget.mode", "")
        health = self.service.health(now=BASE)
        self.assertEqual(health["settings"]["budget_mode"], "shadow")
        self.assertIsNone(health["settings"]["worker_dispatch_enabled"])
        self.assertIsNone(health["settings"]["side_effect_writes_enabled"])
