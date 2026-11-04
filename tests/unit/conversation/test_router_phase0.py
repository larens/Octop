"""Router Phase 0 persistence, context, trace, and budget governance."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from octop.infra.conversation.budget import RouterBudgetGate
from octop.infra.conversation.context import compile_context_pack, estimate_text_tokens
from octop.infra.conversation.evaluation import (
    ClassificationSample,
    evaluate_classifier,
    evaluate_low_confidence_with_llm,
)
from octop.infra.conversation.retention import run_retention
from octop.infra.conversation.service import (
    BudgetExceededError,
    ConversationService,
    InvalidTransitionError,
    NotFoundError,
    RetryLimitExceededError,
    RouterPhase0Error,
)
from octop.infra.conversation.shadow import (
    _memory_candidates_from_payload,
    _retrieve_memory_candidates,
    classify_shadow_route,
    observe_shadow_turn,
)
from octop.infra.cron.delivery import CronDeliveryService
from octop.infra.cron.job import CronJob
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.threads import ThreadRepo
from octop.infra.db.repos.users import UserRepo

ROUTER_ENV_KEYS = (
    "OCTOP_ROUTER_BUDGET_MODE",
    "OCTOP_ROUTER_BUDGET_ENFORCED_SURFACES",
    "OCTOP_ROUTER_BUDGET_USER_DAILY_TOKENS",
    "OCTOP_ROUTER_BUDGET_AGENT_DAILY_TOKENS",
    "OCTOP_ROUTER_BUDGET_GLOBAL_DAILY_TOKENS",
    "OCTOP_ROUTER_BUDGET_TASK_DAILY_TOKENS",
    "OCTOP_ROUTER_BUDGET_COST_PER_1K_TOKENS_MICROS",
)


class MemorySettings:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = dict(values)

    def get(self, key: str) -> str | None:
        return self.values.get(key)


class FakeThreadMessages:
    def __init__(self, rows: list[object] | None = None) -> None:
        self.rows = list(rows or [])

    def page(self, _thread_id: str, limit: int = 100) -> tuple[list[object], bool]:
        return self.rows[:limit], False


@pytest.fixture
def db(tmp_path: Path) -> SqlitePool:
    pool = SqlitePool(tmp_path / "octop.db")
    run_migrations(pool)
    users = UserRepo(pool)
    users.create(username="alice", password_hash="h", role="user")
    users.create(username="bob", password_hash="h", role="user")
    agents = AgentRepo(pool)
    agents.create(agent_id="agent-a", user_id=1, name="Agent A")
    agents.create(agent_id="agent-b", user_id=2, name="Agent B")
    threads = ThreadRepo(pool)
    threads.insert(
        thread_id="old-dashboard",
        agent_id="agent-a",
        user_id=1,
        channel_type="dashboard",
        session_key="agent-a:dashboard:1:dm",
        last_active=100,
    )
    threads.insert(
        thread_id="new-dashboard",
        agent_id="agent-a",
        user_id=1,
        channel_type="dashboard",
        session_key="agent-a:dashboard:1:dm",
        last_active=200,
    )
    yield pool
    pool.close()


@pytest.fixture(autouse=True)
def clean_router_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ROUTER_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _conversation(
    service: ConversationService,
    *,
    agent_id: str = "agent-a",
    user_id: int = 1,
    router_thread_id: str | None = None,
) -> dict[str, object]:
    return service.ensure_main_conversation(
        agent_id=agent_id,
        user_id=user_id,
        router_thread_id=router_thread_id,
    )


def test_v24_upgrade_backfills_latest_dashboard_thread(tmp_path: Path) -> None:
    pool = SqlitePool(tmp_path / "upgrade.db")
    run_migrations(pool)
    UserRepo(pool).create(username="alice", password_hash="h", role="user")
    AgentRepo(pool).create(agent_id="agent-a", user_id=1, name="Agent A")
    threads = ThreadRepo(pool)
    threads.insert(
        thread_id="old-dashboard",
        agent_id="agent-a",
        user_id=1,
        channel_type="dashboard",
        session_key="agent-a:dashboard:1:dm",
        last_active=100,
    )
    threads.insert(
        thread_id="new-dashboard",
        agent_id="agent-a",
        user_id=1,
        channel_type="dashboard",
        session_key="agent-a:dashboard:1:dm",
        last_active=200,
    )
    threads.insert(
        thread_id="cli-thread",
        agent_id="agent-a",
        user_id=1,
        channel_type="cli",
        session_key="agent-a:cli:1:dm",
        last_active=300,
    )
    with pool.transaction() as conn:
        conn.execute("DELETE FROM main_conversations")
        conn.execute("UPDATE _schema_version SET version = 24")

    run_migrations(pool)

    with pool.connect() as conn:
        version = conn.execute("SELECT version FROM _schema_version").fetchone()[0]
        row = conn.execute(
            "SELECT router_thread_id FROM main_conversations "
            "WHERE agent_id = 'agent-a' AND user_id = 1"
        ).fetchone()
        usage_columns = {
            column["name"] for column in conn.execute("PRAGMA table_info(usage_log)").fetchall()
        }
    assert version == 28
    assert row is not None
    assert row["router_thread_id"] == "new-dashboard"
    assert {"trace_id", "task_id", "run_id"}.issubset(usage_columns)
    pool.close()


def test_main_conversation_is_unique_and_rebinds_without_replacing_id(db: SqlitePool) -> None:
    service = ConversationService(db)
    first = _conversation(service, router_thread_id="new-dashboard")
    second = _conversation(service, router_thread_id="old-dashboard")

    assert first["conversation_id"] == second["conversation_id"]
    assert second["router_thread_id"] == "old-dashboard"
    with db.connect() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM main_conversations WHERE agent_id = 'agent-a' AND user_id = 1"
        ).fetchone()[0]
    assert count == 1

    created = _conversation(service, agent_id="agent-b", user_id=2)
    assert str(created["conversation_id"]).startswith("conv_")
    assert created["router_thread_id"] is None


def test_task_and_run_state_machines_are_bounded(db: SqlitePool) -> None:
    service = ConversationService(db)
    conversation = _conversation(service)
    task = service.create_task(
        conversation_id=str(conversation["conversation_id"]),
        task_type="calendar_conflict",
        title="Check calendar conflicts",
        subject_key="week-44-calendar",
        objective="Find conflicts and propose two alternatives",
    )
    run = service.create_run(task_id=str(task["task_id"]), max_attempts=3)
    assert run["attempt_no"] == 1
    task_after = service.get_task(
        agent_id="agent-a",
        user_id=1,
        task_id=str(task["task_id"]),
    )
    assert task_after["status"] == "in_progress"

    service.transition_run(run_id=str(run["run_id"]), status="running")
    finished = service.transition_run(run_id=str(run["run_id"]), status="succeeded")
    assert finished["status"] == "succeeded"

    for _ in range(2):
        service.create_run(task_id=str(task["task_id"]), max_attempts=3)
    with pytest.raises(RetryLimitExceededError):
        service.create_run(task_id=str(task["task_id"]), max_attempts=3)
    with pytest.raises(RouterPhase0Error):
        service.create_run(task_id=str(task["task_id"]), max_attempts=99)

    done = service.transition_task(
        task_id=str(task["task_id"]),
        status="done",
        capsule={"next_step": "Wait for the user to choose"},
    )
    assert done["capsule"]["next_step"] == "Wait for the user to choose"
    with pytest.raises(InvalidTransitionError):
        service.transition_task(task_id=str(task["task_id"]), status="in_progress")


def test_new_run_resumes_needs_user_task(db: SqlitePool) -> None:
    service = ConversationService(db)
    conversation = _conversation(service)
    task = service.create_task(
        conversation_id=str(conversation["conversation_id"]),
        task_type="calendar_conflict",
        title="Check calendar conflicts",
        subject_key="week-44-calendar",
        objective="Resume after the user chooses a slot",
        requires_user=True,
    )
    service.create_run(task_id=str(task["task_id"]))
    service.transition_task(task_id=str(task["task_id"]), status="needs_user")
    resumed = service.create_run(task_id=str(task["task_id"]))
    assert resumed["attempt_no"] == 2
    task_after = service.get_task(
        agent_id="agent-a",
        user_id=1,
        task_id=str(task["task_id"]),
    )
    assert task_after["status"] == "in_progress"


def test_events_are_ordered_idempotent_and_scope_checked(db: SqlitePool) -> None:
    service = ConversationService(db)
    conversation = _conversation(service)
    conversation_id = str(conversation["conversation_id"])
    first = service.append_event(
        conversation_id=conversation_id,
        trace_id="trace-1",
        event_type="router.received",
        status="received",
        summary="Received user message",
        event_id="evt-fixed",
        usage={"input_tokens": 20},
    )
    duplicate = service.append_event(
        conversation_id=conversation_id,
        trace_id="trace-1",
        event_type="router.received",
        event_id="evt-fixed",
    )
    second = service.append_event(
        conversation_id=conversation_id,
        trace_id="trace-1",
        event_type="task.updated",
        status="running",
        summary="Checking the calendar",
    )
    assert first["seq"] == duplicate["seq"] == 1
    assert second["seq"] == 2
    assert (
        len(
            service.list_events(
                agent_id="agent-a",
                user_id=1,
                conversation_id=conversation_id,
            )
        )
        == 2
    )
    trace = service.get_trace(agent_id="agent-a", user_id=1, trace_id="trace-1")
    assert [event["seq"] for event in trace["events"]] == [1, 2]
    with pytest.raises(NotFoundError):
        service.get_trace(agent_id="agent-a", user_id=2, trace_id="trace-1")


def test_event_references_cannot_cross_conversations(db: SqlitePool) -> None:
    service = ConversationService(db)
    own = _conversation(service)
    other = _conversation(service, agent_id="agent-b", user_id=2)
    own_task = service.create_task(
        conversation_id=str(own["conversation_id"]),
        task_type="lookup",
        title="Own task",
        subject_key="own",
        objective="Read only",
    )
    other_task = service.create_task(
        conversation_id=str(other["conversation_id"]),
        task_type="lookup",
        title="Other task",
        subject_key="other",
        objective="Read only",
    )
    other_run = service.create_run(task_id=str(other_task["task_id"]))
    other_event = service.append_event(
        conversation_id=str(other["conversation_id"]),
        trace_id="trace-other",
        event_type="task.updated",
        task_id=str(other_task["task_id"]),
    )

    with pytest.raises(RouterPhase0Error):
        service.append_event(
            conversation_id=str(own["conversation_id"]),
            trace_id="trace-cross-task",
            event_type="task.updated",
            task_id=str(other_task["task_id"]),
        )
    with pytest.raises(RouterPhase0Error):
        service.append_event(
            conversation_id=str(own["conversation_id"]),
            trace_id="trace-cross-run",
            event_type="task.updated",
            run_id=str(other_run["run_id"]),
        )
    with pytest.raises(RouterPhase0Error):
        service.append_event(
            conversation_id=str(own["conversation_id"]),
            trace_id="trace-mixed",
            event_type="task.updated",
            task_id=str(own_task["task_id"]),
            run_id=str(other_run["run_id"]),
        )
    with pytest.raises(RouterPhase0Error):
        service.append_event(
            conversation_id=str(own["conversation_id"]),
            trace_id="trace-parent",
            event_type="task.updated",
            parent_event_id=str(other_event["event_id"]),
        )


def test_budget_reservation_and_settlement_are_hard_limits(db: SqlitePool) -> None:
    service = ConversationService(db)
    period = {"period_start": 100, "period_end": 200}
    ledger = service.reserve_budget(
        scope_type="task",
        scope_id="task-1",
        tokens=80,
        cost_micros=80_000,
        limit_tokens=100,
        limit_cost_micros=100_000,
        **period,
    )
    assert ledger["reserved_tokens"] == 80
    with pytest.raises(BudgetExceededError):
        service.reserve_budget(
            scope_type="task",
            scope_id="task-1",
            tokens=30,
            cost_micros=30_000,
            limit_tokens=100,
            limit_cost_micros=100_000,
            **period,
        )
    service.release_budget(
        scope_type="task",
        scope_id="task-1",
        tokens=20,
        cost_micros=20_000,
        **period,
    )
    service.reserve_budget(
        scope_type="task",
        scope_id="task-1",
        tokens=30,
        cost_micros=30_000,
        limit_tokens=100,
        limit_cost_micros=100_000,
        **period,
    )
    settled = service.settle_budget(
        scope_type="task",
        scope_id="task-1",
        reserved_tokens=90,
        reserved_cost_micros=90_000,
        spent_tokens=75,
        spent_cost_micros=75_000,
        **period,
    )
    assert settled["reserved_tokens"] == 0
    assert settled["spent_tokens"] == 75


def test_multi_scope_budget_reservation_rolls_back_atomically(db: SqlitePool) -> None:
    service = ConversationService(db)
    period = {"period_start": 100, "period_end": 200}
    with pytest.raises(BudgetExceededError):
        service.reserve_budgets(
            [
                {
                    "scope_type": "task",
                    "scope_id": "task-ok",
                    "tokens": 10,
                    "cost_micros": 10_000,
                    "limit_tokens": 100,
                    "limit_cost_micros": 100_000,
                    **period,
                },
                {
                    "scope_type": "user",
                    "scope_id": "user-1",
                    "tokens": 120,
                    "cost_micros": 120_000,
                    "limit_tokens": 100,
                    "limit_cost_micros": 100_000,
                    **period,
                },
            ]
        )
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT scope_type, reserved_tokens FROM budget_ledgers "
            "WHERE period_start = ? AND period_end = ?",
            (period["period_start"], period["period_end"]),
        ).fetchall()
    assert list(rows) == []


def test_concurrent_budget_reservations_settle_their_own_deltas(db: SqlitePool) -> None:
    service = ConversationService(db)
    period = {"period_start": 100, "period_end": 200}

    def reserve(run_id: str) -> list[dict[str, Any]]:
        return service.reserve_budgets(
            [
                {
                    "scope_type": "run",
                    "scope_id": run_id,
                    "tokens": 50,
                    "cost_micros": 50_000,
                    "limit_tokens": 50,
                    "limit_cost_micros": 50_000,
                    **period,
                },
                {
                    "scope_type": "task_daily",
                    "scope_id": "shared-task",
                    "tokens": 50,
                    "cost_micros": 50_000,
                    "limit_tokens": 200,
                    "limit_cost_micros": 200_000,
                    **period,
                },
            ]
        )

    first = reserve("run-1")
    second = reserve("run-2")
    assert second[1]["reserved_tokens"] == 100

    def settle(reservations: list[dict[str, Any]], actual: int) -> None:
        service.settle_budgets(
            [
                {
                    "scope_type": item["scope_type"],
                    "scope_id": item["scope_id"],
                    "period_start": item["period_start"],
                    "period_end": item["period_end"],
                    "reserved_tokens": item["reservation_tokens"],
                    "reserved_cost_micros": item["reservation_cost_micros"],
                    "spent_tokens": actual,
                    "spent_cost_micros": actual * 1_000,
                }
                for item in reservations
            ]
        )

    settle(first, 10)
    settle(second, 20)
    shared = service.get_budget(
        scope_type="task_daily",
        scope_id="shared-task",
        **period,
    )
    assert shared["reserved_tokens"] == 0
    assert shared["spent_tokens"] == 30


def test_shadow_budget_reports_block_without_consuming_or_stopping_work(db: SqlitePool) -> None:
    gate = RouterBudgetGate(
        db,
        MemorySettings(
            {
                "router.budget.mode": "shadow",
                "router.budget.user_daily_tokens": "5",
                "router.budget.run_tokens.cron": "10",
            }
        ),
        timezone_name="Asia/Shanghai",
    )
    lease = gate.start(
        surface="cron",
        agent_id="agent-a",
        user_id=1,
        source_id="cron-shadow",
    )
    assert not lease.enforced
    assert lease.would_block
    assert lease.reservations == []
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM budget_ledgers").fetchone()[0] == 0
    lease.finish(actual_tokens=25)

    events = ConversationService(db).list_events(
        agent_id="agent-a",
        user_id=1,
        conversation_id=lease.conversation_id,
        include_payload=True,
    )
    assert [event["event_type"] for event in events] == [
        "budget.shadow_checked",
        "budget.shadow_settled",
    ]
    assert events[0]["status"] == "would_block"
    assert events[1]["usage"] == {"total_tokens": 25}
    assert events[1]["payload"]["actual_tokens"] == 25
    assert events[1]["payload"]["estimated_tokens"] == 10
    assert events[1]["payload"]["enforced_tokens"] == 10
    assert events[1]["payload"]["overrun_tokens"] == 15


def test_task_budget_shadow_reuses_usage_across_attempts(db: SqlitePool) -> None:
    service = ConversationService(db)
    gate = RouterBudgetGate(
        db,
        MemorySettings(
            {
                "router.budget.mode": "shadow",
                "router.budget.run_tokens.cron": "10",
                "router.budget.task_daily_tokens.cron": "35",
            }
        ),
        timezone_name="Asia/Shanghai",
        service=service,
    )

    first = gate.start(
        surface="cron",
        agent_id="agent-a",
        user_id=1,
        source_id="same-job",
    )
    assert first.task_key == "cron:same-job"
    first.finish(actual_tokens=30)
    service.record_usage(
        agent_id="agent-a",
        user_id=1,
        source="cron",
        total_tokens=30,
        trace_id=first.trace_id,
        task_id=first.task_key,
    )

    second = gate.start(
        surface="cron",
        agent_id="agent-a",
        user_id=1,
        source_id="same-job",
    )
    events = service.list_events(
        agent_id="agent-a",
        user_id=1,
        conversation_id=second.conversation_id,
        include_payload=True,
    )
    latest_check = next(
        event for event in reversed(events) if event["event_type"] == "budget.shadow_checked"
    )
    task_check = next(
        check for check in latest_check["payload"]["checks"] if check["scope_type"] == "task_daily"
    )
    assert task_check["scope_id"] == "cron:same-job"
    assert task_check["baseline_tokens"] == 30
    assert task_check["would_block"]
    assert second.would_block


def test_classifier_evaluation_reports_side_effect_miss_rate() -> None:
    metrics = evaluate_classifier(
        [
            ClassificationSample("你好", "direct_reply"),
            ClassificationSample("帮我看一下明天的日历", "read_only_task"),
            ClassificationSample("please handle the thing", "side_effect_task"),
        ]
    )

    assert metrics["total"] == 3
    assert metrics["side_effect_total"] == 1
    assert metrics["side_effect_missed"] == 1
    assert metrics["side_effect_miss_rate"] == 1.0
    assert not metrics["dispatch_enabled"]


@pytest.mark.asyncio
async def test_low_confidence_llm_evaluation_has_independent_budget() -> None:
    calls = 0

    async def classify(text: str) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {
            "intent": "side_effect_task",
            "confidence": 0.9,
            "tokens": 400,
            "dispatch_enabled": False,
        }

    samples = [
        ClassificationSample("please handle the thing", "side_effect_task"),
        ClassificationSample("look at that later", "read_only_task"),
        ClassificationSample("maybe deal with it", "side_effect_task"),
    ]
    metrics = await evaluate_low_confidence_with_llm(
        samples,
        classify,
        max_calls=1,
        token_budget=400,
        estimated_tokens_per_call=400,
    )

    assert calls == 1
    assert metrics["second_stage_calls"] == 1
    assert metrics["second_stage_estimated_tokens"] <= metrics["second_stage_token_budget"]
    assert metrics["second_stage_skipped_for_budget"] >= 1
    assert not metrics["dispatch_enabled"]


def test_shadow_evaluation_errors_never_block_work() -> None:
    service = MagicMock()
    service.ensure_main_conversation.return_value = {"conversation_id": "conv-shadow"}
    service.check_budget.return_value = {"would_block": False}
    service.sum_usage_tokens.side_effect = RuntimeError("usage database unavailable")
    gate = RouterBudgetGate(
        MagicMock(),
        MemorySettings({"router.budget.mode": "shadow"}),
        service=service,
    )

    lease = gate.start(
        surface="cron",
        agent_id="agent-a",
        user_id=1,
        source_id="cron-shadow-error",
    )

    assert not lease.enforced
    assert not lease.would_block
    event_call = service.append_event.call_args
    assert event_call.kwargs["status"] == "evaluation_error"
    assert "usage database unavailable" in event_call.kwargs["payload"]["evaluation_error"]


def test_enforce_mode_blocks_before_work_when_any_scope_is_exhausted(db: SqlitePool) -> None:
    gate = RouterBudgetGate(
        db,
        MemorySettings(
            {
                "router.budget.mode": "enforce",
                "router.budget.user_daily_tokens": "10",
            }
        ),
        timezone_name="Asia/Shanghai",
    )
    with pytest.raises(BudgetExceededError):
        gate.start(
            surface="cron",
            agent_id="agent-a",
            user_id=1,
            source_id="cron-blocked",
        )
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM budget_ledgers").fetchone()[0] == 0
        blocked = conn.execute(
            "SELECT status FROM conversation_events WHERE event_type = 'budget.blocked'"
        ).fetchall()
    assert [row[0] for row in blocked] == ["blocked"]


def test_enforced_lease_stops_after_usage_crosses_the_run_reservation(db: SqlitePool) -> None:
    gate = RouterBudgetGate(
        db,
        MemorySettings(
            {
                "router.budget.mode": "enforce",
                "router.budget.run_tokens.cron": "100",
                "router.budget.daily_tokens.cron": "1000",
                "router.budget.user_daily_tokens": "1000",
                "router.budget.agent_daily_tokens": "1000",
                "router.budget.global_daily_tokens": "1000",
                "router.budget.cost_per_1k_tokens_micros": "1000",
            }
        ),
        timezone_name="Asia/Shanghai",
    )
    lease = gate.start(
        surface="cron",
        agent_id="agent-a",
        user_id=1,
        source_id="cron-overflow",
    )
    with pytest.raises(BudgetExceededError):
        lease.observe({"total_tokens": 101})
    lease.finish(failed=True)

    with db.connect() as conn:
        run_ledger = conn.execute(
            "SELECT reserved_tokens, spent_tokens FROM budget_ledgers "
            "WHERE scope_type = 'run' AND scope_id = ?",
            (lease.trace_id,),
        ).fetchone()
    assert (run_ledger["reserved_tokens"], run_ledger["spent_tokens"]) == (0, 100)


def test_budget_settlement_errors_do_not_break_delivery(db: SqlitePool) -> None:
    service = ConversationService(db)
    gate = RouterBudgetGate(
        db,
        MemorySettings({"router.budget.mode": "enforce"}),
        timezone_name="Asia/Shanghai",
        service=service,
    )
    lease = gate.start(
        surface="cron",
        agent_id="agent-a",
        user_id=1,
        source_id="cron-settlement-error",
    )
    with patch.object(
        service,
        "settle_budgets",
        side_effect=RuntimeError("ledger database unavailable"),
    ):
        lease.finish(actual_tokens=5)

    events = service.list_events(
        agent_id="agent-a",
        user_id=1,
        conversation_id=lease.conversation_id,
        include_payload=True,
    )
    assert events[-1]["event_type"] == "budget.settlement_failed"
    assert events[-1]["status"] == "error"


def test_context_pack_and_usage_keep_trace_identifiers(db: SqlitePool) -> None:
    service = ConversationService(db)
    conversation = _conversation(service)
    conversation_id = str(conversation["conversation_id"])
    pack = service.record_context_pack(
        conversation_id=conversation_id,
        trace_id="trace-pack",
        policy_version="router-v1",
        model="test-model",
        used_tokens=1234,
        sections=[
            {"kind": "recent_turns", "tokens": 600},
            {"kind": "task_capsules", "tokens": 400},
        ],
        included_refs=["memory://recent", "knowledge://kb-1"],
        omitted_refs=["thread://old-worker"],
    )
    assert pack["sections"][0]["kind"] == "recent_turns"
    trace = service.get_trace(agent_id="agent-a", user_id=1, trace_id="trace-pack")
    assert trace["context_packs"][0]["pack_id"] == pack["pack_id"]
    service.record_usage(
        agent_id="agent-a",
        user_id=1,
        thread_id="new-dashboard",
        model="test-model",
        input_tokens=100,
        output_tokens=40,
        total_tokens=140,
        trace_id="trace-pack",
        task_id="task-1",
        run_id="run-1",
    )
    with db.connect() as conn:
        row = conn.execute(
            "SELECT trace_id, task_id, run_id FROM usage_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert tuple(row) == ("trace-pack", "task-1", "run-1")
    assert service.sum_usage_tokens(period_start=0, period_end=4_000_000_000) == 140


def test_context_pack_is_bounded_and_explains_omissions() -> None:
    pack = compile_context_pack(
        task_capsules=[
            {
                "ref": "task://task-1",
                "summary": "Waiting for the user to choose a slot",
                "tokens": 3,
                "priority": 2,
                "updated_at": 20,
            }
        ],
        recent_discussion=[
            {"ref": "turn://1", "content": "Older turn", "tokens": 4},
            {"ref": "turn://2", "content": "Latest turn", "tokens": 4},
        ],
        short_term_memory=[
            {
                "ref": "memory://useful",
                "summary": "Prefers afternoons",
                "tokens": 2,
                "score": 0.8,
            },
            {
                "ref": "memory://weak",
                "summary": "Unrelated",
                "tokens": 2,
                "score": 0.1,
            },
        ],
        knowledge_base=[
            {
                "ref": "kb://doc",
                "excerpt": "Process notes",
                "tokens": 4,
                "score": 0.9,
            }
        ],
        policy={
            "max_total_tokens": 8,
            "recent_discussion": {"max_items": 2, "max_tokens": 8},
        },
    )
    assert pack["used_tokens"] == 7
    assert [section["kind"] for section in pack["sections"]] == [
        "task_capsules",
        "recent_discussion",
    ]
    assert pack["included_refs"] == ["task://task-1", "turn://2"]
    reasons = {(item["ref"], item["reason"]) for item in pack["omitted_refs"]}
    assert ("turn://1", "token_budget") in reasons
    assert ("memory://weak", "score_below_threshold") in reasons
    assert ("memory://useful", "token_budget") in reasons
    assert ("kb://doc", "token_budget") in reasons


def test_context_pack_caps_manifest_omissions_and_estimates_missing_tokens() -> None:
    oversized = compile_context_pack(
        recent_discussion=[
            {
                "ref": "turn://oversized",
                "content": "x" * 80,
                "tokens": 0,
            }
        ],
        policy={"max_total_tokens": 10, "recent_discussion": {"max_items": 1}},
    )
    assert oversized["used_tokens"] == 0
    assert oversized["included_refs"] == []
    assert oversized["omitted_by_reason"] == {"token_budget": 1}

    many = compile_context_pack(
        knowledge_base=[
            {
                "ref": f"kb://{index}",
                "excerpt": "unrelated",
                "tokens": 1,
                "score": 0,
            }
            for index in range(250)
        ],
    )
    assert many["omitted_count"] == 250
    assert len(many["omitted_refs"]) == 200
    assert many["omitted_refs_truncated"]
    assert many["omitted_by_reason"] == {"score_below_threshold": 250}


def test_token_estimator_does_not_underprice_chinese_or_emoji() -> None:
    assert estimate_text_tokens("") == 0
    assert estimate_text_tokens("abcd") == 1
    assert estimate_text_tokens("你好世界") == 4
    assert estimate_text_tokens("🙂") == 1
    assert estimate_text_tokens("检查明天的日程冲突") > 2


def test_shadow_router_classifies_without_dispatch() -> None:
    assert classify_shadow_route("你好")["intent"] == "direct_reply"
    assert classify_shadow_route("请只回复 OK，不要调用任何工具。")["intent"] == "direct_reply"
    assert classify_shadow_route("帮我看一下明天的日历")["intent"] == "read_only_task"
    assert classify_shadow_route("把总结发送给张三")["intent"] == "side_effect_task"
    assert classify_shadow_route("读取文件后回复一句话")["intent"] == "read_only_task"
    assert (
        classify_shadow_route("只做订单预览，搜索拿铁，绝对不要调用 createOrder。")["intent"]
        == "read_only_task"
    )
    assert classify_shadow_route("汇总需要我回复的邮件")["intent"] == "read_only_task"
    assert classify_shadow_route("请发送邮件给张三")["intent"] == "side_effect_task"
    assert classify_shadow_route("删除这个任务")["intent"] == "side_effect_task"
    assert (
        classify_shadow_route("只做订单预览，如果现在打烊，说明最早可取时间；不要提交订单。")[
            "intent"
        ]
        == "read_only_task"
    )
    assert (
        classify_shadow_route("取暖费1200，每年12月前在微信~支付~城市服务里缴纳。")["intent"]
        == "direct_reply"
    )
    assert classify_shadow_route("帮忙看下失败的任务重新跑一下")["intent"] == "side_effect_task"
    assert (
        classify_shadow_route("把这次讨论落入记忆里，敏感数据另存")["intent"] == "side_effect_task"
    )
    assert (
        classify_shadow_route("没有看到文件，给我一个可以直接下载的")["intent"] == "read_only_task"
    )
    assert classify_shadow_route("怎么把修好的文件下载到手机里转发")["intent"] == "direct_reply"
    assert classify_shadow_route("知识库里怎么没有找到这些文档")["intent"] == "read_only_task"
    mixed = classify_shadow_route("查一下后直接发送")
    assert mixed["intent"] == "side_effect_task"
    assert not mixed["dispatch_enabled"]
    assert mixed["suggested_task"]["requires_user"]


def test_router_retention_is_dry_run_bounded_and_auditable(tmp_path: Path) -> None:
    now = 2_000_000_000
    db_path = tmp_path / "retention.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;
        CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE conversation_tasks (
            task_id TEXT PRIMARY KEY,
            status TEXT NOT NULL
        );
        CREATE TABLE task_runs (
            run_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL REFERENCES conversation_tasks(task_id),
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE conversation_events (
            event_id TEXT PRIMARY KEY,
            payload_ref TEXT,
            payload_json TEXT,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE context_pack_manifests (
            pack_id TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE budget_ledgers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            period_end INTEGER NOT NULL
        );
        CREATE TABLE audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            actor TEXT,
            action TEXT NOT NULL,
            target TEXT,
            payload TEXT
        );
        """
    )
    conn.execute("INSERT INTO conversation_tasks VALUES ('task_done', 'done')")
    conn.execute("INSERT INTO conversation_tasks VALUES ('task_active', 'in_progress')")
    conn.execute(
        "INSERT INTO task_runs VALUES ('run_done', 'task_done', 'succeeded', ?)",
        (now - 181 * 86400,),
    )
    conn.execute(
        "INSERT INTO task_runs VALUES ('run_active', 'task_active', 'succeeded', ?)",
        (now - 181 * 86400,),
    )
    conn.execute(
        "INSERT INTO context_pack_manifests VALUES ('pack_old', ?)",
        (now - 91 * 86400,),
    )
    conn.execute(
        "INSERT INTO context_pack_manifests VALUES ('pack_new', ?)",
        (now - 89 * 86400,),
    )
    conn.execute(
        "INSERT INTO conversation_events VALUES "
        "('event_old', 'context-pack://pack_old', '{\"private\":true}', ?)",
        (now - 91 * 86400,),
    )
    conn.execute(
        "INSERT INTO conversation_events VALUES "
        "('event_middle', 'context-pack://pack_new', '{\"private\":true}', ?)",
        (now - 31 * 86400,),
    )
    conn.execute(
        "INSERT INTO conversation_events VALUES ('event_new', NULL, '{\"private\":true}', ?)",
        (now - 29 * 86400,),
    )
    conn.execute(
        "INSERT INTO budget_ledgers(period_end) VALUES (?)",
        (now - 401 * 86400,),
    )
    conn.commit()

    dry_run = run_retention(db_path, now=now)
    assert dry_run["apply"] is False
    assert dry_run["counts"]["conversation_events_deleted"] == 1
    assert dry_run["counts"]["terminal_task_runs_deleted"] == 1
    assert conn.execute("SELECT COUNT(*) FROM conversation_events").fetchone()[0] == 3

    applied = run_retention(db_path, now=now, apply=True)
    assert applied["audit_recorded"] is True
    assert applied["counts"]["event_payloads_redacted"] == 2
    assert applied["counts"]["context_pack_refs_cleared"] == 1
    assert applied["counts"]["conversation_events_deleted"] == 1
    assert (
        conn.execute(
            "SELECT payload_json FROM conversation_events WHERE event_id = 'event_middle'"
        ).fetchone()[0]
        is None
    )
    assert conn.execute("SELECT COUNT(*) FROM task_runs").fetchone()[0] == 1
    audit = conn.execute(
        "SELECT action, payload FROM audit_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert audit[0] == "router.retention.applied"
    assert json.loads(audit[1])["policy_days"]["event_days"] == 90
    conn.close()


def test_memory_retrieval_payload_is_bounded_scored_and_deduplicated() -> None:
    candidates = _memory_candidates_from_payload(
        {
            "items": [
                {
                    "id": "atom-1",
                    "assertion": "用户偏好下午开会",
                    "importance": "high",
                },
                {
                    "id": "atom-1",
                    "assertion": "重复记录",
                    "importance": "high",
                },
                {
                    "assertion": "定时任务触发规则不应进入主会话上下文",
                    "importance": "high",
                },
                {
                    "id": "atom-2",
                    "assertion": "用户正在改造 Router",
                    "importance": "medium",
                    "confidence": "high",
                },
            ]
        },
        limit=2,
    )
    assert len(candidates) == 2
    assert candidates[0]["ref"] == "memory://atom/atom-1"
    assert candidates[0]["score"] == 0.9
    assert "定时任务" not in candidates[1]["summary"]
    assert candidates[1]["ref"] == "memory://atom/atom-2"


def test_shadow_memory_retrieval_never_exposes_shared_agent_owner_memory() -> None:
    memory = MagicMock()
    memory.list_atoms.return_value = [
        SimpleNamespace(
            id="private-atom",
            assertion="The owner's private preference",
            importance="high",
            confidence="high",
        )
    ]
    registry = MagicMock()
    registry.get_row.return_value = SimpleNamespace(user_id=2)
    registry.get_agent.return_value = SimpleNamespace(memory=memory)
    server = SimpleNamespace(
        user_manager=MagicMock(),
        app_runtime=SimpleNamespace(agent_registry=registry),
    )
    server.user_manager.get_by_id.return_value = SimpleNamespace(id=1, is_admin=False)

    candidates = asyncio.run(
        _retrieve_memory_candidates(
            server,
            agent_id="shared-agent",
            user_id=1,
            query="private",
            enabled=True,
        )
    )
    assert candidates == []
    registry.get_agent.assert_not_called()


def test_shadow_observation_writes_bounded_context_manifest_and_trace(db: SqlitePool) -> None:
    service = ConversationService(db)
    conversation = _conversation(service)
    task = service.create_task(
        conversation_id=str(conversation["conversation_id"]),
        task_type="calendar_conflict",
        title="Check tomorrow",
        subject_key="tomorrow-calendar",
        objective="Find tomorrow's conflicts",
        requires_user=True,
    )
    service.transition_task(
        task_id=str(task["task_id"]),
        status="needs_user",
        capsule={"next_step": "Ask the user which meeting to move"},
    )
    messages = FakeThreadMessages(
        [
            SimpleNamespace(
                seq=1,
                message_json=json.dumps({"content": "上一轮讨论"}),
                created_at=90,
            )
        ]
    )
    server = SimpleNamespace(
        services=SimpleNamespace(
            db=db,
            repos=SimpleNamespace(thread_message_repo=messages),
        ),
        app_runtime=SimpleNamespace(),
    )
    result = asyncio.run(
        observe_shadow_turn(
            server,
            agent_id="agent-a",
            user_id=1,
            thread_id="new-dashboard",
            session_key="agent-a:dashboard:1:dm",
            text="帮我查一下明天的日历",
            has_attachments=True,
            target_agent_ids=("agent-a",),
            memory_candidates=[
                {
                    "ref": "memory://useful",
                    "summary": "Prefers afternoon meetings",
                    "tokens": 3,
                    "score": 0.8,
                },
                {
                    "ref": "memory://weak",
                    "summary": "Unrelated preference",
                    "tokens": 3,
                    "score": 0.1,
                },
            ],
            knowledge_candidates=[
                {
                    "ref": "kb://calendar-policy",
                    "excerpt": "Calendar conflict policy",
                    "tokens": 4,
                    "score": 0.9,
                }
            ],
        )
    )

    assert result["decision"]["intent"] == "read_only_task"
    assert not result["decision"]["dispatch_enabled"]
    assert (
        service.ensure_main_conversation(agent_id="agent-a", user_id=1)["router_thread_id"]
        == "new-dashboard"
    )
    assert result["context_pack"]["used_tokens"] <= 6_000
    assert f"task://{task['task_id']}" in result["context_pack"]["included_refs"]
    assert "memory://weak" not in result["context_pack"]["included_refs"]
    events = service.list_events(
        agent_id="agent-a",
        user_id=1,
        conversation_id=str(conversation["conversation_id"]),
        include_payload=True,
    )
    assert [event["event_type"] for event in events] == [
        "router.received",
        "context.pack.compiled",
        "router.shadow.decision",
    ]
    trace = service.get_trace(
        agent_id="agent-a",
        user_id=1,
        trace_id=str(result["trace_id"]),
    )
    assert len(trace["context_packs"]) == 1
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM task_runs").fetchone()[0] == 0


def test_shadow_observation_uses_bounded_auto_retrieval_when_not_supplied(
    db: SqlitePool,
) -> None:
    service = ConversationService(db)
    conversation = _conversation(service)
    server = SimpleNamespace(
        services=SimpleNamespace(
            db=db,
            repos=SimpleNamespace(thread_message_repo=FakeThreadMessages()),
        ),
        app_runtime=SimpleNamespace(),
    )
    with patch(
        "octop.infra.conversation.shadow._auto_retrieval_candidates",
        new=AsyncMock(
            return_value=(
                [
                    {
                        "ref": "memory://atom/auto",
                        "summary": "The user prefers concise answers",
                        "tokens": 5,
                        "score": 0.8,
                    }
                ],
                [
                    {
                        "ref": "knowledge://retrieval/auto",
                        "excerpt": "Project constraints",
                        "tokens": 3,
                        "score": 0.9,
                    }
                ],
            )
        ),
    ):
        result = asyncio.run(
            observe_shadow_turn(
                server,
                agent_id="agent-a",
                user_id=1,
                thread_id="new-dashboard",
                session_key="agent-a:dashboard:1:dm",
                text="继续按这个方案推进",
            )
        )
    assert "memory://atom/auto" in result["context_pack"]["included_refs"]
    assert "knowledge://retrieval/auto" in result["context_pack"]["included_refs"]
    assert (
        service.list_events(
            agent_id="agent-a",
            user_id=1,
            conversation_id=str(conversation["conversation_id"]),
        )[0]["event_type"]
        == "router.received"
    )


@pytest.mark.asyncio
async def test_cron_job_stops_before_delivery_when_budget_gate_blocks() -> None:
    gate = MagicMock()
    gate.start.side_effect = BudgetExceededError("hard budget limit would be exceeded")
    delivery = MagicMock(spec=CronDeliveryService)
    delivery.deliver = AsyncMock()
    cron_repo = MagicMock()
    audit_repo = MagicMock()
    job = CronJob(
        cron_id="cron-1",
        name="Daily check",
        agent_id="agent-a",
        prompt="check",
        fresh_thread=False,
        session_key="agent-a:cron:1:dm",
        model=None,
        task_type="agent",
        mcp_servers=None,
        user_id=1,
        delivery_service=delivery,
        cron_repo=cron_repo,
        audit_repo=audit_repo,
        budget_gate=gate,
    )
    await job.run()
    delivery.deliver.assert_not_awaited()
    cron_repo.set_run_status.assert_called_once()
    assert cron_repo.set_run_status.call_args.kwargs["status"] == "error"
    audit_repo.write.assert_called_once()
