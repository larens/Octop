"""Router Phase 3 bounded read-only Worker orchestration."""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from octop.infra.conversation.budget import RouterBudgetGate
from octop.infra.conversation.orchestrator import (
    ProcessorReadOnlyRunner,
    RouterWorkerService,
    TaskOrchestrator,
    WorkerConfig,
    WorkerContext,
    WorkerDispatchRejected,
    WorkerResult,
    worker_config,
)
from octop.infra.conversation.service import ConversationService
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.sessions import SessionRepo
from octop.infra.db.repos.threads import ThreadRepo
from octop.infra.db.repos.users import UserRepo
from octop.infra.gateway.process.processor import GlobalProcessor
from octop.infra.gateway.process.usage_record import UsageTracker
from octop.infra.gateway.threads import ThreadRegistry

ROUTER_ENV_KEYS = (
    "OCTOP_ROUTER_WORKER_DISPATCH_ENABLED",
    "OCTOP_ROUTER_BUDGET_MODE",
    "OCTOP_ROUTER_BUDGET_ENFORCED_SURFACES",
)


class MemorySettings:
    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = dict(values or {})

    def get(self, key: str) -> str | None:
        return self.values.get(key)


@pytest.fixture
def db(tmp_path: Path) -> SqlitePool:
    pool = SqlitePool(tmp_path / "octop.db")
    run_migrations(pool)
    UserRepo(pool).create(username="alice", password_hash="h", role="user")
    AgentRepo(pool).create(agent_id="agent-a", user_id=1, name="Agent A")
    yield pool
    pool.close()


@pytest.fixture(autouse=True)
def clean_router_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ROUTER_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _conversation(service: ConversationService) -> dict[str, Any]:
    return service.ensure_main_conversation(agent_id="agent-a", user_id=1)


def _worker_config(
    *,
    max_attempts: int = 2,
    timeout_seconds: int = 5,
) -> WorkerConfig:
    return WorkerConfig(
        enabled=True,
        ready=True,
        reason="enabled",
        max_concurrency=1,
        lease_seconds=90,
        heartbeat_seconds=30,
        poll_seconds=0.01,
        timeout_seconds=timeout_seconds,
        max_attempts=max_attempts,
        backoff_seconds=1,
        max_backoff_seconds=1,
        progress_event_seconds=1,
    )


def _registry(db: SqlitePool) -> ThreadRegistry:
    return ThreadRegistry(
        session_repo=SessionRepo(db),
        thread_repo=ThreadRepo(db),
    )


def _enforce_settings(
    *,
    run_tokens: int = 60,
    task_tokens: int = 100,
) -> MemorySettings:
    return MemorySettings(
        {
            "router.worker_dispatch.enabled": "true",
            "router.budget.mode": "enforce",
            "router.budget.enforced_surfaces": "worker",
            "router.budget.run_tokens.worker": str(run_tokens),
            "router.budget.task_daily_tokens.worker": str(task_tokens),
            "router.budget.daily_tokens.worker": "1000",
            "router.budget.user_daily_tokens": "1000",
            "router.budget.agent_daily_tokens": "1000",
            "router.budget.global_daily_tokens": "2000",
        }
    )


def _queue_read_only(
    service: ConversationService,
    *,
    subject_key: str = "calendar:tomorrow",
    available_at: int = 0,
) -> dict[str, Any]:
    conversation = _conversation(service)
    return service.create_read_only_task(
        conversation_id=str(conversation["conversation_id"]),
        title="Check tomorrow's calendar",
        subject_key=subject_key,
        objective="Read tomorrow's calendar and summarize the conflicts",
        capsule={"trace_id": "trace-phase3", "context_pack_id": "pack-phase3"},
        max_attempts=3,
        available_at=available_at,
    )


def test_claim_is_atomic_and_only_claims_due_read_only_runs(db: SqlitePool) -> None:
    service = ConversationService(db)
    due_at = int(time.time()) + 100
    queued = _queue_read_only(service, available_at=due_at)
    assert queued["deduplicated"] is False

    assert service.claim_next_read_only_run(now=due_at - 1, lease_seconds=30) is None

    def claim() -> dict[str, Any] | None:
        return service.claim_next_read_only_run(now=due_at, lease_seconds=30)

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(lambda _: claim(), range(2)))

    claimed = [item for item in claims if item is not None]
    assert len(claimed) == 1
    assert claimed[0]["run"]["run_id"] == queued["runs"][0]["run_id"]
    assert claimed[0]["run"]["status"] == "running"

    non_read_only = service.create_task(
        conversation_id=str(queued["task"]["conversation_id"]),
        task_type="calendar_conflict",
        title="Write calendar",
        subject_key="calendar:write",
        objective="Create an event",
    )
    service.create_run(task_id=str(non_read_only["task_id"]), max_attempts=1)
    assert service.claim_next_read_only_run(now=due_at + 1, lease_seconds=30) is None


def test_read_only_queue_deduplicates_same_subject_atomically(db: SqlitePool) -> None:
    service = ConversationService(db)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = list(
            pool.map(
                lambda _: _queue_read_only(service, subject_key="same-subject"),
                range(2),
            )
        )

    assert first["task"]["task_id"] == second["task"]["task_id"]
    assert sorted([first["deduplicated"], second["deduplicated"]]) == [False, True]
    with db.connect() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM conversation_tasks WHERE subject_key = 'same-subject'"
        ).fetchone()[0]
    assert count == 1


def test_cancel_and_expired_lease_prevent_revival(db: SqlitePool) -> None:
    service = ConversationService(db)
    queued = _queue_read_only(service)
    base = int(time.time()) + 1
    claimed = service.claim_next_read_only_run(now=base, lease_seconds=10)
    assert claimed is not None

    cancelled = service.cancel_task(task_id=str(queued["task"]["task_id"]))
    assert cancelled["status"] == "cancelled"
    assert service.get_run(run_id=str(claimed["run"]["run_id"]))["status"] == "cancelled"
    assert service.claim_next_read_only_run(now=base + 1, lease_seconds=10) is None

    queued = _queue_read_only(service, subject_key="lease-expiry")
    claimed = service.claim_next_read_only_run(now=base + 2, lease_seconds=10)
    assert claimed is not None
    recovered = service.recover_expired_runs(now=base + 13)
    assert [run["run_id"] for run in recovered] == [claimed["run"]["run_id"]]
    assert service.get_run(run_id=str(claimed["run"]["run_id"]))["status"] == "interrupted"


@pytest.mark.asyncio
async def test_retry_reuses_one_task_budget_and_is_bounded(db: SqlitePool) -> None:
    service = ConversationService(db)
    settings = _enforce_settings()
    config = _worker_config()
    attempts: list[int] = []

    async def runner(
        ctx: WorkerContext,
        observe_usage: Any,
        _progress: Any,
    ) -> WorkerResult:
        attempts.append(ctx.attempt_no)
        usage = {"total_tokens": 40 if ctx.attempt_no == 1 else 20}
        await observe_usage(usage)
        if ctx.attempt_no == 1:
            raise RuntimeError("transient tool failure")
        return WorkerResult(summary="done", output="done", usage=usage)

    orchestrator = TaskOrchestrator(
        service=service,
        thread_registry=_registry(db),
        budget_gate=RouterBudgetGate(db, settings),
        runner=runner,
        config=config,
    )
    queued = await orchestrator.enqueue_read_only(
        conversation_id=str(_conversation(service)["conversation_id"]),
        title="Check calendar",
        objective="Read tomorrow's calendar",
        subject_key="calendar:retry",
        trace_id="trace-retry",
        context_pack_id="pack-retry",
    )
    task_id = str(queued["task"]["task_id"])

    assert await orchestrator.execute_once() is True
    task = service.get_task(agent_id="agent-a", user_id=1, task_id=task_id)
    assert task["status"] == "in_progress"
    assert attempts == [1]

    with db.transaction() as conn:
        conn.execute("UPDATE task_runs SET lease_until = 0 WHERE status = 'queued'")

    assert await orchestrator.execute_once() is True
    task = service.get_task(agent_id="agent-a", user_id=1, task_id=task_id)
    assert task["status"] == "done"
    assert [run["status"] for run in task["runs"]] == ["succeeded", "failed"]
    assert attempts == [1, 2]

    with db.connect() as conn:
        ledgers = conn.execute(
            "SELECT * FROM budget_ledgers WHERE scope_type = 'task_daily' AND scope_id = ?",
            (f"worker:{task_id}",),
        ).fetchall()
        usage_rows = conn.execute(
            "SELECT total_tokens FROM usage_log WHERE task_id = ? ORDER BY id",
            (task_id,),
        ).fetchall()
    assert len(ledgers) == 1
    assert ledgers[0]["spent_tokens"] == 60
    assert ledgers[0]["reserved_tokens"] == 0
    assert ledgers[0]["limit_tokens"] == 100
    assert [row["total_tokens"] for row in usage_rows] == [40]


@pytest.mark.asyncio
async def test_budget_overrun_stops_without_retry(db: SqlitePool) -> None:
    service = ConversationService(db)
    settings = _enforce_settings(run_tokens=10, task_tokens=100)

    async def runner(
        _ctx: WorkerContext,
        observe_usage: Any,
        _progress: Any,
    ) -> WorkerResult:
        await observe_usage({"total_tokens": 11})
        return WorkerResult(summary="too expensive", output="too expensive", usage={})

    orchestrator = TaskOrchestrator(
        service=service,
        thread_registry=_registry(db),
        budget_gate=RouterBudgetGate(db, settings),
        runner=runner,
        config=_worker_config(max_attempts=3),
    )
    queued = await orchestrator.enqueue_read_only(
        conversation_id=str(_conversation(service)["conversation_id"]),
        title="Expensive read",
        objective="Read a large dataset",
        subject_key="dataset:large",
        trace_id="trace-budget",
        context_pack_id="pack-budget",
    )
    task_id = str(queued["task"]["task_id"])

    assert await orchestrator.execute_once() is True
    task = service.get_task(agent_id="agent-a", user_id=1, task_id=task_id)
    assert task["status"] == "failed"
    assert len(task["runs"]) == 1
    assert task["runs"][0]["failure_class"] == "budget_exceeded"
    assert service.claim_next_read_only_run(now=10**10, lease_seconds=30) is None
    assert task["capsule"]["usage"]["total_tokens"] == 11
    with db.connect() as conn:
        usage = conn.execute(
            "SELECT total_tokens, source FROM usage_log WHERE task_id = ?",
            (task_id,),
        ).fetchall()
        failed_event = conn.execute(
            "SELECT usage_json FROM conversation_events "
            "WHERE task_id = ? AND event_type = 'worker.failed'",
            (task_id,),
        ).fetchone()
    assert [(row["total_tokens"], row["source"]) for row in usage] == [(11, "worker")]
    assert json.loads(failed_event["usage_json"])["total_tokens"] == 11


def test_worker_dispatch_is_fail_closed_without_all_safety_switches() -> None:
    assert worker_config(MemorySettings()).ready is False
    assert (
        worker_config(
            MemorySettings(
                {
                    "router.worker_dispatch.enabled": "true",
                    "router.budget.mode": "shadow",
                    "router.budget.enforced_surfaces": "worker",
                }
            )
        ).ready
        is False
    )
    assert (
        worker_config(
            MemorySettings(
                {
                    "router.worker_dispatch.enabled": "true",
                    "router.budget.mode": "enforce",
                    "router.budget.enforced_surfaces": "cron",
                }
            )
        ).ready
        is False
    )
    assert worker_config(_enforce_settings()).ready is True


@pytest.mark.asyncio
async def test_side_effect_request_never_reaches_the_queue(db: SqlitePool) -> None:
    settings = _enforce_settings()
    registry = _registry(db)
    server = type(
        "FakeServer",
        (),
        {
            "services": type(
                "FakeServices",
                (),
                {
                    "db": db,
                    "settings_repo": settings,
                    "config": type("FakeConfig", (), {"default_timezone": "Asia/Shanghai"})(),
                },
            )(),
            "app_runtime": type(
                "FakeRuntime",
                (),
                {"gateway": type("FakeGateway", (), {"thread_registry": registry})()},
            )(),
        },
    )()
    worker = RouterWorkerService(server)

    with pytest.raises(WorkerDispatchRejected):
        await worker.submit_read_only(
            agent_id="agent-a",
            user_id=1,
            title="Delete files",
            objective="删除所有文件并发送通知",
        )
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM conversation_tasks").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_worker_threads_are_hidden_from_user_history(db: SqlitePool) -> None:
    registry = _registry(db)
    dashboard = await registry.get_or_create(
        agent_id="agent-a",
        user_id=1,
        channel_type=ThreadRegistry.CHANNEL_DASHBOARD,
        channel_subject_id="1",
    )
    await registry.get_or_create_by_key(
        session_key=ThreadRegistry.worker_key(
            agent_id="agent-a",
            task_id="task-1",
            run_id="run-1",
        ),
        agent_id="agent-a",
        user_id=1,
        channel_type=ThreadRegistry.CHANNEL_WORKER,
        channel_metadata={"task_id": "task-1", "run_id": "run-1", "hidden": True},
    )

    visible = registry.list_threads(agent_id="agent-a", user_id=1)
    assert [row.thread_id for row in visible] == [dashboard]


def test_processor_usage_keeps_worker_attribution() -> None:
    class Capture:
        kwargs: dict[str, Any] = {}

        def record(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

    capture = Capture()
    processor = object.__new__(GlobalProcessor)
    processor._usage_repo = capture
    processor._record_turn_usage(
        user_id=1,
        agent_id="agent-a",
        thread_id="worker-thread",
        usage={"input_tokens": 4, "output_tokens": 2, "model": "test"},
        source="worker",
        trace_id="trace-worker",
        task_id="task-worker",
        run_id="run-worker",
    )
    assert capture.kwargs["source"] == "worker"
    assert capture.kwargs["trace_id"] == "trace-worker"
    assert capture.kwargs["task_id"] == "task-worker"
    assert capture.kwargs["run_id"] == "run-worker"


@pytest.mark.asyncio
async def test_runner_usage_is_cumulative_for_budget_observation() -> None:
    tracker = UsageTracker()
    tracker.observe(
        {"type": "usage", "call_id": "call-1", "usage": {"input_tokens": 3, "output_tokens": 2}}
    )
    tracker.observe(
        {"type": "usage", "call_id": "call-2", "usage": {"input_tokens": 5, "output_tokens": 4}}
    )
    assert tracker.usage is not None
    assert tracker.usage["total_tokens"] == 14


@pytest.mark.asyncio
async def test_processor_runner_forces_ask_mode_and_worker_attribution(db: SqlitePool) -> None:
    captured: dict[str, Any] = {}

    class FakeProcessor:
        def iter_turn_chunks(self, msg: Any) -> Any:
            captured["metadata"] = dict(msg.metadata)
            captured["channel_type"] = msg.channel_type

            async def chunks() -> Any:
                yield {
                    "type": "usage",
                    "call_id": "worker-call",
                    "usage": {"input_tokens": 3, "output_tokens": 2},
                }
                yield {"type": "token", "content": "No calendar conflicts tomorrow."}

            return chunks()

    server = SimpleNamespace(
        services=SimpleNamespace(db=db),
        app_runtime=SimpleNamespace(
            gateway=SimpleNamespace(processor=FakeProcessor()),
        ),
    )
    runner = ProcessorReadOnlyRunner(server)
    ctx = WorkerContext(
        task_id="task-worker",
        run_id="run-worker",
        conversation_id="conversation-worker",
        agent_id="agent-a",
        user_id=1,
        trace_id="trace-worker",
        objective="Read tomorrow's calendar",
        title="Check calendar",
        worker_thread_id="thread-worker",
        attempt_no=2,
        context_pack_id="",
    )
    observed: list[dict[str, Any]] = []

    async def observe_usage(usage: Any) -> None:
        observed.append(dict(usage))

    result = await runner(ctx, observe_usage, lambda: None)

    assert captured["channel_type"] == ThreadRegistry.CHANNEL_WORKER
    assert captured["metadata"] == {
        "thread_id": "thread-worker",
        "session_key": ThreadRegistry.worker_key(
            agent_id="agent-a",
            task_id="task-worker",
            run_id="run-worker",
        ),
        "conversation_mode": "ask",
        "mcp_servers": [],
        "skills": [],
        "usage_source": "worker",
        "trace_id": "trace-worker",
        "task_id": "task-worker",
        "run_id": "run-worker",
        "worker": True,
    }
    assert observed[-1]["total_tokens"] == 5
    assert result.summary == "No calendar conflicts tomorrow."
    assert result.usage["total_tokens"] == 5
