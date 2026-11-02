"""Phase 3 Worker API is present but fail-closed by default."""

from __future__ import annotations

from typing import Any

from octop.infra.conversation.orchestrator import WorkerDispatchRejected


async def test_worker_status_route_precedes_task_lookup(
    env_with_main_agent: tuple[Any, Any, dict[str, str], str],
) -> None:
    client, _server, auth, agent_id = env_with_main_agent
    response = await client.get(
        f"/api/agents/{agent_id}/tasks/worker-status",
        headers=auth,
    )

    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is False
    assert body["ready"] is False
    assert body["running"] is False
    assert body["read_only_only"] is True


async def test_read_only_submit_is_unavailable_until_explicitly_enabled(
    env_with_main_agent: tuple[Any, Any, dict[str, str], str],
) -> None:
    client, _server, auth, agent_id = env_with_main_agent
    response = await client.post(
        f"/api/agents/{agent_id}/tasks/read-only",
        headers=auth,
        json={
            "title": "Check tomorrow's calendar",
            "objective": "Read tomorrow's calendar and summarize conflicts",
        },
    )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "INTERNAL_ERROR"


async def test_side_effect_dispatch_rejection_is_a_forbidden_response(
    env_with_main_agent: tuple[Any, Any, dict[str, str], str],
) -> None:
    client, server, auth, agent_id = env_with_main_agent

    class RejectingWorker:
        async def submit_read_only(self, **_kwargs: Any) -> dict[str, Any]:
            raise WorkerDispatchRejected("only read-only requests can be dispatched")

    original = server.app_runtime.router_worker_service
    server.app_runtime.router_worker_service = RejectingWorker()
    try:
        response = await client.post(
            f"/api/agents/{agent_id}/tasks/read-only",
            headers=auth,
            json={
                "title": "Delete calendar events",
                "objective": "删除明天的全部日程并发送通知",
            },
        )
    finally:
        server.app_runtime.router_worker_service = original

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "FORBIDDEN"
