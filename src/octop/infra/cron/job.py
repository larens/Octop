"""CronJob — APScheduler callable with status and audit bookkeeping."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from octop.infra.cron.delivery import CronDeliveryCommand
from octop.infra.cron.task_type import normalize_cron_task_type
from octop.infra.db.repos.audit import ACTOR_SYSTEM

if TYPE_CHECKING:
    from octop.infra.cron.delivery import CronDeliveryService
    from octop.infra.db.repos.audit import AuditRepo
    from octop.infra.db.repos.cron import CronJobRepo, CronJobRow

logger = logging.getLogger(__name__)


class CronJob:
    def __init__(
        self,
        *,
        cron_id: str,
        name: str,
        agent_id: str,
        prompt: str,
        fresh_thread: bool,
        session_key: str,
        model: str | None,
        task_type: str,
        mcp_servers: list[str] | None,
        user_id: int,
        delivery_service: CronDeliveryService,
        cron_repo: CronJobRepo,
        audit_repo: AuditRepo,
        budget_gate: Any | None = None,
    ) -> None:
        self._cron_id = cron_id
        self._name = name
        self._agent_id = agent_id
        self._prompt = prompt
        self._fresh_thread = fresh_thread
        self._session_key = session_key
        self._model = model
        self._task_type = normalize_cron_task_type(task_type)
        self._mcp_servers = list(mcp_servers or [])
        self._user_id = user_id
        self._delivery_service = delivery_service
        self._cron_repo = cron_repo
        self._audit_repo = audit_repo
        self._budget_gate = budget_gate

    @classmethod
    def from_row(
        cls,
        row: CronJobRow,
        *,
        delivery_service: CronDeliveryService,
        cron_repo: CronJobRepo,
        audit_repo: AuditRepo,
        budget_gate: Any | None = None,
    ) -> CronJob:
        return cls(
            cron_id=row.cron_id,
            name=row.name,
            agent_id=row.agent_id,
            prompt=row.prompt,
            fresh_thread=bool(row.fresh_thread),
            session_key=row.session_key,
            model=row.model,
            task_type=row.task_type,
            mcp_servers=list(row.mcp_servers),
            user_id=row.user_id,
            delivery_service=delivery_service,
            cron_repo=cron_repo,
            audit_repo=audit_repo,
            budget_gate=budget_gate,
        )

    async def run(self, *, raise_on_error: bool = False) -> None:
        from octop.infra.metrics import METRICS  # noqa: PLC0415

        METRICS.inc("cron_runs_total")
        ts = int(time.time())
        budget_lease = None
        if self._budget_gate is not None:
            try:
                budget_lease = self._budget_gate.start(
                    surface="cron",
                    agent_id=self._agent_id,
                    user_id=self._user_id,
                    source_id=self._cron_id,
                )
            except Exception as exc:
                METRICS.inc("cron_errors_total")
                err = str(exc)
                logger.warning("cron job %s blocked by router budget: %s", self._cron_id, err)
                self._cron_repo.set_run_status(self._cron_id, ts=ts, status="error", error=err)
                self._audit_repo.write(
                    actor=ACTOR_SYSTEM,
                    action="cron.run_blocked",
                    target=self._cron_id,
                    payload=err,
                )
                if raise_on_error:
                    raise
                return

        try:
            result = await self._delivery_service.deliver(
                CronDeliveryCommand(
                    cron_id=self._cron_id,
                    cron_name=self._name,
                    agent_id=self._agent_id,
                    user_id=self._user_id,
                    session_key=self._session_key,
                    prompt=self._prompt,
                    fresh_thread=self._fresh_thread,
                    task_type=self._task_type,
                    model=self._model,
                    mcp_servers=tuple(self._mcp_servers),
                    budget_lease=budget_lease,
                )
            )
        except Exception as exc:
            METRICS.inc("cron_errors_total")
            logger.exception("cron job %s failed", self._cron_id)
            err = str(exc)
            if budget_lease is not None:
                budget_lease.finish(failed=True)
            self._cron_repo.set_run_status(self._cron_id, ts=ts, status="error", error=err)
            self._audit_repo.write(
                actor=ACTOR_SYSTEM,
                action="cron.run_failed",
                target=self._cron_id,
                payload=err,
            )
            if raise_on_error:
                raise
            return
        if budget_lease is not None:
            actual_tokens = (
                int(result.get("total_tokens", 0) or 0) if isinstance(result, dict) else 0
            )
            budget_lease.finish(
                actual_tokens=actual_tokens,
                failed=False,
            )
        self._cron_repo.set_run_status(self._cron_id, ts=ts, status="ok", error=None)
        self._audit_repo.write(
            actor=ACTOR_SYSTEM,
            action="cron.run_ok",
            target=self._cron_id,
        )
