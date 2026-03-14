from __future__ import annotations

import argparse
import logging
import os
import socket
import time

from fin_ai_auditor.config import get_settings
from fin_ai_auditor.runtime_logging import setup_logging
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository
from fin_ai_auditor.services.audit_service import AuditService
from fin_ai_auditor.services.atlassian_oauth_service import AtlassianOAuthService
from fin_ai_auditor.services.pipeline_service import AuditPipelineService
from fin_ai_auditor.services.runtime_observability_service import RuntimeObservabilityService

logger = logging.getLogger(__name__)


def process_once() -> bool:
    settings = get_settings()
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    audit_service = AuditService(repository=repository, settings=settings)
    atlassian_oauth_service = AtlassianOAuthService(repository=repository, settings=settings)
    observability = RuntimeObservabilityService(repository=repository)
    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    run = audit_service.claim_next_planned_run(worker_id=worker_id)
    if run is None:
        observability.increment_counter(
            metric_key="worker_idle_polls_total",
            worker_id=worker_id,
            labels={"result": "empty"},
        )
        return False

    pipeline = AuditPipelineService(
        audit_service=audit_service,
        settings=settings,
        allow_remote_llm=True,
        atlassian_oauth_service=atlassian_oauth_service,
    )
    try:
        with observability.trace_span(
            trace_id=f"worker_trace_{run.run_id}",
            run_id=run.run_id,
            worker_id=worker_id,
            span_name="worker.process_once",
            metadata={"claimed_run_id": run.run_id},
        ):
            completed = pipeline.execute_run(run_id=run.run_id, worker_id=worker_id)
    except Exception as exc:
        observability.increment_counter(
            metric_key="worker_run_failures_total",
            run_id=run.run_id,
            worker_id=worker_id,
            labels={"error_type": type(exc).__name__},
        )
        audit_service.fail_run(run_id=run.run_id, error=f"{type(exc).__name__}: {exc}", worker_id=worker_id)
        logger.error(
            "worker_run_failed",
            extra={
                "event_name": "worker_run_failed",
                "event_payload": {"run_id": run.run_id, "worker_id": worker_id, "error": f"{type(exc).__name__}: {exc}"},
            },
            exc_info=True,
        )
        return True

    observability.increment_counter(
        metric_key="worker_runs_completed_total",
        run_id=completed.run_id,
        worker_id=worker_id,
        labels={"status": completed.status},
    )
    logger.info(
        "worker_run_completed",
        extra={
            "event_name": "worker_run_completed",
            "event_payload": {
                "run_id": completed.run_id,
                "worker_id": worker_id,
                "claim_count": len(completed.claims),
                "finding_count": len(completed.findings),
            },
        },
    )
    return True


def main() -> None:
    setup_logging(level=logging.INFO)
    parser = argparse.ArgumentParser(description="FIN-AI Auditor worker")
    parser.add_argument("--once", action="store_true", help="Verarbeitet genau einen geplanten Audit-Run.")
    args = parser.parse_args()

    if args.once:
        process_once()
        return

    while True:
        processed = process_once()
        if not processed:
            logger.info(
                "worker_idle",
                extra={"event_name": "worker_idle", "event_payload": {"sleep_seconds": 10}},
            )
        time.sleep(10)


if __name__ == "__main__":
    main()
