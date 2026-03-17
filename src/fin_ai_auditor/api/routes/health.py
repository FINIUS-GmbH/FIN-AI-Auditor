from __future__ import annotations

from fastapi import APIRouter, Depends

from fin_ai_auditor.api.dependencies import get_repository
from fin_ai_auditor.config import get_settings
from fin_ai_auditor.domain.models import utc_now_iso
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository
from fin_ai_auditor.services.gold_set_benchmark import (
    evaluate_reference_delta_gold_set,
    evaluate_reference_gold_set,
)
from fin_ai_auditor.services.operational_readiness import (
    build_go_live_gate_summary,
    build_operational_alert_summary,
    build_runtime_guard,
)

router = APIRouter(prefix="/api")


@router.get("/health")
def health(
    repository: SQLiteAuditRepository = Depends(get_repository),
) -> dict[str, object]:
    settings = get_settings()
    runtime_guard = build_runtime_guard(settings=settings, repository=repository, context="api")
    observability = repository.get_runtime_observability_summary()
    lease_recovery = repository.get_stale_run_recovery_summary(now_iso=utc_now_iso())
    operational_alerts = build_operational_alert_summary(
        observability=observability,
        worker_recovery=lease_recovery,
    )
    gold_gate = evaluate_reference_gold_set()
    delta_gate = evaluate_reference_delta_gold_set()
    go_live_gate = build_go_live_gate_summary(
        runtime_guard=runtime_guard,
        gold_set_gate={
            "passed": gold_gate.passed,
            "failure_reasons": gold_gate.failure_reasons,
        },
        delta_gate={
            "passed": delta_gate.passed,
            "failure_reasons": delta_gate.failure_reasons,
        },
        alert_summary=operational_alerts,
    )
    return {
        "ok": True,
        "service": settings.app_name,
        "env": settings.env,
        "operational_mode": settings.operational_mode,
        "writeback_target_policy": {
            "mode": settings.writeback_target_mode,
            "allowed_confluence_space_keys": settings.get_allowed_writeback_confluence_space_keys(),
            "allowed_jira_project_keys": settings.get_allowed_writeback_jira_project_keys(),
        },
        "secret_storage": repository.get_secret_storage_summary(),
        "persistence_profile": runtime_guard["persistence_profile"],
        "runtime_guard": runtime_guard,
        "observability": observability,
        "lease_recovery": lease_recovery,
        "operational_alerts": operational_alerts,
        "go_live_gate": go_live_gate,
        "confluence_analysis_cache": repository.get_confluence_analysis_cache_summary(),
    }
