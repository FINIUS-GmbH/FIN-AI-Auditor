from __future__ import annotations

from fastapi import APIRouter, Depends

from fin_ai_auditor.api.dependencies import get_repository
from fin_ai_auditor.config import get_settings
from fin_ai_auditor.domain.models import utc_now_iso
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository

router = APIRouter(prefix="/api")


@router.get("/health")
def health(
    repository: SQLiteAuditRepository = Depends(get_repository),
) -> dict[str, object]:
    settings = get_settings()
    return {
        "ok": True,
        "service": settings.app_name,
        "env": settings.env,
        "observability": repository.get_runtime_observability_summary(),
        "lease_recovery": repository.get_stale_run_recovery_summary(now_iso=utc_now_iso()),
        "confluence_analysis_cache": repository.get_confluence_analysis_cache_summary(),
    }
