from __future__ import annotations

from fastapi import APIRouter, Depends

from fin_ai_auditor.api.dependencies import get_atlassian_oauth_service, get_repository
from fin_ai_auditor.config import get_settings
from fin_ai_auditor.domain.models import utc_now_iso
from fin_ai_auditor.llm import is_embedding_slot_configured
from fin_ai_auditor.services.atlassian_oauth_service import AtlassianOAuthService
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository
from fin_ai_auditor.services.gold_set_benchmark import (
    evaluate_reference_delta_gold_set,
    evaluate_reference_gold_set,
)

router = APIRouter(prefix="/api")


@router.get("/bootstrap")
def bootstrap(
    atlassian_service: AtlassianOAuthService = Depends(get_atlassian_oauth_service),
    repository: SQLiteAuditRepository = Depends(get_repository),
) -> dict[str, object]:
    settings = get_settings()
    configured_llm_slots = settings.get_configured_llm_slots()
    direct_metamodel = settings.get_direct_metamodel_config()
    atlassian_status = atlassian_service.get_auth_status()
    granted_scopes = atlassian_service.get_granted_scope_set()
    configured_scopes = atlassian_service.get_configured_scope_set()
    confluence_read_scopes = {
        "read:confluence-content.summary",
        "read:confluence-content.all",
        "read:page:confluence",
        "read:content-details:confluence",
    }
    jira_write_scopes = {"write:jira-work"}
    atlassian_oauth_ready = bool(
        atlassian_status.enabled
        and atlassian_status.client_configured
        and atlassian_status.redirect_uri_matches_local_api
    )
    confluence_live_read_ready = bool(
        atlassian_status.token_valid and granted_scopes.intersection(confluence_read_scopes)
    )
    jira_write_scope_ready = bool(
        atlassian_status.token_valid and jira_write_scopes.issubset(granted_scopes)
    )
    gold_set_gate = evaluate_reference_gold_set()
    delta_gate = evaluate_reference_delta_gold_set()
    return {
        "app_name": settings.app_name,
        "defaults": {
            "github_repo_url": settings.default_finai_github_repo_url,
            "local_repo_path": str(settings.default_finai_local_repo_path),
            "github_ref": settings.default_finai_github_ref,
            "confluence_space_keys": [settings.fixed_confluence_space_key],
            "confluence_page_ids": [],
            "jira_project_keys": [settings.fixed_jira_project_key],
            "include_metamodel": True,
            "include_local_docs": True,
        },
        "source_profile": {
            "confluence_url": settings.confluence_home_url,
            "jira_url": settings.jira_board_url,
            "confluence_space_key": settings.fixed_confluence_space_key,
            "jira_project_key": settings.fixed_jira_project_key,
            "jira_usage": "ticket_creation_only",
            "metamodel_dump_path": str(settings.metamodel_dump_path),
            "metamodel_policy": "direct_read_with_local_dump_fallback" if direct_metamodel else "always_refresh_before_run",
            "metamodel_source": "DIRECT" if direct_metamodel else "LOCAL_DUMP_ONLY",
            "resource_access_mode": settings.external_resource_access_mode,
        },
        "resource_access_policy": {
            "mode": settings.external_resource_access_mode,
            "external_write_requires_user_decision": settings.external_write_requires_user_decision,
            "local_database_is_only_writable_store": settings.local_database_is_only_writable_store,
            "summary": (
                "Bis zu einer expliziten User-Entscheidung erfolgen alle Analysezugriffe auf GitHub, "
                "Confluence und Metamodell ausschliesslich lesend. Jira wird in diesem Modus nicht "
                "lesend analysiert, sondern nur spaeter als Ziel fuer Codeaenderungs-Tickets verwendet. "
                "Schreibend genutzt werden darf nur die lokale FIN-AI Auditor Datenbank."
            ),
        },
        "capabilities": {
            "local_repo_enabled": True,
            "fixed_atlassian_sources": True,
            "metamodel_always_included": True,
            "metamodel_direct_configured": direct_metamodel is not None,
            "jira_analysis_enabled": False,
            "jira_ticket_creation_enabled": True,
            "external_read_only_until_user_decision": settings.external_write_requires_user_decision,
            "atlassian_configured": bool(
                settings.atlassian_oauth_client_id and settings.atlassian_oauth_client_secret
            ),
            "atlassian_oauth_ready": atlassian_oauth_ready,
            "confluence_live_read_ready": confluence_live_read_ready,
            "jira_write_scope_ready": jira_write_scope_ready,
            "llm_configured": bool(configured_llm_slots),
            "llm_slot_count": len(configured_llm_slots),
            "llm_slots": [
                {
                    "slot": int(sc.slot),
                    "display_name": str(sc.display_name or sc.model or f"Slot {sc.slot}"),
                    "model": str(sc.model or ""),
                    "deployment": str(sc.deployment or ""),
                    "provider": str(sc.provider or ""),
                    "purpose": "embedding" if is_embedding_slot_configured(settings=settings, llm_slot=int(sc.slot)) else "chat",
                }
                for sc in configured_llm_slots
            ],
        },
        "operational_readiness": {
            "atlassian_oauth": {
                "ready": atlassian_oauth_ready,
                "granted_scopes": sorted(granted_scopes),
                "configured_scopes": sorted(configured_scopes),
                "notes": (
                    ["OAuth-Consent kann lokal ueber den Auditor durchgefuehrt werden."]
                    if atlassian_oauth_ready
                    else [
                        "OAuth-Consent ist noch nicht voll betriebsbereit. Redirect-URI, Client-Konfiguration oder Aktivierung muessen geprueft werden."
                    ]
                ),
            },
            "confluence_live_read": {
                "ready": confluence_live_read_ready,
                "required_scopes": sorted(confluence_read_scopes),
                "granted_scopes": sorted(granted_scopes),
                "notes": (
                    ["Confluence kann aktuell live und read-only gegen echte FINAI-Seiten verifiziert werden."]
                    if confluence_live_read_ready
                    else [
                        "Fuer echte Confluence-Live-Reads braucht der Auditor einen gueltigen lokalen Access Token mit Confluence-Read-Scope."
                    ]
                ),
            },
            "jira_writeback": {
                "ready": jira_write_scope_ready,
                "required_scopes": sorted(jira_write_scopes),
                "granted_scopes": sorted(granted_scopes),
                "notes": (
                    ["Jira-Writeback ist technisch und scope-seitig bereit, bleibt aber weiter approval-gesteuert."]
                    if jira_write_scope_ready
                    else [
                        "Fuer externen Jira-Writeback fehlt aktuell ein gueltiger Token mit write:jira-work."
                    ]
                ),
            },
        },
        "observability": repository.get_runtime_observability_summary(),
        "atomic_fact_registry": repository.get_atomic_fact_registry_summary(),
        "quality_gate": {
            "gold_set": {
                "passed": gold_set_gate.passed,
                "required_recall": gold_set_gate.required_recall,
                "required_precision": gold_set_gate.required_precision,
                "max_false_positives": gold_set_gate.max_false_positives,
                "recall": gold_set_gate.evaluation.recall,
                "precision": gold_set_gate.evaluation.precision,
                "false_positives": gold_set_gate.evaluation.false_positives,
                "matched_expectations": gold_set_gate.evaluation.matched_expectations,
                "total_expectations": gold_set_gate.evaluation.total_expectations,
                "missing_expectation_labels": gold_set_gate.evaluation.missing_expectation_labels,
                "false_positive_labels": gold_set_gate.evaluation.false_positive_labels,
                "failure_reasons": gold_set_gate.failure_reasons,
            },
            "delta_recompute": {
                "passed": delta_gate.passed,
                "required_recall": delta_gate.required_recall,
                "required_precision": delta_gate.required_precision,
                "max_false_positives": delta_gate.max_false_positives,
                "recall": delta_gate.evaluation.recall,
                "precision": delta_gate.evaluation.precision,
                "false_positives": delta_gate.evaluation.false_positives,
                "matched_expectations": delta_gate.evaluation.matched_expectations,
                "total_expectations": delta_gate.evaluation.total_expectations,
                "missing_expectation_labels": delta_gate.evaluation.missing_expectation_labels,
                "false_positive_labels": delta_gate.evaluation.false_positive_labels,
                "failure_reasons": delta_gate.failure_reasons,
            },
        },
        "worker_recovery": repository.get_stale_run_recovery_summary(now_iso=utc_now_iso()),
        "confluence_analysis_cache": repository.get_confluence_analysis_cache_summary(),
        "atlassian_auth": atlassian_status.model_dump(mode="json"),
    }
