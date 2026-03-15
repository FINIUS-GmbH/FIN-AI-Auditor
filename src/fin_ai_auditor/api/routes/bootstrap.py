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
from fin_ai_auditor.services.operational_readiness import (
    build_go_live_gate_summary,
    build_operational_alert_summary,
    build_runtime_guard,
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
    default_repo_path = settings.default_finai_local_repo_path
    defaults_are_portable = not default_repo_path.is_absolute()
    runtime_access = atlassian_service.get_runtime_access_status()
    atlassian_status = runtime_access["auth_status"]
    granted_scopes = set(runtime_access["granted_scopes"])
    configured_scopes = set(runtime_access["configured_scopes"])
    confluence_read_scopes = {
        "read:confluence-content.summary",
        "read:confluence-content.all",
        "read:page:confluence",
        "read:content-details:confluence",
    }
    jira_write_scopes = {"write:jira-work"}
    atlassian_oauth_ready = bool(
        runtime_access["oauth_ready"]
    )
    confluence_live_read_ready = bool(
        runtime_access["token_available"] and granted_scopes.intersection(confluence_read_scopes)
    )
    jira_write_scope_ready = bool(
        runtime_access["token_available"] and jira_write_scopes.issubset(granted_scopes)
    )
    gold_set_gate = evaluate_reference_gold_set()
    delta_gate = evaluate_reference_delta_gold_set()
    runtime_guard = build_runtime_guard(settings=settings, repository=repository, context="api")
    persistence_profile = runtime_guard["persistence_profile"]
    observability = repository.get_runtime_observability_summary()
    worker_recovery = repository.get_stale_run_recovery_summary(now_iso=utc_now_iso())
    operational_alerts = build_operational_alert_summary(
        observability=observability,
        worker_recovery=worker_recovery,
    )
    go_live_gate = build_go_live_gate_summary(
        runtime_guard=runtime_guard,
        gold_set_gate={
            "passed": gold_set_gate.passed,
            "failure_reasons": gold_set_gate.failure_reasons,
        },
        delta_gate={
            "passed": delta_gate.passed,
            "failure_reasons": delta_gate.failure_reasons,
        },
        alert_summary=operational_alerts,
        confluence_live_read_ready=confluence_live_read_ready,
        jira_writeback_ready=jira_write_scope_ready,
    )
    return {
        "app_name": settings.app_name,
        "operational_mode": settings.operational_mode,
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
            "writeback_target_mode": settings.writeback_target_mode,
            "allowed_confluence_space_keys": settings.get_allowed_writeback_confluence_space_keys(),
            "allowed_jira_project_keys": settings.get_allowed_writeback_jira_project_keys(),
            "summary": (
                "Bis zu einer expliziten User-Entscheidung erfolgen alle Analysezugriffe auf GitHub, "
                "Confluence und Metamodell ausschliesslich lesend. Jira wird in diesem Modus nicht "
                "lesend analysiert, sondern nur spaeter als Ziel fuer Codeaenderungs-Tickets verwendet. "
                "Schreibend genutzt werden darf nur die lokale FIN-AI Auditor Datenbank."
            ),
        },
        "secret_storage": repository.get_secret_storage_summary(),
        "persistence_profile": persistence_profile,
        "runtime_guard": runtime_guard,
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
            "deployment_profile": {
                "operational_mode": settings.operational_mode,
                "portable_defaults": defaults_are_portable,
                "notes": (
                    ["Defaults sind relativ gehalten und dadurch ohne benutzerspezifischen Pfad portabler."]
                    if defaults_are_portable
                    else [
                        "Der Default-Repo-Pfad ist noch workstation-spezifisch und sollte fuer Team-Betrieb explizit gesetzt werden."
                    ]
                ),
            },
            "secret_storage": repository.get_secret_storage_summary(),
            "persistence_profile": persistence_profile,
            "runtime_guard": runtime_guard,
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
                    [
                        "Confluence kann aktuell live und read-only gegen echte FINAI-Seiten verifiziert werden.",
                        *(
                            ["Der lokale Token wurde fuer die Readiness-Pruefung erfolgreich refreshiert."]
                            if runtime_access.get("refreshed_during_check")
                            else []
                        ),
                    ]
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
                    [
                        "Jira-Writeback ist technisch und scope-seitig bereit, bleibt aber weiter approval-gesteuert.",
                        *(
                            ["Der lokale Token wurde fuer die Readiness-Pruefung erfolgreich refreshiert."]
                            if runtime_access.get("refreshed_during_check")
                            else []
                        ),
                    ]
                    if jira_write_scope_ready
                    else [
                        "Fuer externen Jira-Writeback fehlt aktuell ein gueltiger Token mit write:jira-work."
                    ]
                ),
            },
            "writeback_target_policy": {
                "ready": settings.writeback_target_mode != "disabled",
                "mode": settings.writeback_target_mode,
                "allowed_confluence_space_keys": settings.get_allowed_writeback_confluence_space_keys(),
                "allowed_jira_project_keys": settings.get_allowed_writeback_jira_project_keys(),
                "notes": (
                    ["Externer Writeback ist komplett deaktiviert."]
                    if settings.writeback_target_mode == "disabled"
                    else [
                        "Externer Writeback ist nur fuer explizit freigegebene Confluence-Spaces und Jira-Projekte erlaubt."
                    ]
                ),
            },
            "operational_alerts": operational_alerts,
            "go_live_gate": go_live_gate,
        },
        "observability": observability,
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
        "worker_recovery": worker_recovery,
        "go_live_gate": go_live_gate,
        "operational_alerts": operational_alerts,
        "confluence_analysis_cache": repository.get_confluence_analysis_cache_summary(),
        "atlassian_auth": atlassian_status.model_dump(mode="json"),
    }
