from pathlib import Path

from fastapi.testclient import TestClient

from fin_ai_auditor.api.app import create_app
from fin_ai_auditor.api.dependencies import get_atlassian_oauth_service, get_audit_service
from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import AuditTarget, AtlassianOAuthTokenRecord, CreateAuditRunRequest
from fin_ai_auditor.services.atlassian_oauth_service import AtlassianOAuthService
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository
from fin_ai_auditor.services.audit_service import AuditService


def test_health_returns_ok() -> None:
    client = TestClient(create_app())
    response = client.get("/api/health")

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["service"] == "FIN-AI Auditor"
    assert "observability" in body
    assert "lease_recovery" in body
    assert "confluence_analysis_cache" in body


def test_bootstrap_returns_local_repo_defaults() -> None:
    client = TestClient(create_app())
    response = client.get("/api/bootstrap")

    assert response.status_code == 200
    body = response.json()
    assert body["defaults"]["local_repo_path"] == "/Users/martinwaelter/GitHub/FIN-AI"
    assert body["defaults"]["confluence_space_keys"] == ["FP"]
    assert body["defaults"]["confluence_page_ids"] == []
    assert body["defaults"]["jira_project_keys"] == ["FINAI"]
    assert body["source_profile"]["confluence_url"] == "https://fin-ai.atlassian.net/wiki/spaces/FP/overview"
    assert (
        body["source_profile"]["jira_url"]
        == "https://finius.atlassian.net/jira/software/projects/FINAI/boards/67"
    )
    assert body["source_profile"]["jira_usage"] == "ticket_creation_only"
    assert body["source_profile"]["metamodel_policy"] == "direct_read_with_local_dump_fallback"
    assert body["source_profile"]["metamodel_source"] == "DIRECT"
    assert body["source_profile"]["resource_access_mode"] == "read_only"
    assert body["resource_access_policy"]["external_write_requires_user_decision"] is True
    assert body["resource_access_policy"]["local_database_is_only_writable_store"] is True
    assert body["capabilities"]["external_read_only_until_user_decision"] is True
    assert body["capabilities"]["metamodel_direct_configured"] is True
    assert body["capabilities"]["jira_analysis_enabled"] is False
    assert body["capabilities"]["jira_ticket_creation_enabled"] is True
    assert "atlassian_oauth_ready" in body["capabilities"]
    assert "confluence_live_read_ready" in body["capabilities"]
    assert "jira_write_scope_ready" in body["capabilities"]
    assert "atomic_fact_registry" in body
    assert "quality_gate" in body
    assert "gold_set" in body["quality_gate"]
    assert "delta_recompute" in body["quality_gate"]
    assert body["atlassian_auth"]["redirect_uri"] == "http://localhost:8088/api/ingestion/atlassian/auth/callback"


def test_bootstrap_exposes_operational_readiness(monkeypatch, tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        fixed_confluence_space_key="FINAI",
        fixed_jira_project_key="FINAI",
        confluence_home_url="https://finius.atlassian.net/wiki/home",
        jira_board_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
        port=8088,
        host="127.0.0.1",
        atlassian_enabled=True,
        atlassian_oauth_client_id="client-id",
        atlassian_oauth_client_secret="client-secret",
        atlassian_oauth_redirect_uri="http://localhost:8088/api/ingestion/atlassian/auth/callback",
        atlassian_oauth_scope=(
            "read:confluence-content.summary read:confluence-content.all "
            "write:jira-work"
        ),
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    repository.upsert_atlassian_token(
        token=AtlassianOAuthTokenRecord(
            access_token="access-token",
            refresh_token="refresh-token",
            scope="read:confluence-content.summary read:confluence-content.all",
            expires_at="2099-01-01T00:00:00+00:00",
        )
    )
    atlassian_service = AtlassianOAuthService(repository=repository, settings=settings)

    monkeypatch.setattr("fin_ai_auditor.api.routes.bootstrap.get_settings", lambda: settings)
    app = create_app()
    app.dependency_overrides[get_atlassian_oauth_service] = lambda: atlassian_service
    client = TestClient(app)

    response = client.get("/api/bootstrap")

    assert response.status_code == 200
    body = response.json()
    assert body["capabilities"]["atlassian_oauth_ready"] is True
    assert body["capabilities"]["confluence_live_read_ready"] is True
    assert body["capabilities"]["jira_write_scope_ready"] is False
    assert body["operational_readiness"]["confluence_live_read"]["ready"] is True
    assert body["operational_readiness"]["jira_writeback"]["ready"] is False
    assert "observability" in body
    assert "worker_recovery" in body
    assert "confluence_analysis_cache" in body
    assert body["quality_gate"]["gold_set"]["passed"] is True
    assert body["quality_gate"]["delta_recompute"]["passed"] is True


def test_decision_comment_endpoint_returns_updated_run(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        fixed_confluence_space_key="FINAI",
        fixed_jira_project_key="FINAI",
        confluence_home_url="https://finius.atlassian.net/wiki/home",
        jira_board_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
    )
    service = AuditService(
        repository=SQLiteAuditRepository(db_path=settings.database_path),
        settings=settings,
    )
    created = service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path="/Users/martinwaelter/GitHub/FIN-AI",
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )

    app = create_app()
    app.dependency_overrides[get_audit_service] = lambda: service
    client = TestClient(app)

    response = client.post(
        f"/api/audits/runs/{created.run_id}/decision-comments",
        json={
            "comment_text": "Statement darf nur im Review-Status geschrieben werden.",
            "related_finding_ids": ["finding_demo_1"],
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body["analysis_log"]) == 4
    assert body["analysis_log"][-1]["source_type"] == "recommendation_regeneration"


def test_jira_ticket_created_endpoint_returns_change_brief(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        fixed_confluence_space_key="FINAI",
        fixed_jira_project_key="FINAI",
        confluence_home_url="https://finius.atlassian.net/wiki/home",
        jira_board_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
    )
    service = AuditService(
        repository=SQLiteAuditRepository(db_path=settings.database_path),
        settings=settings,
    )
    created = service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path="/Users/martinwaelter/GitHub/FIN-AI",
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    completed = service.complete_run_with_demo_findings(run_id=created.run_id)
    package = completed.decision_packages[0]

    app = create_app()
    app.dependency_overrides[get_audit_service] = lambda: service
    client = TestClient(app)

    approval_response = client.post(
        f"/api/audits/runs/{completed.run_id}/approval-requests",
        json={
            "target_type": "jira_ticket_create",
            "title": f"Jira-Writeback fuer {package.title}",
            "summary": "Lokale Freigabeanfrage fuer das Jira-Ticket.",
            "target_url": "https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
            "related_package_ids": [package.package_id],
            "related_finding_ids": package.related_finding_ids,
            "payload_preview": ["Prompt-Entwurf", "Abnahmekriterien", "Betroffene Teile"],
        },
    )
    assert approval_response.status_code == 200
    approval_request_id = approval_response.json()["approval_requests"][0]["approval_request_id"]

    resolve_response = client.post(
        f"/api/audits/runs/{completed.run_id}/approval-requests/{approval_request_id}/decision",
        json={
            "decision": "approve",
            "comment_text": "AI-Coding-Brief darf erstellt werden.",
        },
    )
    assert resolve_response.status_code == 200

    response = client.post(
        f"/api/audits/runs/{completed.run_id}/implemented-changes/jira-ticket-created",
        json={
            "approval_request_id": approval_request_id,
            "ticket_key": "FINAI-123",
            "ticket_url": "https://finius.atlassian.net/browse/FINAI-123",
            "related_finding_ids": [completed.findings[0].finding_id],
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body["implemented_changes"]) == 1
    assert body["implemented_changes"][0]["jira_ticket"]["ticket_key"] == "FINAI-123"
    assert body["implemented_changes"][0]["jira_ticket"]["acceptance_criteria"]
    assert body["approval_requests"][0]["status"] == "executed"
