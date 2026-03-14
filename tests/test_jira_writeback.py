from __future__ import annotations

from pathlib import Path

import httpx

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import AuditTarget, CreateAuditRunRequest
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository
from fin_ai_auditor.services.audit_service import AuditService
from fin_ai_auditor.services.connectors.jira_connector import (
    JiraCreatedIssue,
    JiraTicketTarget,
    JiraTicketingConnector,
)
from fin_ai_auditor.services.jira_ticket_writer import build_jira_issue_payload
from fin_ai_auditor.services.change_payloads import build_jira_ticket_brief


class _FakeResponse:
    def __init__(self, payload: object, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "boom",
                request=httpx.Request("POST", "https://example.com"),
                response=httpx.Response(self.status_code),
            )

    def json(self) -> object:
        return self._payload


class _FakeJiraClient:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self._headers = kwargs.get("headers", {})

    def __enter__(self) -> "_FakeJiraClient":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def get(self, url: str, params: dict[str, object] | None = None, headers: dict[str, str] | None = None) -> _FakeResponse:
        if url.endswith("/oauth/token/accessible-resources"):
            return _FakeResponse(
                [
                    {
                        "id": "cloud-jira-1",
                        "url": "https://finius.atlassian.net",
                        "scopes": ["read:jira-work", "write:jira-work"],
                    }
                ]
            )
        raise AssertionError(f"Unexpected GET URL: {url} params={params} headers={headers or self._headers}")

    def post(self, url: str, json: dict[str, object] | None = None) -> _FakeResponse:
        assert url == "https://api.atlassian.com/ex/jira/cloud-jira-1/rest/api/3/issue"
        assert isinstance(json, dict)
        return _FakeResponse({"id": "10001", "key": "FINAI-321"})


class _FakeOAuthService:
    def get_valid_access_token_or_raise(self, *, required_scopes: set[str] | None = None) -> str:
        assert required_scopes == {"write:jira-work"}
        return "access-token"


class _FakeJiraConnector:
    def create_ticket(
        self,
        *,
        target: JiraTicketTarget,
        issue_payload: dict[str, object],
        access_token: str,
    ) -> JiraCreatedIssue:
        assert target.project_key == "FINAI"
        assert "description" in issue_payload.get("fields", {})
        assert access_token == "access-token"
        return JiraCreatedIssue(
            issue_id="10001",
            issue_key="FINAI-321",
            issue_url="https://finius.atlassian.net/browse/FINAI-321",
            site_base_url="https://finius.atlassian.net",
            response_payload={"id": "10001", "key": "FINAI-321"},
        )


def test_jira_connector_creates_ticket_via_accessible_resources(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("fin_ai_auditor.services.connectors.jira_connector.httpx.Client", _FakeJiraClient)
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=tmp_path / "metamodel.json")
    connector = JiraTicketingConnector(settings=settings)

    result = connector.create_ticket(
        target=JiraTicketTarget(project_key="FINAI", board_url=settings.jira_board_url),
        issue_payload={"fields": {"project": {"key": "FINAI"}, "summary": "Test"}},
        access_token="access-token",
    )

    assert result.issue_key == "FINAI-321"
    assert result.issue_url == "https://finius.atlassian.net/browse/FINAI-321"


def test_audit_service_executes_approved_jira_writeback(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=tmp_path / "metamodel.json",
        mothership_url="",
        license_key="",
        license_tenant="",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(
        repository=repository,
        settings=settings,
        atlassian_oauth_service=_FakeOAuthService(),
        jira_ticketing_connector=_FakeJiraConnector(),
    )
    run = service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path=str(tmp_path),
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    run_with_approval = service.create_writeback_approval_request(
        run_id=run.run_id,
        target_type="jira_ticket_create",
        title="FIN-AI Codeanpassungs-Ticket erstellen",
        summary="Das Ticket soll nach expliziter Freigabe extern in Jira erstellt werden.",
        target_url=settings.jira_board_url,
        related_package_ids=[],
        related_finding_ids=[],
        payload_preview=[],
    )
    approval_request_id = run_with_approval.approval_requests[0].approval_request_id
    approved = service.resolve_writeback_approval_request(
        run_id=run.run_id,
        approval_request_id=approval_request_id,
        decision="approve",
        comment_text="Freigegeben",
    )

    executed = service.execute_jira_ticket_writeback(
        run_id=approved.run_id,
        approval_request_id=approval_request_id,
    )

    assert executed.implemented_changes
    assert executed.implemented_changes[0].target_label == "FINAI-321"
    assert executed.implemented_changes[0].metadata["execution_mode"] == "external_jira_api"
    assert executed.implemented_changes[0].jira_ticket is not None
    assert executed.implemented_changes[0].jira_ticket.ticket_key == "FINAI-321"
    assert executed.approval_requests[0].status == "executed"
