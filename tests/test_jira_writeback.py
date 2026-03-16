from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import AuditFinding, AuditLocation, AuditTarget, CreateAuditRunRequest
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
        self.headers: dict[str, str] = {}

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
        if url.endswith("/rest/api/3/issue/createmeta/FINAI/issuetypes"):
            return _FakeResponse({"issueTypes": [{"id": "10067", "name": "Story", "subtask": False}]})
        raise AssertionError(f"Unexpected GET URL: {url} params={params} headers={headers or self._headers}")

    def post(self, url: str, json: dict[str, object] | None = None) -> _FakeResponse:
        assert url == "https://api.atlassian.com/ex/jira/cloud-jira-1/rest/api/3/issue"
        assert isinstance(json, dict)
        return _FakeResponse({"id": "10001", "key": "FINAI-321"})


class _FallbackIssueTypeJiraClient(_FakeJiraClient):
    def get(self, url: str, params: dict[str, object] | None = None, headers: dict[str, str] | None = None) -> _FakeResponse:
        if url.endswith("/oauth/token/accessible-resources"):
            return super().get(url, params=params, headers=headers)
        if url.endswith("/rest/api/3/issue/createmeta/FINAI/issuetypes"):
            return _FakeResponse(
                {
                    "issueTypes": [
                        {"id": "10067", "name": "Task", "subtask": False},
                        {"id": "10068", "name": "Epic", "subtask": False},
                    ]
                }
            )
        raise AssertionError(f"Unexpected GET URL: {url} params={params} headers={headers or self._headers}")

    def post(self, url: str, json: dict[str, object] | None = None) -> _FakeResponse:
        assert url == "https://api.atlassian.com/ex/jira/cloud-jira-1/rest/api/3/issue"
        assert isinstance(json, dict)
        fields = json.get("fields") or {}
        assert fields.get("issuetype") == {"name": "Task"}
        return _FakeResponse({"id": "10002", "key": "FINAI-322"})


class _FakeOAuthService:
    def get_valid_access_token_or_raise(self, *, required_scopes: set[str] | None = None) -> str:
        assert required_scopes == {"write:jira-work"}
        return "access-token"

    def build_scope_verification(
        self,
        *,
        required_scopes: set[str] | None = None,
        target_url: str | None = None,
        target_type: str | None = None,
    ) -> dict[str, object]:
        return {
            "required_scopes": sorted(required_scopes or []),
            "granted_scopes": ["write:jira-work"],
            "target_url": target_url,
            "target_type": target_type,
            "oauth_ready": True,
        }


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
            verification_metadata={
                "resource_id": "cloud-jira-1",
                "resource_url": "https://finius.atlassian.net",
            },
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


def test_jira_connector_falls_back_to_project_issue_type(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("fin_ai_auditor.services.connectors.jira_connector.httpx.Client", _FallbackIssueTypeJiraClient)
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=tmp_path / "metamodel.json")
    connector = JiraTicketingConnector(settings=settings)

    result = connector.create_ticket(
        target=JiraTicketTarget(project_key="FINAI", board_url=settings.jira_board_url),
        issue_payload={
            "fields": {
                "project": {"key": "FINAI"},
                "issuetype": {"name": "Story"},
                "summary": "Test",
            }
        },
        access_token="access-token",
    )

    assert result.issue_key == "FINAI-322"
    assert result.verification_metadata["resolved_issue_type"] == "Task"


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
            analysis_mode="deep",
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
    assert executed.implemented_changes[0].metadata["writeback_verification"]["verified"] is True
    assert executed.implemented_changes[0].metadata["writeback_verification"]["resource_id"] == "cloud-jira-1"
    assert executed.implemented_changes[0].jira_ticket is not None
    assert executed.implemented_changes[0].jira_ticket.ticket_key == "FINAI-321"
    assert executed.approval_requests[0].status == "executed"
    jira_log_entries = [entry for entry in executed.analysis_log if entry.title == "Jira-Writeback ausgefuehrt"]
    assert jira_log_entries
    assert jira_log_entries[-1].metadata["writeback_verification"]["resource_id"] == "cloud-jira-1"


def test_audit_service_reuses_existing_jira_writeback_idempotently(tmp_path: Path) -> None:
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
            analysis_mode="deep",
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
    run = service.create_writeback_approval_request(
        run_id=run.run_id,
        target_type="jira_ticket_create",
        title="FIN-AI Codeanpassungs-Ticket erstellen",
        summary="Das Ticket soll nach expliziter Freigabe extern in Jira erstellt werden.",
        target_url=settings.jira_board_url,
        related_package_ids=[],
        related_finding_ids=[],
        payload_preview=[],
    )
    approval_request_id = run.approval_requests[0].approval_request_id
    service.resolve_writeback_approval_request(
        run_id=run.run_id,
        approval_request_id=approval_request_id,
        decision="approve",
        comment_text="Freigegeben",
    )

    first = service.execute_jira_ticket_writeback(run_id=run.run_id, approval_request_id=approval_request_id)
    second = service.execute_jira_ticket_writeback(run_id=run.run_id, approval_request_id=approval_request_id)

    assert len(first.implemented_changes) == 1
    assert len(second.implemented_changes) == 1
    assert second.approval_requests[0].status == "executed"
    assert any(entry.title == "Writeback bereits ausgefuehrt" for entry in second.analysis_log)


def test_jira_writeback_failure_persists_scope_mismatch_metadata(tmp_path: Path) -> None:
    class _MissingScopeOAuthService(_FakeOAuthService):
        def get_valid_access_token_or_raise(self, *, required_scopes: set[str] | None = None) -> str:
            raise ValueError("Dem aktuellen Atlassian-Kontext fehlen die noetigen Scopes fuer diesen Writeback: write:jira-work")

        def build_scope_verification(
            self,
            *,
            required_scopes: set[str] | None = None,
            target_url: str | None = None,
            target_type: str | None = None,
        ) -> dict[str, object]:
            return {
                "required_scopes": sorted(required_scopes or []),
                "granted_scopes": [],
                "missing_scopes": sorted(required_scopes or []),
                "target_url": target_url,
                "target_type": target_type,
                "oauth_ready": True,
                "token_valid": True,
            }

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
        atlassian_oauth_service=_MissingScopeOAuthService(),
        jira_ticketing_connector=_FakeJiraConnector(),
    )
    run = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="deep",
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
    run = service.create_writeback_approval_request(
        run_id=run.run_id,
        target_type="jira_ticket_create",
        title="FIN-AI Codeanpassungs-Ticket erstellen",
        summary="Das Ticket soll nach expliziter Freigabe extern in Jira erstellt werden.",
        target_url=settings.jira_board_url,
        related_package_ids=[],
        related_finding_ids=[],
        payload_preview=[],
    )
    approval_request_id = run.approval_requests[0].approval_request_id
    service.resolve_writeback_approval_request(
        run_id=run.run_id,
        approval_request_id=approval_request_id,
        decision="approve",
        comment_text="Freigegeben",
    )

    with pytest.raises(ValueError, match="fehlen die noetigen Scopes"):
        service.execute_jira_ticket_writeback(run_id=run.run_id, approval_request_id=approval_request_id)

    persisted = service.get_run(run_id=run.run_id)
    assert persisted is not None
    approval = persisted.approval_requests[0]
    assert approval.metadata["last_execution_error"]["failure_class"] == "scope_mismatch"
    assert approval.metadata["writeback_verification"]["required_scopes"] == ["write:jira-work"]
    assert approval.metadata["writeback_preflight"]["status"] == "blocked"


def test_build_jira_ticket_brief_includes_write_sink_context(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=tmp_path / "metamodel.json",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)
    run = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="deep",
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
    finding = AuditFinding(
        severity="high",
        category="implementation_drift",
        title="Statement Write-Pfad driftet",
        summary="Write-Entscheider, API und Sink sind nicht konsistent.",
        recommendation="Write-Kette angleichen.",
        canonical_key="Statement.write_path",
        metadata={
            "atomic_fact_summary": "Statement.write_path: Confluence vs. Code widersprechen sich oder lassen denselben Sachverhalt unvollstaendig.",
            "action_lane": "confluence_and_jira",
            "causal_write_decider_labels": ["JobWorker.persist_statement"],
            "causal_write_apis": ["repo.save"],
            "causal_repository_adapters": ["StatementRepository"],
            "causal_repository_adapter_symbols": ["finai.repositories.statement_repository.StatementRepository"],
            "causal_driver_adapters": ["Neo4jDriver"],
            "causal_driver_adapter_symbols": ["finai.infra.neo4j_driver.Neo4jDriver"],
            "causal_transaction_boundaries": ["Neo4jDriver.session"],
            "causal_retry_paths": ["@retry"],
            "causal_persistence_targets": ["CustomerGraph.Node.Statement"],
            "causal_persistence_sink_kinds": ["node_sink"],
            "causal_persistence_backends": ["neo4j"],
            "causal_persistence_operation_types": ["neo4j_merge_node"],
            "causal_persistence_schema_targets": ["Node:Statement"],
            "causal_schema_validated_targets": ["Node:Statement"],
            "causal_schema_validation_statuses": ["ssot_confirmed"],
        },
    )

    brief = build_jira_ticket_brief(run=run, findings=[finding])

    assert any("Write-Decider: JobWorker.persist_statement" == item for item in brief.affected_parts)
    assert any(item.startswith("Atomarer Fakt: Statement.write_path: Confluence vs. Code") for item in brief.affected_parts)
    assert any("Aktionsspur: confluence_and_jira" == item for item in brief.affected_parts)
    assert any("DB-Write-API: repo.save" == item for item in brief.affected_parts)
    assert any("Repository-Adapter: StatementRepository" == item for item in brief.affected_parts)
    assert any("Repository-Symbol: finai.repositories.statement_repository.StatementRepository" == item for item in brief.affected_parts)
    assert any("Driver-Adapter: Neo4jDriver" == item for item in brief.affected_parts)
    assert any("Driver-Symbol: finai.infra.neo4j_driver.Neo4jDriver" == item for item in brief.affected_parts)
    assert any("Transaktion: Neo4jDriver.session" == item for item in brief.affected_parts)
    assert any("Node-Sink -> CustomerGraph.Node.Statement" in item for item in brief.affected_parts)
    assert any("Persistenz-Backend: neo4j" == item for item in brief.affected_parts)
    assert any("Persistenz-Op: neo4j_merge_node" == item for item in brief.evidence)
    assert any(item.startswith("Atomarer Fakt: Statement.write_path: Confluence vs. Code") for item in brief.evidence)
    assert any("Repository-Symbol: finai.repositories.statement_repository.StatementRepository" == item for item in brief.evidence)
    assert any("Driver-Symbol: finai.infra.neo4j_driver.Neo4jDriver" == item for item in brief.evidence)
    assert any("Schema-Ziel: Node:Statement" == item for item in brief.affected_parts)
    assert any("Schema-Status: ssot_confirmed" == item for item in brief.affected_parts)
    assert any("SSOT-bestaetigt: Node:Statement" == item for item in brief.affected_parts)
    assert any("repo.save" in item for item in brief.acceptance_criteria)
    assert any("neo4j_merge_node" in item for item in brief.acceptance_criteria)
    assert any("ssot_confirmed" in item for item in brief.acceptance_criteria)
    assert any("CustomerGraph.Node.Statement" in item for item in brief.validation_steps)
    assert any("Transaktions" in item for item in brief.validation_steps)


def test_writeback_approval_preview_includes_atomic_facts_and_action_lane(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=tmp_path / "metamodel.json",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)
    run = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="deep",
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
    finding = AuditFinding(
        severity="high",
        category="implementation_drift",
        title="Statement Write-Pfad driftet",
        summary="Write-Entscheider, API und Sink sind nicht konsistent.",
        recommendation="Write-Kette angleichen.",
        canonical_key="Statement.write_path",
        metadata={"object_key": "Statement.write_path"},
        locations=[
            AuditLocation(
                source_type="confluence_page",
                source_id="page-1",
                title="Statement Contract",
            ),
            AuditLocation(
                source_type="github_file",
                source_id="src/statement.py",
                title="statement.py",
            ),
        ],
    )
    snapshot_ids = service._build_demo_snapshot_ids()
    claim = service._build_demo_claims(run=run, snapshot_ids=snapshot_ids)[0]
    source_snapshot = service._build_demo_snapshots(run=run, snapshot_ids=snapshot_ids)[0]
    updated_run = service._repository.upsert_run(
        run=run.model_copy(update={"findings": [finding], "claims": [claim], "source_snapshots": [source_snapshot]})
    )

    approval_run = service.create_writeback_approval_request(
        run_id=updated_run.run_id,
        target_type="jira_ticket_create",
        title="FIN-AI Codeanpassungs-Ticket erstellen",
        summary="Das Ticket soll nach expliziter Freigabe extern in Jira erstellt werden.",
        target_url=settings.jira_board_url,
        related_package_ids=[],
        related_finding_ids=[finding.finding_id],
        payload_preview=[],
    )

    approval = approval_run.approval_requests[0]
    assert any(item.startswith("Fakt: Statement.write_path: Confluence vs. Code") for item in approval.payload_preview)
    assert "Aktionsspur: confluence_and_jira" in approval.payload_preview
    assert "writeback_preflight" in approval.metadata
    assert "execution_token" in approval.metadata


def test_jira_writeback_approval_request_blocks_disallowed_project(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=tmp_path / "metamodel.json",
        fixed_jira_project_key="OTHER",
        allowed_writeback_jira_project_keys=["FINAI"],
        jira_board_url="https://finius.atlassian.net/jira/software/projects/OTHER/boards/1",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)
    run = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="deep",
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

    with pytest.raises(ValueError, match="Projekt OTHER ist nicht freigegeben"):
        service.create_writeback_approval_request(
            run_id=run.run_id,
            target_type="jira_ticket_create",
            title="FIN-AI Codeanpassungs-Ticket erstellen",
            summary="Das Ticket soll nach expliziter Freigabe extern in Jira erstellt werden.",
            target_url=settings.jira_board_url,
            related_package_ids=[],
            related_finding_ids=[],
            payload_preview=[],
        )


def test_jira_writeback_execution_rechecks_target_policy(tmp_path: Path) -> None:
    repository = SQLiteAuditRepository(db_path=tmp_path / "auditor.db")
    allowed_settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=tmp_path / "metamodel.json",
        fixed_jira_project_key="FINAI",
        allowed_writeback_jira_project_keys=["FINAI"],
        jira_board_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
    )
    service = AuditService(repository=repository, settings=allowed_settings)
    run = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="deep",
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
    run = service.create_writeback_approval_request(
        run_id=run.run_id,
        target_type="jira_ticket_create",
        title="FIN-AI Codeanpassungs-Ticket erstellen",
        summary="Das Ticket soll nach expliziter Freigabe extern in Jira erstellt werden.",
        target_url=allowed_settings.jira_board_url,
        related_package_ids=[],
        related_finding_ids=[],
        payload_preview=[],
    )
    approval_request_id = run.approval_requests[0].approval_request_id
    service.resolve_writeback_approval_request(
        run_id=run.run_id,
        approval_request_id=approval_request_id,
        decision="approve",
        comment_text="Freigegeben",
    )

    blocked_service = AuditService(
        repository=repository,
        settings=Settings(
            database_path=tmp_path / "auditor.db",
            metamodel_dump_path=tmp_path / "metamodel.json",
            fixed_jira_project_key="OTHER",
            allowed_writeback_jira_project_keys=["OTHER"],
            jira_board_url="https://finius.atlassian.net/jira/software/projects/OTHER/boards/1",
        ),
        atlassian_oauth_service=_FakeOAuthService(),
        jira_ticketing_connector=_FakeJiraConnector(),
    )

    with pytest.raises(ValueError, match="Projekt FINAI ist nicht freigegeben"):
        blocked_service.execute_jira_ticket_writeback(run_id=run.run_id, approval_request_id=approval_request_id)


def test_jira_connector_retries_after_rate_limit(monkeypatch, tmp_path: Path) -> None:
    class _RetryingJiraClient(_FakeJiraClient):
        issue_attempts = 0

        def post(self, url: str, json: dict[str, object] | None = None) -> _FakeResponse:
            type(self).issue_attempts += 1
            if type(self).issue_attempts == 1:
                response = _FakeResponse({"error": "rate limited"}, status_code=429)
                response.headers = {"Retry-After": "0"}
                return response
            return super().post(url, json=json)

    monkeypatch.setattr("fin_ai_auditor.services.connectors.jira_connector.httpx.Client", _RetryingJiraClient)
    monkeypatch.setattr("fin_ai_auditor.services.connectors.jira_connector._sleep_before_retry", lambda attempt, retry_after: None)
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=tmp_path / "metamodel.json")
    connector = JiraTicketingConnector(settings=settings)

    result = connector.create_ticket(
        target=JiraTicketTarget(project_key="FINAI", board_url=settings.jira_board_url),
        issue_payload={"fields": {"project": {"key": "FINAI"}, "summary": "Test"}},
        access_token="access-token",
    )

    assert result.issue_key == "FINAI-321"
    assert _RetryingJiraClient.issue_attempts == 2
