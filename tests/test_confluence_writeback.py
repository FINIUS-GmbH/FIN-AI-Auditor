from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import (
    AuditTarget,
    ConfluencePatchOperation,
    ConfluencePatchPreview,
    CreateAuditRunRequest,
)
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository
from fin_ai_auditor.services.audit_service import AuditService
from fin_ai_auditor.services.connectors.confluence_connector import (
    ConfluencePageTarget,
    ConfluencePageWriteConnector,
    ConfluenceUpdatedPage,
)


class _FakeResponse:
    def __init__(self, payload: object, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "boom",
                request=httpx.Request("GET", "https://example.com"),
                response=httpx.Response(self.status_code),
            )

    def json(self) -> object:
        return self._payload


class _FakeConfluenceClient:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self._headers = kwargs.get("headers", {})

    def __enter__(self) -> "_FakeConfluenceClient":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def get(self, url: str, params: dict[str, object] | None = None, headers: dict[str, str] | None = None) -> _FakeResponse:
        if url.endswith("/oauth/token/accessible-resources"):
            return _FakeResponse(
                [
                    {
                        "id": "cloud-1",
                        "url": "https://finius.atlassian.net",
                        "scopes": ["read:page:confluence", "write:page:confluence"],
                    }
                ]
            )
        if url.endswith("/wiki/api/v2/pages/page-1"):
            return _FakeResponse(
                {
                    "id": "page-1",
                    "title": "Statement Contract",
                    "spaceId": "space-1",
                    "version": {"number": 7},
                    "space": {"id": "space-1", "key": "FINAI", "name": "FIN-AI"},
                    "body": {
                        "storage": {
                            "value": "<h1>Statement</h1><p>Write path must stay approval-gated.</p>"
                        }
                    },
                }
            )
        if url.endswith("/wiki/api/v2/pages/page-1/ancestors"):
            return _FakeResponse({"results": [{"id": "parent-1", "title": "Contracts"}]})
        if url.endswith("/wiki/api/v2/spaces/space-1"):
            return _FakeResponse({"id": "space-1", "key": "FINAI", "name": "FIN-AI"})
        raise AssertionError(f"Unexpected GET URL: {url} params={params} headers={headers or self._headers}")

    def put(self, url: str, json: dict[str, object] | None = None) -> _FakeResponse:
        assert url == "https://api.atlassian.com/ex/confluence/cloud-1/wiki/api/v2/pages/page-1"
        assert isinstance(json, dict)
        assert json["title"] == "Statement Contract"
        assert json["version"] == {"number": 8, "message": "FIN-AI Auditor approved review patch"}
        body = json.get("body")
        assert isinstance(body, dict)
        assert body["representation"] == "storage"
        assert "FIN-AI Auditor Review" in str(body["value"])
        return _FakeResponse(
            {
                "id": "page-1",
                "title": "Statement Contract",
                "version": {"number": 8},
            }
        )


class _ScopedOAuthService:
    def __init__(self, *, expected_scopes: set[str]) -> None:
        self._expected_scopes = expected_scopes

    def get_valid_access_token_or_raise(self, *, required_scopes: set[str] | None = None) -> str:
        assert required_scopes == self._expected_scopes
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
            "granted_scopes": sorted(self._expected_scopes),
            "target_url": target_url,
            "target_type": target_type,
            "oauth_ready": True,
        }


class _FakeConfluenceWriteConnector:
    def update_page(
        self,
        *,
        target: ConfluencePageTarget,
        patch_preview: ConfluencePatchPreview,
        access_token: str,
    ) -> ConfluenceUpdatedPage:
        assert target.page_id
        assert patch_preview.execution_ready is True
        assert access_token == "access-token"
        return ConfluenceUpdatedPage(
            page_id=target.page_id,
            page_title=target.page_title,
            page_url=target.page_url,
            version_number=8,
            response_payload={"id": target.page_id, "title": target.page_title, "version": {"number": 8}},
            verification_metadata={
                "resource_id": "cloud-1",
                "access_mode": "oauth_cloud",
            },
        )


def _build_patch_preview() -> ConfluencePatchPreview:
    return ConfluencePatchPreview(
        page_id="page-1",
        page_title="Statement Contract",
        page_url="https://finius.atlassian.net/wiki/spaces/FINAI/pages/page-1/Statement+Contract",
        space_key="FINAI",
        base_revision_id="7",
        execution_ready=True,
        changed_sections=["Contracts / Statement"],
        change_summary=["Contracts / Statement: Statement Contract angleichen"],
        review_storage_snippets=["<p>snippet</p>"],
        operations=[
            ConfluencePatchOperation(
                action_type="append_after_heading",
                marker_kind="correct",
                section_path="Contracts / Statement",
                anchor_heading="Statement",
                current_statement="Write path must stay approval-gated.",
                proposed_statement="Write path bleibt approval-gated und referenziert den kanonischen Vertrag.",
                rationale="Dokumentation und Code drifteten auseinander.",
                storage_snippet="<p>snippet</p>",
            )
        ],
    )


def test_confluence_write_connector_updates_page_via_accessible_resources(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("fin_ai_auditor.services.connectors.confluence_connector.httpx.Client", _FakeConfluenceClient)
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=tmp_path / "metamodel.json")
    connector = ConfluencePageWriteConnector(settings=settings)

    result = connector.update_page(
        target=ConfluencePageTarget(
            page_id="page-1",
            page_title="Statement Contract",
            page_url="https://finius.atlassian.net/wiki/spaces/FINAI/pages/page-1/Statement+Contract",
            space_key="FINAI",
        ),
        patch_preview=_build_patch_preview(),
        access_token="access-token",
    )

    assert result.page_id == "page-1"
    assert result.version_number == 8


def test_create_confluence_approval_request_stores_patch_preview(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=tmp_path / "metamodel.json",
        mothership_url="",
        license_key="",
        license_tenant="",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)
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
    snapshot_ids = service._build_demo_snapshot_ids()
    run = service._repository.upsert_run(
        run=run.model_copy(
            update={
                "findings": [service._build_demo_findings(run=run, snapshot_ids=snapshot_ids)[0]],
                "source_snapshots": service._build_demo_snapshots(run=run, snapshot_ids=snapshot_ids),
            }
        )
    )

    updated = service.create_writeback_approval_request(
        run_id=run.run_id,
        target_type="confluence_page_update",
        title="Confluence-Writeback fuer Statement Contract",
        summary="Die Seite soll nach expliziter Freigabe extern aktualisiert werden.",
        target_url=settings.confluence_home_url,
        related_package_ids=[],
        related_finding_ids=[],
        payload_preview=[],
    )

    approval = updated.approval_requests[0]
    preview = approval.metadata.get("confluence_patch_preview")
    assert isinstance(preview, dict)
    assert preview["page_title"]
    assert approval.payload_preview


def test_demo_completion_can_run_multiple_times_without_snapshot_id_collision(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=tmp_path / "metamodel.json",
        mothership_url="",
        license_key="",
        license_tenant="",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)

    first = service.create_run(
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
    second = service.create_run(
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

    first_completed = service.complete_run_with_demo_findings(run_id=first.run_id)
    second_completed = service.complete_run_with_demo_findings(run_id=second.run_id)

    assert len(first_completed.source_snapshots) == 4
    assert len(second_completed.source_snapshots) == 4
    assert {snapshot.snapshot_id for snapshot in first_completed.source_snapshots}.isdisjoint(
        {snapshot.snapshot_id for snapshot in second_completed.source_snapshots}
    )


def test_audit_service_executes_approved_confluence_writeback(tmp_path: Path) -> None:
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
        atlassian_oauth_service=_ScopedOAuthService(expected_scopes={"write:page:confluence"}),
        confluence_page_write_connector=_FakeConfluenceWriteConnector(),
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
    demo_run = service._repository.upsert_run(
        run=run.model_copy(
            update={
                "findings": [service._build_demo_findings(run=run, snapshot_ids=(snapshot_ids := service._build_demo_snapshot_ids()))[0]],
                "source_snapshots": service._build_demo_snapshots(run=run, snapshot_ids=snapshot_ids),
            }
        )
    )
    run_with_approval = service.create_writeback_approval_request(
        run_id=demo_run.run_id,
        target_type="confluence_page_update",
        title="Confluence-Writeback fuer Statement Contract",
        summary="Die Seite soll nach expliziter Freigabe extern aktualisiert werden.",
        target_url=settings.confluence_home_url,
        related_package_ids=[],
        related_finding_ids=[],
        payload_preview=[],
    )
    approval_request_id = run_with_approval.approval_requests[0].approval_request_id
    approved = service.resolve_writeback_approval_request(
        run_id=demo_run.run_id,
        approval_request_id=approval_request_id,
        decision="approve",
        comment_text="Freigegeben",
    )

    executed = service.execute_confluence_page_writeback(
        run_id=approved.run_id,
        approval_request_id=approval_request_id,
    )

    assert executed.implemented_changes
    assert executed.implemented_changes[0].target_label == "Statement Contract"
    assert executed.implemented_changes[0].metadata["execution_mode"] == "external_confluence_api"
    assert executed.implemented_changes[0].metadata["writeback_verification"]["verified"] is True
    assert executed.implemented_changes[0].metadata["writeback_verification"]["resource_id"] == "cloud-1"
    assert executed.implemented_changes[0].confluence_update is not None
    assert executed.implemented_changes[0].confluence_update.applied_revision_id == "8"
    assert executed.approval_requests[0].status == "executed"


def test_confluence_writeback_failure_persists_http_classification(tmp_path: Path) -> None:
    class _FailingConfluenceWriteConnector:
        def update_page(
            self,
            *,
            target: ConfluencePageTarget,
            patch_preview: ConfluencePatchPreview,
            access_token: str,
        ) -> ConfluenceUpdatedPage:
            raise httpx.HTTPStatusError(
                "rate limited",
                request=httpx.Request("PUT", target.page_url),
                response=httpx.Response(429),
            )

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
        atlassian_oauth_service=_ScopedOAuthService(expected_scopes={"write:page:confluence"}),
        confluence_page_write_connector=_FailingConfluenceWriteConnector(),
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
    demo_run = service._repository.upsert_run(
        run=run.model_copy(
            update={
                "findings": [service._build_demo_findings(run=run, snapshot_ids=(snapshot_ids := service._build_demo_snapshot_ids()))[0]],
                "source_snapshots": service._build_demo_snapshots(run=run, snapshot_ids=snapshot_ids),
            }
        )
    )
    run_with_approval = service.create_writeback_approval_request(
        run_id=demo_run.run_id,
        target_type="confluence_page_update",
        title="Confluence-Writeback fuer Statement Contract",
        summary="Die Seite soll nach expliziter Freigabe extern aktualisiert werden.",
        target_url=settings.confluence_home_url,
        related_package_ids=[],
        related_finding_ids=[],
        payload_preview=[],
    )
    approval_request_id = run_with_approval.approval_requests[0].approval_request_id
    service.resolve_writeback_approval_request(
        run_id=demo_run.run_id,
        approval_request_id=approval_request_id,
        decision="approve",
        comment_text="Freigegeben",
    )

    with pytest.raises(httpx.HTTPStatusError):
        service.execute_confluence_page_writeback(run_id=demo_run.run_id, approval_request_id=approval_request_id)

    persisted = service.get_run(run_id=demo_run.run_id)
    assert persisted is not None
    approval = persisted.approval_requests[0]
    assert approval.metadata["last_execution_error"]["failure_class"] == "rate_limited"
    assert approval.metadata["writeback_verification"]["patch_execution_ready"] is True


def test_audit_service_reuses_existing_confluence_writeback_idempotently(tmp_path: Path) -> None:
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
        atlassian_oauth_service=_ScopedOAuthService(expected_scopes={"write:page:confluence"}),
        confluence_page_write_connector=_FakeConfluenceWriteConnector(),
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
    demo_run = service._repository.upsert_run(
        run=run.model_copy(
            update={
                "findings": [service._build_demo_findings(run=run, snapshot_ids=(snapshot_ids := service._build_demo_snapshot_ids()))[0]],
                "source_snapshots": service._build_demo_snapshots(run=run, snapshot_ids=snapshot_ids),
            }
        )
    )
    run_with_approval = service.create_writeback_approval_request(
        run_id=demo_run.run_id,
        target_type="confluence_page_update",
        title="Confluence-Writeback fuer Statement Contract",
        summary="Die Seite soll nach expliziter Freigabe extern aktualisiert werden.",
        target_url=settings.confluence_home_url,
        related_package_ids=[],
        related_finding_ids=[],
        payload_preview=[],
    )
    approval_request_id = run_with_approval.approval_requests[0].approval_request_id
    service.resolve_writeback_approval_request(
        run_id=demo_run.run_id,
        approval_request_id=approval_request_id,
        decision="approve",
        comment_text="Freigegeben",
    )

    first = service.execute_confluence_page_writeback(run_id=demo_run.run_id, approval_request_id=approval_request_id)
    second = service.execute_confluence_page_writeback(run_id=demo_run.run_id, approval_request_id=approval_request_id)

    assert len(first.implemented_changes) == 1
    assert len(second.implemented_changes) == 1
    assert second.approval_requests[0].status == "executed"
    assert any(entry.title == "Writeback bereits ausgefuehrt" for entry in second.analysis_log)
