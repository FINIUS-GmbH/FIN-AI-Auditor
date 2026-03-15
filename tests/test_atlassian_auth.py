from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from fin_ai_auditor.api.app import create_app
from fin_ai_auditor.api.dependencies import get_atlassian_oauth_service
from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import AtlassianOAuthTokenRecord
from fin_ai_auditor.services.atlassian_oauth_service import AtlassianOAuthService
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository


def _build_service(tmp_path: Path) -> AtlassianOAuthService:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        port=8088,
        host="127.0.0.1",
        atlassian_enabled=True,
        atlassian_oauth_client_id="client-id",
        atlassian_oauth_client_secret="client-secret",
        atlassian_oauth_redirect_uri="http://localhost/api/ingestion/atlassian/auth/callback",
        atlassian_oauth_scope="read:confluence-content.summary read:confluence-content.all",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    return AtlassianOAuthService(repository=repository, settings=settings)


def test_atlassian_auth_start_uses_local_auditor_callback(tmp_path: Path) -> None:
    service = _build_service(tmp_path)

    start = service.build_authorization_start()
    status = service.get_auth_status()

    assert start.redirect_uri == "http://localhost:8088/api/ingestion/atlassian/auth/callback"
    assert "state=" in start.authorization_url
    assert status.redirect_uri_matches_local_api is False
    assert any("separaten OAuth-Flow" in note for note in status.notes)


def test_atlassian_auth_start_route_keeps_existing_token(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    service._repository.upsert_atlassian_token(
        token=AtlassianOAuthTokenRecord(
            access_token="access-token",
            refresh_token="refresh-token",
            scope="read:confluence-content.summary read:confluence-content.all",
            expires_at="2099-01-01T00:00:00+00:00",
        )
    )

    app = create_app()
    app.dependency_overrides[get_atlassian_oauth_service] = lambda: service
    client = TestClient(app)

    response = client.get("/api/ingestion/atlassian/auth/start")

    assert response.status_code == 200
    assert service.get_auth_status().token_valid is True


def test_atlassian_callback_route_stores_local_token(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    start = service.build_authorization_start()

    def fake_exchange(*, code: str, redirect_uri: str) -> AtlassianOAuthTokenRecord:
        assert code == "demo-code"
        assert redirect_uri == "http://localhost:8088/api/ingestion/atlassian/auth/callback"
        return AtlassianOAuthTokenRecord(
            access_token="access-token",
            refresh_token="refresh-token",
            scope="read:confluence-content.summary read:confluence-content.all",
            expires_at="2099-01-01T00:00:00+00:00",
        )

    monkeypatch.setattr(service, "_exchange_code_for_token", fake_exchange)

    app = create_app()
    app.dependency_overrides[get_atlassian_oauth_service] = lambda: service
    client = TestClient(app)

    response = client.get(
        f"/api/ingestion/atlassian/auth/callback?code=demo-code&state={start.state_id}"
    )

    assert response.status_code == 200
    assert "Atlassian OAuth abgeschlossen" in response.text
    assert service.get_auth_status().token_valid is True


def test_writeback_scope_check_uses_granted_token_scope_not_only_configured_scope(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
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
    service = AtlassianOAuthService(repository=repository, settings=settings)
    repository.upsert_atlassian_token(
        token=AtlassianOAuthTokenRecord(
            access_token="access-token",
            refresh_token="refresh-token",
            scope="read:confluence-content.summary read:confluence-content.all",
            expires_at="2099-01-01T00:00:00+00:00",
        )
    )

    with pytest.raises(ValueError, match="fehlen die noetigen Scopes"):
        service.get_valid_access_token_or_raise(required_scopes={"write:jira-work"})


def test_runtime_access_status_refreshes_expired_token(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    service._repository.upsert_atlassian_token(
        token=AtlassianOAuthTokenRecord(
            access_token="expired-token",
            refresh_token="refresh-token",
            scope="read:page:confluence write:jira-work",
            expires_at="2000-01-01T00:00:00+00:00",
        )
    )

    def fake_refresh(*, token: AtlassianOAuthTokenRecord) -> AtlassianOAuthTokenRecord:
        assert token.refresh_token == "refresh-token"
        return token.model_copy(
            update={
                "access_token": "fresh-token",
                "expires_at": "2099-01-01T00:00:00+00:00",
            }
        )

    monkeypatch.setattr(service, "_refresh_access_token", fake_refresh)

    status = service.get_runtime_access_status()

    assert status["token_available"] is True
    assert status["token_valid"] is True
    assert status["refreshed_during_check"] is True
