from __future__ import annotations

from functools import lru_cache

from fin_ai_auditor.config import get_settings
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository
from fin_ai_auditor.services.audit_service import AuditService
from fin_ai_auditor.services.atlassian_oauth_service import AtlassianOAuthService
from fin_ai_auditor.services.connectors.confluence_connector import ConfluencePageWriteConnector
from fin_ai_auditor.services.connectors.jira_connector import JiraTicketingConnector


@lru_cache(maxsize=1)
def get_repository() -> SQLiteAuditRepository:
    settings = get_settings()
    return SQLiteAuditRepository(db_path=settings.database_path)


@lru_cache(maxsize=1)
def get_audit_service() -> AuditService:
    settings = get_settings()
    return AuditService(
        repository=get_repository(),
        settings=settings,
        atlassian_oauth_service=get_atlassian_oauth_service(),
        confluence_page_write_connector=get_confluence_page_write_connector(),
        jira_ticketing_connector=get_jira_ticketing_connector(),
    )


@lru_cache(maxsize=1)
def get_atlassian_oauth_service() -> AtlassianOAuthService:
    settings = get_settings()
    return AtlassianOAuthService(repository=get_repository(), settings=settings)


@lru_cache(maxsize=1)
def get_jira_ticketing_connector() -> JiraTicketingConnector:
    settings = get_settings()
    return JiraTicketingConnector(settings=settings)


@lru_cache(maxsize=1)
def get_confluence_page_write_connector() -> ConfluencePageWriteConnector:
    settings = get_settings()
    return ConfluencePageWriteConnector(settings=settings)
