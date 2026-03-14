from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx

from fin_ai_auditor.config import Settings


@dataclass(frozen=True)
class JiraTicketTarget:
    project_key: str
    board_url: str


@dataclass(frozen=True)
class JiraCreatedIssue:
    issue_id: str
    issue_key: str
    issue_url: str
    site_base_url: str
    response_payload: dict[str, Any]


@dataclass(frozen=True)
class _JiraAccessContext:
    api_base_url: str
    site_base_url: str


class JiraTicketingConnector:
    """Connector fuer explizit freigegebene Jira-Codeaenderungs-Tickets.

    Jira ist im Auditor bewusst keine Analysequelle. Dieser Connector ist nur
    fuer den spaeteren, kontrollierten Writeback nach Approval gedacht.
    """

    def __init__(self, *, settings: Settings) -> None:
        self._settings = settings

    def create_ticket(
        self,
        *,
        target: JiraTicketTarget,
        issue_payload: dict[str, Any],
        access_token: str,
    ) -> JiraCreatedIssue:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=20.0, headers=headers) as client:
            access_context = _resolve_access_context(
                client=client,
                settings=self._settings,
                target=target,
                access_token=access_token,
            )
            response = client.post(
                f"{access_context.api_base_url}/rest/api/3/issue",
                json=issue_payload,
            )
            response.raise_for_status()
            payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Jira-Antwort fuer Issue-Erstellung ist ungueltig.")
        issue_id = str(payload.get("id") or "").strip()
        issue_key = str(payload.get("key") or "").strip()
        if not issue_id or not issue_key:
            raise ValueError("Jira-Antwort enthaelt keine gueltige Issue-ID oder keinen Issue-Key.")
        return JiraCreatedIssue(
            issue_id=issue_id,
            issue_key=issue_key,
            issue_url=f"{access_context.site_base_url}/browse/{issue_key}",
            site_base_url=access_context.site_base_url,
            response_payload=dict(payload),
        )


def _resolve_access_context(
    *,
    client: httpx.Client,
    settings: Settings,
    target: JiraTicketTarget,
    access_token: str,
) -> _JiraAccessContext:
    resource = _discover_jira_resource(
        client=client,
        settings=settings,
        target=target,
        access_token=access_token,
    )
    if resource is None:
        raise ValueError(
            "Es konnte keine passende Jira Cloud-Ressource fuer den FIN-AI Auditor gefunden werden. "
            "Pruefe Atlassian-Scopes, User-Consent und Ziel-Site."
        )
    cloud_id = str(resource.get("id") or "").strip()
    site_url = str(resource.get("url") or "").strip().rstrip("/")
    if not cloud_id or not site_url:
        raise ValueError("Die gefundene Jira-Ressource ist unvollstaendig und blockiert den Writeback.")
    return _JiraAccessContext(
        api_base_url=f"https://api.atlassian.com/ex/jira/{cloud_id}",
        site_base_url=site_url,
    )


def _discover_jira_resource(
    *,
    client: httpx.Client,
    settings: Settings,
    target: JiraTicketTarget,
    access_token: str,
) -> dict[str, Any] | None:
    response = client.get(
        "https://api.atlassian.com/oauth/token/accessible-resources",
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        return None

    configured_host = _target_host(settings=settings, target=target)
    candidates: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        scopes = {
            str(scope or "").strip().casefold()
            for scope in item.get("scopes") or []
            if str(scope or "").strip()
        }
        if "write:jira-work" not in scopes and "read:jira-work" not in scopes:
            continue
        candidates.append(item)

    for item in candidates:
        if urlparse(str(item.get("url") or "")).netloc.casefold() == configured_host:
            return item
    return candidates[0] if candidates else None


def _target_host(*, settings: Settings, target: JiraTicketTarget) -> str:
    board_host = urlparse(str(target.board_url or "")).netloc.casefold()
    if board_host:
        return board_host
    settings_host = urlparse(str(settings.jira_board_url or "")).netloc.casefold()
    if settings_host:
        return settings_host
    return urlparse(str(settings.confluence_home_url or "")).netloc.casefold()
