from __future__ import annotations

from dataclasses import dataclass
import logging
import time
from typing import Any
from urllib.parse import urlparse

import httpx

from fin_ai_auditor.config import Settings

logger = logging.getLogger(__name__)


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
    verification_metadata: dict[str, Any]


@dataclass(frozen=True)
class _JiraAccessContext:
    api_base_url: str
    site_base_url: str
    resource_id: str | None = None
    resource_url: str | None = None
    resource_scopes: list[str] | None = None


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
            normalized_payload, resolved_issue_type = _normalize_issue_payload_for_target(
                client=client,
                api_base_url=access_context.api_base_url,
                project_key=target.project_key,
                issue_payload=issue_payload,
            )
            response = _request_with_retry(
                client=client,
                method="POST",
                url=f"{access_context.api_base_url}/rest/api/3/issue",
                json=normalized_payload,
            )
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
            verification_metadata={
                "resolved_target_host": _target_host(settings=self._settings, target=target),
                "resource_id": access_context.resource_id,
                "resource_url": access_context.resource_url,
                "resource_scopes": list(access_context.resource_scopes or []),
                "api_base_url": access_context.api_base_url,
                "site_base_url": access_context.site_base_url,
                "resolved_issue_type": resolved_issue_type,
            },
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
        resource_id=cloud_id,
        resource_url=site_url,
        resource_scopes=sorted(
            str(scope or "").strip()
            for scope in (resource.get("scopes") or [])
            if str(scope or "").strip()
        ),
    )


def _discover_jira_resource(
    *,
    client: httpx.Client,
    settings: Settings,
    target: JiraTicketTarget,
    access_token: str,
) -> dict[str, Any] | None:
    response = _request_with_retry(
        client=client,
        method="GET",
        url="https://api.atlassian.com/oauth/token/accessible-resources",
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
    )
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


def _normalize_issue_payload_for_target(
    *,
    client: httpx.Client,
    api_base_url: str,
    project_key: str,
    issue_payload: dict[str, Any],
) -> tuple[dict[str, Any], str | None]:
    normalized_payload = dict(issue_payload)
    fields = dict(normalized_payload.get("fields") or {})
    requested_issue_type = dict(fields.get("issuetype") or {})
    requested_name = str(requested_issue_type.get("name") or "").strip()
    requested_id = str(requested_issue_type.get("id") or "").strip()
    available_types = _fetch_creatable_issue_types(
        client=client,
        api_base_url=api_base_url,
        project_key=project_key,
    )
    if not available_types:
        normalized_payload["fields"] = fields
        return normalized_payload, requested_name or requested_id or None
    available_names = {
        str(item.get("name") or "").strip().casefold(): str(item.get("name") or "").strip()
        for item in available_types
        if str(item.get("name") or "").strip()
    }
    available_ids = {
        str(item.get("id") or "").strip(): str(item.get("name") or "").strip()
        for item in available_types
        if str(item.get("id") or "").strip()
    }
    if requested_id and requested_id in available_ids:
        normalized_payload["fields"] = fields
        return normalized_payload, available_ids[requested_id] or requested_id
    if requested_name and requested_name.casefold() in available_names:
        normalized_payload["fields"] = fields
        return normalized_payload, available_names[requested_name.casefold()] or requested_name
    resolved_issue_type = _select_preferred_issue_type(available_types=available_types)
    if resolved_issue_type is not None:
        fields["issuetype"] = {"name": resolved_issue_type}
    normalized_payload["fields"] = fields
    return normalized_payload, resolved_issue_type


def _fetch_creatable_issue_types(
    *,
    client: httpx.Client,
    api_base_url: str,
    project_key: str,
) -> list[dict[str, Any]]:
    response = _request_with_retry(
        client=client,
        method="GET",
        url=f"{api_base_url}/rest/api/3/issue/createmeta/{project_key}/issuetypes",
    )
    payload = response.json()
    if not isinstance(payload, dict):
        return []
    issue_types = payload.get("issueTypes")
    if not isinstance(issue_types, list):
        return []
    return [item for item in issue_types if isinstance(item, dict)]


def _select_preferred_issue_type(*, available_types: list[dict[str, Any]]) -> str | None:
    preferred_names = ("Task", "Story", "Bug", "Feature", "Epic", "Idee")
    non_subtasks = [
        item
        for item in available_types
        if not bool(item.get("subtask"))
    ]
    normalized_non_subtasks = {
        str(item.get("name") or "").strip().casefold(): str(item.get("name") or "").strip()
        for item in non_subtasks
        if str(item.get("name") or "").strip()
    }
    for preferred in preferred_names:
        resolved = normalized_non_subtasks.get(preferred.casefold())
        if resolved:
            return resolved
    for item in non_subtasks:
        name = str(item.get("name") or "").strip()
        if name:
            return name
    for item in available_types:
        name = str(item.get("name") or "").strip()
        if name:
            return name
    return None


def _request_with_retry(
    *,
    client: httpx.Client,
    method: str,
    url: str,
    params: dict[str, object] | None = None,
    headers: dict[str, str] | None = None,
    json: dict[str, object] | None = None,
    max_attempts: int = 3,
) -> httpx.Response:
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            request_kwargs: dict[str, Any] = {}
            if params is not None:
                request_kwargs["params"] = params
            if headers is not None:
                request_kwargs["headers"] = headers
            if json is not None:
                request_kwargs["json"] = json
            request_callable = getattr(client, method.lower(), None)
            if callable(request_callable):
                response = request_callable(url, **request_kwargs)
            else:
                response = client.request(method, url, **request_kwargs)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            last_exc = exc
            if attempt >= max_attempts:
                raise
            _sleep_before_retry(attempt=attempt, retry_after=None)
            continue
        if response.status_code in {429, 500, 502, 503, 504}:
            if attempt >= max_attempts:
                response.raise_for_status()
            retry_after = response.headers.get("Retry-After")
            logger.warning(
                "jira_request_retry",
                extra={
                    "event_name": "jira_request_retry",
                    "event_payload": {"attempt": attempt, "status_code": response.status_code, "url": url},
                },
            )
            _sleep_before_retry(attempt=attempt, retry_after=retry_after)
            continue
        response.raise_for_status()
        return response
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Jira-Request-Retry hat keinen Response geliefert.")


def _sleep_before_retry(*, attempt: int, retry_after: str | None) -> None:
    try:
        explicit_delay = float(str(retry_after or "").strip())
    except ValueError:
        explicit_delay = 0.0
    delay_s = explicit_delay if explicit_delay > 0 else min(1.5 * attempt, 5.0)
    time.sleep(delay_s)
