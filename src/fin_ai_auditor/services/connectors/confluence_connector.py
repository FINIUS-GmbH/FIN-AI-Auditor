from __future__ import annotations

import hashlib
import html
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Callable
from typing import Any
from urllib.parse import urlparse

import httpx

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import ConfluencePatchPreview
from fin_ai_auditor.domain.models import AuditSourceSnapshot
from fin_ai_auditor.services.confluence_patch_service import apply_confluence_patch_preview
from fin_ai_auditor.services.pipeline_models import CachedCollectedDocument, CollectionBundle, CollectedDocument


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConfluenceCollectionRequest:
    space_keys: list[str]
    page_ids: list[str] = field(default_factory=list)
    max_pages_per_space: int = 25


@dataclass(frozen=True)
class _ConfluenceAccessContext:
    api_base_url: str
    site_base_url: str
    mode: str
    resource_id: str | None = None
    resource_url: str | None = None
    resource_scopes: list[str] | None = None


@dataclass(frozen=True)
class ConfluencePageTarget:
    page_id: str
    page_title: str
    page_url: str
    space_key: str | None = None


@dataclass(frozen=True)
class ConfluenceUpdatedPage:
    page_id: str
    page_title: str
    page_url: str
    version_number: int
    response_payload: dict[str, Any]
    verification_metadata: dict[str, Any]


class ConfluenceKnowledgeBaseConnector:
    """Read-only Collector fuer Confluence-Seiten.

    Der Connector erwartet einen bereits vorliegenden OAuth Access Token.
    Client-Credentials allein reichen fuer 3LO-Reads nicht aus.
    """

    def __init__(self, *, settings: Settings) -> None:
        self._settings = settings
        self._access_token_provider: Callable[[], str | None] | None = None

    def with_access_token_provider(
        self,
        *,
        access_token_provider: Callable[[], str | None] | None,
    ) -> "ConfluenceKnowledgeBaseConnector":
        self._access_token_provider = access_token_provider
        return self

    def collect_pages(
        self,
        *,
        request: ConfluenceCollectionRequest,
        previous_snapshots: list[AuditSourceSnapshot] | None = None,
        document_cache_lookup: Callable[[str, str, str], CachedCollectedDocument | None] | None = None,
        latest_document_cache_lookup: Callable[[str, str], CachedCollectedDocument | None] | None = None,
    ) -> CollectionBundle:
        access_token = (
            self._access_token_provider() if self._access_token_provider is not None else None
        ) or _resolve_atlassian_access_token(settings=self._settings)
        if access_token is None:
            return CollectionBundle(
                snapshots=[],
                documents=[],
                analysis_notes=[
                    "Confluence-Collector wurde uebersprungen, weil lokal noch kein gueltiger Atlassian Access Token vorliegt.",
                    "Die vorhandenen OAuth Client-Credentials wurden erkannt, reichen fuer 3LO-Reads aber ohne User-Consent-Token nicht aus.",
                ],
            )

        documents: list[CollectedDocument] = []
        snapshots: list[AuditSourceSnapshot] = []
        notes: list[str] = []
        reused_documents = 0
        refreshed_documents = 0
        previous_snapshot_map = {
            snapshot.source_id: snapshot
            for snapshot in previous_snapshots or []
            if snapshot.source_type == "confluence_page"
        }
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

        with httpx.Client(timeout=15.0, headers=headers) as client:
            try:
                access_context = _resolve_access_context(client=client, settings=self._settings, access_token=access_token)
            except (httpx.HTTPError, ValueError) as exc:
                return CollectionBundle(
                    snapshots=[],
                    documents=[],
                    analysis_notes=[
                        "Confluence-Live-Read konnte in diesem Lauf nicht aufgebaut werden.",
                        _describe_http_or_value_error(prefix="Confluence Auth- oder Resource-Ermittlung", exc=exc),
                    ],
                )
            notes.append(
                "Confluence-Zugriff erfolgt read-only ueber "
                f"{'Atlassian 3LO / accessible-resources' if access_context.mode == 'oauth_cloud' else 'direkten Site-Endpunkt'}."
            )
            if access_context.mode == "site_direct":
                notes.append(
                    "Fuer diesen Lauf konnte ueber accessible-resources keine nutzbare Confluence-Cloud-Ressource aufgeloest werden; "
                    "der Auditor prueft deshalb read-only den direkten Site-Endpunkt."
                )
            for space_key in request.space_keys:
                normalized_space_key = str(space_key or "").strip()
                if not normalized_space_key:
                    continue
                try:
                    space = _fetch_space(
                        client=client,
                        api_base_url=access_context.api_base_url,
                        space_key=normalized_space_key,
                    )
                except httpx.HTTPError as exc:
                    notes.append(
                        _describe_http_or_value_error(
                            prefix=f"Confluence Space {normalized_space_key}",
                            exc=exc,
                        )
                    )
                    continue
                if space is None:
                    notes.append(f"Confluence Space {normalized_space_key} konnte nicht gelesen werden.")
                    continue
                selected_page_ids = _normalized_page_ids(request.page_ids)
                if selected_page_ids:
                    page_rows = _collect_selected_page_rows(
                        client=client,
                        api_base_url=access_context.api_base_url,
                        selected_page_ids=selected_page_ids,
                        space_key=normalized_space_key,
                        notes=notes,
                    )
                    notes.append(
                        f"Confluence Space {normalized_space_key}: {len(page_rows)} explizit ausgewaehlte Seiten wurden read-only geladen."
                    )
                else:
                    try:
                        page_rows = _fetch_pages_for_space(
                            client=client,
                            api_base_url=access_context.api_base_url,
                            space_id=str(space.get("id") or ""),
                            limit=int(request.max_pages_per_space),
                        )
                    except httpx.HTTPError as exc:
                        notes.append(
                            _describe_http_or_value_error(
                                prefix=f"Confluence Seitenliste fuer Space {normalized_space_key}",
                                exc=exc,
                            )
                        )
                        continue
                    notes.append(
                        f"Confluence Space {normalized_space_key}: {len(page_rows)} Seiten wurden read-only geladen."
                    )
                for page in page_rows:
                    page_id = str(page.get("id") or "").strip()
                    if not page_id:
                        continue
                    prefetched_detail = page if isinstance(page.get("body"), dict) else None
                    listed_revision = str((page.get("version") or {}).get("number") or "").strip()
                    previous_snapshot = previous_snapshot_map.get(page_id)
                    if (
                        previous_snapshot is not None
                        and listed_revision
                        and listed_revision == str(previous_snapshot.revision_id or "").strip()
                        and document_cache_lookup is not None
                    ):
                        cached_document = document_cache_lookup(
                            "confluence_page",
                            page_id,
                            str(previous_snapshot.content_hash or ""),
                        )
                        if cached_document is not None:
                            title = str(page.get("title") or cached_document.title).strip() or cached_document.title
                            ancestor_titles = list(cached_document.metadata.get("ancestor_titles") or [])
                            space_name = str((space.get("name") or "")).strip() or None
                            page_url = str(cached_document.url or "").strip() or _build_page_url(
                                site_base_url=access_context.site_base_url,
                                space_key=str(space.get("key") or normalized_space_key),
                                page_id=page_id,
                                title=title,
                            )
                            snapshot = AuditSourceSnapshot(
                                source_type="confluence_page",
                                source_id=page_id,
                                revision_id=listed_revision,
                                content_hash=previous_snapshot.content_hash,
                                sync_token=f"confluence:{normalized_space_key}:{page_id}",
                                metadata={
                                    "space_key": normalized_space_key,
                                    "title": title,
                                    "url": page_url,
                                    "incremental_reused": True,
                                    "reused_from_snapshot_id": previous_snapshot.snapshot_id,
                                },
                            )
                            snapshots.append(snapshot)
                            documents.append(
                                CollectedDocument(
                                    snapshot=snapshot,
                                    source_type="confluence_page",
                                    source_id=page_id,
                                    title=title,
                                    body=cached_document.body,
                                    path_hint=cached_document.path_hint,
                                    url=page_url,
                                    metadata={
                                        **cached_document.metadata,
                                        "space_key": normalized_space_key,
                                        "ancestor_titles": ancestor_titles,
                                        "space_name": space_name,
                                        "incremental_reused": True,
                                    },
                                )
                            )
                            reused_documents += 1
                            continue
                    detail = dict(prefetched_detail) if prefetched_detail is not None else None
                    if detail is None:
                        try:
                            detail = _fetch_page_detail(
                                client=client,
                                api_base_url=access_context.api_base_url,
                                page_id=page_id,
                            )
                        except httpx.HTTPError as exc:
                            notes.append(
                                _describe_http_or_value_error(
                                    prefix=f"Confluence Seite {page_id}",
                                    exc=exc,
                                )
                            )
                            continue
                    adf_detail: dict[str, Any] | None = None
                    try:
                        adf_detail = _fetch_page_detail(
                            client=client,
                            api_base_url=access_context.api_base_url,
                            page_id=page_id,
                            body_format="atlas_doc_format",
                        )
                    except httpx.HTTPError:
                        adf_detail = None
                    body_text, extracted_metadata = _extract_page_content(storage_payload=detail or page, adf_payload=adf_detail)
                    if not body_text:
                        notes.append(f"Confluence Seite {page_id} wurde gelesen, enthielt aber keinen auswertbaren Text.")
                        continue
                    title = str((detail or page).get("title") or f"Confluence Page {page_id}").strip()
                    ancestor_titles = _ancestor_titles(detail or page)
                    latest_cached_document = (
                        latest_document_cache_lookup("confluence_page", page_id)
                        if latest_document_cache_lookup is not None
                        else None
                    )
                    delta_metadata = _build_confluence_section_delta_metadata(
                        latest_cached_document=latest_cached_document,
                        current_content_hash=_sha256_text(body_text),
                        extracted_metadata=extracted_metadata,
                    )
                    restriction_state, restriction_metadata = _resolve_confluence_restriction_state(page_payload=detail or page)
                    sensitivity_level, sensitivity_metadata = _resolve_confluence_sensitivity_level(page_payload=detail or page)
                    page_url = _build_page_url(
                        site_base_url=access_context.site_base_url,
                        space_key=str(space.get("key") or normalized_space_key),
                        page_id=page_id,
                        title=title,
                    )
                    content_hash = _sha256_text(body_text)
                    snapshot = AuditSourceSnapshot(
                        source_type="confluence_page",
                        source_id=page_id,
                        revision_id=str((detail or page).get("version", {}).get("number") or ""),
                        content_hash=content_hash,
                        sync_token=f"confluence:{normalized_space_key}:{page_id}",
                        metadata={
                            "space_key": normalized_space_key,
                            "title": title,
                            "url": page_url,
                        },
                    )
                    snapshots.append(snapshot)
                    documents.append(
                        CollectedDocument(
                            snapshot=snapshot,
                            source_type="confluence_page",
                            source_id=page_id,
                            title=title,
                            body=body_text,
                            path_hint=" / ".join(
                                item
                                for item in [f"Space {normalized_space_key}", *ancestor_titles, title]
                                if str(item).strip()
                            ),
                            url=page_url,
                            metadata={
                                **extracted_metadata,
                                **delta_metadata,
                                **restriction_metadata,
                                **sensitivity_metadata,
                                "space_key": normalized_space_key,
                                "ancestor_titles": ancestor_titles,
                                "space_name": str(space.get("name") or "").strip() or None,
                                "restriction_state": restriction_state,
                                "sensitivity_level": sensitivity_level,
                                "attachment_policy": "metadata_only",
                                "analysis_cache_role": "confluence_analysis_cache",
                                "attachment_count": _count_structured_blocks(
                                    blocks=extracted_metadata.get("structured_blocks"),
                                    block_kind="attachment",
                                ),
                                "incremental_reused": False,
                            },
                        )
                    )
                    refreshed_documents += 1

        notes.append(
            f"Confluence inkrementell: {reused_documents} Seiten aus dem lokalen Cache uebernommen, "
            f"{refreshed_documents} Seiten neu von Atlassian gelesen."
        )
        return CollectionBundle(snapshots=snapshots, documents=documents, analysis_notes=notes)


class ConfluencePageWriteConnector:
    """Connector fuer explizit freigegebene Confluence-Seitenupdates."""

    def __init__(self, *, settings: Settings) -> None:
        self._settings = settings

    def update_page(
        self,
        *,
        target: ConfluencePageTarget,
        patch_preview: ConfluencePatchPreview,
        access_token: str,
    ) -> ConfluenceUpdatedPage:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=20.0, headers=headers) as client:
            access_context = _resolve_access_context(client=client, settings=self._settings, access_token=access_token)
            detail = _fetch_page_detail(
                client=client,
                api_base_url=access_context.api_base_url,
                page_id=target.page_id,
            )
            if detail is None:
                raise ValueError(f"Confluence-Seite {target.page_id} konnte fuer den Writeback nicht gelesen werden.")
            page_title = str(detail.get("title") or target.page_title).strip() or target.page_title
            space_key = str(((detail.get("space") or {}) if isinstance(detail.get("space"), dict) else {}).get("key") or target.space_key or "").strip()
            page_url = str(target.page_url or "").strip() or _build_page_url(
                site_base_url=access_context.site_base_url,
                space_key=space_key or "UNKNOWN",
                page_id=target.page_id,
                title=page_title,
            )
            body = detail.get("body")
            storage_body = body.get("storage") if isinstance(body, dict) else None
            current_storage_html = str(storage_body.get("value") or "").strip() if isinstance(storage_body, dict) else ""
            if not current_storage_html:
                raise ValueError("Die Confluence-Seite enthaelt keinen auswertbaren Storage-Body fuer den Patch.")
            current_version = int((detail.get("version") or {}).get("number") or 0)
            if current_version <= 0:
                raise ValueError("Die Confluence-Seite enthaelt keine gueltige Version fuer den Writeback.")
            updated_storage_html = apply_confluence_patch_preview(
                storage_html=current_storage_html,
                patch_preview=patch_preview,
            )
            response = _request_with_retry(
                client=client,
                method="PUT",
                url=f"{access_context.api_base_url}/wiki/api/v2/pages/{target.page_id}",
                json_body={
                    "id": target.page_id,
                    "status": "current",
                    "title": page_title,
                    "spaceId": str(detail.get("spaceId") or "").strip() or None,
                    "version": {
                        "number": current_version + 1,
                        "message": "FIN-AI Auditor approved review patch",
                    },
                    "body": {
                        "representation": "storage",
                        "value": updated_storage_html,
                    },
                },
            )
            response.raise_for_status()
            payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Confluence-Antwort fuer den Writeback ist ungueltig.")
        result_page_id = str(payload.get("id") or target.page_id).strip() or target.page_id
        result_title = str(payload.get("title") or page_title).strip() or page_title
        result_version = int((payload.get("version") or {}).get("number") or (current_version + 1))
        return ConfluenceUpdatedPage(
            page_id=result_page_id,
            page_title=result_title,
            page_url=page_url,
            version_number=result_version,
            response_payload=dict(payload),
            verification_metadata={
                "access_mode": access_context.mode,
                "resource_id": access_context.resource_id,
                "resource_url": access_context.resource_url,
                "resource_scopes": list(access_context.resource_scopes or []),
                "api_base_url": access_context.api_base_url,
                "site_base_url": access_context.site_base_url,
                "base_revision_id": str(current_version),
                "target_page_id": target.page_id,
            },
        )


def _resolve_atlassian_access_token(*, settings: Settings) -> str | None:
    env_map = settings._collect_external_env_map()
    for key in (
        "FIN_AI_AUDITOR_ATLASSIAN_ACCESS_TOKEN",
        "FINAI_ATLASSIAN_ACCESS_TOKEN",
        "ATLASSIAN_ACCESS_TOKEN",
        "CONFLUENCE_ACCESS_TOKEN",
    ):
        value = str(env_map.get(key) or "").strip()
        if value:
            return value
    return None


def _resolve_confluence_base_url(*, settings: Settings) -> str:
    explicit = str(settings.confluence_base_url or "").strip().rstrip("/")
    if explicit:
        parsed = urlparse(explicit)
        if parsed.scheme and parsed.netloc:
            if parsed.path.rstrip("/") == "/wiki":
                return f"{parsed.scheme}://{parsed.netloc}"
            return explicit
    parsed = urlparse(str(settings.confluence_home_url))
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def _resolve_confluence_site_base_url(*, settings: Settings) -> str:
    return f"{_resolve_confluence_base_url(settings=settings)}/wiki"


def _resolve_access_context(
    *,
    client: httpx.Client,
    settings: Settings,
    access_token: str,
) -> _ConfluenceAccessContext:
    resource = _discover_confluence_resource(client=client, settings=settings, access_token=access_token)
    if resource is not None:
        cloud_id = str(resource.get("id") or "").strip()
        site_url = str(resource.get("url") or "").strip().rstrip("/")
        if cloud_id and site_url:
            return _ConfluenceAccessContext(
                api_base_url=f"https://api.atlassian.com/ex/confluence/{cloud_id}",
                site_base_url=f"{site_url}/wiki",
                mode="oauth_cloud",
                resource_id=cloud_id,
                resource_url=site_url,
                resource_scopes=sorted(
                    str(scope or "").strip()
                    for scope in (resource.get("scopes") or [])
                    if str(scope or "").strip()
                ),
            )
    return _ConfluenceAccessContext(
        api_base_url=_resolve_confluence_base_url(settings=settings),
        site_base_url=_resolve_confluence_site_base_url(settings=settings),
        mode="site_direct",
    )


def _discover_confluence_resource(
    *,
    client: httpx.Client,
    settings: Settings,
    access_token: str,
) -> dict[str, Any] | None:
    try:
        response = client.get(
            "https://api.atlassian.com/oauth/token/accessible-resources",
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        )
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError):
        return None
    if not isinstance(payload, list):
        return None
    target_host = urlparse(str(settings.confluence_home_url)).netloc.casefold()
    scoped_candidates: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        scopes = [str(scope or "").strip().casefold() for scope in item.get("scopes") or [] if str(scope or "").strip()]
        has_confluence_scope = any(
            scope in {
                "read:page:confluence", "read:space:confluence",
                "read:content-details:confluence", "read:confluence-content.all",
            }
            for scope in scopes
        )
        if not has_confluence_scope:
            continue
        scoped_candidates.append(item)
    for item in scoped_candidates:
        if urlparse(str(item.get("url") or "")).netloc.casefold() == target_host:
            return item
    return scoped_candidates[0] if scoped_candidates else None


def _fetch_space(*, client: httpx.Client, api_base_url: str, space_key: str) -> dict[str, Any] | None:
    response = _request_with_retry(client=client, method="GET", url=f"{api_base_url}/wiki/api/v2/spaces", params={"keys": space_key})
    response.raise_for_status()
    payload = response.json()
    results = payload.get("results") if isinstance(payload, dict) else []
    if not isinstance(results, list) or not results:
        return None
    return dict(results[0])


def _fetch_pages_for_space(*, client: httpx.Client, api_base_url: str, space_id: str, limit: int) -> list[dict[str, Any]]:
    max_items = max(1, int(limit))
    request_limit = min(max_items, 100)
    next_url = f"{api_base_url}/wiki/api/v2/spaces/{space_id}/pages"
    next_params: dict[str, object] | None = {"space-id": space_id, "status": "current", "limit": request_limit}
    rows: list[dict[str, Any]] = []
    while next_url and len(rows) < max_items:
        response = _request_with_retry(
            client=client,
            method="GET",
            url=next_url,
            params=next_params,
        )
        response.raise_for_status()
        payload = response.json()
        results = payload.get("results") if isinstance(payload, dict) else []
        rows.extend(dict(row) for row in results if isinstance(row, dict))
        next_url = _next_page_link(payload=payload, api_base_url=api_base_url)
        next_params = None
        if not results:
            break
    return rows[:max_items]


def _collect_selected_page_rows(
    *,
    client: httpx.Client,
    api_base_url: str,
    selected_page_ids: list[str],
    space_key: str,
    notes: list[str],
) -> list[dict[str, Any]]:
    page_rows: list[dict[str, Any]] = []
    for page_id in selected_page_ids:
        try:
            detail = _fetch_page_detail(
                client=client,
                api_base_url=api_base_url,
                page_id=page_id,
            )
        except httpx.HTTPError as exc:
            notes.append(
                _describe_http_or_value_error(
                    prefix=f"Confluence Seite {page_id}",
                    exc=exc,
                )
            )
            continue
        if detail is None:
            notes.append(f"Confluence Seite {page_id} konnte nicht gelesen werden.")
            continue
        detail_space_key = str(((detail.get("space") or {}) if isinstance(detail.get("space"), dict) else {}).get("key") or "").strip()
        if detail_space_key and detail_space_key != space_key:
            notes.append(
                f"Confluence Seite {page_id} wurde ignoriert, weil sie nicht zum erwarteten Space {space_key} gehoert."
            )
            continue
        page_rows.append(detail)
    return page_rows


def _normalized_page_ids(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        page_id = str(raw_value or "").strip()
        if not page_id or page_id in seen:
            continue
        normalized.append(page_id)
        seen.add(page_id)
    return normalized


def _next_page_link(*, payload: object, api_base_url: str) -> str | None:
    if not isinstance(payload, dict):
        return None
    links = payload.get("_links")
    if isinstance(links, dict):
        next_candidate = str(links.get("next") or "").strip()
        if next_candidate:
            if next_candidate.startswith("http://") or next_candidate.startswith("https://"):
                return next_candidate
            if next_candidate.startswith("/"):
                return f"{api_base_url.rstrip('/')}{next_candidate}"
            return f"{api_base_url.rstrip('/')}/{next_candidate.lstrip('/')}"
    next_candidate = str(payload.get("next") or "").strip()
    if not next_candidate:
        return None
    if next_candidate.startswith("http://") or next_candidate.startswith("https://"):
        return next_candidate
    if next_candidate.startswith("/"):
        return f"{api_base_url.rstrip('/')}{next_candidate}"
    return f"{api_base_url.rstrip('/')}/{next_candidate.lstrip('/')}"


def _fetch_page_detail(
    *,
    client: httpx.Client,
    api_base_url: str,
    page_id: str,
    body_format: str = "storage",
) -> dict[str, Any] | None:
    response = _request_with_retry(
        client=client,
        method="GET",
        url=f"{api_base_url}/wiki/api/v2/pages/{page_id}",
        params={
            "body-format": body_format,
            "include-operations": "true",
            "include-properties": "true",
            "include-labels": "true",
        },
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        return None
    if not payload.get("ancestors"):
        try:
            ancestors_response = _request_with_retry(
                client=client,
                method="GET",
                url=f"{api_base_url}/wiki/api/v2/pages/{page_id}/ancestors",
            )
            ancestors_response.raise_for_status()
            ancestors_payload = ancestors_response.json()
            if isinstance(ancestors_payload, dict):
                payload["ancestors"] = ancestors_payload.get("results") or []
            elif isinstance(ancestors_payload, list):
                payload["ancestors"] = ancestors_payload
        except (httpx.HTTPError, AssertionError):
            pass
    space_id = str(payload.get("spaceId") or "").strip()
    if space_id and not payload.get("space"):
        try:
            space_response = _request_with_retry(
                client=client,
                method="GET",
                url=f"{api_base_url}/wiki/api/v2/spaces/{space_id}",
            )
            space_response.raise_for_status()
            space_payload = space_response.json()
            if isinstance(space_payload, dict):
                payload["space"] = {
                    "id": space_id,
                    "key": str(space_payload.get("key") or "").strip() or None,
                    "name": str(space_payload.get("name") or "").strip() or None,
                }
        except (httpx.HTTPError, AssertionError):
            pass
    return dict(payload)


def _extract_page_content(
    *,
    storage_payload: dict[str, Any],
    adf_payload: dict[str, Any] | None,
) -> tuple[str, dict[str, Any]]:
    storage_html = _extract_storage_html(storage_payload)
    storage_text, storage_blocks = _extract_storage_text_and_blocks(storage_html)
    adf_document = _extract_adf_document(adf_payload)
    adf_text, adf_blocks = _extract_adf_text_and_blocks(adf_document)
    selected_representation = "atlas_doc_format" if adf_text else "storage"
    selected_text = adf_text or storage_text
    selected_blocks = adf_blocks or storage_blocks
    return selected_text, {
        "confluence_representation": selected_representation,
        "storage_html_present": bool(storage_html),
        "adf_present": adf_document is not None,
        "structured_blocks": selected_blocks[:80],
        "structured_block_count": len(selected_blocks),
        "storage_heading_paths": [block["section_path"] for block in storage_blocks if block.get("section_path")][:18],
        "adf_heading_paths": [block["section_path"] for block in adf_blocks if block.get("section_path")][:18],
    }


def _build_confluence_section_delta_metadata(
    *,
    latest_cached_document: CachedCollectedDocument | None,
    current_content_hash: str,
    extracted_metadata: dict[str, Any],
) -> dict[str, Any]:
    if latest_cached_document is None or str(latest_cached_document.content_hash or "").strip() == current_content_hash:
        return {
            "changed_section_paths": [],
            "added_section_paths": [],
            "removed_section_paths": [],
            "section_delta_status": "unchanged" if latest_cached_document is not None else "new_page",
        }
    previous_blocks = list(latest_cached_document.metadata.get("structured_blocks") or [])
    current_blocks = list(extracted_metadata.get("structured_blocks") or [])
    previous_map = _section_block_digest_map(previous_blocks)
    current_map = _section_block_digest_map(current_blocks)
    previous_paths = set(previous_map)
    current_paths = set(current_map)
    changed_paths = sorted(
        section_path
        for section_path in current_paths.intersection(previous_paths)
        if previous_map.get(section_path) != current_map.get(section_path)
    )
    added_paths = sorted(current_paths - previous_paths)
    removed_paths = sorted(previous_paths - current_paths)
    return {
        "changed_section_paths": changed_paths,
        "added_section_paths": added_paths,
        "removed_section_paths": removed_paths,
        "section_delta_status": "changed" if (changed_paths or added_paths or removed_paths) else "text_changed_without_section_delta",
        "previous_cached_at": latest_cached_document.cached_at,
        "previous_cached_content_hash": latest_cached_document.content_hash,
    }


def _section_block_digest_map(blocks: list[object]) -> dict[str, str]:
    digests: dict[str, str] = {}
    grouped: dict[str, list[str]] = {}
    for raw_block in blocks:
        if not isinstance(raw_block, dict):
            continue
        section_path = str(raw_block.get("section_path") or "").strip() or "__root__"
        text = str(raw_block.get("text") or "").strip()
        kind = str(raw_block.get("kind") or "text").strip()
        grouped.setdefault(section_path, []).append(f"{kind}:{text}")
    for section_path, values in grouped.items():
        joined = "\n".join(values)
        digests[section_path] = _sha256_text(joined)
    return digests


def _resolve_confluence_restriction_state(*, page_payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    if "restrictions" in page_payload:
        restrictions = page_payload.get("restrictions")
        has_restrictions = bool(restrictions)
        return ("restricted" if has_restrictions else "none"), {
            "restriction_signal_source": "page.restrictions",
            "restriction_signal_present": True,
        }
    property_signal = _property_access_signal(page_payload=page_payload)
    if property_signal is not None:
        return property_signal
    label_signal = _label_access_signal(page_payload=page_payload)
    if label_signal is not None:
        return label_signal
    if "operations" in page_payload and isinstance(page_payload.get("operations"), list):
        return "unknown", {
            "restriction_signal_source": "page.operations",
            "restriction_signal_present": True,
            "restriction_signal_note": "operations zeigen nur den aktuellen Access-Kontext, nicht sicher die Abwesenheit von Restriktionen.",
        }
    return "unknown", {
        "restriction_signal_source": None,
        "restriction_signal_present": False,
    }


def _resolve_confluence_sensitivity_level(*, page_payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    explicit_candidates = [
        page_payload.get("classificationLevel"),
        page_payload.get("classification_level"),
        page_payload.get("contentState"),
        page_payload.get("content_state"),
    ]
    for candidate in explicit_candidates:
        normalized = str(candidate or "").strip()
        if not normalized:
            continue
        normalized_lower = normalized.casefold()
        if normalized_lower in {"restricted", "confidential", "secret", "internal-sensitive"}:
            return "restricted", {"sensitivity_signal": normalized}
        return "standard", {"sensitivity_signal": normalized}
    property_signal = _property_sensitivity_signal(page_payload=page_payload)
    if property_signal is not None:
        return property_signal
    label_signal = _label_sensitivity_signal(page_payload=page_payload)
    if label_signal is not None:
        return label_signal
    return "unknown", {"sensitivity_signal": None}


def _property_access_signal(*, page_payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    for property_key, property_value in _iter_page_property_values(page_payload=page_payload):
        normalized_key = property_key.casefold()
        normalized_value = property_value.casefold()
        if not any(token in normalized_key for token in ("restriction", "access", "visibility", "permission")):
            continue
        if any(token in normalized_value for token in ("restricted", "private", "internal", "confidential", "secret")):
            return "restricted", {
                "restriction_signal_source": f"page.properties.{property_key}",
                "restriction_signal_present": True,
            }
        if any(token in normalized_value for token in ("public", "open", "unrestricted")):
            return "none", {
                "restriction_signal_source": f"page.properties.{property_key}",
                "restriction_signal_present": True,
            }
    return None


def _label_access_signal(*, page_payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    labels = _page_label_names(page_payload=page_payload)
    if any(
        any(token in label.casefold() for token in ("restricted", "private", "confidential", "secret"))
        for label in labels
    ):
        return "restricted", {
            "restriction_signal_source": "page.labels",
            "restriction_signal_present": True,
        }
    return None


def _property_sensitivity_signal(*, page_payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    for property_key, property_value in _iter_page_property_values(page_payload=page_payload):
        normalized_key = property_key.casefold()
        normalized_value = property_value.casefold()
        if any(token in normalized_key for token in ("classification", "sensitivity", "data-class", "security")):
            if any(token in normalized_value for token in ("restricted", "confidential", "secret", "internal", "sensitive", "pii", "gdpr")):
                return "restricted", {"sensitivity_signal": f"{property_key}={property_value}"}
            if any(token in normalized_value for token in ("standard", "public", "open")):
                return "standard", {"sensitivity_signal": f"{property_key}={property_value}"}
    return None


def _label_sensitivity_signal(*, page_payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    labels = _page_label_names(page_payload=page_payload)
    for label in labels:
        normalized = label.casefold()
        if any(token in normalized for token in ("confidential", "restricted", "secret", "sensitive", "pii", "gdpr")):
            return "restricted", {"sensitivity_signal": label}
        if any(token in normalized for token in ("public", "standard", "open")):
            return "standard", {"sensitivity_signal": label}
    return None


def _iter_page_property_values(*, page_payload: dict[str, Any]) -> list[tuple[str, str]]:
    raw_properties = page_payload.get("properties")
    if isinstance(raw_properties, dict):
        if isinstance(raw_properties.get("results"), list):
            properties = raw_properties.get("results") or []
        else:
            properties = [raw_properties]
    elif isinstance(raw_properties, list):
        properties = raw_properties
    else:
        properties = []
    values: list[tuple[str, str]] = []
    for raw_property in properties:
        if not isinstance(raw_property, dict):
            continue
        property_key = str(raw_property.get("key") or raw_property.get("name") or "").strip()
        if not property_key:
            continue
        raw_value = raw_property.get("value")
        if isinstance(raw_value, (dict, list)):
            property_value = json.dumps(raw_value, ensure_ascii=False, sort_keys=True)
        else:
            property_value = str(raw_value or "").strip()
        if not property_value:
            continue
        values.append((property_key, property_value))
    return values


def _page_label_names(*, page_payload: dict[str, Any]) -> list[str]:
    raw_labels = page_payload.get("labels")
    if isinstance(raw_labels, dict):
        if isinstance(raw_labels.get("results"), list):
            labels = raw_labels.get("results") or []
        else:
            labels = [raw_labels]
    elif isinstance(raw_labels, list):
        labels = raw_labels
    else:
        labels = []
    names: list[str] = []
    for raw_label in labels:
        if isinstance(raw_label, dict):
            label_name = str(raw_label.get("name") or raw_label.get("label") or "").strip()
        else:
            label_name = str(raw_label or "").strip()
        if label_name:
            names.append(label_name)
    return names


def _count_structured_blocks(*, blocks: object, block_kind: str) -> int:
    if not isinstance(blocks, list):
        return 0
    return sum(
        1
        for block in blocks
        if isinstance(block, dict) and str(block.get("kind") or "").strip() == str(block_kind or "").strip()
    )


def _build_page_url(*, site_base_url: str, space_key: str, page_id: str, title: str) -> str:
    safe_title = title.replace(" ", "+")
    return f"{site_base_url}/spaces/{space_key}/pages/{page_id}/{safe_title}"


def _ancestor_titles(page_payload: dict[str, Any]) -> list[str]:
    raw_ancestors = page_payload.get("ancestors")
    if not isinstance(raw_ancestors, list):
        return []
    titles: list[str] = []
    for item in raw_ancestors:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        if title:
            titles.append(title)
    return titles


def _sha256_text(value: str) -> str:
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _describe_http_or_value_error(*, prefix: str, exc: httpx.HTTPError | ValueError) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        if status_code == 401:
            return f"{prefix} scheiterte mit 401. Access Token ist ungueltig, abgelaufen oder nicht fuer diese Ressource freigegeben."
        if status_code == 403:
            return f"{prefix} scheiterte mit 403. Die aktuelle Atlassian-Freigabe oder der Scope reicht fuer diesen Read nicht aus."
        if status_code == 404:
            return f"{prefix} scheiterte mit 404. Ziel-Site, Space oder Seite konnte ueber den aktuellen API-Pfad nicht gefunden werden."
        if status_code == 429:
            return f"{prefix} scheiterte mit 429. Atlassian hat den Read aktuell rate-limitiert."
        return f"{prefix} scheiterte mit HTTP {status_code}."
    if isinstance(exc, httpx.HTTPError):
        return f"{prefix} scheiterte aufgrund eines HTTP- oder Netzwerkfehlers: {exc.__class__.__name__}."
    return f"{prefix} scheiterte: {str(exc).strip() or exc.__class__.__name__}."


def _request_with_retry(
    *,
    client: httpx.Client,
    method: str,
    url: str,
    params: dict[str, object] | None = None,
    json_body: dict[str, Any] | None = None,
    max_attempts: int = 3,
) -> httpx.Response:
    request_callable = getattr(client, method.lower(), None)
    request_kwargs: dict[str, Any] = {}
    if params is not None:
        request_kwargs["params"] = params
    if json_body is not None:
        request_kwargs["json"] = json_body
    for attempt in range(1, max(1, int(max_attempts)) + 1):
        try:
            if callable(request_callable):
                response = request_callable(url, **request_kwargs)
            else:
                response = client.request(method, url, **request_kwargs)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            if attempt >= max_attempts:
                logger.warning(
                    "confluence_request_failed",
                    extra={
                        "event_name": "confluence_request_failed",
                        "event_payload": {"url": url, "attempt": attempt, "error": exc.__class__.__name__},
                    },
                )
                raise
            _sleep_before_retry(attempt=attempt, retry_after=None)
            continue
        if response.status_code in {429, 502, 503, 504} and attempt < max_attempts:
            retry_after = response.headers.get("Retry-After")
            logger.info(
                "confluence_request_retry",
                extra={
                    "event_name": "confluence_request_retry",
                    "event_payload": {"url": url, "attempt": attempt, "status_code": response.status_code},
                },
            )
            _sleep_before_retry(attempt=attempt, retry_after=retry_after)
            continue
        return response
    raise RuntimeError("Confluence-Request-Retry hat keinen Response geliefert.")


def _sleep_before_retry(*, attempt: int, retry_after: str | None) -> None:
    try:
        explicit_delay = float(str(retry_after or "").strip())
    except ValueError:
        explicit_delay = 0.0
    delay = explicit_delay if explicit_delay > 0 else min(0.35 * (2 ** max(0, attempt - 1)), 2.5)
    time.sleep(delay)


def _extract_storage_html(page_payload: dict[str, Any]) -> str:
    body = page_payload.get("body")
    if not isinstance(body, dict):
        return ""
    storage = body.get("storage")
    if isinstance(storage, dict):
        return str(storage.get("value") or "").strip()
    if isinstance(body.get("value"), str):
        return str(body.get("value") or "").strip()
    return ""


def _extract_storage_text_and_blocks(storage_html: str) -> tuple[str, list[dict[str, str]]]:
    if not storage_html:
        return "", []
    heading_stack: list[str] = []
    blocks: list[dict[str, str]] = []
    normalized = _preprocess_storage_markup(storage_html)
    normalized = re.sub(r"</h[1-6]>", "\n", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"<h([1-6])[^>]*>", lambda match: "\n" + ("#" * int(match.group(1))) + " ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"</p>|</li>|<br\s*/?>", "\n", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"<li[^>]*>", "- ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"<tr[^>]*>", "\n| ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"</tr>", "\n", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"<t[dh][^>]*>", "", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"</t[dh]>", " | ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"<[^>]+>", " ", normalized)
    normalized = html.unescape(normalized)
    lines = [_normalize_storage_line(line) for line in normalized.splitlines()]
    output_lines: list[str] = []
    for line in (line for line in lines if line):
        heading_match = re.match(r"^(#{1,6})\s+(.*)$", line)
        if heading_match is not None:
            depth = len(str(heading_match.group(1)))
            heading_title = str(heading_match.group(2)).strip()
            heading_stack = heading_stack[: depth - 1]
            heading_stack.append(heading_title)
            blocks.append(
                {
                    "kind": "heading",
                    "section_path": " / ".join(heading_stack),
                    "text": heading_title,
                }
            )
            output_lines.append(f"{'#' * depth} {heading_title}")
            continue
        blocks.append(
            {
                "kind": _classify_storage_line_kind(line=line),
                "section_path": " / ".join(heading_stack),
                "text": line,
            }
        )
        output_lines.append(line)
    return "\n".join(output_lines), blocks


def _preprocess_storage_markup(storage_html: str) -> str:
    normalized = storage_html
    normalized = re.sub(
        r"(?is)<ri:attachment[^>]*ri:filename=\"([^\"]+)\"[^>]*/?>",
        lambda match: f"\n[Attachment: {match.group(1).strip()}]\n",
        normalized,
    )
    normalized = re.sub(
        r"(?is)<ac:link[^>]*>\s*<ri:page[^>]*ri:content-title=\"([^\"]+)\"[^>]*/>\s*</ac:link>",
        lambda match: f"[Page Link: {match.group(1).strip()}]",
        normalized,
    )
    normalized = re.sub(
        r"(?is)<ac:structured-macro[^>]*ac:name=\"([^\"]+)\"[^>]*>(.*?)</ac:structured-macro>",
        _render_storage_macro,
        normalized,
    )
    return normalized


def _render_storage_macro(match: re.Match[str]) -> str:
    macro_name = str(match.group(1) or "").strip() or "macro"
    body = str(match.group(2) or "")
    title_match = re.search(r"(?is)<ac:parameter[^>]*ac:name=\"title\"[^>]*>(.*?)</ac:parameter>", body)
    title = html.unescape(re.sub(r"<[^>]+>", " ", str(title_match.group(1) if title_match else ""))).strip()
    rich_text_match = re.search(r"(?is)<ac:rich-text-body>(.*?)</ac:rich-text-body>", body)
    rich_text = str(rich_text_match.group(1) if rich_text_match else body)
    rich_text = re.sub(r"(?is)<ac:plain-text-body><!\[CDATA\[(.*?)\]\]></ac:plain-text-body>", r"\1", rich_text)
    rich_text = html.unescape(re.sub(r"<[^>]+>", " ", rich_text))
    rich_text = " ".join(rich_text.split())
    label = f"[Macro: {macro_name}{f' | {title}' if title else ''}]"
    return f"\n{label}\n{rich_text}\n" if rich_text else f"\n{label}\n"


def _normalize_storage_line(line: str) -> str:
    normalized = " ".join(str(line or "").split()).strip()
    normalized = re.sub(r"\|\s+\|", " | ", normalized)
    normalized = re.sub(r"^\|\s*", "| ", normalized)
    normalized = re.sub(r"\s*\|$", " |", normalized)
    return normalized


def _classify_storage_line_kind(*, line: str) -> str:
    normalized = str(line or "").strip()
    if normalized.startswith("[Attachment:"):
        return "attachment"
    if normalized.startswith("[Macro:"):
        return "macro"
    if normalized.startswith("| "):
        return "table_row"
    return "text"


def _extract_adf_document(page_payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(page_payload, dict):
        return None
    body = page_payload.get("body")
    if not isinstance(body, dict):
        return None
    atlas_doc_format = body.get("atlas_doc_format")
    candidates = [
        atlas_doc_format.get("value") if isinstance(atlas_doc_format, dict) else None,
        atlas_doc_format,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            if isinstance(candidate.get("value"), dict):
                return candidate.get("value")
            return candidate
        if isinstance(candidate, str):
            try:
                parsed = json.loads(candidate)
            except ValueError:
                continue
            if isinstance(parsed, dict):
                return parsed
    return None


def _extract_adf_text_and_blocks(adf_document: dict[str, Any] | None) -> tuple[str, list[dict[str, str]]]:
    if not isinstance(adf_document, dict):
        return "", []
    lines: list[str] = []
    blocks: list[dict[str, str]] = []
    _walk_adf_nodes(
        nodes=list(adf_document.get("content") or []),
        heading_stack=[],
        lines=lines,
        blocks=blocks,
    )
    return "\n".join(line for line in lines if str(line).strip()), blocks


def _walk_adf_nodes(
    *,
    nodes: list[Any],
    heading_stack: list[str],
    lines: list[str],
    blocks: list[dict[str, str]],
) -> None:
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_type = str(node.get("type") or "").strip()
        if node_type == "heading":
            level = int(node.get("attrs", {}).get("level") or 1)
            heading_text = _flatten_adf_text(node)
            if heading_text:
                next_stack = heading_stack[: max(0, level - 1)] + [heading_text]
                heading_stack[:] = next_stack
                lines.append(f"{'#' * max(1, level)} {heading_text}")
                blocks.append({"kind": "heading", "section_path": " / ".join(next_stack), "text": heading_text})
            continue
        if node_type in {"paragraph", "listItem", "tableCell", "tableHeader"}:
            text = _flatten_adf_text(node)
            if text:
                lines.append(text)
                blocks.append({"kind": "text", "section_path": " / ".join(heading_stack), "text": text})
        elif node_type == "tableRow":
            row_cells = _collect_adf_row_cells(node=node)
            if row_cells:
                rendered_row = "| " + " | ".join(row_cells) + " |"
                lines.append(rendered_row)
                blocks.append({"kind": "table_row", "section_path": " / ".join(heading_stack), "text": rendered_row})
        elif node_type == "codeBlock":
            code_text = _flatten_adf_text(node)
            if code_text:
                rendered_code = f"[Code Block] {code_text}"
                lines.append(rendered_code)
                blocks.append({"kind": "code_block", "section_path": " / ".join(heading_stack), "text": rendered_code})
        elif node_type in {"mediaSingle", "mediaGroup", "media"}:
            media_text = _describe_adf_media(node=node)
            if media_text:
                lines.append(media_text)
                blocks.append({"kind": "attachment", "section_path": " / ".join(heading_stack), "text": media_text})
        elif node_type in {"inlineCard", "blockCard"}:
            card_text = _describe_adf_card(node=node)
            if card_text:
                lines.append(card_text)
                blocks.append({"kind": "card", "section_path": " / ".join(heading_stack), "text": card_text})
        elif node_type == "status":
            status_text = _describe_adf_status(node=node)
            if status_text:
                lines.append(status_text)
                blocks.append({"kind": "status", "section_path": " / ".join(heading_stack), "text": status_text})
        elif node_type == "mention":
            mention_text = _describe_adf_mention(node=node)
            if mention_text:
                lines.append(mention_text)
                blocks.append({"kind": "mention", "section_path": " / ".join(heading_stack), "text": mention_text})
        elif node_type in {"expand", "panel", "bodiedExtension", "extension"}:
            extension_header = _describe_adf_extension(node=node)
            if extension_header:
                lines.append(extension_header)
                blocks.append({"kind": "macro", "section_path": " / ".join(heading_stack), "text": extension_header})
            _walk_adf_nodes(
                nodes=list(node.get("content") or []),
                heading_stack=list(heading_stack),
                lines=lines,
                blocks=blocks,
            )
        elif node_type == "rule":
            lines.append("[Horizontal Rule]")
            blocks.append({"kind": "separator", "section_path": " / ".join(heading_stack), "text": "[Horizontal Rule]"})
        elif node_type in {"bulletList", "orderedList", "table"}:
            _walk_adf_nodes(
                nodes=list(node.get("content") or []),
                heading_stack=list(heading_stack),
                lines=lines,
                blocks=blocks,
            )
        elif isinstance(node.get("content"), list):
            _walk_adf_nodes(
                nodes=list(node.get("content") or []),
                heading_stack=list(heading_stack),
                lines=lines,
                blocks=blocks,
            )


def _flatten_adf_text(node: dict[str, Any]) -> str:
    fragments: list[str] = []

    def visit(item: Any) -> None:
        if not isinstance(item, dict):
            return
        if item.get("type") == "text":
            text_value = str(item.get("text") or "").strip()
            if text_value:
                fragments.append(text_value)
        for child in item.get("content") or []:
            visit(child)

    visit(node)
    return " ".join(fragment for fragment in fragments if fragment).strip()


def _collect_adf_row_cells(*, node: dict[str, Any]) -> list[str]:
    cells: list[str] = []
    for child in node.get("content") or []:
        if not isinstance(child, dict):
            continue
        if str(child.get("type") or "").strip() not in {"tableCell", "tableHeader"}:
            continue
        cell_text = _flatten_adf_text(child)
        if cell_text:
            cells.append(cell_text)
    return cells


def _describe_adf_media(*, node: dict[str, Any]) -> str:
    attrs = node.get("attrs") if isinstance(node.get("attrs"), dict) else {}
    media_type = str(attrs.get("type") or "media").strip()
    alt_text = str(attrs.get("alt") or attrs.get("displayName") or attrs.get("fileName") or "").strip()
    collection = str(attrs.get("collection") or "").strip()
    label = alt_text or collection or media_type
    return f"[Attachment: {label}]"


def _describe_adf_card(*, node: dict[str, Any]) -> str:
    attrs = node.get("attrs") if isinstance(node.get("attrs"), dict) else {}
    url = str(attrs.get("url") or "").strip()
    title = str(attrs.get("title") or "").strip()
    label = title or url
    return f"[Card: {label}]" if label else ""


def _describe_adf_status(*, node: dict[str, Any]) -> str:
    attrs = node.get("attrs") if isinstance(node.get("attrs"), dict) else {}
    text = str(attrs.get("text") or "").strip()
    color = str(attrs.get("color") or "").strip()
    if not text and not color:
        return ""
    return f"[Status: {text}{f' | {color}' if color else ''}]"


def _describe_adf_mention(*, node: dict[str, Any]) -> str:
    attrs = node.get("attrs") if isinstance(node.get("attrs"), dict) else {}
    text = str(attrs.get("text") or attrs.get("id") or "").strip()
    return f"[Mention: {text}]" if text else ""


def _describe_adf_extension(*, node: dict[str, Any]) -> str:
    attrs = node.get("attrs") if isinstance(node.get("attrs"), dict) else {}
    extension_key = str(attrs.get("extensionKey") or attrs.get("layout") or node.get("type") or "").strip()
    title = str(attrs.get("title") or attrs.get("text") or "").strip()
    if not extension_key and not title:
        return ""
    return f"[Macro: {extension_key}{f' | {title}' if title else ''}]"
