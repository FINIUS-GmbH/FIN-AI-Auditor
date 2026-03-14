from __future__ import annotations

from pathlib import Path

import httpx

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import AuditSourceSnapshot
from fin_ai_auditor.services.connectors.confluence_connector import (
    ConfluenceCollectionRequest,
    ConfluenceKnowledgeBaseConnector,
)
from fin_ai_auditor.services.pipeline_models import CachedCollectedDocument


class _FakeResponse:
    def __init__(self, payload: object, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("boom", request=httpx.Request("GET", "https://example.com"), response=httpx.Response(self.status_code))

    def json(self) -> object:
        return self._payload


class _FakeClient:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self._headers = kwargs.get("headers", {})

    def __enter__(self) -> "_FakeClient":
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
                        "scopes": ["read:page:confluence", "read:content-details:confluence"],
                    }
                ]
            )
        if url.endswith("/wiki/api/v2/spaces"):
            return _FakeResponse({"results": [{"id": "space-1", "key": "FINAI"}]})
        if url.endswith("/wiki/api/v2/spaces/space-1/pages"):
            return _FakeResponse(
                {
                    "results": [
                        {
                            "id": "page-1",
                            "title": "Statement Contract",
                            "spaceId": "space-1",
                            "version": {"number": 7},
                        }
                    ]
                }
            )
        if url.endswith("/wiki/api/v2/pages/page-1"):
            if params and params.get("body-format") == "atlas_doc_format":
                return _FakeResponse(
                    {
                        "id": "page-1",
                        "title": "Statement Contract",
                        "spaceId": "space-1",
                        "version": {"number": 7},
                        "body": {
                            "atlas_doc_format": {
                                "value": {
                                    "type": "doc",
                                    "content": [
                                        {
                                            "type": "heading",
                                            "attrs": {"level": 1},
                                            "content": [{"type": "text", "text": "Statement"}],
                                        },
                                        {
                                            "type": "paragraph",
                                            "content": [{"type": "text", "text": "Write path must stay approval-gated."}],
                                        },
                                    ],
                                }
                            }
                        },
                    }
                )
            return _FakeResponse(
                {
                    "id": "page-1",
                    "title": "Statement Contract",
                    "spaceId": "space-1",
                    "version": {"number": 7},
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
        raise AssertionError(f"Unexpected URL: {url} params={params} headers={headers or self._headers}")


class _UnauthorizedClient(_FakeClient):
    def get(self, url: str, params: dict[str, object] | None = None, headers: dict[str, str] | None = None) -> _FakeResponse:
        if url.endswith("/oauth/token/accessible-resources"):
            return _FakeResponse({"error": "unauthorized"}, status_code=401)
        if "/wiki/api/v2/" in url:
            return _FakeResponse({"error": "unauthorized"}, status_code=401)
        raise AssertionError(f"Unexpected URL for unauthorized client: {url} params={params} headers={headers or self._headers}")


def test_confluence_connector_reads_pages_via_accessible_resources(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        Settings,
        "_collect_external_env_map",
        lambda self: {"FINAI_ATLASSIAN_ACCESS_TOKEN": "token-123"},
    )
    monkeypatch.setattr("fin_ai_auditor.services.connectors.confluence_connector.httpx.Client", _FakeClient)
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=tmp_path / "metamodel.json")

    connector = ConfluenceKnowledgeBaseConnector(settings=settings)
    bundle = connector.collect_pages(request=ConfluenceCollectionRequest(space_keys=["FINAI"], max_pages_per_space=5))

    assert len(bundle.documents) == 1
    assert bundle.documents[0].source_type == "confluence_page"
    assert "Statement" in bundle.documents[0].body
    assert bundle.documents[0].url == "https://finius.atlassian.net/wiki/spaces/FINAI/pages/page-1/Statement+Contract"
    assert bundle.documents[0].metadata["confluence_representation"] == "atlas_doc_format"
    assert bundle.documents[0].metadata["structured_blocks"]
    assert any("accessible-resources" in note for note in bundle.analysis_notes)


def test_confluence_connector_degrades_gracefully_on_unauthorized_access(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        Settings,
        "_collect_external_env_map",
        lambda self: {"FINAI_ATLASSIAN_ACCESS_TOKEN": "expired-token"},
    )
    monkeypatch.setattr("fin_ai_auditor.services.connectors.confluence_connector.httpx.Client", _UnauthorizedClient)
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=tmp_path / "metamodel.json")

    connector = ConfluenceKnowledgeBaseConnector(settings=settings)
    bundle = connector.collect_pages(request=ConfluenceCollectionRequest(space_keys=["FINAI"], max_pages_per_space=5))

    assert bundle.documents == []
    assert any("direkten Site-Endpunkt" in note for note in bundle.analysis_notes)
    assert any("accessible-resources" in note for note in bundle.analysis_notes)
    assert any("401" in note for note in bundle.analysis_notes)


def test_confluence_connector_reuses_cached_pages_when_revision_is_unchanged(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        Settings,
        "_collect_external_env_map",
        lambda self: {"FINAI_ATLASSIAN_ACCESS_TOKEN": "token-123"},
    )
    monkeypatch.setattr("fin_ai_auditor.services.connectors.confluence_connector.httpx.Client", _FakeClient)
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=tmp_path / "metamodel.json")
    connector = ConfluenceKnowledgeBaseConnector(settings=settings)

    bundle = connector.collect_pages(
        request=ConfluenceCollectionRequest(space_keys=["FINAI"], max_pages_per_space=5),
        previous_snapshots=[
            AuditSourceSnapshot(
                source_type="confluence_page",
                source_id="page-1",
                revision_id="7",
                content_hash="sha256:cached",
            )
        ],
        document_cache_lookup=lambda source_type, source_id, content_hash: CachedCollectedDocument(
            source_type="confluence_page",
            source_id=source_id,
            content_hash=content_hash,
            title="Statement Contract",
            body="# Statement\nCached body",
            path_hint="Space FINAI / Contracts / Statement Contract",
            url="https://finius.atlassian.net/wiki/spaces/FINAI/pages/page-1/Statement+Contract",
            metadata={"ancestor_titles": ["Contracts"], "confluence_representation": "storage"},
        ),
    )

    assert len(bundle.documents) == 1
    assert bundle.documents[0].body == "# Statement\nCached body"
    assert bundle.documents[0].metadata["incremental_reused"] is True
    assert any("Confluence inkrementell" in note for note in bundle.analysis_notes)


def test_confluence_connector_extracts_tables_macros_and_attachments(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        Settings,
        "_collect_external_env_map",
        lambda self: {"FINAI_ATLASSIAN_ACCESS_TOKEN": "token-123"},
    )

    class _ComplexClient(_FakeClient):
        def get(
            self,
            url: str,
            params: dict[str, object] | None = None,
            headers: dict[str, str] | None = None,
        ) -> _FakeResponse:
            if url.endswith("/wiki/api/v2/pages/page-1") and params and params.get("body-format") == "atlas_doc_format":
                return _FakeResponse(
                    {
                        "id": "page-1",
                        "title": "Statement Contract",
                        "spaceId": "space-1",
                        "version": {"number": 7},
                        "body": {
                            "atlas_doc_format": {
                                "value": {
                                    "type": "doc",
                                    "content": [
                                        {"type": "heading", "attrs": {"level": 1}, "content": [{"type": "text", "text": "Statement"}]},
                                        {
                                            "type": "table",
                                            "content": [
                                                {
                                                    "type": "tableRow",
                                                    "content": [
                                                        {"type": "tableHeader", "content": [{"type": "text", "text": "Scope"}]},
                                                        {"type": "tableHeader", "content": [{"type": "text", "text": "Rule"}]},
                                                    ],
                                                },
                                                {
                                                    "type": "tableRow",
                                                    "content": [
                                                        {"type": "tableCell", "content": [{"type": "paragraph", "content": [{"type": "text", "text": "Write"}]}]},
                                                        {"type": "tableCell", "content": [{"type": "paragraph", "content": [{"type": "text", "text": "Approval gated"}]}]},
                                                    ],
                                                },
                                            ],
                                        },
                                        {"type": "mediaSingle", "content": [{"type": "media", "attrs": {"fileName": "statement.png"}}]},
                                        {"type": "status", "attrs": {"text": "Approved", "color": "green"}},
                                    ],
                                }
                            }
                        },
                    }
                )
            if url.endswith("/wiki/api/v2/pages/page-1"):
                return _FakeResponse(
                    {
                        "id": "page-1",
                        "title": "Statement Contract",
                        "spaceId": "space-1",
                        "version": {"number": 7},
                        "body": {
                            "storage": {
                                "value": (
                                    "<h1>Statement</h1>"
                                    "<ac:structured-macro ac:name=\"expand\">"
                                    "<ac:parameter ac:name=\"title\">Review notes</ac:parameter>"
                                    "<ac:rich-text-body><p>Write needs approval.</p></ac:rich-text-body>"
                                    "</ac:structured-macro>"
                                    "<table><tr><th>Scope</th><th>Rule</th></tr><tr><td>Write</td><td>Approval gated</td></tr></table>"
                                    "<ri:attachment ri:filename=\"statement.pdf\" />"
                                )
                            }
                        },
                    }
                )
            return super().get(url, params=params, headers=headers)

    monkeypatch.setattr("fin_ai_auditor.services.connectors.confluence_connector.httpx.Client", _ComplexClient)
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=tmp_path / "metamodel.json")
    connector = ConfluenceKnowledgeBaseConnector(settings=settings)

    bundle = connector.collect_pages(request=ConfluenceCollectionRequest(space_keys=["FINAI"], max_pages_per_space=5))

    assert len(bundle.documents) == 1
    body = bundle.documents[0].body
    structured_blocks = bundle.documents[0].metadata["structured_blocks"]
    assert "| Scope | Rule |" in body
    assert any(block["kind"] == "table_row" for block in structured_blocks)
    assert any(block["kind"] == "attachment" for block in structured_blocks)
    assert any(block["kind"] == "status" for block in structured_blocks)


def test_confluence_connector_marks_changed_section_paths_against_latest_cached_page(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        Settings,
        "_collect_external_env_map",
        lambda self: {"FINAI_ATLASSIAN_ACCESS_TOKEN": "token-123"},
    )
    monkeypatch.setattr("fin_ai_auditor.services.connectors.confluence_connector.httpx.Client", _FakeClient)
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=tmp_path / "metamodel.json")
    connector = ConfluenceKnowledgeBaseConnector(settings=settings)

    bundle = connector.collect_pages(
        request=ConfluenceCollectionRequest(space_keys=["FINAI"], max_pages_per_space=5),
        latest_document_cache_lookup=lambda source_type, source_id: CachedCollectedDocument(
            source_type="confluence_page",
            source_id=source_id,
            content_hash="sha256:old",
            title="Statement Contract",
            body="# Statement\nOld body",
            cached_at="2026-03-10T00:00:00+00:00",
            path_hint="Space FINAI / Contracts / Statement Contract",
            url="https://finius.atlassian.net/wiki/spaces/FINAI/pages/page-1/Statement+Contract",
            metadata={
                "structured_blocks": [
                    {"kind": "heading", "section_path": "Statement", "text": "Statement"},
                    {"kind": "text", "section_path": "Statement", "text": "Old approval rule."},
                ]
            },
        ),
    )

    assert len(bundle.documents) == 1
    metadata = bundle.documents[0].metadata
    assert metadata["section_delta_status"] == "changed"
    assert metadata["changed_section_paths"] == ["Statement"]
    assert metadata["analysis_cache_role"] == "confluence_analysis_cache"


def test_confluence_connector_derives_restriction_and_sensitivity_from_labels_and_properties(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        Settings,
        "_collect_external_env_map",
        lambda self: {"FINAI_ATLASSIAN_ACCESS_TOKEN": "token-123"},
    )

    class _SensitiveClient(_FakeClient):
        def get(
            self,
            url: str,
            params: dict[str, object] | None = None,
            headers: dict[str, str] | None = None,
        ) -> _FakeResponse:
            if url.endswith("/wiki/api/v2/pages/page-1") and not (params and params.get("body-format") == "atlas_doc_format"):
                return _FakeResponse(
                    {
                        "id": "page-1",
                        "title": "Statement Contract",
                        "spaceId": "space-1",
                        "version": {"number": 7},
                        "labels": {"results": [{"name": "confidential"}, {"name": "finai"}]},
                        "properties": {
                            "results": [
                                {"key": "classificationLevel", "value": "internal-sensitive"},
                                {"key": "accessMode", "value": "restricted"},
                            ]
                        },
                        "operations": [{"operation": "read"}],
                        "body": {
                            "storage": {
                                "value": "<h1>Statement</h1><p>Write path must stay approval-gated.</p>"
                            }
                        },
                    }
                )
            return super().get(url, params=params, headers=headers)

    monkeypatch.setattr("fin_ai_auditor.services.connectors.confluence_connector.httpx.Client", _SensitiveClient)
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=tmp_path / "metamodel.json")

    connector = ConfluenceKnowledgeBaseConnector(settings=settings)
    bundle = connector.collect_pages(request=ConfluenceCollectionRequest(space_keys=["FINAI"], max_pages_per_space=5))

    assert len(bundle.documents) == 1
    metadata = bundle.documents[0].metadata
    assert metadata["restriction_state"] == "restricted"
    assert metadata["sensitivity_level"] == "restricted"
    assert metadata["restriction_signal_source"] == "page.properties.accessMode"
    assert metadata["sensitivity_signal"] == "classificationLevel=internal-sensitive"
