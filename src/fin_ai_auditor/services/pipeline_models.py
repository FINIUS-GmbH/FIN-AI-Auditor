from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from fin_ai_auditor.domain.models import (
    AuditFinding,
    AuditFindingLink,
    AuditLocation,
    SemanticEntity,
    SemanticRelation,
    AuditSourceSnapshot,
    RetrievalSegment,
    RetrievalSegmentClaimLink,
    TruthLedgerEntry,
)


CollectedSourceType = Literal["github_file", "confluence_page", "jira_ticket", "metamodel", "local_doc"]


@dataclass(frozen=True)
class CollectedDocument:
    snapshot: AuditSourceSnapshot
    source_type: CollectedSourceType
    source_id: str
    title: str
    body: str
    path_hint: str | None = None
    url: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class CachedCollectedDocument:
    source_type: CollectedSourceType
    source_id: str
    content_hash: str
    title: str
    body: str
    cached_at: str | None = None
    path_hint: str | None = None
    url: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class ExtractedClaimEvidence:
    location: AuditLocation
    matched_text: str


@dataclass(frozen=True)
class ExtractedClaimRecord:
    claim: object
    evidence: ExtractedClaimEvidence


@dataclass(frozen=True)
class CollectionBundle:
    snapshots: list[AuditSourceSnapshot]
    documents: list[CollectedDocument]
    analysis_notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PipelineAnalysisResult:
    source_snapshots: list[AuditSourceSnapshot]
    findings: list[AuditFinding]
    finding_links: list[AuditFindingLink]
    claims: list[object]
    truths: list[TruthLedgerEntry]
    semantic_entities: list[SemanticEntity]
    semantic_relations: list[SemanticRelation]
    retrieval_segments: list[RetrievalSegment]
    retrieval_claim_links: list[RetrievalSegmentClaimLink]
    analysis_log_messages: list[str]
    summary: str
    llm_usage: dict = field(default_factory=dict)
