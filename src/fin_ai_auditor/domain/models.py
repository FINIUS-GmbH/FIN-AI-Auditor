from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


FindingSeverity = Literal["critical", "high", "medium", "low"]
FindingCategory = Literal[
    "contradiction",
    "clarification_needed",
    "missing_definition",
    "stale_source",
    "implementation_drift",
    "read_write_gap",
    "traceability_gap",
    "ownership_gap",
    "policy_conflict",
    "terminology_collision",
    "low_confidence_review",
    "obsolete_documentation",
    "open_decision",
]
AuditRunStatus = Literal["planned", "running", "completed", "failed"]
FindingRelationType = Literal["contradicts", "supports", "duplicates", "depends_on", "gap_hint", "resolved_by"]
AuditProgressStepStatus = Literal["pending", "running", "completed", "failed", "skipped"]
AuditAnalysisLogLevel = Literal["info", "warning", "error"]
AuditAnalysisLogSource = Literal[
    "system",
    "pipeline",
    "decision_comment",
    "truth_update",
    "impact_analysis",
    "recommendation_regeneration",
]
ImplementedChangeType = Literal["confluence_page_updated", "jira_ticket_created"]
ImplementedChangeStatus = Literal["applied", "failed"]
ClaimSourceKind = Literal["github_file", "confluence_page", "jira_ticket", "metamodel", "local_doc", "user_truth"]
ClaimStatus = Literal["active", "superseded", "rejected"]
TruthStatus = Literal["active", "superseded", "rejected"]
TruthSourceKind = Literal["user_specification", "user_acceptance", "system_inference"]
DecisionPackageState = Literal["open", "accepted", "rejected", "specified", "superseded"]
DecisionAction = Literal["accept", "reject", "specify"]
ApprovalTargetType = Literal["confluence_page_update", "jira_ticket_create"]
ApprovalStatus = Literal["pending", "approved", "rejected", "executed", "cancelled"]
ConfluencePatchOperationType = Literal["append_after_heading", "append_to_page"]
ConfluencePatchMarkerKind = Literal["remove", "correct", "confirm", "insert"]
SemanticEntityType = Literal[
    "object",
    "process",
    "phase",
    "question",
    "policy",
    "lifecycle",
    "read_contract",
    "write_contract",
    "documentation_section",
    "code_component",
    "truth",
]
SemanticRelationType = Literal[
    "belongs_to",
    "governs",
    "documents",
    "implements",
    "references",
    "derived_from_truth",
    "contains",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _normalized_string_list(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        value = str(raw_value or "").strip()
        if not value or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    return normalized


def new_run_id() -> str:
    return f"audit_{uuid4().hex}"


def new_snapshot_id() -> str:
    return f"snapshot_{uuid4().hex}"


def new_location_id() -> str:
    return f"location_{uuid4().hex}"


def new_link_id() -> str:
    return f"link_{uuid4().hex}"


def new_log_id() -> str:
    return f"log_{uuid4().hex}"


def new_change_id() -> str:
    return f"change_{uuid4().hex}"


def new_claim_id() -> str:
    return f"claim_{uuid4().hex}"


def new_truth_id() -> str:
    return f"truth_{uuid4().hex}"


def new_package_id() -> str:
    return f"package_{uuid4().hex}"


def new_problem_id() -> str:
    return f"problem_{uuid4().hex}"


def new_decision_id() -> str:
    return f"decision_{uuid4().hex}"


def new_approval_request_id() -> str:
    return f"approval_{uuid4().hex}"


def new_patch_operation_id() -> str:
    return f"patchop_{uuid4().hex}"


def new_segment_id() -> str:
    return f"segment_{uuid4().hex}"


def new_auth_state_id() -> str:
    return f"oauth_state_{uuid4().hex}"


def new_semantic_entity_id() -> str:
    return f"semantic_entity_{uuid4().hex}"


def new_semantic_relation_id() -> str:
    return f"semantic_relation_{uuid4().hex}"


class AuditTarget(BaseModel):
    github_repo_url: str | None = None
    local_repo_path: str | None = None
    github_ref: str = Field(default="main", min_length=1)
    confluence_space_keys: list[str] = Field(default_factory=list)
    confluence_page_ids: list[str] = Field(default_factory=list)
    jira_project_keys: list[str] = Field(default_factory=list)
    include_metamodel: bool = True
    include_local_docs: bool = True

    @model_validator(mode="after")
    def validate_repo_source(self) -> "AuditTarget":
        github_repo_url = str(self.github_repo_url or "").strip()
        local_repo_path = str(self.local_repo_path or "").strip()
        if not github_repo_url and not local_repo_path:
            raise ValueError("Mindestens github_repo_url oder local_repo_path muss gesetzt sein.")
        self.github_repo_url = github_repo_url or None
        self.local_repo_path = local_repo_path or None
        self.confluence_space_keys = _normalized_string_list(self.confluence_space_keys)
        self.confluence_page_ids = _normalized_string_list(self.confluence_page_ids)
        self.jira_project_keys = _normalized_string_list(self.jira_project_keys)
        return self


class AuditLocation(BaseModel):
    location_id: str = Field(default_factory=new_location_id)
    snapshot_id: str | None = None
    source_type: Literal["github_file", "confluence_page", "jira_ticket", "metamodel", "local_doc"]
    source_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    path_hint: str | None = None
    url: str | None = None
    position: "AuditPosition | None" = None
    metadata: dict[str, object] = Field(default_factory=dict)


class AuditPosition(BaseModel):
    anchor_kind: str = Field(min_length=1)
    anchor_value: str = Field(min_length=1)
    section_path: str | None = None
    line_start: int | None = None
    line_end: int | None = None
    char_start: int | None = None
    char_end: int | None = None
    snippet_hash: str | None = None
    content_hash: str | None = None


class AuditSourceSnapshot(BaseModel):
    snapshot_id: str = Field(default_factory=new_snapshot_id)
    source_type: Literal["github_file", "confluence_page", "jira_ticket", "metamodel", "local_doc"]
    source_id: str = Field(min_length=1)
    revision_id: str | None = None
    content_hash: str | None = None
    sync_token: str | None = None
    parent_snapshot_id: str | None = None
    collected_at: str = Field(default_factory=utc_now_iso)
    metadata: dict[str, object] = Field(default_factory=dict)


class AuditFinding(BaseModel):
    finding_id: str = Field(default_factory=lambda: f"finding_{uuid4().hex}")
    severity: FindingSeverity
    category: FindingCategory
    title: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    recommendation: str = Field(min_length=1)
    canonical_key: str | None = None
    resolution_state: Literal["open", "accepted", "dismissed", "superseded"] = "open"
    locations: list[AuditLocation] = Field(default_factory=list)
    proposed_confluence_action: str | None = None
    proposed_jira_action: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class AuditFindingLink(BaseModel):
    link_id: str = Field(default_factory=new_link_id)
    from_finding_id: str = Field(min_length=1)
    to_finding_id: str = Field(min_length=1)
    relation_type: FindingRelationType
    rationale: str = Field(min_length=1)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    metadata: dict[str, object] = Field(default_factory=dict)


class AuditProgressStep(BaseModel):
    step_key: str = Field(min_length=1)
    label: str = Field(min_length=1)
    status: AuditProgressStepStatus = "pending"
    detail: str | None = None
    started_at: str | None = None
    finished_at: str | None = None


class AuditRunProgress(BaseModel):
    progress_pct: int = Field(default=0, ge=0, le=100)
    phase_key: str = Field(default="queued", min_length=1)
    phase_label: str = Field(default="Wartet", min_length=1)
    current_activity: str = Field(default="Run wurde angelegt und wartet auf Verarbeitung.", min_length=1)
    steps: list[AuditProgressStep] = Field(default_factory=list)


class AuditAnalysisLogEntry(BaseModel):
    log_id: str = Field(default_factory=new_log_id)
    created_at: str = Field(default_factory=utc_now_iso)
    level: AuditAnalysisLogLevel = "info"
    source_type: AuditAnalysisLogSource = "system"
    title: str = Field(min_length=1)
    message: str = Field(min_length=1)
    related_finding_ids: list[str] = Field(default_factory=list)
    related_scope_keys: list[str] = Field(default_factory=list)
    derived_changes: list[str] = Field(default_factory=list)
    impact_summary: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


class JiraTicketAICodingBrief(BaseModel):
    ticket_key: str = Field(min_length=1)
    ticket_url: str | None = None
    title: str = Field(min_length=1)
    problem_description: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    correction_measures: list[str] = Field(default_factory=list)
    target_state: list[str] = Field(default_factory=list)
    acceptance_criteria: list[str] = Field(default_factory=list)
    implications: list[str] = Field(default_factory=list)
    affected_parts: list[str] = Field(default_factory=list)
    evidence: list[str] = Field(default_factory=list)
    implementation_notes: list[str] = Field(default_factory=list)
    validation_steps: list[str] = Field(default_factory=list)
    ai_coding_prompt: str = Field(min_length=1)


class ConfluencePatchOperation(BaseModel):
    operation_id: str = Field(default_factory=new_patch_operation_id)
    related_finding_id: str | None = None
    action_type: ConfluencePatchOperationType
    marker_kind: ConfluencePatchMarkerKind
    section_path: str = Field(min_length=1)
    anchor_heading: str = Field(min_length=1)
    current_statement: str | None = None
    proposed_statement: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    storage_snippet: str = Field(min_length=1)


class ConfluencePatchPreview(BaseModel):
    page_id: str | None = None
    page_title: str = Field(min_length=1)
    page_url: str = Field(min_length=1)
    space_key: str | None = None
    base_revision_id: str | None = None
    execution_ready: bool = False
    blockers: list[str] = Field(default_factory=list)
    changed_sections: list[str] = Field(default_factory=list)
    change_summary: list[str] = Field(default_factory=list)
    review_storage_snippets: list[str] = Field(default_factory=list)
    operations: list[ConfluencePatchOperation] = Field(default_factory=list)


class ConfluencePageUpdateDetails(BaseModel):
    page_title: str = Field(min_length=1)
    page_url: str = Field(min_length=1)
    changed_sections: list[str] = Field(default_factory=list)
    change_summary: list[str] = Field(default_factory=list)
    page_id: str | None = None
    applied_revision_id: str | None = None
    execution_mode: str | None = None
    patch_preview: ConfluencePatchPreview | None = None


class AuditImplementedChange(BaseModel):
    change_id: str = Field(default_factory=new_change_id)
    created_at: str = Field(default_factory=utc_now_iso)
    status: ImplementedChangeStatus = "applied"
    change_type: ImplementedChangeType
    title: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    target_label: str = Field(min_length=1)
    target_url: str | None = None
    related_finding_ids: list[str] = Field(default_factory=list)
    implications: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)
    jira_ticket: JiraTicketAICodingBrief | None = None
    confluence_update: ConfluencePageUpdateDetails | None = None


class AtlassianOAuthStateRecord(BaseModel):
    state_id: str = Field(default_factory=new_auth_state_id)
    created_at: str = Field(default_factory=utc_now_iso)
    expires_at: str = Field(min_length=1)
    redirect_uri: str = Field(min_length=1)
    status: Literal["pending", "consumed", "expired", "failed"] = "pending"
    scope: str = Field(min_length=1)
    metadata: dict[str, object] = Field(default_factory=dict)


class AtlassianOAuthTokenRecord(BaseModel):
    provider: Literal["atlassian"] = "atlassian"
    access_token: str = Field(min_length=1)
    refresh_token: str | None = None
    scope: str | None = None
    token_type: str = Field(default="bearer", min_length=1)
    obtained_at: str = Field(default_factory=utc_now_iso)
    expires_at: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class AtlassianAuthStatus(BaseModel):
    enabled: bool = False
    client_configured: bool = False
    token_present: bool = False
    token_valid: bool = False
    needs_user_consent: bool = True
    redirect_uri: str | None = None
    configured_redirect_uri: str | None = None
    recommended_redirect_uri: str | None = None
    redirect_uri_matches_local_api: bool = False
    scope: str | None = None
    token_expires_at: str | None = None
    last_error: str | None = None
    notes: list[str] = Field(default_factory=list)


class AtlassianAuthorizationStart(BaseModel):
    authorization_url: str = Field(min_length=1)
    state_id: str = Field(min_length=1)
    redirect_uri: str = Field(min_length=1)
    notes: list[str] = Field(default_factory=list)


class ConfluenceVerificationResponse(BaseModel):
    ok: bool = False
    space_key: str = Field(min_length=1)
    page_count: int = Field(default=0, ge=0)
    page_titles: list[str] = Field(default_factory=list)
    analysis_notes: list[str] = Field(default_factory=list)


class RetrievalSegment(BaseModel):
    segment_id: str = Field(default_factory=new_segment_id)
    run_id: str = Field(min_length=1)
    snapshot_id: str | None = None
    source_type: Literal["github_file", "confluence_page", "metamodel", "local_doc"]
    source_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    path_hint: str | None = None
    url: str | None = None
    anchor_kind: str = Field(min_length=1)
    anchor_value: str = Field(min_length=1)
    section_path: str | None = None
    ordinal: int = Field(ge=0)
    content: str = Field(min_length=1)
    content_hash: str | None = None
    segment_hash: str = Field(min_length=1)
    token_count: int = Field(default=0, ge=0)
    delta_status: Literal["added", "changed", "unchanged"] = "added"
    keywords: list[str] = Field(default_factory=list)
    embedding: list[float] | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class RetrievalSegmentClaimLink(BaseModel):
    segment_id: str = Field(min_length=1)
    claim_id: str = Field(min_length=1)
    relation_type: Literal["evidence", "semantic_support", "scope_match"] = "evidence"
    score: float = Field(default=0.5, ge=0.0, le=1.0)
    metadata: dict[str, object] = Field(default_factory=dict)


class SemanticEntity(BaseModel):
    entity_id: str = Field(default_factory=new_semantic_entity_id)
    run_id: str = Field(min_length=1)
    entity_type: SemanticEntityType
    canonical_key: str = Field(min_length=1)
    label: str = Field(min_length=1)
    scope_key: str = Field(min_length=1)
    source_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


class SemanticRelation(BaseModel):
    relation_id: str = Field(default_factory=new_semantic_relation_id)
    run_id: str = Field(min_length=1)
    source_entity_id: str = Field(min_length=1)
    target_entity_id: str = Field(min_length=1)
    relation_type: SemanticRelationType
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    metadata: dict[str, object] = Field(default_factory=dict)


class AuditClaimEntry(BaseModel):
    claim_id: str = Field(default_factory=new_claim_id)
    source_snapshot_id: str | None = None
    source_type: ClaimSourceKind
    source_id: str = Field(min_length=1)
    subject_kind: str = Field(min_length=1)
    subject_key: str = Field(min_length=1)
    predicate: str = Field(min_length=1)
    normalized_value: str = Field(min_length=1)
    scope_kind: str = Field(min_length=1)
    scope_key: str = Field(min_length=1)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    fingerprint: str = Field(min_length=1)
    status: ClaimStatus = "active"
    evidence_location_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


class TruthLedgerEntry(BaseModel):
    truth_id: str = Field(default_factory=new_truth_id)
    canonical_key: str = Field(min_length=1)
    subject_kind: str = Field(min_length=1)
    subject_key: str = Field(min_length=1)
    predicate: str = Field(min_length=1)
    normalized_value: str = Field(min_length=1)
    scope_kind: str = Field(min_length=1)
    scope_key: str = Field(min_length=1)
    truth_status: TruthStatus = "active"
    source_kind: TruthSourceKind
    created_from_problem_id: str | None = None
    supersedes_truth_id: str | None = None
    valid_from_snapshot_id: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class DecisionProblemElement(BaseModel):
    problem_id: str = Field(default_factory=new_problem_id)
    finding_id: str | None = None
    category: FindingCategory
    severity: FindingSeverity
    scope_summary: str = Field(min_length=1)
    short_explanation: str = Field(min_length=1)
    recommendation: str = Field(min_length=1)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    affected_claim_ids: list[str] = Field(default_factory=list)
    affected_truth_ids: list[str] = Field(default_factory=list)
    evidence_locations: list[AuditLocation] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


class DecisionPackage(BaseModel):
    package_id: str = Field(default_factory=new_package_id)
    title: str = Field(min_length=1)
    category: FindingCategory
    severity_summary: FindingSeverity
    scope_summary: str = Field(min_length=1)
    decision_state: DecisionPackageState = "open"
    decision_required: bool = True
    rerender_required_after_decision: bool = False
    recommendation_summary: str = Field(min_length=1)
    related_finding_ids: list[str] = Field(default_factory=list)
    problem_elements: list[DecisionProblemElement] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


class DecisionRecord(BaseModel):
    decision_id: str = Field(default_factory=new_decision_id)
    package_id: str = Field(min_length=1)
    action: DecisionAction
    created_at: str = Field(default_factory=utc_now_iso)
    comment_text: str | None = None
    created_truth_ids: list[str] = Field(default_factory=list)
    impacted_package_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


class WritebackApprovalRequest(BaseModel):
    approval_request_id: str = Field(default_factory=new_approval_request_id)
    created_at: str = Field(default_factory=utc_now_iso)
    target_type: ApprovalTargetType
    status: ApprovalStatus = "pending"
    title: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    target_url: str | None = None
    related_package_ids: list[str] = Field(default_factory=list)
    related_finding_ids: list[str] = Field(default_factory=list)
    payload_preview: list[str] = Field(default_factory=list)
    decided_at: str | None = None
    decision_comment: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class AuditRun(BaseModel):
    run_id: str = Field(default_factory=new_run_id)
    status: AuditRunStatus = "planned"
    target: AuditTarget
    created_at: str = Field(default_factory=utc_now_iso)
    updated_at: str = Field(default_factory=utc_now_iso)
    started_at: str | None = None
    finished_at: str | None = None
    summary: str | None = None
    progress: AuditRunProgress = Field(default_factory=AuditRunProgress)
    analysis_log: list[AuditAnalysisLogEntry] = Field(default_factory=list)
    claims: list[AuditClaimEntry] = Field(default_factory=list)
    truths: list[TruthLedgerEntry] = Field(default_factory=list)
    decision_packages: list[DecisionPackage] = Field(default_factory=list)
    decision_records: list[DecisionRecord] = Field(default_factory=list)
    approval_requests: list[WritebackApprovalRequest] = Field(default_factory=list)
    implemented_changes: list[AuditImplementedChange] = Field(default_factory=list)
    source_snapshots: list[AuditSourceSnapshot] = Field(default_factory=list)
    semantic_entities: list[SemanticEntity] = Field(default_factory=list)
    semantic_relations: list[SemanticRelation] = Field(default_factory=list)
    findings: list[AuditFinding] = Field(default_factory=list)
    finding_links: list[AuditFindingLink] = Field(default_factory=list)
    llm_usage: dict = Field(default_factory=dict)
    error: str | None = None


class CreateAuditRunRequest(BaseModel):
    target: AuditTarget


class CreateDecisionCommentRequest(BaseModel):
    comment_text: str = Field(min_length=1)
    related_finding_ids: list[str] = Field(default_factory=list)


class DecisionCommentAnalysis(BaseModel):
    normalized_truths: list[str] = Field(default_factory=list)
    derived_changes: list[str] = Field(default_factory=list)
    impact_summary: list[str] = Field(default_factory=list)
    related_scope_keys: list[str] = Field(default_factory=list)


class DecisionPackageActionRequest(BaseModel):
    action: DecisionAction
    comment_text: str | None = None


class CreateWritebackApprovalRequest(BaseModel):
    target_type: ApprovalTargetType
    title: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    target_url: str | None = None
    related_package_ids: list[str] = Field(default_factory=list)
    related_finding_ids: list[str] = Field(default_factory=list)
    payload_preview: list[str] = Field(default_factory=list)


class ResolveWritebackApprovalRequest(BaseModel):
    decision: Literal["approve", "reject", "cancel"]
    comment_text: str | None = None


class RecordConfluencePageUpdateRequest(BaseModel):
    approval_request_id: str = Field(min_length=1)
    page_title: str = Field(min_length=1)
    page_url: str = Field(min_length=1)
    changed_sections: list[str] = Field(default_factory=list)
    change_summary: list[str] = Field(default_factory=list)
    related_finding_ids: list[str] = Field(default_factory=list)


class RecordJiraTicketCreatedRequest(BaseModel):
    approval_request_id: str = Field(min_length=1)
    ticket_key: str = Field(min_length=1)
    ticket_url: str = Field(min_length=1)
    related_finding_ids: list[str] = Field(default_factory=list)


class AuditRunListResponse(BaseModel):
    items: list[AuditRun]
