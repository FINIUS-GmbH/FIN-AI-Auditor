from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


FindingSeverity = Literal["critical", "high", "medium", "low"]
FindingCategory = Literal[
    "contradiction",
    "architecture_observation",
    "clarification_needed",
    "missing_definition",
    "missing_documentation",
    "stale_source",
    "implementation_drift",
    "read_write_gap",
    "traceability_gap",
    "ownership_gap",
    "policy_conflict",
    "legacy_path_gap",
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
    "clarification_dialog",
]
ImplementedChangeType = Literal["confluence_page_updated", "jira_ticket_created"]
ImplementedChangeStatus = Literal["applied", "failed"]
ClaimSourceKind = Literal["github_file", "confluence_page", "jira_ticket", "metamodel", "local_doc", "user_truth"]
ClaimStatus = Literal["active", "superseded", "rejected"]
ClaimAssertionStatus = Literal["asserted", "excluded", "deprecated", "not_ssot", "secondary_only"]
ClaimSourceAuthority = Literal[
    "explicit_truth",
    "confirmed_decision",
    "ssot",
    "governed",
    "working_doc",
    "historical",
    "runtime_observation",
    "implementation",
    "heuristic",
]
TruthStatus = Literal["active", "superseded", "rejected"]
TruthSourceKind = Literal["user_specification", "user_acceptance", "system_inference", "clarification_dialog"]
SchemaTruthStatus = Literal[
    "confirmed_ssot",
    "provisional_target",
    "observed_only",
    "code_only_inference",
    "rejected_target",
]
SchemaTruthSourceKind = Literal["truth_ledger", "metamodel", "documentation", "runtime_observation", "implementation_inference"]
AtomicFactStatus = Literal["open", "confirmed", "resolved", "superseded"]
AtomicFactActionLane = Literal["confluence_doc", "jira_code", "jira_artifact", "confluence_and_jira"]
DecisionPackageState = Literal["open", "accepted", "rejected", "specified", "superseded"]
DecisionAction = Literal["accept", "reject", "specify"]
ApprovalTargetType = Literal["confluence_page_update", "jira_ticket_create"]
ApprovalStatus = Literal["pending", "approved", "rejected", "executed", "cancelled"]
ConfluencePatchOperationType = Literal["append_after_heading", "append_to_page"]
ConfluencePatchMarkerKind = Literal["remove", "correct", "confirm", "insert"]

# ── Clarification Dialog types ──
ClarificationPurpose = Literal[
    "truth_clarification",
    "rating_explanation",
    "action_routing",
]
ClarificationThreadStatus = Literal["active", "resolved", "dismissed"]
ClarificationMessageRole = Literal["system", "assistant", "user"]
ClarificationMessageType = Literal[
    "question",
    "answer",
    "resolution",
    "explanation",
    "truth_confirmation",
    "conflict_resolution",
]
ClarificationOutcomeType = Literal[
    "truth_confirmed",
    "truth_superseded",
    "indication_captured",
    "context_only",
    "conflict_kept",
]
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


def new_schema_truth_id() -> str:
    return f"schema_truth_{uuid4().hex}"


def new_atomic_fact_id() -> str:
    return f"atomic_fact_{uuid4().hex}"


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


def new_clarification_thread_id() -> str:
    return f"clarify_{uuid4().hex}"


def new_clarification_message_id() -> str:
    return f"clmsg_{uuid4().hex}"


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


def _claim_authority_from_source(*, source_type: ClaimSourceKind, metadata: dict[str, object], truth_source_kind: TruthSourceKind | None = None) -> ClaimSourceAuthority:
    if truth_source_kind in {"user_specification", "user_acceptance"}:
        return "explicit_truth"
    governance_level = str(metadata.get("source_governance_level") or "").strip().casefold()
    temporal_status = str(metadata.get("source_temporal_status") or "").strip().casefold()
    if source_type == "metamodel" or governance_level == "ssot":
        return "ssot"
    if governance_level == "governed":
        return "governed"
    if source_type == "github_file":
        if "observed_only" in str(metadata.get("schema_validation_status") or "").strip().casefold():
            return "runtime_observation"
        return "implementation"
    if temporal_status == "historical" or governance_level == "historical":
        return "historical"
    if source_type in {"confluence_page", "local_doc"}:
        return "working_doc"
    return "heuristic"


def _claim_assertion_status_from_metadata(*, metadata: dict[str, object]) -> ClaimAssertionStatus:
    raw = str(metadata.get("assertion_status") or "").strip().casefold()
    if raw in {"excluded", "deprecated", "not_ssot", "secondary_only"}:
        return raw  # type: ignore[return-value]
    return "asserted"


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
    operator: str | None = None
    constraint: str | None = None
    focus_value: str | None = None
    assertion_status: ClaimAssertionStatus = "asserted"
    source_authority: ClaimSourceAuthority = "heuristic"
    evidence_location_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)

    @model_validator(mode="after")
    def hydrate_structured_fields(self) -> "AuditClaimEntry":
        metadata = dict(self.metadata)
        if self.operator is None:
            self.operator = str(metadata.get("claim_operator") or "").strip() or None
        if self.constraint is None:
            self.constraint = str(metadata.get("claim_constraint") or "").strip() or None
        if self.focus_value is None:
            self.focus_value = str(metadata.get("claim_focus_value") or "").strip() or None
        if self.assertion_status == "asserted":
            self.assertion_status = _claim_assertion_status_from_metadata(metadata=metadata)
        if self.source_authority == "heuristic":
            self.source_authority = _claim_authority_from_source(
                source_type=self.source_type,
                metadata=metadata,
            )
        return self


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
    source_authority: ClaimSourceAuthority = "explicit_truth"
    metadata: dict[str, object] = Field(default_factory=dict)

    @model_validator(mode="after")
    def hydrate_truth_authority(self) -> "TruthLedgerEntry":
        if self.source_authority != "explicit_truth":
            return self
        self.source_authority = _claim_authority_from_source(
            source_type="user_truth",
            metadata=self.metadata,
            truth_source_kind=self.source_kind,
        )
        return self


class SchemaTruthEntry(BaseModel):
    schema_truth_id: str = Field(default_factory=new_schema_truth_id)
    schema_key: str = Field(min_length=1)
    schema_kind: Literal["node", "relationship", "property", "unknown"] = "unknown"
    target_label: str = Field(min_length=1)
    status: SchemaTruthStatus
    source_kind: SchemaTruthSourceKind
    source_authority: ClaimSourceAuthority = "heuristic"
    source_ids: list[str] = Field(default_factory=list)
    evidence_claim_ids: list[str] = Field(default_factory=list)
    related_truth_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


class AtomicFactEntry(BaseModel):
    atomic_fact_id: str = Field(default_factory=new_atomic_fact_id)
    fact_key: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    status: AtomicFactStatus = "open"
    action_lane: AtomicFactActionLane
    primary_package_id: str | None = None
    primary_problem_id: str | None = None
    related_package_ids: list[str] = Field(default_factory=list)
    related_problem_ids: list[str] = Field(default_factory=list)
    related_finding_ids: list[str] = Field(default_factory=list)
    source_types: list[str] = Field(default_factory=list)
    source_ids: list[str] = Field(default_factory=list)
    subject_keys: list[str] = Field(default_factory=list)
    predicates: list[str] = Field(default_factory=list)
    claim_ids: list[str] = Field(default_factory=list)
    truth_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_lists(self) -> "AtomicFactEntry":
        self.related_package_ids = _normalized_string_list(self.related_package_ids)
        self.related_problem_ids = _normalized_string_list(self.related_problem_ids)
        self.related_finding_ids = _normalized_string_list(self.related_finding_ids)
        self.source_types = _normalized_string_list(self.source_types)
        self.source_ids = _normalized_string_list(self.source_ids)
        self.subject_keys = _normalized_string_list(self.subject_keys)
        self.predicates = _normalized_string_list(self.predicates)
        self.claim_ids = _normalized_string_list(self.claim_ids)
        self.truth_ids = _normalized_string_list(self.truth_ids)
        return self


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


# ── Clarification Dialog models ──

class ClarificationMessage(BaseModel):
    message_id: str = Field(default_factory=new_clarification_message_id)
    role: ClarificationMessageRole
    message_type: ClarificationMessageType
    content: str = Field(min_length=1)
    created_at: str = Field(default_factory=utc_now_iso)
    referenced_claim_ids: list[str] = Field(default_factory=list)
    referenced_truth_ids: list[str] = Field(default_factory=list)
    referenced_finding_ids: list[str] = Field(default_factory=list)
    outcome_type: ClarificationOutcomeType | None = None
    created_truth_id: str | None = None
    created_claim_id: str | None = None
    superseded_truth_id: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class ClarificationThread(BaseModel):
    thread_id: str = Field(default_factory=new_clarification_thread_id)
    run_id: str = Field(min_length=1)
    package_id: str | None = None
    atomic_fact_id: str | None = None
    purpose: ClarificationPurpose
    status: ClarificationThreadStatus = "active"
    messages: list[ClarificationMessage] = Field(default_factory=list)
    created_at: str = Field(default_factory=utc_now_iso)
    resolved_at: str | None = None
    resolution_summary: str | None = None
    created_truth_ids: list[str] = Field(default_factory=list)
    created_claim_ids: list[str] = Field(default_factory=list)
    superseded_truth_ids: list[str] = Field(default_factory=list)
    triggered_delta_recompute: bool = False
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
    schema_truths: list[SchemaTruthEntry] = Field(default_factory=list)
    atomic_facts: list[AtomicFactEntry] = Field(default_factory=list)
    decision_packages: list[DecisionPackage] = Field(default_factory=list)
    decision_records: list[DecisionRecord] = Field(default_factory=list)
    approval_requests: list[WritebackApprovalRequest] = Field(default_factory=list)
    implemented_changes: list[AuditImplementedChange] = Field(default_factory=list)
    source_snapshots: list[AuditSourceSnapshot] = Field(default_factory=list)
    semantic_entities: list[SemanticEntity] = Field(default_factory=list)
    semantic_relations: list[SemanticRelation] = Field(default_factory=list)
    clarification_threads: list[ClarificationThread] = Field(default_factory=list)
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


class UpdateAtomicFactStatusRequest(BaseModel):
    status: AtomicFactStatus
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


# ── Clarification Dialog request models ──

class CreateClarificationThreadRequest(BaseModel):
    package_id: str | None = None
    atomic_fact_id: str | None = None
    purpose: ClarificationPurpose
    initial_content: str | None = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def exactly_one_anchor(self) -> "CreateClarificationThreadRequest":
        if not self.package_id and not self.atomic_fact_id:
            raise ValueError("Entweder package_id oder atomic_fact_id muss gesetzt sein.")
        if self.package_id and self.atomic_fact_id:
            raise ValueError("Nur package_id ODER atomic_fact_id, nicht beides.")
        return self


class SendClarificationMessageRequest(BaseModel):
    content: str = Field(min_length=1, max_length=2000)


class ConfirmTruthFromClarificationRequest(BaseModel):
    """Doppelte Bestätigung: 100% sicher, gilt ausnahmslos."""
    truth_canonical_key: str = Field(min_length=1)
    truth_normalized_value: str = Field(min_length=1)
    subject_kind: str = Field(min_length=1)
    subject_key: str = Field(min_length=1)
    predicate: str = Field(min_length=1)
    scope_kind: str = Field(default="global")
    scope_key: str = Field(default="*")
    confirmed_absolute: bool = Field(default=True)


class SupersedeTruthFromClarificationRequest(BaseModel):
    """Bestehende Wahrheit durch neue ersetzen."""
    existing_truth_id: str = Field(min_length=1)
    new_canonical_key: str = Field(min_length=1)
    new_normalized_value: str = Field(min_length=1)
    new_subject_kind: str = Field(min_length=1)
    new_subject_key: str = Field(min_length=1)
    new_predicate: str = Field(min_length=1)
    new_scope_kind: str = Field(default="global")
    new_scope_key: str = Field(default="*")


class AuditRunListResponse(BaseModel):
    items: list[AuditRun]
