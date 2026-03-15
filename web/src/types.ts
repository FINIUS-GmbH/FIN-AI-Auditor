export type AuditTarget = {
  github_repo_url?: string | null;
  local_repo_path?: string | null;
  github_ref: string;
  confluence_space_keys: string[];
  confluence_page_ids: string[];
  jira_project_keys: string[];
  include_metamodel: boolean;
  include_local_docs: boolean;
};

export type SourceProfile = {
  confluence_url: string;
  jira_url: string;
  confluence_space_key: string;
  jira_project_key: string;
  jira_usage: "ticket_creation_only";
  metamodel_dump_path: string;
  metamodel_policy: string;
  metamodel_source?: string;
  resource_access_mode: string;
};

export type BootstrapDefaults = {
  github_repo_url: string;
  local_repo_path: string;
  github_ref: string;
  confluence_space_keys: string[];
  confluence_page_ids: string[];
  jira_project_keys: string[];
  include_metamodel: boolean;
  include_local_docs: boolean;
};

export type BootstrapCapabilities = {
  local_repo_enabled: boolean;
  fixed_atlassian_sources: boolean;
  metamodel_always_included: boolean;
  jira_analysis_enabled: boolean;
  jira_ticket_creation_enabled: boolean;
  external_read_only_until_user_decision: boolean;
  atlassian_configured: boolean;
  atlassian_oauth_ready: boolean;
  confluence_live_read_ready: boolean;
  jira_write_scope_ready: boolean;
  llm_configured: boolean;
  llm_slot_count: number;
};

export type OperationalReadinessItem = {
  ready: boolean;
  required_scopes?: string[];
  granted_scopes: string[];
  configured_scopes?: string[];
  notes: string[];
};

export type AtlassianAuthStatus = {
  enabled: boolean;
  client_configured: boolean;
  token_present: boolean;
  token_valid: boolean;
  needs_user_consent: boolean;
  redirect_uri?: string | null;
  configured_redirect_uri?: string | null;
  recommended_redirect_uri?: string | null;
  redirect_uri_matches_local_api: boolean;
  scope?: string | null;
  token_expires_at?: string | null;
  last_error?: string | null;
  notes: string[];
};

export type AtlassianAuthorizationStart = {
  authorization_url: string;
  state_id: string;
  redirect_uri: string;
  notes: string[];
};

export type ConfluenceVerificationResponse = {
  ok: boolean;
  space_key: string;
  page_count: number;
  page_titles: string[];
  analysis_notes: string[];
};

export type ResourceAccessPolicy = {
  mode: string;
  external_write_requires_user_decision: boolean;
  local_database_is_only_writable_store: boolean;
  summary: string;
};

export type BootstrapData = {
  app_name: string;
  defaults: BootstrapDefaults;
  source_profile: SourceProfile;
  resource_access_policy: ResourceAccessPolicy;
  capabilities: BootstrapCapabilities;
  operational_readiness: {
    atlassian_oauth: OperationalReadinessItem;
    confluence_live_read: OperationalReadinessItem;
    jira_writeback: OperationalReadinessItem;
  };
  atlassian_auth: AtlassianAuthStatus;
};

export type AuditPosition = {
  anchor_kind: string;
  anchor_value: string;
  section_path?: string | null;
  line_start?: number | null;
  line_end?: number | null;
  char_start?: number | null;
  char_end?: number | null;
  snippet_hash?: string | null;
  content_hash?: string | null;
};

export type AuditLocation = {
  location_id?: string;
  snapshot_id?: string | null;
  source_type: "github_file" | "confluence_page" | "jira_ticket" | "metamodel" | "local_doc";
  source_id: string;
  title: string;
  path_hint?: string | null;
  url?: string | null;
  position?: AuditPosition | null;
  metadata?: Record<string, unknown>;
};

export type AuditSourceSnapshot = {
  snapshot_id: string;
  source_type: "github_file" | "confluence_page" | "jira_ticket" | "metamodel" | "local_doc";
  source_id: string;
  revision_id?: string | null;
  content_hash?: string | null;
  sync_token?: string | null;
  parent_snapshot_id?: string | null;
  collected_at: string;
  metadata?: Record<string, unknown>;
};

export type AuditFindingLink = {
  link_id: string;
  from_finding_id: string;
  to_finding_id: string;
  relation_type: "contradicts" | "supports" | "duplicates" | "depends_on" | "gap_hint" | "resolved_by";
  rationale: string;
  confidence: number;
  metadata?: Record<string, unknown>;
};

export type AuditProgressStep = {
  step_key: string;
  label: string;
  status: "pending" | "running" | "completed" | "failed" | "skipped";
  detail?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
};

export type AuditRunProgress = {
  progress_pct: number;
  phase_key: string;
  phase_label: string;
  current_activity: string;
  steps: AuditProgressStep[];
};

export type AuditAnalysisLogEntry = {
  log_id: string;
  created_at: string;
  level: "info" | "warning" | "error";
  source_type:
    | "system"
    | "pipeline"
    | "decision_comment"
    | "truth_update"
    | "impact_analysis"
    | "recommendation_regeneration";
  title: string;
  message: string;
  related_finding_ids: string[];
  related_scope_keys: string[];
  derived_changes: string[];
  impact_summary: string[];
  metadata?: Record<string, unknown>;
};

export type JiraTicketAICodingBrief = {
  ticket_key: string;
  ticket_url?: string | null;
  title: string;
  problem_description: string;
  reason: string;
  correction_measures: string[];
  target_state: string[];
  acceptance_criteria: string[];
  implications: string[];
  affected_parts: string[];
  evidence: string[];
  implementation_notes: string[];
  validation_steps: string[];
  ai_coding_prompt: string;
};

export type ConfluencePageUpdateDetails = {
  page_title: string;
  page_url: string;
  changed_sections: string[];
  change_summary: string[];
  page_id?: string | null;
  applied_revision_id?: string | null;
  execution_mode?: string | null;
  patch_preview?: ConfluencePatchPreview | null;
};

export type ConfluencePatchOperation = {
  operation_id: string;
  related_finding_id?: string | null;
  action_type: "append_after_heading" | "append_to_page";
  marker_kind: "remove" | "correct" | "confirm" | "insert";
  section_path: string;
  anchor_heading: string;
  current_statement?: string | null;
  proposed_statement: string;
  rationale: string;
  storage_snippet: string;
};

export type ConfluencePatchPreview = {
  page_id?: string | null;
  page_title: string;
  page_url: string;
  space_key?: string | null;
  base_revision_id?: string | null;
  execution_ready: boolean;
  blockers: string[];
  changed_sections: string[];
  change_summary: string[];
  review_storage_snippets: string[];
  operations: ConfluencePatchOperation[];
};

export type AuditImplementedChange = {
  change_id: string;
  created_at: string;
  status: "applied" | "failed";
  change_type: "confluence_page_updated" | "jira_ticket_created";
  title: string;
  summary: string;
  target_label: string;
  target_url?: string | null;
  related_finding_ids: string[];
  implications: string[];
  metadata?: Record<string, unknown>;
  jira_ticket?: JiraTicketAICodingBrief | null;
  confluence_update?: ConfluencePageUpdateDetails | null;
};

export type WritebackApprovalMetadata = {
  jira_ticket_brief?: JiraTicketAICodingBrief;
  jira_issue_payload?: Record<string, unknown>;
  confluence_patch_preview?: ConfluencePatchPreview;
  [key: string]: unknown;
};

export type AuditClaimEntry = {
  claim_id: string;
  source_snapshot_id?: string | null;
  source_type: "github_file" | "confluence_page" | "jira_ticket" | "metamodel" | "local_doc" | "user_truth";
  source_id: string;
  subject_kind: string;
  subject_key: string;
  predicate: string;
  normalized_value: string;
  scope_kind: string;
  scope_key: string;
  confidence: number;
  fingerprint: string;
  status: "active" | "superseded" | "rejected";
  evidence_location_ids: string[];
  metadata?: Record<string, unknown>;
};

export type TruthLedgerEntry = {
  truth_id: string;
  canonical_key: string;
  subject_kind: string;
  subject_key: string;
  predicate: string;
  normalized_value: string;
  scope_kind: string;
  scope_key: string;
  truth_status: "active" | "superseded" | "rejected";
  source_kind: "user_specification" | "user_acceptance" | "system_inference";
  created_from_problem_id?: string | null;
  supersedes_truth_id?: string | null;
  valid_from_snapshot_id?: string | null;
  metadata?: Record<string, unknown>;
};

export type DecisionProblemElement = {
  problem_id: string;
  finding_id?: string | null;
  category: AuditFinding["category"];
  severity: AuditFinding["severity"];
  scope_summary: string;
  short_explanation: string;
  recommendation: string;
  confidence: number;
  affected_claim_ids: string[];
  affected_truth_ids: string[];
  evidence_locations: AuditLocation[];
  metadata?: Record<string, unknown>;
};

export type DecisionPackage = {
  package_id: string;
  title: string;
  category: AuditFinding["category"];
  severity_summary: AuditFinding["severity"];
  scope_summary: string;
  decision_state: "open" | "accepted" | "rejected" | "specified" | "superseded";
  decision_required: boolean;
  rerender_required_after_decision: boolean;
  recommendation_summary: string;
  related_finding_ids: string[];
  problem_elements: DecisionProblemElement[];
  metadata?: Record<string, unknown>;
};

export type DecisionRecord = {
  decision_id: string;
  package_id: string;
  action: "accept" | "reject" | "specify";
  created_at: string;
  comment_text?: string | null;
  created_truth_ids: string[];
  impacted_package_ids: string[];
  metadata?: Record<string, unknown>;
};

export type WritebackApprovalRequest = {
  approval_request_id: string;
  created_at: string;
  target_type: "confluence_page_update" | "jira_ticket_create";
  status: "pending" | "approved" | "rejected" | "executed" | "cancelled";
  title: string;
  summary: string;
  target_url?: string | null;
  related_package_ids: string[];
  related_finding_ids: string[];
  payload_preview: string[];
  decided_at?: string | null;
  decision_comment?: string | null;
  metadata?: WritebackApprovalMetadata;
};

export type AuditFinding = {
  finding_id: string;
  severity: "critical" | "high" | "medium" | "low";
  category:
    | "contradiction"
    | "clarification_needed"
    | "missing_definition"
    | "missing_documentation"
    | "stale_source"
    | "implementation_drift"
    | "read_write_gap"
    | "traceability_gap"
    | "ownership_gap"
    | "policy_conflict"
    | "terminology_collision"
    | "low_confidence_review"
    | "obsolete_documentation"
    | "open_decision";
  title: string;
  summary: string;
  recommendation: string;
  canonical_key?: string | null;
  resolution_state?: "open" | "accepted" | "dismissed" | "superseded";
  locations: AuditLocation[];
  proposed_confluence_action?: string | null;
  proposed_jira_action?: string | null;
  metadata?: Record<string, unknown>;
};

export type LlmUsageByModel = {
  calls: number;
  prompt_tokens: number;
  completion_tokens: number;
  cost_usd: number;
  cost_eur: number;
};

export type LlmUsage = {
  by_model?: Record<string, LlmUsageByModel>;
  total_prompt_tokens?: number;
  total_completion_tokens?: number;
  total_cost_usd?: number;
  total_cost_eur?: number;
};

export type AuditRun = {
  run_id: string;
  status: "planned" | "running" | "completed" | "failed";
  target: AuditTarget;
  created_at: string;
  updated_at: string;
  started_at?: string | null;
  finished_at?: string | null;
  summary?: string | null;
  progress: AuditRunProgress;
  analysis_log: AuditAnalysisLogEntry[];
  claims: AuditClaimEntry[];
  truths: TruthLedgerEntry[];
  decision_packages: DecisionPackage[];
  decision_records: DecisionRecord[];
  approval_requests: WritebackApprovalRequest[];
  implemented_changes: AuditImplementedChange[];
  source_snapshots: AuditSourceSnapshot[];
  findings: AuditFinding[];
  finding_links: AuditFindingLink[];
  llm_usage?: LlmUsage;
  error?: string | null;
};
