import { useEffect, useMemo, useState, type FormEvent } from "react";

import type {
  AuditAnalysisLogEntry,
  AuditClaimEntry,
  AuditFinding,
  AuditImplementedChange,
  AuditLocation,
  AuditRun,
  AuditSourceSnapshot,
  ConfluencePatchPreview,
  DecisionPackage,
  ResourceAccessPolicy,
  SourceProfile,
  TruthLedgerEntry,
  WritebackApprovalRequest,
} from "../types";

type FindingListProps = {
  run: AuditRun | null;
  sourceProfile: SourceProfile;
  accessPolicy: ResourceAccessPolicy;
  decisionCommentSubmitting: boolean;
  decisionCommentError: string;
  packageDecisionSubmitting: string;
  packageDecisionError: string;
  approvalSubmitting: string;
  approvalError: string;
  executionSubmitting: string;
  executionError: string;
  onDecisionCommentSubmit: (commentText: string) => Promise<void>;
  onPackageDecisionSubmit: (
    packageId: string,
    action: "accept" | "reject" | "specify",
    commentText?: string,
  ) => Promise<void>;
  onCreateApprovalRequest: (payload: {
    target_type: "confluence_page_update" | "jira_ticket_create";
    title: string;
    summary: string;
    target_url?: string | null;
    related_package_ids: string[];
    related_finding_ids: string[];
    payload_preview: string[];
  }) => Promise<void>;
  onResolveApprovalRequest: (
    approvalRequestId: string,
    decision: "approve" | "reject" | "cancel",
    commentText?: string,
  ) => Promise<void>;
  onRecordConfluencePageUpdate: (payload: {
    approval_request_id: string;
    page_title: string;
    page_url: string;
    changed_sections: string[];
    change_summary: string[];
    related_finding_ids: string[];
  }) => Promise<void>;
  onRecordJiraTicketCreated: (payload: {
    approval_request_id: string;
    ticket_key: string;
    ticket_url: string;
    related_finding_ids: string[];
  }) => Promise<void>;
  onExecuteConfluencePageWriteback: (approvalRequestId: string) => Promise<void>;
  onExecuteJiraTicketWriteback: (approvalRequestId: string) => Promise<void>;
};

function formatTimestamp(value?: string | null): string {
  if (!value) {
    return "noch offen";
  }

  return new Intl.DateTimeFormat("de-DE", {
    dateStyle: "short",
    timeStyle: "short",
  }).format(new Date(value));
}

function formatLocation(location: AuditLocation): string {
  const fragments = [location.title];
  if (location.path_hint) {
    fragments.push(location.path_hint);
  }
  if (location.position?.anchor_value) {
    fragments.push(location.position.anchor_value);
  }
  return fragments.join(" · ");
}

function snapshotSubtitle(snapshot: AuditSourceSnapshot): string {
  const fragments: string[] = [];
  if (snapshot.revision_id) {
    fragments.push(`Revision ${snapshot.revision_id}`);
  }
  if (snapshot.content_hash) {
    fragments.push(snapshot.content_hash);
  }
  return fragments.join(" · ") || "Kein Delta-Marker vorhanden";
}

function progressWidth(progressPct: number): string {
  return `${Math.max(0, Math.min(100, progressPct))}%`;
}

function analysisSourceLabel(entry: AuditAnalysisLogEntry): string {
  switch (entry.source_type) {
    case "system":
      return "System";
    case "pipeline":
      return "Pipeline";
    case "decision_comment":
      return "Kommentar";
    case "truth_update":
      return "Wahrheit";
    case "impact_analysis":
      return "Impact";
    case "recommendation_regeneration":
      return "Neugewichtung";
    default:
      return entry.source_type;
  }
}

function implementedChangeTypeLabel(change: AuditImplementedChange): string {
  switch (change.change_type) {
    case "confluence_page_updated":
      return "Confluence";
    case "jira_ticket_created":
      return "Jira";
    default:
      return change.change_type;
  }
}

function approvalTypeLabel(request: WritebackApprovalRequest): string {
  switch (request.target_type) {
    case "confluence_page_update":
      return "Confluence";
    case "jira_ticket_create":
      return "Jira";
    default:
      return request.target_type;
  }
}

function findingTitle(finding: AuditFinding): string {
  if (!finding.canonical_key) {
    return finding.title;
  }
  return `${finding.title} · ${finding.canonical_key}`;
}

function claimSummary(claim: AuditClaimEntry): string {
  return `${claim.subject_key} · ${claim.predicate} · ${claim.normalized_value}`;
}

function truthSummary(truth: TruthLedgerEntry): string {
  return `${truth.canonical_key} · ${truth.normalized_value}`;
}

function retrievalContextEntries(finding: AuditFinding): string[] {
  const raw = finding.metadata?.retrieval_context;
  return Array.isArray(raw) ? raw.filter((item): item is string => typeof item === "string" && item.trim().length > 0) : [];
}

function metadataStringEntries(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === "string" && item.trim().length > 0)
    : [];
}

function confluencePatchPreview(request: WritebackApprovalRequest): ConfluencePatchPreview | null {
  const raw = request.metadata?.confluence_patch_preview;
  if (!raw || typeof raw !== "object") {
    return null;
  }
  return raw as ConfluencePatchPreview;
}

export function FindingList(props: FindingListProps): JSX.Element {
  const {
    run,
    sourceProfile,
    accessPolicy,
    decisionCommentSubmitting,
    decisionCommentError,
    packageDecisionSubmitting,
    packageDecisionError,
    approvalSubmitting,
    approvalError,
    executionSubmitting,
    executionError,
    onDecisionCommentSubmit,
    onPackageDecisionSubmit,
    onCreateApprovalRequest,
    onResolveApprovalRequest,
    onRecordConfluencePageUpdate,
    onRecordJiraTicketCreated,
    onExecuteConfluencePageWriteback,
    onExecuteJiraTicketWriteback,
  } = props;

  const [decisionCommentDraft, setDecisionCommentDraft] = useState("");
  const [packageCommentDrafts, setPackageCommentDrafts] = useState<Record<string, string>>({});
  const [approvalCommentDrafts, setApprovalCommentDrafts] = useState<Record<string, string>>({});
  const [confluenceExecutionDrafts, setConfluenceExecutionDrafts] = useState<
    Record<string, { pageTitle: string; pageUrl: string; changedSections: string; changeSummary: string }>
  >({});
  const [jiraExecutionDrafts, setJiraExecutionDrafts] = useState<
    Record<string, { ticketKey: string; ticketUrl: string }>
  >({});

  useEffect(() => {
    setDecisionCommentDraft("");
    setPackageCommentDrafts({});
    setApprovalCommentDrafts({});
    setConfluenceExecutionDrafts({});
    setJiraExecutionDrafts({});
  }, [run?.run_id]);

  const logEntries = useMemo(() => [...(run?.analysis_log ?? [])].reverse(), [run?.analysis_log]);
  const implementedChanges = useMemo(
    () => [...(run?.implemented_changes ?? [])].reverse(),
    [run?.implemented_changes],
  );
  const approvalRequests = useMemo(
    () => [...(run?.approval_requests ?? [])].sort((left, right) => right.created_at.localeCompare(left.created_at)),
    [run?.approval_requests],
  );
  const activeTruths = useMemo(
    () => (run?.truths ?? []).filter((truth) => truth.truth_status === "active"),
    [run?.truths],
  );
  const decisionCommentCount = useMemo(
    () => run?.analysis_log.filter((entry) => entry.source_type === "decision_comment").length ?? 0,
    [run?.analysis_log],
  );
  const packageGroups = useMemo(() => {
    const groups = new Map<string, DecisionPackage[]>();
    for (const packageItem of run?.decision_packages ?? []) {
      const current = groups.get(packageItem.category) ?? [];
      current.push(packageItem);
      groups.set(packageItem.category, current);
    }
    return [...groups.entries()];
  }, [run?.decision_packages]);

  async function handleDecisionCommentSubmit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    const nextComment = decisionCommentDraft.trim();
    if (!nextComment) {
      return;
    }
    await onDecisionCommentSubmit(nextComment);
    setDecisionCommentDraft("");
  }

  async function handlePackageDecision(
    packageId: string,
    action: "accept" | "reject" | "specify",
  ): Promise<void> {
    const commentText = packageCommentDrafts[packageId]?.trim() || undefined;
    await onPackageDecisionSubmit(packageId, action, commentText);
    if (action !== "reject") {
      setPackageCommentDrafts((current) => ({ ...current, [packageId]: "" }));
    }
  }

  async function handleCreateApproval(
    packageItem: DecisionPackage,
    targetType: "confluence_page_update" | "jira_ticket_create",
  ): Promise<void> {
    const title =
      targetType === "confluence_page_update"
        ? `Confluence-Writeback fuer ${packageItem.title}`
        : `Jira-Writeback fuer ${packageItem.title}`;
    const summary =
      targetType === "confluence_page_update"
        ? "Lokale Freigabeanfrage fuer ein spaeteres Confluence-Update."
        : "Lokale Freigabeanfrage fuer die spaetere Erstellung eines Jira-Tickets.";
    const payloadPreview =
      targetType === "confluence_page_update"
        ? [
            `Scope: ${packageItem.scope_summary}`,
            `Empfohlene Korrektur: ${packageItem.recommendation_summary}`,
            "Nach Freigabe darf ein Confluence-Patch lokal als writeback-bereit markiert werden.",
          ]
        : [
            `Scope: ${packageItem.scope_summary}`,
            `Empfohlene Korrektur: ${packageItem.recommendation_summary}`,
            "Nach Freigabe darf ein Jira-Ticket mit AI-Coding-Brief erzeugt werden.",
          ];
    await onCreateApprovalRequest({
      target_type: targetType,
      title,
      summary,
      target_url:
        targetType === "confluence_page_update" ? sourceProfile.confluence_url : sourceProfile.jira_url,
      related_package_ids: [packageItem.package_id],
      related_finding_ids: packageItem.related_finding_ids,
      payload_preview: payloadPreview,
    });
  }

  function getConfluenceExecutionDraft(request: WritebackApprovalRequest): {
    pageTitle: string;
    pageUrl: string;
    changedSections: string;
    changeSummary: string;
  } {
    const patchPreview = confluencePatchPreview(request);
    return (
      confluenceExecutionDrafts[request.approval_request_id] ?? {
        pageTitle: patchPreview?.page_title || request.title.replace("Confluence-Writeback fuer ", "") || "FIN-AI Spezifikation",
        pageUrl: patchPreview?.page_url || request.target_url || sourceProfile.confluence_url,
        changedSections: (patchPreview?.changed_sections ?? request.related_package_ids).join("\n"),
        changeSummary: (patchPreview?.change_summary ?? request.payload_preview).join("\n"),
      }
    );
  }

  function getJiraExecutionDraft(request: WritebackApprovalRequest): { ticketKey: string; ticketUrl: string } {
    return (
      jiraExecutionDrafts[request.approval_request_id] ?? {
        ticketKey: "",
        ticketUrl: request.target_url || sourceProfile.jira_url,
      }
    );
  }

  async function handleRecordApprovedConfluence(request: WritebackApprovalRequest): Promise<void> {
    const draft = getConfluenceExecutionDraft(request);
    await onRecordConfluencePageUpdate({
      approval_request_id: request.approval_request_id,
      page_title: draft.pageTitle.trim(),
      page_url: draft.pageUrl.trim(),
      changed_sections: draft.changedSections
        .split("\n")
        .map((item) => item.trim())
        .filter(Boolean),
      change_summary: draft.changeSummary
        .split("\n")
        .map((item) => item.trim())
        .filter(Boolean),
      related_finding_ids: request.related_finding_ids,
    });
  }

  async function handleRecordApprovedJira(request: WritebackApprovalRequest): Promise<void> {
    const draft = getJiraExecutionDraft(request);
    await onRecordJiraTicketCreated({
      approval_request_id: request.approval_request_id,
      ticket_key: draft.ticketKey.trim(),
      ticket_url: draft.ticketUrl.trim(),
      related_finding_ids: request.related_finding_ids,
    });
  }

  if (!run) {
    return (
      <section className="detail-columns">
        <section className="panel findings-panel empty-panel">
          <h2>Arbeitsflaeche</h2>
          <p>
            Links einen Run auswaehlen oder neu anlegen. Hier erscheinen danach Entscheidungspakete, Evidenz und
            Delta-Informationen.
          </p>
        </section>

        <aside className="panel ledger-panel">
          <h3>Analysequellen und Ticket-Ziel</h3>
          <div className="policy-inline-box">
            <strong>Read-only bis Freigabe</strong>
            <p>{accessPolicy.summary}</p>
          </div>
          <div className="source-ledger">
            <a className="ledger-card" href={sourceProfile.confluence_url} target="_blank" rel="noreferrer">
              <span>Analysequelle</span>
              <strong>{sourceProfile.confluence_space_key}</strong>
              <small>{sourceProfile.confluence_url}</small>
            </a>
            <div className="ledger-card">
              <span>Analysequelle</span>
              <strong>Current Dump</strong>
              <small>{sourceProfile.metamodel_dump_path}</small>
            </div>
            <a className="ledger-card" href={sourceProfile.jira_url} target="_blank" rel="noreferrer">
              <span>Ticket-Ziel</span>
              <strong>{sourceProfile.jira_project_key}</strong>
              <small>{sourceProfile.jira_url}</small>
            </a>
          </div>
        </aside>
      </section>
    );
  }

  return (
    <section className="detail-columns">
      <section className="panel findings-panel">
        <div className="panel-heading compact-heading">
          <div>
            <h2>Entscheidungspakete und Evidenz</h2>
            <p>{run.summary || "Noch keine Abschlusszusammenfassung vorhanden."}</p>
          </div>
          <span className={`status-pill status-${run.status}`}>{run.status}</span>
        </div>

        <section className="progress-panel">
          <div className="progress-panel-header">
            <div>
              <span className="detail-label">Analysefortschritt</span>
              <strong>{run.progress.phase_label}</strong>
              <p>{run.progress.current_activity}</p>
            </div>
            <div className="progress-panel-value">{run.progress.progress_pct}%</div>
          </div>
          <div className="progress-bar" aria-hidden="true">
            <div className="progress-bar-fill" style={{ width: progressWidth(run.progress.progress_pct) }} />
          </div>
          <div className="progress-steps">
            {run.progress.steps.map((step) => (
              <article key={step.step_key} className={`progress-step progress-step-${step.status}`}>
                <div className="progress-step-header">
                  <strong>{step.label}</strong>
                  <span>{step.status}</span>
                </div>
                <p>{step.detail || "Noch keine Detailaktivitaet vorhanden."}</p>
              </article>
            ))}
          </div>
        </section>

        <div className="overview-grid">
          <div className="overview-card">
            <span className="detail-label">Pakete</span>
            <strong>{run.decision_packages.length}</strong>
            <small>Atomare UI-Einheiten fuer die Bearbeitung</small>
          </div>
          <div className="overview-card">
            <span className="detail-label">Truths</span>
            <strong>{activeTruths.length}</strong>
            <small>Aktive kanonische Wahrheiten im Ledger</small>
          </div>
          <div className="overview-card">
            <span className="detail-label">Claims</span>
            <strong>{run.claims.length}</strong>
              <small>Lokaler Claim-Index aus Code, Doku und Metamodell</small>
          </div>
          <div className="overview-card">
            <span className="detail-label">Freigaben</span>
            <strong>{run.approval_requests.length}</strong>
            <small>Explizite Approval-Schritte fuer spaetere Writebacks</small>
          </div>
        </div>

        {packageDecisionError ? <div className="error-box">{packageDecisionError}</div> : null}
        <section className="package-stack">
          {run.decision_packages.length === 0 ? (
            <div className="finding-empty">
              <strong>Noch keine Entscheidungspakete vorhanden.</strong>
              <p>Nach der Analyse werden hier atomare Pakete statt einer flachen Finding-Liste aufgebaut.</p>
            </div>
          ) : (
            packageGroups.map(([category, packageItems]) => (
              <div className="package-group" key={category}>
                <div className="section-subhead">
                  <h3>{category}</h3>
                  <p>{packageItems.length} Entscheidungspaket(e) in dieser Bewertungskategorie.</p>
                </div>
                {packageItems.map((packageItem) => (
                  <article className="finding-card package-card" key={packageItem.package_id}>
                    <div className="finding-header">
                      <div className="finding-badges">
                        <span className={`severity severity-${packageItem.severity_summary}`}>
                          {packageItem.severity_summary}
                        </span>
                        <span className="finding-category">{packageItem.category}</span>
                        <span className={`resolution-state resolution-${packageItem.decision_state}`}>
                          {packageItem.decision_state}
                        </span>
                      </div>
                    </div>
                    <h3>{packageItem.title}</h3>
                    <p>{packageItem.scope_summary}</p>
                    <div className="finding-block">
                      <strong>Empfehlung</strong>
                      <p>{packageItem.recommendation_summary}</p>
                    </div>
                    {metadataStringEntries(packageItem.metadata?.delta_summary).length > 0 ? (
                      <div className="finding-block">
                        <strong>Delta-Hinweise</strong>
                        <ul className="finding-list">
                          {metadataStringEntries(packageItem.metadata?.delta_summary).map((entry) => (
                            <li key={`${packageItem.package_id}-${entry}`}>{entry}</li>
                          ))}
                        </ul>
                      </div>
                    ) : null}
                    <div className="finding-block">
                      <strong>Problemelemente</strong>
                      <div className="problem-stack">
                        {packageItem.problem_elements.map((problem) => (
                          <article className="problem-card" key={problem.problem_id}>
                            <div className="problem-card-header">
                              <span className={`severity severity-${problem.severity}`}>{problem.severity}</span>
                              <span>{Math.round(problem.confidence * 100)}%</span>
                            </div>
                            <strong>{problem.scope_summary}</strong>
                            <p>{problem.short_explanation}</p>
                            <div className="status-log-tags">
                              {problem.affected_claim_ids.map((claimId) => (
                                <span className="status-log-tag" key={claimId}>
                                  Claim {claimId}
                                </span>
                              ))}
                              {problem.affected_truth_ids.map((truthId) => (
                                <span className="status-log-tag" key={truthId}>
                                  Truth {truthId}
                                </span>
                              ))}
                            </div>
                            {problem.evidence_locations.length > 0 ? (
                              <ul className="finding-list">
                                {problem.evidence_locations.map((location) => (
                                  <li key={location.location_id || `${location.source_id}-${location.title}`}>
                                    {location.url ? (
                                      <a href={location.url} target="_blank" rel="noreferrer">
                                        {formatLocation(location)}
                                      </a>
                                    ) : (
                                      <span>{formatLocation(location)}</span>
                                    )}
                                  </li>
                                ))}
                              </ul>
                            ) : null}
                          </article>
                        ))}
                      </div>
                    </div>

                    <div className="decision-panel">
                      <label>
                        <span>Spezifizierung oder Begruendung</span>
                        <textarea
                          value={packageCommentDrafts[packageItem.package_id] ?? ""}
                          onChange={(event) =>
                            setPackageCommentDrafts((current) => ({
                              ...current,
                              [packageItem.package_id]: event.target.value,
                            }))
                          }
                          placeholder="Optional fuer accept/reject, erforderlich fuer specify."
                          rows={3}
                        />
                      </label>
                      <div className="form-actions">
                        <button
                          type="button"
                          className="secondary-button"
                          disabled={packageDecisionSubmitting === packageItem.package_id}
                          onClick={() => void handlePackageDecision(packageItem.package_id, "accept")}
                        >
                          {packageDecisionSubmitting === packageItem.package_id ? "Speichere..." : "Empfehlung annehmen"}
                        </button>
                        <button
                          type="button"
                          className="secondary-button"
                          disabled={packageDecisionSubmitting === packageItem.package_id}
                          onClick={() => void handlePackageDecision(packageItem.package_id, "reject")}
                        >
                          Ablehnen
                        </button>
                        <button
                          type="button"
                          className="primary-button"
                          disabled={packageDecisionSubmitting === packageItem.package_id}
                          onClick={() => void handlePackageDecision(packageItem.package_id, "specify")}
                        >
                          Spezifizieren
                        </button>
                      </div>
                    </div>

                    <div className="package-approval-row">
                      <button
                        type="button"
                        className="secondary-button"
                        disabled={approvalSubmitting === `create:confluence_page_update`}
                        onClick={() => void handleCreateApproval(packageItem, "confluence_page_update")}
                      >
                        Confluence-Freigabe anfordern
                      </button>
                      <button
                        type="button"
                        className="secondary-button"
                        disabled={approvalSubmitting === `create:jira_ticket_create`}
                        onClick={() => void handleCreateApproval(packageItem, "jira_ticket_create")}
                      >
                        Jira-Freigabe anfordern
                      </button>
                    </div>
                  </article>
                ))}
              </div>
            ))
          )}
        </section>

        <section className="finding-stack">
          <div className="section-subhead">
            <h3>Rohe Findings</h3>
            <p>Nur zur Evidenzkontrolle. Die eigentliche Bearbeitung laeuft ueber Entscheidungspakete.</p>
          </div>
          {run.findings.length === 0 ? (
            <div className="finding-empty">
              <strong>Noch keine Findings vorhanden.</strong>
              <p>Der Worker verarbeitet geplante Runs weiter und hinterlegt danach Findings, Snapshots und Links.</p>
            </div>
          ) : (
            run.findings.map((finding) => (
              <article className="finding-card" key={finding.finding_id}>
                <div className="finding-header">
                  <div className="finding-badges">
                    <span className={`severity severity-${finding.severity}`}>{finding.severity}</span>
                    <span className="finding-category">{finding.category}</span>
                    <span className={`resolution-state resolution-${finding.resolution_state || "open"}`}>
                      {finding.resolution_state || "open"}
                    </span>
                  </div>
                </div>
                <h3>{findingTitle(finding)}</h3>
                <p>{finding.summary}</p>
                <div className="finding-block">
                  <strong>Empfehlung</strong>
                  <p>{finding.recommendation}</p>
                </div>
                {retrievalContextEntries(finding).length > 0 ? (
                  <div className="finding-block">
                    <strong>Retrieval-Kontext</strong>
                    <ul className="finding-list">
                      {retrievalContextEntries(finding).map((entry, index) => (
                        <li key={`${finding.finding_id}-retrieval-${index}`}>{entry}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
                <div className="finding-block">
                  <strong>Originalpositionen</strong>
                  <ul className="finding-list">
                    {finding.locations.map((location) => (
                      <li key={location.location_id || `${location.source_id}-${location.title}`}>
                        {location.url ? (
                          <a href={location.url} target="_blank" rel="noreferrer">
                            {formatLocation(location)}
                          </a>
                        ) : (
                          <span>{formatLocation(location)}</span>
                        )}
                      </li>
                    ))}
                  </ul>
                </div>
              </article>
            ))
          )}
        </section>
      </section>

      <aside className="panel ledger-panel">
        <div className="panel-heading compact-heading">
          <div>
            <h2>Ledger und Freigaben</h2>
            <p>Truths, Claims, Approval-Gates, umgesetzte Aenderungen und Delta-Nachweise des ausgewaehlten Runs.</p>
          </div>
        </div>

        <div className="ledger-section">
          <div className="ledger-section-header">
            <div>
              <h3>Freigabe-Queue</h3>
              <p className="status-text">
                Jeder spaetere externe Writeback braucht zuerst eine explizite lokale Approval-Entscheidung.
              </p>
            </div>
            <span className="log-count-pill">{approvalRequests.length}</span>
          </div>
          {approvalError ? <div className="error-box">{approvalError}</div> : null}
          {executionError ? <div className="error-box">{executionError}</div> : null}
          {approvalRequests.length === 0 ? (
            <div className="status-log-hint">
              <strong>Noch keine Freigaben angefordert.</strong>
              <p>Die Buttons an den Entscheidungspaketen legen lokale Approval-Requests an.</p>
            </div>
          ) : (
            <div className="status-log-list">
              {approvalRequests.map((request) => {
                const patchPreview = confluencePatchPreview(request);
                return (
                <article className="status-log-entry" key={request.approval_request_id}>
                  <div className="status-log-header">
                    <div>
                      <strong>{request.title}</strong>
                      <small>{formatTimestamp(request.created_at)}</small>
                    </div>
                    <div className="status-log-badges">
                      <span className={`status-log-source source-${request.target_type}`}>{approvalTypeLabel(request)}</span>
                      <span className="status-log-level-badge level-info">{request.status}</span>
                    </div>
                  </div>
                  <p>{request.summary}</p>
                  {request.payload_preview.length > 0 ? (
                    <div className="status-log-block">
                      <strong>Payload Preview</strong>
                      <ul className="finding-list">
                        {request.payload_preview.map((item, index) => (
                          <li key={`${request.approval_request_id}-preview-${index}`}>{item}</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}
                  {request.target_type === "confluence_page_update" && patchPreview ? (
                    <div className="status-log-block">
                      <strong>Section-Anchored Patch Preview</strong>
                      <div className="confluence-preview-block">
                        <p>
                          <strong>{patchPreview.page_title}</strong>
                          {" · "}
                          {patchPreview.execution_ready ? "extern ausfuehrbar" : "noch blockiert"}
                        </p>
                        {patchPreview.blockers.length > 0 ? (
                          <ul className="finding-list">
                            {patchPreview.blockers.map((entry) => (
                              <li key={`${request.approval_request_id}-blocker-${entry}`}>{entry}</li>
                            ))}
                          </ul>
                        ) : null}
                        <ul className="finding-list">
                          {patchPreview.operations.slice(0, 6).map((operation) => (
                            <li key={operation.operation_id}>
                              <strong>{operation.section_path}</strong>
                              {" · "}
                              {operation.marker_kind}
                              {" · "}
                              {operation.proposed_statement}
                            </li>
                          ))}
                        </ul>
                      </div>
                    </div>
                  ) : null}
                  {request.status === "pending" ? (
                    <div className="decision-panel">
                      <label>
                        <span>Freigabe-Kommentar</span>
                        <textarea
                          value={approvalCommentDrafts[request.approval_request_id] ?? ""}
                          onChange={(event) =>
                            setApprovalCommentDrafts((current) => ({
                              ...current,
                              [request.approval_request_id]: event.target.value,
                            }))
                          }
                          rows={2}
                          placeholder="Optionaler Kommentar zur Freigabeentscheidung."
                        />
                      </label>
                      <div className="form-actions">
                        <button
                          type="button"
                          className="primary-button"
                          disabled={approvalSubmitting === request.approval_request_id}
                          onClick={() =>
                            void onResolveApprovalRequest(
                              request.approval_request_id,
                              "approve",
                              approvalCommentDrafts[request.approval_request_id],
                            )
                          }
                        >
                          Genehmigen
                        </button>
                        <button
                          type="button"
                          className="secondary-button"
                          disabled={approvalSubmitting === request.approval_request_id}
                          onClick={() =>
                            void onResolveApprovalRequest(
                              request.approval_request_id,
                              "reject",
                              approvalCommentDrafts[request.approval_request_id],
                            )
                          }
                        >
                          Ablehnen
                        </button>
                        <button
                          type="button"
                          className="secondary-button"
                          disabled={approvalSubmitting === request.approval_request_id}
                          onClick={() =>
                            void onResolveApprovalRequest(
                              request.approval_request_id,
                              "cancel",
                              approvalCommentDrafts[request.approval_request_id],
                            )
                          }
                        >
                          Abbrechen
                        </button>
                      </div>
                    </div>
                  ) : null}
                  {request.status === "approved" && request.target_type === "confluence_page_update" ? (
                    <div className="decision-panel execution-panel">
                      {patchPreview?.execution_ready ? (
                        <div className="form-actions">
                          <button
                            type="button"
                            className="primary-button"
                            disabled={executionSubmitting === request.approval_request_id}
                            onClick={() => void onExecuteConfluencePageWriteback(request.approval_request_id)}
                          >
                            {executionSubmitting === request.approval_request_id
                              ? "Fuehre aus..."
                              : "Confluence-Patch extern ausfuehren"}
                          </button>
                        </div>
                      ) : null}
                      <label>
                        <span>Seitentitel</span>
                        <input
                          value={getConfluenceExecutionDraft(request).pageTitle}
                          onChange={(event) =>
                            setConfluenceExecutionDrafts((current) => ({
                              ...current,
                              [request.approval_request_id]: {
                                ...getConfluenceExecutionDraft(request),
                                pageTitle: event.target.value,
                              },
                            }))
                          }
                        />
                      </label>
                      <label>
                        <span>Seiten-URL</span>
                        <input
                          value={getConfluenceExecutionDraft(request).pageUrl}
                          onChange={(event) =>
                            setConfluenceExecutionDrafts((current) => ({
                              ...current,
                              [request.approval_request_id]: {
                                ...getConfluenceExecutionDraft(request),
                                pageUrl: event.target.value,
                              },
                            }))
                          }
                        />
                      </label>
                      <label>
                        <span>Geaenderte Abschnitte</span>
                        <textarea
                          value={getConfluenceExecutionDraft(request).changedSections}
                          onChange={(event) =>
                            setConfluenceExecutionDrafts((current) => ({
                              ...current,
                              [request.approval_request_id]: {
                                ...getConfluenceExecutionDraft(request),
                                changedSections: event.target.value,
                              },
                            }))
                          }
                          rows={3}
                        />
                      </label>
                      <label>
                        <span>Aenderungszusammenfassung</span>
                        <textarea
                          value={getConfluenceExecutionDraft(request).changeSummary}
                          onChange={(event) =>
                            setConfluenceExecutionDrafts((current) => ({
                              ...current,
                              [request.approval_request_id]: {
                                ...getConfluenceExecutionDraft(request),
                                changeSummary: event.target.value,
                              },
                            }))
                          }
                          rows={3}
                        />
                      </label>
                      <div className="form-actions">
                        <button
                          type="button"
                          className="primary-button"
                          disabled={executionSubmitting === request.approval_request_id}
                          onClick={() => void handleRecordApprovedConfluence(request)}
                        >
                          {executionSubmitting === request.approval_request_id
                            ? "Verbucht..."
                            : "Confluence-Update lokal verbuchen"}
                        </button>
                      </div>
                    </div>
                  ) : null}
                  {request.status === "approved" && request.target_type === "jira_ticket_create" ? (
                    <div className="decision-panel execution-panel">
                      <div className="form-actions">
                        <button
                          type="button"
                          className="primary-button"
                          disabled={executionSubmitting === request.approval_request_id}
                          onClick={() => void onExecuteJiraTicketWriteback(request.approval_request_id)}
                        >
                          {executionSubmitting === request.approval_request_id
                            ? "Fuehre aus..."
                            : "Jira-Ticket extern erstellen"}
                        </button>
                      </div>
                      <label>
                        <span>Ticket-Key</span>
                        <input
                          value={getJiraExecutionDraft(request).ticketKey}
                          onChange={(event) =>
                            setJiraExecutionDrafts((current) => ({
                              ...current,
                              [request.approval_request_id]: {
                                ...getJiraExecutionDraft(request),
                                ticketKey: event.target.value,
                              },
                            }))
                          }
                          placeholder="FINAI-123"
                        />
                      </label>
                      <label>
                        <span>Ticket-URL</span>
                        <input
                          value={getJiraExecutionDraft(request).ticketUrl}
                          onChange={(event) =>
                            setJiraExecutionDrafts((current) => ({
                              ...current,
                              [request.approval_request_id]: {
                                ...getJiraExecutionDraft(request),
                                ticketUrl: event.target.value,
                              },
                            }))
                          }
                        />
                      </label>
                      <div className="form-actions">
                        <button
                          type="button"
                          className="primary-button"
                          disabled={executionSubmitting === request.approval_request_id}
                          onClick={() => void handleRecordApprovedJira(request)}
                        >
                          {executionSubmitting === request.approval_request_id
                            ? "Verbucht..."
                            : "Jira-Ticket lokal verbuchen"}
                        </button>
                      </div>
                    </div>
                  ) : null}
                </article>
              )})}
            </div>
          )}
        </div>

        <div className="ledger-section">
          <div className="ledger-section-header">
            <div>
              <h3>Truth-Ledger</h3>
              <p className="status-text">Aktive und ersetzte Wahrheiten, die neue Re-Audits beeinflussen.</p>
            </div>
            <span className="log-count-pill">{run.truths.length}</span>
          </div>
          <div className="ledger-list">
            {run.truths.length === 0 ? (
              <p className="status-text">Noch keine Wahrheiten gespeichert.</p>
            ) : (
              run.truths.map((truth) => (
                <article className="ledger-item" key={truth.truth_id}>
                  <div className="ledger-item-header">
                    <strong>{truth.truth_status}</strong>
                    <span>{truth.source_kind}</span>
                  </div>
                  <p>{truthSummary(truth)}</p>
                  <small>{truth.scope_key}</small>
                </article>
              ))
            )}
          </div>
        </div>

        <div className="ledger-section">
          <div className="ledger-section-header">
            <div>
              <h3>Claim-Index</h3>
              <p className="status-text">Extrahierte Aussagen aus Code, Doku, Metamodell und spaeteren Truths.</p>
            </div>
            <span className="log-count-pill">{run.claims.length}</span>
          </div>
          <div className="ledger-list">
            {run.claims.length === 0 ? (
              <p className="status-text">Noch keine Claims gespeichert.</p>
            ) : (
              run.claims.map((claim) => (
                <article className="ledger-item" key={claim.claim_id}>
                  <div className="ledger-item-header">
                    <strong>{claim.status}</strong>
                    <span>{Math.round(claim.confidence * 100)}%</span>
                  </div>
                  <p>{claimSummary(claim)}</p>
                  <small>
                    {claim.source_type} · {claim.scope_key}
                  </small>
                </article>
              ))
            )}
          </div>
        </div>

        <div className="ledger-section">
          <div className="ledger-section-header">
            <div>
              <h3>Umgesetzte Aenderungen</h3>
              <p className="status-text">
                Hier werden nach expliziter Freigabe umgesetzte Confluence-Updates und erstellte Jira-Tickets lokal
                protokolliert.
              </p>
            </div>
            <span className="log-count-pill">{run.implemented_changes.length}</span>
          </div>

          {implementedChanges.length === 0 ? (
            <div className="status-log-hint">
              <strong>Noch keine extern umgesetzten Aenderungen verbucht.</strong>
              <p>
                Der Auditor bleibt bis zur User-Freigabe read-only. Erst nach einer freigegebenen Aktion werden hier
                aktualisierte Confluence-Seiten oder erstellte Jira-Tickets zur Codeaenderung gelistet.
              </p>
            </div>
          ) : (
            <div className="status-log-list">
              {implementedChanges.map((change) => (
                <article className="status-log-entry implemented-change-entry" key={change.change_id}>
                  <div className="status-log-header">
                    <div>
                      <strong>{change.title}</strong>
                      <small>{formatTimestamp(change.created_at)}</small>
                    </div>
                    <div className="status-log-badges">
                      <span className={`status-log-source source-${change.change_type}`}>
                        {implementedChangeTypeLabel(change)}
                      </span>
                      <span className="status-log-level-badge level-info">{change.status}</span>
                    </div>
                  </div>
                  <p>{change.summary}</p>
                  {change.target_url ? (
                    <a className="implemented-change-link" href={change.target_url} target="_blank" rel="noreferrer">
                      {change.target_label}
                    </a>
                  ) : (
                    <strong>{change.target_label}</strong>
                  )}
                  {change.implications.length > 0 ? (
                    <div className="status-log-block">
                      <strong>Implikationen</strong>
                      <ul className="finding-list">
                        {change.implications.map((impact, index) => (
                          <li key={`${change.change_id}-impact-${index}`}>{impact}</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}
                  {change.jira_ticket ? (
                    <details className="implemented-change-details">
                      <summary>AI-Coding-Brief fuer {change.jira_ticket.ticket_key}</summary>
                      <div className="implemented-change-details-body">
                        <div className="status-log-block">
                          <strong>Problem</strong>
                          <p>{change.jira_ticket.problem_description}</p>
                        </div>
                        <div className="status-log-block">
                          <strong>Grund</strong>
                          <p>{change.jira_ticket.reason}</p>
                        </div>
                        <div className="status-log-block">
                          <strong>Korrekturmassnahmen</strong>
                          <ul className="finding-list">
                            {change.jira_ticket.correction_measures.map((item, index) => (
                              <li key={`${change.change_id}-measure-${index}`}>{item}</li>
                            ))}
                          </ul>
                        </div>
                        <div className="status-log-block">
                          <strong>Zielbild</strong>
                          <ul className="finding-list">
                            {change.jira_ticket.target_state.map((item, index) => (
                              <li key={`${change.change_id}-target-${index}`}>{item}</li>
                            ))}
                          </ul>
                        </div>
                        <div className="status-log-block">
                          <strong>Abnahmekriterien</strong>
                          <ul className="finding-list">
                            {change.jira_ticket.acceptance_criteria.map((item, index) => (
                              <li key={`${change.change_id}-acceptance-${index}`}>{item}</li>
                            ))}
                          </ul>
                        </div>
                        <div className="status-log-block">
                          <strong>Betroffene Teile</strong>
                          <ul className="finding-list">
                            {change.jira_ticket.affected_parts.map((item, index) => (
                              <li key={`${change.change_id}-part-${index}`}>{item}</li>
                            ))}
                          </ul>
                        </div>
                        <div className="status-log-block">
                          <strong>AI Coding Prompt</strong>
                          <pre className="ai-coding-prompt">{change.jira_ticket.ai_coding_prompt}</pre>
                        </div>
                      </div>
                    </details>
                  ) : null}
                  {change.confluence_update?.patch_preview ? (
                    <details className="implemented-change-details">
                      <summary>
                        Confluence-Patch fuer {change.confluence_update.page_title}
                        {change.confluence_update.applied_revision_id
                          ? ` · Revision ${change.confluence_update.applied_revision_id}`
                          : ""}
                      </summary>
                      <div className="implemented-change-details-body">
                        <div className="status-log-block">
                          <strong>Geaenderte Abschnitte</strong>
                          <ul className="finding-list">
                            {change.confluence_update.changed_sections.map((item, index) => (
                              <li key={`${change.change_id}-section-${index}`}>{item}</li>
                            ))}
                          </ul>
                        </div>
                        <div className="status-log-block">
                          <strong>Patch-Operationen</strong>
                          <ul className="finding-list">
                            {change.confluence_update.patch_preview.operations.map((operation) => (
                              <li key={operation.operation_id}>
                                {operation.section_path} · {operation.marker_kind} · {operation.proposed_statement}
                              </li>
                            ))}
                          </ul>
                        </div>
                      </div>
                    </details>
                  ) : null}
                </article>
              ))}
            </div>
          )}
        </div>

        <div className="ledger-section">
          <div className="ledger-section-header">
            <div>
              <h3>KI-Statuslog</h3>
              <p className="status-text">
                Hier erscheinen die Ableitungen aus User-Kommentaren, gespeicherten Wahrheiten und
                Regenerierungsfolgen.
              </p>
            </div>
            <span className="log-count-pill">{run.analysis_log.length}</span>
          </div>

          <form className="decision-comment-form" onSubmit={(event) => void handleDecisionCommentSubmit(event)}>
            <label>
              <span>Globale Spezifizierung fuer die Neubewertung</span>
              <textarea
                value={decisionCommentDraft}
                onChange={(event) => setDecisionCommentDraft(event.target.value)}
                placeholder="z. B. Statement darf nur im Review-Status geschrieben werden."
                rows={3}
              />
            </label>
            <div className="form-actions">
              <button
                type="submit"
                className="primary-button"
                disabled={decisionCommentSubmitting || decisionCommentDraft.trim().length === 0}
              >
                {decisionCommentSubmitting ? "Kommentar wird ausgewertet..." : "Kommentar auswerten"}
              </button>
            </div>
          </form>

          {decisionCommentError ? <div className="error-box">{decisionCommentError}</div> : null}
          {decisionCommentCount === 0 ? (
            <div className="status-log-hint">
              <strong>Noch keine User-Kommentare ausgewertet.</strong>
              <p>
                Pipeline-Logs sind bereits sichtbar. Sobald eine Spezifizierung gespeichert wird, erscheinen hier auch
                abgeleitete Wahrheiten, Scope-Cluster und Auswirkungsfolgen.
              </p>
            </div>
          ) : null}

          <div className="status-log-list">
            {logEntries.map((entry) => (
              <article className={`status-log-entry status-log-level-${entry.level}`} key={entry.log_id}>
                <div className="status-log-header">
                  <div>
                    <strong>{entry.title}</strong>
                    <small>{formatTimestamp(entry.created_at)}</small>
                  </div>
                  <div className="status-log-badges">
                    <span className={`status-log-source source-${entry.source_type}`}>
                      {analysisSourceLabel(entry)}
                    </span>
                    <span className={`status-log-level-badge level-${entry.level}`}>{entry.level}</span>
                  </div>
                </div>
                <p>{entry.message}</p>
                {entry.related_scope_keys.length > 0 ? (
                  <div className="status-log-tags">
                    {entry.related_scope_keys.map((scopeKey) => (
                      <span className="status-log-tag" key={scopeKey}>
                        {scopeKey}
                      </span>
                    ))}
                  </div>
                ) : null}
                {entry.related_finding_ids.length > 0 ? (
                  <div className="status-log-block">
                    <strong>Betroffene Problemelemente</strong>
                    <div className="status-log-tags">
                      {entry.related_finding_ids.map((findingId) => (
                        <span className="status-log-tag" key={findingId}>
                          {findingId}
                        </span>
                      ))}
                    </div>
                  </div>
                ) : null}
                {entry.derived_changes.length > 0 ? (
                  <div className="status-log-block">
                    <strong>Abgeleitete Aenderungen</strong>
                    <ul className="finding-list">
                      {entry.derived_changes.map((change, index) => (
                        <li key={`${entry.log_id}-change-${index}`}>{change}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
                {entry.impact_summary.length > 0 ? (
                  <div className="status-log-block">
                    <strong>Auswirkungen</strong>
                    <ul className="finding-list">
                      {entry.impact_summary.map((impact, index) => (
                        <li key={`${entry.log_id}-impact-${index}`}>{impact}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
              </article>
            ))}
          </div>
        </div>

        <div className="ledger-section">
          <h3>Zugriffspolitik</h3>
          <div className="policy-inline-box">
            <strong>Externe Systeme bleiben unveraendert</strong>
            <p>{accessPolicy.summary}</p>
          </div>
        </div>

        <div className="ledger-section">
          <h3>Source Snapshots</h3>
          <div className="ledger-list">
            {run.source_snapshots.length === 0 ? (
              <p className="status-text">Noch keine Snapshots gespeichert.</p>
            ) : (
              run.source_snapshots.map((snapshot) => (
                <article className="ledger-item" key={snapshot.snapshot_id}>
                  <div className="ledger-item-header">
                    <strong>{snapshot.source_type}</strong>
                    <span>{formatTimestamp(snapshot.collected_at)}</span>
                  </div>
                  <p>{snapshot.source_id}</p>
                  <small>{snapshotSubtitle(snapshot)}</small>
                </article>
              ))
            )}
          </div>
        </div>

        <div className="ledger-section">
          <h3>Finding Links</h3>
          <div className="ledger-list">
            {run.finding_links.length === 0 ? (
              <p className="status-text">Noch keine Verknuepfungen vorhanden.</p>
            ) : (
              run.finding_links.map((link) => (
                <article className="ledger-item" key={link.link_id}>
                  <div className="ledger-item-header">
                    <strong>{link.relation_type}</strong>
                    <span>{Math.round(link.confidence * 100)}%</span>
                  </div>
                  <p>
                    {link.from_finding_id} → {link.to_finding_id}
                  </p>
                  <small>{link.rationale}</small>
                </article>
              ))
            )}
          </div>
        </div>

        <div className="ledger-section">
          <h3>Quellenprofil</h3>
          <div className="source-ledger">
            <a className="ledger-card" href={sourceProfile.confluence_url} target="_blank" rel="noreferrer">
              <span>Analysequelle</span>
              <strong>{sourceProfile.confluence_space_key}</strong>
              <small>{sourceProfile.confluence_url}</small>
            </a>
            <div className="ledger-card">
              <span>Analysequelle</span>
              <strong>Current Dump</strong>
              <small>{sourceProfile.metamodel_dump_path}</small>
            </div>
            <a className="ledger-card" href={sourceProfile.jira_url} target="_blank" rel="noreferrer">
              <span>Ticket-Ziel</span>
              <strong>{sourceProfile.jira_project_key}</strong>
              <small>{sourceProfile.jira_url}</small>
            </a>
          </div>
        </div>
      </aside>
    </section>
  );
}
