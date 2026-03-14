import { useMemo, useState } from "react";
import type {
  AuditRun,
  WritebackApprovalRequest,
  ConfluencePatchPreview,
  SourceProfile,
} from "../types";

type ApprovalsViewProps = {
  run: AuditRun | null;
  sourceProfile: SourceProfile;
  approvalSubmitting: string;
  approvalError: string;
  executionSubmitting: string;
  executionError: string;
  onResolveApprovalRequest: (
    approvalRequestId: string,
    decision: "approve" | "reject" | "cancel",
    commentText?: string
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

function formatTs(value?: string | null): string {
  if (!value) return "–";
  return new Intl.DateTimeFormat("de-DE", { dateStyle: "short", timeStyle: "short" }).format(new Date(value));
}

function getTypeLabel(req: WritebackApprovalRequest): string {
  return req.target_type === "confluence_page_update" ? "Confluence" : "Jira";
}

function getTypeIcon(req: WritebackApprovalRequest): string {
  return req.target_type === "confluence_page_update" ? "📄" : "🎫";
}

function getPatchPreview(req: WritebackApprovalRequest): ConfluencePatchPreview | null {
  const raw = req.metadata?.confluence_patch_preview;
  if (!raw || typeof raw !== "object") return null;
  return raw as ConfluencePatchPreview;
}

export function ApprovalsView({
  run,
  sourceProfile,
  approvalSubmitting,
  approvalError,
  executionSubmitting,
  executionError,
  onResolveApprovalRequest,
  onRecordConfluencePageUpdate,
  onRecordJiraTicketCreated,
  onExecuteConfluencePageWriteback,
  onExecuteJiraTicketWriteback,
}: ApprovalsViewProps): JSX.Element {
  const [commentDrafts, setCommentDrafts] = useState<Record<string, string>>({});
  const [confluenceDrafts, setConfluenceDrafts] = useState<
    Record<string, { pageTitle: string; pageUrl: string; changedSections: string; changeSummary: string }>
  >({});
  const [jiraDrafts, setJiraDrafts] = useState<Record<string, { ticketKey: string; ticketUrl: string }>>({});

  const requests = useMemo(
    () => [...(run?.approval_requests ?? [])].sort((a, b) => b.created_at.localeCompare(a.created_at)),
    [run?.approval_requests]
  );

  const pendingCount = requests.filter((r) => r.status === "pending").length;
  const approvedCount = requests.filter((r) => r.status === "approved").length;

  function getConfDraft(req: WritebackApprovalRequest) {
    const pp = getPatchPreview(req);
    return confluenceDrafts[req.approval_request_id] ?? {
      pageTitle: pp?.page_title || req.title.replace("Confluence-Writeback für ", "") || "FIN-AI Spezifikation",
      pageUrl: pp?.page_url || req.target_url || sourceProfile.confluence_url,
      changedSections: (pp?.changed_sections ?? req.related_package_ids).join("\n"),
      changeSummary: (pp?.change_summary ?? req.payload_preview).join("\n"),
    };
  }

  function getJiraDraft(req: WritebackApprovalRequest) {
    return jiraDrafts[req.approval_request_id] ?? {
      ticketKey: "",
      ticketUrl: req.target_url || sourceProfile.jira_url,
    };
  }

  if (!run) {
    return (
      <div className="empty-state">
        <div className="empty-state-icon">✅</div>
        <strong>Kein Run ausgewählt</strong>
        <p>Wähle einen Audit-Run aus, um Freigaben zu verwalten.</p>
      </div>
    );
  }

  return (
    <>
      <div className="info-box">
        <strong>{pendingCount}</strong> ausstehend · <strong>{approvedCount}</strong> genehmigt ·{" "}
        {requests.length} gesamt
      </div>

      {approvalError ? <div className="error-box">{approvalError}</div> : null}
      {executionError ? <div className="error-box">{executionError}</div> : null}

      {requests.length === 0 ? (
        <div className="empty-state">
          <div className="empty-state-icon">✅</div>
          <strong>Noch keine Freigaben angefordert</strong>
          <p>Die Buttons an den Entscheidungspaketen legen lokale Approval-Requests an.</p>
        </div>
      ) : (
        requests.map((req) => {
          const patchPreview = getPatchPreview(req);

          return (
            <div className="approval-card" key={req.approval_request_id}>
              <div className="approval-head">
                <div>
                  <div className="ledger-item-title">
                    {getTypeIcon(req)} {req.title}
                  </div>
                  <div className="ledger-item-meta">{formatTs(req.created_at)}</div>
                </div>
                <div className="approval-head-badges">
                  <span className="badge badge-category">{getTypeLabel(req)}</span>
                  <span className={`badge badge-state badge-${req.status}`}>{req.status}</span>
                </div>
              </div>

              <div className="approval-body">{req.summary}</div>

              {/* Payload preview */}
              {req.payload_preview.length > 0 ? (
                <div className="approval-preview">
                  <ul>
                    {req.payload_preview.map((item, i) => (
                      <li key={`${req.approval_request_id}-pv-${i}`}>{item}</li>
                    ))}
                  </ul>
                </div>
              ) : null}

              {/* Confluence patch preview */}
              {req.target_type === "confluence_page_update" && patchPreview ? (
                <div className="patch-preview">
                  <div className="patch-preview-title">
                    Section-Anchored Patch Preview · {patchPreview.page_title}
                    {" · "}
                    <span style={{ color: patchPreview.execution_ready ? "var(--color-success)" : "var(--color-warning)" }}>
                      {patchPreview.execution_ready ? "extern ausführbar" : "blockiert"}
                    </span>
                  </div>
                  {patchPreview.blockers.length > 0 ? (
                    <ul className="evidence-list">
                      {patchPreview.blockers.map((b) => (
                        <li key={b} style={{ color: "var(--color-error)" }}>{b}</li>
                      ))}
                    </ul>
                  ) : null}
                  {patchPreview.operations.slice(0, 6).map((op) => (
                    <div className="patch-operation" key={op.operation_id}>
                      <strong>{op.section_path}</strong> · {op.marker_kind} · {op.proposed_statement}
                    </div>
                  ))}
                </div>
              ) : null}

              {/* Jira ticket brief preview */}
              {req.target_type === "jira_ticket_create" && req.metadata?.jira_ticket_brief ? (
                <div className="patch-preview">
                  <div className="patch-preview-title">
                    🎫 AI-Coding Brief: {req.metadata.jira_ticket_brief.title}
                  </div>
                  <p className="text-xs text-secondary" style={{ margin: "4px 0" }}>
                    {req.metadata.jira_ticket_brief.problem_description}
                  </p>
                  {req.metadata.jira_ticket_brief.correction_measures.length > 0 ? (
                    <ul className="evidence-list">
                      {req.metadata.jira_ticket_brief.correction_measures.map((m, i) => (
                        <li key={i}>{m}</li>
                      ))}
                    </ul>
                  ) : null}
                </div>
              ) : null}

              {/* Pending: approval actions */}
              {req.status === "pending" ? (
                <div style={{ marginTop: "var(--space-md)" }}>
                  <textarea
                    value={commentDrafts[req.approval_request_id] ?? ""}
                    onChange={(e) =>
                      setCommentDrafts((c) => ({ ...c, [req.approval_request_id]: e.target.value }))
                    }
                    rows={2}
                    placeholder="Optionaler Kommentar zur Freigabeentscheidung"
                    style={{ marginBottom: "var(--space-sm)" }}
                  />
                  <div className="approval-actions">
                    <button
                      className="btn btn-success btn-sm"
                      disabled={approvalSubmitting === req.approval_request_id}
                      onClick={() =>
                        void onResolveApprovalRequest(req.approval_request_id, "approve", commentDrafts[req.approval_request_id])
                      }
                    >
                      Genehmigen
                    </button>
                    <button
                      className="btn btn-danger btn-sm"
                      disabled={approvalSubmitting === req.approval_request_id}
                      onClick={() =>
                        void onResolveApprovalRequest(req.approval_request_id, "reject", commentDrafts[req.approval_request_id])
                      }
                    >
                      Ablehnen
                    </button>
                    <button
                      className="btn btn-secondary btn-sm"
                      disabled={approvalSubmitting === req.approval_request_id}
                      onClick={() => void onResolveApprovalRequest(req.approval_request_id, "cancel")}
                    >
                      Stornieren
                    </button>
                  </div>
                </div>
              ) : null}

              {/* Approved: execution forms */}
              {req.status === "approved" && req.target_type === "confluence_page_update" ? (
                <div style={{ marginTop: "var(--space-md)" }}>
                  <div className="package-section-label">Confluence-Writeback ausführen</div>
                  <div className="form-stack" style={{ gap: "var(--space-sm)" }}>
                    <div className="form-field">
                      <label>Seitentitel</label>
                      <input
                        value={getConfDraft(req).pageTitle}
                        onChange={(e) => setConfluenceDrafts((c) => ({ ...c, [req.approval_request_id]: { ...getConfDraft(req), pageTitle: e.target.value } }))}
                      />
                    </div>
                    <div className="form-field">
                      <label>Seiten-URL</label>
                      <input
                        value={getConfDraft(req).pageUrl}
                        onChange={(e) => setConfluenceDrafts((c) => ({ ...c, [req.approval_request_id]: { ...getConfDraft(req), pageUrl: e.target.value } }))}
                      />
                    </div>
                    <div className="approval-actions">
                      <button
                        className="btn btn-primary btn-sm"
                        disabled={executionSubmitting === req.approval_request_id}
                        onClick={() => void onExecuteConfluencePageWriteback(req.approval_request_id)}
                      >
                        Extern ausführen
                      </button>
                      <button
                        className="btn btn-secondary btn-sm"
                        disabled={executionSubmitting === req.approval_request_id}
                        onClick={() => {
                          const d = getConfDraft(req);
                          void onRecordConfluencePageUpdate({
                            approval_request_id: req.approval_request_id,
                            page_title: d.pageTitle.trim(),
                            page_url: d.pageUrl.trim(),
                            changed_sections: d.changedSections.split("\n").map((s) => s.trim()).filter(Boolean),
                            change_summary: d.changeSummary.split("\n").map((s) => s.trim()).filter(Boolean),
                            related_finding_ids: req.related_finding_ids,
                          });
                        }}
                      >
                        Lokal verbuchen
                      </button>
                    </div>
                  </div>
                </div>
              ) : null}

              {req.status === "approved" && req.target_type === "jira_ticket_create" ? (
                <div style={{ marginTop: "var(--space-md)" }}>
                  <div className="package-section-label">Jira-Ticket ausführen</div>
                  <div className="form-stack" style={{ gap: "var(--space-sm)" }}>
                    <div className="form-row">
                      <div className="form-field">
                        <label>Ticket Key</label>
                        <input
                          value={getJiraDraft(req).ticketKey}
                          onChange={(e) => setJiraDrafts((c) => ({ ...c, [req.approval_request_id]: { ...getJiraDraft(req), ticketKey: e.target.value } }))}
                          placeholder="FINAI-123"
                        />
                      </div>
                      <div className="form-field">
                        <label>Ticket URL</label>
                        <input
                          value={getJiraDraft(req).ticketUrl}
                          onChange={(e) => setJiraDrafts((c) => ({ ...c, [req.approval_request_id]: { ...getJiraDraft(req), ticketUrl: e.target.value } }))}
                        />
                      </div>
                    </div>
                    <div className="approval-actions">
                      <button
                        className="btn btn-primary btn-sm"
                        disabled={executionSubmitting === req.approval_request_id}
                        onClick={() => void onExecuteJiraTicketWriteback(req.approval_request_id)}
                      >
                        Extern ausführen
                      </button>
                      <button
                        className="btn btn-secondary btn-sm"
                        disabled={executionSubmitting === req.approval_request_id}
                        onClick={() => {
                          const d = getJiraDraft(req);
                          void onRecordJiraTicketCreated({
                            approval_request_id: req.approval_request_id,
                            ticket_key: d.ticketKey.trim(),
                            ticket_url: d.ticketUrl.trim(),
                            related_finding_ids: req.related_finding_ids,
                          });
                        }}
                      >
                        Lokal verbuchen
                      </button>
                    </div>
                  </div>
                </div>
              ) : null}
            </div>
          );
        })
      )}

      {/* Implemented changes ledger */}
      {(run.implemented_changes.length > 0) ? (
        <div className="ledger-section mt-xl">
          <div className="ledger-section-head">
            <h3>Vollzugsledger</h3>
            <span className="ledger-count">{run.implemented_changes.length}</span>
          </div>
          <div className="ledger-list">
            {[...run.implemented_changes].reverse().map((change) => (
              <div className="ledger-item" key={change.change_id}>
                <div className="ledger-item-head">
                  <span className="ledger-item-title">
                    {change.change_type === "confluence_page_updated" ? "📄" : "🎫"} {change.title}
                  </span>
                  <span className={`badge badge-${change.status === "applied" ? "completed" : "failed"}`}>
                    {change.status}
                  </span>
                </div>
                <div className="ledger-item-body">{change.summary}</div>
                <div className="ledger-item-meta">{formatTs(change.created_at)}</div>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </>
  );
}
