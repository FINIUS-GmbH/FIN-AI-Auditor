import { useMemo, useState } from "react";
import { categoryLabel, categorySortKey } from "../categoryLabels";
import type { AuditRun, AuditFinding, AuditLocation } from "../types";

type FindingsViewProps = {
  run: AuditRun | null;
  decisionCommentSubmitting: boolean;
  decisionCommentError: string;
  onDecisionCommentSubmit: (commentText: string) => Promise<void>;
};

const SOURCE_ICONS: Record<string, { icon: string; label: string; cls: string }> = {
  github_file: { icon: "⌨", label: "Code", cls: "src-github" },
  confluence_page: { icon: "📄", label: "Confluence", cls: "src-confluence" },
  metamodel: { icon: "🔷", label: "Metamodell", cls: "src-metamodel" },
  local_doc: { icon: "📋", label: "Lokal", cls: "src-local" },
  jira_ticket: { icon: "🎫", label: "Jira", cls: "src-jira" },
  user_truth: { icon: "✦", label: "Nutzer", cls: "src-user" },
};

function sourceInfo(sourceType: string): { icon: string; label: string; cls: string } {
  return SOURCE_ICONS[sourceType] ?? { icon: "•", label: sourceType, cls: "src-default" };
}

function formatLocation(loc: AuditLocation): string {
  const parts = [loc.title];
  if (loc.path_hint) parts.push(loc.path_hint);
  if (loc.position?.anchor_value) parts.push(loc.position.anchor_value);
  return parts.join(" · ");
}

function retrievalContext(finding: AuditFinding): string[] {
  const raw = finding.metadata?.retrieval_context;
  return Array.isArray(raw) ? raw.filter((x): x is string => typeof x === "string" && x.trim().length > 0) : [];
}

const SEVERITY_ORDER: Record<string, number> = { critical: 0, high: 1, medium: 2, low: 3 };

export function FindingsView({
  run,
  decisionCommentSubmitting,
  decisionCommentError,
  onDecisionCommentSubmit,
}: FindingsViewProps): JSX.Element {
  const [filter, setFilter] = useState("Alle");
  const [sortBy, setSortBy] = useState<"severity" | "category">("severity");
  const [feedbackDrafts, setFeedbackDrafts] = useState<Record<string, string>>({});
  const [expandedFindings, setExpandedFindings] = useState<Set<string>>(new Set());

  const findings = useMemo(() => {
    let filtered = run?.findings ?? [];
    if (filter !== "Alle") {
      filtered = filtered.filter((f) => f.category === filter);
    }
    return [...filtered].sort((a, b) => {
      if (sortBy === "severity") {
        return (
          (SEVERITY_ORDER[a.severity] ?? 4)
          - (SEVERITY_ORDER[b.severity] ?? 4)
          || categorySortKey(a.category)[0] - categorySortKey(b.category)[0]
          || a.title.localeCompare(b.title)
        );
      }
      return (
        categorySortKey(a.category)[0] - categorySortKey(b.category)[0]
        || (SEVERITY_ORDER[a.severity] ?? 4) - (SEVERITY_ORDER[b.severity] ?? 4)
        || a.title.localeCompare(b.title)
      );
    });
  }, [run?.findings, filter, sortBy]);

  const categories = useMemo(() => {
    const cats = new Set(run?.findings.map((f) => f.category) ?? []);
    return [
      "Alle",
      ...[...cats].sort((left, right) => {
        const [leftIdx, leftValue] = categorySortKey(left);
        const [rightIdx, rightValue] = categorySortKey(right);
        return leftIdx - rightIdx || leftValue.localeCompare(rightValue);
      }),
    ];
  }, [run?.findings]);

  const severityCounts = useMemo(() => {
    const counts = { critical: 0, high: 0, medium: 0, low: 0 };
    for (const f of run?.findings ?? []) {
      counts[f.severity] = (counts[f.severity] || 0) + 1;
    }
    return counts;
  }, [run?.findings]);
  const boundaryFindingCount = useMemo(
    () => run?.findings.filter((finding) => finding.category === "legacy_path_gap").length ?? 0,
    [run?.findings],
  );

  function toggleExpanded(findingId: string): void {
    setExpandedFindings((prev) => {
      const next = new Set(prev);
      if (next.has(findingId)) next.delete(findingId);
      else next.add(findingId);
      return next;
    });
  }

  async function handleFindingFeedback(findingId: string, action: "accept" | "dismiss"): Promise<void> {
    const draft = feedbackDrafts[findingId]?.trim();
    const prefix = action === "accept" ? "[ANNEHMEN]" : "[ABLEHNEN]";
    const text = draft
      ? `${prefix} Finding ${findingId}: ${draft}`
      : `${prefix} Finding ${findingId}`;
    await onDecisionCommentSubmit(text);
    setFeedbackDrafts((c) => ({ ...c, [findingId]: "" }));
  }

  if (!run) {
    return (
      <div className="empty-state">
        <div className="empty-state-icon">🔍</div>
        <strong>Kein Run ausgewählt</strong>
        <p>Wähle einen Audit-Run aus, um Findings zu sehen.</p>
      </div>
    );
  }

  return (
    <>
      {/* Severity summary */}
      <div className="metrics-grid" style={{ marginBottom: "var(--space-xl)" }}>
        <div className="metric-card mc-red">
          <span className="metric-label">Critical</span>
          <span className="metric-value">{severityCounts.critical}</span>
        </div>
        <div className="metric-card mc-amber">
          <span className="metric-label">High</span>
          <span className="metric-value">{severityCounts.high}</span>
        </div>
        <div className="metric-card" style={{ borderLeft: "none" }}>
          <span className="metric-label">Medium</span>
          <span className="metric-value">{severityCounts.medium}</span>
        </div>
        <div className="metric-card mc-purple">
          <span className="metric-label">Low</span>
          <span className="metric-value">{severityCounts.low}</span>
        </div>
        <div className="metric-card mc-copper">
          <span className="metric-label">Boundary-Pfade</span>
          <span className="metric-value">{boundaryFindingCount}</span>
        </div>
      </div>

      {/* Filters */}
      <div className="flex-row justify-between mb-xl flex-wrap gap-md">
        <div className="filter-bar" style={{ marginBottom: 0 }}>
          {categories.map((cat) => (
            <button
              key={cat}
              className={`filter-pill${filter === cat ? " active" : ""}`}
              onClick={() => setFilter(cat)}
            >
              {cat === "Alle" ? `Alle (${run.findings.length})` : categoryLabel(cat)}
            </button>
          ))}
        </div>
        <div className="form-actions">
          <button
            className={`btn btn-sm ${sortBy === "severity" ? "btn-primary" : "btn-secondary"}`}
            onClick={() => setSortBy("severity")}
          >
            Nach Severity
          </button>
          <button
            className={`btn btn-sm ${sortBy === "category" ? "btn-primary" : "btn-secondary"}`}
            onClick={() => setSortBy("category")}
          >
            Nach Kategorie
          </button>
        </div>
      </div>

      {decisionCommentError ? <div className="error-box">{decisionCommentError}</div> : null}

      {/* Finding cards */}
      {findings.length === 0 ? (
        <div className="empty-state">
          <div className="empty-state-icon">🔍</div>
          <strong>Keine Findings</strong>
          <p>
            {filter !== "Alle"
              ? "Kein Finding in dieser Kategorie."
              : "Der Worker verarbeitet geplante Runs und hinterlegt danach Findings."}
          </p>
        </div>
      ) : (
        findings.map((finding) => {
          const expanded = expandedFindings.has(finding.finding_id);
          const ctxEntries = retrievalContext(finding);

          return (
            <article className="finding-card" key={finding.finding_id} id={`finding-${finding.finding_id}`}>
              {/* Header badges */}
              <div className="finding-card-header">
                <span className={`badge badge-${finding.severity}`}>{finding.severity}</span>
                <span className="badge badge-category">{categoryLabel(finding.category)}</span>
                <span className={`badge badge-state badge-${finding.resolution_state || "open"}`}>
                  {finding.resolution_state || "open"}
                </span>
                <button
                  className="btn btn-ghost btn-sm"
                  onClick={() => toggleExpanded(finding.finding_id)}
                  style={{ marginLeft: "auto" }}
                >
                  {expanded ? "▲ Weniger" : "▼ Details"}
                </button>
              </div>

              {/* Title */}
              <div className="finding-card-title">
                {finding.title}
                {finding.canonical_key ? (
                  <span className="text-muted text-xs" style={{ marginLeft: 8 }}>
                    {finding.canonical_key}
                  </span>
                ) : null}
              </div>

              {/* Summary */}
              <div className="finding-card-summary">{finding.summary}</div>

              {/* Evidence locations — always visible with source badges */}
              <div className="package-section">
                <div className="package-section-label">Evidenz</div>
                <div className="problem-evidence">
                  {finding.locations.map((loc) => {
                    const src = sourceInfo(loc.source_type);
                    return (
                      <span
                        key={loc.location_id || `${loc.source_id}-${loc.title}`}
                        style={{ display: "flex", alignItems: "center", gap: 6, padding: "3px 0" }}
                      >
                        <span
                          className={`badge-source ${src.cls}`}
                          style={{
                            display: "inline-flex",
                            alignItems: "center",
                            gap: 3,
                            padding: "1px 7px",
                            borderRadius: 4,
                            fontSize: 11,
                            fontWeight: 600,
                            background:
                              loc.source_type === "github_file"
                                ? "rgba(99,102,241,0.12)"
                                : loc.source_type === "confluence_page"
                                ? "rgba(16,185,129,0.12)"
                                : loc.source_type === "metamodel"
                                ? "rgba(75,123,236,0.12)"
                                : "rgba(100,116,139,0.12)",
                            color:
                              loc.source_type === "github_file"
                                ? "#818cf8"
                                : loc.source_type === "confluence_page"
                                ? "#34d399"
                                : loc.source_type === "metamodel"
                                ? "#93b4f5"
                                : "#94a3b8",
                            flexShrink: 0,
                          }}
                        >
                          {src.icon} {src.label}
                        </span>
                        {loc.url ? (
                          <a href={loc.url} target="_blank" rel="noreferrer" style={{ fontSize: 12 }}>
                            {formatLocation(loc)}
                          </a>
                        ) : (
                          <span style={{ fontSize: 12, color: "var(--text-muted)" }}>
                            {formatLocation(loc)}
                          </span>
                        )}
                      </span>
                    );
                  })}
                </div>
              </div>

              {/* Expanded details */}
              {expanded ? (
                <>
                  {/* Recommendation */}
                  <div className="package-section">
                    <div className="package-section-label">Empfehlung</div>
                    <div className="package-recommendation">{finding.recommendation}</div>
                  </div>

                  {/* Retrieval context */}
                  {ctxEntries.length > 0 ? (
                    <div className="package-section">
                      <div className="package-section-label">Retrieval-Kontext</div>
                      <ul className="evidence-list">
                        {ctxEntries.map((c, i) => (
                          <li key={`${finding.finding_id}-ctx-${i}`}>{c}</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}

                  {/* Proposed actions */}
                  {finding.proposed_confluence_action || finding.proposed_jira_action ? (
                    <div className="package-section">
                      <div className="package-section-label">Vorgeschlagene Aktionen</div>
                      {finding.proposed_confluence_action ? (
                        <p className="text-xs text-secondary" style={{ marginBottom: 4 }}>
                          📄 Confluence: {finding.proposed_confluence_action}
                        </p>
                      ) : null}
                      {finding.proposed_jira_action ? (
                        <p className="text-xs text-secondary">
                          🎫 Jira: {finding.proposed_jira_action}
                        </p>
                      ) : null}
                    </div>
                  ) : null}
                </>
              ) : null}

              {/* Feedback & decision — always visible per user request */}
              <div className="decision-actions">
                <div className="decision-comment-area">
                  <textarea
                    value={feedbackDrafts[finding.finding_id] ?? ""}
                    onChange={(e) =>
                      setFeedbackDrafts((c) => ({ ...c, [finding.finding_id]: e.target.value }))
                    }
                    placeholder="Feedback, Neubewertung oder Begründung zu diesem Finding…"
                    rows={2}
                  />
                </div>
                <div className="decision-buttons">
                  <button
                    className="btn btn-success btn-sm"
                    disabled={decisionCommentSubmitting}
                    onClick={() => void handleFindingFeedback(finding.finding_id, "accept")}
                    title="Finding als korrekt annehmen"
                  >
                    ✓ Annehmen
                  </button>
                  <button
                    className="btn btn-danger btn-sm"
                    disabled={decisionCommentSubmitting}
                    onClick={() => void handleFindingFeedback(finding.finding_id, "dismiss")}
                    title="Finding als irrelevant ablehnen"
                  >
                    ✗ Ablehnen
                  </button>
                </div>
              </div>
            </article>
          );
        })
      )}
    </>
  );
}
