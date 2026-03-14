import { useMemo, useState } from "react";
import type {
  AuditRun,
  AuditLocation,
  DecisionPackage,
  SourceProfile,
} from "../types";

type PackagesViewProps = {
  run: AuditRun | null;
  sourceProfile: SourceProfile;
  packageDecisionSubmitting: string;
  packageDecisionError: string;
  approvalSubmitting: string;
  approvalError: string;
  onPackageDecisionSubmit: (
    packageId: string,
    action: "accept" | "reject" | "specify",
    commentText?: string
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
};

function formatLocation(loc: AuditLocation): string {
  const parts = [loc.title];
  if (loc.path_hint) parts.push(loc.path_hint);
  if (loc.position?.anchor_value) parts.push(loc.position.anchor_value);
  return parts.join(" · ");
}

function metaStrings(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((x): x is string => typeof x === "string" && x.trim().length > 0)
    : [];
}

const ALL_CATEGORIES = [
  "Alle",
  "contradiction",
  "implementation_drift",
  "policy_conflict",
  "missing_definition",
  "clarification_needed",
  "read_write_gap",
  "stale_source",
  "traceability_gap",
  "ownership_gap",
  "terminology_collision",
  "low_confidence_review",
  "obsolete_documentation",
  "open_decision",
] as const;

export function PackagesView({
  run,
  sourceProfile,
  packageDecisionSubmitting,
  packageDecisionError,
  approvalSubmitting,
  approvalError,
  onPackageDecisionSubmit,
  onCreateApprovalRequest,
}: PackagesViewProps): JSX.Element {
  const [filter, setFilter] = useState("Alle");
  const [commentDrafts, setCommentDrafts] = useState<Record<string, string>>({});

  const packageGroups = useMemo(() => {
    const groups = new Map<string, DecisionPackage[]>();
    for (const pkg of run?.decision_packages ?? []) {
      if (filter !== "Alle" && pkg.category !== filter) continue;
      const current = groups.get(pkg.category) ?? [];
      current.push(pkg);
      groups.set(pkg.category, current);
    }
    return [...groups.entries()];
  }, [run?.decision_packages, filter]);

  const totalCount = run?.decision_packages.length ?? 0;
  const openCount = run?.decision_packages.filter((p) => p.decision_state === "open").length ?? 0;
  const usedCategories = new Set(run?.decision_packages.map((p) => p.category) ?? []);

  async function handleDecision(pkgId: string, action: "accept" | "reject" | "specify"): Promise<void> {
    const comment = commentDrafts[pkgId]?.trim() || undefined;
    await onPackageDecisionSubmit(pkgId, action, comment);
    if (action !== "reject") {
      setCommentDrafts((c) => ({ ...c, [pkgId]: "" }));
    }
  }

  async function handleCreateApproval(
    pkg: DecisionPackage,
    targetType: "confluence_page_update" | "jira_ticket_create"
  ): Promise<void> {
    const isConfl = targetType === "confluence_page_update";
    await onCreateApprovalRequest({
      target_type: targetType,
      title: `${isConfl ? "Confluence" : "Jira"}-Writeback für ${pkg.title}`,
      summary: isConfl
        ? "Lokale Freigabeanfrage für ein späteres Confluence-Update."
        : "Lokale Freigabeanfrage für die spätere Erstellung eines Jira-Tickets.",
      target_url: isConfl ? sourceProfile.confluence_url : sourceProfile.jira_url,
      related_package_ids: [pkg.package_id],
      related_finding_ids: pkg.related_finding_ids,
      payload_preview: [
        `Scope: ${pkg.scope_summary}`,
        `Empfehlung: ${pkg.recommendation_summary}`,
        isConfl
          ? "Nach Freigabe darf ein Confluence-Patch lokal als writeback-bereit markiert werden."
          : "Nach Freigabe darf ein Jira-Ticket mit AI-Coding-Brief erzeugt werden.",
      ],
    });
  }

  if (!run) {
    return (
      <div className="empty-state">
        <div className="empty-state-icon">📦</div>
        <strong>Kein Run ausgewählt</strong>
        <p>Wähle einen Audit-Run aus, um Entscheidungspakete zu bearbeiten.</p>
      </div>
    );
  }

  return (
    <>
      {/* Filter bar */}
      <div className="filter-bar">
        {ALL_CATEGORIES.filter(
          (c) => c === "Alle" || usedCategories.has(c)
        ).map((cat) => (
          <button
            key={cat}
            className={`filter-pill${filter === cat ? " active" : ""}`}
            onClick={() => setFilter(cat)}
          >
            {cat === "Alle" ? `Alle (${totalCount})` : cat}
          </button>
        ))}
      </div>

      {/* Summary banner */}
      <div className="info-box">
        <strong>{openCount}</strong> offene Pakete von {totalCount} gesamt ·{" "}
        {run.decision_packages.filter((p) => p.decision_state === "accepted").length} angenommen ·{" "}
        {run.decision_packages.filter((p) => p.decision_state === "rejected").length} abgelehnt ·{" "}
        {run.decision_packages.filter((p) => p.decision_state === "specified").length} spezifiziert
      </div>

      {packageDecisionError ? <div className="error-box">{packageDecisionError}</div> : null}
      {approvalError ? <div className="error-box">{approvalError}</div> : null}

      {/* Package cards */}
      {packageGroups.length === 0 ? (
        <div className="empty-state">
          <div className="empty-state-icon">📦</div>
          <strong>Keine Entscheidungspakete</strong>
          <p>
            {filter !== "Alle"
              ? "Kein Paket in dieser Kategorie. Versuche einen anderen Filter."
              : "Nach der Analyse werden hier atomare Pakete aufgebaut."}
          </p>
        </div>
      ) : (
        packageGroups.map(([category, packages]) => (
          <div className="category-group" key={category}>
            <div className="category-group-head">
              <h3>{category}</h3>
              <span className="category-group-count">{packages.length} Paket(e)</span>
            </div>

            {packages.map((pkg) => (
              <article className="package-card" key={pkg.package_id} id={`pkg-${pkg.package_id}`}>
                {/* Badges */}
                <div className="package-badges">
                  <span className={`badge badge-${pkg.severity_summary}`}>{pkg.severity_summary}</span>
                  <span className="badge badge-category">{pkg.category}</span>
                  <span className={`badge badge-state badge-${pkg.decision_state}`}>{pkg.decision_state}</span>
                </div>

                {/* Title & Scope */}
                <h3 className="package-title">{pkg.title}</h3>
                <p className="package-scope">{pkg.scope_summary}</p>

                {/* Recommendation */}
                <div className="package-section">
                  <div className="package-section-label">Empfehlung</div>
                  <div className="package-recommendation">{pkg.recommendation_summary}</div>
                </div>

                {/* Delta hints */}
                {metaStrings(pkg.metadata?.delta_summary).length > 0 ? (
                  <div className="package-section">
                    <div className="package-section-label">Delta-Hinweise</div>
                    <ul className="evidence-list">
                      {metaStrings(pkg.metadata?.delta_summary).map((entry, i) => (
                        <li key={`${pkg.package_id}-delta-${i}`}>{entry}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}

                {/* Problem Elements */}
                <div className="package-section">
                  <div className="package-section-label">
                    Problemelemente ({pkg.problem_elements.length})
                  </div>
                  <div className="problem-stack">
                    {pkg.problem_elements.map((prob) => (
                      <div className="problem-element" key={prob.problem_id}>
                        <div className="problem-head">
                          <span className={`badge badge-${prob.severity}`}>{prob.severity}</span>
                          <span className="badge badge-category">{prob.category}</span>
                          <span className="problem-confidence">{Math.round(prob.confidence * 100)}%</span>
                        </div>
                        <div className="problem-scope">{prob.scope_summary}</div>
                        <div className="problem-explanation">{prob.short_explanation}</div>
                        {prob.evidence_locations.length > 0 ? (
                          <div className="problem-evidence">
                            {prob.evidence_locations.map((loc) => (
                              <span key={loc.location_id || `${loc.source_id}-${loc.title}`}>
                                {loc.url ? (
                                  <a href={loc.url} target="_blank" rel="noreferrer">
                                    {formatLocation(loc)}
                                  </a>
                                ) : (
                                  formatLocation(loc)
                                )}
                              </span>
                            ))}
                          </div>
                        ) : null}
                        {(prob.affected_claim_ids.length > 0 || prob.affected_truth_ids.length > 0) ? (
                          <div className="problem-tags">
                            {prob.affected_claim_ids.map((id) => (
                              <span className="problem-tag" key={id}>Claim</span>
                            ))}
                            {prob.affected_truth_ids.map((id) => (
                              <span className="problem-tag" key={id}>Truth</span>
                            ))}
                          </div>
                        ) : null}
                      </div>
                    ))}
                  </div>
                </div>

                {/* Decision area */}
                <div className="decision-actions">
                  <div className="decision-comment-area">
                    <textarea
                      value={commentDrafts[pkg.package_id] ?? ""}
                      onChange={(e) =>
                        setCommentDrafts((c) => ({ ...c, [pkg.package_id]: e.target.value }))
                      }
                      placeholder="Spezifizierung oder Begründung (optional für annehmen/ablehnen, erforderlich für spezifizieren)"
                      rows={2}
                    />
                  </div>
                  <div className="decision-buttons">
                    <button
                      className="btn btn-success btn-sm"
                      disabled={packageDecisionSubmitting === pkg.package_id}
                      onClick={() => void handleDecision(pkg.package_id, "accept")}
                    >
                      {packageDecisionSubmitting === pkg.package_id ? "…" : "Annehmen"}
                    </button>
                    <button
                      className="btn btn-danger btn-sm"
                      disabled={packageDecisionSubmitting === pkg.package_id}
                      onClick={() => void handleDecision(pkg.package_id, "reject")}
                    >
                      Ablehnen
                    </button>
                    <button
                      className="btn btn-primary btn-sm"
                      disabled={packageDecisionSubmitting === pkg.package_id}
                      onClick={() => void handleDecision(pkg.package_id, "specify")}
                    >
                      Spezifizieren
                    </button>
                  </div>
                </div>

                {/* Writeback actions */}
                <div className="writeback-actions">
                  <button
                    className="btn btn-secondary btn-sm"
                    disabled={approvalSubmitting === "create:confluence_page_update"}
                    onClick={() => void handleCreateApproval(pkg, "confluence_page_update")}
                  >
                    Confluence-Freigabe
                  </button>
                  <button
                    className="btn btn-secondary btn-sm"
                    disabled={approvalSubmitting === "create:jira_ticket_create"}
                    onClick={() => void handleCreateApproval(pkg, "jira_ticket_create")}
                  >
                    Jira-Freigabe
                  </button>
                </div>
              </article>
            ))}
          </div>
        ))
      )}
    </>
  );
}
