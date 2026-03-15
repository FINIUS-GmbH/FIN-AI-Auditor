import { useMemo } from "react";
import type { AuditRun } from "../types";

type TruthsViewProps = {
  run: AuditRun | null;
};

const SOURCE_ICONS: Record<string, { icon: string; label: string }> = {
  github_file: { icon: "⌨", label: "Code" },
  confluence_page: { icon: "📄", label: "Confluence" },
  metamodel: { icon: "🔷", label: "Metamodell" },
  local_doc: { icon: "📋", label: "Lokal" },
  jira_ticket: { icon: "🎫", label: "Jira" },
  user_truth: { icon: "✦", label: "Nutzer" },
};

function srcLabel(st: string): string {
  return SOURCE_ICONS[st]?.icon + " " + (SOURCE_ICONS[st]?.label ?? st);
}

export function TruthsView({ run }: TruthsViewProps): JSX.Element {
  const activeTruths = useMemo(
    () => (run?.truths ?? []).filter((t) => t.truth_status === "active"),
    [run?.truths]
  );
  const allTruths = run?.truths ?? [];
  const schemaTruths = run?.schema_truths ?? [];
  const atomicFacts = run?.atomic_facts ?? [];
  const claims = run?.claims ?? [];

  const claimsBySource = useMemo(() => {
    const groups = new Map<string, number>();
    for (const c of claims) {
      groups.set(c.source_type, (groups.get(c.source_type) ?? 0) + 1);
    }
    return [...groups.entries()];
  }, [claims]);

  if (!run) {
    return (
      <div className="empty-state">
        <div className="empty-state-icon">📜</div>
        <strong>Kein Run ausgewählt</strong>
        <p>Wähle einen Audit-Run, um das Wahrheitsregister und den Behauptungsindex zu sehen.</p>
      </div>
    );
  }

  return (
    <>
      {/* Claims overview */}
      <div className="section-head">
        <div>
          <h2>Behauptungsindex</h2>
          <p>{claims.length} extrahierte Behauptungen aus allen Quellen</p>
        </div>
      </div>

      <div className="metrics-grid" style={{ marginBottom: "var(--space-2xl)" }}>
        {claimsBySource.map(([sourceType, count]) => (
          <div className="metric-card mc-blue" key={sourceType}>
            <span className="metric-label">{srcLabel(sourceType)}</span>
            <span className="metric-value">{count}</span>
          </div>
        ))}
      </div>

      {/* Sample claims */}
      <div className="ledger-section">
        <div className="ledger-section-head">
          <h3>Behauptungen (letzte 50)</h3>
          <span className="ledger-count">{claims.length} gesamt</span>
        </div>
        {claims.length === 0 ? (
          <div className="ledger-hint">Noch keine Behauptungen extrahiert.</div>
        ) : (
          <div className="ledger-list">
            {claims.slice(0, 50).map((claim) => {
              const src = SOURCE_ICONS[claim.source_type];
              return (
                <div className="claim-item" key={claim.claim_id}>
                  <span
                    style={{
                      display: "inline-flex",
                      alignItems: "center",
                      gap: 4,
                      padding: "1px 7px",
                      borderRadius: 4,
                      fontSize: 10,
                      fontWeight: 600,
                      marginRight: 8,
                      background:
                        claim.source_type === "github_file"
                          ? "rgba(99,102,241,0.12)"
                          : claim.source_type === "confluence_page"
                          ? "rgba(16,185,129,0.12)"
                          : claim.source_type === "metamodel"
                          ? "rgba(75,123,236,0.12)"
                          : "rgba(100,116,139,0.12)",
                      color:
                        claim.source_type === "github_file"
                          ? "#818cf8"
                          : claim.source_type === "confluence_page"
                          ? "#34d399"
                          : claim.source_type === "metamodel"
                          ? "#93b4f5"
                          : "#94a3b8",
                    }}
                  >
                    {src?.icon ?? "•"} {src?.label ?? claim.source_type}
                  </span>
                  <strong>{claim.subject_key}</strong> · {claim.predicate} · {claim.normalized_value}
                  <span className="text-muted" style={{ marginLeft: 8 }}>
                    ({Math.round(claim.confidence * 100)}%)
                  </span>
                  <div className="text-xs text-muted mt-xs">
                    Behauptungsstatus: {claim.assertion_status ?? "asserted"} · Autorität: {claim.source_authority ?? "heuristic"}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Truth Ledger */}
      <div className="ledger-section mt-xl">
        <div className="ledger-section-head">
          <h3>Wahrheitsregister</h3>
          <span className="ledger-count">{activeTruths.length} aktiv / {allTruths.length} gesamt</span>
        </div>
        {allTruths.length === 0 ? (
          <div className="ledger-hint">
            Noch keine Wahrheiten definiert. Durch „Spezifizieren" bei Entscheidungspaketen
            werden Nutzer-Wahrheiten mit fachlichen Implikationen gespeichert.
          </div>
        ) : (
          <div className="ledger-list">
            {allTruths.map((truth) => (
              <div
                className="truth-item"
                key={truth.truth_id}
                style={{
                  opacity: truth.truth_status !== "active" ? 0.5 : 1,
                }}
              >
                <div className="flex-row justify-between gap-sm" style={{ marginBottom: 4 }}>
                  <strong style={{ fontSize: 13, color: "var(--text-primary)" }}>
                    {truth.canonical_key}
                  </strong>
                  <span className={`badge badge-state badge-${truth.truth_status === "active" ? "accepted" : truth.truth_status === "superseded" ? "superseded" : "rejected"}`}>
                    {truth.truth_status}
                  </span>
                </div>
                <div>
                  {truth.subject_key} · {truth.predicate} = <strong>{truth.normalized_value}</strong>
                </div>
                <div className="text-xs text-muted mt-xs">
                  Quelle: {truth.source_kind} · Scope: {truth.scope_kind}/{truth.scope_key}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="ledger-section mt-xl">
        <div className="ledger-section-head">
          <h3>Schema-Wahrheitsregister</h3>
          <span className="ledger-count">{schemaTruths.length}</span>
        </div>
        {schemaTruths.length === 0 ? (
          <div className="ledger-hint">
            Noch keine Schema-Ziele registriert. Nach einem Lauf erscheinen hier bestaetigte, beobachtete
            und nur aus Code inferierte Persistenzziele.
          </div>
        ) : (
          <div className="ledger-list">
            {schemaTruths.map((entry) => (
              <div className="truth-item" key={entry.schema_truth_id}>
                <div className="flex-row justify-between gap-sm" style={{ marginBottom: 4 }}>
                  <strong style={{ fontSize: 13, color: "var(--text-primary)" }}>
                    {entry.target_label}
                  </strong>
                  <span className={`badge badge-state badge-${entry.status === "confirmed_ssot" ? "accepted" : entry.status === "rejected_target" ? "rejected" : "open"}`}>
                    {entry.status}
                  </span>
                </div>
                <div>
                  {entry.schema_kind} · {entry.source_kind} · <strong>{entry.source_authority}</strong>
                </div>
                <div className="text-xs text-muted mt-xs">
                  Quellen: {entry.source_ids.slice(0, 3).join(", ") || "–"}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="ledger-section mt-xl">
        <div className="ledger-section-head">
          <h3>Atomare Fakten</h3>
          <span className="ledger-count">{atomicFacts.length}</span>
        </div>
        {atomicFacts.length === 0 ? (
          <div className="ledger-hint">
            Noch keine atomaren Fakten persistiert. Nach einem Analyse-Lauf erscheinen hier die kleinsten
            bewertbaren Sachverhalte inklusive Aktionsspur.
          </div>
        ) : (
          <div className="ledger-list">
            {atomicFacts.map((fact) => (
              <div className="truth-item" key={fact.atomic_fact_id}>
                <div className="flex-row justify-between gap-sm" style={{ marginBottom: 4 }}>
                  <strong style={{ fontSize: 13, color: "var(--text-primary)" }}>
                    {fact.fact_key}
                  </strong>
                  <span className={`badge badge-state badge-${fact.status === "confirmed" ? "accepted" : fact.status === "resolved" ? "superseded" : "open"}`}>
                    {fact.status}
                  </span>
                </div>
                <div>{fact.summary}</div>
                <div className="text-xs text-muted mt-xs">
                  Aktionsspur: <strong>{fact.action_lane}</strong> · Quellen: {fact.source_types.join(", ") || "–"}
                </div>
                <div className="text-xs text-muted mt-xs">
                  Pakete: {fact.related_package_ids.slice(0, 3).join(", ") || "–"} · Behauptungen: {fact.claim_ids.length} · Wahrheiten: {fact.truth_ids.length}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Snapshots */}
      <div className="ledger-section mt-xl">
        <div className="ledger-section-head">
          <h3>Quell-Schnappschüsse</h3>
          <span className="ledger-count">{run.source_snapshots.length}</span>
        </div>
        {run.source_snapshots.length === 0 ? (
          <div className="ledger-hint">Noch keine Snapshots vorhanden.</div>
        ) : (
          <div className="ledger-list">
            {run.source_snapshots.map((snap) => {
              const src = SOURCE_ICONS[snap.source_type];
              return (
                <div className="snapshot-item" key={snap.snapshot_id}>
                  <span
                    style={{
                      display: "inline-flex",
                      alignItems: "center",
                      gap: 4,
                      padding: "1px 6px",
                      borderRadius: 4,
                      fontSize: 10,
                      fontWeight: 600,
                      background:
                        snap.source_type === "github_file"
                          ? "rgba(99,102,241,0.12)"
                          : snap.source_type === "confluence_page"
                          ? "rgba(16,185,129,0.12)"
                          : snap.source_type === "metamodel"
                          ? "rgba(75,123,236,0.12)"
                          : "rgba(100,116,139,0.12)",
                      color:
                        snap.source_type === "github_file"
                          ? "#818cf8"
                          : snap.source_type === "confluence_page"
                          ? "#34d399"
                          : snap.source_type === "metamodel"
                          ? "#93b4f5"
                          : "#94a3b8",
                      flexShrink: 0,
                    }}
                  >
                    {src?.icon ?? "•"} {src?.label ?? snap.source_type}
                  </span>
                  <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {snap.source_id}
                  </span>
                  <span className="text-muted">
                    {snap.revision_id ? `Rev ${snap.revision_id}` : snap.content_hash || "–"}
                  </span>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </>
  );
}
