import type { AuditRun, BootstrapData } from "../types";

type DashboardViewProps = {
  runs: AuditRun[];
  selectedRun: AuditRun | null;
  bootstrap: BootstrapData | null;
  onNavigate: (view: string) => void;
};

function formatTs(value?: string | null): string {
  if (!value) return "–";
  return new Intl.DateTimeFormat("de-DE", {
    dateStyle: "short",
    timeStyle: "short",
  }).format(new Date(value));
}

export function DashboardView({
  runs,
  selectedRun,
  bootstrap,
  onNavigate,
}: DashboardViewProps): JSX.Element {
  const activeRuns = runs.filter((r) => r.status === "running" || r.status === "planned").length;
  const totalFindings = runs.reduce((sum, r) => sum + r.findings.length, 0);
  const totalPackages = runs.reduce((sum, r) => sum + r.decision_packages.length, 0);
  const pendingApprovals = runs.reduce(
    (sum, r) => sum + r.approval_requests.filter((a) => a.status === "pending").length,
    0
  );
  const totalClaims = selectedRun?.claims.length ?? 0;
  const totalTruths = selectedRun?.truths.filter((t) => t.truth_status === "active").length ?? 0;

  const recentFindings = selectedRun?.findings.slice(0, 5) ?? [];

  return (
    <>
      {/* Metrics */}
      <div className="metrics-grid">
        <button className="metric-card mc-blue" onClick={() => onNavigate("runs")} style={{ textAlign: "left", cursor: "pointer" }}>
          <span className="metric-label">Aktive Runs</span>
          <span className="metric-value">{activeRuns}</span>
          <span className="metric-hint">{runs.length} gesamt</span>
        </button>
        <button className="metric-card mc-amber" onClick={() => onNavigate("findings")} style={{ textAlign: "left", cursor: "pointer" }}>
          <span className="metric-label">Offene Befunde</span>
          <span className="metric-value">{totalFindings}</span>
          <span className="metric-hint">Über alle Runs</span>
        </button>
        <button className="metric-card mc-purple" onClick={() => onNavigate("packages")} style={{ textAlign: "left", cursor: "pointer" }}>
          <span className="metric-label">Entscheidungspakete</span>
          <span className="metric-value">{totalPackages}</span>
          <span className="metric-hint">Atomare UI-Einheiten</span>
        </button>
        <button className="metric-card mc-red" onClick={() => onNavigate("approvals")} style={{ textAlign: "left", cursor: "pointer" }}>
          <span className="metric-label">Offene Freigaben</span>
          <span className="metric-value">{pendingApprovals}</span>
          <span className="metric-hint">Ausstehend</span>
        </button>
        <button className="metric-card mc-green" onClick={() => onNavigate("truths")} style={{ textAlign: "left", cursor: "pointer" }}>
          <span className="metric-label">Behauptungen / Wahrheiten</span>
          <span className="metric-value">{totalClaims}</span>
          <span className="metric-hint">{totalTruths} aktive Wahrheiten</span>
        </button>
      </div>

      {/* Progress of selected run */}
      {selectedRun ? (
        <div className="progress-section">
          <div className="progress-header">
            <div className="progress-header-info">
              <h3>Pipeline-Fortschritt</h3>
              <p>
                {selectedRun.progress.phase_label} — {selectedRun.progress.current_activity}
              </p>
            </div>
            <span className="progress-pct">{selectedRun.progress.progress_pct}%</span>
          </div>
          <div className="progress-bar-wrap">
            <div
              className="progress-bar-fill"
              style={{ width: `${Math.max(0, Math.min(100, selectedRun.progress.progress_pct))}%` }}
            />
          </div>
          <div className="progress-steps">
            {selectedRun.progress.steps.map((step) => (
              <div className="progress-step" key={step.step_key}>
                <div className="progress-step-head">
                  <strong>{step.label}</strong>
                  <span className={`step-indicator step-${step.status}`}>
                    {step.status === "completed" ? "✓" : step.status === "failed" ? "✗" : step.status === "running" ? "●" : ""}
                  </span>
                </div>
                <div className="progress-step-detail">
                  {step.detail || "Wartend"}
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {/* Recent findings */}
      <div className="section-head">
        <div>
          <h2>Letzte Befunde</h2>
          <p>Aus dem aktuell ausgewählten Run</p>
        </div>
        {recentFindings.length > 0 ? (
          <button className="btn btn-secondary btn-sm" onClick={() => onNavigate("findings")}>
            Alle anzeigen
          </button>
        ) : null}
      </div>

      {recentFindings.length === 0 ? (
        <div className="empty-state">
          <div className="empty-state-icon">🔍</div>
          <strong>Noch keine Findings</strong>
          <p>Starte einen Audit-Run, um Soll/Ist-Abweichungen zu erkennen.</p>
        </div>
      ) : (
        <div className="run-list">
          {recentFindings.map((f) => (
            <div className="finding-card" key={f.finding_id}>
              <div className="finding-card-header">
                <span className={`badge badge-${f.severity}`}>{f.severity}</span>
                <span className="badge badge-category">{f.category}</span>
                <span className={`badge badge-state badge-${f.resolution_state || "open"}`}>
                  {f.resolution_state || "open"}
                </span>
              </div>
              <div className="finding-card-title">{f.title}</div>
              <div className="finding-card-summary">{f.summary}</div>
            </div>
          ))}
        </div>
      )}

      {/* System info */}
      {bootstrap ? (
        <div className="policy-box mt-xl">
          <strong>Zugriffsmodus: {bootstrap.resource_access_policy.mode === "read_only" ? "Read-only" : bootstrap.resource_access_policy.mode}</strong>
          <p>{bootstrap.resource_access_policy.summary}</p>
        </div>
      ) : null}
    </>
  );
}
