import type { AuditRun } from "../types";

type RunCardProps = {
  run: AuditRun;
  isSelected: boolean;
  onSelect: (runId: string) => void;
};

function findingSummary(run: AuditRun): string {
  if (run.status === "failed") return "Fehlgeschlagen";
  if (run.status === "planned") return "Geplant";
  if (run.status === "running") return "Laeuft";
  return `${run.findings.length} Findings`;
}

function formatTimestamp(value?: string | null): string {
  if (!value) {
    return "gerade erstellt";
  }

  return new Intl.DateTimeFormat("de-DE", {
    dateStyle: "short",
    timeStyle: "short",
  }).format(new Date(value));
}

export function RunCard(props: RunCardProps): JSX.Element {
  const { run, isSelected, onSelect } = props;
  const repoLabel = run.target.local_repo_path || run.target.github_repo_url || "keine Repo-Quelle";
  const progressWidth = `${Math.max(0, Math.min(100, run.progress.progress_pct))}%`;

  return (
    <button
      type="button"
      className={`run-card ${isSelected ? "selected" : ""}`}
      onClick={() => onSelect(run.run_id)}
    >
      <div className="run-card-header">
        <span className={`status-pill status-${run.status}`}>{run.status}</span>
        <span className="run-card-meta">{findingSummary(run)}</span>
      </div>
      <div className="run-card-title">{repoLabel}</div>
      <div className="run-card-subtitle">
        Ref {run.target.github_ref} · {run.progress.phase_label} · Updated {formatTimestamp(run.updated_at)}
      </div>
      <div className="progress-strip" aria-hidden="true">
        <div className="progress-strip-fill" style={{ width: progressWidth }} />
      </div>
      <div className="run-card-progress-text">
        <span>{run.progress.progress_pct}%</span>
        <span>{run.progress.current_activity}</span>
      </div>
      <div className="run-card-footer">
        <span>{run.source_snapshots.length} Snapshots</span>
        <span>{run.finding_links.length} Links</span>
      </div>
      <div className="run-card-id">{run.run_id}</div>
    </button>
  );
}
