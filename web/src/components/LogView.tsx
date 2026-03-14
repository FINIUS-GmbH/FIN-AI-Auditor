import { useMemo, useState, type FormEvent } from "react";
import type { AuditRun, AuditAnalysisLogEntry } from "../types";

type LogViewProps = {
  run: AuditRun | null;
  decisionCommentSubmitting: boolean;
  decisionCommentError: string;
  onDecisionCommentSubmit: (commentText: string) => Promise<void>;
};

function formatTs(value?: string | null): string {
  if (!value) return "–";
  return new Intl.DateTimeFormat("de-DE", { dateStyle: "short", timeStyle: "short" }).format(new Date(value));
}

function sourceLabel(entry: AuditAnalysisLogEntry): string {
  switch (entry.source_type) {
    case "system": return "System";
    case "pipeline": return "Pipeline";
    case "decision_comment": return "Kommentar";
    case "truth_update": return "Wahrheit";
    case "impact_analysis": return "Impact";
    case "recommendation_regeneration": return "Neugewichtung";
    default: return entry.source_type;
  }
}

export function LogView({
  run,
  decisionCommentSubmitting,
  decisionCommentError,
  onDecisionCommentSubmit,
}: LogViewProps): JSX.Element {
  const [commentDraft, setCommentDraft] = useState("");
  const [filterType, setFilterType] = useState("Alle");

  const logEntries = useMemo(() => {
    let entries = [...(run?.analysis_log ?? [])].reverse();
    if (filterType !== "Alle") {
      entries = entries.filter((e) => e.source_type === filterType);
    }
    return entries;
  }, [run?.analysis_log, filterType]);

  const sourceTypes = useMemo(() => {
    const types = new Set(run?.analysis_log.map((e) => e.source_type) ?? []);
    return ["Alle", ...types];
  }, [run?.analysis_log]);

  async function handleSubmit(e: FormEvent): Promise<void> {
    e.preventDefault();
    const text = commentDraft.trim();
    if (!text) return;
    await onDecisionCommentSubmit(text);
    setCommentDraft("");
  }

  if (!run) {
    return (
      <div className="empty-state">
        <div className="empty-state-icon">📋</div>
        <strong>Kein Run ausgewählt</strong>
        <p>Wähle einen Audit-Run, um das Aktivitätslog zu sehen.</p>
      </div>
    );
  }

  return (
    <>
      {/* Comment input */}
      <div className="comment-box">
        <h3>Kommentar / Spezifizierung</h3>
        <p>
          Kommentare fließen als User-Wahrheiten in den Truth Ledger ein und beeinflussen
          zukünftige Analysen.
        </p>
        {decisionCommentError ? <div className="error-box">{decisionCommentError}</div> : null}
        <form onSubmit={(e) => void handleSubmit(e)}>
          <textarea
            value={commentDraft}
            onChange={(e) => setCommentDraft(e.target.value)}
            placeholder="Fachliche Spezifizierung, Entscheidung oder Rückfrage…"
            rows={3}
          />
          <div className="form-actions mt-sm">
            <button type="submit" className="btn btn-primary btn-sm" disabled={decisionCommentSubmitting}>
              {decisionCommentSubmitting ? "Sende…" : "Absenden"}
            </button>
          </div>
        </form>
      </div>

      {/* Filter */}
      <div className="filter-bar">
        {sourceTypes.map((st) => (
          <button
            key={st}
            className={`filter-pill${filterType === st ? " active" : ""}`}
            onClick={() => setFilterType(st)}
          >
            {st === "Alle" ? `Alle (${run.analysis_log.length})` : st}
          </button>
        ))}
      </div>

      {/* Log entries */}
      {logEntries.length === 0 ? (
        <div className="empty-state">
          <div className="empty-state-icon">📋</div>
          <strong>Kein Aktivitätslog</strong>
          <p>
            {filterType !== "Alle"
              ? "Keine Einträge mit diesem Typ."
              : "Analyse-Aktivitäten erscheinen hier."}
          </p>
        </div>
      ) : (
        <div>
          {logEntries.map((entry) => (
            <div className="log-entry" key={entry.log_id}>
              <span className="log-time">{formatTs(entry.created_at)}</span>
              <div className="log-body">
                <div className="flex-row gap-sm" style={{ marginBottom: 4 }}>
                  <span className={`log-level log-level-${entry.level}`}>{entry.level}</span>
                  <span className="badge badge-category" style={{ fontSize: 10 }}>{sourceLabel(entry)}</span>
                </div>
                <strong>{entry.title}</strong>
                <p className="text-xs text-muted mt-xs">{entry.message}</p>
                {entry.impact_summary.length > 0 ? (
                  <ul className="evidence-list mt-xs">
                    {entry.impact_summary.map((s, i) => (
                      <li key={`${entry.log_id}-impact-${i}`}>{s}</li>
                    ))}
                  </ul>
                ) : null}
                {entry.derived_changes.length > 0 ? (
                  <div className="problem-tags mt-xs">
                    {entry.derived_changes.map((c, i) => (
                      <span className="problem-tag" key={`${entry.log_id}-change-${i}`}>{c}</span>
                    ))}
                  </div>
                ) : null}
              </div>
            </div>
          ))}
        </div>
      )}
    </>
  );
}
