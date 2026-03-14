import { useMemo } from "react";
import type { AuditRun } from "../types";

const SRC: Record<string, { icon: string; label: string; cls: string }> = {
  github_file:     { icon: "⌨", label: "Code",       cls: "src-code" },
  confluence_page: { icon: "📄", label: "Confluence", cls: "src-confluence" },
  metamodel:       { icon: "🔷", label: "Metamodell", cls: "src-metamodel" },
  local_doc:       { icon: "📋", label: "Lokal",      cls: "src-local" },
  jira_ticket:     { icon: "🎫", label: "Jira",       cls: "src-jira" },
  user_truth:      { icon: "✦",  label: "User",       cls: "src-user" },
};
function SrcBadge({ type }: { type: string }) {
  const s = SRC[type] ?? { icon: "•", label: type, cls: "src-local" };
  return <span className={`src-badge ${s.cls}`}>{s.icon} {s.label}</span>;
}

function fmtTs(v?: string | null) { if (!v) return "–"; return new Intl.DateTimeFormat("de-DE", { dateStyle: "short", timeStyle: "short" }).format(new Date(v)); }

type Props = { run: AuditRun | null; runs: AuditRun[] };

export function HistoryPanel({ run }: Props) {
  const decidedPkgs = useMemo(() => (run?.decision_packages ?? []).filter(p => p.decision_state !== "open"), [run?.decision_packages]);
  const resolvedFindings = useMemo(() => (run?.findings ?? []).filter(f => f.resolution_state && f.resolution_state !== "open"), [run?.findings]);
  const truths = useMemo(() => (run?.truths ?? []).filter(t => t.truth_status === "active"), [run?.truths]);
  const log = useMemo(() => [...(run?.analysis_log ?? [])].reverse(), [run?.analysis_log]);
  const changes = useMemo(() => [...(run?.implemented_changes ?? [])].reverse(), [run?.implemented_changes]);
  const doneApps = useMemo(() => (run?.approval_requests ?? []).filter(a => ["executed","rejected","cancelled"].includes(a.status)), [run?.approval_requests]);
  const claimGroups = useMemo(() => { const m = new Map<string,number>(); (run?.claims ?? []).forEach(c => m.set(c.source_type, (m.get(c.source_type) ?? 0) + 1)); return [...m.entries()]; }, [run?.claims]);

  if (!run) return <div className="empty"><div className="empty-icon">📋</div><strong>Kein Run ausgewählt</strong><p>Wähle einen Audit-Run in der Sidebar.</p></div>;

  return <>
    {/* Summary */}
    <section className="hsection">
      <h2 className="hsection-title">Run-Zusammenfassung</h2>
      <div className="hgrid">
        <div className="hstat"><span className="hstat-val">{run.findings.length}</span><span className="hstat-label">Findings</span></div>
        <div className="hstat"><span className="hstat-val">{run.decision_packages.length}</span><span className="hstat-label">Pakete</span></div>
        <div className="hstat"><span className="hstat-val">{run.claims.length}</span><span className="hstat-label">Claims</span></div>
        <div className="hstat"><span className="hstat-val">{truths.length}</span><span className="hstat-label">Truths</span></div>
        <div className="hstat"><span className="hstat-val">{changes.length}</span><span className="hstat-label">Umgesetzt</span></div>
      </div>
      {run.summary && <p className="text-secondary">{run.summary}</p>}
    </section>

    {/* Decided packages */}
    {decidedPkgs.length > 0 && <section className="hsection">
      <h2 className="hsection-title">Entschiedene Pakete <span className="hsection-count">{decidedPkgs.length}</span></h2>
      {decidedPkgs.map(p => (
        <div className="hitem" key={p.package_id}>
          <div className="hitem-head">
            <span className={`badge badge-${p.severity_summary}`}>{p.severity_summary}</span>
            <span className="badge badge-cat">{p.category}</span>
            <span className={`badge badge-${p.decision_state}`}>{p.decision_state}</span>
          </div>
          <strong>{p.title}</strong>
          <p>{p.scope_summary}</p>
        </div>
      ))}
    </section>}

    {/* Resolved Findings */}
    {resolvedFindings.length > 0 && <section className="hsection">
      <h2 className="hsection-title">Bewertete Findings <span className="hsection-count">{resolvedFindings.length}</span></h2>
      {resolvedFindings.map(f => (
        <div className="hitem" key={f.finding_id}>
          <div className="hitem-head">
            <span className={`badge badge-${f.severity}`}>{f.severity}</span>
            <span className="badge badge-cat">{f.category}</span>
            <span className={`badge badge-${f.resolution_state}`}>{f.resolution_state}</span>
          </div>
          <strong>{f.title}</strong>
          <p>{f.summary}</p>
        </div>
      ))}
    </section>}

    {/* Implemented changes */}
    {changes.length > 0 && <section className="hsection">
      <h2 className="hsection-title">Vollzugsledger <span className="hsection-count">{changes.length}</span></h2>
      {changes.map(c => (
        <div className="hitem" key={c.change_id}>
          <div className="hitem-head">
            <span className="badge badge-cat">{c.change_type === "confluence_page_updated" ? "📄 Confluence" : "🎫 Jira"}</span>
            <span className={`badge badge-${c.status === "applied" ? "completed" : "failed"}`}>{c.status}</span>
            <span className="text-xs text-muted" style={{ marginLeft: "auto" }}>{fmtTs(c.created_at)}</span>
          </div>
          <strong>{c.title}</strong>
          <p>{c.summary}</p>
        </div>
      ))}
    </section>}

    {/* Resolved approvals */}
    {doneApps.length > 0 && <section className="hsection">
      <h2 className="hsection-title">Abgeschlossene Freigaben <span className="hsection-count">{doneApps.length}</span></h2>
      {doneApps.map(a => (
        <div className="hitem" key={a.approval_request_id}>
          <div className="hitem-head">
            <span className="badge badge-cat">{a.target_type === "confluence_page_update" ? "📄 Confluence" : "🎫 Jira"}</span>
            <span className={`badge badge-${a.status}`}>{a.status}</span>
          </div>
          <strong>{a.title}</strong>
          {a.decision_comment && <p>„{a.decision_comment}"</p>}
        </div>
      ))}
    </section>}

    {/* Truths */}
    {truths.length > 0 && <section className="hsection">
      <h2 className="hsection-title">Truth Ledger <span className="hsection-count">{truths.length} aktiv</span></h2>
      {truths.map(t => (
        <div className="truth" key={t.truth_id}>
          <strong style={{ fontSize: 12 }}>{t.canonical_key}</strong>
          <div className="text-xs text-muted">{t.subject_key} · {t.predicate} = {t.normalized_value}</div>
        </div>
      ))}
    </section>}

    {/* Claims */}
    {claimGroups.length > 0 && <section className="hsection">
      <h2 className="hsection-title">Claim-Index <span className="hsection-count">{run.claims.length}</span></h2>
      <div className="hgrid">
        {claimGroups.map(([st, n]) => (
          <div className="hstat" key={st}><SrcBadge type={st} /><span className="hstat-val" style={{ marginTop: 4 }}>{n}</span></div>
        ))}
      </div>
    </section>}

    {/* Log */}
    {log.length > 0 && <section className="hsection">
      <h2 className="hsection-title">Aktivitätslog <span className="hsection-count">{log.length}</span></h2>
      {log.slice(0, 30).map(e => (
        <div className="log-entry" key={e.log_id}>
          <span className="log-ts">{fmtTs(e.created_at)}</span>
          <div className="log-body">
            <span className={`log-level log-${e.level}`}>{e.level}</span> <strong>{e.title}</strong>
            <p className="text-xs text-muted mt-xs">{e.message}</p>
          </div>
        </div>
      ))}
    </section>}

    {/* Snapshots */}
    {run.source_snapshots.length > 0 && <section className="hsection">
      <h2 className="hsection-title">Snapshots <span className="hsection-count">{run.source_snapshots.length}</span></h2>
      {run.source_snapshots.map(s => (
        <div className="snapshot" key={s.snapshot_id}>
          <SrcBadge type={s.source_type} />
          <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontSize: 11 }}>{s.source_id}</span>
          <span className="text-xs text-muted">{s.revision_id ? `Rev ${s.revision_id}` : s.content_hash || "–"}</span>
        </div>
      ))}
    </section>}
  </>;
}
