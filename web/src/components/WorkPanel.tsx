import { useMemo, useState } from "react";
import type {
  AuditRun, AuditFinding, AuditLocation, DecisionPackage,
  SourceProfile, WritebackApprovalRequest, ConfluencePatchPreview,
} from "../types";

/* Source badge helper */
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
function fmtLoc(l: AuditLocation) {
  const p = [l.title]; if (l.path_hint) p.push(l.path_hint);
  if (l.position?.anchor_value) p.push(l.position.anchor_value);
  return p.join(" · ");
}
function strings(v: unknown): string[] { return Array.isArray(v) ? v.filter((x): x is string => typeof x === "string" && x.trim().length > 0) : []; }
function getPatch(r: WritebackApprovalRequest): ConfluencePatchPreview | null { const v = r.metadata?.confluence_patch_preview; return v && typeof v === "object" ? v as ConfluencePatchPreview : null; }

type Props = {
  run: AuditRun | null;
  sourceProfile: SourceProfile;
  commentSubmitting: boolean; commentError: string;
  pkgSubmitting: string; pkgError: string;
  approvalSubmitting: string; approvalError: string;
  execSubmitting: string; execError: string;
  onComment: (t: string) => Promise<void>;
  onPkgDecision: (id: string, a: "accept"|"reject"|"specify", c?: string) => Promise<void>;
  onCreateApproval: (p: { target_type: "confluence_page_update"|"jira_ticket_create"; title: string; summary: string; target_url?: string|null; related_package_ids: string[]; related_finding_ids: string[]; payload_preview: string[] }) => Promise<void>;
  onResolveApproval: (id: string, d: "approve"|"reject"|"cancel", c?: string) => Promise<void>;
  onRecordConfluence: (p: { approval_request_id: string; page_title: string; page_url: string; changed_sections: string[]; change_summary: string[]; related_finding_ids: string[] }) => Promise<void>;
  onRecordJira: (p: { approval_request_id: string; ticket_key: string; ticket_url: string; related_finding_ids: string[] }) => Promise<void>;
  onExecConfluence: (id: string) => Promise<void>;
  onExecJira: (id: string) => Promise<void>;
};

export function WorkPanel({ run, sourceProfile: sp, commentSubmitting: cs, commentError: ce, pkgSubmitting: ps, pkgError: pe, approvalSubmitting: as_, approvalError: ae, execSubmitting: es, execError: ee, onComment, onPkgDecision, onCreateApproval, onResolveApproval, onRecordConfluence, onRecordJira, onExecConfluence, onExecJira }: Props) {
  const [drafts, setDrafts] = useState<Record<string,string>>({});
  const [appDrafts, setAppDrafts] = useState<Record<string,string>>({});

  const openPkgs = useMemo(() => (run?.decision_packages ?? []).filter(p => p.decision_state === "open"), [run?.decision_packages]);
  const pendApps = useMemo(() => (run?.approval_requests ?? []).filter(a => a.status === "pending"), [run?.approval_requests]);
  const apprvdApps = useMemo(() => (run?.approval_requests ?? []).filter(a => a.status === "approved"), [run?.approval_requests]);
  const pkgFindIds = useMemo(() => { const s = new Set<string>(); openPkgs.forEach(p => p.related_finding_ids.forEach(id => s.add(id))); return s; }, [openPkgs]);
  const soloFindings = useMemo(() => (run?.findings ?? []).filter(f => (!f.resolution_state || f.resolution_state === "open") && !pkgFindIds.has(f.finding_id)), [run?.findings, pkgFindIds]);
  const total = openPkgs.length + soloFindings.length + pendApps.length + apprvdApps.length;

  async function actFinding(fid: string, a: "accept"|"dismiss") { const d = drafts[fid]?.trim(); await onComment(d ? `[${a === "accept" ? "ANNEHMEN" : "ABLEHNEN"}] Finding ${fid}: ${d}` : `[${a === "accept" ? "ANNEHMEN" : "ABLEHNEN"}] Finding ${fid}`); setDrafts(c => ({ ...c, [fid]: "" })); }
  async function actPkg(pid: string, a: "accept"|"reject"|"specify") { await onPkgDecision(pid, a, drafts[pid]?.trim() || undefined); setDrafts(c => ({ ...c, [pid]: "" })); }
  async function reqApproval(pkg: DecisionPackage, t: "confluence_page_update"|"jira_ticket_create") { await onCreateApproval({ target_type: t, title: `${t === "confluence_page_update" ? "Confluence" : "Jira"}-Writeback: ${pkg.title}`, summary: `Freigabeanfrage.`, target_url: t === "confluence_page_update" ? sp.confluence_url : sp.jira_url, related_package_ids: [pkg.package_id], related_finding_ids: pkg.related_finding_ids, payload_preview: [`Scope: ${pkg.scope_summary}`, `Empfehlung: ${pkg.recommendation_summary}`] }); }

  if (!run) return <div className="empty"><div className="empty-icon">🛡</div><strong>Willkommen beim FIN-AI Auditor</strong><p>Erstelle einen Audit-Run über den Button in der Sidebar, um Soll/Ist-Abweichungen zu analysieren.</p></div>;
  if (total === 0 && run.status === "completed") return <div className="empty"><div className="empty-icon">✅</div><strong>Alle Bewertungen abgeschlossen</strong><p>Keine offenen Widersprüche. Im Verlauf findest du die historisierten Entscheidungen.</p></div>;
  if (total === 0) return <div className="empty"><div className="empty-icon">⏳</div><strong>Analyse läuft…</strong><p>Sobald Widersprüche erkannt werden, erscheinen sie hier zur Bewertung.</p></div>;

  return <>
    {pe && <div className="error-box">{pe}</div>}
    {ce && <div className="error-box">{ce}</div>}
    {ae && <div className="error-box">{ae}</div>}
    {ee && <div className="error-box">{ee}</div>}

    {/* OPEN PACKAGES */}
    {openPkgs.length > 0 && <section>
      <div className="section-head"><h2>Zur Bewertung</h2><span className="section-count">{openPkgs.length} offene Widersprüche</span></div>
      {openPkgs.map(pkg => (
        <article className="wc" key={pkg.package_id}>
          <div className="wc-badges"><span className={`badge badge-${pkg.severity_summary}`}>{pkg.severity_summary}</span><span className="badge badge-cat">{pkg.category}</span></div>
          <h3 className="wc-title">{pkg.title}</h3>
          <p className="wc-scope">{pkg.scope_summary}</p>
          <div className="wc-evidence">
            <div className="wc-label">Evidenz</div>
            {pkg.problem_elements.map((el, i) => (
              <div className="ev-block" key={i}>
                <div className="ev-head"><span className={`badge badge-${el.severity}`} style={{ fontSize: 10 }}>{el.severity}</span><span className="text-xs text-muted">{Math.round(el.confidence * 100)}% Konfidenz</span></div>
                <p className="ev-explain">{el.short_explanation}</p>
                <div className="ev-locs">
                  {el.evidence_locations.map(loc => (
                    <div className="ev-loc" key={loc.location_id || `${loc.source_id}-${loc.title}`}>
                      <SrcBadge type={loc.source_type} />
                      {loc.url ? <a href={loc.url} target="_blank" rel="noreferrer">{fmtLoc(loc)}</a> : <span>{fmtLoc(loc)}</span>}
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
          {strings(pkg.metadata?.delta_summary).length > 0 && <div className="wc-context"><div className="wc-label">Kontext</div><ul>{strings(pkg.metadata?.delta_summary).map((h,i) => <li key={i}>{h}</li>)}</ul></div>}
          <div className="wc-rec"><div className="wc-label">Empfehlung</div><div className="rec-text">{pkg.recommendation_summary}</div></div>
          <div className="wc-actions">
            <textarea value={drafts[pkg.package_id] ?? ""} onChange={e => setDrafts(c => ({ ...c, [pkg.package_id]: e.target.value }))} placeholder="Kommentar, Neubewertung oder Begründung…" />
            <div className="wc-btns">
              <button className="btn btn-accept" disabled={ps === pkg.package_id || cs} onClick={() => void actPkg(pkg.package_id, "accept")}>✓ Annehmen</button>
              <button className="btn btn-reject" disabled={ps === pkg.package_id || cs} onClick={() => void actPkg(pkg.package_id, "reject")}>✗ Ablehnen</button>
              <button className="btn btn-specify" disabled={ps === pkg.package_id || cs} onClick={() => void actPkg(pkg.package_id, "specify")}>Spezifizieren</button>
            </div>
            <div className="wc-writeback">
              <button className="btn btn-ghost btn-sm" disabled={as_ === "c:confluence_page_update"} onClick={() => void reqApproval(pkg, "confluence_page_update")}>📄 Confluence-Freigabe</button>
              <button className="btn btn-ghost btn-sm" disabled={as_ === "c:jira_ticket_create"} onClick={() => void reqApproval(pkg, "jira_ticket_create")}>🎫 Jira-Freigabe</button>
            </div>
          </div>
        </article>
      ))}
    </section>}

    {/* STANDALONE FINDINGS */}
    {soloFindings.length > 0 && <section>
      <div className="section-head"><h2>Einzelne Findings</h2><span className="section-count">{soloFindings.length}</span></div>
      {soloFindings.map(f => (
        <article className="wc" key={f.finding_id}>
          <div className="wc-badges"><span className={`badge badge-${f.severity}`}>{f.severity}</span><span className="badge badge-cat">{f.category}</span></div>
          <h3 className="wc-title">{f.title}</h3>
          <p className="wc-scope">{f.summary}</p>
          <div className="wc-evidence">
            <div className="wc-label">Evidenz</div>
            <div className="ev-block">
              <p className="ev-explain">{f.summary}</p>
              <div className="ev-locs">
                {f.locations.map(loc => <div className="ev-loc" key={loc.location_id || `${loc.source_id}-${loc.title}`}><SrcBadge type={loc.source_type} />{loc.url ? <a href={loc.url} target="_blank" rel="noreferrer">{fmtLoc(loc)}</a> : <span>{fmtLoc(loc)}</span>}</div>)}
              </div>
            </div>
          </div>
          {f.recommendation && <div className="wc-rec"><div className="wc-label">Empfehlung</div><div className="rec-text">{f.recommendation}</div></div>}
          <div className="wc-actions">
            <textarea value={drafts[f.finding_id] ?? ""} onChange={e => setDrafts(c => ({ ...c, [f.finding_id]: e.target.value }))} placeholder="Kommentar, Neubewertung oder Begründung…" />
            <div className="wc-btns">
              <button className="btn btn-accept" disabled={cs} onClick={() => void actFinding(f.finding_id, "accept")}>✓ Annehmen</button>
              <button className="btn btn-reject" disabled={cs} onClick={() => void actFinding(f.finding_id, "dismiss")}>✗ Ablehnen</button>
            </div>
          </div>
        </article>
      ))}
    </section>}

    {/* PENDING APPROVALS */}
    {pendApps.length > 0 && <section>
      <div className="section-head"><h2>Ausstehende Freigaben</h2><span className="section-count">{pendApps.length}</span></div>
      {pendApps.map(req => {
        const patch = getPatch(req);
        return (
          <article className="wc" key={req.approval_request_id}>
            <div className="wc-badges"><span className="badge badge-cat">{req.target_type === "confluence_page_update" ? "📄 Confluence" : "🎫 Jira"}</span><span className="badge badge-pending">ausstehend</span></div>
            <h3 className="wc-title">{req.title}</h3>
            <p className="wc-scope">{req.summary}</p>
            {req.payload_preview.length > 0 && <ul className="wc-preview">{req.payload_preview.map((x,i) => <li key={i}>{x}</li>)}</ul>}
            {patch && <div className="patch"><div className="patch-title">{patch.page_title} · {patch.execution_ready ? "✓ bereit" : "✗ blockiert"}</div>{patch.operations.slice(0,4).map(op => <div className="patch-op" key={op.operation_id}><strong>{op.section_path}</strong> · {op.marker_kind} · {op.proposed_statement}</div>)}</div>}
            <div className="wc-actions">
              <textarea value={appDrafts[req.approval_request_id] ?? ""} onChange={e => setAppDrafts(c => ({ ...c, [req.approval_request_id]: e.target.value }))} placeholder="Optionaler Kommentar…" />
              <div className="wc-btns">
                <button className="btn btn-accept" disabled={as_ === req.approval_request_id} onClick={() => void onResolveApproval(req.approval_request_id, "approve", appDrafts[req.approval_request_id])}>✓ Genehmigen</button>
                <button className="btn btn-reject" disabled={as_ === req.approval_request_id} onClick={() => void onResolveApproval(req.approval_request_id, "reject", appDrafts[req.approval_request_id])}>✗ Ablehnen</button>
                <button className="btn btn-outline" disabled={as_ === req.approval_request_id} onClick={() => void onResolveApproval(req.approval_request_id, "cancel")}>Stornieren</button>
              </div>
            </div>
          </article>
        );
      })}
    </section>}

    {/* APPROVED → EXECUTE */}
    {apprvdApps.length > 0 && <section>
      <div className="section-head"><h2>Bereit zur Ausführung</h2><span className="section-count">{apprvdApps.length} genehmigt</span></div>
      {apprvdApps.map(req => {
        const isC = req.target_type === "confluence_page_update";
        const patch = getPatch(req);
        return (
          <article className="wc" key={req.approval_request_id}>
            <div className="wc-badges"><span className="badge badge-cat">{isC ? "📄 Confluence" : "🎫 Jira"}</span><span className="badge badge-approved">genehmigt</span></div>
            <h3 className="wc-title">{req.title}</h3>
            <div className="wc-actions">
              <div className="wc-btns">
                <button className="btn btn-primary" disabled={es === req.approval_request_id} onClick={() => void (isC ? onExecConfluence : onExecJira)(req.approval_request_id)}>Extern ausführen</button>
                <button className="btn btn-outline" disabled={es === req.approval_request_id} onClick={() => { if (isC) onRecordConfluence({ approval_request_id: req.approval_request_id, page_title: patch?.page_title||req.title, page_url: patch?.page_url||req.target_url||sp.confluence_url, changed_sections: patch?.changed_sections??[], change_summary: patch?.change_summary??req.payload_preview, related_finding_ids: req.related_finding_ids }); else onRecordJira({ approval_request_id: req.approval_request_id, ticket_key: "", ticket_url: req.target_url||sp.jira_url, related_finding_ids: req.related_finding_ids }); }}>Lokal verbuchen</button>
              </div>
            </div>
          </article>
        );
      })}
    </section>}
  </>;
}
