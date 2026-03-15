import { useEffect, useMemo, useState, type ReactNode } from "react";
import {
  createAuditRun, createWritebackApprovalRequest,
  executeConfluencePageWriteback, executeJiraTicketWriteback,
  getAtlassianAuthStatus, getBootstrapData, listAuditRuns,
  recordConfluencePageUpdate, recordJiraTicketCreated, resetAuditDatabase,
  resolveWritebackApprovalRequest, startAtlassianAuthorization,
  submitDecisionComment, submitPackageDecision, verifyConfluenceAccess,
} from "./api";
import RunModal from "./components/RunModal";
import type {
  AtlassianAuthStatus, AuditLocation, AuditRun, AuditTarget,
  BootstrapData, DecisionPackage, SourceProfile, WritebackApprovalRequest,
  ConfluencePatchPreview,
} from "./types";

/* ============================================================
   HELPERS
   ============================================================ */

function ts(v?: string | null): string {
  if (!v) return "–";
  return new Intl.DateTimeFormat("de-DE", { dateStyle: "short", timeStyle: "short" }).format(new Date(v));
}

const STATUS_DE: Record<string, string> = {
  planned: "Geplant", running: "Läuft", completed: "Abgeschlossen", failed: "Fehlgeschlagen",
  open: "Offen", pending: "Ausstehend", approved: "Genehmigt", rejected: "Abgelehnt",
  applied: "Umgesetzt", accepted: "Akzeptiert", specified: "Präzisiert",
  superseded: "Ersetzt", dismissed: "Verworfen", executed: "Ausgeführt", cancelled: "Storniert",
};
const SEVERITY_DE: Record<string, string> = {
  critical: "Kritisch", high: "Hoch", medium: "Mittel", low: "Gering",
};
const CATEGORY_DE: Record<string, string> = {
  contradiction: "⚠️ Widerspruch", gap: "💭 Lücke", inconsistency: "🔀 Inkonsistenz",
  missing_implementation: "❌ Fehlende Umsetzung", missing_documentation: "📝 Fehlende Doku",
  missing_definition: "❓ Definitionslücke", stale_documentation: "📅 Veraltete Doku",
  policy_violation: "🛡️ Richtlinienverstoß", policy_conflict: "🛡️ Richtlinienkonflikt",
  process_gap: "⚙️ Prozesslücke", semantic_drift: "🎯 Semantische Abweichung",
  implementation_drift: "🔧 Implementierungsabweichung", traceability_gap: "🔗 Nachverfolgbarkeitslücke",
  clarification_needed: "❓ Klärungsbedarf", stale_source: "📅 Veraltete Quelle",
  read_write_gap: "↔️ Read/Write-Lücke", ownership_gap: "👥 Ownership-Lücke",
  terminology_collision: "🧭 Begriffskollision", low_confidence_review: "🔍 Niedrige Sicherheit",
  obsolete_documentation: "🗃️ Obsolete Doku", open_decision: "🧩 Offene Entscheidung",
};
function de(v: string): string { return STATUS_DE[v] ?? SEVERITY_DE[v] ?? CATEGORY_DE[v] ?? v; }

function clusterScopeKey(item: { kind: "pkg"; pkg: DecisionPackage } | { kind: "finding"; f: AuditRun["findings"][number] }): string {
  const rawScope = item.kind === "pkg"
    ? typeof item.pkg.metadata?.cluster_key === "string" && item.pkg.metadata.cluster_key.trim()
      ? item.pkg.metadata.cluster_key
      : item.pkg.scope_summary
    : (item.f.canonical_key ?? item.f.finding_id);
  if (rawScope.startsWith("embedding_contradiction|")) return rawScope;
  const pipeBase = rawScope.split("|")[0] ?? rawScope;
  const dotted = pipeBase.split(".").slice(0, 2).join(".");
  return dotted || pipeBase || rawScope;
}

/* SVG icons for source badges */
const GH_SVG = <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>;
const CONF_SVG = <svg width="14" height="14" viewBox="0 0 32 32" fill="currentColor"><path d="M3.82 22.54c-.29.47-.62 1-.87 1.34a1.2 1.2 0 00.37 1.66l5.17 3.18a1.2 1.2 0 001.66-.42c.2-.34.52-.86.89-1.43 2.64-4.14 5.3-3.63 10.11-1.62l5.31 2.23a1.2 1.2 0 001.57-.65l2.54-5.87a1.2 1.2 0 00-.63-1.57c-1.57-.68-4.68-1.99-6.27-2.67C14.5 13.53 8.62 14.17 3.82 22.54zM28.18 9.46c.29-.47.62-1 .87-1.34a1.2 1.2 0 00-.37-1.66L23.5 3.28a1.2 1.2 0 00-1.66.42c-.2.34-.52.86-.89 1.43-2.64 4.14-5.3 3.63-10.11 1.62L5.53 4.52a1.2 1.2 0 00-1.57.65L1.42 11.04a1.2 1.2 0 00.63 1.57c1.57.68 4.68 1.99 6.27 2.67 9.08 3.69 14.96 3.05 19.86-5.82z"/></svg>;
const JIRA_SVG = <svg width="14" height="14" viewBox="0 0 32 32" fill="currentColor"><path d="M30.28 14.72L17.28 1.72 16 .44 5.37 11.07l-3.65 3.65a1.51 1.51 0 000 2.13l9.34 9.34L16 31.24l10.63-10.63.3-.3 3.35-3.46a1.51 1.51 0 000-2.13zM16 20.45l-4.45-4.45L16 11.55l4.45 4.45z"/></svg>;

const SRC_CFG: Record<string, { icon: ReactNode; label: string; cls: string }> = {
  github_file:     { icon: GH_SVG,     label: "Code",       cls: "src-code" },
  confluence_page: { icon: CONF_SVG,   label: "Confluence", cls: "src-confluence" },
  metamodel:       { icon: <span>◆</span>, label: "Metamodell", cls: "src-metamodel" },
  local_doc:       { icon: <span>📋</span>, label: "Lokal",      cls: "src-local" },
  jira_ticket:     { icon: JIRA_SVG,   label: "Jira",       cls: "src-jira" },
  user_truth:      { icon: <span>✦</span>,  label: "User",       cls: "src-user" },
};

function SrcBadge({ t }: { t: string }): ReactNode {
  const s = SRC_CFG[t] ?? { icon: <span>•</span>, label: t, cls: "src-local" };
  return <span className={`src-badge ${s.cls}`}>{s.icon} {s.label}</span>;
}

function locStr(l: AuditLocation): string {
  const p = [l.title];
  if (l.path_hint) p.push(l.path_hint);
  if (l.position?.anchor_value) p.push(l.position.anchor_value);
  return p.join(" · ");
}

function strs(v: unknown): string[] {
  return Array.isArray(v) ? v.filter((x): x is string => typeof x === "string" && x.trim().length > 0) : [];
}

function patch(r: WritebackApprovalRequest): ConfluencePatchPreview | null {
  const v = r.metadata?.confluence_patch_preview;
  return v && typeof v === "object" ? (v as ConfluencePatchPreview) : null;
}

/* Empty defaults used only while bootstrap is loading */
const EMPTY_SP: SourceProfile = {
  confluence_url: "", jira_url: "",
  confluence_space_key: "FP", jira_project_key: "FINAI",
  jira_usage: "ticket_creation_only",
  metamodel_dump_path: "", metamodel_policy: "", resource_access_mode: "read_only",
};
const EMPTY_AUTH: AtlassianAuthStatus = {
  enabled: false, client_configured: false, token_present: false, token_valid: false,
  needs_user_consent: false, redirect_uri: "", configured_redirect_uri: null,
  recommended_redirect_uri: "", redirect_uri_matches_local_api: false,
  scope: null, token_expires_at: null, last_error: null, notes: [],
};

/* ============================================================
   APP
   ============================================================ */

export default function App(): ReactNode {
  const [view, setView] = useState<"work" | "history">("work");
  const [runs, setRuns] = useState<AuditRun[]>([]);
  const [selId, setSelId] = useState("");
  const [boot, setBoot] = useState<BootstrapData | null>(null);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [globalErr, setGlobalErr] = useState("");

  // Submission states
  const [commentBusy, setCommentBusy] = useState(false);
  const [commentErr, setCommentErr] = useState("");
  const [pkgBusy, setPkgBusy] = useState("");
  const [pkgErr, setPkgErr] = useState("");
  const [appBusy, setAppBusy] = useState("");
  const [appErr, setAppErr] = useState("");
  const [exBusy, setExBusy] = useState("");
  const [exErr, setExErr] = useState("");

  // Atlassian
  const [atlAuth, setAtlAuth] = useState<AtlassianAuthStatus | null>(null);
  const [atlBusy, setAtlBusy] = useState(false);
  const [atlErr, setAtlErr] = useState("");
  const [confMsg, setConfMsg] = useState("");
  const [showModal, setShowModal] = useState(false);

  // Drafts
  const [drafts, setDrafts] = useState<Record<string, string>>({});

  // Derived
  const run = useMemo(() => runs.find((r) => r.run_id === selId) ?? runs[0] ?? null, [runs, selId]);
  const hasActive = useMemo(() => runs.some((r) => r.status === "planned" || r.status === "running"), [runs]);
  const sp = boot?.source_profile ?? EMPTY_SP;
  const ea = atlAuth ?? boot?.atlassian_auth ?? EMPTY_AUTH;
  const openPkgs = useMemo(() => (run?.decision_packages ?? []).filter((p) => p.decision_state === "open"), [run]);
  const pendApps = useMemo(() => (run?.approval_requests ?? []).filter((a) => a.status === "pending"), [run]);
  const apprvd = useMemo(() => (run?.approval_requests ?? []).filter((a) => a.status === "approved"), [run]);
  const pkgFids = useMemo(() => { const s = new Set<string>(); openPkgs.forEach((p) => p.related_finding_ids.forEach((id) => s.add(id))); return s; }, [openPkgs]);
  const soloFindings = useMemo(() => (run?.findings ?? []).filter((f) => (!f.resolution_state || f.resolution_state === "open") && !pkgFids.has(f.finding_id)), [run, pkgFids]);
  const openCount = openPkgs.length + soloFindings.length;
  const pendCount = pendApps.length;
  const [cardIdx, setCardIdx] = useState(0);
  const [elapsed, setElapsed] = useState("");
  // Reset card index when run changes
  useEffect(() => { setCardIdx(0); }, [run?.run_id]);
  // Elapsed timer for running runs
  useEffect(() => {
    const startStr = run?.started_at ?? run?.created_at;
    if (!startStr || (run?.status !== "running" && run?.status !== "planned")) { setElapsed(""); return; }
    const start = new Date(startStr).getTime();
    const tick = () => { const s = Math.floor((Date.now() - start) / 1000); setElapsed(`${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`); };
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [run?.run_id, run?.status, run?.started_at, run?.created_at]);

  // Effects
  useEffect(() => { void fetchRuns(); void fetchBoot(); }, []);
  useEffect(() => {
    if (!hasActive) return;
    const id = setInterval(() => void fetchRuns(true), 1500);
    return () => clearInterval(id);
  }, [hasActive]);

  // API
  async function fetchRuns(silent?: boolean) {
    if (!silent) setLoading(true);
    try {
      const r = await listAuditRuns();
      setRuns(r);
      if (r.length && !selId) setSelId(r[0].run_id);
      setGlobalErr("");
    } catch (e) {
      if (!silent) {
        const msg = e instanceof Error ? e.message : String(e);
        if (msg.includes("Load failed") || msg.includes("fetch") || msg.includes("NetworkError")) {
          setGlobalErr("Backend nicht erreichbar (127.0.0.1:8088)");
        } else {
          setGlobalErr(msg);
        }
      }
    }
    finally { if (!silent) setLoading(false); }
  }

  async function fetchBoot() {
    try { const b = await getBootstrapData(); setBoot(b); setAtlAuth(b.atlassian_auth); }
    catch { /* ignore */ }
  }

  function upd(u: AuditRun) { setRuns((p) => p.map((r) => (r.run_id === u.run_id ? u : r))); setSelId(u.run_id); }

  async function doCreate(t: AuditTarget) {
    setSubmitting(true); setGlobalErr("");
    try { const c = await createAuditRun(t); setRuns((p) => [c, ...p]); setSelId(c.run_id); setShowModal(false); setView("work"); }
    catch (e) { setGlobalErr(String(e)); }
    finally { setSubmitting(false); }
  }

  async function doComment(txt: string) {
    if (!run) return; setCommentBusy(true); setCommentErr("");
    try { upd(await submitDecisionComment(run.run_id, txt)); }
    catch (e) { setCommentErr(String(e)); }
    finally { setCommentBusy(false); setCardIdx(i => i); /* triggers re-render; card list shrinks so it auto-adjusts */ }
  }

  async function doPkg(id: string, a: "accept" | "reject" | "specify", c?: string) {
    if (!run) return; setPkgBusy(id); setPkgErr("");
    try { upd(await submitPackageDecision(run.run_id, id, a, c)); /* card list will shrink, cardIdx stays → shows next */ }
    catch (e) { setPkgErr(String(e)); }
    finally { setPkgBusy(""); }
  }

  async function doCreateApp(p: Parameters<typeof createWritebackApprovalRequest>[1]) {
    if (!run) return; setAppBusy("c"); setAppErr("");
    try { upd(await createWritebackApprovalRequest(run.run_id, p)); }
    catch (e) { setAppErr(String(e)); }
    finally { setAppBusy(""); }
  }

  async function doResolve(id: string, d: "approve" | "reject" | "cancel", c?: string) {
    if (!run) return; setAppBusy(id); setAppErr("");
    try { upd(await resolveWritebackApprovalRequest(run.run_id, id, d, c)); }
    catch (e) { setAppErr(String(e)); }
    finally { setAppBusy(""); }
  }

  async function doExConf(id: string) {
    if (!run) return; setExBusy(id); setExErr("");
    try { upd(await executeConfluencePageWriteback(run.run_id, id)); }
    catch (e) { setExErr(String(e)); }
    finally { setExBusy(""); }
  }

  async function doExJira(id: string) {
    if (!run) return; setExBusy(id); setExErr("");
    try { upd(await executeJiraTicketWriteback(run.run_id, id)); }
    catch (e) { setExErr(String(e)); }
    finally { setExBusy(""); }
  }

  async function doRecConf(p: Parameters<typeof recordConfluencePageUpdate>[1]) {
    if (!run) return; setExBusy(p.approval_request_id); setExErr("");
    try { upd(await recordConfluencePageUpdate(run.run_id, p)); }
    catch (e) { setExErr(String(e)); }
    finally { setExBusy(""); }
  }

  async function doRecJira(p: Parameters<typeof recordJiraTicketCreated>[1]) {
    if (!run) return; setExBusy(p.approval_request_id); setExErr("");
    try { upd(await recordJiraTicketCreated(run.run_id, p)); }
    catch (e) { setExErr(String(e)); }
    finally { setExBusy(""); }
  }

  async function refreshAtl() { try { setAtlAuth(await getAtlassianAuthStatus()); } catch { /* */ } }
  async function startAtl() {
    setAtlBusy(true); setAtlErr("");
    const hadValidToken = atlAuth?.token_valid ?? false;
    // Open popup IMMEDIATELY in the user-gesture context (before any await)
    // Otherwise browsers block the popup as "not user initiated"
    const popupName = `auditor-atl-auth-${Date.now()}`;
    let popup: Window | null = null;
    try {
      popup = window.open("", popupName, "width=600,height=700,left=200,top=100");
      if (popup && !popup.closed) {
        try { popup.document.title = "Atlassian Anmeldung"; popup.document.body.innerHTML = "<p>Atlassian-Authentifizierung wird gestartet…</p>"; } catch { /* x-origin */ }
      }
    } catch { popup = null; }
    try {
      const s = await startAtlassianAuthorization();
      const url = s.authorization_url;
      if (!url) throw new Error("authorization_url fehlt");
      if (popup && !popup.closed) {
        // Navigate the already-open popup to the OAuth URL
        const reused = window.open(url, popupName, "width=600,height=700,left=200,top=100");
        if (reused && !reused.closed) popup = reused;
      } else {
        // Fallback: try opening directly (may be blocked)
        popup = window.open(url, "_blank");
      }
      // Poll for auth completion while popup is open
      const pollId = window.setInterval(async () => {
        try {
          if (popup && popup.closed) { window.clearInterval(pollId); await refreshAtl(); return; }
          if (hadValidToken) return;
          const status = await getAtlassianAuthStatus();
          if (status.token_valid) { window.clearInterval(pollId); setAtlAuth(status); try { popup?.close(); } catch { /* */ } }
        } catch { /* ignore */ }
      }, 2000);
      // Safety: stop polling after 5 minutes
      setTimeout(() => window.clearInterval(pollId), 5 * 60 * 1000);
    } catch (e) {
      try { popup?.close(); } catch { /* */ }
      setAtlErr(String(e));
    } finally { setAtlBusy(false); }
  }
  async function verConf() {
    setAtlBusy(true);
    try { const r = await verifyConfluenceAccess(sp.confluence_space_key); setConfMsg(r.ok ? `✓ ${r.page_count} Seiten` : "✗ Fehler"); await refreshAtl(); }
    catch (e) { setAtlErr(String(e)); }
    finally { setAtlBusy(false); }
  }

  // Draft helpers
  function draft(id: string) { return drafts[id] ?? ""; }
  function setDraft(id: string, v: string) { setDrafts((c) => ({ ...c, [id]: v })); }

  /* ============================================================
     RENDER
     ============================================================ */
  return (
    <div className="app-shell">
      {/* ═══════════════ SIDEBAR ═══════════════ */}
      <nav className="sidebar">
        <div className="sidebar-brand">
          <div className="sidebar-brand-icon">🛡</div>
          <div className="sidebar-brand-text">
            <span className="sidebar-brand-name">FIN-AI Auditor</span>
            <span className="sidebar-brand-sub">Governance Workbench</span>
          </div>
        </div>

        <div className="sidebar-nav">
          <button className={`nav-item${view === "work" ? " active" : ""}`} onClick={() => setView("work")}>
            <span className="nav-icon">⚡</span>
            <span className="nav-text">Arbeitsfläche</span>
            {(openCount + pendCount) > 0 && <span className="nav-badge">{openCount + pendCount}</span>}
          </button>
          <button className={`nav-item${view === "history" ? " active" : ""}`} onClick={() => setView("history")}>
            <span className="nav-icon">📋</span>
            <span className="nav-text">Verlauf</span>
          </button>
        </div>


        <div className="sidebar-footer">
          <div className="conn"><span className={`conn-dot ${sp.metamodel_dump_path ? "ok" : "off"}`} />Metamodell {sp.metamodel_dump_path ? "✓ lokal" : "✗"}</div>
          <div className="conn"><span className={`conn-dot ${ea.token_valid ? "ok" : "off"}`} />Confluence {ea.token_valid ? "✓" : "✗"}</div>
          {(() => {
            const slots = (boot?.capabilities as Record<string, unknown>)?.llm_slots as Array<{purpose:string}> | undefined;
            const chatCount = slots?.filter(s => s.purpose === "chat").length ?? 0;
            const embedCount = slots?.filter(s => s.purpose === "embedding").length ?? 0;
            const total = boot?.capabilities?.llm_slot_count ?? 0;
            return (
              <div className="conn"><span className={`conn-dot ${boot?.capabilities?.llm_configured ? "ok" : "off"}`} />
                LLM {total > 0 ? `${chatCount} Chat · ${embedCount} Embed` : "–"}
              </div>
            );
          })()}
        </div>
      </nav>

      {/* ═══════════════ MAIN ═══════════════ */}
      <main className="main">
        {/* Header */}
        <header className="header">
          <div className="header-left">
            <h1>{view === "work" ? "Arbeitsfläche" : "Verlauf"}</h1>
            {view === "work" && run && (
              <p className="header-sub">{openCount} offene Bewertungen · {pendCount} ausstehende Freigaben</p>
            )}
          </div>
          <div className="header-right">
            {run && (
              <>
                <span className={`badge badge-${run.status}`}>{de(run.status)}{elapsed ? ` ${elapsed}` : ""}</span>
                <span className="header-ts">{ts(run.updated_at)}</span>
              </>
            )}
            <button className="btn btn-primary header-run-btn" onClick={() => setShowModal(true)}>Neu einlesen</button>
            <button className="btn btn-outline header-run-btn" style={{ fontSize: 12 }} onClick={async () => {
              if (!window.confirm("Alle Audit-Runs löschen?")) return;
              try {
                await resetAuditDatabase();
                setRuns([]); setSelId(""); setCardIdx(0);
                await fetchRuns();
              } catch (e) { console.error("Reset failed", e); }
            }}>🗑 Reset</button>
          </div>
        </header>

        {/* Progress bar */}
        {run && (run.status === "running" || run.status === "planned") && (
          <div className="gprogress">
            <div className="gprogress-row">
              <span className="gprogress-label">{run.progress.phase_label} — {run.progress.current_activity}</span>
              <span className="gprogress-pct">{run.progress.progress_pct}%</span>
            </div>
            <div className="gprogress-bar">
              <div className="gprogress-fill" style={{ width: `${Math.max(0, Math.min(100, run.progress.progress_pct))}%` }} />
            </div>
          </div>
        )}

        {/* Body */}
        <div className="body">
          {globalErr && <div className="error-box">{globalErr}</div>}

          {view === "work" ? (
            /* ═══════════════ WORK PANEL ═══════════════ */
            <>
              {/* ── Dashboard Overview — always visible ── */}
              <div className="metrics-row">
                <div className="metric-card mc-blue">
                  <div className="metric-icon">📄</div>
                  <div className="metric-body">
                    <span className="metric-label">Quellen</span>
                    <span className="metric-value">{run?.source_snapshots.length ?? 0}</span>
                    <span className="metric-sub">{run ? `${run.claims.length} Claims` : "–"}</span>
                  </div>
                </div>
                <div className="metric-card mc-amber">
                  <div className="metric-icon">🔍</div>
                  <div className="metric-body">
                    <span className="metric-label">Findings</span>
                    <span className="metric-value">{run?.findings.length ?? 0}</span>
                    <span className="metric-sub">{run ? (() => { const c = run.findings.filter(f => f.severity === "critical").length; const h = run.findings.filter(f => f.severity === "high").length; return c || h ? `${c} Kritisch · ${h} Hoch` : "–"; })() : "–"}</span>
                  </div>
                </div>
                <div className="metric-card mc-purple">
                  <div className="metric-icon">📦</div>
                  <div className="metric-body">
                    <span className="metric-label">Pakete</span>
                    <span className="metric-value">{run?.decision_packages.length ?? 0}</span>
                    <span className="metric-sub">{run ? `${run.decision_records.length} Entscheidungen` : "–"}</span>
                  </div>
                </div>
                <div className="metric-card mc-green">
                  <div className="metric-icon">✅</div>
                  <div className="metric-body">
                    <span className="metric-label">Freigaben</span>
                    <span className="metric-value">{pendApps.length}</span>
                    <span className="metric-sub">{run ? `${run.implemented_changes.length} umgesetzt` : "–"}</span>
                  </div>
                </div>
              </div>

              {/* Pipeline — horizontal under KPIs */}
              <div className="pipeline-h">
                {(() => {
                  const phaseKey = (run?.progress.phase_key ?? "").toLowerCase();
                  const isRunning = run?.status === "running" || run?.status === "planned";
                  const isDone = run?.status === "completed";
                  // Match backend AUDIT_PIPELINE_STEPS order
                  const steps = [
                    { label: "Metamodell",    keys: ["metamodel_check", "metamodel", "meta"] },
                    { label: "Code",          keys: ["finai_code_check", "code", "github", "ingestion"] },
                    { label: "Confluence",    keys: ["confluence_check", "confluence", "atlassian"] },
                    { label: "Delta",         keys: ["delta_reconciliation", "delta", "retrieval_indexing", "retrieval"] },
                    { label: "Findings",      keys: ["finding_generation", "finding", "analysis", "claim"] },
                    { label: "Empfehlungen",  keys: ["llm_recommendations", "recommend", "decision_packages", "package", "decision"] },
                  ];
                  // Find which step is currently active by matching phase_key
                  let activeIdx = -1;
                  if (isRunning) {
                    activeIdx = steps.findIndex(s => s.keys.some(k => phaseKey === k || phaseKey.includes(k)));
                    if (activeIdx === -1 && phaseKey) activeIdx = 0;
                  }
                  return steps.map((step, i) => {
                    const done = isDone || (activeIdx >= 0 && i < activeIdx);
                    const active = isRunning && i === activeIdx;
                    return (
                      <div className={`ph-step ${done ? "done" : active ? "active blink" : ""}`} key={i}>
                        <div className="ph-dot">{done ? "✓" : active ? "◉" : "○"}</div>
                        <span className="ph-label">{step.label}</span>
                      </div>
                    );
                  });
                })()}
              </div>
              {!run && (
                <div className="empty">
                  <div className="empty-icon">🛡</div>
                  <strong>Willkommen beim FIN-AI Auditor</strong>
                  <p>Erstelle einen Audit-Run über den Button in der Sidebar.</p>
                </div>
              )}

              {run && openCount === 0 && pendCount === 0 && apprvd.length === 0 && run.status === "completed" && (
                <div className="empty">
                  <div className="empty-icon">✅</div>
                  <strong>Alle Bewertungen abgeschlossen</strong>
                  <p>Keine offenen Widersprüche. Im Verlauf findest du die Entscheidungen.</p>
                </div>
              )}

              {run && openCount === 0 && pendCount === 0 && apprvd.length === 0 && run.status !== "completed" && (
                <div className="empty">
                  <div className="empty-icon">⏳</div>
                  <strong>Analyse läuft…</strong>
                  <p>Sobald Widersprüche erkannt werden, erscheinen sie hier.</p>
                </div>
              )}

              {pkgErr && <div className="error-box">{pkgErr}</div>}
              {commentErr && <div className="error-box">{commentErr}</div>}
              {appErr && <div className="error-box">{appErr}</div>}
              {exErr && <div className="error-box">{exErr}</div>}

              {/* ── Issue Card Stack ── */}
              {(() => {
                /* Build unified card list: packages first, then solo findings */
                type CardItem = { kind: "pkg"; pkg: typeof openPkgs[0] } | { kind: "finding"; f: typeof soloFindings[0] };
                const cards: CardItem[] = [
                  ...openPkgs.map(pkg => ({ kind: "pkg" as const, pkg })),
                  ...soloFindings.map(f => ({ kind: "finding" as const, f })),
                ];
                if (cards.length === 0) return null;
                // Group by scope cluster: show root issue per group, count related
                const sevOrder: Record<string, number> = { critical: 0, high: 1, medium: 2, low: 3 };
                const sorted = [...cards].sort((a, b) => {
                  const sa = a.kind === "pkg" ? a.pkg.severity_summary : a.f.severity;
                  const sb = b.kind === "pkg" ? b.pkg.severity_summary : b.f.severity;
                  return (sevOrder[sa] ?? 9) - (sevOrder[sb] ?? 9);
                });
                // Cluster by base scope — first per cluster = root issue
                const scopeGroups = new Map<string, CardItem[]>();
                for (const card of sorted) {
                  const baseScope = clusterScopeKey(card);
                  if (!scopeGroups.has(baseScope)) scopeGroups.set(baseScope, []);
                  scopeGroups.get(baseScope)!.push(card);
                }
                // Root issues = first of each group, sorted by severity
                const finalCards = [...scopeGroups.values()].map(g => ({ root: g[0], related: g.length - 1 }))
                  .sort((a, b) => {
                    const sa = a.root.kind === "pkg" ? a.root.pkg.severity_summary : a.root.f.severity;
                    const sb = b.root.kind === "pkg" ? b.root.pkg.severity_summary : b.root.f.severity;
                    return (sevOrder[sa] ?? 9) - (sevOrder[sb] ?? 9);
                  });
                const idx = Math.min(cardIdx, finalCards.length - 1);
                const visibleCards = finalCards.slice(idx, idx + 3);
                return (
                  <section>
                    <div className="section-head">
                      <h2>Offene Probleme</h2>
                      <span className="section-count">{finalCards.length} von {cards.length} angezeigt</span>
                    </div>
                    <div className="card-stack-nav">
                      <button className="btn btn-outline btn-sm" disabled={idx === 0} onClick={() => setCardIdx(Math.max(0, idx - 1))}>← Zurück</button>
                      <span className="text-sm text-muted">{idx + 1} / {finalCards.length}</span>
                      <button className="btn btn-outline btn-sm" disabled={idx >= finalCards.length - 1} onClick={() => setCardIdx(idx + 1)}>Weiter →</button>
                    </div>
                    <div className="card-stack">
                      {visibleCards.map((group, stackPos) => {
                        const item = group.root;
                        const zIndex = 10 - stackPos;
                        const offset = stackPos * 8;
                        const scale = 1 - stackPos * 0.03;
                        const opacity = stackPos === 0 ? 1 : 0.7 - stackPos * 0.15;
                        return (
                          <div key={item.kind === "pkg" ? item.pkg.package_id : item.f.finding_id}
                            className="card-stack-item" style={{ zIndex, transform: `translateY(${offset}px) scale(${scale})`, opacity, pointerEvents: stackPos === 0 ? "auto" : "none" }}>
                            {group.related > 0 && <div className="wc-related-hint" style={{ fontSize: 11, color: "var(--text-muted)", padding: "4px 12px", borderBottom: "1px solid var(--border-subtle)" }}>+ {group.related} verwandte Probleme (werden nach Bewertung neu priorisiert)</div>}
                            {item.kind === "pkg" ? (
                              <WorkCard id={item.pkg.package_id}
                                severity={item.pkg.severity_summary} category={item.pkg.category}
                                title={item.pkg.title} scope={item.pkg.scope_summary}
                                recommendation={item.pkg.recommendation_summary}
                                deltaHints={strs(item.pkg.metadata?.delta_summary)}
                                elements={item.pkg.problem_elements.map(el => ({ severity: el.severity, confidence: el.confidence, explanation: el.short_explanation, locations: el.evidence_locations }))}
                                feedback={draft(item.pkg.package_id)} onFeedback={v => setDraft(item.pkg.package_id, v)}
                                busy={pkgBusy === item.pkg.package_id || commentBusy}
                                onAccept={() => void doPkg(item.pkg.package_id, "accept", draft(item.pkg.package_id) || undefined)}
                                onReject={() => void doPkg(item.pkg.package_id, "reject", draft(item.pkg.package_id) || undefined)}
                                onSpecify={() => void doPkg(item.pkg.package_id, "specify", draft(item.pkg.package_id) || undefined)}
                                onConfluence={() => void doCreateApp({ target_type: "confluence_page_update", title: `Confluence-Writeback: ${item.pkg.title}`, summary: "Freigabeanfrage.", target_url: sp.confluence_url, related_package_ids: [item.pkg.package_id], related_finding_ids: item.pkg.related_finding_ids, payload_preview: [item.pkg.scope_summary, item.pkg.recommendation_summary] })}
                                onJira={() => void doCreateApp({ target_type: "jira_ticket_create", title: `Jira-Ticket: ${item.pkg.title}`, summary: "Freigabeanfrage.", target_url: sp.jira_url, related_package_ids: [item.pkg.package_id], related_finding_ids: item.pkg.related_finding_ids, payload_preview: [item.pkg.scope_summary, item.pkg.recommendation_summary] })}
                                appBusy={appBusy}
                              />
                            ) : (
                              <WorkCard id={item.f.finding_id}
                                severity={item.f.severity} category={item.f.category}
                                title={item.f.title} scope={item.f.summary}
                                recommendation={item.f.recommendation}
                                elements={[{ severity: item.f.severity, confidence: 1, explanation: item.f.summary, locations: item.f.locations }]}
                                feedback={draft(item.f.finding_id)} onFeedback={v => setDraft(item.f.finding_id, v)}
                                busy={commentBusy}
                                onAccept={() => { const d = draft(item.f.finding_id); void doComment(d ? `[ANNEHMEN] ${item.f.finding_id}: ${d}` : `[ANNEHMEN] ${item.f.finding_id}`); setDraft(item.f.finding_id, ""); }}
                                onReject={() => { const d = draft(item.f.finding_id); void doComment(d ? `[ABLEHNEN] ${item.f.finding_id}: ${d}` : `[ABLEHNEN] ${item.f.finding_id}`); setDraft(item.f.finding_id, ""); }}
                              />
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </section>
                );
              })()}

              {/* ── Pending approvals ── */}
              {pendApps.length > 0 && (
                <section>
                  <div className="section-head">
                    <h2>Ausstehende Freigaben</h2>
                    <span className="section-count">{pendApps.length}</span>
                  </div>
                  {pendApps.map((req) => (
                    <article className="wc" key={req.approval_request_id}>
                      <div className="wc-badges">
                        <span className="badge badge-cat">{req.target_type === "confluence_page_update" ? "📄 Confluence" : "🎫 Jira"}</span>
                        <span className="badge badge-pending">ausstehend</span>
                      </div>
                      <h3 className="wc-title">{req.title}</h3>
                      <p className="wc-scope">{req.summary}</p>
                      <div className="wc-actions">
                        <textarea value={draft(req.approval_request_id)} onChange={(e) => setDraft(req.approval_request_id, e.target.value)} placeholder="Optionaler Kommentar…" />
                        <div className="wc-btns">
                          <button className="btn btn-accept" disabled={appBusy === req.approval_request_id} onClick={() => void doResolve(req.approval_request_id, "approve", draft(req.approval_request_id))}>✓ Genehmigen</button>
                          <button className="btn btn-reject" disabled={appBusy === req.approval_request_id} onClick={() => void doResolve(req.approval_request_id, "reject", draft(req.approval_request_id))}>✗ Ablehnen</button>
                          <button className="btn btn-outline" disabled={appBusy === req.approval_request_id} onClick={() => void doResolve(req.approval_request_id, "cancel")}>Stornieren</button>
                        </div>
                      </div>
                    </article>
                  ))}
                </section>
              )}

              {/* ── Approved ── */}
              {apprvd.length > 0 && (
                <section>
                  <div className="section-head">
                    <h2>Bereit zur Ausführung</h2>
                    <span className="section-count">{apprvd.length}</span>
                  </div>
                  {apprvd.map((req) => {
                    const isC = req.target_type === "confluence_page_update";
                    const pp = patch(req);
                    return (
                      <article className="wc" key={req.approval_request_id}>
                        <div className="wc-badges">
                          <span className="badge badge-cat">{isC ? "📄 Confluence" : "🎫 Jira"}</span>
                          <span className="badge badge-approved">genehmigt</span>
                        </div>
                        <h3 className="wc-title">{req.title}</h3>
                        <div className="wc-actions">
                          <div className="wc-btns">
                            <button className="btn btn-primary" disabled={exBusy === req.approval_request_id} onClick={() => void (isC ? doExConf : doExJira)(req.approval_request_id)}>Extern ausführen</button>
                            <button className="btn btn-outline" disabled={exBusy === req.approval_request_id} onClick={() => {
                              if (isC) void doRecConf({ approval_request_id: req.approval_request_id, page_title: pp?.page_title || req.title, page_url: pp?.page_url || req.target_url || sp.confluence_url, changed_sections: pp?.changed_sections ?? [], change_summary: pp?.change_summary ?? req.payload_preview, related_finding_ids: req.related_finding_ids });
                              else void doRecJira({ approval_request_id: req.approval_request_id, ticket_key: "", ticket_url: req.target_url || sp.jira_url, related_finding_ids: req.related_finding_ids });
                            }}>Lokal verbuchen</button>
                          </div>
                        </div>
                      </article>
                    );
                  })}
                </section>
              )}
            </>
          ) : (
            /* ═══════════════ HISTORY PANEL ═══════════════ */
            <HistoryView run={run} />
          )}
        </div>
      </main>

      {/* ═══════════════ NEW RUN MODAL ═══════════════ */}
      {showModal && <RunModal
        ea={ea}
        sp={sp}
        boot={boot}
        onClose={() => setShowModal(false)}
        onStart={async (t) => { await doCreate(t); }}
        submitting={submitting}
      />}
    </div>
  );
}

/* ============================================================
   WORK CARD (inline component)
   ============================================================ */

function WorkCard(props: {
  id: string; severity: string; category: string; title: string; scope: string;
  recommendation: string; deltaHints?: string[];
  elements: { severity: string; confidence: number; explanation: string; locations: AuditLocation[] }[];
  feedback: string; onFeedback: (v: string) => void; busy: boolean;
  onAccept: () => void; onReject: () => void; onSpecify?: () => void;
  onConfluence?: () => void; onJira?: () => void; appBusy?: string;
}): ReactNode {
  return (
    <article className="wc">
      {/* 1. Typ-Badge + Schweregrad */}
      <div className="wc-badges">
        <span className="badge badge-cat">{de(props.category)}</span>
        <span className={`badge badge-${props.severity}`}>{de(props.severity)}</span>
      </div>

      {/* 2. Kurzbeschreibung: Was ist das Problem? */}
      <h3 className="wc-title">{props.title}</h3>
      <p className="wc-scope">{props.scope}</p>

      {/* 3. Quellen mit Zitaten */}
      <div className="wc-evidence">
        <div className="wc-label">Betroffene Quellen</div>
        {props.elements.map((el, i) => (
          <div className="ev-block" key={i}>
            <p className="ev-explain">{el.explanation}</p>
            <div className="ev-locs">
              {el.locations.map((loc) => (
                <div className="ev-loc" key={loc.location_id || `${loc.source_id}-${loc.title}`}>
                  <SrcBadge t={loc.source_type} />
                  {loc.url ? <a href={loc.url} target="_blank" rel="noreferrer">{locStr(loc)}</a> : <span>{locStr(loc)}</span>}
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>

      {/* 4. Empfehlung mit Begründung */}
      {props.recommendation && (
        <div className="wc-rec">
          <div className="wc-label">Empfohlene Auflösung</div>
          <div className="rec-text">{props.recommendation}</div>
        </div>
      )}

      {/* Kontext */}
      {props.deltaHints && props.deltaHints.length > 0 && (
        <div className="wc-context">
          <div className="wc-label">Änderungskontext</div>
          <ul>{props.deltaHints.map((h, i) => <li key={i}>{h}</li>)}</ul>
        </div>
      )}

      {/* Aktionen */}
      <div className="wc-actions">
        <textarea value={props.feedback} onChange={(e) => props.onFeedback(e.target.value)} placeholder="Begründung oder Anmerkung…" />
        <div className="wc-btns">
          <button className="btn btn-accept" disabled={props.busy} onClick={props.onAccept}>✓ Annehmen</button>
          <button className="btn btn-reject" disabled={props.busy} onClick={props.onReject}>✗ Ablehnen</button>
          {props.onSpecify && <button className="btn btn-specify" disabled={props.busy} onClick={props.onSpecify}>Präzisieren</button>}
        </div>
        {(props.onConfluence || props.onJira) && (
          <div className="wc-writeback">
            {props.onConfluence && <button className="btn btn-ghost btn-sm" disabled={props.appBusy === "c"} onClick={props.onConfluence}>📄 Confluence-Freigabe</button>}
            {props.onJira && <button className="btn btn-ghost btn-sm" disabled={props.appBusy === "c"} onClick={props.onJira}>🎫 Jira-Freigabe</button>}
          </div>
        )}
      </div>
    </article>
  );
}

/* ============================================================
   HISTORY VIEW (inline)
   ============================================================ */

function HistoryView({ run }: { run: AuditRun | null }): ReactNode {
  if (!run) return <div className="empty"><div className="empty-icon">📋</div><strong>Kein Run ausgewählt</strong></div>;

  const decided = run.decision_packages.filter((p) => p.decision_state !== "open");
  const resolved = run.findings.filter((f) => f.resolution_state && f.resolution_state !== "open");
  const truths = run.truths.filter((t) => t.truth_status === "active");
  const changes = [...run.implemented_changes].reverse();
  const log = [...run.analysis_log].reverse().slice(0, 30);
  const claimGroups: [string, number][] = [];
  const m = new Map<string, number>();
  run.claims.forEach((c) => m.set(c.source_type, (m.get(c.source_type) ?? 0) + 1));
  m.forEach((v, k) => claimGroups.push([k, v]));

  return (
    <>
      <section className="hsection">
        <h2 className="hsection-title">Run-Zusammenfassung</h2>
        <div className="hgrid">
          <div className="hstat"><span className="hstat-val">{run.findings.length}</span><span className="hstat-label">Findings</span></div>
          <div className="hstat"><span className="hstat-val">{run.decision_packages.length}</span><span className="hstat-label">Pakete</span></div>
          <div className="hstat"><span className="hstat-val">{run.claims.length}</span><span className="hstat-label">Behauptungen</span></div>
          <div className="hstat"><span className="hstat-val">{truths.length}</span><span className="hstat-label">Wahrheiten</span></div>
          <div className="hstat"><span className="hstat-val">{changes.length}</span><span className="hstat-label">Umgesetzt</span></div>
        </div>
        {run.summary && <p className="text-secondary">{run.summary}</p>}
      </section>

      {decided.length > 0 && (
        <section className="hsection">
          <h2 className="hsection-title">Entschiedene Pakete <span className="hsection-count">{decided.length}</span></h2>
          {decided.map((p) => (
            <div className="hitem" key={p.package_id}>
              <div className="hitem-head">
                <span className={`badge badge-${p.severity_summary}`}>{de(p.severity_summary)}</span>
                <span className="badge badge-cat">{de(p.category)}</span>
                <span className={`badge badge-${p.decision_state}`}>{de(p.decision_state)}</span>
              </div>
              <strong>{p.title}</strong><p>{p.scope_summary}</p>
            </div>
          ))}
        </section>
      )}

      {resolved.length > 0 && (
        <section className="hsection">
          <h2 className="hsection-title">Bewertete Findings <span className="hsection-count">{resolved.length}</span></h2>
          {resolved.map((f) => (
            <div className="hitem" key={f.finding_id}>
              <div className="hitem-head">
                <span className={`badge badge-${f.severity}`}>{de(f.severity)}</span>
                <span className={`badge badge-${f.resolution_state}`}>{de(f.resolution_state ?? "open")}</span>
              </div>
              <strong>{f.title}</strong><p>{f.summary}</p>
            </div>
          ))}
        </section>
      )}

      {changes.length > 0 && (
        <section className="hsection">
          <h2 className="hsection-title">Vollzugsledger <span className="hsection-count">{changes.length}</span></h2>
          {changes.map((c) => (
            <div className="hitem" key={c.change_id}>
              <div className="hitem-head">
                <span className="badge badge-cat">{c.change_type === "confluence_page_updated" ? "📄" : "🎫"} {c.change_type === "confluence_page_updated" ? "Confluence" : "Jira"}</span>
                <span className={`badge badge-${c.status === "applied" ? "completed" : "failed"}`}>{de(c.status)}</span>
              </div>
              <strong>{c.title}</strong><p>{c.summary}</p>
            </div>
          ))}
        </section>
      )}

      {truths.length > 0 && (
        <section className="hsection">
          <h2 className="hsection-title">Wahrheitsregister <span className="hsection-count">{truths.length}</span></h2>
          {truths.map((t) => (
            <div className="truth" key={t.truth_id}>
              <strong>{t.canonical_key}</strong>
              <span className="truth-detail">{t.subject_key} · {t.predicate} = {t.normalized_value}</span>
            </div>
          ))}
        </section>
      )}

      {claimGroups.length > 0 && (
        <section className="hsection">
          <h2 className="hsection-title">Behauptungen <span className="hsection-count">{run.claims.length}</span></h2>
          <div className="hgrid">
            {claimGroups.map(([st, n]) => <div className="hstat" key={st}><SrcBadge t={st} /><span className="hstat-val" style={{ marginTop: 4 }}>{n}</span></div>)}
          </div>
        </section>
      )}

      {log.length > 0 && (
        <section className="hsection">
          <h2 className="hsection-title">Aktivitätslog <span className="hsection-count">{run.analysis_log.length}</span></h2>
          {log.map((e) => (
            <div className="log-entry" key={e.log_id}>
              <span className="log-ts">{ts(e.created_at)}</span>
              <div className="log-body"><span className={`log-level log-${e.level}`}>{e.level}</span> <strong>{e.title}</strong><p>{e.message}</p></div>
            </div>
          ))}
        </section>
      )}

      {run.source_snapshots.length > 0 && (
        <section className="hsection">
          <h2 className="hsection-title">Quelldateien <span className="hsection-count">{run.source_snapshots.length}</span></h2>
          {run.source_snapshots.map((s) => (
            <div className="snapshot" key={s.snapshot_id}><SrcBadge t={s.source_type} /><span className="snapshot-id">{s.source_id}</span><span className="snapshot-rev">{s.revision_id || s.content_hash || "–"}</span></div>
          ))}
        </section>
      )}
    </>
  );
}
