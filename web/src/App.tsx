import { useEffect, useMemo, useState, type ReactNode } from "react";
import {
  createAuditRun, createWritebackApprovalRequest,
  executeConfluencePageWriteback, executeJiraTicketWriteback,
  getAtlassianAuthStatus, getBootstrapData, listAuditRuns,
  recordConfluencePageUpdate, recordJiraTicketCreated, resetAuditDatabase,
  resolveWritebackApprovalRequest, startAtlassianAuthorization,
  submitDecisionComment, submitPackageDecision, updateAtomicFactStatus,
  verifyConfluenceAccess,
} from "./api";
import { categoryLabel } from "./categoryLabels";
import RunModal from "./components/RunModal";
import { ClarificationPanel } from "./components/ClarificationPanel";
import DocStructureView from "./components/DocStructureView";
import type {
  AtlassianAuthStatus, AuditLocation, AuditRun, AuditTarget,
  AtomicFactEntry,
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
  confirmed: "Bestätigt", resolved: "Aufgelöst",
  applied: "Umgesetzt", accepted: "Akzeptiert", specified: "Präzisiert",
  superseded: "Ersetzt", dismissed: "Verworfen", executed: "Ausgeführt", cancelled: "Storniert",
};
const SEVERITY_DE: Record<string, string> = {
  critical: "Kritisch", high: "Hoch", medium: "Mittel", low: "Gering",
};
const CATEGORY_DE: Record<string, string> = {
  contradiction: categoryLabel("contradiction"), gap: categoryLabel("gap"), inconsistency: categoryLabel("inconsistency"),
  architecture_observation: categoryLabel("architecture_observation"),
  missing_implementation: categoryLabel("missing_implementation"), missing_documentation: categoryLabel("missing_documentation"),
  missing_definition: categoryLabel("missing_definition"), stale_documentation: categoryLabel("stale_documentation"),
  policy_violation: categoryLabel("policy_violation"), policy_conflict: categoryLabel("policy_conflict"),
  process_gap: categoryLabel("process_gap"), semantic_drift: categoryLabel("semantic_drift"),
  implementation_drift: categoryLabel("implementation_drift"), traceability_gap: categoryLabel("traceability_gap"),
  clarification_needed: categoryLabel("clarification_needed"), stale_source: categoryLabel("stale_source"),
  read_write_gap: categoryLabel("read_write_gap"), ownership_gap: categoryLabel("ownership_gap"),
  legacy_path_gap: categoryLabel("legacy_path_gap"),
  terminology_collision: categoryLabel("terminology_collision"), low_confidence_review: categoryLabel("low_confidence_review"),
  obsolete_documentation: categoryLabel("obsolete_documentation"), open_decision: categoryLabel("open_decision"),
};
function de(v: string): string { return STATUS_DE[v] ?? SEVERITY_DE[v] ?? CATEGORY_DE[v] ?? v; }

const ACTION_LANE_DE: Record<AtomicFactEntry["action_lane"], string> = {
  confluence_doc: "Confluence-Doku",
  jira_code: "Jira-Code",
  jira_artifact: "Jira-Artefakt",
  confluence_and_jira: "Confluence + Jira",
};

function actionLaneDe(v: AtomicFactEntry["action_lane"]): string {
  return ACTION_LANE_DE[v] ?? v;
}

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
const NEO4J_SVG = <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><circle cx="6" cy="18" r="3"/><circle cx="18" cy="18" r="3"/><circle cx="12" cy="6" r="3"/><line x1="9" y1="7.5" x2="7.5" y2="16" stroke="currentColor" strokeWidth="1.5" fill="none"/><line x1="15" y1="7.5" x2="16.5" y2="16" stroke="currentColor" strokeWidth="1.5" fill="none"/><line x1="9" y1="18" x2="15" y2="18" stroke="currentColor" strokeWidth="1.5" fill="none"/></svg>;

const SRC_CFG: Record<string, { icon: ReactNode; label: string; cls: string }> = {
  github_file:     { icon: GH_SVG,     label: "Code",       cls: "src-code" },
  confluence_page: { icon: CONF_SVG,   label: "Confluence", cls: "src-confluence" },
  metamodel:       { icon: NEO4J_SVG,  label: "Metamodell", cls: "src-metamodel" },
  local_doc:       { icon: <span>📋</span>, label: "Lokal",      cls: "src-local" },
  jira_ticket:     { icon: JIRA_SVG,   label: "Jira",       cls: "src-jira" },
  user_truth:      { icon: <span>✦</span>,  label: "Nutzer",     cls: "src-user" },
};

function SrcBadge({ t }: { t: string }): ReactNode {
  const s = SRC_CFG[t] ?? { icon: <span>•</span>, label: t, cls: "src-local" };
  return <span className={`src-badge ${s.cls}`}>{s.icon} {s.label}</span>;
}

function sourceTypeLabel(sourceType: string): string {
  return SRC_CFG[sourceType]?.label ?? sourceType;
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

function packageContextLines(pkg: DecisionPackage): string[] {
  const meta = pkg.metadata ?? {};
  const lines: string[] = [];
  const rootCauseLabel = typeof meta.root_cause_label === "string" ? meta.root_cause_label : "";
  if (rootCauseLabel) lines.push(`Primärursache: ${rootCauseLabel}`);
  const truthOverlap = strs(meta.truth_overlap_keys).slice(0, 3);
  if (truthOverlap.length > 0) lines.push(`Betroffene Wahrheiten: ${truthOverlap.join(", ")}`);
  const actionLanes = strs(meta.action_lanes).map((lane) => actionLaneDe(lane as AtomicFactEntry["action_lane"]));
  if (actionLanes.length > 0) lines.push(`Aktionsspuren: ${actionLanes.join(", ")}`);
  const writeDeciders = strs(meta.causal_write_deciders).slice(0, 2);
  if (writeDeciders.length > 0) lines.push(`Write-Decider: ${writeDeciders.join(", ")}`);
  const repoSymbols = strs(meta.causal_repository_adapter_symbols).slice(0, 2);
  if (repoSymbols.length > 0) lines.push(`Repository-Symbole: ${repoSymbols.join(", ")}`);
  const driverSymbols = strs(meta.causal_driver_adapter_symbols).slice(0, 2);
  if (driverSymbols.length > 0) lines.push(`Driver-Symbole: ${driverSymbols.join(", ")}`);
  const sinks = strs(meta.causal_persistence_targets).slice(0, 2);
  const sinkKinds = strs(meta.causal_persistence_sink_kinds);
  if (sinks.length > 0) {
    const formatted = sinks.map((sink, index) => `${sinkKinds[index] ? `${sinkKinds[index]} -> ` : ""}${sink}`);
    lines.push(`Persistenz-Sinks: ${formatted.join(", ")}`);
  }
  const schemaTargets = strs(meta.causal_persistence_schema_targets).slice(0, 2);
  if (schemaTargets.length > 0) lines.push(`Schema-Ziele: ${schemaTargets.join(", ")}`);
  const validatedTargets = strs(meta.causal_schema_validated_targets).slice(0, 2);
  if (validatedTargets.length > 0) lines.push(`SSOT-bestätigt: ${validatedTargets.join(", ")}`);
  const unconfirmedTargets = strs(meta.causal_schema_unconfirmed_targets).slice(0, 2);
  if (unconfirmedTargets.length > 0) lines.push(`Noch unbestätigt: ${unconfirmedTargets.join(", ")}`);
  return lines;
}

function packageNextActions(pkg: DecisionPackage): string[] {
  const actionLanes = new Set(strs(pkg.metadata?.action_lanes));
  const actions: string[] = [];
  if (actionLanes.has("confluence_doc") || actionLanes.has("confluence_and_jira")) {
    actions.push("Confluence-Doku geradeziehen oder Patch-Preview freigeben");
  }
  if (actionLanes.has("jira_code")) {
    actions.push("Jira-Code-Ticket mit qualifiziertem Write-Pfad erzeugen");
  }
  if (actionLanes.has("jira_artifact") || actionLanes.has("confluence_and_jira")) {
    actions.push("Artefaktkorrektur per Jira gegen PUML/Metamodell/Dump anstoßen");
  }
  if (actions.length === 0) {
    actions.push("Paket fachlich bewerten und bei Bedarf spezifizieren");
  }
  return actions;
}

function approvalPreflight(req: WritebackApprovalRequest): { blockers: string[]; warnings: string[] } {
  const raw = req.metadata?.writeback_preflight;
  if (!raw || typeof raw !== "object") return { blockers: [], warnings: [] };
  const value = raw as Record<string, unknown>;
  return {
    blockers: strs(value.blockers),
    warnings: strs(value.warnings),
  };
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
  const [view, setView] = useState<"work" | "structure" | "history">("work");
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
  const [factBusy, setFactBusy] = useState("");
  const [factErr, setFactErr] = useState("");
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
  const activeFacts = useMemo(
    () => (run?.atomic_facts ?? []).filter((fact) => fact.status === "open" || fact.status === "confirmed"),
    [run],
  );
  const pkgFids = useMemo(() => { const s = new Set<string>(); openPkgs.forEach((p) => p.related_finding_ids.forEach((id) => s.add(id))); return s; }, [openPkgs]);
  const soloFindings = useMemo(
    () => (run?.findings ?? []).filter(
      (f) =>
        (!f.resolution_state || f.resolution_state === "open") &&
        !pkgFids.has(f.finding_id) &&
        f.category !== "architecture_observation",
    ),
    [run, pkgFids],
  );
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

  async function doPkg(id: string, a: "accept" | "reject", c?: string) {
    if (!run) return; setPkgBusy(id); setPkgErr("");
    try { upd(await submitPackageDecision(run.run_id, id, a, c)); /* card list will shrink, cardIdx stays → shows next */ }
    catch (e) { setPkgErr(String(e)); }
    finally { setPkgBusy(""); }
  }

  async function doAtomicFact(
    atomicFactId: string,
    status: AtomicFactEntry["status"],
    commentText?: string,
  ) {
    if (!run) return;
    setFactBusy(atomicFactId);
    setFactErr("");
    try { upd(await updateAtomicFactStatus(run.run_id, atomicFactId, status, commentText)); }
    catch (e) { setFactErr(String(e)); }
    finally { setFactBusy(""); }
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
            <span className="sidebar-brand-sub">Governance-Konsole</span>
          </div>
        </div>

        <div className="sidebar-nav">
          <button className={`nav-item${view === "work" ? " active" : ""}`} onClick={() => setView("work")}>
            <span className="nav-icon">🔍</span>
            <span className="nav-text">Befund-Auditierung</span>
            {(openCount + pendCount) > 0 && <span className="nav-badge">{openCount + pendCount}</span>}
          </button>
          <button className={`nav-item${view === "structure" ? " active" : ""}`} onClick={() => setView("structure")}>
            <span className="nav-icon">📐</span>
            <span className="nav-text">Doku-Strukturierung</span>
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
            <h1>{view === "work" ? "Befund-Auditierung" : view === "structure" ? "Doku-Strukturierung" : "Verlauf"}</h1>
            {view === "work" && run && (
              <p className="header-sub">{openCount} Probleme zu bewerten · {activeFacts.length} bestätigte Fakten · {pendCount} Änderungen zur Freigabe</p>
            )}
            {view === "structure" && (
              <p className="header-sub">Confluence-Dokumentation analysieren · Struktur bewerten · Fachlich / Technisch trennen</p>
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
                <div className="metric-card mc-amber">
                  <div className="metric-icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg></div>
                  <div className="metric-body">
                    <span className="metric-label">Unstimmigkeiten</span>
                    <span className="metric-value">{run?.findings.length ?? 0}</span>
                    <span className="metric-sub">{run ? (() => { const c = run.findings.filter(f => f.severity === "critical").length; const h = run.findings.filter(f => f.severity === "high").length; return c || h ? `${c} Kritisch · ${h} Hoch` : "Keine kritischen"; })() : "–"}</span>
                  </div>
                </div>
                <div className="metric-card mc-purple">
                  <div className="metric-icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="12" y1="8" x2="12" y2="16"/><line x1="8" y1="12" x2="16" y2="12"/></svg></div>
                  <div className="metric-body">
                    <span className="metric-label">Offene Entscheidungen</span>
                    <span className="metric-value">{openPkgs.length}</span>
                    <span className="metric-sub">{run ? `${pendApps.length} Freigaben ausstehend` : "–"}</span>
                  </div>
                </div>
                <div className="metric-card mc-green">
                  <div className="metric-icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12"/></svg></div>
                  <div className="metric-body">
                    <span className="metric-label">Entschieden</span>
                    <span className="metric-value">{run ? run.decision_packages.filter(p => p.decision_state !== "open").length : 0}</span>
                    <span className="metric-sub">{run ? `${run.implemented_changes.length} umgesetzt · ${run.decision_records.length} Bewertungen` : "–"}</span>
                  </div>
                </div>
                <div className="metric-card">
                  <div className="metric-icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg></div>
                  <div className="metric-body">
                    <span className="metric-label">Widersprüche</span>
                    <span className="metric-value">{run ? run.findings.filter(f => ["contradiction","policy_conflict","terminology_collision"].includes(f.category)).length : 0}</span>
                    <span className="metric-sub">Konflikte zwischen Quellen</span>
                  </div>
                </div>
                <div className="metric-card">
                  <div className="metric-icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg></div>
                  <div className="metric-body">
                    <span className="metric-label">Fehler</span>
                    <span className="metric-value">{run ? run.findings.filter(f => ["implementation_drift","stale_source","read_write_gap"].includes(f.category)).length : 0}</span>
                    <span className="metric-sub">Drift und veraltete Quellen</span>
                  </div>
                </div>
                <div className="metric-card mc-copper">
                  <div className="metric-icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 3v18"/><path d="M7 8h6a3 3 0 1 1 0 6H9"/><path d="M9 14h7a3 3 0 1 1 0 6H7"/></svg></div>
                  <div className="metric-body">
                    <span className="metric-label">Boundary-Pfade</span>
                    <span className="metric-value">{run ? run.findings.filter(f => f.category === "legacy_path_gap").length : 0}</span>
                    <span className="metric-sub">Manuelle oder Legacy-Entry-Points</span>
                  </div>
                </div>
                <div className="metric-card">
                  <div className="metric-icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M13 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V9z"/><polyline points="13 2 13 9 20 9"/><line x1="10" y1="14" x2="14" y2="14"/></svg></div>
                  <div className="metric-body">
                    <span className="metric-label">Lücken</span>
                    <span className="metric-value">{run ? run.findings.filter(f => ["missing_definition","missing_documentation","traceability_gap","ownership_gap","clarification_needed"].includes(f.category)).length : 0}</span>
                    <span className="metric-sub">Fehlende Definitionen & Doku</span>
                  </div>
                </div>
                <div className="metric-card mc-teal">
                  <div className="metric-icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 000 7h5a3.5 3.5 0 010 7H6"/></svg></div>
                  <div className="metric-body">
                    <span className="metric-label">Token & Kosten</span>
                    <span className="metric-value">{run?.llm_usage?.total_cost_eur ? `${run.llm_usage.total_cost_eur.toFixed(2)}€` : "0€"}</span>
                    <span className="metric-sub">{run?.llm_usage?.total_prompt_tokens ? `${((run.llm_usage.total_prompt_tokens ?? 0) + (run.llm_usage.total_completion_tokens ?? 0)).toLocaleString("de-DE")} Token` : "–"}</span>
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
              {factErr && <div className="error-box">{factErr}</div>}
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
                // SDD: documentation sources are primary, code is only indication
                const srcPriority = (card: CardItem): number => {
                  const srcs: string[] = card.kind === "pkg"
                    ? (card.pkg.metadata?.source_types as string[] ?? [])
                    : (card.f.metadata?.source_types as string[] ?? (card.f.metadata?.source_type ? [card.f.metadata.source_type as string] : []));
                  // Doku-Quellen = Priorität 0 (höchste), Code = 2 (niedrigste)
                  if (srcs.some(s => ["confluence_page", "metamodel", "local_doc"].includes(s))) return 0;
                  if (srcs.some(s => ["jira_ticket"].includes(s))) return 1;
                  return 2; // github_file / code
                };
                const sorted = [...cards].sort((a, b) => {
                  const sa = a.kind === "pkg" ? a.pkg.severity_summary : a.f.severity;
                  const sb = b.kind === "pkg" ? b.pkg.severity_summary : b.f.severity;
                  const sevDiff = (sevOrder[sa] ?? 9) - (sevOrder[sb] ?? 9);
                  if (sevDiff !== 0) return sevDiff;
                  // Bei gleicher Severity: Doku-Findings vor Code-Findings
                  return srcPriority(a) - srcPriority(b);
                });
                // Cluster by base scope — first per cluster = root issue
                const scopeGroups = new Map<string, CardItem[]>();
                for (const card of sorted) {
                  const baseScope = clusterScopeKey(card);
                  if (!scopeGroups.has(baseScope)) scopeGroups.set(baseScope, []);
                  scopeGroups.get(baseScope)!.push(card);
                }
                // Root issues = first of each group, sorted by severity + source priority
                const finalCards = [...scopeGroups.values()].map(g => ({ root: g[0], related: g.length - 1 }))
                  .sort((a, b) => {
                    const sa = a.root.kind === "pkg" ? a.root.pkg.severity_summary : a.root.f.severity;
                    const sb = b.root.kind === "pkg" ? b.root.pkg.severity_summary : b.root.f.severity;
                    const sevDiff = (sevOrder[sa] ?? 9) - (sevOrder[sb] ?? 9);
                    if (sevDiff !== 0) return sevDiff;
                    return srcPriority(a.root) - srcPriority(b.root);
                  });
                const idx = Math.min(cardIdx, finalCards.length - 1);
                const visibleCards = finalCards.slice(idx, idx + 3);
                return (
                  <section>
                    <div className="section-head">
                      <h2>Offene Probleme</h2>
                      <span className="section-count">{finalCards.length} Entscheidungen ausstehend — sortiert nach Dringlichkeit</span>
                    </div>
                    <div className="card-list">
                      {finalCards.map((group, cardRank) => {
                        const item = group.root;
                        return (
                          <div key={item.kind === "pkg" ? item.pkg.package_id : item.f.finding_id} className="card-list-item">
                            {group.related > 0 && <div className="wc-related-hint" style={{ fontSize: 11, color: "var(--text-muted)", padding: "4px 12px", borderBottom: "1px solid var(--border-subtle)" }}>+ {group.related} verwandte Probleme (werden nach Bewertung neu priorisiert)</div>}
                            {item.kind === "pkg" ? (
                              <WorkCard id={item.pkg.package_id}
                                rank={cardRank + 1}
                                severity={item.pkg.severity_summary} category={item.pkg.category}
                                title={item.pkg.title} scope={item.pkg.scope_summary}
                                recommendation={item.pkg.recommendation_summary}
                                positiveConsequences={strs(item.pkg.metadata?.positive_consequences)}
                                negativeConsequences={strs(item.pkg.metadata?.negative_consequences)}
                                deltaHints={strs(item.pkg.metadata?.delta_summary)}
                                analysisContext={packageContextLines(item.pkg)}
                                nextActions={packageNextActions(item.pkg)}
                                elements={item.pkg.problem_elements.map(el => ({ severity: el.severity, confidence: el.confidence, explanation: el.short_explanation, locations: el.evidence_locations }))}
                                busy={pkgBusy === item.pkg.package_id || commentBusy}
                                onAccept={() => void doPkg(item.pkg.package_id, "accept")}
                                onReject={() => void doPkg(item.pkg.package_id, "reject")}
                                onConfluence={() => void doCreateApp({ target_type: "confluence_page_update", title: `Confluence-Writeback: ${item.pkg.title}`, summary: "Freigabeanfrage.", target_url: sp.confluence_url, related_package_ids: [item.pkg.package_id], related_finding_ids: item.pkg.related_finding_ids, payload_preview: [item.pkg.scope_summary, item.pkg.recommendation_summary] })}
                                onJira={() => void doCreateApp({ target_type: "jira_ticket_create", title: `Jira-Ticket: ${item.pkg.title}`, summary: "Freigabeanfrage.", target_url: sp.jira_url, related_package_ids: [item.pkg.package_id], related_finding_ids: item.pkg.related_finding_ids, payload_preview: [item.pkg.scope_summary, item.pkg.recommendation_summary] })}
                                appBusy={appBusy}
                                clarificationPanel={
                                  run ? <ClarificationPanel run={run} packageId={item.pkg.package_id} onRunUpdated={upd} /> : undefined
                                }
                              />
                            ) : (
                              <WorkCard id={item.f.finding_id}
                                rank={cardRank + 1}
                                severity={item.f.severity} category={item.f.category}
                                title={item.f.title} scope={item.f.summary}
                                recommendation={item.f.recommendation}
                                positiveConsequences={strs(item.f.metadata?.positive_consequences)}
                                negativeConsequences={strs(item.f.metadata?.negative_consequences)}
                                analysisContext={[
                                  ...strs(item.f.metadata?.semantic_context).slice(0, 2),
                                  ...strs(item.f.metadata?.causal_write_decider_labels).slice(0, 1).map((v) => `Write-Decider: ${v}`),
                                  ...strs(item.f.metadata?.causal_persistence_schema_targets).slice(0, 1).map((v) => `Schema-Ziel: ${v}`),
                                ]}
                                proposedPageMd={typeof item.f.metadata?.proposed_page_md === "string" ? item.f.metadata.proposed_page_md : undefined}
                                proposedPageTitle={typeof item.f.metadata?.proposed_page_title === "string" ? item.f.metadata.proposed_page_title : undefined}
                                metaSourceType={typeof item.f.metadata?.source_type === "string" ? item.f.metadata.source_type : undefined}
                                metaSourceTypes={Array.isArray(item.f.metadata?.source_types) ? (item.f.metadata.source_types as string[]) : undefined}
                                elements={[{ severity: item.f.severity, confidence: 1, explanation: item.f.summary, locations: item.f.locations }]}
                                busy={commentBusy}
                                onAccept={() => { void doComment(`[ANNEHMEN] ${item.f.finding_id}`); setDraft(item.f.finding_id, ""); }}
                                onReject={() => { void doComment(`[ABLEHNEN] ${item.f.finding_id}`); setDraft(item.f.finding_id, ""); }}
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
                      {(approvalPreflight(req).blockers.length > 0 || approvalPreflight(req).warnings.length > 0) && (
                        <div className="wc-context">
                          <div className="wc-label">Preflight</div>
                          <ul>
                            {approvalPreflight(req).blockers.map((item) => <li key={`b:${item}`}>Blocker: {item}</li>)}
                            {approvalPreflight(req).warnings.map((item) => <li key={`w:${item}`}>Warnung: {item}</li>)}
                          </ul>
                        </div>
                      )}
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
                        {(approvalPreflight(req).blockers.length > 0 || approvalPreflight(req).warnings.length > 0) && (
                          <div className="wc-context">
                            <div className="wc-label">Preflight</div>
                            <ul>
                              {approvalPreflight(req).blockers.map((item) => <li key={`b:${item}`}>Blocker: {item}</li>)}
                              {approvalPreflight(req).warnings.map((item) => <li key={`w:${item}`}>Warnung: {item}</li>)}
                            </ul>
                          </div>
                        )}
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

              {run && run.atomic_facts.length > 0 && (
                <section>
                  <div className="section-head">
                    <h2>Atomare Fakten</h2>
                    <span className="section-count">{activeFacts.length} aktiv · {run.atomic_facts.length} gesamt</span>
                  </div>
                  {[...run.atomic_facts]
                    .sort((left, right) => {
                      const order: Record<AtomicFactEntry["status"], number> = {
                        open: 0,
                        confirmed: 1,
                        resolved: 2,
                        superseded: 3,
                      };
                      return (order[left.status] ?? 9) - (order[right.status] ?? 9);
                    })
                    .map((fact) => {
                      const lastComment = typeof fact.metadata?.last_status_comment === "string"
                        ? fact.metadata.last_status_comment
                        : "";
                      return (
                        <article className="wc" key={fact.atomic_fact_id}>
                          <div className="wc-badges">
                            <span className={`badge badge-${fact.status === "confirmed" ? "approved" : fact.status === "resolved" ? "completed" : fact.status === "superseded" ? "rejected" : "pending"}`}>
                              {de(fact.status)}
                            </span>
                            <span className="badge badge-cat">{actionLaneDe(fact.action_lane)}</span>
                          </div>
                          <h3 className="wc-title">{fact.summary}</h3>
                          <p className="wc-scope">{fact.fact_key}</p>
                          <div className="wc-context">
                            <div className="wc-label">Faktenbild</div>
                            <ul>
                              {fact.subject_keys.length > 0 && <li>Subjekte: {fact.subject_keys.join(", ")}</li>}
                              {fact.predicates.length > 0 && <li>Prädikate: {fact.predicates.join(", ")}</li>}
                              {fact.source_types.length > 0 && <li>Quellen: {fact.source_types.map((sourceType) => sourceTypeLabel(sourceType)).join(", ")}</li>}
                              <li>Pakete/Probleme: {fact.related_package_ids.length}/{fact.related_problem_ids.length}</li>
                              {typeof fact.metadata?.occurrence_count === "number" && <li>Auftreten: {String(fact.metadata.occurrence_count)} Lauf/Läufe</li>}
                              {typeof fact.metadata?.previous_run_id === "string" && fact.metadata.previous_run_id && <li>Vorläufer-Run: {fact.metadata.previous_run_id}</li>}
                              {typeof fact.metadata?.reopened_from_status === "string" && fact.metadata.reopened_from_status && <li>Wiederaufgetreten nach: {String(fact.metadata.reopened_from_status)}</li>}
                            </ul>
                          </div>
                          {lastComment && (
                            <div className="wc-rec">
                              <div className="wc-label">Letzte Statusbegründung</div>
                              <div className="rec-text">{lastComment}</div>
                            </div>
                          )}
                          <div className="wc-actions">
                            <textarea
                              value={draft(`fact:${fact.atomic_fact_id}`)}
                              onChange={(e) => setDraft(`fact:${fact.atomic_fact_id}`, e.target.value)}
                              placeholder="Begründung für Faktenstatus oder Folgeaktion…"
                            />
                            <div className="wc-btns">
                              <button
                                className="btn btn-accept"
                                disabled={factBusy === fact.atomic_fact_id}
                                onClick={() => void doAtomicFact(fact.atomic_fact_id, "confirmed", draft(`fact:${fact.atomic_fact_id}`) || undefined)}
                              >
                                ✓ Bestätigen
                              </button>
                              <button
                                className="btn btn-specify"
                                disabled={factBusy === fact.atomic_fact_id}
                                onClick={() => void doAtomicFact(fact.atomic_fact_id, "resolved", draft(`fact:${fact.atomic_fact_id}`) || undefined)}
                              >
                                Erledigt
                              </button>
                              <button
                                className="btn btn-reject"
                                disabled={factBusy === fact.atomic_fact_id}
                                onClick={() => void doAtomicFact(fact.atomic_fact_id, "superseded", draft(`fact:${fact.atomic_fact_id}`) || undefined)}
                              >
                                Ersetzt
                              </button>
                              {fact.status !== "open" && (
                                <button
                                  className="btn btn-outline"
                                  disabled={factBusy === fact.atomic_fact_id}
                                  onClick={() => void doAtomicFact(fact.atomic_fact_id, "open", draft(`fact:${fact.atomic_fact_id}`) || undefined)}
                                >
                                  Zurück auf offen
                                </button>
                              )}
                            </div>
                          </div>
                        </article>
                      );
                    })}
                </section>
              )}

            </>
          ) : view === "structure" ? (
            /* ═══════════════ DOKU-STRUKTURIERUNG ═══════════════ */
            <DocStructureView run={run} boot={boot} sp={sp} />
          ) : (
            /* ═══════════════ HISTORY PANEL ═══════════════ */
            <HistoryView run={run} boot={boot} />
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
  proposedPageMd?: string; proposedPageTitle?: string;
  elements: { severity: string; confidence: number; explanation: string; locations: AuditLocation[] }[];
  analysisContext?: string[];
  nextActions?: string[];
  positiveConsequences?: string[];
  negativeConsequences?: string[];
  busy: boolean;
  onAccept: () => void; onReject: () => void;
  onConfluence?: () => void; onJira?: () => void; appBusy?: string;
  metaSourceType?: string; metaSourceTypes?: string[];
  clarificationPanel?: ReactNode;
  rank?: number;
}): ReactNode {
  const [showMd, setShowMd] = useState(false);
  // Determine the PRIMARY source — the one most likely causing the irregularity
  // Priority: metaSourceType (from detector) → first location → metaSourceTypes[0]
  const primarySrc = props.metaSourceType
    || props.elements.flatMap(el => el.locations.map(l => l.source_type)).filter(Boolean)[0]
    || (props.metaSourceTypes?.[0])
    || "";
  const primaryCfg = primarySrc ? (SRC_CFG[primarySrc] ?? null) : null;
  return (
    <article className="wc" data-severity={props.severity}>
      {/* Rang-Badge */}
      {props.rank != null && <div className="wc-rank" title={`Priorität ${props.rank}`}>#{props.rank}</div>}
      {/* 1. Typ-Badge + Schweregrad + Primary Source */}
      <div className="wc-badges">
        <span className="badge badge-cat">{de(props.category)}</span>
        <span className={`badge badge-${props.severity}`}>{de(props.severity)}</span>
        {primaryCfg && (
          <span className={`wc-primary-source ${primaryCfg.cls}`} title={`Ursprung: ${primaryCfg.label}`}>
            {primaryCfg.icon}
          </span>
        )}
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

      {/* 4b. Konsequenzen */}
      {(props.positiveConsequences?.length || props.negativeConsequences?.length) ? (
        <div className="wc-consequences">
          {props.positiveConsequences && props.positiveConsequences.length > 0 && (
            <div className="wc-cons wc-cons-pos">
              <div className="wc-cons-head">✅ Wenn umgesetzt:</div>
              <ul>{props.positiveConsequences.map((c, i) => <li key={i}>{c}</li>)}</ul>
            </div>
          )}
          {props.negativeConsequences && props.negativeConsequences.length > 0 && (
            <div className="wc-cons wc-cons-neg">
              <div className="wc-cons-head">⚠️ Wenn nicht umgesetzt:</div>
              <ul>{props.negativeConsequences.map((c, i) => <li key={i}>{c}</li>)}</ul>
            </div>
          )}
        </div>
      ) : null}

      {/* Kontext */}
      {props.deltaHints && props.deltaHints.length > 0 && (
        <div className="wc-context">
          <div className="wc-label">Änderungskontext</div>
          <ul>{props.deltaHints.map((h, i) => <li key={i}>{h}</li>)}</ul>
        </div>
      )}

      {props.analysisContext && props.analysisContext.length > 0 && (
        <div className="wc-context">
          <div className="wc-label">Entscheidungskontext</div>
          <ul>{props.analysisContext.map((h, i) => <li key={i}>{h}</li>)}</ul>
        </div>
      )}

      {props.nextActions && props.nextActions.length > 0 && (
        <div className="wc-context">
          <div className="wc-label">Naechste Folgeschritte</div>
          <ul>{props.nextActions.map((h, i) => <li key={i}>{h}</li>)}</ul>
        </div>
      )}

      {/* Proposed Confluence Page (MD preview) */}
      {props.proposedPageMd && (
        <div className="wc-md-proposal">
          <button className="btn btn-outline btn-sm wc-md-toggle" onClick={() => setShowMd(!showMd)}>
            {showMd ? "▾ Seitenvorschlag verbergen" : "▸ Confluence-Seitenvorschlag anzeigen"}
            {props.proposedPageTitle && <span className="wc-md-title-hint"> — {props.proposedPageTitle}</span>}
          </button>
          {showMd && (
            <div className="wc-md-content">
              <div className="wc-md-actions">
                <button className="btn btn-ghost btn-sm" onClick={() => { navigator.clipboard.writeText(props.proposedPageMd!); }}>📋 Kopieren</button>
              </div>
              <pre className="wc-md-pre">{props.proposedPageMd}</pre>
            </div>
          )}
        </div>
      )}

      {/* Entscheidung */}
      <div className="wc-actions">
        <div className="wc-decision-label">Ihre Entscheidung</div>
        <div className="wc-btns wc-btns-primary">
          <button className="btn btn-accept" disabled={props.busy} onClick={props.onAccept}>✓ Annehmen</button>
          <button className="btn btn-reject" disabled={props.busy} onClick={props.onReject}>✗ Ablehnen</button>
        </div>
        {(props.onConfluence || props.onJira) && (
          <div className="wc-writeback">
            {props.onConfluence && <button className="btn btn-ghost btn-sm" disabled={props.appBusy === "c"} onClick={props.onConfluence}>📄 Confluence-Freigabe</button>}
            {props.onJira && <button className="btn btn-ghost btn-sm" disabled={props.appBusy === "c"} onClick={props.onJira}>🎫 Jira-Freigabe</button>}
          </div>
        )}
      </div>

      {/* Klärungsdialog */}
      {props.clarificationPanel && (
        <div className="wc-context">
          <div className="wc-label">Klärung & Rückfragen</div>
          {props.clarificationPanel}
        </div>
      )}
    </article>
  );
}

/* ============================================================
   HISTORY VIEW (inline)
   ============================================================ */

function HistoryView({ run, boot }: { run: AuditRun | null; boot: BootstrapData | null }): ReactNode {
  if (!run) return <div className="empty"><div className="empty-icon">📋</div><strong>Kein Run ausgewählt</strong></div>;

  const decided = run.decision_packages.filter((p) => p.decision_state !== "open");
  const resolved = run.findings.filter((f) => f.resolution_state && f.resolution_state !== "open");
  const truths = run.truths.filter((t) => t.truth_status === "active");
  const atomicFacts = [...run.atomic_facts].sort((left, right) => left.fact_key.localeCompare(right.fact_key));
  const globalAtomicFacts = boot?.atomic_fact_registry?.latest_facts ?? [];
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
          <div className="hstat"><span className="hstat-val">{run.findings.length}</span><span className="hstat-label">Befunde</span></div>
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

      {atomicFacts.length > 0 && (
        <section className="hsection">
          <h2 className="hsection-title">Atomare Fakten <span className="hsection-count">{atomicFacts.length}</span></h2>
          {atomicFacts.map((fact) => (
            <div className="hitem" key={fact.atomic_fact_id}>
              <div className="hitem-head">
                <span className={`badge badge-${fact.status === "confirmed" ? "approved" : fact.status === "resolved" ? "completed" : fact.status === "superseded" ? "rejected" : "pending"}`}>{de(fact.status)}</span>
                <span className="badge badge-cat">{actionLaneDe(fact.action_lane)}</span>
              </div>
              <strong>{fact.summary}</strong>
              <p>
                {fact.fact_key}
                {typeof fact.metadata?.occurrence_count === "number" ? ` · ${String(fact.metadata.occurrence_count)} Lauf/Läufe` : ""}
                {typeof fact.metadata?.previous_run_id === "string" && fact.metadata.previous_run_id ? ` · Vorläufer ${fact.metadata.previous_run_id}` : ""}
              </p>
            </div>
          ))}
        </section>
      )}

      {globalAtomicFacts.length > 0 && (
        <section className="hsection">
          <h2 className="hsection-title">
            Globales Faktenregister
            <span className="hsection-count">{boot?.atomic_fact_registry?.unique_fact_count ?? globalAtomicFacts.length}</span>
          </h2>
          {globalAtomicFacts.map((fact) => (
            <div className="hitem" key={`${fact.fact_key}:${fact.run_id}`}>
              <div className="hitem-head">
                <span className={`badge badge-${fact.status === "confirmed" ? "approved" : fact.status === "resolved" ? "completed" : fact.status === "superseded" ? "rejected" : "pending"}`}>{de(fact.status)}</span>
                <span className="badge badge-cat">{actionLaneDe(fact.action_lane)}</span>
              </div>
              <strong>{fact.summary}</strong>
              <p>
                {fact.fact_key} · Run {fact.run_id} · {fact.occurrence_count} Lauf/Läufe
                {fact.carry_over_mode ? ` · ${fact.carry_over_mode}` : ""}
                {fact.previous_run_id ? ` · Vorläufer ${fact.previous_run_id}` : ""}
              </p>
              <p className="text-secondary">
                {(fact.scope_summary ? `${fact.scope_summary} · ` : "")}
                {fact.root_cause_bucket ? `Ursache ${fact.root_cause_bucket} · ` : ""}
                Behauptungen {fact.claim_count} · Wahrheiten {fact.truth_count}
              </p>
              {(fact.subject_keys.length > 0 || fact.source_types.length > 0) && (
                <p className="text-secondary">
                  {fact.subject_keys.slice(0, 2).join(", ") || "–"} · {fact.source_types.map(sourceTypeLabel).join(", ") || "–"}
                </p>
              )}
              {fact.last_status_comment && <p className="text-secondary">{fact.last_status_comment}</p>}
            </div>
          ))}
        </section>
      )}

      {boot?.quality_gate?.gold_set && boot?.quality_gate?.delta_recompute && (
        <section className="hsection">
          <h2 className="hsection-title">
            Qualitäts-Gate
            <span className="hsection-count">
              {boot.quality_gate.gold_set.passed && boot.quality_gate.delta_recompute.passed ? "grün" : "offen"}
            </span>
          </h2>
          <div className="hitem">
            <div className="hitem-head">
              <span className={`badge badge-${boot.quality_gate.gold_set.passed ? "approved" : "pending"}`}>
                {boot.quality_gate.gold_set.passed ? "Referenz-Set grün" : "Referenz-Set offen"}
              </span>
            </div>
            <strong>Referenz-Benchmark</strong>
            <p>
              {boot.quality_gate.gold_set.matched_expectations}/{boot.quality_gate.gold_set.total_expectations} Erwartungen erfüllt ·
              {" "}Trefferquote {Math.round(boot.quality_gate.gold_set.recall * 100)}% ·
              {" "}Präzision {Math.round(boot.quality_gate.gold_set.precision * 100)}%
            </p>
          </div>
          <div className="hitem">
            <div className="hitem-head">
              <span className={`badge badge-${boot.quality_gate.delta_recompute.passed ? "approved" : "pending"}`}>
                {boot.quality_gate.delta_recompute.passed ? "Delta grün" : "Delta offen"}
              </span>
            </div>
            <strong>Delta-Neuberechnung</strong>
            <p>
              {boot.quality_gate.delta_recompute.matched_expectations}/{boot.quality_gate.delta_recompute.total_expectations} Erwartungen erfüllt ·
              {" "}Trefferquote {Math.round(boot.quality_gate.delta_recompute.recall * 100)}% ·
              {" "}Präzision {Math.round(boot.quality_gate.delta_recompute.precision * 100)}%
            </p>
          </div>
        </section>
      )}

      {(boot?.go_live_gate || boot?.operational_alerts) && (
        <section className="hsection">
          <h2 className="hsection-title">
            Betriebsfreigabe
            <span className="hsection-count">
              {boot?.go_live_gate?.ready ? "bereit" : "offen"}
            </span>
          </h2>
          {boot?.go_live_gate && (
            <div className="hitem">
              <div className="hitem-head">
                <span className={`badge badge-${boot.go_live_gate.ready ? "approved" : "pending"}`}>
                  {boot.go_live_gate.ready ? "Go-Live-Gate grün" : "Go-Live-Gate offen"}
                </span>
              </div>
              <strong>Operative Freigabe</strong>
              <p>
                {boot.go_live_gate.checks.filter((check) => check.passed).length}/{boot.go_live_gate.checks.length} Gates erfüllt
              </p>
              {boot.go_live_gate.blocking_gates.length > 0 && (
                <ul>
                  {boot.go_live_gate.blocking_gates.map((item) => <li key={item}>{item}</li>)}
                </ul>
              )}
            </div>
          )}
          {boot?.operational_alerts && (
            <div className="hitem">
              <div className="hitem-head">
                <span className={`badge badge-${boot.operational_alerts.status === "ok" ? "approved" : "pending"}`}>
                  {boot.operational_alerts.status === "ok" ? "Operative Signale ruhig" : "Operative Signale offen"}
                </span>
              </div>
              <strong>Monitoring & Recovery</strong>
              <p>
                Traces {boot.operational_alerts.observability_signals.trace_count} ·
                {" "}Metriken {boot.operational_alerts.observability_signals.metric_sample_count} ·
                {" "}Fehlerspans 24h {boot.operational_alerts.observability_signals.recent_error_span_count} ·
                {" "}stale/reclaimbar {boot.operational_alerts.recovery_signals.reclaimable_run_count}
              </p>
              {(boot.operational_alerts.blockers.length > 0 || boot.operational_alerts.warnings.length > 0) && (
                <ul>
                  {boot.operational_alerts.blockers.map((item) => <li key={`b:${item}`}>Blocker: {item}</li>)}
                  {boot.operational_alerts.warnings.map((item) => <li key={`w:${item}`}>Warnung: {item}</li>)}
                </ul>
              )}
            </div>
          )}
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
