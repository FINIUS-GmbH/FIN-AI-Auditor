import type { BootstrapData, AtlassianAuthStatus, AuditRun } from "../types";

type SidebarProps = {
  activeView: string;
  onNavigate: (view: string) => void;
  runs: AuditRun[];
  selectedRun: AuditRun | null;
  bootstrap: BootstrapData | null;
  atlassianAuth: AtlassianAuthStatus | null;
};

const svgIcon = (d: string) => <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d={d}/></svg>;
const NAV_ICONS: Record<string, JSX.Element> = {
  dashboard: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg>,
  runs: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>,
  packages: svgIcon("M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"),
  findings: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>,
  approvals: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>,
  truths: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>,
  log: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg>,
};
const NAV_ITEMS = [
  { key: "dashboard", label: "Dashboard" },
  { key: "runs", label: "Audit-Läufe" },
  { key: "packages", label: "Entscheidungen" },
  { key: "findings", label: "Befunde" },
  { key: "approvals", label: "Freigaben" },
  { key: "truths", label: "Wahrheitsregister" },
  { key: "log", label: "Aktivitätslog" },
];

export function Sidebar({
  activeView,
  onNavigate,
  runs,
  selectedRun,
  bootstrap,
  atlassianAuth,
}: SidebarProps): JSX.Element {
  const openPackages = selectedRun?.decision_packages.filter(
    (p) => p.decision_state === "open"
  ).length ?? 0;
  const pendingApprovals = selectedRun?.approval_requests.filter(
    (a) => a.status === "pending"
  ).length ?? 0;
  const totalFindings = selectedRun?.findings.length ?? 0;

  const confluenceReady = bootstrap?.capabilities.confluence_live_read_ready ?? false;
  const metamodelSource = bootstrap?.source_profile.metamodel_source;
  const llmOk = bootstrap?.capabilities.llm_configured ?? false;
  const tokenValid = atlassianAuth?.token_valid ?? false;

  function getBadge(key: string): { count: number; warn: boolean } | null {
    switch (key) {
      case "runs":
        return runs.length > 0 ? { count: runs.length, warn: false } : null;
      case "packages":
        return openPackages > 0 ? { count: openPackages, warn: true } : null;
      case "findings":
        return totalFindings > 0 ? { count: totalFindings, warn: false } : null;
      case "approvals":
        return pendingApprovals > 0 ? { count: pendingApprovals, warn: true } : null;
      default:
        return null;
    }
  }

  return (
    <nav className="sidebar" id="sidebar-nav">
      <div className="sidebar-brand">
        <div className="sidebar-brand-icon">🛡</div>
        <div className="sidebar-brand-text">
          <strong>FIN-AI Auditor</strong>
          <small>Governance Workbench</small>
        </div>
      </div>

      <div className="sidebar-nav">
        <div className="nav-section-label">Navigation</div>
        {NAV_ITEMS.map((item) => {
          const badge = getBadge(item.key);
          return (
            <button
              key={item.key}
              className={`nav-item${activeView === item.key ? " active" : ""}`}
              onClick={() => onNavigate(item.key)}
              id={`nav-${item.key}`}
            >
              <span className="nav-item-icon">{NAV_ICONS[item.key]}</span>
              <span>{item.label}</span>
              {badge ? (
                <span className={`nav-item-badge${badge.warn ? " warn" : ""}`}>
                  {badge.count}
                </span>
              ) : null}
            </button>
          );
        })}
      </div>

      <div className="sidebar-footer">
        <div className="connection-indicator">
          <span className={`connection-dot ${tokenValid && confluenceReady ? "connected" : tokenValid ? "partial" : "disconnected"}`} />
          <span>Confluence</span>
        </div>
        <div className="connection-indicator">
          <span className={`connection-dot ${metamodelSource === "DIRECT" ? "connected" : "partial"}`} />
          <span>Metamodell</span>
        </div>
        <div className="connection-indicator">
          <span className={`connection-dot ${llmOk ? "connected" : "disconnected"}`} />
          <span>LLM ({bootstrap?.capabilities.llm_slot_count ?? 0} Slots)</span>
        </div>
      </div>
    </nav>
  );
}
