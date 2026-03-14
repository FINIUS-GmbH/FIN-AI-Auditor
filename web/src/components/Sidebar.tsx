import type { BootstrapData, AtlassianAuthStatus, AuditRun } from "../types";

type SidebarProps = {
  activeView: string;
  onNavigate: (view: string) => void;
  runs: AuditRun[];
  selectedRun: AuditRun | null;
  bootstrap: BootstrapData | null;
  atlassianAuth: AtlassianAuthStatus | null;
};

const NAV_ITEMS = [
  { key: "dashboard", label: "Dashboard", icon: "📊" },
  { key: "runs", label: "Audit Runs", icon: "🔄" },
  { key: "packages", label: "Entscheidungen", icon: "📦" },
  { key: "findings", label: "Findings", icon: "🔍" },
  { key: "approvals", label: "Freigaben", icon: "✅" },
  { key: "truths", label: "Truth Ledger", icon: "📜" },
  { key: "log", label: "Aktivitätslog", icon: "📋" },
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
              <span className="nav-item-icon">{item.icon}</span>
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
