import { useState } from "react";
import type {
  AuditRun,
  AuditTarget,
  AtlassianAuthStatus,
  BootstrapData,
  SourceProfile,
} from "../types";

type RunsViewProps = {
  runs: AuditRun[];
  selectedRunId: string;
  onSelectRun: (runId: string) => void;
  onCreateRun: (target: AuditTarget) => Promise<void>;
  onRefreshRuns: () => Promise<void>;
  bootstrap: BootstrapData | null;
  atlassianAuth: AtlassianAuthStatus | null;
  submitting: boolean;
  error: string;
  onNavigate: (view: string) => void;
  onRefreshAtlassianStatus: () => Promise<void>;
  onStartAtlassianAuth: () => Promise<void>;
  onVerifyConfluence: () => Promise<void>;
  atlassianAuthSubmitting: boolean;
  atlassianAuthError: string;
  confluenceVerifyMessage: string;
};

function formatTs(value?: string | null): string {
  if (!value) return "–";
  return new Intl.DateTimeFormat("de-DE", { dateStyle: "short", timeStyle: "short" }).format(
    new Date(value)
  );
}

const EMPTY_PROFILE: SourceProfile = {
  confluence_url: "",
  jira_url: "",
  confluence_space_key: "FINAI",
  jira_project_key: "FINAI",
  jira_usage: "ticket_creation_only",
  metamodel_dump_path: "",
  metamodel_policy: "",
  resource_access_mode: "read_only",
};

export function RunsView({
  runs,
  selectedRunId,
  onSelectRun,
  onCreateRun,
  onRefreshRuns,
  bootstrap,
  atlassianAuth,
  submitting,
  error,
  onNavigate,
  onRefreshAtlassianStatus,
  onStartAtlassianAuth,
  onVerifyConfluence,
  atlassianAuthSubmitting,
  atlassianAuthError,
  confluenceVerifyMessage,
}: RunsViewProps): JSX.Element {
  const [showForm, setShowForm] = useState(false);
  const defaults = bootstrap?.defaults;
  const [form, setForm] = useState<AuditTarget>({
    github_repo_url: defaults?.github_repo_url ?? "",
    local_repo_path: defaults?.local_repo_path ?? "",
    github_ref: defaults?.github_ref ?? "main",
    confluence_space_keys: defaults?.confluence_space_keys ?? ["FINAI"],
    jira_project_keys: defaults?.jira_project_keys ?? ["FINAI"],
    include_metamodel: true,
    include_local_docs: defaults?.include_local_docs ?? true,
  });

  const sourceProfile = bootstrap?.source_profile ?? EMPTY_PROFILE;
  const effectiveAuth = atlassianAuth ?? bootstrap?.atlassian_auth;
  const confluenceReady = bootstrap?.capabilities.confluence_live_read_ready ?? false;
  const jiraWriteReady = bootstrap?.capabilities.jira_write_scope_ready ?? false;
  const oauthReady = bootstrap?.capabilities.atlassian_oauth_ready ?? false;

  async function handleSubmit(e: React.FormEvent): Promise<void> {
    e.preventDefault();
    await onCreateRun({
      ...form,
      confluence_space_keys: [sourceProfile.confluence_space_key],
      jira_project_keys: [sourceProfile.jira_project_key],
      include_metamodel: true,
    });
    setShowForm(false);
  }

  return (
    <>
      {/* Source overview */}
      <div className="source-cards">
        <a className="source-card" href={sourceProfile.confluence_url} target="_blank" rel="noreferrer">
          <span className="source-card-type">Analysequelle</span>
          <span className="source-card-name">{sourceProfile.confluence_space_key}</span>
          <span className="source-card-url">{sourceProfile.confluence_url}</span>
        </a>
        <div className="source-card">
          <span className="source-card-type">Analysequelle</span>
          <span className="source-card-name">Metamodell</span>
          <span className="source-card-url">{sourceProfile.metamodel_dump_path}</span>
        </div>
        <a className="source-card" href={sourceProfile.jira_url} target="_blank" rel="noreferrer">
          <span className="source-card-type">Ticket-Ziel</span>
          <span className="source-card-name">{sourceProfile.jira_project_key}</span>
          <span className="source-card-url">{sourceProfile.jira_url}</span>
        </a>
      </div>

      {/* Atlassian connection */}
      <div className="card card-sm mb-xl">
        <div className="flex-row justify-between mb-lg">
          <div>
            <h3 style={{ fontSize: "var(--font-size-md)", fontWeight: 600 }}>Atlassian-Verbindung</h3>
            <p className="text-xs text-muted mt-xs">
              {effectiveAuth?.token_valid
                ? "Lokaler Access Token vorhanden."
                : "Für echte Confluence-Live-Reads wird ein 3LO-Consent benötigt."}
            </p>
          </div>
          <div className="flex-row gap-sm">
            <span className={`badge badge-status ${confluenceReady ? "badge-completed" : "badge-planned"}`}>
              {confluenceReady ? "Live-Read ✓" : "Live-Read ✗"}
            </span>
            <span className={`badge badge-status ${jiraWriteReady ? "badge-completed" : "badge-planned"}`}>
              {jiraWriteReady ? "Jira Write ✓" : "Jira Write ✗"}
            </span>
          </div>
        </div>
        {atlassianAuthError ? <div className="error-box">{atlassianAuthError}</div> : null}
        {confluenceVerifyMessage ? <div className="info-box">{confluenceVerifyMessage}</div> : null}
        <div className="form-actions">
          <button className="btn btn-secondary btn-sm" onClick={() => void onRefreshAtlassianStatus()} disabled={atlassianAuthSubmitting}>
            Status laden
          </button>
          <button className="btn btn-secondary btn-sm" onClick={() => void onStartAtlassianAuth()} disabled={atlassianAuthSubmitting || !bootstrap?.capabilities.atlassian_configured}>
            Confluence verbinden
          </button>
          <button className="btn btn-secondary btn-sm" onClick={() => void onVerifyConfluence()} disabled={atlassianAuthSubmitting || !effectiveAuth?.token_valid}>
            Live-Read prüfen
          </button>
        </div>
      </div>

      {/* Runs list header */}
      <div className="section-head">
        <div>
          <h2>Audit Runs</h2>
          <p>{runs.length} gespeicherte Läufe</p>
        </div>
        <div className="form-actions">
          <button className="btn btn-secondary btn-sm" onClick={() => void onRefreshRuns()}>Aktualisieren</button>
          <button className="btn btn-primary" onClick={() => setShowForm(true)} id="btn-new-run">
            + Neuer Audit-Run
          </button>
        </div>
      </div>

      {error ? <div className="error-box">{error}</div> : null}

      {runs.length === 0 ? (
        <div className="empty-state">
          <div className="empty-state-icon">🔄</div>
          <strong>Noch keine Audit-Runs</strong>
          <p>Lege den ersten Audit-Run an, um die Analyse zu starten.</p>
        </div>
      ) : (
        <div className="run-list">
          {runs.map((run) => (
            <button
              key={run.run_id}
              className={`run-list-item${run.run_id === selectedRunId ? " selected" : ""}`}
              onClick={() => { onSelectRun(run.run_id); onNavigate("packages"); }}
              id={`run-${run.run_id}`}
            >
              <div className="run-list-info">
                <div className="flex-row gap-sm mb-lg" style={{ marginBottom: 4 }}>
                  <span className={`badge badge-status badge-${run.status}`}>{run.status}</span>
                  <span className="run-list-title">
                    {run.target.local_repo_path || run.target.github_repo_url || run.run_id}
                  </span>
                </div>
                <div className="run-list-meta">
                  {formatTs(run.created_at)} · {run.progress.phase_label} · {run.progress.progress_pct}%
                </div>
              </div>
              <div className="run-list-counts">
                <div className="run-count">
                  <span className="run-count-value">{run.findings.length}</span>
                  <span className="run-count-label">Findings</span>
                </div>
                <div className="run-count">
                  <span className="run-count-value">{run.decision_packages.length}</span>
                  <span className="run-count-label">Pakete</span>
                </div>
                <div className="run-count">
                  <span className="run-count-value">{run.claims.length}</span>
                  <span className="run-count-label">Claims</span>
                </div>
              </div>
            </button>
          ))}
        </div>
      )}

      {/* New run modal */}
      {showForm ? (
        <div className="new-run-overlay" onClick={() => setShowForm(false)}>
          <form
            className="new-run-dialog"
            onClick={(e) => e.stopPropagation()}
            onSubmit={(e) => void handleSubmit(e)}
          >
            <h2>Neuer Audit-Run</h2>
            <p>
              Confluence und Metamodell sind feste Analysequellen, Jira nur Ticket-Ziel. Bedienbar
              bleiben Repo-Quelle, Ref und lokale Doku.
            </p>

            <div className="form-stack">
              <div className="form-field">
                <label>Lokaler FIN-AI Repo-Pfad</label>
                <input
                  value={form.local_repo_path || ""}
                  onChange={(e) => setForm({ ...form, local_repo_path: e.target.value })}
                  placeholder="/Users/martinwaelter/GitHub/FIN-AI"
                />
              </div>
              <div className="form-row">
                <div className="form-field">
                  <label>GitHub Repo URL</label>
                  <input
                    value={form.github_repo_url || ""}
                    onChange={(e) => setForm({ ...form, github_repo_url: e.target.value })}
                    placeholder="https://github.com/FINIUS-GmbH/FIN-AI.git"
                  />
                </div>
                <div className="form-field">
                  <label>Git Ref</label>
                  <input
                    value={form.github_ref}
                    onChange={(e) => setForm({ ...form, github_ref: e.target.value })}
                    required
                  />
                </div>
              </div>
              <label className="checkbox-label">
                <input
                  type="checkbox"
                  checked={form.include_local_docs}
                  onChange={(e) => setForm({ ...form, include_local_docs: e.target.checked })}
                />
                <span>Lokale _docs und Arbeitsdokumente einbeziehen</span>
              </label>

              <div className="policy-box">
                <strong>Feste Quellen</strong>
                <p>
                  Confluence-Space {sourceProfile.confluence_space_key} · Jira-Zielprojekt{" "}
                  {sourceProfile.jira_project_key} · Metamodell immer aktiv · Jira nicht Teil der Analyse
                </p>
              </div>

              <div className="form-actions">
                <button type="submit" className="btn btn-primary btn-lg" disabled={submitting}>
                  {submitting ? "Lege an…" : "Audit-Run anlegen"}
                </button>
                <button type="button" className="btn btn-secondary" onClick={() => setShowForm(false)}>
                  Abbrechen
                </button>
              </div>
            </div>
          </form>
        </div>
      ) : null}
    </>
  );
}
