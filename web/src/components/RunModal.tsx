import { useCallback, useEffect, useState, type ReactNode } from "react";
import {
  getAtlassianAuthStatus,
  listConfluencePages,
  startAtlassianAuthorization,
  type ConfluencePageNode,
} from "../api";
import type { AtlassianAuthStatus, AuditTarget, BootstrapData, SourceProfile } from "../types";

/* ============================================================
   Tree helpers
   ============================================================ */

interface TreeNode {
  id: string;
  title: string;
  children: TreeNode[];
}

function buildTree(pages: ConfluencePageNode[]): TreeNode[] {
  const map = new Map<string, TreeNode>();
  for (const p of pages) map.set(p.id, { id: p.id, title: p.title, children: [] });
  const roots: TreeNode[] = [];
  for (const p of pages) {
    const node = map.get(p.id)!;
    const parent = p.parentId ? map.get(p.parentId) : null;
    if (parent) parent.children.push(node);
    else roots.push(node);
  }
  return roots;
}

/* ============================================================
   TreeView component
   ============================================================ */

function TreeItem({ node, checked, onToggle, depth }: {
  node: TreeNode;
  checked: Set<string>;
  onToggle: (id: string) => void;
  depth: number;
}): ReactNode {
  const [open, setOpen] = useState(depth < 1);
  const hasChildren = node.children.length > 0;
  return (
    <div className="tree-item">
      <div className="tree-row" style={{ paddingLeft: depth * 20 + 8 }}>
        {hasChildren ? (
          <button type="button" className="tree-toggle" onClick={() => setOpen(!open)}>
            {open ? "▾" : "▸"}
          </button>
        ) : (
          <span className="tree-toggle-spacer" />
        )}
        <label className="tree-label">
          <input type="checkbox" checked={checked.has(node.id)} onChange={() => onToggle(node.id)} />
          <span>{node.title || `Page ${node.id}`}</span>
        </label>
      </div>
      {open && hasChildren && node.children.map(c => (
        <TreeItem key={c.id} node={c} checked={checked} onToggle={onToggle} depth={depth + 1} />
      ))}
    </div>
  );
}

/* ============================================================
   RunModal
   ============================================================ */

type Step = "auth" | "scope" | "confirm";

// Fixed config — not user-editable
const GITHUB_URL = "https://github.com/FINIUS-GmbH/FIN-AI";
const LOCAL_PATH = "../FIN-AI";
const GIT_REF = "main";

interface RunModalProps {
  ea: AtlassianAuthStatus;
  sp: SourceProfile;
  boot: BootstrapData | null;
  onClose: () => void;
  onStart: (t: AuditTarget) => Promise<void>;
  submitting: boolean;
}

export default function RunModal({ ea, sp, onClose, onStart, submitting }: RunModalProps): ReactNode {
  const [step, setStep] = useState<Step>(ea.token_valid ? "scope" : "auth");
  const [authStatus, setAuthStatus] = useState<AtlassianAuthStatus>(ea);
  const [authBusy, setAuthBusy] = useState(false);
  const [authErr, setAuthErr] = useState("");

  // Scope step
  const [treeData, setTreeData] = useState<TreeNode[]>([]);
  const [treeBusy, setTreeBusy] = useState(false);
  const [treeErr, setTreeErr] = useState("");
  const [selectedPages, setSelectedPages] = useState<Set<string>>(new Set());
  const [selectAll, setSelectAll] = useState(true);
  const [spaceKey, setSpaceKey] = useState(sp.confluence_space_key || "FINAI");
  const [spaceName, setSpaceName] = useState("");

  const loadTree = useCallback(async (key: string) => {
    setTreeBusy(true);
    setTreeErr("");
    try {
      const data = await listConfluencePages(key);
      const tree = buildTree(data.pages);
      setTreeData(tree);
      setSpaceName(data.space_name);
      const all = new Set(data.pages.map(p => p.id));
      setSelectedPages(all);
      setSelectAll(true);
    } catch (e) {
      setTreeErr(e instanceof Error ? e.message : String(e));
    } finally {
      setTreeBusy(false);
    }
  }, []);

  useEffect(() => {
    if (step === "scope" && treeData.length === 0 && !treeBusy) {
      void loadTree(spaceKey);
    }
  }, [step, treeData.length, treeBusy, loadTree, spaceKey]);

  // Auth handlers
  async function doConnect() {
    setAuthBusy(true);
    setAuthErr("");
    // Open popup BEFORE async call to avoid popup blocker
    const popupName = `auditor-atl-auth-modal-${Date.now()}`;
    let popup: Window | null = null;
    try {
      popup = window.open("", popupName, "width=600,height=700,left=200,top=100");
      if (popup && !popup.closed) {
        try { popup.document.title = "Atlassian Anmeldung"; popup.document.body.innerHTML = "<p>Atlassian-Authentifizierung wird gestartet…</p>"; } catch { /* */ }
      }
    } catch { popup = null; }
    try {
      const s = await startAtlassianAuthorization();
      const url = s.authorization_url;
      if (!url) throw new Error("authorization_url fehlt");
      if (popup && !popup.closed) {
        const reused = window.open(url, popupName, "width=600,height=700,left=200,top=100");
        if (reused && !reused.closed) popup = reused;
      } else {
        popup = window.open(url, "_blank");
      }
      // Poll for auth completion while popup is open
      const pollId = window.setInterval(async () => {
        try {
          if (popup && popup.closed) { window.clearInterval(pollId); void checkAuth(); return; }
          const status = await getAtlassianAuthStatus();
          if (status.token_valid) { window.clearInterval(pollId); setAuthStatus(status); setStep("scope"); try { popup?.close(); } catch { /* */ } }
        } catch { /* ignore */ }
      }, 2000);
      setTimeout(() => window.clearInterval(pollId), 5 * 60 * 1000);
    } catch (e) {
      try { popup?.close(); } catch { /* */ }
      setAuthErr(String(e));
    } finally { setAuthBusy(false); }
  }

  async function checkAuth() {
    setAuthBusy(true);
    try {
      const s = await getAtlassianAuthStatus();
      setAuthStatus(s);
      if (s.token_valid) setStep("scope");
    } catch (e) { setAuthErr(String(e)); }
    finally { setAuthBusy(false); }
  }

  function togglePage(id: string) {
    setSelectedPages(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
    setSelectAll(false);
  }

  function toggleAll() {
    if (selectAll) {
      setSelectedPages(new Set());
      setSelectAll(false);
    } else {
      const all = new Set<string>();
      function collect(nodes: TreeNode[]) { for (const n of nodes) { all.add(n.id); collect(n.children); } }
      collect(treeData);
      setSelectedPages(all);
      setSelectAll(true);
    }
  }

  function handleStart() {
    void onStart({
      github_repo_url: GITHUB_URL,
      local_repo_path: LOCAL_PATH,
      github_ref: GIT_REF,
      confluence_space_keys: [spaceKey],
      jira_project_keys: [sp.jira_project_key],
      include_metamodel: true,
      include_local_docs: true,
    });
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal modal-lg" onClick={e => e.stopPropagation()}>
        {/* Step indicator */}
        <div className="modal-steps">
          {(["auth", "scope", "confirm"] as Step[]).map((s, i) => (
            <div key={s} className={`modal-step ${step === s ? "active" : (step === "confirm" && s !== "confirm") || (step === "scope" && s === "auth") ? "done" : ""}`}>
              <span className="modal-step-num">{i + 1}</span>
              <span className="modal-step-label">{s === "auth" ? "Anmelden" : s === "scope" ? "Prüfbereich" : "Starten"}</span>
            </div>
          ))}
        </div>

        {/* ── Step 1: Auth ── */}
        {step === "auth" && (
          <div className="modal-body">
            <h2>Atlassian-Anmeldung</h2>
            <p>Verbinde dich mit Atlassian, um Confluence-Seiten als Audit-Quelle zu nutzen.</p>
            {authErr && <div className="error-box">{authErr}</div>}
            <div className="auth-status-box">
              <div className="conn"><span className={`conn-dot ${authStatus.token_valid ? "ok" : "off"}`} />
                {authStatus.token_valid ? "✓ Verbunden" : "✗ Nicht verbunden"}
              </div>
              {authStatus.scope && <p className="text-xs text-muted" style={{ marginTop: 8 }}>Scopes: {authStatus.scope}</p>}
            </div>
            <div className="form-actions">
              <button type="button" className="btn btn-primary" onClick={() => void doConnect()} disabled={authBusy}>
                {authBusy ? "Öffne Login…" : "Mit Atlassian verbinden"}
              </button>
              <button type="button" className="btn btn-outline" onClick={() => void checkAuth()} disabled={authBusy}>
                Status prüfen
              </button>
              {authStatus.token_valid && (
                <button type="button" className="btn btn-primary" onClick={() => setStep("scope")}>Weiter →</button>
              )}
            </div>
          </div>
        )}

        {/* ── Step 2: Scope / TreeView ── */}
        {step === "scope" && (
          <div className="modal-body">
            <h2>Prüfbereich auswählen</h2>
            <p>Wähle die Confluence-Seiten aus, die du prüfen möchtest.</p>
            <div className="form-field" style={{ marginBottom: 12 }}>
              <label>Space Key</label>
              <div className="flex-row gap-sm">
                <input value={spaceKey} onChange={e => setSpaceKey(e.target.value.toUpperCase())} style={{ width: 120 }} />
                <button type="button" className="btn btn-outline btn-sm" onClick={() => { setTreeData([]); void loadTree(spaceKey); }} disabled={treeBusy}>Laden</button>
                {spaceName && <span className="text-sm text-muted">{spaceName}</span>}
              </div>
            </div>

            {treeErr && <div className="error-box">{treeErr}</div>}

            {treeBusy ? (
              <div className="tree-loading">Lade Seitenstruktur…</div>
            ) : treeData.length > 0 ? (
              <div className="tree-container">
                <div className="tree-header">
                  <label className="tree-label">
                    <input type="checkbox" checked={selectAll} onChange={toggleAll} />
                    <strong>Alle auswählen</strong>
                  </label>
                  <span className="text-xs text-muted">{selectedPages.size} ausgewählt</span>
                </div>
                <div className="tree-scroll">
                  {treeData.map(n => <TreeItem key={n.id} node={n} checked={selectedPages} onToggle={togglePage} depth={0} />)}
                </div>
              </div>
            ) : !treeErr ? (
              <p className="text-muted">Keine Seiten im Space gefunden.</p>
            ) : null}

            <div className="form-actions">
              <button type="button" className="btn btn-outline" onClick={() => setStep("auth")}>← Zurück</button>
              <button type="button" className="btn btn-primary" onClick={() => setStep("confirm")} disabled={selectedPages.size === 0}>
                Weiter → ({selectedPages.size} Seiten)
              </button>
            </div>
          </div>
        )}

        {/* ── Step 3: Confirm ── */}
        {step === "confirm" && (
          <div className="modal-body">
            <h2>Audit-Run starten</h2>
            <div className="confirm-summary">
              <div className="confirm-row">{CONF_SVG} <strong>{selectedPages.size} Confluence-Seiten</strong> aus Space <strong>{spaceKey}</strong></div>
              <div className="confirm-row">{GH_SVG} <strong>FINIUS-GmbH/FIN-AI</strong> @ <code>{GIT_REF}</code></div>
              <div className="confirm-row">◆ <strong>Metamodell</strong> — lokal</div>
            </div>
            <div className="form-actions">
              <button type="button" className="btn btn-outline" onClick={() => setStep("scope")}>← Zurück</button>
              <button type="button" className="btn btn-primary btn-lg" onClick={handleStart} disabled={submitting}>
                {submitting ? "Lege an…" : "🚀 Audit-Run starten"}
              </button>
              <button type="button" className="btn btn-outline" onClick={onClose}>Abbrechen</button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

/* Small inline SVGs for the confirm summary */
const CONF_SVG = <svg width="16" height="16" viewBox="0 0 32 32" fill="var(--accent)" style={{ verticalAlign: "middle", marginRight: 4 }}><path d="M3.82 22.54c-.29.47-.62 1-.87 1.34a1.2 1.2 0 00.37 1.66l5.17 3.18a1.2 1.2 0 001.66-.42c.2-.34.52-.86.89-1.43 2.64-4.14 5.3-3.63 10.11-1.62l5.31 2.23a1.2 1.2 0 001.57-.65l2.54-5.87a1.2 1.2 0 00-.63-1.57c-1.57-.68-4.68-1.99-6.27-2.67C14.5 13.53 8.62 14.17 3.82 22.54zM28.18 9.46c.29-.47.62-1 .87-1.34a1.2 1.2 0 00-.37-1.66L23.5 3.28a1.2 1.2 0 00-1.66.42c-.2.34-.52.86-.89 1.43-2.64 4.14-5.3 3.63-10.11 1.62L5.53 4.52a1.2 1.2 0 00-1.57.65L1.42 11.04a1.2 1.2 0 00.63 1.57c1.57.68 4.68 1.99 6.27 2.67 9.08 3.69 14.96 3.05 19.86-5.82z"/></svg>;
const GH_SVG = <svg width="16" height="16" viewBox="0 0 16 16" fill="var(--text-primary)" style={{ verticalAlign: "middle", marginRight: 4 }}><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>;
