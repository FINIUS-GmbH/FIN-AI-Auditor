import { useEffect, useState, useMemo, useRef, type ReactNode } from "react";
import {
  listConfluencePages,
  renameConfluencePage,
  moveConfluencePage,
  type ConfluencePageNode,
  type ConfluencePageTree,
} from "../api";
import type { AuditRun, BootstrapData, SourceProfile } from "../types";

/* ============================================================
   TYPES
   ============================================================ */

type PageAnalysis = {
  id: string;
  title: string;
  parentId: string;
  depth: number;
  children: string[];
  docType: "fachlich" | "technisch" | "gemischt" | "unklar";
  issues: string[];
  crossRefs: number;
};

type DocKPI = {
  label: string;
  value: string | number;
  sub?: string;
  color?: string;
  icon: string;
  tooltip: string;
};

type StructureSuggestion = {
  id: string;
  type: "rename" | "move" | "merge" | "split" | "link" | "create";
  severity: "info" | "warning" | "improvement";
  title: string;
  description: string;
  affectedPages: string[];
};

/* ============================================================
   CONSTANTS
   ============================================================ */

const TECH_KEYWORDS = [
  "api", "code", "deployment", "docker", "pipeline", "cicd", "ci/cd",
  "architektur", "architecture", "schema", "migration", "config",
  "endpoint", "swagger", "openapi", "terraform", "helm", "kubernetes",
  "git", "repository", "branch", "build", "test", "unit", "e2e",
  "backend", "frontend", "service", "microservice", "database", "db",
  "redis", "neo4j", "postgres", "sql", "cypher", "query",
  "debugging", "troubleshooting", "monitoring", "logging", "log",
  "devops", "infrastructure", "infra", "server", "cluster",
  "cython", "pydantic", "fastapi", "react", "vite", "typescript",
];

const FACH_KEYWORDS = [
  "anforderung", "requirement", "prozess", "process", "workflow",
  "geschäft", "business", "kunde", "customer", "benutzer", "user",
  "anleitung", "guide", "handbuch", "manual", "tutorial",
  "faq", "hilfe", "help", "support", "bedienung", "nutzung",
  "konzept", "concept", "strategie", "strategy", "roadmap",
  "rollen", "role", "berechtigung", "permission", "governance",
  "bsm", "metamodell", "metamodel", "audit", "compliance",
  "dokumentation", "doku", "übersicht", "overview", "zusammenfassung",
  "entscheidung", "decision", "richtlinie", "policy", "vorgabe",
  "onboarding", "schulung", "training", "einführung",
];

/* Known docs in the Git repository — used to reference for linking */
const GIT_DOC_FILES = [
  { path: "docs/architecture.md", label: "Architektur", type: "technisch" as const },
  { path: "docs/data-model.md", label: "Datenmodell", type: "technisch" as const },
  { path: "docs/decision-packages-and-retrieval.md", label: "Decision Packages & Retrieval", type: "technisch" as const },
  { path: "docs/delta-sync-and-resolution.md", label: "Delta-Sync & Resolution", type: "technisch" as const },
  { path: "docs/product-scope.md", label: "Produktscope", type: "fachlich" as const },
  { path: "docs/production-readiness-runbook.md", label: "Production Readiness Runbook", type: "technisch" as const },
  { path: "docs/roadmap.md", label: "Roadmap", type: "fachlich" as const },
  { path: "docs/target-picture.md", label: "Zielbild", type: "fachlich" as const },
  { path: "docs/causal-graph-design.md", label: "Kausal-Graph Design", type: "technisch" as const },
  { path: "docs/README.md", label: "Doku-Übersicht", type: "fachlich" as const },
  { path: "README.md", label: "Repository README", type: "technisch" as const },
  { path: "PLAN.md", label: "Projektplan", type: "fachlich" as const },
];

/* Proposed target structure for Confluence */
const PROPOSED_STRUCTURE: { title: string; children: string[]; description: string; icon: string }[] = [
  {
    title: "🏠 Projektübersicht",
    children: ["Vision & Zielbild", "Roadmap & Releases", "Stakeholder & Rollen", "Entscheidungslog", "Glossar"],
    description: "Einstiegsseite für alle Beteiligten. Beantwortet: Was ist das Projekt, wohin geht es, wer ist beteiligt?",
    icon: "🏠",
  },
  {
    title: "📘 Fachliche Dokumentation",
    children: ["BSM-Prozess & Workflow", "Metamodell-Erklärung", "Audit-Konzept", "Benutzerhandbuch", "Admin-Handbuch", "FAQ & Troubleshooting"],
    description: "Alle fachlichen Konzepte in einfacher Sprache. Zielgruppe: Fachverantwortliche, Endbenutzer, Admins.",
    icon: "📘",
  },
  {
    title: "⚙️ Technische Referenz (Brücke zu Git)",
    children: ["Architektur → Zusammenfassung aus docs/architecture.md", "Datenmodell → Zusammenfassung aus docs/data-model.md", "API-Dokumentation → Link zu Swagger/OpenAPI", "Deployment & Infrastruktur → Zusammenfassung aus Runbook"],
    description: "Brükenseiten die automatisch generierte Git-Doku fachlich zusammenfassen. Keine Kopien — Kernaussagen extrahieren und verständlich aufbereiten.",
    icon: "⚙️",
  },
  {
    title: "🛠️ Betrieb & Administration",
    children: ["Monitoring & Logging", "Konfigurationsparameter", "Lizenzierung", "Backup & Recovery"],
    description: "Operative Dokumentation für Betriebsteam und Administratoren.",
    icon: "🛠️",
  },
  {
    title: "✅ Qualitätssicherung",
    children: ["Teststrategie & Testfälle", "Architektur-Guardrails", "Audit-Ergebnisse & Historie", "Release-Gate Dokumentation"],
    description: "Qualitäts- und Compliance-Nachweise. Referenzen zu automatisierten Checks.",
    icon: "✅",
  },
];

/* ============================================================
   ANALYSIS ENGINE
   ============================================================ */

function classifyPage(title: string): PageAnalysis["docType"] {
  const lower = title.toLowerCase();
  const techScore = TECH_KEYWORDS.filter(k => lower.includes(k)).length;
  const fachScore = FACH_KEYWORDS.filter(k => lower.includes(k)).length;
  if (techScore > 0 && fachScore > 0) return "gemischt";
  if (techScore > fachScore) return "technisch";
  if (fachScore > techScore) return "fachlich";
  return "unklar";
}

function analyzePages(pages: ConfluencePageNode[], run?: AuditRun | null): PageAnalysis[] {
  const childMap = new Map<string, string[]>();
  const parentMap = new Map<string, string>();
  for (const p of pages) {
    parentMap.set(p.id, p.parentId);
    if (p.parentId) {
      const siblings = childMap.get(p.parentId) ?? [];
      siblings.push(p.id);
      childMap.set(p.parentId, siblings);
    }
  }

  function depth(id: string): number {
    let d = 0;
    let cur = id;
    while (parentMap.has(cur) && parentMap.get(cur)) {
      cur = parentMap.get(cur)!;
      d++;
      if (d > 20) break;
    }
    return d;
  }

  return pages.map(p => {
    const children = childMap.get(p.id) ?? [];
    const d = depth(p.id);
    const issues: string[] = [];
    const docType = classifyPage(p.title);

    // Issue detection
    if (d > 4) issues.push("Zu tiefe Verschachtelung (> 4 Ebenen)");
    if (p.title.length > 80) issues.push("Seitentitel zu lang (> 80 Zeichen)");
    if (p.title.length < 4) issues.push("Seitentitel zu kurz (< 4 Zeichen)");
    if (/[_]{2,}|[-]{3,}/.test(p.title)) issues.push("Seitentitel enthält unschöne Sonderzeichen");
    if (children.length > 12) issues.push(`Zu viele direkte Unterseiten (${children.length})`);
    if (docType === "gemischt") issues.push("Mischung aus technischer und fachlicher Dokumentation");
    if (docType === "unklar") issues.push("Thematische Zuordnung unklar");
    // Check for duplicate-ish titles
    const similar = pages.filter(
      other => other.id !== p.id && other.title.toLowerCase().trim() === p.title.toLowerCase().trim()
    );
    if (similar.length > 0) issues.push("Mögliches Duplikat (gleicher Titel)");

    // Count cross-refs from audit findings
    const matchedFindings = run?.findings?.filter(f =>
      f.locations.some(l =>
        l.source_type === "confluence_page" &&
        (l.title.toLowerCase().includes(p.title.toLowerCase()) || l.source_id === p.id)
      )
    )?.length ?? 0;

    return {
      id: p.id,
      title: p.title,
      parentId: p.parentId,
      depth: d,
      children: children,
      docType,
      issues,
      crossRefs: matchedFindings,
    };
  });
}

function computeKPIs(analyses: PageAnalysis[], run: AuditRun | null): DocKPI[] {
  const total = analyses.length;
  const fach = analyses.filter(a => a.docType === "fachlich").length;
  const tech = analyses.filter(a => a.docType === "technisch").length;
  const gemischt = analyses.filter(a => a.docType === "gemischt").length;
  const unklar = analyses.filter(a => a.docType === "unklar").length;
  const withIssues = analyses.filter(a => a.issues.length > 0).length;
  const totalIssues = analyses.reduce((s, a) => s + a.issues.length, 0);
  const avgDepth = total > 0 ? (analyses.reduce((s, a) => s + a.depth, 0) / total).toFixed(1) : "0";
  const maxDepth = total > 0 ? Math.max(...analyses.map(a => a.depth)) : 0;
  const orphans = analyses.filter(a => !a.parentId && a.depth === 0 && a.children.length === 0).length;

  // Coverage score (percentage of pages that are clearly classified)
  const classified = fach + tech;
  const coverageRaw = total > 0 ? Math.round((classified / total) * 100) : 0;
  
  // Structure quality score
  const depthPenalty = analyses.filter(a => a.depth > 4).length;
  const tooManyChildren = analyses.filter(a => a.children.length > 12).length;
  const dupPenalty = analyses.filter(a => a.issues.some(i => i.includes("Duplikat"))).length;
  const structureScore = Math.max(0, Math.min(100,
    100 - (depthPenalty * 8) - (tooManyChildren * 10) - (dupPenalty * 15) - (gemischt * 5) - (unklar * 3)
  ));

  // Readability score
  const longTitles = analyses.filter(a => a.title.length > 60).length;
  const shortTitles = analyses.filter(a => a.title.length < 8).length;
  const readabilityScore = Math.max(0, Math.min(100,
    100 - (longTitles * 8) - (shortTitles * 5) - (gemischt * 6)
  ));

  // Cross-reference potential (how many pages reference each other via findings)
  const findingCount = run?.findings?.length ?? 0;
  const crossRefScore = findingCount > 0 ? Math.min(100, Math.round((findingCount / Math.max(total, 1)) * 20)) : 0;

  return [
    {
      label: "Seiten gesamt",
      value: total,
      icon: "📄",
      tooltip: "Gesamtzahl aller Confluence-Seiten im analysierten Bereich",
      color: "default",
    },
    {
      label: "Strukturqualität",
      value: `${structureScore}%`,
      sub: structureScore >= 80 ? "Gut" : structureScore >= 50 ? "Verbesserbar" : "Kritisch",
      icon: "🏗️",
      tooltip: "Bewertung der Seitenhierarchie: Tiefe, Gruppierung, Duplikate, Konsistenz",
      color: structureScore >= 80 ? "green" : structureScore >= 50 ? "amber" : "red",
    },
    {
      label: "Inhaltliche Abdeckung",
      value: `${coverageRaw}%`,
      sub: `${classified}/${total} zugeordnet`,
      icon: "📊",
      tooltip: "Anteil der Seiten mit klarer thematischer Zuordnung (fachlich/technisch)",
      color: coverageRaw >= 80 ? "green" : coverageRaw >= 50 ? "amber" : "red",
    },
    {
      label: "Lesbarkeit",
      value: `${readabilityScore}%`,
      sub: readabilityScore >= 80 ? "Gut" : "Verbesserbar",
      icon: "👁️",
      tooltip: "Bewertung der Seitentitel-Qualität: Länge, Klarheit, sprechende Benennung",
      color: readabilityScore >= 80 ? "green" : readabilityScore >= 50 ? "amber" : "red",
    },
    {
      label: "Querverweise",
      value: findingCount,
      sub: `Beziehungsdichte: ${crossRefScore}%`,
      icon: "🔗",
      tooltip: "Anzahl der erkannten Querbeziehungen zwischen Dokumenten und Code/Metamodell",
    },
    {
      label: "Probleme",
      value: totalIssues,
      sub: `${withIssues} Seiten betroffen`,
      icon: "⚠️",
      tooltip: "Erkannte Strukturprobleme: Tiefe, Duplikate, unscharfe Zuordnung, etc.",
      color: totalIssues === 0 ? "green" : totalIssues < 10 ? "amber" : "red",
    },
    {
      label: "Fachlich / Technisch",
      value: `${fach} / ${tech}`,
      sub: `${gemischt} gemischt · ${unklar} unklar`,
      icon: "📑",
      tooltip: "Aufteilung der Seiten nach Dokumentationstyp",
    },
    {
      label: "Ø Tiefe / Max",
      value: `${avgDepth} / ${maxDepth}`,
      sub: orphans > 0 ? `${orphans} verwaist` : "Keine verwaisten",
      icon: "📐",
      tooltip: "Durchschnittliche und maximale Verschachtelungstiefe der Seitenhierarchie",
    },
  ];
}

function generateSuggestions(analyses: PageAnalysis[], sp: SourceProfile): StructureSuggestion[] {
  const suggestions: StructureSuggestion[] = [];
  let sid = 0;

  // 1. Deep pages should be flattened
  const deepPages = analyses.filter(a => a.depth > 4);
  if (deepPages.length > 0) {
    suggestions.push({
      id: `s-${sid++}`,
      type: "move",
      severity: "warning",
      title: "Zu tiefe Seitenstruktur vereinfachen",
      description: `${deepPages.length} Seite(n) sind tiefer als 4 Ebenen verschachtelt. Confluence-Seiten sollten max. 3–4 Ebenen tief liegen, damit Nutzer sie schnell finden. Empfehlung: flachere Hierarchie mit thematischen Clustern.`,
      affectedPages: deepPages.map(p => p.title),
    });
  }

  // 2. Pages with too many children
  const fatParents = analyses.filter(a => a.children.length > 12);
  if (fatParents.length > 0) {
    suggestions.push({
      id: `s-${sid++}`,
      type: "split",
      severity: "warning",
      title: "Überladene Elternseiten aufteilen",
      description: `${fatParents.length} Seite(n) haben mehr als 12 direkte Unterseiten. Das erschwert die Navigation. Empfehlung: Gruppiere Unterseiten thematisch in Zwischenebenen.`,
      affectedPages: fatParents.map(p => `${p.title} (${p.children.length} Kinder)`),
    });
  }

  // 3. Mixed pages should be split
  const mixed = analyses.filter(a => a.docType === "gemischt");
  if (mixed.length > 0) {
    suggestions.push({
      id: `s-${sid++}`,
      type: "split",
      severity: "improvement",
      title: "Technische und fachliche Inhalte trennen",
      description: `${mixed.length} Seite(n) mischen technische und fachliche Dokumentation. Best Practice: Trenne in separate Seiten. Technische Details → Git/Code-Doku verlinken. Fachliches → in Confluence mit einfachen Erklärungen und Diagrammen aufbereiten.`,
      affectedPages: mixed.map(p => p.title),
    });
  }

  // 4. Unclear pages need classification
  const unclear = analyses.filter(a => a.docType === "unklar");
  if (unclear.length > 0) {
    suggestions.push({
      id: `s-${sid++}`,
      type: "rename",
      severity: "info",
      title: "Seiten mit unklarer Zuordnung einordnen",
      description: `${unclear.length} Seite(n) konnten nicht eindeutig als fachlich oder technisch klassifiziert werden. Empfehlung: Sprechende Titel vergeben, die den Inhalt und die Zielgruppe klar benennen (z.B. \"Benutzerhandbuch: Projektauswahl\" statt \"Auswahl\").`,
      affectedPages: unclear.map(p => p.title),
    });
  }

  // 5. Duplicate titles
  const titleMap = new Map<string, PageAnalysis[]>();
  for (const a of analyses) {
    const key = a.title.toLowerCase().trim();
    if (!titleMap.has(key)) titleMap.set(key, []);
    titleMap.get(key)!.push(a);
  }
  const dupes = [...titleMap.entries()].filter(([, v]) => v.length > 1);
  if (dupes.length > 0) {
    suggestions.push({
      id: `s-${sid++}`,
      type: "merge",
      severity: "warning",
      title: "Duplikate zusammenführen",
      description: `${dupes.length} Seitentitel existieren mehrfach. Redundanz vermeiden: Inhalte zusammenführen und Verweise statt Kopien nutzen.`,
      affectedPages: dupes.flatMap(([, v]) => v.map(p => p.title)),
    });
  }

  // 6. Best-practice suggestions
  suggestions.push({
    id: `s-${sid++}`,
    type: "create",
    severity: "info",
    title: "Empfohlene Gliederungsstruktur",
    description: `Best Practice für Projekt-Dokumentation:\n\n• Bereich 1 – Projektübersicht: Vision, Roadmap, Stakeholder, Entscheidungslog\n• Bereich 2 – Fachliche Doku: Prozesse, Rollen, Konzepte, Glossar, FAQ\n• Bereich 3 – Technische Referenz: Architektur, API, Datenmodell, Deployment (→ Links zu Git)\n• Bereich 4 – Betrieb & Admin: Runbooks, Monitoring, Konfiguration\n• Bereich 5 – Qualitätssicherung: Teststrategie, Guardrails, Audit-Ergebnisse`,
    affectedPages: [],
  });

  // 7. Git-Repo link suggestion
  suggestions.push({
    id: `s-${sid++}`,
    type: "link",
    severity: "info",
    title: "Code-Dokumentation in Git verlinken statt kopieren",
    description: `Technische Details (Code-Kommentare, API-Specs, README, Architektur-Docs) sollten im Git-Repository gepflegt und aus Confluence heraus verlinkt werden. So bleibt die technische Doku versioniert und es gibt keine veralteten Kopien. Beispiel: In Confluence eine Brückenseite „Technische Referenz" anlegen, die auf docs/ im Repository verweist.`,
    affectedPages: [],
  });

  // 8. Confluence diagrams suggestion
  suggestions.push({
    id: `s-${sid++}`,
    type: "create",
    severity: "info",
    title: "Confluence-taugliche Diagramme einbauen",
    description: `Empfehlung: Nutze Confluence-native Diagramme (z.B. draw.io, Mermaid via Makros) in fachlichen Seiten. Insbesondere:\n• Prozessfluss-Diagramme für BSM-Workflows\n• Komponentendiagramme für Systemzusammenhänge\n• Entscheidungsbäume für Audit-Logik\n\nDiagramme in Confluence statt als Screenshots einfügen — so bleiben sie editierbar.`,
    affectedPages: [],
  });

  return suggestions;
}


/* ============================================================
   TREE RENDERING
   ============================================================ */

function PageTreeNode({
  analysis,
  allAnalyses,
  expanded,
  onToggle,
  selectedId,
  onSelect,
  repoUrl,
}: {
  analysis: PageAnalysis;
  allAnalyses: PageAnalysis[];
  expanded: Set<string>;
  onToggle: (id: string) => void;
  selectedId: string | null;
  onSelect: (id: string) => void;
  repoUrl?: string;
}): ReactNode {
  const children = allAnalyses.filter(a => a.parentId === analysis.id);
  const hasChildren = children.length > 0;
  const isExpanded = expanded.has(analysis.id);
  const isSelected = selectedId === analysis.id;

  const docTypeCfg: Record<PageAnalysis["docType"], { label: string; cls: string; icon: string }> = {
    fachlich: { label: "Fachlich", cls: "ds-type-fach", icon: "📘" },
    technisch: { label: "Technisch", cls: "ds-type-tech", icon: "⚙️" },
    gemischt: { label: "Gemischt", cls: "ds-type-mix", icon: "🔀" },
    unklar: { label: "Unklar", cls: "ds-type-unklar", icon: "❓" },
  };
  const cfg = docTypeCfg[analysis.docType];

  return (
    <div className="ds-tree-item">
      <div
        className={`ds-tree-row ${isSelected ? "ds-tree-selected" : ""} ${analysis.issues.length > 0 ? "ds-tree-has-issues" : ""}`}
        style={{ paddingLeft: `${analysis.depth * 20 + 8}px` }}
        onClick={() => onSelect(analysis.id)}
      >
        {hasChildren ? (
          <button
            className="tree-toggle"
            onClick={(e) => { e.stopPropagation(); onToggle(analysis.id); }}
          >
            {isExpanded ? "▾" : "▸"}
          </button>
        ) : (
          <span className="tree-toggle-spacer" />
        )}
        <span className={`ds-type-badge ${cfg.cls}`} title={cfg.label}>{cfg.icon}</span>
        <span className="ds-tree-title">{analysis.title}</span>
        {analysis.issues.length > 0 && (
          <span className="ds-tree-issue-count" title={analysis.issues.join("\n")}>
            {analysis.issues.length} ⚠
          </span>
        )}
        {analysis.crossRefs > 0 && (
          <span className="ds-tree-finding-count" title={`${analysis.crossRefs} Audit-Finding(s) referenzieren diese Seite`}>
            {analysis.crossRefs} 🔍
          </span>
        )}
        {analysis.docType === "technisch" && repoUrl && (
          <span className="ds-tree-link-hint" title="Sollte ins Git-Repository verlinken">
            🔗
          </span>
        )}
        {hasChildren && (
          <span className="ds-tree-child-count">{children.length}</span>
        )}
      </div>
      {hasChildren && isExpanded && (
        <div className="ds-tree-children">
          {children.map(child => (
            <PageTreeNode
              key={child.id}
              analysis={child}
              allAnalyses={allAnalyses}
              expanded={expanded}
              onToggle={onToggle}
              selectedId={selectedId}
              onSelect={onSelect}
              repoUrl={repoUrl}
            />
          ))}
        </div>
      )}
    </div>
  );
}


/* ============================================================
   MAIN COMPONENT
   ============================================================ */

export default function DocStructureView({
  run,
  boot,
  sp,
}: {
  run: AuditRun | null;
  boot: BootstrapData | null;
  sp: SourceProfile;
}): ReactNode {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tree, setTree] = useState<ConfluencePageTree | null>(null);

  // Writeback state
  const [renameTarget, setRenameTarget] = useState<{ id: string; currentTitle: string } | null>(null);
  const [moveTarget, setMoveTarget] = useState<{ id: string; title: string } | null>(null);
  const [writebackBusy, setWritebackBusy] = useState(false);
  const [writebackError, setWritebackError] = useState<string | null>(null);
  const [writebackSuccess, setWritebackSuccess] = useState<string | null>(null);
  const renameInputRef = useRef<HTMLInputElement>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<"overview" | "tree" | "suggestions" | "separation" | "target">("overview");

  const spaceKey = sp.confluence_space_key || "FP";

  // Fetch pages on mount
  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      setError(null);
      try {
        const result = await listConfluencePages(spaceKey);
        if (cancelled) return;
        if (result.auth_required) {
          setError("Kein Atlassian-Token vorhanden. Bitte zuerst über die Sidebar OAuth-Anmeldung durchführen.");
        } else if (result.access_denied) {
          setError(result.error_message || "Zugriff verweigert.");
        } else {
          setTree(result);
          // Auto-expand first two levels
          const analyses = analyzePages(result.pages, null);
          const rootNodes = analyses.filter(a => !a.parentId || a.depth === 0);
          const autoExpand = new Set<string>();
          for (const r of rootNodes) {
            autoExpand.add(r.id);
            for (const c of r.children) autoExpand.add(c);
          }
          setExpanded(autoExpand);
        }
      } catch (e) {
        if (!cancelled) setError(String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    void load();
    return () => { cancelled = true; };
  }, [spaceKey]);

  const analyses = useMemo(() => tree ? analyzePages(tree.pages, run) : [], [tree, run]);
  const kpis = useMemo(() => computeKPIs(analyses, run), [analyses, run]);
  const suggestions = useMemo(() => generateSuggestions(analyses, sp), [analyses, sp]);
  const selectedPage = useMemo(() => analyses.find(a => a.id === selectedId), [analyses, selectedId]);
  const rootPages = useMemo(() => analyses.filter(a => !a.parentId || !analyses.some(x => x.id === a.parentId)), [analyses]);

  // Separation view data
  const fachPages = useMemo(() => analyses.filter(a => a.docType === "fachlich"), [analyses]);
  const techPages = useMemo(() => analyses.filter(a => a.docType === "technisch"), [analyses]);
  const mixedPages = useMemo(() => analyses.filter(a => a.docType === "gemischt"), [analyses]);
  const unclearPages = useMemo(() => analyses.filter(a => a.docType === "unklar"), [analyses]);

  const repoUrl = sp.metamodel_source || "";
  const confluenceBase = sp.confluence_url?.replace(/\/wiki.*$/, '') || "";

  function confluencePageUrl(pageId: string): string {
    if (!confluenceBase) return "";
    return `${confluenceBase}/wiki/pages/viewpage.action?pageId=${pageId}`;
  }

  function findingsForPage(pageTitle: string, pageId: string) {
    if (!run?.findings) return [];
    return run.findings.filter(f =>
      f.locations.some(l =>
        l.source_type === "confluence_page" &&
        (l.title.toLowerCase().includes(pageTitle.toLowerCase()) || l.source_id === pageId)
      )
    );
  }

  function toggleExpand(id: string) {
    setExpanded(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function expandAll() {
    setExpanded(new Set(analyses.map(a => a.id)));
  }

  function collapseAll() {
    setExpanded(new Set());
  }

  async function handleRename(pageId: string, newTitle: string) {
    if (!newTitle.trim()) return;
    setWritebackBusy(true);
    setWritebackError(null);
    setWritebackSuccess(null);
    try {
      await renameConfluencePage(pageId, newTitle.trim());
      // optimistic local update
      if (tree) {
        setTree({
          ...tree,
          pages: tree.pages.map(p => p.id === pageId ? { ...p, title: newTitle.trim() } : p),
        });
      }
      setWritebackSuccess(`Seite erfolgreich umbenannt zu "${newTitle.trim()}".`);
      setRenameTarget(null);
    } catch (e) {
      setWritebackError(`Umbenennen fehlgeschlagen: ${String(e)}`);
    } finally {
      setWritebackBusy(false);
    }
  }

  async function handleMove(pageId: string, newParentId: string) {
    if (!newParentId.trim()) return;
    setWritebackBusy(true);
    setWritebackError(null);
    setWritebackSuccess(null);
    try {
      const result = await moveConfluencePage(pageId, newParentId.trim());
      // optimistic local update
      if (tree) {
        setTree({
          ...tree,
          pages: tree.pages.map(p => p.id === pageId ? { ...p, parentId: result.new_parent_id } : p),
        });
      }
      setWritebackSuccess(`Seite erfolgreich verschoben.`);
      setMoveTarget(null);
    } catch (e) {
      setWritebackError(`Verschieben fehlgeschlagen: ${String(e)}`);
    } finally {
      setWritebackBusy(false);
    }
  }

  /* ── Render ── */

  if (loading) {
    return (
      <div className="ds-loading">
        <div className="ds-loading-spinner" />
        <p>Confluence-Seitenstruktur wird analysiert…</p>
        <p className="text-muted">Space: {spaceKey}</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="ds-error-state">
        <div className="empty-icon">🔒</div>
        <h3>Zugriff nicht möglich</h3>
        <p>{error}</p>
      </div>
    );
  }

  if (!tree || tree.pages.length === 0) {
    return (
      <div className="ds-error-state">
        <div className="empty-icon">📭</div>
        <h3>Keine Seiten gefunden</h3>
        <p>Der Confluence-Space „{spaceKey}" enthält keine Seiten oder ist nicht erreichbar.</p>
      </div>
    );
  }

  return (
    <div className="ds-root">

      {/* ── Rename Dialog ── */}
      {renameTarget && (() => {
        let draftTitle = renameTarget.currentTitle;
        return (
          <div className="ds-modal-overlay" onClick={() => setRenameTarget(null)}>
            <div className="ds-modal" onClick={e => e.stopPropagation()}>
              <h3 className="ds-modal-title">✏️ Seite umbenennen</h3>
              <p className="ds-modal-sub">Aktueller Titel: <em>{renameTarget.currentTitle}</em></p>
              <input
                ref={renameInputRef}
                className="ds-modal-input"
                defaultValue={renameTarget.currentTitle}
                autoFocus
                onChange={e => { draftTitle = e.target.value; }}
                onKeyDown={e => { if (e.key === "Enter") void handleRename(renameTarget.id, draftTitle); }}
                placeholder="Neuer Seitentitel…"
              />
              {writebackError && <p className="ds-modal-error">{writebackError}</p>}
              <div className="ds-modal-actions">
                <button className="btn btn-ghost btn-sm" onClick={() => setRenameTarget(null)}>Abbrechen</button>
                <button
                  className="btn btn-primary btn-sm"
                  disabled={writebackBusy}
                  onClick={() => void handleRename(renameTarget.id, renameInputRef.current?.value ?? draftTitle)}
                >
                  {writebackBusy ? "Wird gespeichert…" : "Umbenennen"}
                </button>
              </div>
            </div>
          </div>
        );
      })()}

      {/* ── Move Dialog ── */}
      {moveTarget && (() => {
        let draftParentId = "";
        return (
          <div className="ds-modal-overlay" onClick={() => setMoveTarget(null)}>
            <div className="ds-modal" onClick={e => e.stopPropagation()}>
              <h3 className="ds-modal-title">📦 Seite verschieben</h3>
              <p className="ds-modal-sub">Seite: <em>{moveTarget.title}</em></p>
              <p className="ds-modal-sub" style={{ marginTop: 4 }}>Wähle eine Ziel-Seite als neue Elternseite:</p>
              <select
                className="ds-modal-input"
                autoFocus
                onChange={e => { draftParentId = e.target.value; }}
              >
                <option value="">— Elternseite wählen —</option>
                {analyses
                  .filter(a => a.id !== moveTarget.id)
                  .map(a => (
                    <option key={a.id} value={a.id}>
                      {"  ".repeat(a.depth)}{a.title}
                    </option>
                  ))}
              </select>
              {writebackError && <p className="ds-modal-error">{writebackError}</p>}
              <div className="ds-modal-actions">
                <button className="btn btn-ghost btn-sm" onClick={() => setMoveTarget(null)}>Abbrechen</button>
                <button
                  className="btn btn-primary btn-sm"
                  disabled={writebackBusy}
                  onClick={() => { if (draftParentId) void handleMove(moveTarget.id, draftParentId); }}
                >
                  {writebackBusy ? "Wird verschoben…" : "Verschieben"}
                </button>
              </div>
            </div>
          </div>
        );
      })()}

      {/* ── Feedback Banner ── */}
      {writebackSuccess && (
        <div className="ds-feedback-banner ds-feedback-ok">
          ✅ {writebackSuccess}
          <button className="ds-feedback-close" onClick={() => setWritebackSuccess(null)}>×</button>
        </div>
      )}
      {writebackError && !renameTarget && !moveTarget && (
        <div className="ds-feedback-banner ds-feedback-err">
          ❌ {writebackError}
          <button className="ds-feedback-close" onClick={() => setWritebackError(null)}>×</button>
        </div>
      )}

      {/* ── Space Identifier ── */}
      <div className="ds-space-bar">
        <div className="ds-space-info">
          <span className="ds-space-icon">📚</span>
          <div>
            <strong>{tree.space_name}</strong>
            <span className="ds-space-key">{tree.space_key}</span>
          </div>
        </div>
        <span className="ds-space-count">{tree.pages.length} Seiten analysiert</span>
      </div>


      {/* ── KPI Dashboard ── */}
      <div className="ds-kpi-grid">
        {kpis.map((kpi, i) => (
          <div
            key={i}
            className={`ds-kpi-card ${kpi.color === "green" ? "ds-kpi-green" : kpi.color === "amber" ? "ds-kpi-amber" : kpi.color === "red" ? "ds-kpi-red" : ""}`}
            title={kpi.tooltip}
          >
            <div className="ds-kpi-icon">{kpi.icon}</div>
            <div className="ds-kpi-body">
              <span className="ds-kpi-label">{kpi.label}</span>
              <span className="ds-kpi-value">{kpi.value}</span>
              {kpi.sub && <span className="ds-kpi-sub">{kpi.sub}</span>}
            </div>
          </div>
        ))}
      </div>

      {/* ── Tab Navigation ── */}
      <div className="ds-tabs">
        {(["overview", "tree", "separation", "target", "suggestions"] as const).map(tab => (
          <button
            key={tab}
            className={`ds-tab ${activeTab === tab ? "ds-tab-active" : ""}`}
            onClick={() => setActiveTab(tab)}
          >
            {tab === "overview" && "📊 Übersicht"}
            {tab === "tree" && `🌳 Seitenbaum (${analyses.length})`}
            {tab === "separation" && "📑 Fachlich / Technisch"}
            {tab === "target" && "🎯 Soll-Struktur"}
            {tab === "suggestions" && `💡 Vorschläge (${suggestions.length})`}
          </button>
        ))}
      </div>

      {/* ── TAB: Overview ── */}
      {activeTab === "overview" && (
        <div className="ds-tab-content">
          {/* Issue distribution */}
          <section className="ds-section">
            <h2 className="ds-section-title">
              Problemverteilung
              <span className="ds-section-count">
                {analyses.filter(a => a.issues.length > 0).length} von {analyses.length} Seiten
              </span>
            </h2>
            <div className="ds-bar-chart">
              {(() => {
                const issueTypes: Record<string, number> = {};
                for (const a of analyses) {
                  for (const issue of a.issues) {
                    const key = issue.split("(")[0].trim();
                    issueTypes[key] = (issueTypes[key] ?? 0) + 1;
                  }
                }
                const entries = Object.entries(issueTypes).sort((a, b) => b[1] - a[1]);
                const max = Math.max(...entries.map(e => e[1]), 1);
                if (entries.length === 0) {
                  return <p className="text-muted ds-no-issues">✅ Keine strukturellen Probleme erkannt</p>;
                }
                return entries.map(([label, count]) => (
                  <div className="ds-bar-row" key={label}>
                    <span className="ds-bar-label">{label}</span>
                    <div className="ds-bar-track">
                      <div
                        className="ds-bar-fill"
                        style={{ width: `${(count / max) * 100}%` }}
                      />
                    </div>
                    <span className="ds-bar-value">{count}</span>
                  </div>
                ));
              })()}
            </div>
          </section>

          {/* Document type distribution */}
          <section className="ds-section">
            <h2 className="ds-section-title">Dokumenttyp-Verteilung</h2>
            <div className="ds-type-grid">
              {[
                { label: "Fachlich", count: fachPages.length, icon: "📘", cls: "ds-type-fach", desc: "Konzepte, Prozesse, Anleitungen — für Stakeholder und Endbenutzer" },
                { label: "Technisch", count: techPages.length, icon: "⚙️", cls: "ds-type-tech", desc: "Architektur, API, Deployment — besser im Git-Repository pflegen" },
                { label: "Gemischt", count: mixedPages.length, icon: "🔀", cls: "ds-type-mix", desc: "Mischung aus beidem — sollte aufgetrennt werden" },
                { label: "Unklar", count: unclearPages.length, icon: "❓", cls: "ds-type-unklar", desc: "Nicht eindeutig zuordenbar — Titel & Inhalt prüfen" },
              ].map(t => (
                <div key={t.label} className={`ds-type-card ${t.cls}`}>
                  <div className="ds-type-card-head">
                    <span className="ds-type-card-icon">{t.icon}</span>
                    <span className="ds-type-card-count">{t.count}</span>
                  </div>
                  <strong>{t.label}</strong>
                  <p>{t.desc}</p>
                </div>
              ))}
            </div>
          </section>

          {/* Best practices box */}
          <section className="ds-section">
            <h2 className="ds-section-title">📋 Best Practices für Projektdokumentation</h2>
            <div className="ds-best-practices">
              <div className="ds-bp-item">
                <div className="ds-bp-num">1</div>
                <div>
                  <strong>Redundanz vermeiden</strong>
                  <p>Keine Inhalte kopieren — stattdessen Verweise (Links) nutzen. Eine Information an genau einem Ort pflegen.</p>
                </div>
              </div>
              <div className="ds-bp-item">
                <div className="ds-bp-num">2</div>
                <div>
                  <strong>Einfache Sprache</strong>
                  <p>Fachliche Doku so schreiben, dass auch Nicht-Techniker den Zusammenhang verstehen. Fachbegriffe erklären oder ins Glossar verlinken.</p>
                </div>
              </div>
              <div className="ds-bp-item">
                <div className="ds-bp-num">3</div>
                <div>
                  <strong>Technik ↔ Fachlich trennen</strong>
                  <p>Technische Details (Code, API, Config) im Git pflegen. Fachliches (Konzepte, Prozesse) in Confluence mit Diagrammen erklären.</p>
                </div>
              </div>
              <div className="ds-bp-item">
                <div className="ds-bp-num">4</div>
                <div>
                  <strong>Flache Hierarchie</strong>
                  <p>Max. 3–4 Ebenen. Thematische Cluster statt tiefer Bäume. Seitentitel = Navigation.</p>
                </div>
              </div>
              <div className="ds-bp-item">
                <div className="ds-bp-num">5</div>
                <div>
                  <strong>Diagramme statt Text</strong>
                  <p>Zusammenhänge als Confluence-native Diagramme (draw.io / Mermaid) visualisieren. Prozessabläufe, Entscheidungsbäume, Systemübersichten.</p>
                </div>
              </div>
              <div className="ds-bp-item">
                <div className="ds-bp-num">6</div>
                <div>
                  <strong>Metamodell & Code verlinken</strong>
                  <p>Querverweise zum Metamodell und relevanten Code-Stellen herstellen. Zusammenhang zwischen Fach- und Technikseite erklären.</p>
                </div>
              </div>
              <div className="ds-bp-item">
                <div className="ds-bp-num">7</div>
                <div>
                  <strong>Review-Zyklus</strong>
                  <p>Regelmäßig prüfen: Sind Seiten noch aktuell? Gibt es verwaiste Seiten? Stimmen Verlinkungen noch? Labels für Verantwortlichkeit nutzen.</p>
                </div>
              </div>
              <div className="ds-bp-item">
                <div className="ds-bp-num">8</div>
                <div>
                  <strong>Versionierung & Changelog</strong>
                  <p>Wichtige Entscheidungen und Änderungen dokumentieren. Confluence-Seitenhistorie nutzen. Entscheidungslog als eigene Seite führen.</p>
                </div>
              </div>
            </div>
          </section>
        </div>
      )}

      {/* ── TAB: Tree ── */}
      {activeTab === "tree" && (
        <div className="ds-tab-content">
          <div className="ds-tree-panel">
            <div className="ds-tree-toolbar">
              <button className="btn btn-ghost btn-sm" onClick={expandAll}>Alle aufklappen</button>
              <button className="btn btn-ghost btn-sm" onClick={collapseAll}>Alle zuklappen</button>
              <span className="ds-tree-legend">
                <span className="ds-type-badge ds-type-fach">📘</span> Fachlich
                <span className="ds-type-badge ds-type-tech">⚙️</span> Technisch
                <span className="ds-type-badge ds-type-mix">🔀</span> Gemischt
                <span className="ds-type-badge ds-type-unklar">❓</span> Unklar
              </span>
            </div>
            <div className="ds-tree-scroll">
              {rootPages.map(page => (
                <PageTreeNode
                  key={page.id}
                  analysis={page}
                  allAnalyses={analyses}
                  expanded={expanded}
                  onToggle={toggleExpand}
                  selectedId={selectedId}
                  onSelect={setSelectedId}
                  repoUrl={repoUrl}
                />
              ))}
            </div>
          </div>

          {/* Detail panel for selected page */}
          {selectedPage && (
            <div className="ds-detail-panel">
              <div className="ds-detail-head">
                <h3>{selectedPage.title}</h3>
                <span className={`ds-type-badge ${
                  selectedPage.docType === "fachlich" ? "ds-type-fach" :
                  selectedPage.docType === "technisch" ? "ds-type-tech" :
                  selectedPage.docType === "gemischt" ? "ds-type-mix" : "ds-type-unklar"
                }`}>
                  {selectedPage.docType === "fachlich" ? "📘 Fachlich" :
                   selectedPage.docType === "technisch" ? "⚙️ Technisch" :
                   selectedPage.docType === "gemischt" ? "🔀 Gemischt" : "❓ Unklar"}
                </span>
                <div className="ds-detail-actions">
                  <button
                    className="btn btn-ghost btn-sm"
                    title="Seite umbenennen"
                    onClick={() => setRenameTarget({ id: selectedPage.id, currentTitle: selectedPage.title })}
                  >
                    ✏️ Umbenennen
                  </button>
                  <button
                    className="btn btn-ghost btn-sm"
                    title="Seite verschieben"
                    onClick={() => setMoveTarget({ id: selectedPage.id, title: selectedPage.title })}
                  >
                    📦 Verschieben
                  </button>
                </div>
              </div>

              <div className="ds-detail-grid">
                <div className="ds-detail-stat">
                  <span className="ds-detail-stat-label">Ebene</span>
                  <span className="ds-detail-stat-value">{selectedPage.depth}</span>
                </div>
                <div className="ds-detail-stat">
                  <span className="ds-detail-stat-label">Unterseiten</span>
                  <span className="ds-detail-stat-value">{selectedPage.children.length}</span>
                </div>
                <div className="ds-detail-stat">
                  <span className="ds-detail-stat-label">Probleme</span>
                  <span className="ds-detail-stat-value">{selectedPage.issues.length}</span>
                </div>
              </div>
              {selectedPage.issues.length > 0 && (
                <div className="ds-detail-issues">
                  <div className="wc-label">Erkannte Probleme</div>
                  <ul>
                    {selectedPage.issues.map((issue, i) => (
                      <li key={i}>{issue}</li>
                    ))}
                  </ul>
                </div>
              )}
              {selectedPage.docType === "technisch" && (
                <div className="ds-detail-hint ds-hint-tech">
                  <strong>💡 Empfehlung:</strong> Technische Inhalte sollten im Git-Repository gepflegt werden.
                  Diese Confluence-Seite könnte als Brücke dienen, die auf die entsprechende Code-Dokumentation verlinkt.
                  {repoUrl && (
                    <p className="mt-sm">
                      Repository: <a href={repoUrl} target="_blank" rel="noreferrer">{repoUrl}</a>
                    </p>
                  )}
                </div>
              )}
              {selectedPage.docType === "gemischt" && (
                <div className="ds-detail-hint ds-hint-mix">
                  <strong>💡 Empfehlung:</strong> Diese Seite sollte aufgeteilt werden:
                  <ul>
                    <li>Fachliche Erklärungen → eigene Confluence-Seite mit einfacher Sprache und Diagrammen</li>
                    <li>Technische Details → Git-Repository (README, docs/) mit Link aus Confluence</li>
                  </ul>
                </div>
              )}
              {selectedPage.docType === "fachlich" && (
                <div className="ds-detail-hint ds-hint-fach">
                  <strong>✅ Korrekte Einordnung:</strong> Diese Seite gehört in Confluence.
                  Achte auf einfache Sprache, Diagramme und Querverweise zum Metamodell und anderen Projektteilen.
                </div>
              )}
              {/* Confluence direct link */}
              {confluenceBase && (
                <div className="ds-detail-hint" style={{ background: 'var(--bg-raised)', border: '1px solid var(--border-subtle)' }}>
                  <strong>🔗 Direkt-Link:</strong>{' '}
                  <a href={confluencePageUrl(selectedPage.id)} target="_blank" rel="noreferrer">
                    Seite in Confluence öffnen →
                  </a>
                </div>
              )}
              {/* Audit findings for this page */}
              {(() => {
                const pf = findingsForPage(selectedPage.title, selectedPage.id);
                if (pf.length === 0) return null;
                return (
                  <div className="ds-detail-findings">
                    <div className="wc-label">Audit-Findings für diese Seite ({pf.length})</div>
                    {pf.map(f => (
                      <div key={f.finding_id} className="ds-detail-finding-item">
                        <span className={`badge badge-${f.severity}`} style={{ fontSize: '9px' }}>
                          {f.severity}
                        </span>
                        <span>{f.title}</span>
                      </div>
                    ))}
                  </div>
                );
              })()}
            </div>
          )}
        </div>
      )}

      {/* ── TAB: Separation ── */}
      {activeTab === "separation" && (
        <div className="ds-tab-content">
          <div className="ds-sep-intro">
            <p>
              <strong>Trennungsprinzip:</strong> Dokumentation wird in zwei Bereiche aufgeteilt —
              <em> Fachlich</em> (bleibt in Confluence) und <em>Technisch</em> (gehört ins Git-Repository).
              Querverweise verbinden beide Welten.
            </p>
          </div>
          <div className="ds-sep-columns">
            {/* Confluence = Fachlich */}
            <div className="ds-sep-col ds-sep-fach">
              <div className="ds-sep-col-head">
                <span className="ds-sep-col-icon">📘</span>
                <div>
                  <strong>Confluence — Fachliche Doku</strong>
                  <p>Konzepte, Prozesse, Anleitungen, FAQ</p>
                </div>
                <span className="ds-sep-col-count">{fachPages.length} Seiten</span>
              </div>
              <div className="ds-sep-col-body">
                {fachPages.length === 0 && <p className="text-muted">Keine rein fachlichen Seiten erkannt.</p>}
                {fachPages.map(p => (
                  <div className="ds-sep-item" key={p.id}>
                    <span className="ds-sep-item-icon">📄</span>
                    <span>{p.title}</span>
                    {p.issues.length > 0 && <span className="ds-tree-issue-count">{p.issues.length} ⚠</span>}
                  </div>
                ))}
              </div>
            </div>
            {/* Git = Technisch */}
            <div className="ds-sep-col ds-sep-tech">
              <div className="ds-sep-col-head">
                <span className="ds-sep-col-icon">⚙️</span>
                <div>
                  <strong>Git-Repository — Technische Doku</strong>
                  <p>Code-Doku, API-Specs, Architektur, Deployment</p>
                </div>
                <span className="ds-sep-col-count">{techPages.length} Seiten</span>
              </div>
              <div className="ds-sep-col-body">
                {techPages.length === 0 && <p className="text-muted">Keine rein technischen Seiten erkannt.</p>}
                {techPages.map(p => (
                  <div className="ds-sep-item" key={p.id}>
                    <span className="ds-sep-item-icon">🔗</span>
                    <span>{p.title}</span>
                    <span className="ds-sep-action">→ In Git verlinken</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
          {/* Mixed section */}
          {(mixedPages.length > 0 || unclearPages.length > 0) && (
            <div className="ds-sep-mixed">
              <h3>🔀 Aufzuteilende Seiten ({mixedPages.length + unclearPages.length})</h3>
              <p className="text-muted">Diese Seiten sollten in fachliche und technische Teile getrennt werden:</p>
              <div className="ds-sep-mixed-list">
                {[...mixedPages, ...unclearPages].map(p => (
                  <div className="ds-sep-item ds-sep-item-action" key={p.id}>
                    <span className="ds-sep-item-icon">{p.docType === "gemischt" ? "🔀" : "❓"}</span>
                    <div className="ds-sep-item-body">
                      <strong>{p.title}</strong>
                      {p.issues.length > 0 && <p className="text-muted text-xs">{p.issues.join(" · ")}</p>}
                    </div>
                    <div className="ds-sep-arrows">
                      <span className="ds-sep-arrow-fach">📘 → Confluence</span>
                      <span className="ds-sep-arrow-tech">⚙️ → Git</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Git docs reference */}
          <div className="ds-sep-col" style={{ border: '1px solid var(--border-subtle)', borderTop: '3px solid var(--success)', borderRadius: 'var(--r-lg)', marginTop: 'var(--sp-lg)' }}>
            <div className="ds-sep-col-head">
              <span className="ds-sep-col-icon">📂</span>
              <div>
                <strong>Code-generierte Git-Dokumentation (Input-Quelle)</strong>
                <p>Automatisch generiert — fachliche Beschreibungen können als Input für Confluence-Seiten dienen</p>
              </div>
              <span className="ds-sep-col-count">{GIT_DOC_FILES.length} Dateien</span>
            </div>
            <div className="ds-sep-col-body">
              {GIT_DOC_FILES.map(doc => (
                <div className="ds-sep-item" key={doc.path}>
                  <span className="ds-sep-item-icon">{doc.type === 'technisch' ? '⚙️' : '📘'}</span>
                  <span>{doc.label}</span>
                  <span className="ds-sep-action" style={{ fontFamily: 'var(--font-mono)', fontSize: '10px' }}>→ Input für Confluence</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* ── TAB: Suggestions ── */}
      {activeTab === "suggestions" && (
        <div className="ds-tab-content">
          {suggestions.map(sug => (
            <article
              key={sug.id}
              className={`ds-suggestion ${
                sug.severity === "warning" ? "ds-sug-warning" :
                sug.severity === "improvement" ? "ds-sug-improvement" : "ds-sug-info"
              }`}
            >
              <div className="ds-sug-head">
                <span className={`ds-sug-type ds-sug-type-${sug.type}`}>
                  {sug.type === "rename" && "✏️ Umbenennen"}
                  {sug.type === "move" && "📦 Verschieben"}
                  {sug.type === "merge" && "🔗 Zusammenführen"}
                  {sug.type === "split" && "✂️ Aufteilen"}
                  {sug.type === "link" && "🔗 Verlinken"}
                  {sug.type === "create" && "➕ Anlegen"}
                </span>
                <span className={`badge ${
                  sug.severity === "warning" ? "badge-high" :
                  sug.severity === "improvement" ? "badge-medium" : "badge-low"
                }`}>
                  {sug.severity === "warning" ? "Wichtig" : sug.severity === "improvement" ? "Empfohlen" : "Info"}
                </span>
              </div>
              <h3 className="ds-sug-title">{sug.title}</h3>
              <p className="ds-sug-desc">{sug.description}</p>
              {sug.affectedPages.length > 0 && (
                <div className="ds-sug-pages">
                  <div className="wc-label">Betroffene Seiten</div>
                  <div className="ds-sug-page-list">
                    {sug.affectedPages.map((p, i) => (
                      <span key={i} className="ds-sug-page">{p}</span>
                    ))}
                  </div>
                </div>
              )}
            </article>
          ))}
        </div>
      )}

      {/* ── TAB: Target Structure ── */}
      {activeTab === "target" && (
        <div className="ds-tab-content">
          <section className="ds-section">
            <h2 className="ds-section-title">🎯 Empfohlene Soll-Struktur für den Confluence-Bereich</h2>
            <p className="text-muted" style={{ marginBottom: 'var(--sp-lg)', fontSize: 'var(--fs-sm)', lineHeight: 1.6 }}>
              Diese Struktur folgt Best Practices für Projektdokumentation: flache Hierarchie, klare thematische Trennung,
              einfache Sprache, Redundanzvermeidung durch Verlinkung, und separate Bereiche für verschiedene Zielgruppen.
            </p>
            <div className="ds-target-tree">
              {PROPOSED_STRUCTURE.map((area, areaIdx) => (
                <div key={areaIdx} className="ds-target-area">
                  <div className="ds-target-area-head">
                    <span className="ds-target-area-icon">{area.icon}</span>
                    <div>
                      <strong>{area.title}</strong>
                      <p>{area.description}</p>
                    </div>
                  </div>
                  <div className="ds-target-children">
                    {area.children.map((child, ci) => (
                      <div key={ci} className="ds-target-child">
                        <span className="ds-target-connector">{ci === area.children.length - 1 ? '└' : '├'}</span>
                        <span className="ds-target-child-label">{child}</span>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </section>

          {/* Mapping: Existing pages → proposed location */}
          <section className="ds-section">
            <h2 className="ds-section-title">
              📍 Zuordnung bestehender Seiten
              <span className="ds-section-count">{analyses.length} Seiten</span>
            </h2>
            <p className="text-muted" style={{ marginBottom: 'var(--sp-lg)', fontSize: 'var(--fs-sm)' }}>
              Vorschlag, in welchen Soll-Bereich jede bestehende Confluence-Seite eingeordnet werden sollte:
            </p>
            <div className="ds-target-mapping">
              {analyses.map(page => {
                const targetArea = page.docType === 'fachlich'
                  ? '📘 Fachliche Dokumentation'
                  : page.docType === 'technisch'
                  ? '⚙️ Technische Referenz'
                  : page.docType === 'gemischt'
                  ? '✂️ Aufteilen: Fachlich + Technisch'
                  : '❓ Einordnung prüfen';
                return (
                  <div key={page.id} className="ds-target-map-row">
                    <span className={`ds-type-badge ${page.docType === 'fachlich' ? 'ds-type-fach' : page.docType === 'technisch' ? 'ds-type-tech' : page.docType === 'gemischt' ? 'ds-type-mix' : 'ds-type-unklar'}`}>
                      {page.docType === 'fachlich' ? '📘' : page.docType === 'technisch' ? '⚙️' : page.docType === 'gemischt' ? '🔀' : '❓'}
                    </span>
                    <span className="ds-target-map-title">{page.title}</span>
                    <span className="ds-target-map-arrow">→</span>
                    <span className="ds-target-map-target">{targetArea}</span>
                    {page.crossRefs > 0 && (
                      <span className="ds-tree-finding-count">{page.crossRefs} 🔍</span>
                    )}
                  </div>
                );
              })}
            </div>
          </section>
        </div>
      )}
    </div>
  );
}
