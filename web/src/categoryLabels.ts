export const CATEGORY_LABELS: Record<string, string> = {
  contradiction: "⚠️ Widerspruch",
  gap: "💭 Lücke",
  inconsistency: "🔀 Inkonsistenz",
  architecture_observation: "🧱 Architektur-Befund",
  missing_implementation: "❌ Fehlende Umsetzung",
  missing_documentation: "📝 Fehlende Doku",
  missing_definition: "❓ Definitionslücke",
  stale_documentation: "📅 Veraltete Doku",
  policy_violation: "🛡️ Richtlinienverstoß",
  policy_conflict: "🛡️ Richtlinienkonflikt",
  process_gap: "⚙️ Prozesslücke",
  semantic_drift: "🎯 Semantische Abweichung",
  implementation_drift: "🔧 Implementierungsabweichung",
  traceability_gap: "🔗 Nachverfolgbarkeitslücke",
  clarification_needed: "❓ Klärungsbedarf",
  stale_source: "📅 Veraltete Quelle",
  read_write_gap: "↔️ Read/Write-Lücke",
  ownership_gap: "👥 Ownership-Lücke",
  legacy_path_gap: "🪤 Boundary-/Legacy-Pfad",
  terminology_collision: "🧭 Begriffskollision",
  low_confidence_review: "🔍 Niedrige Sicherheit",
  obsolete_documentation: "🗃️ Obsolete Doku",
  open_decision: "🧩 Offene Entscheidung",
};

export const CATEGORY_ORDER: string[] = [
  "contradiction",
  "policy_conflict",
  "terminology_collision",
  "read_write_gap",
  "implementation_drift",
  "legacy_path_gap",
  "stale_source",
  "missing_definition",
  "missing_documentation",
  "traceability_gap",
  "ownership_gap",
  "clarification_needed",
  "architecture_observation",
  "low_confidence_review",
  "obsolete_documentation",
  "open_decision",
];

export function categoryLabel(value: string): string {
  return CATEGORY_LABELS[value] ?? value;
}

export function categorySortKey(value: string): [number, string] {
  const idx = CATEGORY_ORDER.indexOf(value);
  return [idx === -1 ? 999 : idx, value];
}
