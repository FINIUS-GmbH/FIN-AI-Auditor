from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ForensicFindingClassSpec:
    class_id: str
    title: str
    priority: str
    current_strength: str
    target_modules: tuple[str, ...]
    needs_reference_cases: bool = True


FORENSIC_FINDING_CLASSES: tuple[ForensicFindingClassSpec, ...] = (
    ForensicFindingClassSpec(
        class_id="F01",
        title="Doku-gegen-Doku-Widerspruch",
        priority="P0",
        current_strength="strong",
        target_modules=("finding_engine", "claim_extractor"),
    ),
    ForensicFindingClassSpec(
        class_id="F02",
        title="Doku-gegen-Metamodell-Widerspruch",
        priority="P0",
        current_strength="strong",
        target_modules=("finding_engine", "claim_extractor"),
    ),
    ForensicFindingClassSpec(
        class_id="F03",
        title="Doku-gegen-Code-Drift",
        priority="P0",
        current_strength="medium",
        target_modules=("claim_extractor", "finding_engine", "semantic_graph_service"),
    ),
    ForensicFindingClassSpec(
        class_id="F04",
        title="Dokumentierter Read-/Write-Pfad fehlt im Code",
        priority="P0",
        current_strength="medium",
        target_modules=("claim_extractor", "finding_engine"),
    ),
    ForensicFindingClassSpec(
        class_id="F05",
        title="Codepfad ist fachlich nicht dokumentiert",
        priority="P1",
        current_strength="weak",
        target_modules=("claim_extractor", "finding_engine"),
    ),
    ForensicFindingClassSpec(
        class_id="F06",
        title="Policy-/Approval-/Allowlist-Verstoss",
        priority="P0",
        current_strength="strong",
        target_modules=("claim_extractor", "finding_engine", "audit_service"),
    ),
    ForensicFindingClassSpec(
        class_id="F07",
        title="Lifecycle-/Status-Drift",
        priority="P1",
        current_strength="medium",
        target_modules=("claim_extractor", "finding_engine"),
    ),
    ForensicFindingClassSpec(
        class_id="F08",
        title="Kettenbruch in fachlichen Objektpfaden",
        priority="P0",
        current_strength="weak",
        target_modules=("semantic_graph_service", "claim_extractor", "finding_engine"),
    ),
    ForensicFindingClassSpec(
        class_id="F09",
        title="Temporale Luecke / Eventual Consistency",
        priority="P0",
        current_strength="weak",
        target_modules=("claim_extractor", "finding_engine", "bsm_domain_claim_extractor"),
    ),
    ForensicFindingClassSpec(
        class_id="F10",
        title="Supersede-/Refine-/Rebuild-Luecke",
        priority="P0",
        current_strength="weak",
        target_modules=("claim_extractor", "finding_engine", "bsm_domain_claim_extractor"),
    ),
    ForensicFindingClassSpec(
        class_id="F11",
        title="Feld-Propagation-/Schema-Vollstaendigkeitsfehler",
        priority="P0",
        current_strength="weak",
        target_modules=("claim_extractor", "finding_engine", "bsm_domain_claim_extractor"),
    ),
    ForensicFindingClassSpec(
        class_id="F12",
        title="Legacy-/Nebenpfad schwaecher als Hauptpfad",
        priority="P1",
        current_strength="weak",
        target_modules=("claim_extractor", "finding_engine", "semantic_graph_service"),
    ),
)
