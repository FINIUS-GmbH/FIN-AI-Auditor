"""Deterministic BSM domain contradiction detection.

Compares BSM domain claims across documents to find contradictions.
This is a DETERMINISTIC detector — no LLM needed. It works by:

1. Grouping claims by subject_key (e.g. "Statement.hitl", "BSM_Element.initial_state")
2. Within each group, checking if different sources make conflicting assertions
3. Generating findings with full evidence chains for each contradiction

This catches the specific contradictions documented in the project benchmark,
plus any additional contradictions following the same structural patterns.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Sequence

from fin_ai_auditor.domain.models import AuditFinding, AuditLocation
from fin_ai_auditor.services.pipeline_models import ExtractedClaimRecord

logger = logging.getLogger(__name__)

# Predicates that represent opposing values when different
_VALUE_CONFLICT_PREDICATES: set[str] = {
    "defined_status", "puml_defined_status", "code_defined_status", "yaml_defined_status",
    "hitl_decision", "puml_hitl_decision", "code_hitl_decision",
    "run_hierarchy", "puml_run_hierarchy",
    "evidence_chain_type", "puml_evidence_chain", "code_evidence_chain_type", "yaml_evidence_chain_type",
    "phase_scope_type",
    "to_modify_inclusion", "puml_to_modify_usage",
    "initial_status",
    "relationship_lifecycle_type", "puml_relationship_lifecycle",
    "relationship_change_approach",
    "code_cypher_relationship", "yaml_allowlist_relationship",
    "code_eventual_consistency_risk",
    "code_chain_interruption_risk",
    "code_schema_missing_fields",
    "code_field_propagation_gap",
    "code_evidence_chain_hop",
    "yaml_evidence_chain_hop",
}

# Role predicates where text differences indicate contradictions
_ROLE_PREDICATES: set[str] = {
    "entity_role_assertion",
    "puml_entity_role",
    "metamodel_entity_exists",
    "code_cypher_node_usage",
    "yaml_allowlist_node",
    "yaml_write_allowlist_entry",
}

# German labels for contradiction types
_CONTRADICTION_LABELS: dict[str, str] = {
    "status_canon": "Status-Kanon",
    "hitl": "HITL-Entscheidung",
    "run_hierarchy": "Run-Modell",
    "evidence_chain": "Evidenzkette",
    "phase_scope": "Phasen-/Scope-Trennung",
    "to_modify": "TO_MODIFY-Rolle",
    "initial_state": "Initialstatus",
    "lifecycle": "Lifecycle-Modell",
    "change_model": "Aenderungsmodell",
    "role": "Entity-Rolle",
    "write_contract": "Write-Contract",
    "field_propagation": "Feld-Propagation",
    "hop_statement_element": "Statement-Element-Hop",
}

_CODE_RISK_CONFIG: dict[str, dict[str, str]] = {
    "code_evidence_chain_variant_conflict": {
        "category": "implementation_drift",
        "severity": "high",
        "title": "Parallele aktive Evidenzkettenvarianten im Implementierungspfad",
        "recommendation": "Die aktive Kettenvariante fachlich festlegen und konkurrierende Pfade angleichen oder entfernen, damit nicht mehrere aktive Statement-Ableitungen nebeneinander bestehen.",
    },
    "code_evidence_chain_break": {
        "category": "implementation_drift",
        "severity": "high",
        "title": "Aktive Evidenzkette ist im Implementierungspfad unvollstaendig",
        "recommendation": "Die aktive Evidenzkette ueber alle erwarteten Hops vervollstaendigen oder explizit dokumentieren, warum ein Schritt in diesem Pfad bewusst fehlt.",
    },
    "code_eventual_consistency_risk": {
        "category": "read_write_gap",
        "severity": "high",
        "title": "Eventual-Consistency-Luecke im BSM-Schreibpfad",
        "recommendation": "Den Pfad auf atomische Persistenz oder klar abgesicherte Nachverarbeitung umstellen und die Zwischenphase explizit dokumentieren.",
    },
    "code_chain_interruption_risk": {
        "category": "read_write_gap",
        "severity": "high",
        "title": "Reaggregation unterbricht die aktive BSM-Kette",
        "recommendation": "Supersede- und Rebuild-Schritte transaktional koppeln oder eine parallel gueltige Ersatzkette aufbauen, bevor die alte deaktiviert wird.",
    },
    "code_schema_missing_fields": {
        "category": "implementation_drift",
        "severity": "high",
        "title": "Statement-Output-Schema verliert notwendige Unit-Zuordnung",
        "recommendation": "Das Ausgabe-Schema um belastbare Unit-Referenzen erweitern und die Persistenzpfade darauf ausrichten.",
    },
    "code_field_propagation_gap": {
        "category": "implementation_drift",
        "severity": "medium",
        "title": "Feld-Propagation im BSM-Sonderpfad unvollstaendig",
        "recommendation": "Pfadparameter zwischen Refine-/Manual-/Legacy-Pfaden vereinheitlichen und fehlende Run-/Phase-Felder explizit weiterreichen.",
    },
}

_MANUAL_BOUNDARY_RISK_CONFIG: dict[str, str] = {
    "category": "legacy_path_gap",
    "severity": "medium",
    "title": "Manueller Boundary-Pfad propagiert phase_run_id nicht vollstaendig",
    "recommendation": "Manuelle Antwortpfade auf einen kanonischen Entry-Point zusammenziehen und phase_run_id/run_id konsistent bis zur bsmAnswer-Persistenz durchreichen.",
}

_GROUPED_CHAIN_INTERRUPTION_RISK_CONFIG: dict[str, str] = {
    "category": "read_write_gap",
    "severity": "high",
    "title": "Reaggregation unterbricht die aktive BSM-Kette",
    "recommendation": "Supersede- und Rebuild-Schritte ueber alle Reaggregationspfade transaktional koppeln oder eine parallel gueltige Ersatzkette aufbauen, bevor die alte deaktiviert wird.",
}

_EVENTUAL_CONSISTENCY_SUBTYPE_CONFIG: dict[str, dict[str, str]] = {
    "manual_answer_enqueue": {
        "label": "Manueller Antwortpfad",
        "title": "Eventual-Consistency-Luecke im manuellen Antwortpfad",
        "recommendation": "Persistenz und Reaggregation fuer manuelle Antwortpfade atomisch koppeln oder den kanonischen Schreibpfad synchron abschliessen, bevor Folgejobs enqueued werden.",
        "grouped_package_title": "Manuelle Antwortpersistenz atomisch machen",
        "grouped_scope_label": "Manuelle Antwort-/Phase-Run-Pfade",
    },
    "reaggregation_enqueue": {
        "label": "Reaggregationspfad",
        "title": "Eventual-Consistency-Luecke im Reaggregationspfad",
        "recommendation": "Reaggregationstrigger erst nach abgeschlossener Persistenz und konsistenter Zwischenmaterialisierung freigeben oder den Trigger in denselben Schutzraum ziehen.",
        "grouped_package_title": "Reaggregations-Trigger atomisch machen",
        "grouped_scope_label": "Reaggregationspfade",
    },
    "connector_ingestion_enqueue": {
        "label": "Connector-/Ingestion-Pfad",
        "title": "Eventual-Consistency-Luecke im Connector-/Ingestion-Pfad",
        "recommendation": "Connector-nahe Persistenz und Folgejobs in einen klaren Schutzraum ziehen oder die asynchrone Uebergabe mit belastbaren Zwischenzustaenden absichern.",
        "grouped_package_title": "Connector-/Ingestion-Pfade atomisch machen",
        "grouped_scope_label": "Connector-/Ingestion-Pfade",
    },
    "upload_enqueue": {
        "label": "Datei-/Upload-Pfad",
        "title": "Eventual-Consistency-Luecke im Datei-/Upload-Pfad",
        "recommendation": "Upload-nahe Persistenz und Folgejobs konsistent takten oder den asynchronen Uebergang ueber einen belastbaren Zwischenzustand absichern.",
        "grouped_package_title": "Datei-/Upload-Pfade atomisch machen",
        "grouped_scope_label": "Datei-/Upload-Pfade",
    },
    "storage_reconcile_enqueue": {
        "label": "Storage-Reconcile-Pfad",
        "title": "Eventual-Consistency-Luecke im Storage-Reconcile-Pfad",
        "recommendation": "Storage-Reconcile, Index-Update und nachgelagerte Transkriptionsjobs klar serialisieren oder den asynchronen Uebergang ueber belastbare Zwischenzustaende absichern.",
        "grouped_package_title": "Storage-Reconcile-Pfade atomisch machen",
        "grouped_scope_label": "Storage-Reconcile-Pfade",
    },
    "workflow_registry_enqueue": {
        "label": "Workflow-Registry-Pfad",
        "title": "Eventual-Consistency-Luecke im Workflow-Registry-Pfad",
        "recommendation": "Registry-Update und Folgebenachrichtigung klar serialisieren oder die Notification erst nach bestaetigter Persistenz ausloesen.",
        "grouped_package_title": "Workflow-Registry atomisch machen",
        "grouped_scope_label": "Workflow-Registry-Pfade",
    },
    "ingestion_recovery_enqueue": {
        "label": "Ingestion-Recovery-Pfad",
        "title": "Eventual-Consistency-Luecke im Ingestion-Recovery-Pfad",
        "recommendation": "Recovery- und Registry-Updates erst nach abgeschlossenem Persistenzschritt weitertriggern oder die Wiederanlauf-Queue ueber belastbare Zwischenzustaende entkoppeln.",
        "grouped_package_title": "Ingestion-Recovery atomisch machen",
        "grouped_scope_label": "Ingestion-Recovery-Pfade",
    },
    "phase_execution_enqueue": {
        "label": "Phasen-Ausfuehrungspfad",
        "title": "Eventual-Consistency-Luecke im Phasen-Ausfuehrungspfad",
        "recommendation": "Chunk-/Phasen-Status und Folgejobs fuer die Phasen-Ausfuehrung in denselben Schutzraum ziehen oder den Uebergang explizit absichern.",
        "grouped_package_title": "Phasen-Ausfuehrung atomisch machen",
        "grouped_scope_label": "Phasen-Ausfuehrungspfade",
    },
    "run_start_enqueue": {
        "label": "Run-Start-Pfad",
        "title": "Eventual-Consistency-Luecke im Run-Start-Pfad",
        "recommendation": "Run-Start und Folgejob-Initialisierung erst nach abgeschlossenem Persistenzschritt freigeben oder transaktional koppeln.",
        "grouped_package_title": "Run-Start atomisch machen",
        "grouped_scope_label": "Run-Start-Pfade",
    },
    "worker_failure_enqueue": {
        "label": "Worker-Fehlerpfad",
        "title": "Eventual-Consistency-Luecke im Worker-Fehlerpfad",
        "recommendation": "Fehlerpropagation und Folgejobs erst nach stabiler Persistenz des Fehlerzustands ausloesen oder den Pfad ueber einen belastbaren Zwischenstatus absichern.",
        "grouped_package_title": "Worker-Fehlerpfade atomisch machen",
        "grouped_scope_label": "Worker-Fehlerpfade",
    },
    "ui_action_dispatch_enqueue": {
        "label": "UI-Aktionspfad",
        "title": "Eventual-Consistency-Luecke im UI-Aktionspfad",
        "recommendation": "UI-nahe Zustandsupdates und Folgeaktionen klar serialisieren oder den Dispatch erst nach stabiler Persistenz ausloesen.",
        "grouped_package_title": "UI-Aktionspfade atomisch machen",
        "grouped_scope_label": "UI-Aktionspfade",
    },
    "graph_system_enqueue": {
        "label": "Graph-Systempfad",
        "title": "Eventual-Consistency-Luecke im Graph-Systempfad",
        "recommendation": "Systemnahe Graph-Persistenz und Folgeoperationen ueber klar definierte Zwischenzustaende entkoppeln oder transaktional absichern.",
        "grouped_package_title": "Graph-Systempfade atomisch machen",
        "grouped_scope_label": "Graph-Systempfade",
    },
    "orchestration_async_gap": {
        "label": "Orchestrierungs-/Worker-Pfad",
        "title": "Eventual-Consistency-Luecke im Orchestrierungs-/Worker-Pfad",
        "recommendation": "Worker- und Orchestrierungspfad erst nach abgeschlossenem Persistenzschritt weitertriggern oder den Uebergang transaktional absichern.",
        "grouped_package_title": "Orchestrierungs-/Worker-Pfade atomisch machen",
        "grouped_scope_label": "Orchestrierungs-/Worker-Pfade",
    },
    "generic_async_gap": {
        "label": "Asynchroner Schreibpfad",
        "title": "Eventual-Consistency-Luecke im BSM-Schreibpfad",
        "recommendation": "Den Pfad auf atomische Persistenz oder klar abgesicherte Nachverarbeitung umstellen und die Zwischenphase explizit dokumentieren.",
        "grouped_package_title": "Asynchrone Schreibpfade atomisch machen",
        "grouped_scope_label": "Asynchrone Schreibpfade",
    },
}

_GROUPABLE_EVENTUAL_CONSISTENCY_SUBTYPES: frozenset[str] = frozenset(
    {"manual_answer_enqueue", "connector_ingestion_enqueue", "phase_execution_enqueue"}
)

_ARCHITECTURE_OBSERVATION_CONFIG: dict[str, dict[str, str]] = {
    "code_evidence_chain_hop": {
        "category": "architecture_observation",
        "severity": "low",
        "title": "Statement-zu-BSM-Element-Hop ist im Hauptpfad vorhanden",
        "recommendation": "Kein Sofort-Fix noetig. Diesen Hop als bestaetigten Architekturbaustein in Zielbild, Guardrails und Folgeanalysen fuehren.",
    },
    "yaml_evidence_chain_hop": {
        "category": "architecture_observation",
        "severity": "low",
        "title": "Statement-zu-BSM-Element-Hop ist in der Write-Allowlist verankert",
        "recommendation": "Kein Sofort-Fix noetig. Die Allowlist-Bestaetigung als Teil des belastbaren Hauptpfads dokumentieren.",
    },
    "puml_evidence_chain_hop": {
        "category": "architecture_observation",
        "severity": "low",
        "title": "Statement-zu-BSM-Element-Hop ist im Zielbild modelliert",
        "recommendation": "Kein Sofort-Fix noetig. Das modellierte SUPPORTS-Glied als bestaetigten Architekturbaustein weiterverwenden.",
    },
}


def detect_bsm_domain_contradictions(
    *, claim_records: list[ExtractedClaimRecord],
) -> list[AuditFinding]:
    """Find contradictions in BSM domain claims across different documents.

    Strategy:
    1. Filter to BSM domain claims (subject_kind == "bsm_domain")
    2. Group by subject_key (e.g. "Statement.status_canon")
    3. For value-conflict predicates: find groups where different sources claim different values
    4. For role predicates: find conflicting role assertions
    """
    domain_claims = [
        r for r in claim_records
        if r.claim.subject_kind == "bsm_domain"
    ]
    if not domain_claims:
        return []

    # Group claims by subject_key
    subject_groups: dict[str, list[ExtractedClaimRecord]] = defaultdict(list)
    for record in domain_claims:
        subject_groups[record.claim.subject_key].append(record)

    findings: list[AuditFinding] = []

    for subject_key, records in subject_groups.items():
        # Only interesting if claims come from multiple sources
        source_ids = {r.claim.source_id for r in records}
        if len(source_ids) < 2:
            continue

        # Check for value conflicts
        findings.extend(
            _check_value_conflicts(subject_key=subject_key, records=records)
        )

        # Check for role contradictions
        findings.extend(
            _check_role_contradictions(subject_key=subject_key, records=records)
        )

    findings.extend(_detect_code_risks(domain_claims=domain_claims))
    findings.extend(_detect_evidence_chain_variant_conflicts(domain_claims=domain_claims))
    findings.extend(_detect_evidence_chain_breaks(domain_claims=domain_claims))
    findings.extend(_detect_architecture_observations(domain_claims=domain_claims))
    findings = _annotate_evidence_chain_full_path_conflicts(
        findings=findings,
        domain_claims=domain_claims,
    )

    logger.info(
        "bsm_domain_contradictions_found",
        extra={
            "event_name": "bsm_domain_contradictions_found",
            "event_payload": {
                "total_domain_claims": len(domain_claims),
                "subject_groups": len(subject_groups),
                "contradictions_found": len(findings),
            },
        },
    )
    return findings


def _check_value_conflicts(
    *, subject_key: str, records: list[ExtractedClaimRecord],
) -> list[AuditFinding]:
    """Check for value conflicts where different sources assert different values."""
    findings: list[AuditFinding] = []

    # Group by predicate
    by_predicate: dict[str, list[ExtractedClaimRecord]] = defaultdict(list)
    for r in records:
        by_predicate[r.claim.predicate].append(r)

    # Also check cross-predicate conflicts (e.g. documented_lifecycle vs puml_defined_status)
    # Merge predicates that refer to the same concept
    merged: dict[str, list[ExtractedClaimRecord]] = defaultdict(list)
    for predicate, pred_records in by_predicate.items():
        # Normalize predicate to concept so code/yaml/puml/docs compare against the same aspect.
        concept = _normalize_predicate_concept(predicate)
        merged[concept].extend(pred_records)

    for concept, concept_records in merged.items():
        source_values: dict[str, list[ExtractedClaimRecord]] = defaultdict(list)
        for r in concept_records:
            val = r.claim.normalized_value.strip().upper()
            source_values[val].append(r)

        # If all claims agree, no contradiction
        if len(source_values) <= 1:
            continue

        # We have a contradiction! Build the finding
        entity_part = subject_key.split(".")[0] if "." in subject_key else subject_key
        aspect_part = subject_key.split(".")[-1] if "." in subject_key else concept
        aspect_label = _CONTRADICTION_LABELS.get(aspect_part, aspect_part)

        # Docs-first weighting using explicit source authority; code is implementation evidence,
        # not the primary target-state source.
        value_weights: dict[str, float] = {}
        for value, value_records in source_values.items():
            weight = 0.0
            for r in value_records:
                weight += _record_weight(record=r)
            value_weights[value] = weight

        # Sort by confidence (highest first)
        ranked = sorted(value_weights.items(), key=lambda x: -x[1])
        majority_value, majority_weight = ranked[0]
        minority_values = [(v, w) for v, w in ranked[1:]]

        # Build evidence quotes from each side
        evidence_parts: list[str] = []
        all_locations: list[AuditLocation] = []
        for value, value_records in source_values.items():
            count = len(value_records)
            code_count = sum(1 for r in value_records if r.claim.source_type == "github_file")
            weight_label = f" [{count}x erwaehnt"
            if code_count:
                weight_label += f", {code_count}x im Code"
            weight_label += "]"
            for r in value_records[:2]:  # Max 2 sources per value
                source_label = _source_label(r)
                quote = str(r.evidence.matched_text or "")[:150]
                evidence_parts.append(f"{source_label}: \u00ab{quote}\u00bb → {value}{weight_label}")
                if r.evidence.location:
                    all_locations.append(r.evidence.location)

        # Recommendation with majority/minority
        minority_names = ", ".join(v for v, _ in minority_values)
        recommendation = (
            f"Mehrheitsposition ({majority_value}, Gewicht {majority_weight:.0f}): "
            f"wahrscheinlich die intendierte Definition. "
            f"Minderheitsposition ({minority_names}): "
            f"wahrscheinlich veraltet oder fehlerhaft — pruefen und konsolidieren."
        )

        values_list = list(source_values.keys())
        finding = AuditFinding(
            severity="high",
            category="contradiction",
            title=f"Widerspruch: {entity_part} {aspect_label}",
            summary=(
                f"Verschiedene Quellen definieren widersprüchliche Werte für "
                f"{entity_part}.{aspect_part}:\n\n"
                + "\n".join(evidence_parts)
            ),
            recommendation=recommendation,
            canonical_key=f"bsm_contradiction|{subject_key}|{concept}",
            locations=all_locations[:4],
            proposed_jira_action=_proposed_jira_action(subject_key=subject_key, records=concept_records, target_value=majority_value),
            metadata={
                "generated_by": "bsm_domain_contradiction_detector",
                "subject_key": subject_key,
                "concept": concept,
                "conflicting_values": values_list,
                "majority_value": majority_value,
                "majority_weight": majority_weight,
                "source_count": len({r.claim.source_id for r in concept_records}),
                "source_authorities": sorted({str(r.claim.source_authority) for r in concept_records}),
            },
        )
        findings.append(finding)

    return findings


def _check_role_contradictions(
    *, subject_key: str, records: list[ExtractedClaimRecord],
) -> list[AuditFinding]:
    """Check for role contradictions (entity should exist vs should not exist)."""
    findings: list[AuditFinding] = []

    role_records = [r for r in records if r.claim.predicate in _ROLE_PREDICATES]
    if len(role_records) < 2:
        return findings

    # Look for opposing role claims from different sources
    source_claims: dict[str, list[ExtractedClaimRecord]] = defaultdict(list)
    for r in role_records:
        source_claims[r.claim.source_id].append(r)

    if len(source_claims) < 2:
        return findings

    # Check if assertions contain opposing signals
    positive_signals = {"pflicht", "zentral", "required", "mandatory", "eigenstaendig", "exists", "definiert"}
    negative_signals = {"entfaellt", "entfällt", "kein", "nicht", "no ", "removed", "deprecated", "soll nicht"}

    positive_sources: list[ExtractedClaimRecord] = []
    negative_sources: list[ExtractedClaimRecord] = []
    existence_sources: list[ExtractedClaimRecord] = []

    for r in role_records:
        val_lower = r.claim.normalized_value.lower()
        if r.claim.predicate == "metamodel_entity_exists":
            existence_sources.append(r)
        elif any(sig in val_lower for sig in negative_signals):
            negative_sources.append(r)
        elif any(sig in val_lower for sig in positive_signals):
            positive_sources.append(r)

    # Contradiction: negative assertion + positive assertion or metamodel existence
    if negative_sources and (positive_sources or existence_sources):
        entity_part = subject_key.split(".")[0] if "." in subject_key else subject_key
        evidence_parts: list[str] = []
        all_locations: list[AuditLocation] = []

        for r in negative_sources[:2]:
            source_label = _source_label(r)
            quote = str(r.evidence.matched_text or "")[:150]
            evidence_parts.append(f"{source_label} (NEGATIV): \u00ab{quote}\u00bb")
            if r.evidence.location:
                all_locations.append(r.evidence.location)

        for r in (positive_sources + existence_sources)[:2]:
            source_label = _source_label(r)
            quote = str(r.evidence.matched_text or "")[:150]
            evidence_parts.append(f"{source_label} (POSITIV): \u00ab{quote}\u00bb")
            if r.evidence.location:
                all_locations.append(r.evidence.location)

        finding = AuditFinding(
            severity="high",
            category="contradiction",
            title=f"Rollenwiderspruch: {entity_part}",
            summary=(
                f"{entity_part} hat widersprüchliche Rollendefinitionen:\n\n"
                + "\n".join(evidence_parts)
            ),
            recommendation=(
                f"Die Rolle von {entity_part} muss eindeutig geklärt werden. "
                f"Entweder ist es ein eigenstaendiges Element oder es entfaellt — "
                f"nicht beides gleichzeitig."
            ),
            canonical_key=f"bsm_role_contradiction|{subject_key}",
            locations=all_locations[:4],
            proposed_jira_action=_proposed_jira_action(subject_key=subject_key, records=role_records, target_value="role_clarification_required"),
            metadata={
                "generated_by": "bsm_domain_contradiction_detector",
                "subject_key": subject_key,
                "positive_count": len(positive_sources) + len(existence_sources),
                "negative_count": len(negative_sources),
                "source_authorities": sorted({str(r.claim.source_authority) for r in role_records}),
            },
        )
        findings.append(finding)

    return findings


def _source_label(record: ExtractedClaimRecord) -> str:
    """Build a human-readable source label."""
    labels = {
        "confluence_page": "Confluence",
        "local_doc": "Lokales Dokument",
        "github_file": "Code",
        "metamodel": "Metamodell",
    }
    base = labels.get(record.claim.source_type, record.claim.source_type)
    source_id = record.claim.source_id or ""
    # Shorten source_id to filename
    if "/" in source_id:
        source_id = source_id.split("/")[-1]
    section = ""
    if record.evidence.location and record.evidence.location.position:
        section = f", {record.evidence.location.position.section_path or ''}"
    return f"{base} ({source_id}{section})"


def _normalize_predicate_concept(predicate: str) -> str:
    normalized = predicate
    for prefix in ("puml_", "documented_", "code_", "yaml_", "metamodel_"):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):]
    normalized = normalized.replace("allowlist_", "")
    return normalized


def _detect_code_risks(*, domain_claims: Sequence[ExtractedClaimRecord]) -> list[AuditFinding]:
    findings: list[AuditFinding] = []
    seen_keys: set[str] = set()
    support_index: dict[tuple[str, str], list[ExtractedClaimRecord]] = defaultdict(list)
    grouped_manual_boundary_records: dict[tuple[str, str, str], list[ExtractedClaimRecord]] = defaultdict(list)
    grouped_chain_interruption_records: dict[tuple[str, str, str], list[ExtractedClaimRecord]] = defaultdict(list)
    grouped_eventual_consistency_records: dict[tuple[str, str, str], list[ExtractedClaimRecord]] = defaultdict(list)
    for record in domain_claims:
        if record.claim.predicate in {"code_temporal_sequence", "code_missing_required_field", "code_propagation_context"}:
            support_index[(record.claim.source_id, record.claim.subject_key)].append(record)
    for record in domain_claims:
        config = _CODE_RISK_CONFIG.get(record.claim.predicate)
        if config is None:
            continue
        canonical_key = f"bsm_code_risk|{record.claim.predicate}|{record.claim.source_id}|{record.claim.subject_key}"
        if canonical_key in seen_keys:
            continue
        seen_keys.add(canonical_key)
        quote = str(record.evidence.matched_text or record.claim.normalized_value or "").strip()
        support_records = support_index.get((record.claim.source_id, record.claim.subject_key), [])
        sequence_values = sorted(
            {
                str(item.claim.normalized_value or "").strip()
                for item in support_records
                if item.claim.predicate == "code_temporal_sequence"
            }
        )
        sequence_functions = sorted(
            {
                str((item.claim.metadata or {}).get("function_name") or "").strip()
                for item in support_records
                if item.claim.predicate == "code_temporal_sequence"
                and str((item.claim.metadata or {}).get("function_name") or "").strip()
            }
        )
        sequence_line_windows = sorted(
            {
                f"L{(item.claim.metadata or {}).get('sequence_start_line')}→L{(item.claim.metadata or {}).get('sequence_end_line')}"
                for item in support_records
                if item.claim.predicate == "code_temporal_sequence"
                and (item.claim.metadata or {}).get("sequence_start_line") is not None
                and (item.claim.metadata or {}).get("sequence_end_line") is not None
            }
        )
        missing_fields = sorted(
            {
                str(item.claim.normalized_value or "").strip()
                for item in support_records
                if item.claim.predicate == "code_missing_required_field"
            }
        )
        propagation_contexts = sorted(
            {
                str(item.claim.normalized_value or "").strip()
                for item in support_records
                if item.claim.predicate == "code_propagation_context"
            }
        )
        effective_config = config
        boundary_path_type = ""
        eventual_consistency_subtype = ""
        eventual_config: dict[str, str] | None = None
        if record.claim.predicate == "code_eventual_consistency_risk":
            eventual_consistency_subtype = _eventual_consistency_subtype(
                source_id=record.claim.source_id,
                support_records=support_records,
                fallback_function_name=str((record.claim.metadata or {}).get("function_name") or "").strip(),
            )
            eventual_config = _eventual_consistency_config(subtype=eventual_consistency_subtype)
            effective_config = {
                "category": config["category"],
                "severity": config["severity"],
                "title": eventual_config["title"],
                "recommendation": eventual_config["recommendation"],
            }
        if (
            record.claim.predicate == "code_field_propagation_gap"
            and record.claim.subject_key == "bsmAnswer.field_propagation"
            and propagation_contexts == ["line_context"]
        ):
            boundary_path_type = "manual_answer_entrypoint"
            grouped_manual_boundary_records[
                (record.claim.predicate, record.claim.subject_key, boundary_path_type)
            ].append(record)
            continue
        if (
            record.claim.predicate == "code_eventual_consistency_risk"
            and eventual_consistency_subtype in _GROUPABLE_EVENTUAL_CONSISTENCY_SUBTYPES
        ):
            grouped_eventual_consistency_records[
                (record.claim.predicate, record.claim.subject_key, eventual_consistency_subtype)
            ].append(record)
            continue
        if (
            record.claim.predicate == "code_chain_interruption_risk"
            and record.claim.subject_key == "TemporalConsistency.supersede_then_rebuild"
        ):
            grouped_chain_interruption_records[
                (record.claim.predicate, record.claim.subject_key, "reaggregation_rebuild_path")
            ].append(record)
            continue
        findings.append(
            AuditFinding(
                severity=effective_config["severity"],  # type: ignore[arg-type]
                category=effective_config["category"],  # type: ignore[arg-type]
                title=effective_config["title"],
                summary=(
                    f"{record.claim.subject_key} zeigt ein strukturelles Code-Risiko in "
                    f"{_source_label(record)}.\n\n"
                    + _risk_summary_detail(
                        risk_predicate=record.claim.predicate,
                        support_records=support_records,
                        subject_key=record.claim.subject_key,
                    )
                    + f"Extrahierter Hinweis: {record.claim.normalized_value}\n"
                    f"Evidenz: «{quote[:220]}»"
                ),
                recommendation=effective_config["recommendation"],
                canonical_key=canonical_key,
                locations=[record.evidence.location] if record.evidence.location else [],
                metadata={
                    "generated_by": "bsm_domain_contradiction_detector",
                    "risk_predicate": record.claim.predicate,
                    "subject_key": record.claim.subject_key,
                    "source_id": record.claim.source_id,
                    "support_predicates": sorted({item.claim.predicate for item in support_records}),
                    "sequence_values": sequence_values,
                    "sequence_functions": sequence_functions,
                    "sequence_line_windows": sequence_line_windows,
                    "missing_fields": missing_fields,
                    "propagation_contexts": propagation_contexts,
                    "eventual_consistency_subtype": eventual_consistency_subtype,
                    "eventual_consistency_label": (eventual_config or {}).get("label", ""),
                    "boundary_path_type": boundary_path_type,
                    "legacy_path_gap": bool(boundary_path_type),
                    **_risk_support_metadata(
                        risk_predicate=record.claim.predicate,
                        support_records=support_records,
                        subject_key=record.claim.subject_key,
                    ),
                },
            )
        )
    for (risk_predicate, subject_key, eventual_subtype), records in grouped_eventual_consistency_records.items():
        if not records:
            continue
        canonical_key = f"bsm_code_risk_group|{risk_predicate}|{subject_key}|{eventual_subtype}"
        if canonical_key in seen_keys:
            continue
        seen_keys.add(canonical_key)
        config = _eventual_consistency_config(subtype=eventual_subtype)
        source_ids = sorted({record.claim.source_id for record in records})
        locations = [record.evidence.location for record in records if record.evidence.location]
        support_predicates: set[str] = set()
        sequence_values: set[str] = set()
        sequence_functions: set[str] = set()
        sequence_line_windows: set[str] = set()
        for record in records:
            support_records = support_index.get((record.claim.source_id, record.claim.subject_key), [])
            support_predicates.update(item.claim.predicate for item in support_records)
            sequence_values.update(
                str(item.claim.normalized_value or "").strip()
                for item in support_records
                if item.claim.predicate == "code_temporal_sequence"
            )
            sequence_functions.update(
                str((item.claim.metadata or {}).get("function_name") or "").strip()
                for item in support_records
                if item.claim.predicate == "code_temporal_sequence"
                and str((item.claim.metadata or {}).get("function_name") or "").strip()
            )
            sequence_line_windows.update(
                f"L{(item.claim.metadata or {}).get('sequence_start_line')}→L{(item.claim.metadata or {}).get('sequence_end_line')}"
                for item in support_records
                if item.claim.predicate == "code_temporal_sequence"
                and (item.claim.metadata or {}).get("sequence_start_line") is not None
                and (item.claim.metadata or {}).get("sequence_end_line") is not None
            )
        path_count = max(len(sequence_functions), len(records), len(source_ids))
        findings.append(
            AuditFinding(
                severity=_CODE_RISK_CONFIG[risk_predicate]["severity"],  # type: ignore[arg-type]
                category=_CODE_RISK_CONFIG[risk_predicate]["category"],  # type: ignore[arg-type]
                title=config["title"],
                summary=(
                    f"{subject_key} zeigt dieselbe Async-Luecke in {path_count} Pfaden vom Typ {config['label']} "
                    f"({', '.join(_source_label(record) for record in records[:3])}).\n\n"
                    f"Extrahierter Hinweis: Persistenz wird vor dem Enqueue abgeschlossen, ohne geschuetzten Reaggregationsschritt dazwischen.\n"
                    f"Betroffene Funktionen: {', '.join(sorted(sequence_functions)) or 'unbekannt'}"
                ),
                recommendation=config["recommendation"],
                canonical_key=canonical_key,
                locations=locations,
                metadata={
                    "generated_by": "bsm_domain_contradiction_detector",
                    "risk_predicate": risk_predicate,
                    "subject_key": subject_key,
                    "source_ids": source_ids,
                    "support_predicates": sorted(support_predicates),
                    "sequence_values": sorted(item for item in sequence_values if item),
                    "sequence_functions": sorted(item for item in sequence_functions if item),
                    "sequence_line_windows": sorted(item for item in sequence_line_windows if item),
                    "eventual_consistency_subtype": eventual_subtype,
                    "eventual_consistency_label": config["label"],
                    "grouped_eventual_package_title": config["grouped_package_title"],
                    "grouped_eventual_scope_label": config["grouped_scope_label"],
                    "grouped_eventual_paths": path_count > 1,
                    "eventual_path_type": eventual_subtype,
                    "path_count": path_count,
                    **_grouped_sequence_metadata(
                        risk_predicate=risk_predicate,
                        records=records,
                        support_index=support_index,
                    ),
                },
            )
        )
    for (risk_predicate, subject_key, chain_path_type), records in grouped_chain_interruption_records.items():
        if not records:
            continue
        canonical_key = f"bsm_code_risk_group|{risk_predicate}|{subject_key}|{chain_path_type}"
        if canonical_key in seen_keys:
            continue
        seen_keys.add(canonical_key)
        source_ids = sorted({record.claim.source_id for record in records})
        locations = [record.evidence.location for record in records if record.evidence.location]
        support_predicates: set[str] = set()
        sequence_values: set[str] = set()
        sequence_functions: set[str] = set()
        sequence_line_windows: set[str] = set()
        for record in records:
            support_records = support_index.get((record.claim.source_id, record.claim.subject_key), [])
            support_predicates.update(item.claim.predicate for item in support_records)
            sequence_values.update(
                str(item.claim.normalized_value or "").strip()
                for item in support_records
                if item.claim.predicate == "code_temporal_sequence"
            )
            sequence_functions.update(
                str((item.claim.metadata or {}).get("function_name") or "").strip()
                for item in support_records
                if item.claim.predicate == "code_temporal_sequence"
                and str((item.claim.metadata or {}).get("function_name") or "").strip()
            )
            sequence_line_windows.update(
                f"L{(item.claim.metadata or {}).get('sequence_start_line')}→L{(item.claim.metadata or {}).get('sequence_end_line')}"
                for item in support_records
                if item.claim.predicate == "code_temporal_sequence"
                and (item.claim.metadata or {}).get("sequence_start_line") is not None
                and (item.claim.metadata or {}).get("sequence_end_line") is not None
            )
        findings.append(
            AuditFinding(
                severity=_GROUPED_CHAIN_INTERRUPTION_RISK_CONFIG["severity"],  # type: ignore[arg-type]
                category=_GROUPED_CHAIN_INTERRUPTION_RISK_CONFIG["category"],  # type: ignore[arg-type]
                title=_GROUPED_CHAIN_INTERRUPTION_RISK_CONFIG["title"],
                summary=(
                    f"{subject_key} zeigt dieselbe Kettenunterbrechung in {len(source_ids)} Reaggregationspfaden "
                    f"({', '.join(_source_label(record) for record in records[:3])}).\n\n"
                    f"Extrahierter Hinweis: Supersede erfolgt vor Rebuild/Materialisierung.\n"
                    f"Betroffene Funktionen: {', '.join(sorted(sequence_functions)) or 'unbekannt'}"
                ),
                recommendation=_GROUPED_CHAIN_INTERRUPTION_RISK_CONFIG["recommendation"],
                canonical_key=canonical_key,
                locations=locations,
                metadata={
                    "generated_by": "bsm_domain_contradiction_detector",
                    "risk_predicate": risk_predicate,
                    "subject_key": subject_key,
                    "source_ids": source_ids,
                    "support_predicates": sorted(support_predicates),
                    "sequence_values": sorted(item for item in sequence_values if item),
                    "sequence_functions": sorted(item for item in sequence_functions if item),
                    "sequence_line_windows": sorted(item for item in sequence_line_windows if item),
                    "grouped_chain_paths": True,
                    "chain_path_type": chain_path_type,
                    "path_count": len(source_ids),
                    **_grouped_sequence_metadata(
                        risk_predicate=risk_predicate,
                        records=records,
                        support_index=support_index,
                    ),
                },
            )
        )
    for (risk_predicate, subject_key, boundary_path_type), records in grouped_manual_boundary_records.items():
        if not records:
            continue
        canonical_key = f"bsm_code_risk_group|{risk_predicate}|{subject_key}|{boundary_path_type}"
        if canonical_key in seen_keys:
            continue
        seen_keys.add(canonical_key)
        source_ids = sorted({record.claim.source_id for record in records})
        locations = [record.evidence.location for record in records if record.evidence.location]
        support_predicates: set[str] = set()
        missing_fields: set[str] = set()
        propagation_contexts: set[str] = set()
        function_names: set[str] = set()
        for record in records:
            support_records = support_index.get((record.claim.source_id, record.claim.subject_key), [])
            support_predicates.update(item.claim.predicate for item in support_records)
            missing_fields.update(
                str(item.claim.normalized_value or "").strip()
                for item in support_records
                if item.claim.predicate == "code_missing_required_field"
            )
            propagation_contexts.update(
                str(item.claim.normalized_value or "").strip()
                for item in support_records
                if item.claim.predicate == "code_propagation_context"
            )
            function_name = str((record.claim.metadata or {}).get("function_name") or "").strip()
            if function_name:
                function_names.add(function_name)
        findings.append(
            AuditFinding(
                severity=_MANUAL_BOUNDARY_RISK_CONFIG["severity"],  # type: ignore[arg-type]
                category=_MANUAL_BOUNDARY_RISK_CONFIG["category"],  # type: ignore[arg-type]
                title=_MANUAL_BOUNDARY_RISK_CONFIG["title"],
                summary=(
                    f"{subject_key} zeigt dieselbe Scope-Luecke in {len(source_ids)} manuellen Boundary-Pfaden "
                    f"({', '.join(_source_label(record) for record in records[:2])}).\n\n"
                    f"Extrahierter Hinweis: phase_run_id fehlt im manuellen Antwort-Entry-Point.\n"
                    f"Betroffene Funktionen: {', '.join(sorted(function_names)) or 'unbekannt'}"
                ),
                recommendation=_MANUAL_BOUNDARY_RISK_CONFIG["recommendation"],
                canonical_key=canonical_key,
                locations=locations,
                metadata={
                    "generated_by": "bsm_domain_contradiction_detector",
                    "risk_predicate": risk_predicate,
                    "subject_key": subject_key,
                    "source_ids": source_ids,
                    "support_predicates": sorted(support_predicates),
                    "missing_fields": sorted(item for item in missing_fields if item),
                    "propagation_contexts": sorted(item for item in propagation_contexts if item),
                    "boundary_path_type": boundary_path_type,
                    "boundary_function_names": sorted(function_names),
                    "legacy_path_gap": True,
                    "grouped_boundary_paths": True,
                    "path_count": len(source_ids),
                    **_grouped_propagation_metadata(
                        records=records,
                        support_index=support_index,
                        subject_key=subject_key,
                    ),
                },
            )
        )
    return findings


def _risk_summary_detail(
    *,
    risk_predicate: str,
    support_records: Sequence[ExtractedClaimRecord],
    subject_key: str,
) -> str:
    if risk_predicate in {"code_eventual_consistency_risk", "code_chain_interruption_risk"}:
        variants = _sequence_variants(support_records=support_records)
        if not variants:
            return ""
        primary = variants[0]
        segment = str(primary.get("missing_sequence_segment_path") or "").strip()
        return (
            f"Fehlende Sequenzsicherung: {segment or 'unbekannt'}\n"
            f"Beobachteter Ablauf: {' -> '.join(primary['observed_sequence_path'])}\n"
        )
    if risk_predicate == "code_field_propagation_gap":
        variants = _propagation_variants(support_records=support_records, subject_key=subject_key)
        if not variants:
            return ""
        primary = variants[0]
        missing = ", ".join(primary["missing_field_segments"]) or "unbekannt"
        return (
            f"Fehlende Feldsegmente: {missing}\n"
            f"Betroffener Propagationspfad: {' -> '.join(primary['propagation_path'])}\n"
        )
    return ""


def _risk_support_metadata(
    *,
    risk_predicate: str,
    support_records: Sequence[ExtractedClaimRecord],
    subject_key: str,
) -> dict[str, object]:
    if risk_predicate in {"code_eventual_consistency_risk", "code_chain_interruption_risk"}:
        variants = _sequence_variants(support_records=support_records)
        if not variants:
            return {}
        primary = variants[0]
        return {
            "sequence_break_mode": primary["sequence_break_mode"],
            "observed_sequence_path": primary["observed_sequence_path"],
            "expected_sequence_path": primary["expected_sequence_path"],
            "missing_sequence_segments": primary["missing_sequence_segments"],
            "missing_sequence_segment_path": primary["missing_sequence_segment_path"],
            "sequence_break_before": primary["sequence_break_before"],
            "sequence_break_after": primary["sequence_break_after"],
            "sequence_rejoin_at": primary["sequence_rejoin_at"],
            "matched_sequence_variants": variants,
        }
    if risk_predicate == "code_field_propagation_gap":
        variants = _propagation_variants(support_records=support_records, subject_key=subject_key)
        if not variants:
            return {}
        primary = variants[0]
        return {
            "propagation_break_mode": primary["propagation_break_mode"],
            "propagation_path": primary["propagation_path"],
            "expected_propagation_fields": primary["expected_propagation_fields"],
            "missing_field_segments": primary["missing_field_segments"],
            "propagation_break_before": primary["propagation_break_before"],
            "propagation_break_after": primary["propagation_break_after"],
            "propagation_rejoin_at": primary["propagation_rejoin_at"],
            "matched_propagation_variants": variants,
        }
    return {}


def _grouped_sequence_metadata(
    *,
    risk_predicate: str,
    records: Sequence[ExtractedClaimRecord],
    support_index: dict[tuple[str, str], list[ExtractedClaimRecord]],
) -> dict[str, object]:
    variants: list[dict[str, object]] = []
    for record in records:
        variants.extend(
            _sequence_variants(
                support_records=support_index.get((record.claim.source_id, record.claim.subject_key), [])
            )
        )
    unique_variants = _unique_variant_dicts(variants)
    if not unique_variants:
        return {}
    primary = unique_variants[0]
    return {
        "sequence_break_mode": primary["sequence_break_mode"],
        "observed_sequence_path": primary["observed_sequence_path"],
        "expected_sequence_path": primary["expected_sequence_path"],
        "missing_sequence_segments": primary["missing_sequence_segments"],
        "missing_sequence_segment_path": primary["missing_sequence_segment_path"],
        "sequence_break_before": primary["sequence_break_before"],
        "sequence_break_after": primary["sequence_break_after"],
        "sequence_rejoin_at": primary["sequence_rejoin_at"],
        "matched_sequence_variants": unique_variants,
        "grouped_sequence_variants": True,
    }


def _grouped_propagation_metadata(
    *,
    records: Sequence[ExtractedClaimRecord],
    support_index: dict[tuple[str, str], list[ExtractedClaimRecord]],
    subject_key: str,
) -> dict[str, object]:
    variants: list[dict[str, object]] = []
    for record in records:
        variants.extend(
            _propagation_variants(
                support_records=support_index.get((record.claim.source_id, record.claim.subject_key), []),
                subject_key=subject_key,
            )
        )
    unique_variants = _unique_variant_dicts(variants)
    if not unique_variants:
        return {}
    primary = unique_variants[0]
    return {
        "propagation_break_mode": primary["propagation_break_mode"],
        "propagation_path": primary["propagation_path"],
        "expected_propagation_fields": primary["expected_propagation_fields"],
        "missing_field_segments": primary["missing_field_segments"],
        "propagation_break_before": primary["propagation_break_before"],
        "propagation_break_after": primary["propagation_break_after"],
        "propagation_rejoin_at": primary["propagation_rejoin_at"],
        "matched_propagation_variants": unique_variants,
        "grouped_propagation_variants": True,
    }


def _sequence_variants(
    *,
    support_records: Sequence[ExtractedClaimRecord],
) -> list[dict[str, object]]:
    variants: list[dict[str, object]] = []
    for item in support_records:
        if item.claim.predicate != "code_temporal_sequence":
            continue
        metadata = item.claim.metadata or {}
        observed_sequence_path = list(metadata.get("sequence_path") or _fallback_sequence_path(item.claim.normalized_value))
        expected_sequence_path = list(metadata.get("expected_sequence_path") or _fallback_expected_sequence_path(item.claim.normalized_value))
        missing_sequence_segments = [
            part for part in expected_sequence_path
            if part not in observed_sequence_path
        ]
        variants.append(
            {
                "function_name": str(metadata.get("function_name") or "").strip(),
                "line_window": (
                    f"L{metadata.get('sequence_start_line')}→L{metadata.get('sequence_end_line')}"
                    if metadata.get("sequence_start_line") is not None
                    and metadata.get("sequence_end_line") is not None
                    else ""
                ),
                "sequence_break_mode": str(metadata.get("sequence_break_mode") or _fallback_sequence_break_mode(item.claim.normalized_value)),
                "observed_sequence_path": observed_sequence_path,
                "expected_sequence_path": expected_sequence_path,
                "missing_sequence_segments": missing_sequence_segments,
                "missing_sequence_segment_path": " -> ".join(missing_sequence_segments),
                "sequence_break_before": metadata.get("sequence_break_before") or (observed_sequence_path[0] if observed_sequence_path else None),
                "sequence_break_after": metadata.get("sequence_break_after") or (observed_sequence_path[-1] if observed_sequence_path else None),
                "sequence_rejoin_at": observed_sequence_path[-1] if observed_sequence_path else None,
            }
        )
    return variants


def _propagation_variants(
    *,
    support_records: Sequence[ExtractedClaimRecord],
    subject_key: str,
) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], dict[str, object]] = {}
    subject_entity = subject_key.split(".")[0] if "." in subject_key else subject_key
    for item in support_records:
        metadata = item.claim.metadata or {}
        function_name = str(metadata.get("function_name") or "").strip()
        target_entity = str(metadata.get("target_entity") or subject_entity).strip()
        key = (function_name, target_entity)
        entry = grouped.setdefault(
            key,
            {
                "function_name": function_name,
                "target_entity": target_entity,
                "propagation_break_mode": str(metadata.get("propagation_break_mode") or "field_drop"),
                "propagation_path": list(metadata.get("propagation_path") or [function_name or "entrypoint", target_entity]),
                "expected_propagation_fields": list(metadata.get("expected_propagation_fields") or ["phase_run_id"]),
                "missing_field_segments": [],
                "propagation_contexts": [],
                "propagation_break_before": function_name or "entrypoint",
                "propagation_break_after": target_entity,
                "propagation_rejoin_at": target_entity,
            },
        )
        if item.claim.predicate == "code_missing_required_field":
            field_name = str(item.claim.normalized_value or "").strip()
            if field_name and field_name not in entry["missing_field_segments"]:
                entry["missing_field_segments"].append(field_name)
        if item.claim.predicate == "code_propagation_context":
            context_name = str(item.claim.normalized_value or "").strip()
            if context_name and context_name not in entry["propagation_contexts"]:
                entry["propagation_contexts"].append(context_name)
    return sorted(grouped.values(), key=lambda item: (str(item["function_name"]), str(item["target_entity"])))


def _unique_variant_dicts(variants: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    unique: list[dict[str, object]] = []
    seen: set[str] = set()
    for item in variants:
        key = repr(sorted(item.items()))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _fallback_sequence_path(value: str) -> list[str]:
    normalized = str(value or "").strip()
    if normalized == "persist_before_enqueue":
        return ["persist", "enqueue"]
    if normalized == "supersede_before_rebuild":
        return ["supersede", "rebuild"]
    return [normalized] if normalized else []


def _fallback_expected_sequence_path(value: str) -> list[str]:
    normalized = str(value or "").strip()
    if normalized == "persist_before_enqueue":
        return ["persist", "protected_reaggregation", "enqueue"]
    if normalized == "supersede_before_rebuild":
        return ["supersede", "replacement_chain_available", "rebuild"]
    return _fallback_sequence_path(normalized)


def _fallback_sequence_break_mode(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized == "persist_before_enqueue":
        return "async_gap"
    if normalized == "supersede_before_rebuild":
        return "replacement_gap"
    return "sequence_gap"


def _eventual_consistency_config(*, subtype: str) -> dict[str, str]:
    return _EVENTUAL_CONSISTENCY_SUBTYPE_CONFIG.get(
        subtype,
        _EVENTUAL_CONSISTENCY_SUBTYPE_CONFIG["generic_async_gap"],
    )


def _eventual_consistency_subtype(
    *,
    source_id: str,
    support_records: Sequence[ExtractedClaimRecord],
    fallback_function_name: str,
) -> str:
    path = str(source_id or "").casefold()
    function_names = {
        str((item.claim.metadata or {}).get("function_name") or "").strip().casefold()
        for item in support_records
        if str((item.claim.metadata or {}).get("function_name") or "").strip()
    }
    fallback_name = str(fallback_function_name or "").strip().casefold()
    if fallback_name:
        function_names.add(fallback_name)

    if (
        any(
            token in function_name
            for function_name in function_names
            for token in ("manual_answer", "phase_run_answer", "capture_ba_answer", "save_ba_answer")
        )
        or ("router_mining.py" in path and any("answer" in function_name for function_name in function_names))
    ):
        return "manual_answer_enqueue"
    if (
        "router_bsm_readiness.py" in path
        or "reaggregation" in path
        or any(
            token in function_name
            for function_name in function_names
            for token in ("refine_statement", "reaggregate", "reaggregation", "consolidate")
        )
    ):
        return "reaggregation_enqueue"
    if (
        any(
            token in path
            for token in (
                "router_ingestion.py",
                "atlassian_connector_ingest_helpers.py",
                "connector_orchestrator.py",
                "ingestion_single_runner.py",
            )
        )
        or any(
            token in function_name
            for function_name in function_names
            for token in ("ingest_stream", "persist_and_ingest", "execute_connector_sync")
        )
    ):
        return "connector_ingestion_enqueue"
    if (
        any(
            token in path
            for token in (
                "router_ui_files.py",
            )
        )
        or any(
            token in function_name
            for function_name in function_names
            for token in ("upload_files",)
        )
    ):
        return "upload_enqueue"
    if (
        "storage_index_service.py" in path
        or any(
            token in function_name
            for function_name in function_names
            for token in ("reconcile_from_storage",)
        )
    ):
        return "storage_reconcile_enqueue"
    if (
        any(
            token in path
            for token in (
                "ingestion/progress_tracker.py",
            )
        )
        or any(
            token in function_name
            for function_name in function_names
            for token in ("set_registry",)
        )
    ):
        return "workflow_registry_enqueue"
    if (
        any(
            token in path
            for token in (
                "ingestion/recovery.py",
            )
        )
        or any(
            token in function_name
            for function_name in function_names
            for token in ("recover_orphan_mining_tasks",)
        )
    ):
        return "ingestion_recovery_enqueue"
    if (
        any(
            token in path
            for token in (
                "bsm_phase_execution_service.py",
                "job_worker_bsm_phase_helpers.py",
            )
        )
        or any(
            token in function_name
            for function_name in function_names
            for token in ("enqueue_phase_for_chunk",)
        )
    ):
        return "phase_execution_enqueue"
    if (
        any(
            token in path
            for token in (
                "run_orchestration_service.py",
            )
        )
        or any(
            token in function_name
            for function_name in function_names
            for token in ("start_phase_run",)
        )
    ):
        return "run_start_enqueue"
    if (
        any(
            token in path
            for token in (
                "job_worker.py",
            )
        )
        or any(
            token in function_name
            for function_name in function_names
            for token in ("propagate_bsm_phase_run_failure",)
        )
    ):
        return "worker_failure_enqueue"
    if (
        any(
            token in path
            for token in (
                "service_chat_action.py",
            )
        )
    ):
        return "ui_action_dispatch_enqueue"
    if (
        any(
            token in path
            for token in (
                "graph_context.py",
            )
        )
    ):
        return "graph_system_enqueue"
    if (
        any(
            token in path
            for token in (
                "ingestion",
            )
        )
        or any(
            token in function_name
            for function_name in function_names
            for token in ("ingest",)
        )
    ):
        return "connector_ingestion_enqueue"
    if (
        any(
            token in path
            for token in (
                "job_worker",
            )
        )
        or any(
            token in function_name
            for function_name in function_names
            for token in ("enqueue_phase", "propagate", "start_phase_run")
        )
    ):
        return "orchestration_async_gap"
    return "generic_async_gap"


def _detect_evidence_chain_variant_conflicts(*, domain_claims: Sequence[ExtractedClaimRecord]) -> list[AuditFinding]:
    path_records = [
        record
        for record in domain_claims
        if record.claim.predicate in {"code_evidence_chain_path", "yaml_evidence_chain_path"}
    ]
    if len(path_records) < 2:
        return []

    families: dict[str, list[ExtractedClaimRecord]] = defaultdict(list)
    for record in path_records:
        family = _evidence_chain_family(str(record.claim.normalized_value or ""))
        if family:
            families[family].append(record)

    if len(families) < 2:
        return []

    observed_chain_variants = sorted({str(record.claim.normalized_value or "").strip() for record in path_records if str(record.claim.normalized_value or "").strip()})
    source_ids = sorted({record.claim.source_id for record in path_records})
    locations = [record.evidence.location for record in path_records[:4] if record.evidence.location]
    family_labels = sorted(families.keys())
    evidence_parts = [
        f"{_source_label(record)}: «{str(record.claim.normalized_value or '').strip()}»"
        for record in path_records[:4]
    ]

    return [
        AuditFinding(
            severity=_CODE_RISK_CONFIG["code_evidence_chain_variant_conflict"]["severity"],  # type: ignore[arg-type]
            category=_CODE_RISK_CONFIG["code_evidence_chain_variant_conflict"]["category"],  # type: ignore[arg-type]
            title=_CODE_RISK_CONFIG["code_evidence_chain_variant_conflict"]["title"],
            summary=(
                "Die aktive Implementierung zeigt mehrere konkurrierende Evidenzkettenvarianten.\n\n"
                + "\n".join(evidence_parts)
            ),
            recommendation=_CODE_RISK_CONFIG["code_evidence_chain_variant_conflict"]["recommendation"],
            canonical_key="bsm_code_risk|code_evidence_chain_variant_conflict|EvidenceChain.active_path",
            locations=locations,
            metadata={
                "generated_by": "bsm_domain_contradiction_detector",
                "risk_predicate": "code_evidence_chain_variant_conflict",
                "subject_key": "EvidenceChain.active_path",
                "source_ids": source_ids,
                "observed_chain_variants": observed_chain_variants,
                "variant_families": family_labels,
            },
        )
    ]


def _detect_evidence_chain_breaks(*, domain_claims: Sequence[ExtractedClaimRecord]) -> list[AuditFinding]:
    step_records = [
        record
        for record in domain_claims
        if record.claim.predicate in {"code_evidence_chain_step", "yaml_evidence_chain_step"}
    ]
    path_records = [
        record
        for record in domain_claims
        if record.claim.predicate in {"code_evidence_chain_path", "yaml_evidence_chain_path"}
    ]
    full_path_records = [
        record
        for record in domain_claims
        if record.claim.predicate in {"code_evidence_chain_full_path", "yaml_evidence_chain_full_path"}
    ]
    if not step_records:
        return []

    derived_records = [
        record
        for record in step_records
        if str((record.claim.metadata or {}).get("start_label") or "").strip() == "Statement"
        and str((record.claim.metadata or {}).get("relationship_type") or "").strip() == "DERIVED_FROM"
    ]
    support_records = [
        record
        for record in step_records
        if str((record.claim.metadata or {}).get("start_label") or "").strip() == "Statement"
        and str((record.claim.metadata or {}).get("end_label") or "").strip() == "BSM_Element"
        and str((record.claim.metadata or {}).get("relationship_type") or "").strip() == "SUPPORTS"
    ]
    if not derived_records and not support_records:
        return []

    observed_records = [*derived_records, *support_records]
    observed_chain_steps = _sorted_chain_steps(
        str(record.claim.normalized_value or "").strip() for record in observed_records
    )
    derived_targets = sorted(
        {
            str((record.claim.metadata or {}).get("end_label") or "").strip()
            for record in derived_records
            if str((record.claim.metadata or {}).get("end_label") or "").strip()
        }
    )
    if derived_records and not support_records:
        expected_chain_steps = [
            *observed_chain_steps,
            "Statement -[:SUPPORTS]-> BSM_Element",
        ]
        chain_break_at = "Statement.SUPPORTS"
        missing_expected_step = "Statement -[:SUPPORTS]-> BSM_Element"
        quote_record = derived_records[0]
        observed_chain_variants = sorted(
            {
                str(record.claim.normalized_value or "").strip()
                for record in path_records
                if str(record.claim.normalized_value or "").strip()
            }
        ) or [f"{target} -> Statement" for target in derived_targets] or ["Statement"]
        expected_chain_variants = [
            f"{target} -> Statement -> BSM_Element"
            for target in derived_targets
        ] or ["<summary_source> -> Statement -> BSM_Element"]
        observed_full_chain_variants = sorted(
            {
                str(record.claim.normalized_value or "").strip()
                for record in full_path_records
                if str(record.claim.normalized_value or "").strip()
            }
        ) or [f"bsmAnswer -> {target} -> Statement" for target in derived_targets] or ["bsmAnswer -> <summary_source> -> Statement"]
        expected_full_chain_variants = [
            f"bsmAnswer -> {target} -> Statement -> BSM_Element"
            for target in derived_targets
        ] or ["bsmAnswer -> <summary_source> -> Statement -> BSM_Element"]
    elif support_records and not derived_records:
        expected_chain_steps = [
            "Statement -[:DERIVED_FROM]-> <summary_source>",
            *observed_chain_steps,
        ]
        chain_break_at = "Statement.DERIVED_FROM"
        missing_expected_step = "Statement -[:DERIVED_FROM]-> <summary_source>"
        quote_record = support_records[0]
        observed_chain_variants = sorted(
            {
                str(record.claim.normalized_value or "").strip()
                for record in path_records
                if str(record.claim.normalized_value or "").strip()
            }
        ) or ["Statement -> BSM_Element"]
        expected_chain_variants = ["<summary_source> -> Statement -> BSM_Element"]
        observed_full_chain_variants = sorted(
            {
                str(record.claim.normalized_value or "").strip()
                for record in full_path_records
                if str(record.claim.normalized_value or "").strip()
            }
        ) or ["Statement -> BSM_Element"]
        expected_full_chain_variants = ["bsmAnswer -> <summary_source> -> Statement -> BSM_Element"]
    else:
        return []

    quote = str(quote_record.evidence.matched_text or quote_record.claim.normalized_value or "").strip()
    implementation_sources = sorted({record.claim.source_id for record in observed_records})
    break_matching = _match_break_variants(
        observed_full_chain_variants=observed_full_chain_variants,
        expected_full_chain_variants=expected_full_chain_variants,
    )
    break_context = _best_break_context(
        chain_break_at=chain_break_at,
        observed_full_chain_variants=observed_full_chain_variants,
        expected_full_chain_variants=expected_full_chain_variants,
        matched_pairs=break_matching["matched_pairs"],
    )
    incomplete_variant_count = len(break_matching["matched_pairs"]) or 1
    summary_prefix = (
        "Die aktive Implementierung materialisiert aktive Evidenzkettenvarianten, "
        "zeigt aber keinen korrespondierenden Statement-zu-BSM_Element-Hop."
        if chain_break_at == "Statement.SUPPORTS"
        else "Die aktive Implementierung materialisiert aktive Evidenzkettenvarianten mit Statement-zu-BSM_Element-Hop, "
        "zeigt aber keine korrespondierende Statement-Ableitung aus einer Summary-Quelle."
    )
    missing_segment_path = str(break_context.get("missing_chain_segment_path") or "").strip()
    return [
        AuditFinding(
            severity=_CODE_RISK_CONFIG["code_evidence_chain_break"]["severity"],  # type: ignore[arg-type]
            category=_CODE_RISK_CONFIG["code_evidence_chain_break"]["category"],  # type: ignore[arg-type]
            title=_CODE_RISK_CONFIG["code_evidence_chain_break"]["title"],
            summary=(
                f"{summary_prefix}\n\n"
                f"Unvollstaendige Varianten: {incomplete_variant_count}\n"
                + (f"Fehlende Segmentfolge: {missing_segment_path}\n" if missing_segment_path else "")
                + f"Extrahierter Hinweis: {observed_chain_steps[0]}\n"
                + f"Evidenz: «{quote[:220]}»"
            ),
            recommendation=_CODE_RISK_CONFIG["code_evidence_chain_break"]["recommendation"],
            canonical_key="bsm_code_risk|code_evidence_chain_break|EvidenceChain.active_path",
            locations=[record.evidence.location for record in observed_records[:3] if record.evidence.location],
            metadata={
                "generated_by": "bsm_domain_contradiction_detector",
                "risk_predicate": "code_evidence_chain_break",
                "subject_key": "EvidenceChain.active_path",
                "source_ids": implementation_sources,
                "chain_steps": observed_chain_steps,
                "observed_chain_path": observed_chain_steps,
                "expected_chain_path": expected_chain_steps,
                "observed_chain_variants": observed_chain_variants,
                "expected_chain_variants": expected_chain_variants,
                "observed_full_chain_variants": observed_full_chain_variants,
                "expected_full_chain_variants": expected_full_chain_variants,
                "matched_break_variants": break_matching["matched_pairs"],
                "unmatched_observed_full_chain_variants": break_matching["unmatched_observed_paths"],
                "unmatched_expected_full_chain_variants": break_matching["unmatched_expected_paths"],
                "chain_break_at": chain_break_at,
                "chain_break_mode": break_context["chain_break_mode"],
                "chain_break_index": break_context["chain_break_index"],
                "chain_break_before": break_context["chain_break_before"],
                "chain_break_after": break_context["chain_break_after"],
                "chain_rejoin_at": break_context["chain_rejoin_at"],
                "observed_full_chain_path": break_context["observed_full_chain_path"],
                "expected_full_chain_path": break_context["expected_full_chain_path"],
                "missing_chain_segments": break_context["missing_chain_segments"],
                "missing_chain_segment_path": break_context["missing_chain_segment_path"],
                "remaining_expected_path": break_context["remaining_expected_path"],
                "common_prefix": break_context["common_prefix"],
                "common_suffix": break_context["common_suffix"],
                "derived_targets": derived_targets,
                "missing_expected_step": missing_expected_step,
            },
        )
    ]


def _sorted_chain_steps(values: Sequence[str]) -> list[str]:
    unique = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return sorted(unique, key=_chain_step_sort_key)


def _chain_step_sort_key(value: str) -> tuple[int, str]:
    normalized = str(value or "").strip()
    if "DERIVED_FROM" in normalized:
        return (0, normalized)
    if "SUPPORTS" in normalized:
        return (1, normalized)
    return (9, normalized)


def _evidence_chain_family(value: str) -> str:
    normalized = str(value or "").strip().casefold()
    if "summarisedanswerunit" in normalized:
        return "unit_centric"
    if "summarisedanswer" in normalized:
        return "summary_centric"
    return ""


def _annotate_evidence_chain_full_path_conflicts(
    *,
    findings: list[AuditFinding],
    domain_claims: Sequence[ExtractedClaimRecord],
) -> list[AuditFinding]:
    documented_paths = sorted(
        {
            str(record.claim.normalized_value or "").strip()
            for record in domain_claims
            if record.claim.subject_key == "EvidenceChain.full_path"
            and record.claim.predicate in {"documented_evidence_chain_full_path", "puml_evidence_chain_full_path"}
            and str(record.claim.normalized_value or "").strip()
        }
    )
    implemented_paths = sorted(
        {
            str(record.claim.normalized_value or "").strip()
            for record in domain_claims
            if record.claim.subject_key == "EvidenceChain.full_path"
            and record.claim.predicate in {"code_evidence_chain_full_path", "yaml_evidence_chain_full_path"}
            and str(record.claim.normalized_value or "").strip()
        }
    )
    if not documented_paths or not implemented_paths:
        return findings

    enriched: list[AuditFinding] = []
    for finding in findings:
        if not (
            finding.category == "contradiction"
            and str(finding.metadata.get("subject_key") or "").strip() == "EvidenceChain.full_path"
        ):
            enriched.append(finding)
            continue

        path_matching = _match_full_path_variants(
            documented_paths=documented_paths,
            implemented_paths=implemented_paths,
        )
        documented_path, implemented_path, divergence = _best_full_path_divergence(
            documented_paths=documented_paths,
            implemented_paths=implemented_paths,
            matched_pairs=path_matching["matched_pairs"],
        )
        if divergence is None:
            enriched.append(finding)
            continue
        enriched.append(
            finding.model_copy(
                update={
                    "metadata": {
                        **finding.metadata,
                        "documented_full_chain_variants": documented_paths,
                        "implemented_full_chain_variants": implemented_paths,
                        "matched_full_chain_pairs": path_matching["matched_pairs"],
                        "unmatched_documented_full_chain_variants": path_matching["unmatched_documented_paths"],
                        "unmatched_implemented_full_chain_variants": path_matching["unmatched_implemented_paths"],
                        "documented_full_chain_path": documented_path,
                        "implemented_full_chain_path": implemented_path,
                        "documented_variant_family": _evidence_chain_family(documented_path),
                        "implemented_variant_family": _evidence_chain_family(implemented_path),
                        "full_path_divergence_index": divergence["index"],
                        "full_path_divergence_documented": divergence["documented_part"],
                        "full_path_divergence_implemented": divergence["implemented_part"],
                        "full_path_divergence_mode": divergence["divergence_mode"],
                        "full_path_divergence_prefix": divergence["common_prefix"],
                        "full_path_common_suffix": divergence["common_suffix"],
                        "full_path_documented_gap_segments": divergence["documented_gap_segments"],
                        "full_path_implemented_gap_segments": divergence["implemented_gap_segments"],
                        "full_path_documented_gap_segment_path": divergence["documented_gap_segment_path"],
                        "full_path_implemented_gap_segment_path": divergence["implemented_gap_segment_path"],
                        "full_path_rejoin_at": divergence["rejoin_at"],
                    }
                }
            )
        )
    return enriched


def _best_break_context(
    *,
    chain_break_at: str,
    observed_full_chain_variants: Sequence[str],
    expected_full_chain_variants: Sequence[str],
    matched_pairs: Sequence[dict[str, object]] | None = None,
) -> dict[str, object]:
    if matched_pairs:
        first_pair = matched_pairs[0]
        return {
            "chain_break_mode": first_pair["break_mode"],
            "chain_break_index": first_pair["chain_break_index"],
            "chain_break_before": first_pair["chain_break_before"],
            "chain_break_after": first_pair["chain_break_after"],
            "chain_rejoin_at": first_pair["chain_rejoin_at"],
            "observed_full_chain_path": first_pair["observed_path"],
            "expected_full_chain_path": first_pair["expected_path"],
            "missing_chain_segments": first_pair["missing_chain_segments"],
            "missing_chain_segment_path": first_pair["missing_chain_segment_path"],
            "remaining_expected_path": first_pair["remaining_expected_path"],
            "common_prefix": first_pair["common_prefix"],
            "common_suffix": first_pair["common_suffix"],
        }
    observed_full_chain_path = next(
        (str(value).strip() for value in observed_full_chain_variants if str(value).strip()),
        "",
    )
    expected_full_chain_path = next(
        (str(value).strip() for value in expected_full_chain_variants if str(value).strip()),
        "",
    )
    observed_parts = [part.strip() for part in observed_full_chain_path.split("->") if part.strip()]
    expected_parts = [part.strip() for part in expected_full_chain_path.split("->") if part.strip()]
    if chain_break_at == "Statement.SUPPORTS":
        before = "Statement"
        after = "BSM_Element"
        index = max(len(observed_parts), 0)
        missing_chain_segments = ["BSM_Element"]
        break_mode = "tail_gap"
        chain_rejoin_at = None
    elif chain_break_at == "Statement.DERIVED_FROM":
        before = expected_parts[-3] if len(expected_parts) >= 3 else "<summary_source>"
        after = "Statement"
        statement_index = expected_parts.index("Statement") if "Statement" in expected_parts else len(expected_parts)
        index = statement_index
        missing_chain_segments = expected_parts[:statement_index]
        break_mode = "prefix_gap"
        chain_rejoin_at = "Statement"
    else:
        before = observed_parts[-1] if observed_parts else None
        after = expected_parts[len(observed_parts)] if len(expected_parts) > len(observed_parts) else None
        index = len(observed_parts)
        missing_chain_segments = expected_parts[len(observed_parts):]
        break_mode = "tail_gap"
        chain_rejoin_at = None
    return {
        "chain_break_mode": break_mode,
        "chain_break_index": index,
        "chain_break_before": before,
        "chain_break_after": after,
        "chain_rejoin_at": chain_rejoin_at,
        "observed_full_chain_path": observed_full_chain_path,
        "expected_full_chain_path": expected_full_chain_path,
        "missing_chain_segments": missing_chain_segments,
        "missing_chain_segment_path": " -> ".join(missing_chain_segments),
        "remaining_expected_path": (
            " -> ".join(expected_parts[index:])
            if index < len(expected_parts)
            else ""
        ),
        "common_prefix": observed_parts[:index] if break_mode == "tail_gap" else [],
        "common_suffix": expected_parts[index:] if break_mode == "prefix_gap" else [],
    }


def _match_break_variants(
    *,
    observed_full_chain_variants: Sequence[str],
    expected_full_chain_variants: Sequence[str],
) -> dict[str, object]:
    candidates: list[dict[str, object]] = []
    for observed_path in observed_full_chain_variants:
        observed_parts = _chain_path_parts(observed_path)
        for expected_path in expected_full_chain_variants:
            expected_parts = _chain_path_parts(expected_path)
            break_details = _path_break_details(
                observed_parts=observed_parts,
                expected_parts=expected_parts,
            )
            if break_details is None:
                continue
            score = _break_match_score(
                observed_path=observed_path,
                expected_path=expected_path,
                break_details=break_details,
            )
            candidates.append(
                {
                    "observed_path": observed_path,
                    "expected_path": expected_path,
                    "score": score,
                    **break_details,
                }
            )

    candidates.sort(
        key=lambda item: (
            item["score"],
            -len(str(item["observed_path"])),
            -len(str(item["expected_path"])),
        ),
        reverse=True,
    )

    used_observed: set[str] = set()
    used_expected: set[str] = set()
    matched_pairs: list[dict[str, object]] = []
    for candidate in candidates:
        observed_path = str(candidate["observed_path"])
        expected_path = str(candidate["expected_path"])
        if observed_path in used_observed or expected_path in used_expected:
            continue
        used_observed.add(observed_path)
        used_expected.add(expected_path)
        matched_pairs.append(candidate)

    unmatched_observed_paths = [path for path in observed_full_chain_variants if path not in used_observed]
    unmatched_expected_paths = [path for path in expected_full_chain_variants if path not in used_expected]
    return {
        "matched_pairs": matched_pairs,
        "unmatched_observed_paths": unmatched_observed_paths,
        "unmatched_expected_paths": unmatched_expected_paths,
    }


def _path_break_details(
    *,
    observed_parts: Sequence[str],
    expected_parts: Sequence[str],
) -> dict[str, object] | None:
    if not observed_parts or not expected_parts or list(observed_parts) == list(expected_parts):
        return None
    divergence = _path_divergence(
        documented_parts=expected_parts,
        implemented_parts=observed_parts,
    )
    common_prefix = list(divergence["common_prefix"])
    common_suffix = list(divergence["common_suffix"])
    missing_start = len(common_prefix)
    missing_end = len(expected_parts) - len(common_suffix)
    missing_chain_segments = list(expected_parts[missing_start:missing_end])
    if not missing_chain_segments:
        return None

    if common_prefix and common_suffix:
        break_mode = "internal_gap"
        chain_break_index = missing_start
        chain_break_before = expected_parts[missing_start - 1] if missing_start > 0 else None
        chain_break_after = expected_parts[missing_start] if missing_start < len(expected_parts) else None
    elif common_prefix:
        break_mode = "tail_gap"
        chain_break_index = missing_start
        chain_break_before = expected_parts[missing_start - 1] if missing_start > 0 else None
        chain_break_after = expected_parts[missing_start] if missing_start < len(expected_parts) else None
    elif common_suffix:
        break_mode = "prefix_gap"
        chain_break_index = missing_end
        chain_break_before = expected_parts[missing_end - 1] if missing_end > 0 else None
        chain_break_after = expected_parts[missing_end] if missing_end < len(expected_parts) else None
    else:
        break_mode = "full_gap"
        chain_break_index = missing_start
        chain_break_before = expected_parts[missing_end - 1] if missing_end > 0 else None
        chain_break_after = expected_parts[missing_end] if missing_end < len(expected_parts) else None

    return {
        "break_mode": break_mode,
        "chain_break_index": chain_break_index,
        "chain_break_before": chain_break_before,
        "chain_break_after": chain_break_after,
        "chain_rejoin_at": expected_parts[missing_end] if missing_end < len(expected_parts) else None,
        "missing_chain_segments": missing_chain_segments,
        "missing_chain_segment_path": " -> ".join(missing_chain_segments),
        "remaining_expected_path": " -> ".join(expected_parts[chain_break_index:]),
        "common_prefix": common_prefix,
        "common_suffix": common_suffix,
    }


def _break_match_score(
    *,
    observed_path: str,
    expected_path: str,
    break_details: dict[str, object],
) -> tuple[int, int, int, int, int]:
    same_family = int(
        bool(_evidence_chain_family(observed_path))
        and _evidence_chain_family(observed_path) == _evidence_chain_family(expected_path)
    )
    common_prefix_len = len(break_details["common_prefix"])
    common_suffix_len = len(break_details["common_suffix"])
    missing_count = len(break_details["missing_chain_segments"])
    return (
        same_family,
        common_prefix_len + common_suffix_len,
        common_suffix_len,
        common_prefix_len,
        -missing_count,
    )


def _best_full_path_divergence(
    *,
    documented_paths: Sequence[str],
    implemented_paths: Sequence[str],
    matched_pairs: Sequence[dict[str, object]] | None = None,
) -> tuple[str, str, dict[str, object] | None]:
    if matched_pairs:
        first_pair = matched_pairs[0]
        return (
            str(first_pair["documented_path"]),
            str(first_pair["implemented_path"]),
            {
                "index": first_pair["divergence_index"],
                "documented_part": first_pair["divergence_documented"],
                "implemented_part": first_pair["divergence_implemented"],
                "common_prefix": first_pair["common_prefix"],
                "common_suffix": first_pair["common_suffix"],
                "divergence_mode": first_pair["divergence_mode"],
                "documented_gap_segments": first_pair["documented_gap_segments"],
                "implemented_gap_segments": first_pair["implemented_gap_segments"],
                "documented_gap_segment_path": first_pair["documented_gap_segment_path"],
                "implemented_gap_segment_path": first_pair["implemented_gap_segment_path"],
                "rejoin_at": first_pair["rejoin_at"],
            },
        )
    best_pair = ("", "", None)
    best_score: tuple[int, int, int] | None = None
    for documented_path in documented_paths:
        documented_parts = _chain_path_parts(documented_path)
        for implemented_path in implemented_paths:
            implemented_parts = _chain_path_parts(implemented_path)
            divergence = _path_divergence(
                documented_parts=documented_parts,
                implemented_parts=implemented_parts,
            )
            score = _full_path_match_score(
                documented_path=documented_path,
                implemented_path=implemented_path,
                divergence=divergence,
            )
            if best_score is None or score > best_score:
                best_pair = (documented_path, implemented_path, divergence)
                best_score = score
    return best_pair


def _match_full_path_variants(
    *,
    documented_paths: Sequence[str],
    implemented_paths: Sequence[str],
) -> dict[str, object]:
    candidates: list[dict[str, object]] = []
    for documented_path in documented_paths:
        documented_parts = _chain_path_parts(documented_path)
        for implemented_path in implemented_paths:
            implemented_parts = _chain_path_parts(implemented_path)
            divergence = _path_divergence(
                documented_parts=documented_parts,
                implemented_parts=implemented_parts,
            )
            score = _full_path_match_score(
                documented_path=documented_path,
                implemented_path=implemented_path,
                divergence=divergence,
            )
            candidates.append(
                {
                    "documented_path": documented_path,
                    "implemented_path": implemented_path,
                    "score": score,
                    "divergence_index": divergence["index"],
                    "divergence_documented": divergence["documented_part"],
                    "divergence_implemented": divergence["implemented_part"],
                    "common_prefix": divergence["common_prefix"],
                    "common_suffix": divergence["common_suffix"],
                    "divergence_mode": divergence["divergence_mode"],
                    "documented_gap_segments": divergence["documented_gap_segments"],
                    "implemented_gap_segments": divergence["implemented_gap_segments"],
                    "documented_gap_segment_path": divergence["documented_gap_segment_path"],
                    "implemented_gap_segment_path": divergence["implemented_gap_segment_path"],
                    "rejoin_at": divergence["rejoin_at"],
                    "documented_family": _evidence_chain_family(documented_path),
                    "implemented_family": _evidence_chain_family(implemented_path),
                }
            )

    candidates.sort(
        key=lambda item: (
            item["score"],
            -len(str(item["documented_path"])),
            -len(str(item["implemented_path"])),
        ),
        reverse=True,
    )

    used_documented: set[str] = set()
    used_implemented: set[str] = set()
    matched_pairs: list[dict[str, object]] = []
    for candidate in candidates:
        documented_path = str(candidate["documented_path"])
        implemented_path = str(candidate["implemented_path"])
        if documented_path in used_documented or implemented_path in used_implemented:
            continue
        used_documented.add(documented_path)
        used_implemented.add(implemented_path)
        matched_pairs.append(candidate)

    unmatched_documented_paths = [path for path in documented_paths if path not in used_documented]
    unmatched_implemented_paths = [path for path in implemented_paths if path not in used_implemented]
    return {
        "matched_pairs": matched_pairs,
        "unmatched_documented_paths": unmatched_documented_paths,
        "unmatched_implemented_paths": unmatched_implemented_paths,
    }


def _chain_path_parts(path: str) -> list[str]:
    return [part.strip() for part in str(path or "").split("->") if part.strip()]


def _path_divergence(
    *,
    documented_parts: Sequence[str],
    implemented_parts: Sequence[str],
) -> dict[str, object]:
    common_prefix: list[str] = []
    for documented_part, implemented_part in zip(documented_parts, implemented_parts):
        if documented_part != implemented_part:
            break
        common_prefix.append(documented_part)
    divergence_index = len(common_prefix)
    documented_part = documented_parts[divergence_index] if divergence_index < len(documented_parts) else None
    implemented_part = implemented_parts[divergence_index] if divergence_index < len(implemented_parts) else None
    common_suffix: list[str] = []
    for documented_part_suffix, implemented_part_suffix in zip(reversed(documented_parts), reversed(implemented_parts)):
        if documented_part_suffix != implemented_part_suffix:
            break
        common_suffix.insert(0, documented_part_suffix)
    documented_gap_segments = list(
        documented_parts[divergence_index: len(documented_parts) - len(common_suffix)]
        if len(common_suffix) <= len(documented_parts)
        else []
    )
    implemented_gap_segments = list(
        implemented_parts[divergence_index: len(implemented_parts) - len(common_suffix)]
        if len(common_suffix) <= len(implemented_parts)
        else []
    )
    return {
        "index": divergence_index,
        "documented_part": documented_part,
        "implemented_part": implemented_part,
        "common_prefix": common_prefix,
        "common_suffix": common_suffix,
        "divergence_mode": _divergence_mode(
            common_prefix=common_prefix,
            common_suffix=common_suffix,
            documented_gap_segments=documented_gap_segments,
            implemented_gap_segments=implemented_gap_segments,
        ),
        "documented_gap_segments": documented_gap_segments,
        "implemented_gap_segments": implemented_gap_segments,
        "documented_gap_segment_path": " -> ".join(documented_gap_segments),
        "implemented_gap_segment_path": " -> ".join(implemented_gap_segments),
        "rejoin_at": common_suffix[0] if common_suffix else None,
    }


def _divergence_mode(
    *,
    common_prefix: Sequence[str],
    common_suffix: Sequence[str],
    documented_gap_segments: Sequence[str],
    implemented_gap_segments: Sequence[str],
) -> str:
    documented_gap = bool(documented_gap_segments)
    implemented_gap = bool(implemented_gap_segments)
    if documented_gap and not implemented_gap:
        if common_prefix and common_suffix:
            return "implemented_internal_gap"
        if common_prefix:
            return "implemented_tail_gap"
        if common_suffix:
            return "implemented_prefix_gap"
        return "implemented_full_gap"
    if implemented_gap and not documented_gap:
        if common_prefix and common_suffix:
            return "documented_internal_gap"
        if common_prefix:
            return "documented_tail_gap"
        if common_suffix:
            return "documented_prefix_gap"
        return "documented_full_gap"
    if documented_gap and implemented_gap:
        if common_prefix and common_suffix:
            return "internal_substitution"
        if common_prefix:
            return "tail_substitution"
        if common_suffix:
            return "prefix_substitution"
        return "full_substitution"
    return "aligned"


def _full_path_match_score(
    *,
    documented_path: str,
    implemented_path: str,
    divergence: dict[str, object],
) -> tuple[int, int, int]:
    same_family = int(
        bool(_evidence_chain_family(documented_path))
        and _evidence_chain_family(documented_path) == _evidence_chain_family(implemented_path)
    )
    prefix_len = len(divergence["common_prefix"])
    suffix_len = len(divergence["common_suffix"])
    return (same_family, prefix_len, suffix_len)


def _detect_architecture_observations(*, domain_claims: Sequence[ExtractedClaimRecord]) -> list[AuditFinding]:
    findings: list[AuditFinding] = []
    grouped: dict[str, list[ExtractedClaimRecord]] = defaultdict(list)
    for record in domain_claims:
        if record.claim.predicate not in _ARCHITECTURE_OBSERVATION_CONFIG:
            continue
        grouped[record.claim.subject_key].append(record)

    for subject_key, records in grouped.items():
        if len({record.claim.source_id for record in records}) < 2:
            continue
        primary_record = records[0]
        config = _ARCHITECTURE_OBSERVATION_CONFIG[primary_record.claim.predicate]
        evidence_parts = []
        locations: list[AuditLocation] = []
        for record in records[:3]:
            evidence_parts.append(
                f"{_source_label(record)}: «{str(record.evidence.matched_text or record.claim.normalized_value)[:160]}»"
            )
            if record.evidence.location:
                locations.append(record.evidence.location)
        findings.append(
            AuditFinding(
                severity=config["severity"],  # type: ignore[arg-type]
                category=config["category"],  # type: ignore[arg-type]
                title="Statement-zu-BSM-Element-Hop ist im aktiven Pfad bestaetigt",
                summary=(
                    f"{subject_key} ist ueber mehrere Implementierungsquellen konsistent bestaetigt.\n\n"
                    + "\n".join(evidence_parts)
                ),
                recommendation=config["recommendation"],
                canonical_key=f"bsm_architecture_observation|{subject_key}",
                locations=locations,
                metadata={
                    "generated_by": "bsm_domain_contradiction_detector",
                    "observation_kind": "confirmed_architecture_path",
                    "subject_key": subject_key,
                    "source_count": len({record.claim.source_id for record in records}),
                },
            )
        )

    return findings


def _record_weight(*, record: ExtractedClaimRecord) -> float:
    authority = str(record.claim.source_authority or "").strip()
    status = str(record.claim.assertion_status or "asserted").strip()
    weight = {
        "explicit_truth": 5.0,
        "confirmed_decision": 4.0,
        "ssot": 3.5,
        "governed": 2.8,
        "working_doc": 2.0,
        "implementation": 1.2,
        "runtime_observation": 0.9,
        "historical": 0.5,
        "heuristic": 0.4,
    }.get(authority, 1.0)
    if status in {"deprecated", "secondary_only", "not_ssot"}:
        weight *= 0.7
    return weight


def _proposed_jira_action(*, subject_key: str, records: Sequence[ExtractedClaimRecord], target_value: str) -> str | None:
    external_sources = []
    for record in records:
        path_hint = str(record.evidence.location.path_hint or record.claim.source_id or "")
        if path_hint.endswith((".puml", ".plantuml")) or record.claim.source_type == "metamodel":
            external_sources.append(path_hint or record.claim.source_id)
    if not external_sources:
        return None
    unique_sources = ", ".join(sorted({source for source in external_sources if source})[:3])
    return (
        f"Jira-Ticket erstellen: {unique_sources} auf den Zielwert '{target_value}' fuer '{subject_key}' angleichen "
        "und alle widerspruechlichen Rollen-/Statusdefinitionen konsolidieren."
    )
