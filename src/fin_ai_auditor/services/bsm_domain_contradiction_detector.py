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
    "defined_status", "puml_defined_status",
    "hitl_decision", "puml_hitl_decision",
    "run_hierarchy", "puml_run_hierarchy",
    "evidence_chain_type", "puml_evidence_chain",
    "phase_scope_type",
    "to_modify_inclusion", "puml_to_modify_usage",
    "initial_status",
    "relationship_lifecycle_type", "puml_relationship_lifecycle",
    "relationship_change_approach",
}

# Role predicates where text differences indicate contradictions
_ROLE_PREDICATES: set[str] = {
    "entity_role_assertion",
    "puml_entity_role",
    "metamodel_entity_exists",
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
        # Normalize predicate to concept
        concept = predicate.replace("puml_", "").replace("documented_", "")
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
