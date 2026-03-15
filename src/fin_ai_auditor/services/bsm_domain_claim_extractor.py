"""BSM-domain-specific claim extraction for high-quality contradiction detection.

This module extracts structured claims about BSM domain concepts:
 - Entity roles (summarisedAnswer, Statement, BSM_Element, Relationship)
 - Status/lifecycle values (STAGED, PROPOSED, VERIFIED, etc.)
 - HITL decisions (review included/excluded)
 - Run model hierarchy (3-tier vs run_id-centric)
 - Phase/scope distinctions (UI vs business phases)
 - Evidence chain direction (unit-centric vs summary-centric)

These claims enable the finding engine to detect the specific contradictions
documented in the project benchmark.
"""
from __future__ import annotations

import re
from typing import Final, Sequence

from fin_ai_auditor.domain.models import AuditClaimEntry, AuditLocation, AuditPosition
from fin_ai_auditor.services.pipeline_models import CollectedDocument, ExtractedClaimEvidence, ExtractedClaimRecord

# ── Domain entity names we track ────────────────────────────────────

BSM_ENTITIES: Final[tuple[str, ...]] = (
    "summarisedAnswer", "summarisedAnswerUnit", "Statement", "BSM_Element",
    "bsmAnswer", "Relationship", "analysisRun", "FINAI_AnalysisRun",
    "FINAI_PhaseRun", "FINAI_ChunkPhaseRun",
)

# ── Status values we track ──────────────────────────────────────────

STATUS_VALUES: Final[tuple[str, ...]] = (
    "STAGED", "PROPOSED", "VALIDATED", "VERIFIED", "REJECTED", "ACTIVE",
    "HISTORIC", "MODIFIED", "REFINED", "IN_RUN", "TO_MODIFY",
    "HISTORIC:MODIFIED",
)

# ── Compiled patterns ───────────────────────────────────────────────

# Matches entity names as whole words (case-sensitive for PascalCase entities)
_ENTITY_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(e) for e in BSM_ENTITIES) + r")\b"
)

# Matches status values as whole words (case-insensitive)
_STATUS_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(s) for s in STATUS_VALUES) + r")\b",
    re.IGNORECASE,
)

# Matches role assertions: "X soll/muss/ist/wird... <assertion>"
_ROLE_ASSERTION = re.compile(
    r"\b(?P<entity>" + "|".join(re.escape(e) for e in BSM_ENTITIES) + r")\b"
    r"\s+(?:soll|muss|ist|wird|darf|kann|entf[aä]llt|bleibt|startet|beginnt)"
    r"\s+(?P<assertion>[^.;]{5,120})",
    re.IGNORECASE,
)

# Matches "No HITL" / "kein HITL" / "without HITL" / "zentral reviewbar"
_HITL_PATTERN = re.compile(
    r"(?:no\s+hitl|kein\s+hitl|without\s+hitl|no\s+(?:human|manual)\s+review|"
    r"hitl\s+(?:decision|entscheidung)|review(?:bar|able)|"
    r"accept[/ ]reject|human.in.the.loop|"
    r"(?:Statement|BSM_Element|Relationship)[\s-]*(?:review|HITL|validation))",
    re.IGNORECASE,
)

# Matches run hierarchy concepts
_RUN_HIERARCHY_PATTERN = re.compile(
    r"(?:FINAI_AnalysisRun|FINAI_PhaseRun|FINAI_ChunkPhaseRun|"
    r"analysisRun|run_id|IN_RUN|PhaseRun|ChunkPhaseRun|"
    r"drei(?:stufig|teilig)|three.tier|run.hierarchy|"
    r"run[\s_-]?modell|run[\s_-]?model)",
    re.IGNORECASE,
)

# Matches evidence chain concepts
_EVIDENCE_CHAIN_PATTERN = re.compile(
    r"(?:traceability|evidenz|evidence|chain|kette|bucket|"
    r"bsmAnswer\s*->\s*\w+|"
    r"summarisedAnswer(?:Unit)?.*?Statement|"
    r"Statement.*?BSM_Element)",
    re.IGNORECASE,
)

# Matches phase/scope distinctions
_PHASE_SCOPE_PATTERN = re.compile(
    r"(?:ui_phase_id|phase_id|UI.only|fachlich|business.phase|"
    r"ingestion|genai_ba|ui.kennt|fachphase)",
    re.IGNORECASE,
)

# PlantUML specific patterns
_PUML_STATE_PATTERN = re.compile(
    r"(?:state\s+|:)(?P<entity>\w+)\s*(?::|{|\[)"
    r"|(?P<status>STAGED|PROPOSED|VALIDATED|VERIFIED|REJECTED|ACTIVE|TO_MODIFY|IN_RUN)"
    r"|(?P<transition>\w+)\s*-+>?\s*(?:\[.*?\])?\s*(?P<target>\w+)",
    re.IGNORECASE,
)


def extract_bsm_domain_claims(
    *, documents: Sequence[CollectedDocument]
) -> list[ExtractedClaimRecord]:
    """Extract BSM-domain-specific claims from all documents."""
    records: list[ExtractedClaimRecord] = []
    for doc in documents:
        if doc.source_type in {"confluence_page", "local_doc"}:
            records.extend(_extract_from_documentation(document=doc))
        elif doc.source_type == "github_file":
            path = (doc.path_hint or doc.source_id or "").lower()
            if path.endswith(".puml") or path.endswith(".plantuml"):
                records.extend(_extract_from_puml(document=doc))
            elif "metamodel" in path and path.endswith(".json"):
                records.extend(_extract_from_metamodel_json(document=doc))
    return records


def _extract_from_documentation(
    *, document: CollectedDocument
) -> list[ExtractedClaimRecord]:
    """Extract domain claims from Confluence/local documentation."""
    records: list[ExtractedClaimRecord] = []
    lines = document.body.splitlines()
    heading_stack: list[str] = []
    doc_context = document.title or document.source_id or "doc"

    for line_no, raw_line in enumerate(lines, start=1):
        stripped = raw_line.strip()
        if not stripped:
            continue

        # Track headings for context
        heading_match = re.match(r"^(#{1,6})\s+(.+)", raw_line)
        if heading_match:
            level = len(heading_match.group(1))
            title = heading_match.group(2).strip()
            heading_stack = heading_stack[:level - 1] + [title]
            continue

        section = " > ".join([doc_context] + heading_stack)

        # 1. Entity role assertions
        for m in _ROLE_ASSERTION.finditer(stripped):
            entity = m.group("entity")
            assertion = m.group("assertion").strip()
            records.append(_make_claim(
                document=document, line_no=line_no, line_text=stripped,
                subject_key=f"{entity}.role",
                predicate="entity_role_assertion",
                normalized_value=assertion[:200],
                section_path=section,
            ))

        # 2. Status value claims — which statuses apply to which entity
        entities_in_line = _ENTITY_PATTERN.findall(stripped)
        statuses_in_line = _STATUS_PATTERN.findall(stripped)
        if entities_in_line and statuses_in_line:
            for entity in set(entities_in_line):
                for status in set(statuses_in_line):
                    records.append(_make_claim(
                        document=document, line_no=line_no, line_text=stripped,
                        subject_key=f"{entity}.status_canon",
                        predicate="defined_status",
                        normalized_value=status.upper(),
                        section_path=section,
                    ))

        # 3. HITL claims
        hitl_match = _HITL_PATTERN.search(stripped)
        if hitl_match:
            is_exclusion = bool(re.search(r"(?:no|kein|without|nicht|excluded|ausgeschlossen)", stripped, re.IGNORECASE))
            entities = _ENTITY_PATTERN.findall(stripped) or ["general"]
            for entity in set(entities):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key=f"{entity}.hitl",
                    predicate="hitl_decision",
                    normalized_value="excluded" if is_exclusion else "included",
                    section_path=section,
                ))

        # 4. Run hierarchy claims
        if _RUN_HIERARCHY_PATTERN.search(stripped):
            is_hierarchical = bool(re.search(
                r"(?:FINAI_AnalysisRun.*PhaseRun|drei(?:stufig|teilig)|three.tier|hierarchy|hierarchie|IN_RUN.*fuehrend)",
                stripped, re.IGNORECASE,
            ))
            is_flat = bool(re.search(
                r"(?:run_id.*zentriert|run_id.*SSOT|run_id.*fuehrend|ohne.*PhaseRun|vereinfacht)",
                stripped, re.IGNORECASE,
            ))
            if is_hierarchical or is_flat:
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="Run.model",
                    predicate="run_hierarchy",
                    normalized_value="hierarchical_3tier" if is_hierarchical else "run_id_centric",
                    section_path=section,
                ))

        # 5. Evidence chain claims
        if _EVIDENCE_CHAIN_PATTERN.search(stripped):
            is_unit_centric = bool(re.search(r"summarisedAnswerUnit", stripped))
            is_summary_centric = bool(re.search(r"summarisedAnswer(?!Unit)", stripped))
            if is_unit_centric or is_summary_centric:
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="EvidenceChain.direction",
                    predicate="evidence_chain_type",
                    normalized_value="unit_centric" if is_unit_centric else "summary_centric",
                    section_path=section,
                ))

        # 6. Phase/scope distinction claims
        if _PHASE_SCOPE_PATTERN.search(stripped):
            is_separated = bool(re.search(r"(?:ui_phase_id.*UI.only|getrennt|separate|distinct)", stripped, re.IGNORECASE))
            is_mixed = bool(re.search(r"(?:UI.*kennt.*Fachphasen|vermisch|mixed|ingestion.*genai_ba)", stripped, re.IGNORECASE))
            if is_separated or is_mixed:
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="Phase.scope_distinction",
                    predicate="phase_scope_type",
                    normalized_value="separated" if is_separated else "mixed",
                    section_path=section,
                ))

        # 7. TO_MODIFY role claims
        if re.search(r"\bTO_MODIFY\b", stripped):
            is_excluded = bool(re.search(r"(?:nicht.*Teil|nicht.*Zielbild|ausgeschlossen|excluded|not.*part)", stripped, re.IGNORECASE))
            is_included = bool(re.search(r"(?:Workflow|offiziell|Label|state|status|:TO_MODIFY)", stripped))
            if is_excluded or is_included:
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="TO_MODIFY.role",
                    predicate="to_modify_inclusion",
                    normalized_value="excluded" if is_excluded else "included",
                    section_path=section,
                ))

        # 8. Initial state claims
        start_match = re.search(
            r"\b(?P<entity>" + "|".join(re.escape(e) for e in BSM_ENTITIES) + r")\b"
            r".*?(?:startet|beginnt|start|initial)\s+(?:als|as|mit|with)?\s*"
            r"(?P<status>STAGED|PROPOSED|VALIDATED|ACTIVE)",
            stripped, re.IGNORECASE,
        )
        if start_match:
            records.append(_make_claim(
                document=document, line_no=line_no, line_text=stripped,
                subject_key=f"{start_match.group('entity')}.initial_state",
                predicate="initial_status",
                normalized_value=start_match.group("status").upper(),
                section_path=section,
            ))

        # 9. Relationship lifecycle claims
        if re.search(r"\bRelationship\b", stripped) and statuses_in_line:
            lifecycle_type = "staged_based" if "STAGED" in [s.upper() for s in statuses_in_line] else "proposed_based"
            records.append(_make_claim(
                document=document, line_no=line_no, line_text=stripped,
                subject_key="Relationship.lifecycle",
                predicate="relationship_lifecycle_type",
                normalized_value=lifecycle_type,
                section_path=section,
            ))

        # 10. Versioning vs edge-state claims for relationships
        if re.search(r"\bRelationship\b", stripped, re.IGNORECASE):
            if re.search(r"(?:version|versionier|immutable|snapshot)", stripped, re.IGNORECASE):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="Relationship.change_model",
                    predicate="relationship_change_approach",
                    normalized_value="full_versioning",
                    section_path=section,
                ))
            if re.search(r"(?:edge.state|state.*wechsel|direkt.*relationship|to_modify)", stripped, re.IGNORECASE):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="Relationship.change_model",
                    predicate="relationship_change_approach",
                    normalized_value="edge_state_only",
                    section_path=section,
                ))

    return records


def _extract_from_puml(*, document: CollectedDocument) -> list[ExtractedClaimRecord]:
    """Extract domain claims from PlantUML pipeline diagrams."""
    records: list[ExtractedClaimRecord] = []
    lines = document.body.splitlines()
    path = document.path_hint or document.source_id or "puml"
    doc_context = path.split("/")[-1] if "/" in path else path

    for line_no, raw_line in enumerate(lines, start=1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("'") or stripped.startswith("@"):
            continue

        section = f"{doc_context}:{line_no}"

        # Entity + status mentions in PUML
        entities_in_line = _ENTITY_PATTERN.findall(stripped)
        statuses_in_line = _STATUS_PATTERN.findall(stripped)

        if entities_in_line and statuses_in_line:
            for entity in set(entities_in_line):
                for status in set(statuses_in_line):
                    records.append(_make_claim(
                        document=document, line_no=line_no, line_text=stripped,
                        subject_key=f"{entity}.status_canon",
                        predicate="puml_defined_status",
                        normalized_value=status.upper(),
                        section_path=section,
                    ))

        # HITL in PUML
        hitl_match = _HITL_PATTERN.search(stripped)
        if hitl_match:
            is_exclusion = bool(re.search(r"(?:no|kein|without|nicht)", stripped, re.IGNORECASE))
            entities = _ENTITY_PATTERN.findall(stripped) or ["general"]
            for entity in set(entities):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key=f"{entity}.hitl",
                    predicate="puml_hitl_decision",
                    normalized_value="excluded" if is_exclusion else "included",
                    section_path=section,
                ))

        # IN_RUN / run_id patterns
        if _RUN_HIERARCHY_PATTERN.search(stripped):
            is_hierarchical = bool(re.search(r"(?:PhaseRun|ChunkPhaseRun|IN_RUN)", stripped))
            is_flat = bool(re.search(r"(?:run_id)", stripped)) and not is_hierarchical
            if is_hierarchical or is_flat:
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="Run.model",
                    predicate="puml_run_hierarchy",
                    normalized_value="hierarchical_3tier" if is_hierarchical else "run_id_centric",
                    section_path=section,
                ))

        # TO_MODIFY in PUML
        if re.search(r"\bTO_MODIFY\b|:TO_MODIFY\b|to_modify", stripped):
            records.append(_make_claim(
                document=document, line_no=line_no, line_text=stripped,
                subject_key="TO_MODIFY.role",
                predicate="puml_to_modify_usage",
                normalized_value="included",
                section_path=section,
            ))

        # Evidence chain in PUML
        if _EVIDENCE_CHAIN_PATTERN.search(stripped):
            is_unit_centric = bool(re.search(r"summarisedAnswerUnit", stripped))
            is_summary_centric = bool(re.search(r"summarisedAnswer(?!Unit)", stripped))
            if is_unit_centric or is_summary_centric:
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="EvidenceChain.direction",
                    predicate="puml_evidence_chain",
                    normalized_value="unit_centric" if is_unit_centric else "summary_centric",
                    section_path=section,
                ))

        # Entity role in PUML (e.g., summarisedAnswer as Traceability-Node)
        for entity in entities_in_line:
            if re.search(rf"\b{re.escape(entity)}\b.*(?:node|bucket|root|element|artefakt)", stripped, re.IGNORECASE):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key=f"{entity}.role",
                    predicate="puml_entity_role",
                    normalized_value=stripped[:200],
                    section_path=section,
                ))

        # Relationship lifecycle in PUML
        if re.search(r"\bRelationship\b", stripped) and statuses_in_line:
            lifecycle_type = "staged_based" if "STAGED" in [s.upper() for s in statuses_in_line] else "proposed_based"
            records.append(_make_claim(
                document=document, line_no=line_no, line_text=stripped,
                subject_key="Relationship.lifecycle",
                predicate="puml_relationship_lifecycle",
                normalized_value=lifecycle_type,
                section_path=section,
            ))

    return records


def _extract_from_metamodel_json(*, document: CollectedDocument) -> list[ExtractedClaimRecord]:
    """Extract entity existence claims from metamodel JSON exports."""
    import json
    records: list[ExtractedClaimRecord] = []
    try:
        data = json.loads(document.body)
    except (ValueError, TypeError):
        return records

    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return records

    # Search for BSM entities defined as metaclasses
    body_text = document.body
    for entity in BSM_ENTITIES:
        # Case-sensitive search for the entity name in the JSON
        occurrences = [m.start() for m in re.finditer(re.escape(entity), body_text)]
        if occurrences:
            line_no = body_text[:occurrences[0]].count("\n") + 1
            records.append(_make_claim(
                document=document, line_no=line_no,
                line_text=f"{entity} existiert als Metaclass im Metamodell-Export",
                subject_key=f"{entity}.role",
                predicate="metamodel_entity_exists",
                normalized_value=f"{entity} ist als Metaclass definiert",
                section_path=f"metamodel_export:{line_no}",
            ))

    return records


def _make_claim(
    *,
    document: CollectedDocument,
    line_no: int,
    line_text: str,
    subject_key: str,
    predicate: str,
    normalized_value: str,
    section_path: str,
) -> ExtractedClaimRecord:
    """Build a domain-specific claim record."""
    claim = AuditClaimEntry(
        source_type=document.source_type,
        source_id=document.source_id,
        source_snapshot_id=document.snapshot.snapshot_id if document.snapshot else None,
        subject_kind="bsm_domain",
        subject_key=subject_key,
        predicate=predicate,
        normalized_value=normalized_value,
        scope_kind="document",
        scope_key=document.source_id,
        confidence=0.9,
        fingerprint=f"bsm_domain|{subject_key}|{predicate}|{document.source_id}|{line_no}",
        status="active",
    )
    location = AuditLocation(
        source_type=document.source_type,
        source_id=document.source_id,
        snapshot_id=document.snapshot.snapshot_id if document.snapshot else None,
        title=document.title or document.source_id,
        path_hint=document.path_hint,
        url=document.url,
        position=AuditPosition(
            anchor_kind="line",
            anchor_value=str(line_no),
            section_path=section_path,
            line_start=line_no,
            line_end=line_no,
        ),
    )
    evidence = ExtractedClaimEvidence(
        location=location,
        matched_text=line_text[:300],
    )
    return ExtractedClaimRecord(claim=claim, evidence=evidence)
