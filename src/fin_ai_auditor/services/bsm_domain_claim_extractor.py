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

import logging
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
    r"\s+(?:soll|muss|ist|wird|darf|kann|entf[aä]llt|entfaellt|bleibt|startet|beginnt)"
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

_NEGATION_PATTERN = re.compile(
    r"(?:\bno\b|\bkein(?:e|er|em|en)?\b|\bwithout\b|\bnicht\b|\bexcluded\b|\bausgeschlossen\b|\bnot\b)",
    re.IGNORECASE,
)
_DEPRECATED_PATTERN = re.compile(
    r"(?:\bdeprecated\b|\blegacy\b|\bhistoric\b|\bveraltet\b|\bentf[aä]llt\b|\bentfaellt\b|\bremoved\b)",
    re.IGNORECASE,
)
_NOT_SSOT_PATTERN = re.compile(
    r"(?:\bnot\s+ssot\b|\bkein\s+ssot\b|\bnicht\s+ssot\b|\bsekund[aä]r\b|\bsecondary\b)",
    re.IGNORECASE,
)
_TABLE_ROW_PATTERN = re.compile(r"^\|(.+)\|$")


logger = logging.getLogger(__name__)


def extract_bsm_domain_claims(
    *, documents: Sequence[CollectedDocument]
) -> list[ExtractedClaimRecord]:
    """Extract BSM-domain-specific claims from all documents."""
    records: list[ExtractedClaimRecord] = []
    doc_count = len(documents)
    logger.info("bsm_domain_extraction_start", extra={"event_name": "bsm_domain_extraction_start", "event_payload": {"document_count": doc_count}})
    for doc in documents:
        before = len(records)
        try:
            if doc.source_type in {"confluence_page", "local_doc"}:
                records.extend(_extract_from_documentation(document=doc))
            elif doc.source_type == "metamodel":
                records.extend(_extract_from_metamodel_json(document=doc))
            elif doc.source_type == "github_file":
                path = (doc.path_hint or doc.source_id or "").lower()
                if path.endswith(".puml") or path.endswith(".plantuml"):
                    records.extend(_extract_from_puml(document=doc))
                elif "metamodel" in path and path.endswith(".json"):
                    records.extend(_extract_from_metamodel_json(document=doc))
        except Exception as exc:
            logger.warning("bsm_domain_doc_failed", extra={"event_name": "bsm_domain_doc_failed", "event_payload": {"source_id": doc.source_id, "error": str(exc)}})
        added = len(records) - before
        if added > 0:
            logger.debug("bsm_domain_doc_claims", extra={"event_name": "bsm_domain_doc_claims", "event_payload": {"source_id": doc.source_id, "claims": added}})
    logger.info("bsm_domain_extraction_done", extra={"event_name": "bsm_domain_extraction_done", "event_payload": {"total_claims": len(records), "documents_processed": doc_count}})
    return records


def _extract_from_documentation(
    *, document: CollectedDocument
) -> list[ExtractedClaimRecord]:
    """Extract domain claims from Confluence/local documentation."""
    records: list[ExtractedClaimRecord] = []
    lines = document.body.splitlines()
    heading_stack: list[str] = []
    table_headers: list[str] = []
    doc_context = document.title or document.source_id or "doc"

    for line_no, raw_line in enumerate(lines, start=1):
        stripped = raw_line.strip()
        if not stripped:
            table_headers = []
            continue

        # Track headings for context
        heading_match = re.match(r"^(#{1,6})\s+(.+)", raw_line)
        if heading_match:
            level = len(heading_match.group(1))
            title = heading_match.group(2).strip()
            heading_stack = heading_stack[:level - 1] + [title]
            table_headers = []
            continue

        section = " > ".join([doc_context] + heading_stack)
        table_cells = _parse_table_row(stripped)
        if table_cells is not None:
            if _looks_like_table_header(table_cells):
                table_headers = table_cells
                continue
            records.extend(
                _extract_from_table_row(
                    document=document,
                    line_no=line_no,
                    section=section,
                    headers=table_headers,
                    cells=table_cells,
                    raw_line=stripped,
                )
            )

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
            is_exclusion = bool(_NEGATION_PATTERN.search(stripped))
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
            is_secondary = bool(
                re.search(r"(?:run_id)", stripped, re.IGNORECASE)
                and _NOT_SSOT_PATTERN.search(stripped)
            )
            if is_hierarchical or is_flat or is_secondary:
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="Run.model",
                    predicate="run_hierarchy",
                    normalized_value="run_id_secondary_only" if is_secondary else ("hierarchical_3tier" if is_hierarchical else "run_id_centric"),
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
            is_excluded = bool(
                re.search(r"(?:nicht.*Teil|nicht.*Zielbild|ausgeschlossen|excluded|not.*part)", stripped, re.IGNORECASE)
                or _NEGATION_PATTERN.search(stripped)
            )
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
            is_exclusion = bool(_NEGATION_PATTERN.search(stripped))
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
            is_secondary = bool(re.search(r"(?:run_id)", stripped)) and bool(_NOT_SSOT_PATTERN.search(stripped))
            is_flat = bool(re.search(r"(?:run_id)", stripped)) and not is_hierarchical and not is_secondary
            if is_hierarchical or is_flat or is_secondary:
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="Run.model",
                    predicate="puml_run_hierarchy",
                    normalized_value="run_id_secondary_only" if is_secondary else ("hierarchical_3tier" if is_hierarchical else "run_id_centric"),
                    section_path=section,
                ))

        # TO_MODIFY in PUML
        if re.search(r"\bTO_MODIFY\b|:TO_MODIFY\b|to_modify", stripped):
            records.append(_make_claim(
                document=document, line_no=line_no, line_text=stripped,
                subject_key="TO_MODIFY.role",
                predicate="puml_to_modify_inclusion",
                normalized_value="excluded" if _NEGATION_PATTERN.search(stripped) else "included",
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
                    predicate="puml_evidence_chain_type",
                    normalized_value="unit_centric" if is_unit_centric else "summary_centric",
                    section_path=section,
                ))

        # Phase/scope distinction in PUML
        if _PHASE_SCOPE_PATTERN.search(stripped):
            is_separated = bool(
                re.search(r"(?:ui_phase_id.*UI.only|getrennt|separate|distinct)", stripped, re.IGNORECASE)
            )
            is_mixed = bool(
                re.search(r"(?:UI.*kennt.*Fachphasen|vermisch|mixed|ingestion.*genai_ba)", stripped, re.IGNORECASE)
            )
            if is_separated or is_mixed:
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="Phase.scope_distinction",
                    predicate="puml_phase_scope_type",
                    normalized_value="separated" if is_separated else "mixed",
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
                predicate="puml_relationship_lifecycle_type",
                normalized_value=lifecycle_type,
                section_path=section,
            ))

        # Initial state in PUML
        start_match = re.search(
            r"\b(?P<entity>" + "|".join(re.escape(e) for e in BSM_ENTITIES) + r")\b"
            r".*?(?:startet|beginnt|start|initial)\s+(?:als|as|mit|with|=)?\s*"
            r"(?P<status>STAGED|PROPOSED|VALIDATED|ACTIVE)",
            stripped,
            re.IGNORECASE,
        )
        if start_match:
            records.append(_make_claim(
                document=document, line_no=line_no, line_text=stripped,
                subject_key=f"{start_match.group('entity')}.initial_state",
                predicate="puml_initial_status",
                normalized_value=start_match.group("status").upper(),
                section_path=section,
            ))

        # Relationship change model in PUML
        if re.search(r"\bRelationship\b", stripped, re.IGNORECASE):
            if re.search(r"(?:version|versionier|immutable|snapshot)", stripped, re.IGNORECASE):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="Relationship.change_model",
                    predicate="puml_relationship_change_approach",
                    normalized_value="full_versioning",
                    section_path=section,
                ))
            if re.search(r"(?:edge.state|state.*wechsel|direkt.*relationship|to_modify)", stripped, re.IGNORECASE):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="Relationship.change_model",
                    predicate="puml_relationship_change_approach",
                    normalized_value="edge_state_only",
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
    assertion_status = _assertion_status(line_text)
    source_authority = _source_authority(document=document)
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
        operator=_claim_operator(predicate=predicate, assertion_status=assertion_status, normalized_value=normalized_value),
        constraint=normalized_value,
        focus_value=normalized_value,
        assertion_status=assertion_status,
        source_authority=source_authority,
        metadata={
            "assertion_status": assertion_status,
            "source_authority": source_authority,
            "claim_operator": _claim_operator(predicate=predicate, assertion_status=assertion_status, normalized_value=normalized_value),
            "claim_constraint": normalized_value,
            "claim_focus_value": normalized_value,
            "source_governance_level": source_authority,
            "source_temporal_status": "historical" if source_authority == "historical" else "current",
            "title": document.title,
            "path_hint": document.path_hint,
        },
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


def _parse_table_row(line: str) -> list[str] | None:
    match = _TABLE_ROW_PATTERN.match(line)
    if match is None:
        return None
    cells = [cell.strip() for cell in match.group(1).split("|")]
    return [cell for cell in cells if cell]


def _looks_like_table_header(cells: list[str]) -> bool:
    lowered = [cell.casefold() for cell in cells]
    return any(token in cell for cell in lowered for token in ("status", "state", "start", "initial", "entity", "artefakt", "object"))


def _extract_from_table_row(
    *,
    document: CollectedDocument,
    line_no: int,
    section: str,
    headers: list[str],
    cells: list[str],
    raw_line: str,
) -> list[ExtractedClaimRecord]:
    records: list[ExtractedClaimRecord] = []
    row_text = " | ".join(cells)
    entities = _ENTITY_PATTERN.findall(row_text)
    if not entities:
        return records
    header_map = {
        headers[index].casefold(): cells[index]
        for index in range(min(len(headers), len(cells)))
    }
    for entity in set(entities):
        initial_value = next(
            (
                value
                for header, value in header_map.items()
                if any(token in header for token in ("initial", "start"))
                and _STATUS_PATTERN.search(value)
            ),
            "",
        )
        if initial_value:
            status_match = _STATUS_PATTERN.search(initial_value)
            if status_match is not None:
                records.append(
                    _make_claim(
                        document=document,
                        line_no=line_no,
                        line_text=raw_line,
                        subject_key=f"{entity}.initial_state",
                        predicate="initial_status",
                        normalized_value=status_match.group(1).upper(),
                        section_path=section,
                    )
                )
        status_values = {
            status.upper()
            for status in _STATUS_PATTERN.findall(row_text)
        }
        for status in status_values:
            records.append(
                _make_claim(
                    document=document,
                    line_no=line_no,
                    line_text=raw_line,
                    subject_key=f"{entity}.status_canon",
                    predicate="defined_status",
                    normalized_value=status,
                    section_path=section,
                )
            )
    return records


def _assertion_status(line_text: str) -> str:
    if _DEPRECATED_PATTERN.search(line_text):
        return "deprecated"
    if _NOT_SSOT_PATTERN.search(line_text):
        return "not_ssot"
    if _NEGATION_PATTERN.search(line_text):
        return "excluded"
    return "asserted"


def _source_authority(*, document: CollectedDocument) -> str:
    descriptor = " ".join([str(document.title or ""), str(document.path_hint or ""), str(document.source_id or "")]).casefold()
    if document.source_type == "metamodel":
        return "ssot"
    if any(token in descriptor for token in ("ssot", "target", "reference", "scope-matrix", "run-ssot", "contract")):
        return "ssot"
    if any(token in descriptor for token in ("architecture", "policy", "process", "guardrail")):
        return "governed"
    if any(token in descriptor for token in ("as_is", "legacy", "deprecated", "archive", "historic", "historical")):
        return "historical"
    if document.source_type == "github_file":
        return "implementation"
    return "working_doc"


def _claim_operator(*, predicate: str, assertion_status: str, normalized_value: str) -> str:
    lowered = predicate.casefold()
    if assertion_status in {"excluded", "not_ssot"}:
        return "forbids"
    if "status" in lowered or "lifecycle" in lowered:
        return "defines"
    if "hierarchy" in lowered or "chain" in lowered or "role" in lowered:
        return "describes"
    return "states"
