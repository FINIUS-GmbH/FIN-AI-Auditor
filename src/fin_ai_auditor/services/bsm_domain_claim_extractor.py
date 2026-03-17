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

import ast
from collections import defaultdict
import logging
import re
from typing import Final, Sequence, TypedDict


class _CypherFragment(TypedDict):
    text: str
    start_line: int
    end_line: int


class _CypherRelationship(TypedDict):
    rel_type: str
    start_label: str
    end_label: str

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
_EVIDENCE_CHAIN_ARROW_SPLIT = re.compile(r"\s*(?:->|→)\s*")

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

# ── Cypher-in-Python patterns ───────────────────────────────────────

WORD = r"[A-Za-z_][A-Za-z0-9_]*"
_CYPHER_EDGE_PATTERN = re.compile(
    rf"\(\s*(?P<start_var>{WORD})?\s*(?::\s*(?P<start_label>{WORD}))?[^)]*\)"
    rf"\s*-\s*\[:(?P<rel_type>[A-Z_]+)\]\s*-?>\s*"
    rf"\(\s*(?P<end_var>{WORD})?\s*(?::\s*(?P<end_label>{WORD}))?[^)]*\)",
    re.IGNORECASE | re.DOTALL,
)
# Matches Cypher node labels (n:Label) or (:Label)
_CYPHER_NODE_LABEL_PATTERN = re.compile(
    rf"\(?\s*{WORD}?\s*:\s*(?P<label>{WORD})\s*"
)
_CYPHER_NODE_BINDING_PATTERN = re.compile(
    rf"\(\s*(?P<var>{WORD})\s*:\s*(?P<label>{WORD})",
    re.IGNORECASE,
)
# Detects multi-line Cypher strings in Python — triple-quoted or f-strings
_CYPHER_STRING_START = re.compile(
    r'(?:f?"""|\'\'\'|f?")\s*(?=.*(?:MATCH|MERGE|CREATE|SET|RETURN|WITH|WHERE|OPTIONAL\s+MATCH))',
    re.IGNORECASE,
)
_CYPHER_KEYWORD_PATTERN = re.compile(
    r"\b(?:MATCH|MERGE|CREATE|SET|RETURN|WITH|WHERE|OPTIONAL\s+MATCH|DETACH\s+DELETE|REMOVE|UNWIND|CALL)\b",
    re.IGNORECASE,
)

# ── Temporal / eventual-consistency patterns ────────────────────────

_PERSIST_CALL_PATTERN = re.compile(
    r"(?:\b|_)(?:persist\w*|save\w*|upsert\w*|merge\w*|create_node\w*|create_relationship\w*|"
    r"merge_relationship\w*|execute_write\w*|bulk_write\w*|write_transaction\w*|"
    r"write_cypher_guarded|apply_write_rule|run_write_rule)\s*\(",
    re.IGNORECASE,
)
_ENQUEUE_CALL_PATTERN = re.compile(
    r"(?:\b|_)(?:enqueue\w*|add_job\w*|submit\w*|schedule\w*|dispatch\w*|delay\w*|send_task\w*|"
    r"apply_async|create_job\w*|queue_job\w*|publish\w*)\s*\(",
    re.IGNORECASE,
)
_SUPERSEDE_PATTERN = re.compile(
    r"(?:\b|_)(?:supersede\w*|historize\w*|historise\w*|archive\w*|deactivate\w*|set_historic\w*|mark_historic\w*)\s*\(",
    re.IGNORECASE,
)
_REBUILD_PATTERN = re.compile(
    r"(?:\b|_)(?:rebuild\w*|regenerate\w*|rematerializ\w*|materializ\w*|consolidat\w*|"
    r"create_new\w*|build_new\w*|generate_new\w*)\s*\(",
    re.IGNORECASE,
)

# ── Schema-completeness patterns (LLM output parsing) ──────────────

_SCHEMA_OUTPUT_FIELDS_PATTERN = re.compile(
    r"[\"'](?P<field>text|confidence|rationale|unit_ids?|source_unit|answer_unit_id|unit_reference|unit_mapping)[\"']",
    re.IGNORECASE,
)
_LLM_OUTPUT_CONTEXT_PATTERN = re.compile(
    r"(?:response_schema|output_schema|json_schema|structured_output|function_call|tool_call|response_format|parse_response|extract_from_response)",
    re.IGNORECASE,
)

# ── YAML allowlist patterns ─────────────────────────────────────────

_YAML_NODE_LABEL_PATTERN = re.compile(r"^\s*-?\s*(?:label|node_label|nodeLabel)\s*:\s*(?P<label>[A-Za-z_]\w*)")
_YAML_REL_TYPE_PATTERN = re.compile(r"^\s*-?\s*(?:type|rel_type|relationship_type|relType)\s*:\s*(?P<rel>[A-Z_]+)")
_YAML_SECTION_PATTERN = re.compile(r"^(?P<key>[a-zA-Z_][a-zA-Z0-9_]*)\s*:")
# Match lines that are entries under allowed_writes / write_operations / write_allowlist etc.
_YAML_WRITE_ENTRY_PATTERN = re.compile(
    r"^\s+-\s+(?P<entry>[A-Za-z_][A-Za-z0-9_:>\-]+)",
)

# ── Cross-service data flow / field propagation ─────────────────────

_FIELD_ASSIGNMENT_PATTERN = re.compile(
    r"(?:[\"'](?P<quoted>phase_run_id|run_id|analysis_run_id|chunk_phase_run_id)[\"']\s*[:=]"
    r"|\b(?P<bare>phase_run_id|run_id|analysis_run_id|chunk_phase_run_id)\b\s*=)",
    re.IGNORECASE,
)
_FIELD_MISSING_NAME_CONTEXT = re.compile(
    r"(?<![A-Za-z0-9])(?:refine|manual|legacy|fallback|v1|compat)(?![A-Za-z0-9])",
    re.IGNORECASE,
)
_FIELD_MISSING_LINE_CONTEXT = re.compile(
    r"(?<![A-Za-z0-9])(?:refine|manual(?!_raw)(?:\b|[_:]))",
    re.IGNORECASE,
)
_FIELD_CONTEXT_PROXIMITY_LINES: Final[int] = 24
_FIELD_ENTITY_FORWARD_LINES: Final[int] = 1
_BSM_WRITE_CONTEXT_PATTERN = re.compile(
    r"(?:write_cypher_guarded|write_domain_bridge|persist_statement_artifact|persist_bsm_answer_artifact|"
    r"persist_bsm_element|persist_summarised_answer_unit|update_statement_status|historize_.*statement|"
    r"rebuild_bsm_element|save_ba_answer|capture_ba_answer|refine_statement)",
    re.IGNORECASE,
)
_BSM_PATH_CONTEXT_PATTERN = re.compile(
    r"(?:^|/)(?:router_bsm_|bsm_|.*bsm.*|evidence_validator_service)\w*",
    re.IGNORECASE,
)
_LOCAL_CALL_PATTERN = re.compile(r"\b(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\(")
_YAML_BLOCK_HEADER_PATTERN = re.compile(
    r"^(?P<indent>\s*)(?P<key>[A-Za-z_][A-Za-z0-9_-]*)\s*:\s*(?:\||>)\s*$"
)
_PUML_PARTITION_PATTERN = re.compile(r'^\s*partition\s+"(?P<name>[^"]+)"', re.IGNORECASE)
_PUML_NOTE_START_PATTERN = re.compile(
    r"^\s*note\b(?P<context>[^:]*?)(?::\s*(?P<inline>.*))?$",
    re.IGNORECASE,
)
_PUML_LEGEND_START_PATTERN = re.compile(
    r"^\s*legend\b(?::\s*(?P<inline>.*))?$",
    re.IGNORECASE,
)
_PUML_END_NOTE_PATTERN = re.compile(r"^\s*end\s+note\b", re.IGNORECASE)
_PUML_END_LEGEND_PATTERN = re.compile(r"^\s*end\s+legend\b", re.IGNORECASE)
_STATEMENT_SCHEMA_CONTEXT_PATTERN = re.compile(
    r"(?:statement|statements|raw_statements|unit_ids|rationale|statement generation)",
    re.IGNORECASE,
)
_IGNORED_LOCAL_CALL_NAMES: Final[frozenset[str]] = frozenset(
    {"if", "for", "while", "return", "yield", "case", "match", "with"}
)
_FUNCTION_ENTITY_HINTS: dict[str, str] = {
    "statement": "Statement",
    "bsm_element": "BSM_Element",
    "bsm element": "BSM_Element",
    "summarisedanswerunit": "summarisedAnswerUnit",
    "summarisedanswer": "summarisedAnswer",
    "bsmanswer": "bsmAnswer",
}
_FIELD_PROPAGATION_TARGET_ENTITIES: Final[frozenset[str]] = frozenset(
    {"Statement", "BSM_Element", "summarisedAnswerUnit", "summarisedAnswer", "bsmAnswer"}
)


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
                elif path.endswith(".py"):
                    records.extend(_extract_from_python_code(document=doc))
                elif path.endswith((".yaml", ".yml")):
                    records.extend(_extract_from_yaml_config(document=doc))
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
            for path_value in _documented_evidence_chain_path_values(stripped):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="EvidenceChain.active_path",
                    predicate="documented_evidence_chain_path",
                    normalized_value=path_value,
                    section_path=section,
                    extra_metadata={"support_claim": True, "chain_path": path_value},
                ))
            for path_value in _documented_evidence_chain_full_path_values(stripped):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="EvidenceChain.full_path",
                    predicate="documented_evidence_chain_full_path",
                    normalized_value=path_value,
                    section_path=section,
                    extra_metadata={
                        "support_claim": True,
                        "chain_path": path_value,
                        "path_scope": "full",
                    },
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
    current_partition = ""
    previous_nonempty_line = ""
    active_puml_context = ""
    active_puml_context_kind = ""

    for line_no, raw_line in enumerate(lines, start=1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("'") or stripped.startswith("@"):
            continue
        if _PUML_END_NOTE_PATTERN.match(stripped) or _PUML_END_LEGEND_PATTERN.match(stripped):
            active_puml_context = ""
            active_puml_context_kind = ""
            previous_nonempty_line = stripped
            continue
        partition_match = _PUML_PARTITION_PATTERN.match(raw_line)
        if partition_match:
            current_partition = str(partition_match.group("name") or "").strip()
            previous_nonempty_line = stripped
            continue
        note_start_match = _PUML_NOTE_START_PATTERN.match(raw_line)
        if note_start_match:
            inline_body = str(note_start_match.group("inline") or "").strip()
            note_context = " ".join(
                part
                for part in [
                    current_partition,
                    f"note {str(note_start_match.group('context') or '').strip()}".strip(),
                ]
                if part
            ).strip()
            if inline_body:
                active_puml_context = ""
                active_puml_context_kind = ""
                stripped = inline_body
                analysis_text = " ".join(part for part in [note_context, inline_body] if part).strip()
            else:
                active_puml_context = note_context
                active_puml_context_kind = "note"
                previous_nonempty_line = stripped
                continue
        else:
            legend_start_match = _PUML_LEGEND_START_PATTERN.match(raw_line)
            if legend_start_match:
                inline_body = str(legend_start_match.group("inline") or "").strip()
                legend_context = " ".join(part for part in [current_partition, "legend"] if part).strip()
                if inline_body:
                    active_puml_context = ""
                    active_puml_context_kind = ""
                    stripped = inline_body
                    analysis_text = " ".join(part for part in [legend_context, inline_body] if part).strip()
                else:
                    active_puml_context = legend_context
                    active_puml_context_kind = "legend"
                    previous_nonempty_line = stripped
                    continue
            elif active_puml_context:
                analysis_text = " ".join(part for part in [active_puml_context, stripped] if part).strip()
            else:
                analysis_text = stripped

        section = f"{doc_context}:{line_no}"
        if active_puml_context_kind:
            section = f"{section}:{active_puml_context_kind}"

        # Entity + status mentions in PUML
        entities_in_line = _ENTITY_PATTERN.findall(analysis_text)
        statuses_in_line = _STATUS_PATTERN.findall(analysis_text)

        if entities_in_line and statuses_in_line:
            for entity in set(entities_in_line):
                for status in set(statuses_in_line):
                    records.append(_make_claim(
                        document=document, line_no=line_no, line_text=analysis_text,
                        subject_key=f"{entity}.status_canon",
                        predicate="puml_defined_status",
                        normalized_value=status.upper(),
                        section_path=section,
                    ))

        # HITL in PUML
        hitl_match = _HITL_PATTERN.search(analysis_text)
        if hitl_match:
            is_exclusion = bool(_NEGATION_PATTERN.search(analysis_text))
            entities = _ENTITY_PATTERN.findall(analysis_text) or ["general"]
            for entity in set(entities):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=analysis_text,
                    subject_key=f"{entity}.hitl",
                    predicate="puml_hitl_decision",
                    normalized_value="excluded" if is_exclusion else "included",
                    section_path=section,
                ))

        # IN_RUN / run_id patterns
        if _RUN_HIERARCHY_PATTERN.search(analysis_text):
            is_hierarchical = bool(re.search(r"(?:PhaseRun|ChunkPhaseRun|IN_RUN)", analysis_text))
            is_secondary = bool(re.search(r"(?:run_id)", analysis_text)) and bool(_NOT_SSOT_PATTERN.search(analysis_text))
            is_flat = bool(re.search(r"(?:run_id)", analysis_text)) and not is_hierarchical and not is_secondary
            if is_hierarchical or is_flat or is_secondary:
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=analysis_text,
                    subject_key="Run.model",
                    predicate="puml_run_hierarchy",
                    normalized_value="run_id_secondary_only" if is_secondary else ("hierarchical_3tier" if is_hierarchical else "run_id_centric"),
                    section_path=section,
                ))

        # TO_MODIFY in PUML
        if re.search(r"\bTO_MODIFY\b|:TO_MODIFY\b|to_modify", analysis_text):
            records.append(_make_claim(
                document=document, line_no=line_no, line_text=analysis_text,
                subject_key="TO_MODIFY.role",
                predicate="puml_to_modify_inclusion",
                normalized_value="excluded" if _NEGATION_PATTERN.search(analysis_text) else "included",
                section_path=section,
            ))

        # Evidence chain in PUML
        if _EVIDENCE_CHAIN_PATTERN.search(analysis_text):
            is_unit_centric = bool(re.search(r"summarisedAnswerUnit", analysis_text))
            is_summary_centric = bool(re.search(r"summarisedAnswer(?!Unit)", analysis_text))
            if is_unit_centric or is_summary_centric:
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=analysis_text,
                    subject_key="EvidenceChain.direction",
                    predicate="puml_evidence_chain_type",
                    normalized_value="unit_centric" if is_unit_centric else "summary_centric",
                    section_path=section,
                ))
            for path_value in _documented_evidence_chain_path_values(analysis_text):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=analysis_text,
                    subject_key="EvidenceChain.active_path",
                    predicate="puml_evidence_chain_path",
                    normalized_value=path_value,
                    section_path=section,
                    extra_metadata={"support_claim": True, "chain_path": path_value},
                ))
            for path_value in _documented_evidence_chain_full_path_values(analysis_text):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=analysis_text,
                    subject_key="EvidenceChain.full_path",
                    predicate="puml_evidence_chain_full_path",
                    normalized_value=path_value,
                    section_path=section,
                    extra_metadata={
                        "support_claim": True,
                        "chain_path": path_value,
                        "path_scope": "full",
                    },
                ))
        hop_context = " ".join([current_partition, previous_nonempty_line, analysis_text]).strip()
        if re.search(r"\bSUPPORTS\b", analysis_text, re.IGNORECASE):
            if (
                re.search(r"\bStatement\b", hop_context, re.IGNORECASE)
                and re.search(r"\bBSM_Element\b|\bBSM Elements\b", hop_context, re.IGNORECASE)
            ):
                records.append(_make_claim(
                    document=document,
                    line_no=line_no,
                    line_text=hop_context[:300],
                    subject_key="EvidenceChain.hop_statement_element",
                    predicate="puml_evidence_chain_hop",
                    normalized_value="Statement -[:SUPPORTS]-> BSM_Element materialisiert",
                    section_path=section,
                ))

        # Phase/scope distinction in PUML
        if _PHASE_SCOPE_PATTERN.search(analysis_text):
            is_separated = bool(
                re.search(r"(?:ui_phase_id.*UI.only|getrennt|separate|distinct)", analysis_text, re.IGNORECASE)
            )
            is_mixed = bool(
                re.search(r"(?:UI.*kennt.*Fachphasen|vermisch|mixed|ingestion.*genai_ba)", analysis_text, re.IGNORECASE)
            )
            if is_separated or is_mixed:
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=analysis_text,
                    subject_key="Phase.scope_distinction",
                    predicate="puml_phase_scope_type",
                    normalized_value="separated" if is_separated else "mixed",
                    section_path=section,
                ))

        # Entity role in PUML (e.g., summarisedAnswer as Traceability-Node)
        for entity in entities_in_line:
            if re.search(rf"\b{re.escape(entity)}\b.*(?:node|bucket|root|element|artefakt)", analysis_text, re.IGNORECASE):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=analysis_text,
                    subject_key=f"{entity}.role",
                    predicate="puml_entity_role",
                    normalized_value=analysis_text[:200],
                    section_path=section,
                ))

        # Relationship lifecycle in PUML
        if re.search(r"\bRelationship\b", analysis_text) and statuses_in_line:
            lifecycle_type = "staged_based" if "STAGED" in [s.upper() for s in statuses_in_line] else "proposed_based"
            records.append(_make_claim(
                document=document, line_no=line_no, line_text=analysis_text,
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
            analysis_text,
            re.IGNORECASE,
        )
        if start_match:
            records.append(_make_claim(
                document=document, line_no=line_no, line_text=analysis_text,
                subject_key=f"{start_match.group('entity')}.initial_state",
                predicate="puml_initial_status",
                normalized_value=start_match.group("status").upper(),
                section_path=section,
            ))

        # Relationship change model in PUML
        if re.search(r"\bRelationship\b", analysis_text, re.IGNORECASE):
            if re.search(r"(?:version|versionier|immutable|snapshot)", analysis_text, re.IGNORECASE):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=analysis_text,
                    subject_key="Relationship.change_model",
                    predicate="puml_relationship_change_approach",
                    normalized_value="full_versioning",
                    section_path=section,
                ))
            if re.search(r"(?:edge.state|state.*wechsel|direkt.*relationship|to_modify)", analysis_text, re.IGNORECASE):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=analysis_text,
                    subject_key="Relationship.change_model",
                    predicate="puml_relationship_change_approach",
                    normalized_value="edge_state_only",
                    section_path=section,
                ))
        previous_nonempty_line = stripped

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


def _extract_from_python_code(*, document: CollectedDocument) -> list[ExtractedClaimRecord]:
    """Extract BSM domain claims from Python source files.

    Capabilities:
    1. Cypher query analysis — relationship types, node labels, evidence chain direction
    2. Temporal sequencing — persist→enqueue patterns (eventual consistency risks)
    3. Schema completeness — LLM output schemas missing expected fields
    4. Cross-service data flow — field propagation gaps (e.g. missing phase_run_id)
    """
    records: list[ExtractedClaimRecord] = []
    lines = document.body.splitlines()
    path = document.path_hint or document.source_id or "py"
    doc_context = path.split("/")[-1] if "/" in path else path
    docstring_ranges = _python_docstring_line_ranges(body=document.body)
    function_scope_by_line = _python_function_scope_by_line(body=document.body)
    function_name = ""
    function_indent: int | None = None
    function_signature_open = False
    class_name = ""
    class_indent: int | None = None

    # ── Collect all Cypher fragments across the whole file ────────────
    cypher_fragments = _extract_cypher_fragments(body=document.body)
    for fragment in cypher_fragments:
        if _line_range_overlaps_docstring(
            start_line=fragment["start_line"],
            end_line=fragment["end_line"],
            docstring_ranges=docstring_ranges,
        ):
            continue
        section = f"{doc_context}:{fragment['start_line']}"
        cypher_relationships = _extract_cypher_relationships(fragment["text"])
        _emit_relationship_claims(
            records=records,
            document=document,
            line_no=fragment["start_line"],
            line_text=fragment["text"],
            section_path=section,
            relationships=cypher_relationships,
            relationship_predicate="code_cypher_relationship",
            direction_predicate="code_evidence_chain_type",
            step_predicate="code_evidence_chain_step",
            hop_predicate="code_evidence_chain_hop",
            path_predicate="code_evidence_chain_path",
            full_path_predicate="code_evidence_chain_full_path",
            node_usage_predicate="code_cypher_node_usage",
        )

    # ── Line-level analysis ──────────────────────────────────────────
    # Track function boundaries for temporal analysis
    function_persist_lines: dict[str, list[int]] = defaultdict(list)
    function_enqueue_lines: dict[str, list[int]] = defaultdict(list)
    function_supersede_lines: dict[str, list[int]] = defaultdict(list)
    function_rebuild_lines: dict[str, list[int]] = defaultdict(list)
    function_call_lines: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    function_context_tags: dict[str, set[str]] = defaultdict(set)
    function_context_line_lines: dict[str, list[int]] = defaultdict(list)
    function_entity_refs: dict[str, set[str]] = defaultdict(set)
    function_entity_ref_lines: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    function_fields: dict[str, set[str]] = defaultdict(set)
    function_bsm_write_context_lines: dict[str, list[int]] = defaultdict(list)
    function_start_lines: dict[str, int] = {}
    function_names_to_keys: dict[str, set[str]] = defaultdict(set)
    schema_context_active = False
    schema_fields_found: set[str] = set()
    schema_context_start_line = 0
    schema_context_function = ""
    schema_context_lines: list[str] = []

    for line_no, raw_line in enumerate(lines, start=1):
        stripped = raw_line.strip()
        if not stripped:
            continue
        if _line_is_within_docstring(line_no=line_no, docstring_ranges=docstring_ranges):
            continue
        indent = len(raw_line) - len(raw_line.lstrip())

        # Reset stale scopes when indentation returns to an outer level.
        if function_name and function_indent is not None and not function_signature_open and indent <= function_indent:
            function_name = ""
            function_indent = None
        if class_name and class_indent is not None and not function_name and indent <= class_indent:
            class_name = ""
            class_indent = None

        # Track class/function context
        class_match = re.match(r"^class\s+(\w+)", stripped)
        if class_match:
            class_name = class_match.group(1)
            class_indent = indent
            continue
        func_match = re.match(r"^\s*(?:async\s+)?def\s+(\w+)", raw_line)
        if func_match:
            # Emit schema completeness finding for previous function if applicable
            if schema_context_active:
                _emit_schema_completeness_claims(
                    records=records,
                    document=document,
                    fields_found=schema_fields_found,
                    start_line=schema_context_start_line,
                    function_name=schema_context_function,
                    doc_context=doc_context,
                    raw_line=stripped,
                    context_lines=schema_context_lines,
                )
                schema_context_active = False
                schema_fields_found = set()
                schema_context_lines = []
            if class_name and class_indent is not None and indent <= class_indent:
                class_name = ""
                class_indent = None
            function_name = func_match.group(1)
            function_indent = indent
            function_signature_open = not stripped.endswith(":")
            current_func_key = f"{class_name}.{function_name}" if class_name and function_name else function_name
            function_start_lines.setdefault(current_func_key, line_no)
            function_names_to_keys[function_name].add(current_func_key)
            if _FIELD_MISSING_NAME_CONTEXT.search(function_name):
                function_context_tags[current_func_key].add("name_context")
            continue
        if function_signature_open and stripped.endswith(":"):
            function_signature_open = False

        current_func_key = (
            f"{class_name}.{function_name}" if class_name and function_name else function_name
        ) or function_scope_by_line.get(line_no, "")
        section = f"{doc_context}:{line_no}"
        if stripped.startswith("#"):
            continue

        # 2. Temporal sequencing: persist then enqueue
        if _PERSIST_CALL_PATTERN.search(stripped):
            function_persist_lines[current_func_key].append(line_no)
        if _ENQUEUE_CALL_PATTERN.search(stripped):
            function_enqueue_lines[current_func_key].append(line_no)

        # 2b. Supersede then rebuild
        if _SUPERSEDE_PATTERN.search(stripped):
            function_supersede_lines[current_func_key].append(line_no)
        if _REBUILD_PATTERN.search(stripped):
            function_rebuild_lines[current_func_key].append(line_no)

        # 3. Schema completeness: detect output schema context
        if _LLM_OUTPUT_CONTEXT_PATTERN.search(stripped):
            schema_context_active = True
            schema_context_start_line = line_no
            schema_context_function = current_func_key
            schema_context_lines = [stripped]
        if schema_context_active:
            if stripped not in schema_context_lines:
                schema_context_lines.append(stripped)
            for field_match in _SCHEMA_OUTPUT_FIELDS_PATTERN.finditer(stripped):
                schema_fields_found.add(field_match.group("field").lower())

        # 4. Cross-service field propagation
        if _FIELD_MISSING_LINE_CONTEXT.search(stripped):
            function_context_tags[current_func_key].add("line_context")
            function_context_line_lines[current_func_key].append(line_no)
        field_matches = _FIELD_ASSIGNMENT_PATTERN.findall(stripped)
        for quoted_field, bare_field in field_matches:
            detected_field = quoted_field or bare_field
            if detected_field:
                function_fields[current_func_key].add(detected_field.lower())
        for entity in _entity_hints_from_text(stripped):
            function_entity_refs[current_func_key].add(entity)
            function_entity_ref_lines[current_func_key][entity].append(line_no)
        if _BSM_WRITE_CONTEXT_PATTERN.search(stripped):
            function_bsm_write_context_lines[current_func_key].append(line_no)
        for call_match in _LOCAL_CALL_PATTERN.finditer(stripped):
            called_name = str(call_match.group("name") or "").strip()
            if not called_name or called_name in _IGNORED_LOCAL_CALL_NAMES:
                continue
            function_call_lines[current_func_key][called_name].append(line_no)

        # 5. Evidence chain entities in regular code lines
        if _EVIDENCE_CHAIN_PATTERN.search(stripped):
            is_unit_centric = bool(re.search(r"summarisedAnswerUnit", stripped))
            is_summary_centric = bool(re.search(r"summarisedAnswer(?!Unit)", stripped))
            if is_unit_centric or is_summary_centric:
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="EvidenceChain.direction",
                    predicate="code_evidence_chain_type",
                    normalized_value="unit_centric" if is_unit_centric else "summary_centric",
                    section_path=section,
                ))

        # 6. Entity + status in code
        entities_in_line = _ENTITY_PATTERN.findall(stripped)
        statuses_in_line = _STATUS_PATTERN.findall(stripped)
        if entities_in_line and statuses_in_line:
            for entity in set(entities_in_line):
                for status in set(statuses_in_line):
                    records.append(_make_claim(
                        document=document, line_no=line_no, line_text=stripped,
                        subject_key=f"{entity}.status_canon",
                        predicate="code_defined_status",
                        normalized_value=status.upper(),
                        section_path=section,
                    ))

        # 7. HITL claims in code
        hitl_match = _HITL_PATTERN.search(stripped)
        if hitl_match:
            is_exclusion = bool(_NEGATION_PATTERN.search(stripped))
            entities = _ENTITY_PATTERN.findall(stripped) or ["general"]
            for entity in set(entities):
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key=f"{entity}.hitl",
                    predicate="code_hitl_decision",
                    normalized_value="excluded" if is_exclusion else "included",
                    section_path=section,
                ))

    # ── Post-pass: emit temporal sequencing findings ─────────────────
    enqueue_cache: dict[str, bool] = {}

    def _function_or_helper_has_enqueue(func_key: str, *, stack: set[str] | None = None) -> bool:
        cached = enqueue_cache.get(func_key)
        if cached is not None:
            return cached
        if function_enqueue_lines.get(func_key):
            enqueue_cache[func_key] = True
            return True
        local_stack = set(stack or set())
        if func_key in local_stack:
            enqueue_cache[func_key] = False
            return False
        local_stack.add(func_key)
        for called_name in function_call_lines.get(func_key, {}):
            for target_func_key in function_names_to_keys.get(called_name, set()):
                if target_func_key == func_key:
                    continue
                if _function_or_helper_has_enqueue(target_func_key, stack=local_stack):
                    enqueue_cache[func_key] = True
                    return True
        enqueue_cache[func_key] = False
        return False

    for func_key in set(function_persist_lines.keys()) | set(function_enqueue_lines.keys()) | set(function_call_lines.keys()):
        persist_lines = function_persist_lines.get(func_key, [])
        enqueue_lines = list(function_enqueue_lines.get(func_key, []))
        for called_name, call_lines in function_call_lines.get(func_key, {}).items():
            if any(
                _function_or_helper_has_enqueue(target_func_key)
                for target_func_key in function_names_to_keys.get(called_name, set())
                if target_func_key != func_key
            ):
                enqueue_lines.extend(call_lines)
        if persist_lines and enqueue_lines:
            earliest_persist = min(persist_lines)
            latest_enqueue = max(enqueue_lines)
            if earliest_persist < latest_enqueue:
                support_metadata = {
                    "support_claim": True,
                    "function_name": func_key,
                    "sequence_kind": "persist_then_enqueue",
                    "sequence_start_line": earliest_persist,
                    "sequence_end_line": latest_enqueue,
                    "sequence_path": ["persist", "enqueue"],
                    "expected_sequence_path": ["persist", "protected_reaggregation", "enqueue"],
                    "sequence_break_mode": "async_gap",
                    "sequence_break_before": "persist",
                    "sequence_break_after": "enqueue",
                }
                records.append(_make_claim(
                    document=document,
                    line_no=earliest_persist,
                    line_text=f"{func_key}: persist vor enqueue",
                    subject_key="TemporalConsistency.persist_then_enqueue",
                    predicate="code_temporal_sequence",
                    normalized_value="persist_before_enqueue",
                    section_path=f"{doc_context}:{earliest_persist}",
                    extra_metadata=support_metadata,
                ))
                records.append(_make_claim(
                    document=document,
                    line_no=earliest_persist,
                    line_text=f"{func_key}: persist (L{earliest_persist}) vor enqueue (L{latest_enqueue})",
                    subject_key="TemporalConsistency.persist_then_enqueue",
                    predicate="code_eventual_consistency_risk",
                    normalized_value=f"eventual_consistent: {func_key} persistiert in L{earliest_persist}, enqueued in L{latest_enqueue}",
                    section_path=f"{doc_context}:{earliest_persist}",
                    extra_metadata=support_metadata,
                ))

    for func_key in set(function_supersede_lines.keys()) | set(function_rebuild_lines.keys()):
        supersede_lines = function_supersede_lines.get(func_key, [])
        rebuild_lines = function_rebuild_lines.get(func_key, [])
        if supersede_lines and rebuild_lines:
            earliest_supersede = min(supersede_lines)
            latest_rebuild = max(rebuild_lines)
            if earliest_supersede < latest_rebuild:
                support_metadata = {
                    "support_claim": True,
                    "function_name": func_key,
                    "sequence_kind": "supersede_then_rebuild",
                    "sequence_start_line": earliest_supersede,
                    "sequence_end_line": latest_rebuild,
                    "sequence_path": ["supersede", "rebuild"],
                    "expected_sequence_path": ["supersede", "replacement_chain_available", "rebuild"],
                    "sequence_break_mode": "replacement_gap",
                    "sequence_break_before": "supersede",
                    "sequence_break_after": "rebuild",
                }
                records.append(_make_claim(
                    document=document,
                    line_no=earliest_supersede,
                    line_text=f"{func_key}: supersede vor rebuild",
                    subject_key="TemporalConsistency.supersede_then_rebuild",
                    predicate="code_temporal_sequence",
                    normalized_value="supersede_before_rebuild",
                    section_path=f"{doc_context}:{earliest_supersede}",
                    extra_metadata=support_metadata,
                ))
                records.append(_make_claim(
                    document=document,
                    line_no=earliest_supersede,
                    line_text=f"{func_key}: supersede (L{earliest_supersede}) vor rebuild (L{latest_rebuild})",
                    subject_key="TemporalConsistency.supersede_then_rebuild",
                    predicate="code_chain_interruption_risk",
                    normalized_value=f"chain_interrupted: {func_key} superseded in L{earliest_supersede}, rebuilt in L{latest_rebuild}",
                    section_path=f"{doc_context}:{earliest_supersede}",
                    extra_metadata=support_metadata,
                ))

    for func_key, context_tags in function_context_tags.items():
        if not context_tags:
            continue
        if not (
            function_persist_lines.get(func_key)
            or function_supersede_lines.get(func_key)
            or function_rebuild_lines.get(func_key)
        ):
            continue
        path_context = bool(_BSM_PATH_CONTEXT_PATTERN.search(path))
        write_context_lines = function_bsm_write_context_lines.get(func_key, [])
        if not (path_context or write_context_lines):
            continue
        if "phase_run_id" in function_fields.get(func_key, set()):
            continue
        relevant_write_lines = sorted(
            set(
                write_context_lines
                + function_persist_lines.get(func_key, [])
                + function_supersede_lines.get(func_key, [])
                + function_rebuild_lines.get(func_key, [])
            )
        )
        entity_refs = {
            entity
            for entity in function_entity_refs.get(func_key, set())
            if entity in _FIELD_PROPAGATION_TARGET_ENTITIES
        }
        if not entity_refs:
            continue
        entity_refs = {
            entity
            for entity in entity_refs
            if any(
                min(abs(entity_line - write_line) for write_line in relevant_write_lines) <= _FIELD_CONTEXT_PROXIMITY_LINES
                and entity_line <= (max(relevant_write_lines) + _FIELD_ENTITY_FORWARD_LINES)
                for entity_line in function_entity_ref_lines.get(func_key, {}).get(entity, [])
            )
        }
        if not entity_refs:
            continue
        effective_context_tags: set[str] = set()
        if "name_context" in context_tags:
            effective_context_tags.add("name_context")
        if "line_context" in context_tags:
            context_line_lines = function_context_line_lines.get(func_key, [])
            if (
                not relevant_write_lines
                or any(
                    min(abs(context_line - write_line) for write_line in relevant_write_lines)
                    <= _FIELD_CONTEXT_PROXIMITY_LINES
                    for context_line in context_line_lines
                )
            ):
                effective_context_tags.add("line_context")
        if not effective_context_tags:
            continue
        start_line = function_start_lines.get(func_key, 1)
        for entity in sorted(entity_refs):
            support_metadata = {
                "support_claim": True,
                "function_name": func_key,
                "missing_field": "phase_run_id",
                "context_tags": sorted(effective_context_tags),
                "path_context": path_context,
                "write_context_lines": write_context_lines,
                "target_entity": entity,
                "propagation_path": [func_key, entity],
                "expected_propagation_fields": ["phase_run_id"],
                "propagation_break_mode": "field_drop",
            }
            records.append(_make_claim(
                document=document,
                line_no=start_line,
                line_text=f"{func_key}: fehlendes Feld phase_run_id",
                subject_key=f"{entity}.field_propagation",
                predicate="code_missing_required_field",
                normalized_value="phase_run_id",
                section_path=f"{doc_context}:{start_line}",
                extra_metadata=support_metadata,
            ))
            records.append(_make_claim(
                document=document,
                line_no=start_line,
                line_text=f"{func_key}: Kontext {', '.join(sorted(effective_context_tags))}",
                subject_key=f"{entity}.field_propagation",
                predicate="code_propagation_context",
                normalized_value=",".join(sorted(effective_context_tags)),
                section_path=f"{doc_context}:{start_line}",
                extra_metadata=support_metadata,
            ))
            records.append(_make_claim(
                document=document,
                line_no=start_line,
                line_text=f"{func_key}: Kontext {', '.join(sorted(effective_context_tags))}, aber ohne phase_run_id",
                subject_key=f"{entity}.field_propagation",
                predicate="code_field_propagation_gap",
                normalized_value=f"{entity} in {func_key}: refine/legacy/manual Kontext ohne phase_run_id",
                section_path=f"{doc_context}:{start_line}",
                extra_metadata=support_metadata,
            ))

    # ── Final schema context if still active at EOF ──────────────────
    if schema_context_active:
        _emit_schema_completeness_claims(
            records=records,
            document=document,
            fields_found=schema_fields_found,
            start_line=schema_context_start_line,
            function_name=schema_context_function,
            doc_context=doc_context,
            raw_line="<EOF>",
            context_lines=schema_context_lines,
        )

    return records


def _python_docstring_line_ranges(*, body: str) -> list[tuple[int, int]]:
    try:
        tree = ast.parse(body)
    except SyntaxError:
        return []

    ranges: list[tuple[int, int]] = []

    def _visit(node: ast.AST) -> None:
        body_nodes = getattr(node, "body", None)
        if isinstance(body_nodes, list) and body_nodes:
            first_stmt = body_nodes[0]
            if (
                isinstance(first_stmt, ast.Expr)
                and isinstance(first_stmt.value, ast.Constant)
                and isinstance(first_stmt.value.value, str)
            ):
                start_line = getattr(first_stmt, "lineno", None)
                end_line = getattr(first_stmt, "end_lineno", start_line)
                if start_line is not None:
                    ranges.append((int(start_line), int(end_line or start_line)))
            for child in body_nodes:
                if isinstance(child, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                    _visit(child)

    _visit(tree)
    return ranges


def _python_function_scope_by_line(*, body: str) -> dict[int, str]:
    try:
        tree = ast.parse(body)
    except SyntaxError:
        return {}

    scope_by_line: dict[int, tuple[int, str]] = {}

    def _visit(
        node: ast.AST,
        *,
        class_stack: tuple[str, ...],
        function_stack: tuple[str, ...],
    ) -> None:
        body_nodes = getattr(node, "body", None)
        if not isinstance(body_nodes, list):
            return
        for child in body_nodes:
            if isinstance(child, ast.ClassDef):
                _visit(child, class_stack=class_stack + (child.name,), function_stack=function_stack)
                continue
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                nested_stack = function_stack + (child.name,)
                function_key = ".".join((*class_stack, *nested_stack))
                depth = len(class_stack) + len(nested_stack)
                start_line = int(getattr(child, "lineno", 0) or 0)
                end_line = int(getattr(child, "end_lineno", start_line) or start_line)
                for line_no in range(start_line, end_line + 1):
                    current = scope_by_line.get(line_no)
                    if current is None or depth >= current[0]:
                        scope_by_line[line_no] = (depth, function_key)
                _visit(child, class_stack=class_stack, function_stack=nested_stack)

    _visit(tree, class_stack=(), function_stack=())
    return {line_no: function_key for line_no, (_, function_key) in scope_by_line.items()}


def _line_is_within_docstring(*, line_no: int, docstring_ranges: Sequence[tuple[int, int]]) -> bool:
    return any(start_line <= line_no <= end_line for start_line, end_line in docstring_ranges)


def _line_range_overlaps_docstring(
    *,
    start_line: int,
    end_line: int,
    docstring_ranges: Sequence[tuple[int, int]],
) -> bool:
    return any(start_line <= doc_end and end_line >= doc_start for doc_start, doc_end in docstring_ranges)


def _extract_cypher_fragments(*, body: str) -> list[_CypherFragment]:
    """Extract Cypher query fragments from Python source code.

    Looks for triple-quoted strings or regular strings containing Cypher keywords.
    Returns a list of _CypherFragment dicts with 'text', 'start_line', 'end_line'.
    """
    fragments: list[_CypherFragment] = []
    lines = body.splitlines()
    in_triple_string = False
    triple_char = ""
    current_fragment_lines: list[str] = []
    current_start_line = 0

    for line_no, raw_line in enumerate(lines, start=1):
        stripped = raw_line.strip()

        if in_triple_string:
            current_fragment_lines.append(raw_line)
            # Check for end of triple-quoted string
            if triple_char in stripped:
                fragment_text = "\n".join(current_fragment_lines)
                if _CYPHER_KEYWORD_PATTERN.search(fragment_text):
                    fragments.append({
                        "text": fragment_text,
                        "start_line": current_start_line,
                        "end_line": line_no,
                    })
                in_triple_string = False
                current_fragment_lines = []
            continue

        # Detect start of triple-quoted string with Cypher content
        for triple in ('"""', "'''"):
            if triple in stripped:
                # Count occurrences — if odd, we're entering a triple-quoted string
                count = stripped.count(triple)
                if count == 1:
                    in_triple_string = True
                    triple_char = triple
                    current_start_line = line_no
                    current_fragment_lines = [raw_line]
                    break
                elif count >= 2:
                    # Self-contained triple-quoted string on one line
                    if _CYPHER_KEYWORD_PATTERN.search(stripped):
                        fragments.append({
                            "text": stripped,
                            "start_line": line_no,
                            "end_line": line_no,
                        })
                    break
        else:
            # Check regular strings for Cypher
            if _CYPHER_KEYWORD_PATTERN.search(stripped) and ('"' in stripped or "'" in stripped):
                # Extract string content
                for string_match in re.finditer(r'["\']([^"\']{20,})["\']', stripped):
                    candidate = string_match.group(1)
                    if _CYPHER_KEYWORD_PATTERN.search(candidate):
                        fragments.append({
                            "text": candidate,
                            "start_line": line_no,
                            "end_line": line_no,
                        })

    return fragments


def _extract_yaml_cypher_fragments(*, body: str) -> list[_CypherFragment]:
    fragments: list[_CypherFragment] = []
    lines = body.splitlines()
    line_count = len(lines)
    index = 0

    while index < line_count:
        raw_line = lines[index]
        header_match = _YAML_BLOCK_HEADER_PATTERN.match(raw_line)
        if header_match is None:
            index += 1
            continue
        key = str(header_match.group("key") or "").strip().lower()
        if not any(token in key for token in ("cypher", "query", "template")):
            index += 1
            continue
        base_indent = len(header_match.group("indent") or "")
        block_lines: list[str] = []
        block_start_line = index + 2
        next_index = index + 1
        while next_index < line_count:
            block_raw = lines[next_index]
            if block_raw.strip():
                block_indent = len(block_raw) - len(block_raw.lstrip())
                if block_indent <= base_indent:
                    break
                block_lines.append(block_raw[base_indent + 2 :] if len(block_raw) > base_indent + 2 else "")
            else:
                block_lines.append("")
            next_index += 1
        fragment_text = "\n".join(block_lines).strip("\n")
        if fragment_text and _CYPHER_KEYWORD_PATTERN.search(fragment_text):
            fragments.append(
                {
                    "text": fragment_text,
                    "start_line": block_start_line,
                    "end_line": max(block_start_line, next_index),
                }
            )
        index = next_index

    return fragments


def _extract_cypher_relationships(text: str) -> list[_CypherRelationship]:
    alias_to_label = {
        match.group("var"): match.group("label")
        for match in _CYPHER_NODE_BINDING_PATTERN.finditer(text)
        if match.group("var") and match.group("label")
    }
    relationships: list[_CypherRelationship] = []
    for match in _CYPHER_EDGE_PATTERN.finditer(text):
        start_label = match.group("start_label") or alias_to_label.get(match.group("start_var") or "", "")
        end_label = match.group("end_label") or alias_to_label.get(match.group("end_var") or "", "")
        relationships.append(
            {
                "rel_type": match.group("rel_type").upper(),
                "start_label": start_label,
                "end_label": end_label,
            }
        )
    return relationships


def _emit_relationship_claims(
    *,
    records: list[ExtractedClaimRecord],
    document: CollectedDocument,
    line_no: int,
    line_text: str,
    section_path: str,
    relationships: Sequence[_CypherRelationship],
    relationship_predicate: str,
    direction_predicate: str,
    step_predicate: str,
    hop_predicate: str,
    path_predicate: str,
    full_path_predicate: str,
    node_usage_predicate: str | None = None,
) -> None:
    if not relationships:
        return

    snippet = line_text[:300]
    chain_labels = {"Statement", "BSM_Element", "summarisedAnswer", "summarisedAnswerUnit"}
    evidence_chain_relationships = [
        rel
        for rel in relationships
        if rel["start_label"] in chain_labels or rel["end_label"] in chain_labels
    ]
    for rel in relationships:
        rel_type = rel["rel_type"]
        start_label = rel["start_label"]
        end_label = rel["end_label"]
        records.append(_make_claim(
            document=document,
            line_no=line_no,
            line_text=snippet,
            subject_key=f"CypherWrite.{rel_type}",
            predicate=relationship_predicate,
            normalized_value=(
                f"{start_label} -[:{rel_type}]-> {end_label}"
                if start_label and end_label
                else f"{rel_type} -> {end_label}" if end_label else rel_type
            ),
            section_path=section_path,
        ))

    if node_usage_predicate is not None:
        for label_match in _CYPHER_NODE_LABEL_PATTERN.finditer(line_text):
            label = label_match.group("label")
            if label in BSM_ENTITIES or label in {
                "bsmAnswer", "summarisedAnswer", "summarisedAnswerUnit",
                "Statement", "BSM_Element", "Relationship",
            }:
                records.append(_make_claim(
                    document=document,
                    line_no=line_no,
                    line_text=snippet,
                    subject_key=f"{label}.role",
                    predicate=node_usage_predicate,
                    normalized_value=f"{label} wird in Cypher-Query verwendet",
                    section_path=section_path,
                ))

    derived_from_targets = {
        rel["end_label"]
        for rel in evidence_chain_relationships
        if rel["rel_type"] == "DERIVED_FROM" and rel["end_label"]
    }
    for rel in evidence_chain_relationships:
        if rel["rel_type"] == "DERIVED_FROM" and rel["start_label"] and rel["end_label"]:
            records.append(_make_claim(
                document=document,
                line_no=line_no,
                line_text=snippet,
                subject_key=f"EvidenceChain.step.{rel['start_label']}.DERIVED_FROM.{rel['end_label']}",
                predicate=step_predicate,
                normalized_value=f"{rel['start_label']} -[:DERIVED_FROM]-> {rel['end_label']}",
                section_path=section_path,
                extra_metadata={
                    "start_label": rel["start_label"],
                    "end_label": rel["end_label"],
                    "relationship_type": rel["rel_type"],
                    "hop_kind": "derived_from",
                    "support_claim": True,
                },
            ))
    if derived_from_targets:
        is_unit_centric = any("Unit" in target for target in derived_from_targets)
        records.append(_make_claim(
            document=document,
            line_no=line_no,
            line_text=snippet,
            subject_key="EvidenceChain.direction",
            predicate=direction_predicate,
            normalized_value="unit_centric" if is_unit_centric else "summary_centric",
            section_path=section_path,
        ))

    has_support_hop = any(
        rel["rel_type"] == "SUPPORTS"
        and {rel["start_label"], rel["end_label"]} == {"Statement", "BSM_Element"}
        for rel in evidence_chain_relationships
    )
    if has_support_hop:
        records.append(_make_claim(
            document=document,
            line_no=line_no,
            line_text=snippet,
            subject_key="EvidenceChain.hop_statement_element",
            predicate=hop_predicate,
            normalized_value="Statement -[:SUPPORTS]-> BSM_Element materialisiert",
            section_path=section_path,
        ))
    for rel in evidence_chain_relationships:
        if rel["rel_type"] == "SUPPORTS" and rel["start_label"] and rel["end_label"]:
            records.append(_make_claim(
                document=document,
                line_no=line_no,
                line_text=snippet,
                subject_key=f"EvidenceChain.step.{rel['start_label']}.SUPPORTS.{rel['end_label']}",
                predicate=step_predicate,
                normalized_value=f"{rel['start_label']} -[:SUPPORTS]-> {rel['end_label']}",
                section_path=section_path,
                extra_metadata={
                    "start_label": rel["start_label"],
                    "end_label": rel["end_label"],
                    "relationship_type": rel["rel_type"],
                    "hop_kind": "supports",
                    "support_claim": True,
                },
            ))
    for path_value in _evidence_chain_path_values(
        derived_targets=derived_from_targets,
        has_support_hop=has_support_hop,
    ):
        records.append(_make_claim(
            document=document,
            line_no=line_no,
            line_text=snippet,
            subject_key="EvidenceChain.active_path",
            predicate=path_predicate,
            normalized_value=path_value,
            section_path=section_path,
            extra_metadata={
                "support_claim": True,
                "chain_path": path_value,
                "has_support_hop": has_support_hop,
            },
        ))
    for path_value in _evidence_chain_full_path_values(
        derived_targets=derived_from_targets,
        has_support_hop=has_support_hop,
    ):
        records.append(_make_claim(
            document=document,
            line_no=line_no,
            line_text=snippet,
            subject_key="EvidenceChain.full_path",
            predicate=full_path_predicate,
            normalized_value=path_value,
            section_path=section_path,
            extra_metadata={
                "support_claim": True,
                "chain_path": path_value,
                "has_support_hop": has_support_hop,
                "path_scope": "full",
                "inferred_prefix": ["bsmAnswer"],
            },
        ))


def _emit_schema_completeness_claims(
    *,
    records: list[ExtractedClaimRecord],
    document: CollectedDocument,
    fields_found: set[str],
    start_line: int,
    function_name: str,
    doc_context: str,
    raw_line: str,
    context_lines: Sequence[str],
) -> None:
    """Emit claims about missing fields in LLM output schemas."""
    context_text = " ".join(str(line) for line in context_lines if str(line).strip())
    path_text = " ".join(
        [
            str(document.path_hint or ""),
            str(document.source_id or ""),
            str(function_name or ""),
        ]
    )
    if re.search(r"normaliz", f"{function_name} {context_text}", re.IGNORECASE) and not _STATEMENT_SCHEMA_CONTEXT_PATTERN.search(context_text):
        return
    if not (
        bool({"rationale", "unit_ids"} & fields_found)
        or
        _STATEMENT_SCHEMA_CONTEXT_PATTERN.search(function_name)
        or _STATEMENT_SCHEMA_CONTEXT_PATTERN.search(context_text)
        or (
            "statement" in path_text.lower()
            and not re.search(r"normaliz", function_name, re.IGNORECASE)
        )
    ):
        return
    # Expected fields for statement generation output
    expected_fields = {"text", "confidence", "rationale", "unit_ids"}
    # Only emit if we found at least some expected fields (confirms it's a schema context)
    found_expected = fields_found.intersection(expected_fields)
    if len(found_expected) < 2:
        return
    missing = expected_fields - fields_found
    if missing:
        records.append(_make_claim(
            document=document,
            line_no=start_line,
            line_text=f"Schema in {function_name}: hat {sorted(found_expected)}, fehlt {sorted(missing)}",
            subject_key="SchemaCompleteness.statement_output",
            predicate="code_schema_missing_fields",
            normalized_value=f"fehlende Felder: {', '.join(sorted(missing))}",
            section_path=f"{doc_context}:{start_line}",
        ))


def _extract_from_yaml_config(*, document: CollectedDocument) -> list[ExtractedClaimRecord]:
    """Extract BSM domain claims from YAML configuration files.

    Primarily targets write_allowlist.yaml and similar configs that define
    which node labels and relationship types are permitted.
    """
    records: list[ExtractedClaimRecord] = []
    lines = document.body.splitlines()
    path = document.path_hint or document.source_id or "yaml"
    doc_context = path.split("/")[-1] if "/" in path else path
    is_allowlist = "allowlist" in doc_context.lower() or "allow_list" in doc_context.lower()
    current_section = ""
    section_indent = -1

    for fragment in _extract_yaml_cypher_fragments(body=document.body):
        section = f"{doc_context}:{fragment['start_line']}"
        yaml_relationships = _extract_cypher_relationships(fragment["text"])
        _emit_relationship_claims(
            records=records,
            document=document,
            line_no=fragment["start_line"],
            line_text=fragment["text"],
            section_path=section,
            relationships=yaml_relationships,
            relationship_predicate="yaml_allowlist_relationship" if is_allowlist else "yaml_defined_relationship",
            direction_predicate="yaml_evidence_chain_type",
            step_predicate="yaml_evidence_chain_step",
            hop_predicate="yaml_evidence_chain_hop",
            path_predicate="yaml_evidence_chain_path",
            full_path_predicate="yaml_evidence_chain_full_path",
        )

    for line_no, raw_line in enumerate(lines, start=1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(raw_line) - len(raw_line.lstrip())
        section = f"{doc_context} > {current_section}" if current_section else doc_context

        # Track top-level sections
        section_match = _YAML_SECTION_PATTERN.match(raw_line)
        if section_match and indent <= max(section_indent, 0):
            current_section = section_match.group("key")
            section_indent = indent
            section = f"{doc_context} > {current_section}"

        # Node label definitions
        label_match = _YAML_NODE_LABEL_PATTERN.match(raw_line)
        if label_match:
            label = label_match.group("label")
            if label in BSM_ENTITIES or any(
                e.lower() == label.lower() for e in BSM_ENTITIES
            ):
                predicate = "yaml_allowlist_node" if is_allowlist else "yaml_defined_node"
                records.append(_make_claim(
                    document=document,
                    line_no=line_no,
                    line_text=stripped,
                    subject_key=f"{label}.role",
                    predicate=predicate,
                    normalized_value=f"{label} definiert in {current_section or doc_context}",
                    section_path=section,
                ))

        # Relationship type definitions
        rel_match = _YAML_REL_TYPE_PATTERN.match(raw_line)
        if rel_match:
            rel_type = rel_match.group("rel")
            predicate = "yaml_allowlist_relationship" if is_allowlist else "yaml_defined_relationship"
            records.append(_make_claim(
                document=document,
                line_no=line_no,
                line_text=stripped,
                subject_key=f"CypherWrite.{rel_type}",
                predicate=predicate,
                normalized_value=f"{rel_type} definiert in {current_section or doc_context}",
                section_path=section,
            ))

        # Write entries (e.g., in write_allowlist)
        entry_match = _YAML_WRITE_ENTRY_PATTERN.match(raw_line)
        if entry_match and is_allowlist:
            entry = entry_match.group("entry").strip()
            # Check if the entry mentions BSM entities
            entities_in_entry = _ENTITY_PATTERN.findall(entry)
            for entity in set(entities_in_entry):
                records.append(_make_claim(
                    document=document,
                    line_no=line_no,
                    line_text=stripped,
                    subject_key=f"{entity}.write_contract",
                    predicate="yaml_write_allowlist_entry",
                    normalized_value=f"{entity} erlaubt in Sektion {current_section}: {entry}",
                    section_path=section,
                ))

        # BSM entities mentioned in any YAML value
        entities_in_line = _ENTITY_PATTERN.findall(stripped)
        statuses_in_line = _STATUS_PATTERN.findall(stripped)
        if entities_in_line and statuses_in_line:
            for entity in set(entities_in_line):
                for status in set(statuses_in_line):
                    records.append(_make_claim(
                        document=document,
                        line_no=line_no,
                        line_text=stripped,
                        subject_key=f"{entity}.status_canon",
                        predicate="yaml_defined_status",
                        normalized_value=status.upper(),
                        section_path=section,
                    ))

        # Evidence chain patterns in YAML
        if _EVIDENCE_CHAIN_PATTERN.search(stripped):
            is_unit_centric = bool(re.search(r"summarisedAnswerUnit", stripped))
            is_summary_centric = bool(re.search(r"summarisedAnswer(?!Unit)", stripped))
            if is_unit_centric or is_summary_centric:
                records.append(_make_claim(
                    document=document, line_no=line_no, line_text=stripped,
                    subject_key="EvidenceChain.direction",
                    predicate="yaml_evidence_chain_type",
                    normalized_value="unit_centric" if is_unit_centric else "summary_centric",
                    section_path=section,
                ))
        yaml_relationships = _extract_cypher_relationships(stripped)
        _emit_relationship_claims(
            records=records,
            document=document,
            line_no=line_no,
            line_text=stripped,
            section_path=section,
            relationships=yaml_relationships,
            relationship_predicate="yaml_allowlist_relationship" if is_allowlist else "yaml_defined_relationship",
            direction_predicate="yaml_evidence_chain_type",
            step_predicate="yaml_evidence_chain_step",
            hop_predicate="yaml_evidence_chain_hop",
            path_predicate="yaml_evidence_chain_path",
            full_path_predicate="yaml_evidence_chain_full_path",
        )

    return records


def _entity_hints_from_text(text: str) -> set[str]:
    lowered = text.lower()
    normalized = re.sub(r"[^a-z0-9]+", "", lowered)
    entities = set(_ENTITY_PATTERN.findall(text))
    for needle, entity in _FUNCTION_ENTITY_HINTS.items():
        if needle in lowered or needle in normalized:
            entities.add(entity)
    return entities


def _evidence_chain_path_values(*, derived_targets: set[str], has_support_hop: bool) -> list[str]:
    values: list[str] = []
    for target in sorted({str(item or "").strip() for item in derived_targets if str(item or "").strip()}):
        if has_support_hop:
            values.append(f"{target} -> Statement -> BSM_Element")
        else:
            values.append(f"{target} -> Statement")
    if has_support_hop and not values:
        values.append("Statement -> BSM_Element")
    return values


def _evidence_chain_full_path_values(*, derived_targets: set[str], has_support_hop: bool) -> list[str]:
    values: list[str] = []
    for target in sorted({str(item or "").strip() for item in derived_targets if str(item or "").strip()}):
        if has_support_hop:
            values.append(f"bsmAnswer -> {target} -> Statement -> BSM_Element")
        else:
            values.append(f"bsmAnswer -> {target} -> Statement")
    return values


def _documented_evidence_chain_entities(text: str) -> list[str]:
    normalized_text = str(text or "").strip()
    if "->" not in normalized_text and "→" not in normalized_text:
        return []
    parts = _EVIDENCE_CHAIN_ARROW_SPLIT.split(normalized_text)
    entities: list[str] = []
    for part in parts:
        matches = _ENTITY_PATTERN.findall(part)
        if not matches:
            continue
        entities.append(matches[0])
    return entities


def _documented_evidence_chain_path_values(text: str) -> list[str]:
    entities = _documented_evidence_chain_entities(text)
    if len(entities) < 2:
        return []
    if entities and entities[0] == "bsmAnswer" and len(entities) > 1:
        entities = entities[1:]
    if len(entities) < 2 or "Statement" not in entities:
        return []
    statement_index = entities.index("Statement")
    active_path = entities[max(statement_index - 1, 0) :]
    if len(active_path) < 2:
        return []
    return [" -> ".join(active_path)]


def _documented_evidence_chain_full_path_values(text: str) -> list[str]:
    entities = _documented_evidence_chain_entities(text)
    if len(entities) < 3:
        return []
    if entities[0] != "bsmAnswer":
        return []
    if "Statement" not in entities:
        return []
    statement_index = entities.index("Statement")
    full_path = entities[:]
    if statement_index < 2:
        return []
    return [" -> ".join(full_path)]


def _make_claim(
    *,
    document: CollectedDocument,
    line_no: int,
    line_text: str,
    subject_key: str,
    predicate: str,
    normalized_value: str,
    section_path: str,
    extra_metadata: dict[str, object] | None = None,
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
            **(extra_metadata or {}),
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
