from __future__ import annotations

import ast
import json
import logging
import re
from typing import Final

from fin_ai_auditor.domain.models import AuditClaimEntry, AuditLocation, AuditPosition
from fin_ai_auditor.services.pipeline_models import CollectedDocument, ExtractedClaimEvidence, ExtractedClaimRecord

logger = logging.getLogger(__name__)


READ_LINE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(get|list|load|read|fetch|query|find|collect|resolve)\b|_[a-z0-9]*(get|load|read|fetch|query|find|collect|resolve)\b|@router\.get\(",
    re.IGNORECASE,
)
WRITE_LINE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(create|update|delete|write|persist|save|upsert|merge|patch)\b|_[a-z0-9]*(create|update|delete|write|persist|save|upsert|merge|patch)\b|@router\.(post|put|patch|delete)\(",
    re.IGNORECASE,
)
LIFECYCLE_LINE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(status|lifecycle|promotion|review|histori|freigabe)\b",
    re.IGNORECASE,
)
PROCESS_LINE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(bsm|phase|prozess|process|metamodel|metamodell)\b",
    re.IGNORECASE,
)
POLICY_LINE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(read-only|readonly|allowlist|approval|approve|guarded|scope|tenant|policy|contract)\b",
    re.IGNORECASE,
)
REVIEW_STATUS_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(review status|review-status|in review|draft|approved|released|freigegeben|archived|historic)\b",
    re.IGNORECASE,
)
APPROVAL_POLICY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(approval|approve|freigabe|guarded|allowlist|review-only|without approval|ohne freigabe)\b",
    re.IGNORECASE,
)
SCOPE_POLICY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(tenant|global|project scoped|project-specific|global read)\b",
    re.IGNORECASE,
)
PHASE_REFERENCE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(?:phase|prozessphase|bsm phase)\s*[:#-]?\s*(?P<phase>[a-z0-9][a-z0-9 _/-]{1,40})",
    re.IGNORECASE,
)
QUESTION_REFERENCE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(?:question|frage)\s*[:#-]?\s*(?P<question>[a-z0-9][a-z0-9 _/-]{1,40})",
    re.IGNORECASE,
)
PHASE_ORDER_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(?:phase order|order|reihenfolge)\s*[:=]?\s*(?P<order>\d{1,3})\b",
    re.IGNORECASE,
)
QUESTION_COUNT_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(?P<count>\d{1,3})\s*(?:questions|fragen)\b|\b(?:question count|fragenzahl)\s*[:=]?\s*(?P<count_named>\d{1,3})\b",
    re.IGNORECASE,
)
SUBJECT_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(r"\b([A-Z][A-Za-z0-9_]{2,})\b")
HEADING_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\s{0,3}(?P<hashes>#{1,6})\s+(?P<title>.+?)\s*$")
TYPESCRIPT_SYMBOL_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^\s*(?:export\s+)?(?:default\s+)?(?:(?:async\s+)?function|const|let|class|interface|type)\s+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)",
)
CONFIG_COLON_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^(?P<indent>\s*)(?:-\s*)?(?P<key>[A-Za-z0-9_.'\"-]+)\s*:\s*(?P<value>.*)$"
)
CONFIG_ASSIGN_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^(?P<indent>\s*)(?P<key>[A-Za-z0-9_.'\"-]+)\s*=\s*(?P<value>.+)$"
)

OBJECT_HINTS: Final[tuple[tuple[tuple[str, ...], str], ...]] = (
    (("statement",), "Statement"),
    (("question", "bsmquestion"), "BSM_Question"),
    (("phase", "bsmphase"), "BSM_Phase"),
    (("project", "finai_project"), "FINAI_Project"),
    (("job", "finai_job"), "FINAI_Job"),
    (("preprocessingrun", "run"), "FINAI_Run"),
    (("chunk", "minedchunk"), "Chunk"),
    (("document",), "Document"),
    (("inputsource", "source"), "InputSource"),
    (("prompt",), "Prompt"),
    (("metaclass",), "MetaClass"),
    (("confluence", "page"), "ConfluencePage"),
)

READ_VERBS: Final[tuple[str, ...]] = ("get", "list", "load", "read", "fetch", "query", "find", "collect", "resolve")
WRITE_VERBS: Final[tuple[str, ...]] = ("create", "update", "delete", "write", "persist", "save", "upsert", "merge", "patch")
LIFECYCLE_HINTS: Final[tuple[str, ...]] = ("status", "lifecycle", "promotion", "review", "histori", "freigabe")
POLICY_HINTS: Final[tuple[str, ...]] = ("read_only", "readonly", "allowlist", "approval", "approve", "guard", "scope", "tenant", "policy", "contract")
REFERENCE_STOP_MARKERS: Final[tuple[str, ...]] = (
    " is ",
    " sind ",
    " must ",
    " soll ",
    " should ",
    " with ",
    " mit ",
    " has ",
    " hat ",
    " contains ",
    " enthaelt ",
    " which ",
    " wobei ",
)
REFERENCE_STOP_CHARS: Final[tuple[str, ...]] = (".", ";", ",", "(", ")", "[", "]")


def extract_claim_records(*, documents: list[CollectedDocument]) -> list[ExtractedClaimRecord]:
    logger.info("claim_extraction_start", extra={"event_name": "claim_extraction_start", "event_payload": {"document_count": len(documents)}})
    records: list[ExtractedClaimRecord] = []
    for document in documents:
        try:
            if document.source_type == "metamodel":
                records.extend(_extract_metamodel_claims(document=document))
                continue
            if document.source_type == "github_file":
                records.extend(_extract_code_claims(document=document))
                continue
            if document.source_type in {"confluence_page", "local_doc"}:
                records.extend(_extract_document_claims(document=document))
        except Exception as exc:
            logger.warning("claim_extraction_doc_failed", extra={"event_name": "claim_extraction_doc_failed", "event_payload": {"source_id": document.source_id, "source_type": document.source_type, "error": str(exc)}})

    # BSM domain-specific claims (cross-cutting, all source types)
    from fin_ai_auditor.services.bsm_domain_claim_extractor import extract_bsm_domain_claims
    records.extend(extract_bsm_domain_claims(documents=documents))

    deduped = _deduplicate_claim_records(records=records)
    logger.info("claim_extraction_done", extra={"event_name": "claim_extraction_done", "event_payload": {"total_claims": len(deduped), "before_dedup": len(records)}})
    return deduped


def _extract_code_claims(*, document: CollectedDocument) -> list[ExtractedClaimRecord]:
    records: list[ExtractedClaimRecord] = []
    if _is_python_document(document=document):
        records.extend(_extract_python_ast_claims(document=document))
    elif _is_typescript_document(document=document):
        records.extend(_extract_typescript_claims(document=document))
    elif _is_config_document(document=document):
        records.extend(_extract_config_claims(document=document))
    for line_no, raw_line in enumerate(document.body.splitlines(), start=1):
        stripped = raw_line.strip()
        if not stripped:
            continue
        subject = _derive_subject_label(line=stripped, document=document)
        if READ_LINE_PATTERN.search(stripped):
            records.append(
                _build_claim_record(
                    document=document,
                    line_no=line_no,
                    line_text=stripped,
                    subject_key=f"{subject}.read_path",
                    predicate="implemented_read",
                    section_path=document.title,
                )
            )
        if WRITE_LINE_PATTERN.search(stripped):
            records.append(
                _build_claim_record(
                    document=document,
                    line_no=line_no,
                    line_text=stripped,
                    subject_key=f"{subject}.write_path",
                    predicate="implemented_write",
                    section_path=document.title,
                )
            )
        if LIFECYCLE_LINE_PATTERN.search(stripped):
            records.append(
                _build_claim_record(
                    document=document,
                    line_no=line_no,
                    line_text=stripped,
                    subject_key=f"{subject}.lifecycle",
                    predicate="implemented_lifecycle",
                    section_path=document.title,
                )
            )
        if POLICY_LINE_PATTERN.search(stripped):
            records.append(
                _build_claim_record(
                    document=document,
                    line_no=line_no,
                    line_text=stripped,
                    subject_key=f"{subject}.policy",
                    predicate="implemented_policy",
                    section_path=document.title,
                )
            )
        records.extend(
            _semantic_subclaim_records_for_line(
                document=document,
                line_no=line_no,
                line_text=stripped,
                section_path=document.title,
                subject=subject,
                predicate_prefix="implemented",
            )
        )
    return records


def _extract_typescript_claims(*, document: CollectedDocument) -> list[ExtractedClaimRecord]:
    lines = document.body.splitlines()
    records: list[ExtractedClaimRecord] = []
    block_lines: list[str] = []
    block_start = 1
    current_symbol = document.title

    def flush(line_end: int) -> None:
        nonlocal block_lines
        if not block_lines:
            return
        snippet = "\n".join(block_lines).strip()
        if len(snippet) < 18:
            block_lines = []
            return
        subject = _derive_subject_label_from_hints(
            hint_texts=[current_symbol, snippet, document.title, str(document.path_hint or "")]
        )
        for subject_suffix, predicate in _typescript_claim_predicates(
            symbol_name=current_symbol,
            snippet=snippet,
        ):
            records.append(
                _build_structured_claim_record(
                    document=document,
                    subject_key=f"{subject}.{subject_suffix}",
                    predicate=predicate,
                    matched_text=f"{current_symbol} [{predicate}] {snippet}",
                    line_start=block_start,
                    line_end=max(block_start, line_end),
                    section_path=current_symbol,
                    confidence=0.82,
                    metadata={
                        "path_hint": document.path_hint,
                        "title": document.title,
                        "ts_extracted": True,
                    },
                )
            )
        for subject_key, predicate in _semantic_subclaim_specs(
            subject=subject,
            predicate_prefix="implemented",
            text_fragments=[current_symbol, snippet],
        ):
            records.append(
                _build_structured_claim_record(
                    document=document,
                    subject_key=subject_key,
                    predicate=predicate,
                    matched_text=f"{current_symbol} [{predicate}] {snippet}",
                    line_start=block_start,
                    line_end=max(block_start, line_end),
                    section_path=current_symbol,
                    confidence=0.8,
                    metadata={
                        "path_hint": document.path_hint,
                        "title": document.title,
                        "ts_extracted": True,
                        "semantic_subclaim": True,
                    },
                )
            )
        block_lines = []

    for line_no, raw_line in enumerate(lines, start=1):
        symbol_match = TYPESCRIPT_SYMBOL_PATTERN.match(raw_line)
        if symbol_match is not None:
            flush(line_no - 1)
            current_symbol = symbol_match.group("name")
            block_start = line_no
        stripped = raw_line.strip()
        if not stripped:
            flush(line_no)
            block_start = line_no + 1
            continue
        block_lines.append(raw_line)
    flush(len(lines))
    return records


def _extract_config_claims(*, document: CollectedDocument) -> list[ExtractedClaimRecord]:
    records: list[ExtractedClaimRecord] = []
    key_stack: list[tuple[int, str]] = []
    for line_no, raw_line in enumerate(document.body.splitlines(), start=1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith(("#", "//", ";")):
            continue
        match = CONFIG_COLON_PATTERN.match(raw_line) or CONFIG_ASSIGN_PATTERN.match(raw_line)
        if match is None:
            continue
        indent = len(match.group("indent") or "")
        key = _clean_config_key(match.group("key") or "")
        value = _clean_config_value(match.group("value") or "")
        if not key:
            continue
        key_stack = _updated_config_stack(stack=key_stack, indent=indent, key=key)
        key_path = [entry[1] for entry in key_stack]
        section_path = " > ".join([document.title, *key_path])
        subject = _derive_subject_label_from_hints(
            hint_texts=[section_path, value, document.title, str(document.path_hint or "")]
        )
        context_text = f"{'.'.join(key_path)} = {value}" if value else ".".join(key_path)
        predicates = _config_claim_predicates(key_path=key_path, value=value)
        for subject_suffix, predicate in predicates:
            records.append(
                _build_claim_record(
                    document=document,
                    line_no=line_no,
                    line_text=context_text,
                    subject_key=f"{subject}.{subject_suffix}",
                    predicate=predicate,
                    section_path=section_path,
                )
            )
        records.extend(
            _semantic_subclaim_records_for_line(
                document=document,
                line_no=line_no,
                line_text=context_text,
                section_path=section_path,
                subject=subject,
                predicate_prefix="implemented",
            )
        )
    return records


def _extract_python_ast_claims(*, document: CollectedDocument) -> list[ExtractedClaimRecord]:
    try:
        tree = ast.parse(document.body)
    except SyntaxError:
        return []
    lines = document.body.splitlines()
    records: list[ExtractedClaimRecord] = []

    def visit(node: ast.AST, *, class_stack: list[str]) -> None:
        if isinstance(node, ast.ClassDef):
            next_stack = [*class_stack, node.name]
            for child in node.body:
                visit(child, class_stack=next_stack)
            return
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            records.extend(
                _extract_function_claim_records(
                    document=document,
                    node=node,
                    class_stack=class_stack,
                    lines=lines,
                )
            )
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                visit(child, class_stack=class_stack)

    for child in tree.body:
        visit(child, class_stack=[])
    return records


def _extract_document_claims(*, document: CollectedDocument) -> list[ExtractedClaimRecord]:
    records: list[ExtractedClaimRecord] = []
    base_context = _document_base_context(document=document)
    heading_stack: list[str] = []
    current_heading = _document_section_path(base_context=base_context, heading_stack=heading_stack)
    current_phase_key: str | None = _extract_phase_key(text_fragments=[current_heading])
    for line_no, raw_line in enumerate(document.body.splitlines(), start=1):
        stripped = raw_line.strip()
        if not stripped:
            continue
        heading_match = HEADING_PATTERN.match(raw_line)
        if heading_match is not None:
            heading_stack = _updated_heading_stack(
                stack=heading_stack,
                level=len(heading_match.group("hashes")),
                title=heading_match.group("title").strip(),
            )
            current_heading = _document_section_path(base_context=base_context, heading_stack=heading_stack)
            current_phase_key = _extract_phase_key(text_fragments=[current_heading])
            continue
        subject = _derive_subject_label(line=f"{current_heading} {stripped}", document=document)
        line_phase_key = _extract_phase_key(text_fragments=[current_heading, stripped])
        if line_phase_key is not None:
            current_phase_key = line_phase_key
        if READ_LINE_PATTERN.search(stripped):
            records.append(
                _build_claim_record(
                    document=document,
                    line_no=line_no,
                    line_text=stripped,
                    subject_key=f"{subject}.read_path",
                    predicate="documented_read",
                    section_path=current_heading,
                )
            )
        if WRITE_LINE_PATTERN.search(stripped):
            records.append(
                _build_claim_record(
                    document=document,
                    line_no=line_no,
                    line_text=stripped,
                    subject_key=f"{subject}.write_path",
                    predicate="documented_write",
                    section_path=current_heading,
                )
            )
        if LIFECYCLE_LINE_PATTERN.search(stripped):
            records.append(
                _build_claim_record(
                    document=document,
                    line_no=line_no,
                    line_text=stripped,
                    subject_key=f"{subject}.lifecycle",
                    predicate="documented_lifecycle",
                    section_path=current_heading,
                )
            )
        if PROCESS_LINE_PATTERN.search(stripped):
            records.append(
                _build_claim_record(
                    document=document,
                    line_no=line_no,
                    line_text=stripped,
                    subject_key="BSM.process",
                    predicate="documented_process",
                    section_path=current_heading,
                )
            )
        if POLICY_LINE_PATTERN.search(stripped):
            records.append(
                _build_claim_record(
                    document=document,
                    line_no=line_no,
                    line_text=stripped,
                    subject_key=f"{subject}.policy",
                    predicate="documented_policy",
                    section_path=current_heading,
                )
            )
        records.extend(
            _semantic_subclaim_records_for_line(
                document=document,
                line_no=line_no,
                line_text=stripped,
                section_path=current_heading,
                subject=subject,
                predicate_prefix="documented",
                default_phase_key=current_phase_key,
            )
        )
    return records


def _extract_metamodel_claims(*, document: CollectedDocument) -> list[ExtractedClaimRecord]:
    records: list[ExtractedClaimRecord] = []
    try:
        rows = json.loads(document.body)
    except ValueError:
        rows = []
    if not isinstance(rows, list):
        rows = []
    records.append(
        _build_metamodel_claim_record(
            document=document,
            anchor_value="BSM.process.phase_count",
            subject_key="BSM.process",
            predicate="phase_count",
            normalized_value=str(len(rows)),
            matched_text=f"{len(rows)} Phasen im aktuellen Metamodell-Dump",
        )
    )
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        entity_kind = str(row.get("entity_kind") or "phase").strip().casefold()
        if entity_kind == "metaclass":
            records.extend(_extract_metamodel_metaclass_claims(document=document, row=row, index=index))
            continue
        if entity_kind == "bsm_function":
            records.extend(_extract_metamodel_function_claims(document=document, row=row, index=index))
            continue
        if entity_kind == "label_summary":
            records.extend(_extract_metamodel_label_summary_claims(document=document, row=row, index=index))
            continue
        phase_id = str(
            row.get("phase_id")
            or row.get("phase_public_id")
            or row.get("public_id")
            or row.get("id")
            or f"phase_{index}"
        ).strip()
        phase_name = str(row.get("phase_name") or row.get("name") or phase_id).strip()
        phase_key = _phase_subject_key(phase_id=phase_id, phase_name=phase_name)
        question_count = len(row.get("questions") or []) if isinstance(row.get("questions"), list) else 0
        records.append(
            _build_metamodel_claim_record(
                document=document,
                anchor_value=phase_key,
                subject_key=phase_key,
                predicate="metamodel_phase",
                normalized_value=phase_name,
                matched_text=f"{phase_id}: {phase_name}",
            )
        )
        phase_order = str(row.get("phase_order") or row.get("order") or "").strip()
        if phase_order:
            records.append(
                _build_metamodel_claim_record(
                    document=document,
                    anchor_value=f"{phase_key}.phase_order",
                    subject_key=phase_key,
                    predicate="phase_order",
                    normalized_value=phase_order,
                    matched_text=f"{phase_name} hat Phase-Order {phase_order}",
                )
            )
        records.append(
            _build_metamodel_claim_record(
                document=document,
                anchor_value=f"BSM.process.phase_reference.{phase_key}",
                subject_key="BSM.process",
                predicate="phase_reference",
                normalized_value=phase_name,
                matched_text=f"BSM Prozess enthaelt Phase {phase_name}",
            )
        )
        records.append(
            _build_metamodel_claim_record(
                document=document,
                anchor_value=f"{phase_key}.question_count",
                subject_key=phase_key,
                predicate="question_count",
                normalized_value=str(question_count),
                matched_text=f"{phase_name} hat {question_count} Fragen",
            )
        )
        for raw_question in row.get("questions") or []:
            if not isinstance(raw_question, dict):
                continue
            question_text = str(raw_question.get("question_text") or raw_question.get("text") or raw_question.get("name") or "").strip()
            question_id = str(raw_question.get("question_id") or raw_question.get("id") or "").strip()
            question_key = _slugify(question_text or question_id)
            if not question_key:
                continue
            records.append(
                _build_metamodel_claim_record(
                    document=document,
                    anchor_value=f"{phase_key}.question.{question_key}",
                    subject_key=f"{phase_key}.question.{question_key}",
                    predicate="metamodel_question",
                    normalized_value=question_text or question_id,
                    matched_text=f"{phase_name} Frage {question_text or question_id}",
                )
            )
    return records


def _extract_metamodel_metaclass_claims(
    *,
    document: CollectedDocument,
    row: dict[str, object],
    index: int,
) -> list[ExtractedClaimRecord]:
    metaclass_id = str(row.get("metaclass_id") or row.get("public_id") or f"metaclass_{index}").strip()
    metaclass_name = str(row.get("metaclass_name") or row.get("name") or metaclass_id).strip()
    subject_key = f"MetaClass.{_slugify(metaclass_name) or _slugify(metaclass_id) or f'metaclass_{index}'}"
    records = [
        _build_metamodel_claim_record(
            document=document,
            anchor_value=subject_key,
            subject_key=subject_key,
            predicate="metamodel_metaclass",
            normalized_value=metaclass_name,
            matched_text=f"Metaclass {metaclass_name}",
        )
    ]
    for package_name in _string_list(row.get("package_names"))[:6]:
        records.append(
            _build_metamodel_claim_record(
                document=document,
                anchor_value=f"{subject_key}.package.{_slugify(package_name)}",
                subject_key=subject_key,
                predicate="package_reference",
                normalized_value=package_name,
                matched_text=f"{metaclass_name} liegt im Package {package_name}",
            )
        )
    for relation_type in _string_list(row.get("outbound_relation_types"))[:12]:
        records.append(
            _build_metamodel_claim_record(
                document=document,
                anchor_value=f"{subject_key}.relation.{_slugify(relation_type)}",
                subject_key=subject_key,
                predicate="relation_reference",
                normalized_value=relation_type,
                matched_text=f"{metaclass_name} nutzt Relation {relation_type}",
            )
        )
    return records


def _extract_metamodel_function_claims(
    *,
    document: CollectedDocument,
    row: dict[str, object],
    index: int,
) -> list[ExtractedClaimRecord]:
    function_id = str(row.get("function_id") or f"bsm_function_{index}").strip()
    function_name = str(row.get("function_name") or row.get("name") or function_id).strip()
    subject_key = f"BSM.function.{_slugify(function_name) or _slugify(function_id) or f'function_{index}'}"
    records = [
        _build_metamodel_claim_record(
            document=document,
            anchor_value=subject_key,
            subject_key=subject_key,
            predicate="metamodel_function",
            normalized_value=function_name,
            matched_text=f"BSM Function {function_name}",
        )
    ]
    for label in _string_list(row.get("labels"))[:8]:
        records.append(
            _build_metamodel_claim_record(
                document=document,
                anchor_value=f"{subject_key}.label.{_slugify(label)}",
                subject_key=subject_key,
                predicate="function_label",
                normalized_value=label,
                matched_text=f"{function_name} hat Label {label}",
            )
        )
    for relation_type in _string_list(row.get("relation_types"))[:12]:
        records.append(
            _build_metamodel_claim_record(
                document=document,
                anchor_value=f"{subject_key}.relation.{_slugify(relation_type)}",
                subject_key=subject_key,
                predicate="relation_reference",
                normalized_value=relation_type,
                matched_text=f"{function_name} ist ueber {relation_type} angebunden",
            )
        )
    for question_key in _string_list(row.get("question_keys"))[:12]:
        records.append(
            _build_metamodel_claim_record(
                document=document,
                anchor_value=f"{subject_key}.question.{question_key}",
                subject_key=subject_key,
                predicate="question_reference",
                normalized_value=question_key,
                matched_text=f"{function_name} referenziert Frage {question_key}",
            )
        )
    return records


def _extract_metamodel_label_summary_claims(
    *,
    document: CollectedDocument,
    row: dict[str, object],
    index: int,
) -> list[ExtractedClaimRecord]:
    label = str(row.get("label") or f"label_{index}").strip()
    count = str(row.get("node_count") or row.get("count") or "0").strip()
    subject_key = f"MetaModel.label.{_slugify(label) or f'label_{index}'}"
    return [
        _build_metamodel_claim_record(
            document=document,
            anchor_value=subject_key,
            subject_key=subject_key,
            predicate="label_count",
            normalized_value=count,
            matched_text=f"Label {label} kommt {count} mal vor",
        )
    ]


def _build_claim_record(
    *,
    document: CollectedDocument,
    line_no: int,
    line_text: str,
    subject_key: str,
    predicate: str,
    section_path: str,
) -> ExtractedClaimRecord:
    normalized_value = _normalize_value(line_text)
    location = AuditLocation(
        snapshot_id=document.snapshot.snapshot_id,
        source_type=document.source_type,
        source_id=document.source_id,
        title=document.title,
        path_hint=document.path_hint,
        url=document.url,
        position=AuditPosition(
            anchor_kind="file_line_range" if document.source_type == "github_file" else "document_line_range",
            anchor_value=f"{document.source_id}#L{line_no}",
            section_path=section_path,
            line_start=line_no,
            line_end=line_no,
            snippet_hash=_sha256_text(line_text),
            content_hash=document.snapshot.content_hash,
        ),
        metadata={"matched_text": line_text},
    )
    claim = AuditClaimEntry(
        source_snapshot_id=document.snapshot.snapshot_id,
        source_type=document.source_type,
        source_id=document.source_id,
        subject_kind="object_property" if "." in subject_key else "object",
        subject_key=subject_key,
        predicate=predicate,
        normalized_value=normalized_value,
        scope_kind="project",
        scope_key="FINAI",
        confidence=0.78 if document.source_type == "github_file" else 0.72,
        fingerprint=f"{subject_key}|{predicate}|{normalized_value}|FINAI",
        evidence_location_ids=[location.location_id],
        metadata={
            "path_hint": document.path_hint,
            "title": document.title,
            "matched_text": line_text,
            "evidence_anchor_kind": location.position.anchor_kind if location.position is not None else None,
            "evidence_anchor_value": location.position.anchor_value if location.position is not None else None,
            "evidence_section_path": location.position.section_path if location.position is not None else None,
            "evidence_line_start": location.position.line_start if location.position is not None else None,
            "evidence_line_end": location.position.line_end if location.position is not None else None,
            "evidence_url": document.url,
        },
    )
    return ExtractedClaimRecord(
        claim=claim,
        evidence=ExtractedClaimEvidence(location=location, matched_text=line_text),
    )


def _build_metamodel_claim_record(
    *,
    document: CollectedDocument,
    anchor_value: str,
    subject_key: str,
    predicate: str,
    normalized_value: str,
    matched_text: str,
) -> ExtractedClaimRecord:
    location = AuditLocation(
        snapshot_id=document.snapshot.snapshot_id,
        source_type="metamodel",
        source_id=document.source_id,
        title=document.title,
        path_hint=document.path_hint,
        position=AuditPosition(
            anchor_kind="metamodel_key",
            anchor_value=anchor_value,
            section_path="current_dump",
            snippet_hash=_sha256_text(matched_text),
            content_hash=document.snapshot.content_hash,
        ),
        metadata={"matched_text": matched_text},
    )
    claim = AuditClaimEntry(
        source_snapshot_id=document.snapshot.snapshot_id,
        source_type="metamodel",
        source_id=document.source_id,
        subject_kind="process",
        subject_key=subject_key,
        predicate=predicate,
        normalized_value=_normalize_value(normalized_value),
        scope_kind="global",
        scope_key="FINAI",
        confidence=0.92,
        fingerprint=f"{subject_key}|{predicate}|{_normalize_value(normalized_value)}|FINAI",
        evidence_location_ids=[location.location_id],
        metadata={
            "title": document.title,
            "matched_text": matched_text,
            "path_hint": document.path_hint,
            "evidence_anchor_kind": location.position.anchor_kind if location.position is not None else None,
            "evidence_anchor_value": location.position.anchor_value if location.position is not None else None,
            "evidence_section_path": location.position.section_path if location.position is not None else None,
            "evidence_url": document.url,
        },
    )
    return ExtractedClaimRecord(
        claim=claim,
        evidence=ExtractedClaimEvidence(location=location, matched_text=matched_text),
    )


def _derive_subject_label(*, line: str, document: CollectedDocument) -> str:
    hint_texts = [
        line,
        document.title,
        str(document.path_hint or ""),
        str(document.source_id or ""),
        *_document_context_fragments(document=document),
    ]
    return _derive_subject_label_from_hints(hint_texts=hint_texts)


def _derive_subject_label_from_hints(*, hint_texts: list[str]) -> str:
    for keywords, subject in OBJECT_HINTS:
        if any(keyword in text.casefold() for text in hint_texts for keyword in keywords):
            return subject
    joined = " ".join(hint_texts)
    for token in SUBJECT_TOKEN_PATTERN.findall(joined):
        if token not in {"HTTP", "JSON", "UUID", "POST", "GET", "PUT", "DELETE"}:
            return token
    title_tokens = SUBJECT_TOKEN_PATTERN.findall(joined)
    if title_tokens:
        return title_tokens[0]
    path_hint = joined
    stem = path_hint.rsplit("/", 1)[-1].split(".", 1)[0]
    normalized = stem.replace("_", " ").replace("-", " ").strip().title().replace(" ", "")
    return normalized or "RepositoryArtifact"


def _extract_function_claim_records(
    *,
    document: CollectedDocument,
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    class_stack: list[str],
    lines: list[str],
) -> list[ExtractedClaimRecord]:
    line_start = int(getattr(node, "lineno", 1))
    line_end = int(getattr(node, "end_lineno", line_start))
    section_path = ".".join([*class_stack, node.name]) or node.name
    snippet = "\n".join(lines[max(0, line_start - 1) : line_end]).strip()
    decorator_labels = _decorator_labels(node=node)
    docstring = ast.get_docstring(node) or ""
    hint_texts = [
        node.name,
        *class_stack,
        section_path,
        " ".join(decorator_labels),
        docstring,
        snippet,
        document.title,
        str(document.path_hint or ""),
    ]
    subject = _derive_subject_label_from_hints(hint_texts=hint_texts)
    predicates = _function_claim_predicates(node=node, snippet=snippet)
    records: list[ExtractedClaimRecord] = []
    for subject_suffix, predicate in predicates:
        matched_text = _function_matched_text(
            section_path=section_path,
            snippet=snippet,
            predicate=predicate,
            decorator_labels=decorator_labels,
        )
        records.append(
            _build_structured_claim_record(
                document=document,
                subject_key=f"{subject}.{subject_suffix}",
                predicate=predicate,
                matched_text=matched_text,
                line_start=line_start,
                line_end=line_end,
                section_path=section_path,
                confidence=0.86,
                metadata={
                    "path_hint": document.path_hint,
                    "title": document.title,
                    "ast_extracted": True,
                    "class_stack": class_stack,
                    "decorators": decorator_labels,
                },
            )
        )
    for subject_key, predicate in _semantic_subclaim_specs(
        subject=subject,
        predicate_prefix="implemented",
        text_fragments=[section_path, docstring, snippet, " ".join(decorator_labels)],
    ):
        records.append(
            _build_structured_claim_record(
                document=document,
                subject_key=subject_key,
                predicate=predicate,
                matched_text=_function_matched_text(
                    section_path=section_path,
                    snippet=snippet,
                    predicate=predicate,
                    decorator_labels=decorator_labels,
                ),
                line_start=line_start,
                line_end=line_end,
                section_path=section_path,
                confidence=0.84,
                metadata={
                    "path_hint": document.path_hint,
                    "title": document.title,
                    "ast_extracted": True,
                    "class_stack": class_stack,
                    "decorators": decorator_labels,
                    "semantic_subclaim": True,
                },
            )
        )
    return records


def _function_claim_predicates(
    *,
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    snippet: str,
) -> list[tuple[str, str]]:
    lowered_name = node.name.casefold()
    lowered_snippet = snippet.casefold()
    predicates: list[tuple[str, str]] = []
    for decorator in node.decorator_list:
        if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute):
            route_method = decorator.func.attr.casefold()
            if route_method == "get":
                predicates.append(("read_path", "implemented_read"))
            elif route_method in {"post", "put", "patch", "delete"}:
                predicates.append(("write_path", "implemented_write"))
    if any(f"{verb}_" in lowered_name or lowered_name.startswith(verb) for verb in READ_VERBS):
        predicates.append(("read_path", "implemented_read"))
    if any(f"{verb}_" in lowered_name or lowered_name.startswith(verb) for verb in WRITE_VERBS):
        predicates.append(("write_path", "implemented_write"))
    if any(hint in lowered_name or hint in lowered_snippet for hint in LIFECYCLE_HINTS):
        predicates.append(("lifecycle", "implemented_lifecycle"))
    if any(hint in lowered_name or hint in lowered_snippet for hint in POLICY_HINTS):
        predicates.append(("policy", "implemented_policy"))
    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in predicates:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _typescript_claim_predicates(*, symbol_name: str, snippet: str) -> list[tuple[str, str]]:
    lowered_name = symbol_name.casefold()
    lowered_snippet = snippet.casefold()
    predicates: list[tuple[str, str]] = []
    if any(verb in lowered_name or verb in lowered_snippet for verb in READ_VERBS) or any(
        token in lowered_snippet for token in ("fetch(", ".get(", "queryclient", "load", "usequery")
    ):
        predicates.append(("read_path", "implemented_read"))
    if any(verb in lowered_name or verb in lowered_snippet for verb in WRITE_VERBS) or any(
        token in lowered_snippet for token in (".post(", ".put(", ".patch(", ".delete(", "mutate(", "usemutation")
    ):
        predicates.append(("write_path", "implemented_write"))
    if any(hint in lowered_snippet or hint in lowered_name for hint in LIFECYCLE_HINTS):
        predicates.append(("lifecycle", "implemented_lifecycle"))
    if any(hint in lowered_snippet or hint in lowered_name for hint in POLICY_HINTS):
        predicates.append(("policy", "implemented_policy"))
    return _dedupe_pairs(predicates)


def _config_claim_predicates(*, key_path: list[str], value: str) -> list[tuple[str, str]]:
    joined = ".".join(key_path).casefold()
    value_text = value.casefold()
    predicates: list[tuple[str, str]] = []
    if _contains_any(text=joined, hints=READ_VERBS) or READ_LINE_PATTERN.search(value_text):
        predicates.append(("read_path", "implemented_read"))
    if _contains_any(text=joined, hints=WRITE_VERBS) or WRITE_LINE_PATTERN.search(value_text):
        predicates.append(("write_path", "implemented_write"))
    if _contains_any(text=joined, hints=LIFECYCLE_HINTS) or LIFECYCLE_LINE_PATTERN.search(value_text):
        predicates.append(("lifecycle", "implemented_lifecycle"))
    if _contains_any(text=joined, hints=POLICY_HINTS) or POLICY_LINE_PATTERN.search(value_text):
        predicates.append(("policy", "implemented_policy"))
    return _dedupe_pairs(predicates)


def _function_matched_text(
    *,
    section_path: str,
    snippet: str,
    predicate: str,
    decorator_labels: list[str],
) -> str:
    headline = f"{section_path} [{predicate}]"
    if decorator_labels:
        headline = f"{headline} {' '.join(decorator_labels)}"
    normalized_snippet = _normalize_value(snippet.replace("\n", " "))
    return f"{headline}: {normalized_snippet}"


def _decorator_labels(*, node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    labels: list[str] = []
    for decorator in node.decorator_list:
        if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute):
            base = decorator.func.attr
            route_path = ""
            if decorator.args and isinstance(decorator.args[0], ast.Constant) and isinstance(decorator.args[0].value, str):
                route_path = str(decorator.args[0].value)
            labels.append(f"@{base}({route_path})" if route_path else f"@{base}")
        elif isinstance(decorator, ast.Name):
            labels.append(f"@{decorator.id}")
    return labels


def _build_structured_claim_record(
    *,
    document: CollectedDocument,
    subject_key: str,
    predicate: str,
    matched_text: str,
    line_start: int,
    line_end: int,
    section_path: str,
    confidence: float,
    metadata: dict[str, object],
) -> ExtractedClaimRecord:
    anchor_value = f"{document.source_id}#L{line_start}-L{line_end}"
    location = AuditLocation(
        snapshot_id=document.snapshot.snapshot_id,
        source_type=document.source_type,
        source_id=document.source_id,
        title=document.title,
        path_hint=document.path_hint,
        url=document.url,
        position=AuditPosition(
            anchor_kind="file_line_range",
            anchor_value=anchor_value,
            section_path=section_path,
            line_start=line_start,
            line_end=line_end,
            snippet_hash=_sha256_text(matched_text),
            content_hash=document.snapshot.content_hash,
        ),
        metadata={"matched_text": matched_text, **metadata},
    )
    normalized_value = _normalize_value(matched_text)
    claim = AuditClaimEntry(
        source_snapshot_id=document.snapshot.snapshot_id,
        source_type=document.source_type,
        source_id=document.source_id,
        subject_kind="object_property" if "." in subject_key else "object",
        subject_key=subject_key,
        predicate=predicate,
        normalized_value=normalized_value,
        scope_kind="project",
        scope_key="FINAI",
        confidence=confidence,
        fingerprint=f"{subject_key}|{predicate}|{normalized_value}|FINAI",
        evidence_location_ids=[location.location_id],
        metadata={
            **metadata,
            "matched_text": matched_text,
            "evidence_anchor_kind": location.position.anchor_kind if location.position is not None else None,
            "evidence_anchor_value": location.position.anchor_value if location.position is not None else None,
            "evidence_section_path": location.position.section_path if location.position is not None else None,
            "evidence_line_start": location.position.line_start if location.position is not None else None,
            "evidence_line_end": location.position.line_end if location.position is not None else None,
            "evidence_url": document.url,
        },
    )
    return ExtractedClaimRecord(
        claim=claim,
        evidence=ExtractedClaimEvidence(location=location, matched_text=matched_text),
    )


def _is_python_document(*, document: CollectedDocument) -> bool:
    path_hint = str(document.path_hint or document.source_id or "").casefold()
    return path_hint.endswith(".py")


def _is_typescript_document(*, document: CollectedDocument) -> bool:
    path_hint = str(document.path_hint or document.source_id or "").casefold()
    return path_hint.endswith((".ts", ".tsx", ".js", ".jsx"))


def _is_config_document(*, document: CollectedDocument) -> bool:
    path_hint = str(document.path_hint or document.source_id or "").casefold()
    return path_hint.endswith((".yml", ".yaml", ".json", ".toml", ".ini", ".cfg", ".conf", ".env"))


def _normalize_value(value: str) -> str:
    compact = " ".join(str(value or "").split())
    return compact[:180]


def _deduplicate_claim_records(*, records: list[ExtractedClaimRecord]) -> list[ExtractedClaimRecord]:
    unique: dict[str, ExtractedClaimRecord] = {}
    for record in records:
        unique.setdefault(record.claim.fingerprint, record)
    return list(unique.values())


def _sha256_text(value: str) -> str:
    import hashlib

    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _semantic_subclaim_records_for_line(
    *,
    document: CollectedDocument,
    line_no: int,
    line_text: str,
    section_path: str,
    subject: str,
    predicate_prefix: str,
    default_phase_key: str | None = None,
) -> list[ExtractedClaimRecord]:
    records: list[ExtractedClaimRecord] = []
    for subject_key, predicate in _semantic_subclaim_specs(
        subject=subject,
        predicate_prefix=predicate_prefix,
        text_fragments=[section_path, line_text],
        default_phase_key=default_phase_key,
    ):
        records.append(
            _build_claim_record(
                document=document,
                line_no=line_no,
                line_text=line_text,
                subject_key=subject_key,
                predicate=predicate,
                section_path=section_path,
            )
        )
    return records


def _semantic_subclaim_specs(
    *,
    subject: str,
    predicate_prefix: str,
    text_fragments: list[str],
    default_phase_key: str | None = None,
) -> list[tuple[str, str]]:
    combined = " ".join(fragment for fragment in text_fragments if fragment)
    specs: list[tuple[str, str]] = []
    if REVIEW_STATUS_PATTERN.search(combined):
        specs.append((f"{subject}.review_status", f"{predicate_prefix}_review_status"))
    if APPROVAL_POLICY_PATTERN.search(combined):
        specs.append((f"{subject}.approval_policy", f"{predicate_prefix}_approval_policy"))
    if SCOPE_POLICY_PATTERN.search(combined):
        specs.append((f"{subject}.scope_policy", f"{predicate_prefix}_scope_policy"))
    if PROCESS_LINE_PATTERN.search(combined):
        specs.append(("BSM.process", f"{predicate_prefix}_process"))
        phase_key = _extract_phase_key(text_fragments=text_fragments) or default_phase_key
        if phase_key:
            specs.append((f"BSM.phase.{phase_key}", f"{predicate_prefix}_phase_reference"))
            if _extract_phase_order(text_fragments=text_fragments) is not None:
                specs.append((f"BSM.phase.{phase_key}", f"{predicate_prefix}_phase_order"))
            if _extract_question_count(text_fragments=text_fragments) is not None:
                specs.append((f"BSM.phase.{phase_key}", f"{predicate_prefix}_question_count"))
            question_key = _extract_question_key(text_fragments=text_fragments)
            if question_key:
                specs.append((f"BSM.phase.{phase_key}.question.{question_key}", f"{predicate_prefix}_question_reference"))
        else:
            question_key = _extract_question_key(text_fragments=text_fragments)
            if question_key:
                specs.append((f"BSM.process.question.{question_key}", f"{predicate_prefix}_question_reference"))
    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in specs:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _extract_phase_key(*, text_fragments: list[str]) -> str | None:
    for fragment in text_fragments:
        match = PHASE_REFERENCE_PATTERN.search(fragment)
        if match is None:
            continue
        candidate = _slugify(_clean_reference_candidate(match.group("phase")))
        if candidate:
            return candidate
    return None


def _extract_question_key(*, text_fragments: list[str]) -> str | None:
    for fragment in text_fragments:
        match = QUESTION_REFERENCE_PATTERN.search(fragment)
        if match is None:
            continue
        candidate = _slugify(_clean_reference_candidate(match.group("question")))
        if candidate:
            return candidate
    return None


def _extract_phase_order(*, text_fragments: list[str]) -> str | None:
    for fragment in text_fragments:
        match = PHASE_ORDER_PATTERN.search(fragment)
        if match is None:
            continue
        value = str(match.group("order") or "").strip()
        if value:
            return value
    return None


def _extract_question_count(*, text_fragments: list[str]) -> str | None:
    for fragment in text_fragments:
        match = QUESTION_COUNT_PATTERN.search(fragment)
        if match is None:
            continue
        value = str(match.group("count") or match.group("count_named") or "").strip()
        if value:
            return value
    return None


def _phase_subject_key(*, phase_id: str, phase_name: str) -> str:
    phase_key = _slugify(phase_name) or _slugify(phase_id) or "unknown_phase"
    return f"BSM.phase.{phase_key}"


def _updated_config_stack(*, stack: list[tuple[int, str]], indent: int, key: str) -> list[tuple[int, str]]:
    trimmed = list(stack)
    while trimmed and trimmed[-1][0] >= int(indent):
        trimmed.pop()
    return [*trimmed, (int(indent), key)]


def _clean_config_key(value: str) -> str:
    return str(value or "").strip().strip("\"'").strip()


def _clean_config_value(value: str) -> str:
    text = str(value or "").strip().strip(",").strip()
    return text.strip("\"'")


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _dedupe_pairs(values: list[tuple[str, str]]) -> list[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    result: list[tuple[str, str]] = []
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _contains_any(*, text: str, hints: tuple[str, ...]) -> bool:
    lowered = str(text or "").casefold()
    return any(hint in lowered for hint in hints)


def _slugify(value: str) -> str:
    lowered = str(value or "").strip().casefold()
    collapsed = re.sub(r"[^a-z0-9]+", "_", lowered)
    return collapsed.strip("_")


def _clean_reference_candidate(value: str) -> str:
    candidate = str(value or "").strip()
    for stop_char in REFERENCE_STOP_CHARS:
        marker_index = candidate.find(stop_char)
        if marker_index > 0:
            candidate = candidate[:marker_index].strip(" -:,.")
            break
    lowered = candidate.casefold()
    for marker in REFERENCE_STOP_MARKERS:
        marker_index = lowered.find(marker)
        if marker_index > 0:
            candidate = candidate[:marker_index].strip(" -:,.")
            break
    return candidate.strip(" -:,.")


def _document_context_fragments(*, document: CollectedDocument) -> list[str]:
    raw_ancestors = document.metadata.get("ancestor_titles") if isinstance(document.metadata, dict) else None
    if not isinstance(raw_ancestors, list):
        return []
    return [str(item).strip() for item in raw_ancestors if str(item).strip()]


def _updated_heading_stack(*, stack: list[str], level: int, title: str) -> list[str]:
    normalized_level = max(1, int(level))
    trimmed = list(stack[: normalized_level - 1])
    return [*trimmed, title]


def _document_base_context(*, document: CollectedDocument) -> list[str]:
    return [*_document_context_fragments(document=document), document.title]


def _document_section_path(*, base_context: list[str], heading_stack: list[str]) -> str:
    normalized = [
        str(item).strip()
        for item in [*base_context, *heading_stack]
        if str(item).strip()
    ]
    return " > ".join(normalized)
