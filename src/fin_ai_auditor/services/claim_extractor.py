from __future__ import annotations

import ast
from dataclasses import dataclass, field
import json
import logging
import re
from typing import Callable, Final, cast

from fin_ai_auditor.domain.models import (
    AuditClaimEntry,
    AuditLocation,
    AuditPosition,
    ClaimAssertionStatus,
    ClaimSourceAuthority,
)
from fin_ai_auditor.services.claim_semantics import package_scope_key
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
    r"\b(lifecycle|promotion|histori)\b",
    re.IGNORECASE,
)
PROCESS_LINE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(bsm|phase|prozess|process)\b",
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
PHASE_COUNT_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(?P<count>\d{1,3})\s*(?:phases|phase|phasen)\b|\b(?:phase count|phasenzahl)\s*[:=]?\s*(?P<count_named>\d{1,3})\b",
    re.IGNORECASE,
)
SUBJECT_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(r"\b([A-Z][A-Za-z0-9_]{2,})\b")
BACKTICK_SUBJECT_PATTERN: Final[re.Pattern[str]] = re.compile(r"`(?P<token>[A-Za-z][A-Za-z0-9_.-]{2,})`")
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
    (("inputsource", "input source"), "InputSource"),
    (("prompt",), "Prompt"),
    (("metaclass",), "MetaClass"),
    (("confluence", "page"), "ConfluencePage"),
)
GENERIC_DOCUMENT_SUBJECT_TOKENS: Final[frozenset[str]] = frozenset(
    {
        "architektur",
        "architekturrichtlinien",
        "roadmap",
        "zielbild",
        "ziel",
        "vision",
        "produkt",
        "produktauftrag",
        "produktgrenzen",
        "datenmodell",
        "forensische",
        "entscheidungs",
        "retrieval",
        "readiness",
        "reifephasen",
        "empfohlene",
        "kategorien",
        "aufbau",
        "wirkung",
        "quellen",
        "quelllandschaft",
        "rollen",
        "zielkomponenten",
        "hauptdomänen",
        "hauptdomaenen",
        "startstand",
        "warum",
    }
)

READ_VERBS: Final[tuple[str, ...]] = ("get", "list", "load", "read", "fetch", "query", "find", "collect", "resolve")
WRITE_VERBS: Final[tuple[str, ...]] = ("create", "update", "delete", "write", "persist", "save", "upsert", "merge", "patch", "publish")
LIFECYCLE_HINTS: Final[tuple[str, ...]] = ("status", "lifecycle", "promotion", "review", "histori", "freigabe")
POLICY_HINTS: Final[tuple[str, ...]] = ("read_only", "readonly", "allowlist", "approval", "approve", "guard", "scope", "tenant", "policy", "contract")
PRIMARY_PATH_HINTS: Final[tuple[str, ...]] = ("primary", "main path", "canonical", "hauptpfad", "ssot")
SECONDARY_PATH_HINTS: Final[tuple[str, ...]] = ("secondary", "side path", "nebenpfad", "alternate", "alternative path")
FALLBACK_PATH_HINTS: Final[tuple[str, ...]] = ("fallback", "degrade")
COMPAT_PATH_HINTS: Final[tuple[str, ...]] = ("compat", "compatibility", "v1 path", "legacy api")
META_ANALYSIS_PATH_HINTS: Final[tuple[str, ...]] = (
    "claim_extractor.py",
    "claim_semantics.py",
    "consensus_detector.py",
    "finding_engine.py",
    "documentation_gap_detector.py",
    "gold_set_benchmark.py",
    "semantic_graph_service.py",
    "pipeline_service.py",
    "audit_service.py",
)
META_DOCUMENT_PATH_HINTS: Final[tuple[str, ...]] = (
    "architecture.md",
    "data-model.md",
    "delta-sync-and-resolution.md",
    "decision-packages-and-retrieval.md",
    "forensic-readiness-plan.md",
    "forensic-finding-classes.md",
    "product-scope.md",
    "roadmap.md",
    "target-picture.md",
)
META_ANALYSIS_TEXT_HINTS: Final[tuple[str, ...]] = (
    "subject_key=",
    "predicate=",
    "canonical_key=",
    "fingerprint=",
    "anchor_value=",
    "matched_text=",
    "specs.append(",
    "generated_by",
    "claim",
    "claims",
    "finding",
    "findings",
    "consensus",
    "semantic",
    "detector",
    "gold set",
    "reference case",
    "package_scope_key",
    "documented_process",
    "implemented_process",
    "phase_source",
    "phase_scope",
    "bsmphase",
    "bsmquestion",
    "metamodell",
    "claim-schicht",
    "semantik-scope",
    "progress",
    "analysis_log",
    "current_activity",
    "detail=",
    "demo_",
)
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
DB_WRITE_CALL_HINTS: Final[tuple[str, ...]] = (
    "save",
    "persist",
    "upsert",
    "merge",
    "patch",
    "write",
    "create",
    "create_node",
    "create_relationship",
    "merge_relationship",
    "execute_query",
    "execute_write",
    "write_transaction",
    "run",
    "executemany",
    "bulk_write",
)
REPOSITORY_ADAPTER_HINTS: Final[tuple[str, ...]] = ("repo", "repository", "dao", "store", "adapter")
DRIVER_ADAPTER_HINTS: Final[tuple[str, ...]] = ("driver", "session", "tx", "transaction", "neo4j", "graph", "client")
TRANSACTION_HINTS: Final[tuple[str, ...]] = ("session", "transaction", "tx", "execute_write", "write_transaction", "begin_transaction")
RETRY_HINTS: Final[tuple[str, ...]] = ("retry", "backoff", "tenacity")
BATCH_HINTS: Final[tuple[str, ...]] = ("batch", "bulk", "chunk", "chunks", "records", "items", "executemany")
PYTHON_BUILTIN_TYPE_HINTS: Final[tuple[str, ...]] = (
    "Any",
    "Annotated",
    "AsyncIterator",
    "Awaitable",
    "Callable",
    "Dict",
    "Final",
    "Generic",
    "Iterable",
    "Iterator",
    "List",
    "Literal",
    "Mapping",
    "Optional",
    "Protocol",
    "Sequence",
    "Self",
    "Set",
    "Tuple",
    "Type",
    "TypeAlias",
    "TypeGuard",
    "TypeVar",
    "Union",
    "bool",
    "bytes",
    "dict",
    "float",
    "frozenset",
    "int",
    "list",
    "object",
    "set",
    "str",
    "tuple",
)


@dataclass(slots=True)
class _PythonFunctionDescriptor:
    node: ast.FunctionDef | ast.AsyncFunctionDef
    class_stack: list[str]
    section_path: str
    decorator_labels: list[str]
    docstring: str
    source_id: str = ""
    module_name: str = ""
    descriptor_key: str = ""
    qualified_symbol: str = ""
    class_symbol: str = ""
    import_aliases: dict[str, str] = field(default_factory=dict)
    parameter_type_bindings: dict[str, str] = field(default_factory=dict)
    local_symbol_bindings: dict[str, str] = field(default_factory=dict)
    local_binding_expressions: dict[str, str] = field(default_factory=dict)
    direct_call_chains: list[str] = field(default_factory=list)
    local_callee_keys: list[str] = field(default_factory=list)
    string_literals: list[str] = field(default_factory=list)
    with_context_calls: list[str] = field(default_factory=list)
    loop_contexts: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _PythonFunctionStaticAnalysis:
    section_path: str
    static_call_graph_paths: list[str] = field(default_factory=list)
    static_call_graph_qualified_paths: list[str] = field(default_factory=list)
    repository_adapters: list[str] = field(default_factory=list)
    repository_adapter_symbols: list[str] = field(default_factory=list)
    driver_adapters: list[str] = field(default_factory=list)
    driver_adapter_symbols: list[str] = field(default_factory=list)
    transaction_boundaries: list[str] = field(default_factory=list)
    retry_paths: list[str] = field(default_factory=list)
    batch_paths: list[str] = field(default_factory=list)
    db_write_api_calls: list[str] = field(default_factory=list)
    db_write_api_symbols: list[str] = field(default_factory=list)
    persistence_operation_types: list[str] = field(default_factory=list)
    persistence_schema_targets: list[str] = field(default_factory=list)
    persistence_backends: list[str] = field(default_factory=list)
    constructor_injection_bindings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _SchemaCatalog:
    allowed_node_labels: set[str] = field(default_factory=set)
    observed_node_labels: set[str] = field(default_factory=set)
    allowed_relationship_types: set[str] = field(default_factory=set)
    observed_relationship_types: set[str] = field(default_factory=set)


@dataclass(slots=True)
class _PythonModuleContext:
    document: CollectedDocument
    tree: ast.AST
    module_name: str
    import_aliases: dict[str, str]
    class_symbols: dict[str, str]
    function_symbols: dict[str, str]


@dataclass(slots=True)
class _PythonRepoContext:
    modules_by_source_id: dict[str, _PythonModuleContext] = field(default_factory=dict)
    descriptor_keys_by_source_id: dict[str, list[str]] = field(default_factory=dict)
    descriptors_by_key: dict[str, _PythonFunctionDescriptor] = field(default_factory=dict)
    analyses_by_key: dict[str, _PythonFunctionStaticAnalysis] = field(default_factory=dict)
    class_attribute_bindings_by_class_symbol: dict[str, dict[str, str]] = field(default_factory=dict)
    function_key_by_symbol: dict[str, str] = field(default_factory=dict)
    method_key_by_symbol: dict[tuple[str, str], str] = field(default_factory=dict)
    function_symbols_by_name: dict[str, list[str]] = field(default_factory=dict)
    class_symbols_by_name: dict[str, list[str]] = field(default_factory=dict)
    class_base_symbols_by_symbol: dict[str, list[str]] = field(default_factory=dict)
    subclass_symbols_by_base_symbol: dict[str, list[str]] = field(default_factory=dict)
    interface_like_symbols: set[str] = field(default_factory=set)


def extract_claim_records(*, documents: list[CollectedDocument]) -> list[ExtractedClaimRecord]:
    logger.info("claim_extraction_start", extra={"event_name": "claim_extraction_start", "event_payload": {"document_count": len(documents)}})
    records: list[ExtractedClaimRecord] = []
    schema_catalog = _build_schema_catalog(documents=documents)
    python_repo_records_by_source_id = _build_python_repo_claim_records(
        documents=documents,
        schema_catalog=schema_catalog,
    )
    for document in documents:
        try:
            if document.source_type == "metamodel":
                records.extend(_extract_metamodel_claims(document=document))
                continue
            if document.source_type == "github_file":
                records.extend(
                    _extract_code_claims(
                        document=document,
                        schema_catalog=schema_catalog,
                        precomputed_python_records=python_repo_records_by_source_id,
                    )
                )
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


def _extract_code_claims(
    *,
    document: CollectedDocument,
    schema_catalog: _SchemaCatalog | None = None,
    precomputed_python_records: dict[str, list[ExtractedClaimRecord]] | None = None,
) -> list[ExtractedClaimRecord]:
    records: list[ExtractedClaimRecord] = []
    if _is_python_document(document=document):
        if precomputed_python_records is not None:
            records.extend(precomputed_python_records.get(document.source_id, []))
        else:
            records.extend(_extract_python_ast_claims(document=document, schema_catalog=schema_catalog))
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


def _extract_python_ast_claims(
    *,
    document: CollectedDocument,
    schema_catalog: _SchemaCatalog | None = None,
) -> list[ExtractedClaimRecord]:
    repo_context = _build_python_repo_context(documents=[document])
    return _extract_python_repo_claim_records_for_source(
        source_id=document.source_id,
        repo_context=repo_context,
        schema_catalog=schema_catalog,
    )


def _build_python_repo_claim_records(
    *,
    documents: list[CollectedDocument],
    schema_catalog: _SchemaCatalog | None,
) -> dict[str, list[ExtractedClaimRecord]]:
    repo_context = _build_python_repo_context(documents=documents)
    if not repo_context.modules_by_source_id:
        return {}
    return {
        source_id: _extract_python_repo_claim_records_for_source(
            source_id=source_id,
            repo_context=repo_context,
            schema_catalog=schema_catalog,
        )
        for source_id in repo_context.modules_by_source_id
    }


def _extract_python_repo_claim_records_for_source(
    *,
    source_id: str,
    repo_context: _PythonRepoContext,
    schema_catalog: _SchemaCatalog | None,
) -> list[ExtractedClaimRecord]:
    module_context = repo_context.modules_by_source_id.get(source_id)
    if module_context is None:
        return []
    lines = module_context.document.body.splitlines()
    records: list[ExtractedClaimRecord] = []
    descriptor_keys = repo_context.descriptor_keys_by_source_id.get(source_id, [])
    for descriptor_key in sorted(
        descriptor_keys,
        key=lambda item: int(getattr(repo_context.descriptors_by_key[item].node, "lineno", 0) or 0),
    ):
        descriptor = repo_context.descriptors_by_key[descriptor_key]
        records.extend(
            _extract_function_claim_records(
                document=module_context.document,
                node=descriptor.node,
                class_stack=descriptor.class_stack,
                lines=lines,
                function_analysis=repo_context.analyses_by_key.get(descriptor_key),
                descriptor=descriptor,
                schema_catalog=schema_catalog,
            )
        )
    return records


def _build_python_repo_context(*, documents: list[CollectedDocument]) -> _PythonRepoContext:
    repo_context = _PythonRepoContext()
    for document in documents:
        if document.source_type != "github_file" or not _is_python_document(document=document):
            continue
        try:
            tree = ast.parse(document.body)
        except SyntaxError:
            continue
        module_name = _module_name_from_document(document=document)
        import_aliases = _extract_python_import_aliases(tree=tree, module_name=module_name)
        class_symbols, function_symbols = _extract_python_module_symbols(tree=tree, module_name=module_name)
        module_context = _PythonModuleContext(
            document=document,
            tree=tree,
            module_name=module_name,
            import_aliases=import_aliases,
            class_symbols=class_symbols,
            function_symbols=function_symbols,
        )
        repo_context.modules_by_source_id[document.source_id] = module_context
        for class_name, class_symbol in class_symbols.items():
            repo_context.class_symbols_by_name.setdefault(class_name, []).append(class_symbol)
        for function_name, function_symbol in function_symbols.items():
            repo_context.function_symbols_by_name.setdefault(function_name, []).append(function_symbol)
        class_base_symbols, interface_like_symbols = _extract_python_class_relationships(
            tree=tree,
            module_context=module_context,
        )
        for class_symbol, base_symbols in class_base_symbols.items():
            repo_context.class_base_symbols_by_symbol[class_symbol] = _dedupe_preserve_order(base_symbols)
            for base_symbol in base_symbols:
                repo_context.subclass_symbols_by_base_symbol.setdefault(base_symbol, []).append(class_symbol)
        repo_context.interface_like_symbols.update(interface_like_symbols)

    for source_id, module_context in repo_context.modules_by_source_id.items():
        descriptors, class_attribute_bindings = _collect_repo_python_function_descriptors(module_context=module_context)
        repo_context.descriptor_keys_by_source_id[source_id] = list(descriptors.keys())
        repo_context.descriptors_by_key.update(descriptors)
        for descriptor in descriptors.values():
            repo_context.function_key_by_symbol[descriptor.qualified_symbol] = descriptor.descriptor_key
            if descriptor.class_symbol:
                repo_context.method_key_by_symbol[(descriptor.class_symbol, descriptor.node.name)] = descriptor.descriptor_key
            repo_context.function_symbols_by_name.setdefault(descriptor.node.name, []).append(descriptor.qualified_symbol)
        for class_symbol, bindings in class_attribute_bindings.items():
            existing_bindings = repo_context.class_attribute_bindings_by_class_symbol.setdefault(class_symbol, {})
            for attribute_name, binding_symbol in bindings.items():
                if attribute_name not in existing_bindings:
                    existing_bindings[attribute_name] = binding_symbol

    repo_context.class_symbols_by_name = {
        name: _dedupe_preserve_order(symbols)
        for name, symbols in repo_context.class_symbols_by_name.items()
    }
    repo_context.function_symbols_by_name = {
        name: _dedupe_preserve_order(symbols)
        for name, symbols in repo_context.function_symbols_by_name.items()
    }
    repo_context.subclass_symbols_by_base_symbol = {
        base_symbol: _dedupe_preserve_order(subclass_symbols)
        for base_symbol, subclass_symbols in repo_context.subclass_symbols_by_base_symbol.items()
    }

    analysis_cache: dict[str, _PythonFunctionStaticAnalysis] = {}
    stack: set[str] = set()

    def analyze(descriptor_key: str) -> _PythonFunctionStaticAnalysis:
        cached = analysis_cache.get(descriptor_key)
        if cached is not None:
            return cached
        descriptor = repo_context.descriptors_by_key.get(descriptor_key)
        if descriptor is None:
            return _PythonFunctionStaticAnalysis(section_path=descriptor_key)
        if descriptor_key in stack:
            return _PythonFunctionStaticAnalysis(section_path=descriptor.section_path)
        stack.add(descriptor_key)
        analysis = _analyze_repo_function_descriptor(
            descriptor=descriptor,
            repo_context=repo_context,
            resolve_analysis=analyze,
        )
        analysis_cache[descriptor_key] = analysis
        stack.remove(descriptor_key)
        return analysis

    for descriptor_key in repo_context.descriptors_by_key:
        analyze(descriptor_key)
    repo_context.analyses_by_key = analysis_cache
    return repo_context


def _module_name_from_document(*, document: CollectedDocument) -> str:
    raw_path = str(document.path_hint or document.source_id or document.title or "").strip().replace("\\", "/")
    raw_path = raw_path.split("?", 1)[0].split("#", 1)[0]
    if raw_path.endswith(".py"):
        raw_path = raw_path[:-3]
    parts = [part for part in raw_path.split("/") if part and part not in {".", ".."}]
    if parts[:1] == ["src"]:
        parts = parts[1:]
    if parts[-1:] == ["__init__"]:
        parts = parts[:-1]
    normalized_parts = [re.sub(r"[^A-Za-z0-9_]", "_", part) for part in parts]
    normalized_parts = [part for part in normalized_parts if part]
    return ".".join(normalized_parts) or "runtime_module"


def _extract_python_import_aliases(*, tree: ast.AST, module_name: str) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for child in getattr(tree, "body", []):
        if isinstance(child, ast.Import):
            for alias in child.names:
                imported_name = str(alias.name or "").strip()
                if not imported_name:
                    continue
                alias_name = str(alias.asname or imported_name.split(".", 1)[0]).strip()
                target = imported_name if alias.asname else alias_name
                aliases[alias_name] = target
        elif isinstance(child, ast.ImportFrom):
            base_module = _resolve_relative_import_module(
                module_name=module_name,
                imported_module=str(child.module or "").strip(),
                level=int(getattr(child, "level", 0) or 0),
            )
            for alias in child.names:
                imported_name = str(alias.name or "").strip()
                if not imported_name or imported_name == "*":
                    continue
                alias_name = str(alias.asname or imported_name).strip()
                if not alias_name:
                    continue
                target = f"{base_module}.{imported_name}" if base_module else imported_name
                aliases[alias_name] = target
    return aliases


def _resolve_relative_import_module(*, module_name: str, imported_module: str, level: int) -> str:
    if level <= 0:
        return imported_module
    module_parts = [part for part in module_name.split(".") if part]
    if level <= len(module_parts):
        module_parts = module_parts[:-level]
    else:
        module_parts = []
    imported_parts = [part for part in imported_module.split(".") if part]
    return ".".join([*module_parts, *imported_parts])


def _extract_python_module_symbols(*, tree: ast.AST, module_name: str) -> tuple[dict[str, str], dict[str, str]]:
    class_symbols: dict[str, str] = {}
    function_symbols: dict[str, str] = {}
    for child in getattr(tree, "body", []):
        if isinstance(child, ast.ClassDef):
            class_symbols[child.name] = f"{module_name}.{child.name}" if module_name else child.name
        elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            function_symbols[child.name] = f"{module_name}.{child.name}" if module_name else child.name
    return class_symbols, function_symbols


def _extract_python_class_relationships(
    *,
    tree: ast.AST,
    module_context: _PythonModuleContext,
) -> tuple[dict[str, list[str]], set[str]]:
    class_base_symbols: dict[str, list[str]] = {}
    interface_like_symbols: set[str] = set()

    def visit(node: ast.AST, *, class_stack: list[str]) -> None:
        if isinstance(node, ast.ClassDef):
            next_stack = [*class_stack, node.name]
            class_symbol = _class_symbol_from_stack(
                module_name=module_context.module_name,
                class_stack=next_stack,
                class_symbols=module_context.class_symbols,
            )
            base_symbols: list[str] = []
            interface_like = node.name.endswith(("Protocol", "Interface"))
            for base in node.bases:
                base_label = _call_chain_from_expr(expr=base) if isinstance(base, ast.expr) else ""
                if base_label in {"Protocol", "typing.Protocol", "ABC", "abc.ABC", "ABCMeta", "abc.ABCMeta"}:
                    interface_like = True
                resolved_base_symbol = _annotation_symbol(annotation=base, module_context=module_context)
                if resolved_base_symbol:
                    base_symbols.append(resolved_base_symbol)
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if any(
                        decorator == "abstractmethod" or decorator.endswith(".abstractmethod")
                        for decorator in _decorator_labels(node=child)
                    ):
                        interface_like = True
                elif isinstance(child, ast.Pass):
                    interface_like = True
            class_base_symbols[class_symbol] = _dedupe_preserve_order(base_symbols)
            if interface_like:
                interface_like_symbols.add(class_symbol)
            for child in node.body:
                visit(child, class_stack=next_stack)
            return
        for descendant in ast.iter_child_nodes(node):
            if isinstance(descendant, ast.ClassDef):
                visit(descendant, class_stack=class_stack)

    for child in getattr(tree, "body", []):
        visit(child, class_stack=[])
    return class_base_symbols, interface_like_symbols


def _collect_repo_python_function_descriptors(
    *,
    module_context: _PythonModuleContext,
) -> tuple[dict[str, _PythonFunctionDescriptor], dict[str, dict[str, str]]]:
    descriptors: dict[str, _PythonFunctionDescriptor] = {}
    class_attribute_bindings: dict[str, dict[str, str]] = {}

    def visit(node: ast.AST, *, class_stack: list[str]) -> None:
        if isinstance(node, ast.ClassDef):
            next_stack = [*class_stack, node.name]
            class_symbol = _class_symbol_from_stack(
                module_name=module_context.module_name,
                class_stack=next_stack,
                class_symbols=module_context.class_symbols,
            )
            class_attribute_bindings[class_symbol] = _extract_repo_class_attribute_bindings(
                node=node,
                module_context=module_context,
                class_symbol=class_symbol,
            )
            for child in node.body:
                visit(child, class_stack=next_stack)
            return
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            section_path = ".".join([*class_stack, node.name]) or node.name
            class_symbol = _class_symbol_from_stack(
                module_name=module_context.module_name,
                class_stack=class_stack,
                class_symbols=module_context.class_symbols,
            )
            qualified_symbol = (
                f"{class_symbol}.{node.name}"
                if class_symbol
                else module_context.function_symbols.get(node.name, f"{module_context.module_name}.{node.name}")
            )
            descriptor = _PythonFunctionDescriptor(
                node=node,
                class_stack=list(class_stack),
                section_path=section_path,
                decorator_labels=_decorator_labels(node=node),
                docstring=ast.get_docstring(node) or "",
                source_id=module_context.document.source_id,
                module_name=module_context.module_name,
                descriptor_key=qualified_symbol,
                qualified_symbol=qualified_symbol,
                class_symbol=class_symbol,
                import_aliases=dict(module_context.import_aliases),
                parameter_type_bindings=_extract_parameter_type_bindings(
                    node=node,
                    module_context=module_context,
                ),
            )
            (
                descriptor.local_symbol_bindings,
                descriptor.local_binding_expressions,
            ) = _extract_function_local_symbol_bindings(
                node=node,
                module_context=module_context,
                class_symbol=class_symbol,
                class_attribute_bindings=class_attribute_bindings.get(class_symbol, {}),
                parameter_type_bindings=descriptor.parameter_type_bindings,
            )
            direct_call_chains: list[str] = []
            string_literals: list[str] = []
            with_context_calls: list[str] = []
            loop_contexts: list[str] = []
            for descendant in ast.walk(node):
                if isinstance(descendant, ast.Call):
                    call_chain = _call_chain_from_expr(expr=descendant.func)
                    if call_chain:
                        direct_call_chains.append(call_chain)
                elif isinstance(descendant, (ast.With, ast.AsyncWith)):
                    for item in descendant.items:
                        context_chain = _call_chain_from_context_expr(expr=item.context_expr)
                        if context_chain:
                            with_context_calls.append(context_chain)
                elif isinstance(descendant, (ast.For, ast.AsyncFor)):
                    loop_contexts.append(_loop_context_text(node=descendant))
                elif isinstance(descendant, ast.Constant) and isinstance(descendant.value, str):
                    string_literals.append(descendant.value)
            descriptor.direct_call_chains = _dedupe_preserve_order(direct_call_chains)
            descriptor.string_literals = _dedupe_preserve_order(string_literals)
            descriptor.with_context_calls = _dedupe_preserve_order(with_context_calls)
            descriptor.loop_contexts = _dedupe_preserve_order(loop_contexts)
            descriptors[descriptor.descriptor_key] = descriptor
            return
        for descendant in ast.iter_child_nodes(node):
            if isinstance(descendant, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                visit(descendant, class_stack=class_stack)

    for child in getattr(module_context.tree, "body", []):
        visit(child, class_stack=[])
    return descriptors, class_attribute_bindings


def _class_symbol_from_stack(
    *,
    module_name: str,
    class_stack: list[str],
    class_symbols: dict[str, str],
) -> str:
    if not class_stack:
        return ""
    if len(class_stack) == 1 and class_stack[0] in class_symbols:
        return class_symbols[class_stack[0]]
    return ".".join([module_name, *class_stack]) if module_name else ".".join(class_stack)


def _extract_parameter_type_bindings(
    *,
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    module_context: _PythonModuleContext,
) -> dict[str, str]:
    bindings: dict[str, str] = {}
    args = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
    if node.args.vararg is not None:
        args.append(node.args.vararg)
    if node.args.kwarg is not None:
        args.append(node.args.kwarg)
    for argument in args:
        annotation_symbol = _annotation_symbol(annotation=argument.annotation, module_context=module_context)
        if annotation_symbol:
            bindings[argument.arg] = annotation_symbol
    return bindings


def _extract_function_local_symbol_bindings(
    *,
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    module_context: _PythonModuleContext,
    class_symbol: str,
    class_attribute_bindings: dict[str, str],
    parameter_type_bindings: dict[str, str],
) -> tuple[dict[str, str], dict[str, str]]:
    local_symbol_bindings: dict[str, str] = {}
    local_binding_expressions: dict[str, str] = {}
    for statement in _iter_statements_in_order(statements=list(node.body)):
        if isinstance(statement, ast.Assign):
            binding_expression = _binding_expression_from_value(value=statement.value)
            if not binding_expression:
                continue
            resolved_symbol = _resolve_local_binding_expression(
                binding_expression=binding_expression,
                module_context=module_context,
                class_symbol=class_symbol,
                class_attribute_bindings=class_attribute_bindings,
                parameter_type_bindings=parameter_type_bindings,
                local_symbol_bindings=local_symbol_bindings,
            )
            for target_name in _assignment_target_names(targets=statement.targets):
                local_binding_expressions[target_name] = binding_expression
                if resolved_symbol:
                    local_symbol_bindings[target_name] = resolved_symbol
        elif isinstance(statement, ast.AnnAssign):
            binding_expression = _binding_expression_from_value(value=statement.value)
            annotation_symbol = _annotation_symbol(annotation=statement.annotation, module_context=module_context)
            target_name = _target_name_from_expr(expr=statement.target)
            if not target_name:
                continue
            if binding_expression:
                local_binding_expressions[target_name] = binding_expression
            resolved_symbol = (
                _resolve_local_binding_expression(
                    binding_expression=binding_expression,
                    module_context=module_context,
                    class_symbol=class_symbol,
                    class_attribute_bindings=class_attribute_bindings,
                    parameter_type_bindings=parameter_type_bindings,
                    local_symbol_bindings=local_symbol_bindings,
                )
                if binding_expression
                else None
            ) or annotation_symbol
            if resolved_symbol:
                local_symbol_bindings[target_name] = resolved_symbol
        elif isinstance(statement, (ast.With, ast.AsyncWith)):
            for item in statement.items:
                target_names = _optional_var_target_names(expr=item.optional_vars)
                if not target_names:
                    continue
                binding_expression = _binding_expression_from_context_expr(expr=item.context_expr)
                if not binding_expression:
                    continue
                resolved_symbol = _resolve_local_binding_expression(
                    binding_expression=binding_expression,
                    module_context=module_context,
                    class_symbol=class_symbol,
                    class_attribute_bindings=class_attribute_bindings,
                    parameter_type_bindings=parameter_type_bindings,
                    local_symbol_bindings=local_symbol_bindings,
                )
                for target_name in target_names:
                    local_binding_expressions[target_name] = binding_expression
                    if resolved_symbol:
                        local_symbol_bindings[target_name] = resolved_symbol
    return local_symbol_bindings, local_binding_expressions


def _iter_statements_in_order(*, statements: list[ast.stmt]) -> list[ast.stmt]:
    ordered: list[ast.stmt] = []
    for statement in statements:
        ordered.append(statement)
        nested_blocks = (
            getattr(statement, "body", None),
            getattr(statement, "orelse", None),
            getattr(statement, "finalbody", None),
        )
        for nested in nested_blocks:
            if isinstance(nested, list):
                ordered.extend(_iter_statements_in_order(statements=[item for item in nested if isinstance(item, ast.stmt)]))
        handlers = getattr(statement, "handlers", None)
        if isinstance(handlers, list):
            for handler in handlers:
                if isinstance(handler, ast.ExceptHandler):
                    ordered.extend(_iter_statements_in_order(statements=[item for item in handler.body if isinstance(item, ast.stmt)]))
    return ordered


def _assignment_target_names(*, targets: list[ast.expr]) -> list[str]:
    names: list[str] = []
    for target in targets:
        names.extend(_optional_var_target_names(expr=target))
    return _dedupe_preserve_order(names)


def _optional_var_target_names(*, expr: ast.expr | None) -> list[str]:
    if isinstance(expr, ast.Name):
        return [expr.id]
    if isinstance(expr, (ast.Tuple, ast.List)):
        names: list[str] = []
        for element in expr.elts:
            names.extend(_optional_var_target_names(expr=element))
        return _dedupe_preserve_order(names)
    return []


def _target_name_from_expr(*, expr: ast.expr) -> str:
    if isinstance(expr, ast.Name):
        return expr.id
    return ""


def _binding_expression_from_context_expr(*, expr: ast.expr) -> str:
    if isinstance(expr, ast.Call):
        return _binding_expression_from_value(value=expr)
    return _binding_expression_from_value(value=expr)


def _binding_expression_from_value(*, value: ast.expr | None) -> str:
    if value is None:
        return ""
    if isinstance(value, ast.Constant):
        return ""
    if isinstance(value, ast.Name):
        return value.id
    if isinstance(value, ast.Attribute):
        return _call_chain_from_expr(expr=value)
    if isinstance(value, ast.Call):
        return _call_chain_from_expr(expr=value.func)
    if isinstance(value, ast.BoolOp):
        for inner in value.values:
            binding_expression = _binding_expression_from_value(value=inner)
            if binding_expression:
                return binding_expression
        return ""
    if isinstance(value, ast.IfExp):
        return _binding_expression_from_value(value=value.body) or _binding_expression_from_value(value=value.orelse)
    return ""


def _resolve_local_binding_expression(
    *,
    binding_expression: str,
    module_context: _PythonModuleContext,
    class_symbol: str,
    class_attribute_bindings: dict[str, str],
    parameter_type_bindings: dict[str, str],
    local_symbol_bindings: dict[str, str],
) -> str | None:
    normalized = str(binding_expression or "").strip()
    if not normalized:
        return None
    qualified = _qualify_local_binding_chain(
        call_chain=normalized,
        module_context=module_context,
        class_symbol=class_symbol,
        class_attribute_bindings=class_attribute_bindings,
        parameter_type_bindings=parameter_type_bindings,
        local_symbol_bindings=local_symbol_bindings,
    )
    if not qualified:
        return None
    base_symbol, separator, method_name = qualified.rpartition(".")
    if separator and method_name.casefold() in TRANSACTION_HINTS and base_symbol:
        return base_symbol
    followed_binding = _follow_local_attribute_binding(
        qualified_chain=qualified,
        class_attribute_bindings=class_attribute_bindings,
    )
    return followed_binding or qualified


def _qualify_local_binding_chain(
    *,
    call_chain: str,
    module_context: _PythonModuleContext,
    class_symbol: str,
    class_attribute_bindings: dict[str, str],
    parameter_type_bindings: dict[str, str],
    local_symbol_bindings: dict[str, str],
) -> str:
    normalized = str(call_chain or "").strip()
    if not normalized:
        return ""
    if normalized.startswith("self.") and class_symbol:
        remainder = normalized[len("self.") :]
        attribute_name, separator, tail = remainder.partition(".")
        binding_symbol = class_attribute_bindings.get(attribute_name)
        if binding_symbol:
            return binding_symbol if not separator else f"{binding_symbol}.{tail}"
        return f"{class_symbol}.{remainder}"
    if normalized.startswith("cls.") and class_symbol:
        remainder = normalized[len("cls.") :]
        return f"{class_symbol}.{remainder}"
    if "." not in normalized:
        return (
            local_symbol_bindings.get(normalized)
            or parameter_type_bindings.get(normalized)
            or _known_module_symbol(
                symbol_name=normalized,
                module_context=module_context,
                allow_uppercase_fallback=True,
            )
            or normalized
        )
    root, _, tail = normalized.partition(".")
    resolved_root = (
        local_symbol_bindings.get(root)
        or parameter_type_bindings.get(root)
        or _known_module_symbol(
            symbol_name=root,
            module_context=module_context,
            allow_uppercase_fallback=True,
        )
        or root
    )
    qualified = f"{resolved_root}.{tail}"
    followed_binding = _follow_local_attribute_binding(
        qualified_chain=qualified,
        class_attribute_bindings=class_attribute_bindings,
    )
    return followed_binding or qualified


def _follow_local_attribute_binding(
    *,
    qualified_chain: str,
    class_attribute_bindings: dict[str, str],
) -> str | None:
    parts = [part for part in str(qualified_chain or "").split(".") if part]
    if len(parts) < 2:
        return None
    for split_index in range(len(parts) - 1, 0, -1):
        owner_parts = parts[:split_index]
        attribute_parts = parts[split_index:]
        if not owner_parts or not attribute_parts:
            continue
        attribute_name = owner_parts[-1]
        resolved_root = class_attribute_bindings.get(attribute_name)
        if resolved_root:
            remaining = ".".join(attribute_parts)
            return f"{resolved_root}.{remaining}" if remaining else resolved_root
    return None


def _annotation_symbol(*, annotation: ast.AST | None, module_context: _PythonModuleContext) -> str | None:
    if annotation is None:
        return None
    if isinstance(annotation, ast.Name):
        if annotation.id in PYTHON_BUILTIN_TYPE_HINTS:
            return None
        return _known_module_symbol(
            symbol_name=annotation.id,
            module_context=module_context,
            allow_uppercase_fallback=True,
        )
    if isinstance(annotation, ast.Attribute):
        return _known_module_symbol(
            symbol_name=_call_chain_from_expr(expr=annotation),
            module_context=module_context,
            allow_uppercase_fallback=True,
        )
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        return _known_module_symbol(
            symbol_name=annotation.value,
            module_context=module_context,
            allow_uppercase_fallback=True,
        )
    if isinstance(annotation, ast.Subscript):
        slice_value = getattr(annotation, "slice", None)
        return _annotation_symbol(annotation=slice_value, module_context=module_context) or _annotation_symbol(
            annotation=annotation.value,
            module_context=module_context,
        )
    if isinstance(annotation, ast.BinOp) and isinstance(annotation.op, ast.BitOr):
        return _annotation_symbol(annotation=annotation.left, module_context=module_context) or _annotation_symbol(
            annotation=annotation.right,
            module_context=module_context,
        )
    if isinstance(annotation, ast.Tuple):
        for element in annotation.elts:
            resolved = _annotation_symbol(annotation=element, module_context=module_context)
            if resolved:
                return resolved
        return None
    return None


def _known_module_symbol(
    *,
    symbol_name: str,
    module_context: _PythonModuleContext,
    allow_uppercase_fallback: bool = False,
) -> str | None:
    normalized = str(symbol_name or "").strip().strip("'\"")
    if not normalized or normalized in PYTHON_BUILTIN_TYPE_HINTS:
        return None
    if normalized in module_context.import_aliases:
        return module_context.import_aliases[normalized]
    if normalized in module_context.class_symbols:
        return module_context.class_symbols[normalized]
    if normalized in module_context.function_symbols:
        return module_context.function_symbols[normalized]
    if "." in normalized:
        root, _, tail = normalized.partition(".")
        resolved_root = _known_module_symbol(
            symbol_name=root,
            module_context=module_context,
            allow_uppercase_fallback=allow_uppercase_fallback,
        )
        if resolved_root:
            return f"{resolved_root}.{tail}"
        return normalized
    if allow_uppercase_fallback and normalized[:1].isupper():
        return f"{module_context.module_name}.{normalized}" if module_context.module_name else normalized
    return None


def _extract_repo_class_attribute_bindings(
    *,
    node: ast.ClassDef,
    module_context: _PythonModuleContext,
    class_symbol: str,
) -> dict[str, str]:
    del class_symbol
    bindings: dict[str, str] = {}
    for child in node.body:
        if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) or child.name != "__init__":
            continue
        parameter_type_bindings = _extract_parameter_type_bindings(node=child, module_context=module_context)
        for inner in ast.walk(child):
            if isinstance(inner, ast.Assign):
                binding = _repo_self_attribute_binding(
                    assign_targets=inner.targets,
                    value=inner.value,
                    parameter_type_bindings=parameter_type_bindings,
                    module_context=module_context,
                )
                if binding is not None:
                    attribute_name, binding_symbol = binding
                    bindings[attribute_name] = binding_symbol
            elif isinstance(inner, ast.AnnAssign):
                binding = _repo_self_attribute_binding(
                    assign_targets=[inner.target],
                    value=inner.value,
                    parameter_type_bindings=parameter_type_bindings,
                    module_context=module_context,
                    explicit_annotation=inner.annotation,
                )
                if binding is not None:
                    attribute_name, binding_symbol = binding
                    bindings[attribute_name] = binding_symbol
    return bindings


def _repo_self_attribute_binding(
    *,
    assign_targets: list[ast.expr],
    value: ast.expr | None,
    parameter_type_bindings: dict[str, str],
    module_context: _PythonModuleContext,
    explicit_annotation: ast.AST | None = None,
) -> tuple[str, str] | None:
    for target in assign_targets:
        if not isinstance(target, ast.Attribute) or not isinstance(target.value, ast.Name) or target.value.id != "self":
            continue
        attribute_name = str(target.attr or "").strip()
        if not attribute_name:
            continue
        binding_symbol = _binding_symbol_from_value(
            value=value,
            parameter_type_bindings=parameter_type_bindings,
            module_context=module_context,
        ) or _annotation_symbol(annotation=explicit_annotation, module_context=module_context)
        if binding_symbol:
            return attribute_name, binding_symbol
    return None


def _binding_symbol_from_value(
    *,
    value: ast.expr | None,
    parameter_type_bindings: dict[str, str],
    module_context: _PythonModuleContext,
) -> str | None:
    if value is None:
        return None
    if isinstance(value, ast.Name):
        return parameter_type_bindings.get(value.id) or _known_module_symbol(
            symbol_name=value.id,
            module_context=module_context,
            allow_uppercase_fallback=False,
        )
    if isinstance(value, ast.Call):
        return _known_module_symbol(
            symbol_name=_call_chain_from_expr(expr=value.func),
            module_context=module_context,
            allow_uppercase_fallback=True,
        )
    if isinstance(value, ast.Attribute):
        return _known_module_symbol(
            symbol_name=_call_chain_from_expr(expr=value),
            module_context=module_context,
            allow_uppercase_fallback=False,
        )
    if isinstance(value, ast.BoolOp):
        for inner in value.values:
            resolved = _binding_symbol_from_value(
                value=inner,
                parameter_type_bindings=parameter_type_bindings,
                module_context=module_context,
            )
            if resolved:
                return resolved
        return None
    if isinstance(value, ast.IfExp):
        return _binding_symbol_from_value(
            value=value.body,
            parameter_type_bindings=parameter_type_bindings,
            module_context=module_context,
        ) or _binding_symbol_from_value(
            value=value.orelse,
            parameter_type_bindings=parameter_type_bindings,
            module_context=module_context,
        )
    return None


def _analyze_repo_function_descriptor(
    *,
    descriptor: _PythonFunctionDescriptor,
    repo_context: _PythonRepoContext,
    resolve_analysis: Callable[[str], _PythonFunctionStaticAnalysis],
) -> _PythonFunctionStaticAnalysis:
    resolved_local_symbol_bindings = _resolve_descriptor_local_symbol_bindings(
        descriptor=descriptor,
        repo_context=repo_context,
    )
    normalized_calls = _dedupe_preserve_order(
        [
            _qualify_repo_call_chain(
                call_chain=call_chain,
                descriptor=descriptor,
                repo_context=repo_context,
                local_symbol_bindings=resolved_local_symbol_bindings,
            )
            for call_chain in descriptor.direct_call_chains
            if str(call_chain).strip()
        ]
    )
    normalized_contexts = _dedupe_preserve_order(
        [
            _qualify_repo_call_chain(
                call_chain=context_call,
                descriptor=descriptor,
                repo_context=repo_context,
                local_symbol_bindings=resolved_local_symbol_bindings,
            )
            for context_call in descriptor.with_context_calls
            if str(context_call).strip()
        ]
    )
    repository_adapter_symbols = _dedupe_preserve_order(
        [
            adapter
            for call_chain in normalized_calls
            for adapter in _adapter_symbols_from_call_chain(
                call_chain=call_chain,
                adapter_hints=REPOSITORY_ADAPTER_HINTS,
                repo_context=repo_context,
            )
        ]
    )
    driver_adapter_symbols = _dedupe_preserve_order(
        [
            adapter
            for call_chain in [*normalized_calls, *normalized_contexts]
            for adapter in _adapter_symbols_from_call_chain(
                call_chain=call_chain,
                adapter_hints=DRIVER_ADAPTER_HINTS,
                repo_context=repo_context,
            )
        ]
    )
    repository_adapters = _dedupe_preserve_order([_display_adapter_symbol(symbol) for symbol in repository_adapter_symbols])
    driver_adapters = _dedupe_preserve_order([_display_adapter_symbol(symbol) for symbol in driver_adapter_symbols])
    db_write_api_symbols = _dedupe_preserve_order(
        [
            call_chain
            for call_chain in normalized_calls
            if _call_chain_is_db_write(call_chain=call_chain)
        ]
    )
    db_write_api_calls = _dedupe_preserve_order([_display_call_chain(call_chain) for call_chain in db_write_api_symbols])
    transaction_boundary_symbols = _dedupe_preserve_order(
        [
            boundary
            for boundary in [*normalized_calls, *normalized_contexts]
            if _contains_any(text=boundary.casefold(), hints=TRANSACTION_HINTS)
        ]
    )
    transaction_boundaries = _dedupe_preserve_order(
        [_display_call_chain(boundary) for boundary in transaction_boundary_symbols]
    )
    retry_paths = _dedupe_preserve_order(
        [
            retry_path
            for retry_path in [
                *descriptor.decorator_labels,
                *[_display_call_chain(call_chain) for call_chain in normalized_calls],
            ]
            if _contains_any(text=retry_path.casefold(), hints=RETRY_HINTS)
        ]
    )
    batch_paths = _dedupe_preserve_order(
        [
            *descriptor.loop_contexts,
            *[
                _display_call_chain(path)
                for path in normalized_calls
                if _contains_any(text=path.casefold(), hints=BATCH_HINTS)
            ],
        ]
    )
    constructor_injection_bindings = _dedupe_preserve_order(
        [
            f"{attribute_name}={binding_symbol}"
            for attribute_name, binding_symbol in sorted(
                repo_context.class_attribute_bindings_by_class_symbol.get(descriptor.class_symbol, {}).items()
            )
        ]
    )
    operation_types, schema_targets, backends = _infer_persistence_shape(
        call_chains=normalized_calls,
        string_literals=descriptor.string_literals,
        subject_key_hint=descriptor.qualified_symbol or descriptor.section_path,
    )
    descriptor_display = _display_symbol(descriptor.qualified_symbol or descriptor.section_path)
    descriptor_qualified = descriptor.qualified_symbol or descriptor.section_path
    static_paths = _dedupe_preserve_order(
        [f"{descriptor_display} -> {_display_call_chain(call_chain)}" for call_chain in db_write_api_symbols]
    )
    qualified_paths = _dedupe_preserve_order(
        [f"{descriptor_qualified} -> {call_chain}" for call_chain in db_write_api_symbols]
    )

    resolved_callee_keys = _dedupe_preserve_order(
        [
            callee_key
            for call_chain in normalized_calls
            for callee_key in [_resolve_repo_callee_key(call_chain=call_chain, repo_context=repo_context)]
            if callee_key and callee_key != descriptor.descriptor_key
        ]
    )

    for callee_key in resolved_callee_keys:
        callee_descriptor = repo_context.descriptors_by_key.get(callee_key)
        if callee_descriptor is None:
            continue
        callee_analysis = resolve_analysis(callee_key)
        repository_adapters = _dedupe_preserve_order([*repository_adapters, *callee_analysis.repository_adapters])
        repository_adapter_symbols = _dedupe_preserve_order(
            [*repository_adapter_symbols, *callee_analysis.repository_adapter_symbols]
        )
        driver_adapters = _dedupe_preserve_order([*driver_adapters, *callee_analysis.driver_adapters])
        driver_adapter_symbols = _dedupe_preserve_order([*driver_adapter_symbols, *callee_analysis.driver_adapter_symbols])
        transaction_boundaries = _dedupe_preserve_order([*transaction_boundaries, *callee_analysis.transaction_boundaries])
        retry_paths = _dedupe_preserve_order([*retry_paths, *callee_analysis.retry_paths])
        batch_paths = _dedupe_preserve_order([*batch_paths, *callee_analysis.batch_paths])
        db_write_api_calls = _dedupe_preserve_order([*db_write_api_calls, *callee_analysis.db_write_api_calls])
        db_write_api_symbols = _dedupe_preserve_order([*db_write_api_symbols, *callee_analysis.db_write_api_symbols])
        operation_types = _dedupe_preserve_order([*operation_types, *callee_analysis.persistence_operation_types])
        schema_targets = _dedupe_preserve_order([*schema_targets, *callee_analysis.persistence_schema_targets])
        backends = _dedupe_preserve_order([*backends, *callee_analysis.persistence_backends])
        constructor_injection_bindings = _dedupe_preserve_order(
            [*constructor_injection_bindings, *callee_analysis.constructor_injection_bindings]
        )
        callee_display = _display_symbol(callee_descriptor.qualified_symbol or callee_descriptor.section_path)
        callee_qualified = callee_descriptor.qualified_symbol or callee_descriptor.section_path
        static_paths = _dedupe_preserve_order(
            [
                *static_paths,
                f"{descriptor_display} -> {callee_display}",
                *[
                    f"{descriptor_display} -> {path}"
                    if not path.startswith(f"{descriptor_display} ->")
                    else path
                    for path in callee_analysis.static_call_graph_paths
                ],
            ]
        )
        qualified_paths = _dedupe_preserve_order(
            [
                *qualified_paths,
                f"{descriptor_qualified} -> {callee_qualified}",
                *[
                    f"{descriptor_qualified} -> {path}"
                    if not path.startswith(f"{descriptor_qualified} ->")
                    else path
                    for path in callee_analysis.static_call_graph_qualified_paths
                ],
            ]
        )

    return _PythonFunctionStaticAnalysis(
        section_path=descriptor.section_path,
        static_call_graph_paths=static_paths,
        static_call_graph_qualified_paths=qualified_paths,
        repository_adapters=repository_adapters,
        repository_adapter_symbols=repository_adapter_symbols,
        driver_adapters=driver_adapters,
        driver_adapter_symbols=driver_adapter_symbols,
        transaction_boundaries=transaction_boundaries,
        retry_paths=retry_paths,
        batch_paths=batch_paths,
        db_write_api_calls=db_write_api_calls,
        db_write_api_symbols=db_write_api_symbols,
        persistence_operation_types=operation_types,
        persistence_schema_targets=schema_targets,
        persistence_backends=backends,
        constructor_injection_bindings=constructor_injection_bindings,
    )


def _qualify_repo_call_chain(
    *,
    call_chain: str,
    descriptor: _PythonFunctionDescriptor,
    repo_context: _PythonRepoContext,
    local_symbol_bindings: dict[str, str] | None = None,
) -> str:
    normalized = str(call_chain or "").strip()
    if not normalized:
        return ""
    resolved_local_symbol_bindings = local_symbol_bindings or descriptor.local_symbol_bindings
    if normalized.startswith("self.") and descriptor.class_symbol:
        remainder = normalized[len("self.") :]
        attribute_name, separator, tail = remainder.partition(".")
        binding_symbol = _resolve_class_attribute_binding(
            class_symbol=descriptor.class_symbol,
            attribute_name=attribute_name,
            repo_context=repo_context,
        )
        if binding_symbol:
            return binding_symbol if not separator else f"{binding_symbol}.{tail}"
        return f"{descriptor.class_symbol}.{remainder}"
    if normalized.startswith("cls.") and descriptor.class_symbol:
        remainder = normalized[len("cls.") :]
        return f"{descriptor.class_symbol}.{remainder}"
    if "." not in normalized:
        return _resolve_repo_call_chain_root(
            root=normalized,
            descriptor=descriptor,
            repo_context=repo_context,
            local_symbol_bindings=resolved_local_symbol_bindings,
        ) or normalized
    root, _, tail = normalized.partition(".")
    resolved_root = _resolve_repo_call_chain_root(
        root=root,
        descriptor=descriptor,
        repo_context=repo_context,
        local_symbol_bindings=resolved_local_symbol_bindings,
    )
    if not resolved_root:
        return normalized
    qualified = f"{resolved_root}.{tail}"
    followed_binding = _follow_repo_attribute_binding(
        qualified_chain=qualified,
        repo_context=repo_context,
    )
    return followed_binding or qualified


def _resolve_repo_call_chain_root(
    *,
    root: str,
    descriptor: _PythonFunctionDescriptor,
    repo_context: _PythonRepoContext,
    local_symbol_bindings: dict[str, str] | None = None,
) -> str | None:
    normalized_root = str(root or "").strip()
    if not normalized_root:
        return None
    resolved_local_symbol_bindings = local_symbol_bindings or descriptor.local_symbol_bindings
    if normalized_root in resolved_local_symbol_bindings:
        return resolved_local_symbol_bindings[normalized_root]
    if normalized_root in descriptor.parameter_type_bindings:
        return descriptor.parameter_type_bindings[normalized_root]
    module_context = repo_context.modules_by_source_id.get(descriptor.source_id)
    if module_context is None:
        return None
    if normalized_root in descriptor.import_aliases:
        return descriptor.import_aliases[normalized_root]
    if normalized_root in module_context.class_symbols:
        return module_context.class_symbols[normalized_root]
    if normalized_root in module_context.function_symbols:
        return module_context.function_symbols[normalized_root]
    class_candidates = repo_context.class_symbols_by_name.get(normalized_root, [])
    if len(class_candidates) == 1:
        return class_candidates[0]
    function_candidates = repo_context.function_symbols_by_name.get(normalized_root, [])
    if len(function_candidates) == 1:
        return function_candidates[0]
    return None


def _resolve_descriptor_local_symbol_bindings(
    *,
    descriptor: _PythonFunctionDescriptor,
    repo_context: _PythonRepoContext,
) -> dict[str, str]:
    resolved = dict(descriptor.local_symbol_bindings)
    if not descriptor.local_binding_expressions:
        return resolved
    for _ in range(max(len(descriptor.local_binding_expressions), 1) + 1):
        changed = False
        for target_name, binding_expression in descriptor.local_binding_expressions.items():
            binding_symbol = _resolve_repo_binding_expression(
                binding_expression=binding_expression,
                descriptor=descriptor,
                repo_context=repo_context,
                local_symbol_bindings=resolved,
            )
            if not binding_symbol or resolved.get(target_name) == binding_symbol:
                continue
            resolved[target_name] = binding_symbol
            changed = True
        if not changed:
            break
    return resolved


def _resolve_repo_binding_expression(
    *,
    binding_expression: str,
    descriptor: _PythonFunctionDescriptor,
    repo_context: _PythonRepoContext,
    local_symbol_bindings: dict[str, str],
) -> str | None:
    qualified = _qualify_repo_call_chain(
        call_chain=binding_expression,
        descriptor=descriptor,
        repo_context=repo_context,
        local_symbol_bindings=local_symbol_bindings,
    )
    if not qualified:
        return None
    base_symbol, separator, method_name = qualified.rpartition(".")
    if separator and method_name.casefold() in TRANSACTION_HINTS and base_symbol:
        return base_symbol
    followed_binding = _follow_repo_attribute_binding(
        qualified_chain=qualified,
        repo_context=repo_context,
    )
    return followed_binding or qualified


def _follow_repo_attribute_binding(
    *,
    qualified_chain: str,
    repo_context: _PythonRepoContext,
) -> str | None:
    parts = [part for part in str(qualified_chain or "").split(".") if part]
    if len(parts) < 2:
        return None
    for split_index in range(len(parts) - 1, 0, -1):
        class_symbol = ".".join(parts[:split_index])
        attribute_parts = parts[split_index:]
        resolved = _resolve_attribute_chain_for_class_symbol(
            class_symbol=class_symbol,
            attribute_parts=attribute_parts,
            repo_context=repo_context,
        )
        if resolved:
            return resolved
    return None


def _resolve_attribute_chain_for_class_symbol(
    *,
    class_symbol: str,
    attribute_parts: list[str],
    repo_context: _PythonRepoContext,
) -> str | None:
    current_symbol = str(class_symbol or "").strip()
    if not current_symbol or not attribute_parts:
        return None
    for index, attribute_name in enumerate(attribute_parts):
        is_last = index == len(attribute_parts) - 1
        resolved_binding = _resolve_class_attribute_binding(
            class_symbol=current_symbol,
            attribute_name=attribute_name,
            repo_context=repo_context,
        )
        if resolved_binding:
            current_symbol = resolved_binding
            continue
        return f"{current_symbol}.{attribute_name}" if is_last else None
    return current_symbol


def _resolve_class_attribute_binding(
    *,
    class_symbol: str,
    attribute_name: str,
    repo_context: _PythonRepoContext,
) -> str | None:
    normalized_class_symbol = str(class_symbol or "").strip()
    normalized_attribute = str(attribute_name or "").strip()
    if not normalized_class_symbol or not normalized_attribute:
        return None
    direct_binding = repo_context.class_attribute_bindings_by_class_symbol.get(normalized_class_symbol, {}).get(normalized_attribute)
    if direct_binding:
        return direct_binding
    for base_symbol in _transitive_base_symbols(
        class_symbol=normalized_class_symbol,
        repo_context=repo_context,
    ):
        base_binding = repo_context.class_attribute_bindings_by_class_symbol.get(base_symbol, {}).get(normalized_attribute)
        if base_binding:
            return base_binding
    descendant_bindings = _dedupe_preserve_order(
        [
            binding
            for descendant_symbol in _transitive_descendant_symbols(
                class_symbol=normalized_class_symbol,
                repo_context=repo_context,
            )
            for binding in [_resolve_attribute_binding_in_lineage(
                class_symbol=descendant_symbol,
                attribute_name=normalized_attribute,
                repo_context=repo_context,
            )]
            if binding
        ]
    )
    if len(descendant_bindings) == 1:
        return descendant_bindings[0]
    return None


def _resolve_attribute_binding_in_lineage(
    *,
    class_symbol: str,
    attribute_name: str,
    repo_context: _PythonRepoContext,
) -> str | None:
    direct_binding = repo_context.class_attribute_bindings_by_class_symbol.get(class_symbol, {}).get(attribute_name)
    if direct_binding:
        return direct_binding
    for base_symbol in _transitive_base_symbols(class_symbol=class_symbol, repo_context=repo_context):
        inherited_binding = repo_context.class_attribute_bindings_by_class_symbol.get(base_symbol, {}).get(attribute_name)
        if inherited_binding:
            return inherited_binding
    return None


def _resolve_method_key_with_hierarchy(
    *,
    class_symbol: str,
    method_name: str,
    repo_context: _PythonRepoContext,
) -> str | None:
    direct_match = repo_context.method_key_by_symbol.get((class_symbol, method_name))
    if direct_match is not None and class_symbol not in repo_context.interface_like_symbols:
        return direct_match
    for base_symbol in _transitive_base_symbols(class_symbol=class_symbol, repo_context=repo_context):
        inherited_match = repo_context.method_key_by_symbol.get((base_symbol, method_name))
        if inherited_match is not None:
            return inherited_match
    descendant_matches = _dedupe_preserve_order(
        [
            match
            for descendant_symbol in _transitive_descendant_symbols(
                class_symbol=class_symbol,
                repo_context=repo_context,
            )
            for match in [_resolve_method_key_in_lineage(
                class_symbol=descendant_symbol,
                method_name=method_name,
                repo_context=repo_context,
            )]
            if match is not None
        ]
    )
    if len(descendant_matches) == 1:
        return descendant_matches[0]
    if direct_match is not None:
        return direct_match
    return None


def _resolve_method_key_in_lineage(
    *,
    class_symbol: str,
    method_name: str,
    repo_context: _PythonRepoContext,
) -> str | None:
    direct_match = repo_context.method_key_by_symbol.get((class_symbol, method_name))
    if direct_match is not None:
        return direct_match
    for base_symbol in _transitive_base_symbols(class_symbol=class_symbol, repo_context=repo_context):
        inherited_match = repo_context.method_key_by_symbol.get((base_symbol, method_name))
        if inherited_match is not None:
            return inherited_match
    return None


def _transitive_base_symbols(
    *,
    class_symbol: str,
    repo_context: _PythonRepoContext,
) -> list[str]:
    ordered: list[str] = []
    pending = list(repo_context.class_base_symbols_by_symbol.get(class_symbol, []))
    seen: set[str] = set()
    while pending:
        candidate = pending.pop(0)
        if candidate in seen:
            continue
        seen.add(candidate)
        ordered.append(candidate)
        pending.extend(repo_context.class_base_symbols_by_symbol.get(candidate, []))
    return ordered


def _transitive_descendant_symbols(
    *,
    class_symbol: str,
    repo_context: _PythonRepoContext,
) -> list[str]:
    ordered: list[str] = []
    pending = list(repo_context.subclass_symbols_by_base_symbol.get(class_symbol, []))
    seen: set[str] = set()
    while pending:
        candidate = pending.pop(0)
        if candidate in seen:
            continue
        seen.add(candidate)
        ordered.append(candidate)
        pending.extend(repo_context.subclass_symbols_by_base_symbol.get(candidate, []))
    return ordered


def _resolve_repo_callee_key(*, call_chain: str, repo_context: _PythonRepoContext) -> str | None:
    normalized = str(call_chain or "").strip()
    if not normalized:
        return None
    direct_match = repo_context.function_key_by_symbol.get(normalized)
    if direct_match is not None:
        direct_descriptor = repo_context.descriptors_by_key.get(direct_match)
        if direct_descriptor is None or direct_descriptor.class_symbol not in repo_context.interface_like_symbols:
            return direct_match
    base_symbol, separator, method_name = normalized.rpartition(".")
    if separator and base_symbol:
        return _resolve_method_key_with_hierarchy(
            class_symbol=base_symbol,
            method_name=method_name,
            repo_context=repo_context,
        )
    return None


def _display_call_chain(call_chain: str) -> str:
    return _display_symbol(call_chain)


def _display_adapter_symbol(symbol: str) -> str:
    normalized = str(symbol or "").strip()
    if not normalized:
        return ""
    return normalized.rsplit(".", 1)[-1]


def _display_symbol(symbol: str) -> str:
    normalized = str(symbol or "").strip()
    if not normalized:
        return ""
    parts = [part for part in normalized.split(".") if part]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return parts[0]


def _adapter_symbols_from_call_chain(
    *,
    call_chain: str,
    adapter_hints: tuple[str, ...],
    repo_context: _PythonRepoContext | None = None,
) -> list[str]:
    base_chain, separator, method_name = call_chain.rpartition(".")
    if not separator or not base_chain:
        return []
    descriptor = f"{base_chain}.{method_name}".casefold()
    if not any(hint in descriptor for hint in adapter_hints):
        return []
    candidate_symbols = [base_chain]
    if repo_context is not None:
        concrete_symbol = _preferred_concrete_runtime_symbol(
            class_symbol=base_chain,
            repo_context=repo_context,
        )
        if concrete_symbol:
            candidate_symbols.append(concrete_symbol)
    return _dedupe_preserve_order(candidate_symbols)


def _preferred_concrete_runtime_symbol(
    *,
    class_symbol: str,
    repo_context: _PythonRepoContext,
) -> str | None:
    descendants = _transitive_descendant_symbols(class_symbol=class_symbol, repo_context=repo_context)
    if not descendants:
        return None
    leaf_descendants = [
        descendant
        for descendant in descendants
        if not repo_context.subclass_symbols_by_base_symbol.get(descendant)
    ]
    candidate_symbols = leaf_descendants or descendants
    unique_candidates = _dedupe_preserve_order(candidate_symbols)
    if len(unique_candidates) == 1:
        return unique_candidates[0]
    return None


def _build_python_static_analyses(
    tree: ast.AST,
) -> tuple[dict[str, _PythonFunctionDescriptor], dict[str, _PythonFunctionStaticAnalysis]]:
    descriptors, class_attribute_bindings = _collect_python_function_descriptors(tree=tree)
    analysis_cache: dict[str, _PythonFunctionStaticAnalysis] = {}
    stack: set[str] = set()

    def analyze(section_path: str) -> _PythonFunctionStaticAnalysis:
        cached = analysis_cache.get(section_path)
        if cached is not None:
            return cached
        if section_path in stack:
            return _PythonFunctionStaticAnalysis(section_path=section_path)
        stack.add(section_path)
        descriptor = descriptors[section_path]
        analysis = _analyze_python_function_descriptor(
            descriptor=descriptor,
            descriptors=descriptors,
            class_attribute_bindings=class_attribute_bindings,
            resolve_analysis=analyze,
        )
        analysis_cache[section_path] = analysis
        stack.remove(section_path)
        return analysis

    for section_path in descriptors:
        analyze(section_path)
    return descriptors, analysis_cache


def _collect_python_function_descriptors(
    *,
    tree: ast.AST,
) -> tuple[dict[str, _PythonFunctionDescriptor], dict[tuple[str, ...], dict[str, str]]]:
    descriptors: dict[str, _PythonFunctionDescriptor] = {}
    class_attribute_bindings: dict[tuple[str, ...], dict[str, str]] = {}
    function_paths_by_name: dict[str, list[str]] = {}
    method_paths_by_class: dict[tuple[tuple[str, ...], str], str] = {}

    def visit(node: ast.AST, *, class_stack: list[str]) -> None:
        if isinstance(node, ast.ClassDef):
            next_stack = [*class_stack, node.name]
            class_attribute_bindings[tuple(next_stack)] = _extract_class_attribute_bindings(node=node)
            for child in node.body:
                visit(child, class_stack=next_stack)
            return
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            section_path = ".".join([*class_stack, node.name]) or node.name
            descriptor = _PythonFunctionDescriptor(
                node=node,
                class_stack=list(class_stack),
                section_path=section_path,
                decorator_labels=_decorator_labels(node=node),
                docstring=ast.get_docstring(node) or "",
            )
            descriptors[section_path] = descriptor
            function_paths_by_name.setdefault(node.name, []).append(section_path)
            if class_stack:
                method_paths_by_class[(tuple(class_stack), node.name)] = section_path
        for descendant in ast.iter_child_nodes(node):
            if isinstance(descendant, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                visit(descendant, class_stack=class_stack)

    for child in getattr(tree, "body", []):
        visit(child, class_stack=[])

    for descriptor in descriptors.values():
        direct_call_chains: list[str] = []
        local_callee_keys: list[str] = []
        string_literals: list[str] = []
        with_context_calls: list[str] = []
        loop_contexts: list[str] = []
        for child in ast.walk(descriptor.node):
            if isinstance(child, ast.Call):
                call_chain = _call_chain_from_expr(expr=child.func)
                if call_chain:
                    direct_call_chains.append(call_chain)
                    resolved = _resolve_local_callee(
                        function_name=call_chain,
                        class_stack=descriptor.class_stack,
                        function_paths_by_name=function_paths_by_name,
                        method_paths_by_class=method_paths_by_class,
                    )
                    if resolved is None:
                        resolved = _resolve_local_callee(
                            function_name=_normalize_call_chain(
                                call_chain=call_chain,
                                class_stack=descriptor.class_stack,
                                class_attribute_bindings=class_attribute_bindings,
                            ),
                            class_stack=descriptor.class_stack,
                            function_paths_by_name=function_paths_by_name,
                            method_paths_by_class=method_paths_by_class,
                        )
                    if resolved is not None and resolved != descriptor.section_path:
                        local_callee_keys.append(resolved)
            elif isinstance(child, (ast.With, ast.AsyncWith)):
                for item in child.items:
                    context_chain = _call_chain_from_context_expr(expr=item.context_expr)
                    if context_chain:
                        with_context_calls.append(context_chain)
            elif isinstance(child, (ast.For, ast.AsyncFor)):
                loop_contexts.append(_loop_context_text(node=child))
            elif isinstance(child, ast.Constant) and isinstance(child.value, str):
                string_literals.append(child.value)
        descriptor.direct_call_chains = _dedupe_preserve_order(direct_call_chains)
        descriptor.local_callee_keys = _dedupe_preserve_order(local_callee_keys)
        descriptor.string_literals = _dedupe_preserve_order(string_literals)
        descriptor.with_context_calls = _dedupe_preserve_order(with_context_calls)
        descriptor.loop_contexts = _dedupe_preserve_order(loop_contexts)
    return descriptors, class_attribute_bindings


def _extract_class_attribute_bindings(*, node: ast.ClassDef) -> dict[str, str]:
    bindings: dict[str, str] = {}
    for child in node.body:
        if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) or child.name != "__init__":
            continue
        for inner in ast.walk(child):
            if isinstance(inner, ast.Assign):
                binding = _self_attribute_binding(assign_targets=inner.targets, value=inner.value)
                if binding is not None:
                    attribute_name, binding_label = binding
                    bindings[attribute_name] = binding_label
            elif isinstance(inner, ast.AnnAssign):
                binding = _self_attribute_binding(assign_targets=[inner.target], value=inner.value)
                if binding is not None:
                    attribute_name, binding_label = binding
                    bindings[attribute_name] = binding_label
    return bindings


def _self_attribute_binding(*, assign_targets: list[ast.expr], value: ast.expr | None) -> tuple[str, str] | None:
    for target in assign_targets:
        if not isinstance(target, ast.Attribute) or not isinstance(target.value, ast.Name) or target.value.id != "self":
            continue
        attribute_name = str(target.attr or "").strip()
        if not attribute_name:
            continue
        binding_label = _constructor_binding_label(value=value)
        if binding_label:
            return attribute_name, binding_label
    return None


def _constructor_binding_label(*, value: ast.expr | None) -> str | None:
    if isinstance(value, ast.Call):
        call_label = _call_chain_from_expr(expr=value.func)
        if call_label:
            return call_label.rsplit(".", 1)[-1]
    if isinstance(value, ast.Name):
        return value.id
    if isinstance(value, ast.Attribute):
        return _call_chain_from_expr(expr=value)
    return None


def _resolve_local_callee(
    *,
    function_name: str,
    class_stack: list[str],
    function_paths_by_name: dict[str, list[str]],
    method_paths_by_class: dict[tuple[tuple[str, ...], str], str],
) -> str | None:
    normalized_name = str(function_name or "").strip()
    if not normalized_name:
        return None
    if "." in normalized_name:
        base_name, _, method_name = normalized_name.rpartition(".")
        if base_name in {"self", "cls"} and class_stack:
            return method_paths_by_class.get((tuple(class_stack), method_name))
        for (candidate_class_stack, candidate_method), section_path in method_paths_by_class.items():
            if candidate_method == method_name and candidate_class_stack and candidate_class_stack[-1] == base_name:
                return section_path
        return None
    candidates = function_paths_by_name.get(normalized_name, [])
    if not candidates:
        return None
    if class_stack:
        class_candidates = [
            candidate
            for candidate in candidates
            if candidate.startswith(".".join(class_stack) + ".")
        ]
        if class_candidates:
            return class_candidates[0]
    return candidates[0]


def _call_chain_from_context_expr(*, expr: ast.expr) -> str:
    if isinstance(expr, ast.Call):
        return _call_chain_from_expr(expr=expr.func)
    return _call_chain_from_expr(expr=expr)


def _call_chain_from_expr(*, expr: ast.expr) -> str:
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Attribute):
        base = _call_chain_from_expr(expr=expr.value)
        return f"{base}.{expr.attr}" if base else expr.attr
    if isinstance(expr, ast.Call):
        return _call_chain_from_expr(expr=expr.func)
    if isinstance(expr, ast.Subscript):
        return _call_chain_from_expr(expr=expr.value)
    return ""


def _loop_context_text(*, node: ast.For | ast.AsyncFor) -> str:
    target = _expr_text(expr=node.target)
    iterator = _expr_text(expr=node.iter)
    return f"{target} in {iterator}".strip()


def _expr_text(*, expr: ast.AST) -> str:
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Attribute):
        base = _expr_text(expr=expr.value)
        return f"{base}.{expr.attr}" if base else expr.attr
    if isinstance(expr, ast.Call):
        call_target = _expr_text(expr=expr.func)
        return f"{call_target}()"
    if isinstance(expr, ast.Constant):
        return str(expr.value)
    return ""


def _analyze_python_function_descriptor(
    *,
    descriptor: _PythonFunctionDescriptor,
    descriptors: dict[str, _PythonFunctionDescriptor],
    class_attribute_bindings: dict[tuple[str, ...], dict[str, str]],
    resolve_analysis: Callable[[str], _PythonFunctionStaticAnalysis],
) -> _PythonFunctionStaticAnalysis:
    normalized_calls = _dedupe_preserve_order(
        [
            _normalize_call_chain(
                call_chain=call_chain,
                class_stack=descriptor.class_stack,
                class_attribute_bindings=class_attribute_bindings,
            )
            for call_chain in descriptor.direct_call_chains
            if str(call_chain).strip()
        ]
    )
    normalized_contexts = _dedupe_preserve_order(
        [
            _normalize_call_chain(
                call_chain=context_call,
                class_stack=descriptor.class_stack,
                class_attribute_bindings=class_attribute_bindings,
            )
            for context_call in descriptor.with_context_calls
            if str(context_call).strip()
        ]
    )
    repository_adapters = _dedupe_preserve_order(
        [
            adapter
            for call_chain in normalized_calls
            for adapter in _adapter_labels_from_call_chain(call_chain=call_chain, adapter_hints=REPOSITORY_ADAPTER_HINTS)
        ]
    )
    driver_adapters = _dedupe_preserve_order(
        [
            adapter
            for call_chain in [*normalized_calls, *normalized_contexts]
            for adapter in _adapter_labels_from_call_chain(call_chain=call_chain, adapter_hints=DRIVER_ADAPTER_HINTS)
        ]
    )
    db_write_api_calls = _dedupe_preserve_order(
        [
            call_chain
            for call_chain in normalized_calls
            if _call_chain_is_db_write(call_chain=call_chain)
        ]
    )
    transaction_boundaries = _dedupe_preserve_order(
        [
            boundary
            for boundary in [*normalized_calls, *normalized_contexts]
            if _contains_any(text=boundary.casefold(), hints=TRANSACTION_HINTS)
        ]
    )
    retry_paths = _dedupe_preserve_order(
        [
            retry_path
            for retry_path in [*descriptor.decorator_labels, *normalized_calls]
            if _contains_any(text=retry_path.casefold(), hints=RETRY_HINTS)
        ]
    )
    batch_paths = _dedupe_preserve_order(
        [
            *descriptor.loop_contexts,
            *[
                path
                for path in normalized_calls
                if _contains_any(text=path.casefold(), hints=BATCH_HINTS)
            ],
        ]
    )
    operation_types, schema_targets, backends = _infer_persistence_shape(
        call_chains=normalized_calls,
        string_literals=descriptor.string_literals,
        subject_key_hint=descriptor.section_path,
    )
    static_paths = _dedupe_preserve_order(
        [f"{descriptor.section_path} -> {call_chain}" for call_chain in db_write_api_calls]
    )

    for callee_key in descriptor.local_callee_keys:
        callee = descriptors.get(callee_key)
        if callee is None:
            continue
        callee_analysis = resolve_analysis(callee_key)
        repository_adapters = _dedupe_preserve_order([*repository_adapters, *callee_analysis.repository_adapters])
        driver_adapters = _dedupe_preserve_order([*driver_adapters, *callee_analysis.driver_adapters])
        transaction_boundaries = _dedupe_preserve_order([*transaction_boundaries, *callee_analysis.transaction_boundaries])
        retry_paths = _dedupe_preserve_order([*retry_paths, *callee_analysis.retry_paths])
        batch_paths = _dedupe_preserve_order([*batch_paths, *callee_analysis.batch_paths])
        db_write_api_calls = _dedupe_preserve_order([*db_write_api_calls, *callee_analysis.db_write_api_calls])
        operation_types = _dedupe_preserve_order([*operation_types, *callee_analysis.persistence_operation_types])
        schema_targets = _dedupe_preserve_order([*schema_targets, *callee_analysis.persistence_schema_targets])
        backends = _dedupe_preserve_order([*backends, *callee_analysis.persistence_backends])
        static_paths = _dedupe_preserve_order(
            [
                *static_paths,
                f"{descriptor.section_path} -> {callee_key}",
                *[
                    f"{descriptor.section_path} -> {path}"
                    if not path.startswith(f"{descriptor.section_path} ->")
                    else path
                    for path in callee_analysis.static_call_graph_paths
                ],
            ]
        )

    return _PythonFunctionStaticAnalysis(
        section_path=descriptor.section_path,
        static_call_graph_paths=static_paths,
        repository_adapters=repository_adapters,
        driver_adapters=driver_adapters,
        transaction_boundaries=transaction_boundaries,
        retry_paths=retry_paths,
        batch_paths=batch_paths,
        db_write_api_calls=db_write_api_calls,
        persistence_operation_types=operation_types,
        persistence_schema_targets=schema_targets,
        persistence_backends=backends,
    )


def _normalize_call_chain(
    *,
    call_chain: str,
    class_stack: list[str],
    class_attribute_bindings: dict[tuple[str, ...], dict[str, str]],
) -> str:
    normalized = str(call_chain or "").strip()
    if not normalized:
        return ""
    if not class_stack or not normalized.startswith("self."):
        return normalized
    binding_map = class_attribute_bindings.get(tuple(class_stack), {})
    _, _, remainder = normalized.partition("self.")
    attribute_name, separator, tail = remainder.partition(".")
    binding_label = binding_map.get(attribute_name)
    if not binding_label:
        return normalized
    if not separator:
        return binding_label
    return f"{binding_label}.{tail}"


def _adapter_labels_from_call_chain(*, call_chain: str, adapter_hints: tuple[str, ...]) -> list[str]:
    base_chain, separator, method_name = call_chain.rpartition(".")
    if not separator or not base_chain:
        return []
    descriptor = f"{base_chain}.{method_name}".casefold()
    if not any(hint in descriptor for hint in adapter_hints):
        return []
    return [base_chain]


def _call_chain_is_db_write(*, call_chain: str) -> bool:
    method_name = call_chain.rsplit(".", 1)[-1].casefold()
    return method_name in DB_WRITE_CALL_HINTS


def _infer_persistence_shape(
    *,
    call_chains: list[str],
    string_literals: list[str],
    subject_key_hint: str,
) -> tuple[list[str], list[str], list[str]]:
    operation_types: list[str] = []
    schema_targets: list[str] = []
    backends: list[str] = []
    descriptor = " ".join([*call_chains, *string_literals, subject_key_hint]).casefold()

    if any(token in descriptor for token in ("neo4j", "execute_query", "execute_write", "write_transaction", "session", "tx.run", "merge (", "create (")):
        backends.append("neo4j")
    elif any(token in descriptor for token in ("repo", "repository", "adapter")):
        backends.append("repository")

    for call_chain in call_chains:
        normalized = call_chain.casefold()
        if normalized.endswith(("save", "persist", "upsert", "merge")):
            operation_types.append("repository_upsert")
        if normalized.endswith(("create_relationship", "merge_relationship")):
            operation_types.append("repository_relationship_write")
        if normalized.endswith(("execute_query", "run", "execute_write", "write_transaction")):
            operation_types.append("neo4j_query_write")

    for literal in string_literals:
        normalized = str(literal or "")
        upper = normalized.upper()
        if "MERGE" in upper and ")-[" in upper or "MERGE" in upper and "]-(" in upper:
            operation_types.append("neo4j_merge_relationship")
        if "CREATE" in upper and (")-[" in upper or "]-(" in upper):
            operation_types.append("neo4j_create_relationship")
        if "MERGE" in upper and "(" in upper:
            operation_types.append("neo4j_merge_node")
        if "CREATE" in upper and "(" in upper:
            operation_types.append("neo4j_create_node")
        if "SET " in upper:
            operation_types.append("neo4j_set_properties")
        if "DELETE " in upper:
            operation_types.append("neo4j_delete")
        for label in re.findall(r"\(\s*[A-Za-z_][A-Za-z0-9_]*\s*:\s*([A-Za-z_][A-Za-z0-9_]*)", normalized):
            schema_targets.append(f"Node:{label}")
        for rel_type in re.findall(r"\[\s*[A-Za-z_][A-Za-z0-9_]*\s*:\s*([A-Z_][A-Z0-9_]*)", normalized):
            schema_targets.append(f"Relationship:{rel_type}")

    if any(token in descriptor for token in ("history", "historic", "version", "audit_trail")):
        owner = _slugify(subject_key_hint.split(".", 1)[0]) or "artifact"
        schema_targets.append(f"History:{owner.title().replace('_', '')}")
        operation_types.append("history_append")

    return (
        _dedupe_preserve_order(operation_types),
        _dedupe_preserve_order(schema_targets),
        _dedupe_preserve_order(backends),
    )


def _build_schema_catalog(*, documents: list[CollectedDocument]) -> _SchemaCatalog | None:
    metamodel_documents = [document for document in documents if document.source_type == "metamodel"]
    if not metamodel_documents:
        return None
    catalog = _SchemaCatalog()
    for document in metamodel_documents:
        try:
            rows = json.loads(document.body)
        except ValueError:
            continue
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            entity_kind = str(row.get("entity_kind") or "").strip().casefold()
            if entity_kind == "metaclass":
                metaclass_name = str(row.get("metaclass_name") or row.get("name") or "").strip()
                if metaclass_name:
                    catalog.allowed_node_labels.add(metaclass_name)
                for relation_type in _string_list(row.get("outbound_relation_types")):
                    catalog.allowed_relationship_types.add(relation_type)
            elif entity_kind == "label_summary":
                label = str(row.get("label") or "").strip()
                if label:
                    catalog.observed_node_labels.add(label)
            elif entity_kind == "relationship_summary":
                relation_type = str(row.get("relation_type") or "").strip()
                if relation_type:
                    catalog.observed_relationship_types.add(relation_type)
    return catalog


def _schema_validation_metadata(
    *,
    schema_targets: list[str],
    schema_catalog: _SchemaCatalog | None,
) -> dict[str, object]:
    if schema_catalog is None or not schema_targets:
        return {}
    validated_targets: list[str] = []
    observed_only_targets: list[str] = []
    unconfirmed_targets: list[str] = []
    for target in schema_targets:
        normalized_target = str(target or "").strip()
        if not normalized_target or ":" not in normalized_target:
            continue
        target_kind, _, target_name = normalized_target.partition(":")
        if _schema_target_confirmed(target_kind=target_kind, target_name=target_name, schema_catalog=schema_catalog):
            validated_targets.append(normalized_target)
            continue
        if _schema_target_observed(target_kind=target_kind, target_name=target_name, schema_catalog=schema_catalog):
            observed_only_targets.append(normalized_target)
            continue
        unconfirmed_targets.append(normalized_target)
    status = "unconfirmed"
    if validated_targets and not observed_only_targets and not unconfirmed_targets:
        status = "ssot_confirmed"
    elif validated_targets and (observed_only_targets or unconfirmed_targets):
        status = "partially_confirmed"
    elif observed_only_targets and not unconfirmed_targets:
        status = "observed_only"
    elif observed_only_targets and unconfirmed_targets:
        status = "partially_observed"
    return {
        "schema_validated_targets": _dedupe_preserve_order(validated_targets),
        "schema_observed_only_targets": _dedupe_preserve_order(observed_only_targets),
        "schema_unconfirmed_targets": _dedupe_preserve_order(unconfirmed_targets),
        "schema_validation_status": status,
    }


def _schema_target_confirmed(
    *,
    target_kind: str,
    target_name: str,
    schema_catalog: _SchemaCatalog,
) -> bool:
    normalized_kind = str(target_kind or "").strip().casefold()
    normalized_target = str(target_name or "").strip()
    if not normalized_target:
        return False
    if normalized_kind == "node":
        return normalized_target in schema_catalog.allowed_node_labels
    if normalized_kind == "relationship":
        return normalized_target in schema_catalog.allowed_relationship_types
    if normalized_kind == "history":
        return normalized_target in schema_catalog.allowed_node_labels
    return False


def _schema_target_observed(
    *,
    target_kind: str,
    target_name: str,
    schema_catalog: _SchemaCatalog,
) -> bool:
    normalized_kind = str(target_kind or "").strip().casefold()
    normalized_target = str(target_name or "").strip()
    if not normalized_target:
        return False
    if normalized_kind == "node":
        return normalized_target in schema_catalog.observed_node_labels
    if normalized_kind == "relationship":
        return normalized_target in schema_catalog.observed_relationship_types
    if normalized_kind == "history":
        return normalized_target in schema_catalog.observed_node_labels
    return False


def _extract_document_claims(*, document: CollectedDocument) -> list[ExtractedClaimRecord]:
    records: list[ExtractedClaimRecord] = []
    base_context = _document_base_context(document=document)
    heading_stack: list[str] = []
    current_heading = _document_section_path(base_context=base_context, heading_stack=heading_stack)
    current_phase_key: str | None = _extract_phase_key(
        text_fragments=[current_heading],
        require_process_context=True,
    )
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
            current_phase_key = _extract_phase_key(
                text_fragments=[current_heading],
                require_process_context=True,
            )
            continue
        subject = _derive_document_subject_label(
            line_text=stripped,
            section_path=current_heading,
            document=document,
        )
        line_phase_key = _extract_phase_key(
            text_fragments=[current_heading, stripped],
            require_process_context=True,
        )
        if line_phase_key is not None:
            current_phase_key = line_phase_key
        if subject and READ_LINE_PATTERN.search(stripped):
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
        if subject and WRITE_LINE_PATTERN.search(stripped):
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
        if subject and LIFECYCLE_LINE_PATTERN.search(stripped):
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
        if _has_structural_process_signal(line_text=stripped, context_fragments=[current_heading]):
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
        if subject and POLICY_LINE_PATTERN.search(stripped):
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
        if subject or _has_structural_process_signal(line_text=stripped, context_fragments=[current_heading]):
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
    records.extend(
        _extract_metamodel_lifecycle_claims(
            document=document,
            subject_root=metaclass_name,
            anchor_prefix=subject_key,
            row=row,
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
    records.extend(
        _extract_metamodel_lifecycle_claims(
            document=document,
            subject_root=function_name,
            anchor_prefix=subject_key,
            row=row,
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


def _extract_metamodel_lifecycle_claims(
    *,
    document: CollectedDocument,
    subject_root: str,
    anchor_prefix: str,
    row: dict[str, object],
) -> list[ExtractedClaimRecord]:
    normalized_subject = _slugify(subject_root).replace("_", " ").title().replace(" ", "")
    if not normalized_subject:
        return []

    records: list[ExtractedClaimRecord] = []
    lifecycle_value = str(row.get("lifecycle") or row.get("status_lifecycle") or "").strip()
    review_status_value = str(
        row.get("review_status")
        or row.get("initial_status")
        or row.get("default_status")
        or row.get("status")
        or ""
    ).strip()

    if lifecycle_value:
        records.append(
            _build_metamodel_claim_record(
                document=document,
                anchor_value=f"{anchor_prefix}.lifecycle",
                subject_key=f"{normalized_subject}.lifecycle",
                predicate="metamodel_lifecycle",
                normalized_value=lifecycle_value,
                matched_text=f"{normalized_subject} Lifecycle laut Metamodell: {lifecycle_value}",
            )
        )
    if review_status_value:
        records.append(
            _build_metamodel_claim_record(
                document=document,
                anchor_value=f"{anchor_prefix}.review_status",
                subject_key=f"{normalized_subject}.review_status",
                predicate="metamodel_review_status",
                normalized_value=review_status_value,
                matched_text=f"{normalized_subject} Review-Status laut Metamodell: {review_status_value}",
            )
        )
    return records


def _build_claim_record(
    *,
    document: CollectedDocument,
    line_no: int,
    line_text: str,
    subject_key: str,
    predicate: str,
    section_path: str,
) -> ExtractedClaimRecord:
    normalized_value = _normalize_claim_value(matched_text=line_text, predicate=predicate)
    structured_metadata = _claim_structure_metadata(
        subject_key=subject_key,
        predicate=predicate,
        normalized_value=normalized_value,
        matched_text=line_text,
        section_path=section_path,
        document=document,
        extra_metadata=None,
    )
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
            **structured_metadata,
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
    normalized = _normalize_claim_value(matched_text=normalized_value, predicate=predicate)
    structured_metadata = _claim_structure_metadata(
        subject_key=subject_key,
        predicate=predicate,
        normalized_value=normalized,
        matched_text=matched_text,
        section_path="current_dump",
        document=document,
        extra_metadata=None,
    )
    claim = AuditClaimEntry(
        source_snapshot_id=document.snapshot.snapshot_id,
        source_type="metamodel",
        source_id=document.source_id,
        subject_kind="process",
        subject_key=subject_key,
        predicate=predicate,
        normalized_value=normalized,
        scope_kind="global",
        scope_key="FINAI",
        confidence=0.92,
        fingerprint=f"{subject_key}|{predicate}|{normalized}|FINAI",
        evidence_location_ids=[location.location_id],
        metadata={
            "title": document.title,
            "matched_text": matched_text,
            "path_hint": document.path_hint,
            **structured_metadata,
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


def _derive_document_subject_label(
    *,
    line_text: str,
    section_path: str,
    document: CollectedDocument,
) -> str:
    explicit_subject = _derive_explicit_document_subject_label(
        line_text=line_text,
        blocked_tokens=GENERIC_DOCUMENT_SUBJECT_TOKENS,
    )
    if explicit_subject:
        return explicit_subject
    hint_texts = [
        line_text,
        section_path,
        *_document_context_fragments(document=document),
    ]
    object_hint_subject = _derive_document_object_hint_subject(hint_texts=hint_texts)
    if object_hint_subject:
        return object_hint_subject
    return _derive_subject_label_from_hints(
        hint_texts=hint_texts,
        allow_object_hint_match=False,
        allow_path_fallback=False,
        blocked_tokens=GENERIC_DOCUMENT_SUBJECT_TOKENS,
        allow_generic_token_fallback=False,
    )


def _derive_subject_label_from_hints(
    *,
    hint_texts: list[str],
    allow_object_hint_match: bool = True,
    allow_path_fallback: bool = True,
    blocked_tokens: frozenset[str] | None = None,
    allow_generic_token_fallback: bool = True,
) -> str:
    blocked = blocked_tokens or frozenset()
    if allow_object_hint_match:
        for keywords, subject in OBJECT_HINTS:
            if any(keyword in text.casefold() for text in hint_texts for keyword in keywords):
                return subject
    if not allow_generic_token_fallback:
        return "" if not allow_path_fallback else _path_subject_fallback(hint_texts=hint_texts)
    joined = " ".join(hint_texts)
    for token in SUBJECT_TOKEN_PATTERN.findall(joined):
        if token in {"HTTP", "JSON", "UUID", "POST", "GET", "PUT", "DELETE"}:
            continue
        if token.casefold() in blocked:
            continue
        if token not in {"HTTP", "JSON", "UUID", "POST", "GET", "PUT", "DELETE"}:
            return token
    if not allow_path_fallback:
        return ""
    return _path_subject_fallback(hint_texts=hint_texts)


def _derive_explicit_document_subject_label(*, line_text: str, blocked_tokens: frozenset[str]) -> str:
    for match in BACKTICK_SUBJECT_PATTERN.finditer(line_text):
        candidate = str(match.group("token") or "").strip().split(".", 1)[0]
        if not candidate:
            continue
        normalized_candidate = candidate.casefold()
        if normalized_candidate in blocked_tokens:
            continue
        for keywords, subject in OBJECT_HINTS:
            if any(keyword in normalized_candidate for keyword in keywords):
                return subject
        if SUBJECT_TOKEN_PATTERN.fullmatch(candidate) is not None:
            return candidate
    return ""


def _derive_document_object_hint_subject(*, hint_texts: list[str]) -> str:
    for keywords, subject in OBJECT_HINTS:
        if not any(_document_text_contains_keyword(text=text, keyword=keyword) for text in hint_texts for keyword in keywords):
            continue
        if subject in {"BSM_Phase", "BSM_Question"} and not _has_process_phase_context(text_fragments=hint_texts):
            continue
        return subject
    return ""


def _document_text_contains_keyword(*, text: str, keyword: str) -> bool:
    lowered_text = str(text or "").casefold()
    lowered_keyword = str(keyword or "").casefold().strip()
    if not lowered_text or not lowered_keyword:
        return False
    if " " in lowered_keyword:
        return lowered_keyword in lowered_text
    pattern = re.compile(rf"(?<![a-z0-9]){re.escape(lowered_keyword)}(?![a-z0-9])")
    return pattern.search(lowered_text) is not None


def _path_subject_fallback(*, hint_texts: list[str]) -> str:
    path_hint = " ".join(hint_texts)
    stem = path_hint.rsplit("/", 1)[-1].split(".", 1)[0]
    normalized = stem.replace("_", " ").replace("-", " ").strip().title().replace(" ", "")
    return normalized or "RepositoryArtifact"


def _extract_function_claim_records(
    *,
    document: CollectedDocument,
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    class_stack: list[str],
    lines: list[str],
    function_analysis: _PythonFunctionStaticAnalysis | None = None,
    descriptor: _PythonFunctionDescriptor | None = None,
    schema_catalog: _SchemaCatalog | None = None,
) -> list[ExtractedClaimRecord]:
    line_start = int(getattr(node, "lineno", 1))
    line_end = int(getattr(node, "end_lineno", line_start))
    section_path = ".".join([*class_stack, node.name]) or node.name
    snippet = "\n".join(lines[max(0, line_start - 1) : line_end]).strip()
    decorator_labels = descriptor.decorator_labels if descriptor is not None else _decorator_labels(node=node)
    docstring = descriptor.docstring if descriptor is not None else (ast.get_docstring(node) or "")
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
                    **_python_function_analysis_metadata(function_analysis=function_analysis, schema_catalog=schema_catalog),
                },
            )
        )
        records.extend(
            _path_variant_claim_records_from_structured_record(
                record=records[-1],
                document=document,
                line_start=line_start,
                line_end=line_end,
                section_path=section_path,
                metadata={
                    "path_hint": document.path_hint,
                    "title": document.title,
                    "ast_extracted": True,
                    "class_stack": class_stack,
                    "decorators": decorator_labels,
                    **_python_function_analysis_metadata(function_analysis=function_analysis, schema_catalog=schema_catalog),
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
                    **_python_function_analysis_metadata(function_analysis=function_analysis, schema_catalog=schema_catalog),
                },
            )
        )
        records.extend(
            _path_variant_claim_records_from_structured_record(
                record=records[-1],
                document=document,
                line_start=line_start,
                line_end=line_end,
                section_path=section_path,
                metadata={
                    "path_hint": document.path_hint,
                    "title": document.title,
                    "ast_extracted": True,
                    "class_stack": class_stack,
                    "decorators": decorator_labels,
                    "semantic_subclaim": True,
                    **_python_function_analysis_metadata(function_analysis=function_analysis, schema_catalog=schema_catalog),
                },
            )
        )
    return records


def _python_function_analysis_metadata(
    *,
    function_analysis: _PythonFunctionStaticAnalysis | None,
    schema_catalog: _SchemaCatalog | None = None,
) -> dict[str, object]:
    if function_analysis is None:
        return {}
    schema_validation = _schema_validation_metadata(
        schema_targets=function_analysis.persistence_schema_targets,
        schema_catalog=schema_catalog,
    )
    return {
        "static_call_graph_paths": function_analysis.static_call_graph_paths,
        "static_call_graph_qualified_paths": function_analysis.static_call_graph_qualified_paths,
        "repository_adapters": function_analysis.repository_adapters,
        "repository_adapter_symbols": function_analysis.repository_adapter_symbols,
        "driver_adapters": function_analysis.driver_adapters,
        "driver_adapter_symbols": function_analysis.driver_adapter_symbols,
        "transaction_boundaries": function_analysis.transaction_boundaries,
        "retry_paths": function_analysis.retry_paths,
        "batch_paths": function_analysis.batch_paths,
        "db_write_api_calls": function_analysis.db_write_api_calls,
        "db_write_api_symbols": function_analysis.db_write_api_symbols,
        "persistence_operation_types": function_analysis.persistence_operation_types,
        "persistence_schema_targets": function_analysis.persistence_schema_targets,
        "persistence_backends": function_analysis.persistence_backends,
        "constructor_injection_bindings": function_analysis.constructor_injection_bindings,
        **schema_validation,
    }


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
    normalized_value = _normalize_claim_value(matched_text=matched_text, predicate=predicate)
    structured_metadata = _claim_structure_metadata(
        subject_key=subject_key,
        predicate=predicate,
        normalized_value=normalized_value,
        matched_text=matched_text,
        section_path=section_path,
        document=document,
        extra_metadata=metadata,
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
        confidence=confidence,
        fingerprint=f"{subject_key}|{predicate}|{normalized_value}|FINAI",
        evidence_location_ids=[location.location_id],
        operator=str(structured_metadata.get("claim_operator") or "").strip() or None,
        constraint=str(structured_metadata.get("claim_constraint") or "").strip() or None,
        focus_value=str(structured_metadata.get("claim_focus_value") or "").strip() or None,
        assertion_status=_coerce_claim_assertion_status(structured_metadata.get("assertion_status")),
        source_authority=_coerce_claim_source_authority(structured_metadata.get("source_authority")),
        metadata={
            **metadata,
            "matched_text": matched_text,
            **structured_metadata,
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


def _coerce_claim_assertion_status(value: object) -> ClaimAssertionStatus:
    normalized = str(value or "asserted").strip()
    if normalized in {"asserted", "excluded", "deprecated", "not_ssot", "secondary_only"}:
        return cast(ClaimAssertionStatus, normalized)
    return "asserted"


def _coerce_claim_source_authority(value: object) -> ClaimSourceAuthority:
    normalized = str(value or "heuristic").strip()
    if normalized in {
        "explicit_truth",
        "confirmed_decision",
        "ssot",
        "governed",
        "working_doc",
        "historical",
        "runtime_observation",
        "implementation",
        "heuristic",
    }:
        return cast(ClaimSourceAuthority, normalized)
    return "heuristic"


def _path_variant_claim_records_from_structured_record(
    *,
    record: ExtractedClaimRecord,
    document: CollectedDocument,
    line_start: int,
    line_end: int,
    section_path: str,
    metadata: dict[str, object],
) -> list[ExtractedClaimRecord]:
    subject_key = str(record.claim.subject_key or "").strip()
    if not subject_key.endswith((".write_path", ".read_path", ".policy", ".approval_policy", ".scope_policy")):
        return []

    variant_role = str(record.claim.metadata.get("path_variant_role") or "").strip()
    static_paths = _string_list(record.claim.metadata.get("static_call_graph_paths"))
    qualified_paths = _string_list(record.claim.metadata.get("static_call_graph_qualified_paths"))
    delegation_paths = _dedupe_preserve_order([*qualified_paths, *static_paths, section_path])
    family_group_key = _path_family_group_key_from_record(record=record, section_path=section_path)
    path_strength_score = _path_strength_score_from_record(record=record)
    inference_signal = _path_inference_signal(
        variant_role=variant_role,
        family_group_key=family_group_key,
        path_strength_score=path_strength_score,
        delegation_paths=delegation_paths,
    )
    if not variant_role and not delegation_paths and not family_group_key:
        return []

    records: list[ExtractedClaimRecord] = []
    if variant_role:
        records.append(
            _build_structured_claim_record(
                document=document,
                subject_key=subject_key,
                predicate="implemented_path_variant_role",
                matched_text=variant_role,
                line_start=line_start,
                line_end=line_end,
                section_path=section_path,
                confidence=min(float(record.claim.confidence), 0.84),
                metadata={
                    **metadata,
                    "derived_from_claim_id": record.claim.claim_id,
                    "path_variant_role": variant_role,
                    "path_variant_claim": True,
                },
            )
        )
    if family_group_key:
        records.append(
            _build_structured_claim_record(
                document=document,
                subject_key=subject_key,
                predicate="implemented_path_family_group",
                matched_text=family_group_key,
                line_start=line_start,
                line_end=line_end,
                section_path=section_path,
                confidence=min(float(record.claim.confidence), 0.82),
                metadata={
                    **metadata,
                    "derived_from_claim_id": record.claim.claim_id,
                    "path_variant_role": variant_role or None,
                    "path_variant_claim": True,
                    "path_family_group_key": family_group_key,
                },
            )
        )
    records.append(
        _build_structured_claim_record(
            document=document,
            subject_key=subject_key,
            predicate="implemented_path_strength_score",
            matched_text=str(path_strength_score),
            line_start=line_start,
            line_end=line_end,
            section_path=section_path,
            confidence=min(float(record.claim.confidence), 0.8),
            metadata={
                **metadata,
                "derived_from_claim_id": record.claim.claim_id,
                "path_variant_role": variant_role or None,
                "path_variant_claim": True,
                "path_strength_score": path_strength_score,
            },
        )
    )
    if inference_signal:
        records.append(
            _build_structured_claim_record(
                document=document,
                subject_key=subject_key,
                predicate="implemented_path_inference_signal",
                matched_text=inference_signal,
                line_start=line_start,
                line_end=line_end,
                section_path=section_path,
                confidence=min(float(record.claim.confidence), 0.78),
                metadata={
                    **metadata,
                    "derived_from_claim_id": record.claim.claim_id,
                    "path_variant_role": variant_role or None,
                    "path_variant_claim": True,
                    "path_family_group_key": family_group_key or None,
                    "path_strength_score": path_strength_score,
                    "path_inference_signal": inference_signal,
                },
            )
        )
    for path in delegation_paths[:4]:
        records.append(
            _build_structured_claim_record(
                document=document,
                subject_key=subject_key,
                predicate="implemented_path_delegate",
                matched_text=path,
                line_start=line_start,
                line_end=line_end,
                section_path=section_path,
                confidence=min(float(record.claim.confidence), 0.82),
                metadata={
                    **metadata,
                    "derived_from_claim_id": record.claim.claim_id,
                    "path_variant_role": variant_role or None,
                    "path_variant_claim": True,
                    "delegation_path": path,
                },
            )
        )
    return records


def _path_family_group_key_from_record(*, record: ExtractedClaimRecord, section_path: str) -> str:
    qualified_paths = _string_list(record.claim.metadata.get("static_call_graph_qualified_paths"))
    family_keys: list[str] = []
    for path in qualified_paths:
        for segment in str(path or "").split("->"):
            part = str(segment or "").strip()
            if not part:
                continue
            symbol = part.rsplit(".", 1)[0].strip() if "." in part else part
            if symbol:
                family_keys.append(symbol)
    family_keys = _dedupe_preserve_order(family_keys)
    if family_keys:
        return "|".join(family_keys)

    qualified_adapter_keys = _qualified_adapter_family_keys_from_record(record=record)
    if qualified_adapter_keys:
        return "|".join(qualified_adapter_keys)

    local_paths = _string_list(record.claim.metadata.get("static_call_graph_paths"))
    local_family_keys: list[str] = []
    for path in local_paths:
        for segment in str(path or "").split("->"):
            part = str(segment or "").strip()
            if not part:
                continue
            symbol = part.split(".", 1)[0].strip()
            if symbol:
                local_family_keys.append(symbol)
    local_family_keys = _dedupe_preserve_order(local_family_keys)
    if local_family_keys:
        return "|".join(local_family_keys)

    adapter_keys = _adapter_family_keys_from_record(record=record)
    if adapter_keys:
        return "|".join(adapter_keys)
    return section_path.strip()


def _path_strength_score_from_record(*, record: ExtractedClaimRecord) -> int:
    descriptor = " ".join(
        [
            str(record.claim.normalized_value or ""),
            str(record.claim.subject_key or ""),
            str(record.claim.predicate or ""),
            str(record.claim.metadata.get("matched_text") or ""),
            *[str(item) for item in _string_list(record.claim.metadata.get("static_call_graph_paths"))],
            *[str(item) for item in _string_list(record.claim.metadata.get("static_call_graph_qualified_paths"))],
        ]
    ).replace("_", " ").replace("-", " ").casefold()
    score = 0
    positive_tokens = (
        "approval required",
        "approval",
        "guarded",
        "review",
        "phase_run_id",
        "audit envelope",
        "canonical",
        "validated",
    )
    negative_tokens = (
        "without approval",
        "direct write",
        "direct publish",
        "bypass",
        "manual",
        "compat",
        "legacy",
        "fallback",
        "degraded",
        "v1",
        "raw",
    )
    for token in positive_tokens:
        if token in descriptor:
            score += 2
    for token in negative_tokens:
        if token in descriptor:
            score -= 2
    if "primary" in descriptor:
        score += 1
    if "secondary" in descriptor:
        score -= 1
    return score


def _path_inference_signal(
    *,
    variant_role: str,
    family_group_key: str,
    path_strength_score: int,
    delegation_paths: list[str],
) -> str:
    if variant_role:
        return ""
    descriptor = " ".join([family_group_key, *delegation_paths]).casefold()
    if path_strength_score >= 2:
        return "likely_primary"
    if path_strength_score <= -2:
        return "likely_side_path"
    if any(token in descriptor for token in ("legacy", "compat", "fallback", "degraded", "bypass", "manual", "raw", "v1")):
        return "likely_side_path"
    if any(token in descriptor for token in ("primary", "canonical", "review", "approval", "validated")):
        return "likely_primary"
    return "neutral"


def _qualified_adapter_family_keys_from_record(*, record: ExtractedClaimRecord) -> list[str]:
    metadata = getattr(record.claim, "metadata", {}) or {}
    values = _string_list(metadata.get("repository_adapter_symbols"))
    values.extend(_string_list(metadata.get("driver_adapter_symbols")))
    values.extend(_string_list(metadata.get("constructor_injection_bindings")))
    family_keys: list[str] = []
    for value in values:
        candidate = str(value or "").strip()
        if not candidate:
            continue
        if "." in candidate:
            family_keys.append(candidate.rsplit(".", 1)[0].strip())
        else:
            family_keys.append(candidate)
    return _dedupe_preserve_order([key for key in family_keys if key])


def _adapter_family_keys_from_record(*, record: ExtractedClaimRecord) -> list[str]:
    metadata = getattr(record.claim, "metadata", {}) or {}
    values = _string_list(metadata.get("repository_adapters"))
    values.extend(_string_list(metadata.get("driver_adapters")))
    values.extend(_string_list(metadata.get("constructor_injection_bindings")))
    family_keys: list[str] = []
    for value in values:
        candidate = str(value or "").strip()
        if not candidate:
            continue
        symbol = candidate.split(".", 1)[0].strip()
        if symbol:
            family_keys.append(symbol)
    return _dedupe_preserve_order(family_keys)


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


def _normalize_claim_value(*, matched_text: str, predicate: str) -> str:
    compact = _normalize_value(matched_text)
    if "]: " in compact and "[" in compact:
        candidate = compact.split("]: ", 1)[1]
        if candidate.strip():
            compact = candidate
    elif ": " in compact and predicate in compact:
        candidate = compact.split(": ", 1)[1]
        if candidate.strip():
            compact = candidate
    compact = compact.strip()
    if compact:
        return compact[:180]
    fallback = _normalize_value(matched_text).strip()
    return fallback[:180] if fallback else predicate[:180]


def _claim_structure_metadata(
    *,
    subject_key: str,
    predicate: str,
    normalized_value: str,
    matched_text: str,
    section_path: str,
    document: CollectedDocument,
    extra_metadata: dict[str, object] | None,
) -> dict[str, object]:
    subject_root, _, subject_property = subject_key.partition(".")
    focus_value = _claim_focus_value(normalized_value=normalized_value, matched_text=matched_text)
    operator = _claim_operator(predicate=predicate, focus_value=focus_value)
    constraint = _claim_constraint(predicate=predicate, focus_value=focus_value)
    governance_level = _source_governance_level(document=document, predicate=predicate)
    temporal_status = _source_temporal_status(document=document)
    assertion_status = _claim_assertion_status(matched_text=matched_text, document=document)
    if _is_secondary_analysis_claim(
        document=document,
        subject_key=subject_key,
        predicate=predicate,
        matched_text=matched_text,
        extra_metadata=extra_metadata,
    ):
        assertion_status = "secondary_only"
    path_variant_role = _path_variant_role(
        document=document,
        matched_text=matched_text,
        section_path=section_path,
        extra_metadata=extra_metadata,
    )
    return {
        "claim_subject_root": subject_root or subject_key,
        "claim_property": subject_property or predicate,
        "claim_operator": operator,
        "claim_constraint": constraint,
        "claim_scope_key": package_scope_key(subject_key),
        "claim_focus_value": focus_value,
        "claim_value_kind": _claim_value_kind(predicate=predicate, focus_value=focus_value),
        "assertion_status": assertion_status,
        "source_authority": _source_authority_from_governance_level(
            governance_level=governance_level,
            temporal_status=temporal_status,
        ),
        "claim_section_path": section_path,
        "source_governance_level": governance_level,
        "source_temporal_status": temporal_status,
        "path_variant_role": path_variant_role or None,
    }


def _claim_focus_value(*, normalized_value: str, matched_text: str) -> str:
    compact = _normalize_value(normalized_value or matched_text)
    if "]: " in compact and "[" in compact:
        return compact.split("]: ", 1)[1]
    if ": " in compact and len(compact.split(": ", 1)[1]) >= 8:
        return compact.split(": ", 1)[1]
    return compact


def _claim_operator(*, predicate: str, focus_value: str) -> str:
    lowered_predicate = predicate.casefold()
    lowered_value = focus_value.casefold()
    if any(token in lowered_predicate for token in ("phase_count", "question_count", "label_count")):
        return "count_equals"
    if "phase_order" in lowered_predicate:
        return "order_equals"
    if "reference" in lowered_predicate:
        return "references"
    if any(token in lowered_predicate for token in ("policy", "approval")):
        return "forbids" if any(token in lowered_value for token in ("no ", "not ", "without ", "ohne ")) else "requires"
    if any(token in lowered_predicate for token in ("implemented", "documented", "metamodel")):
        return "describes"
    return "states"


def _claim_constraint(*, predicate: str, focus_value: str) -> str:
    lowered_predicate = predicate.casefold()
    if any(token in lowered_predicate for token in ("policy", "approval", "scope_policy")):
        return focus_value
    if any(token in lowered_predicate for token in ("lifecycle", "review_status")):
        return focus_value
    if any(token in lowered_predicate for token in ("write", "read")):
        return focus_value
    return ""


def _claim_value_kind(*, predicate: str, focus_value: str) -> str:
    lowered_predicate = predicate.casefold()
    if any(token in lowered_predicate for token in ("count", "order")):
        return "numeric"
    if any(token in lowered_predicate for token in ("reference", "label_count")):
        return "reference"
    if any(token in lowered_predicate for token in ("policy", "lifecycle", "write", "read")):
        return "constraint"
    return "text"


def _source_governance_level(*, document: CollectedDocument, predicate: str) -> str:
    descriptor = " ".join(
        [
            str(document.title or ""),
            str(document.path_hint or ""),
            predicate,
        ]
    ).casefold()
    if document.source_type == "github_file":
        return "implementation"
    if any(token in descriptor for token in ("ssot", "target", "reference", "scope-matrix", "run-ssot", "contract")):
        return "ssot"
    if any(token in descriptor for token in ("architecture", "policy", "process", "guardrail")):
        return "governed"
    if any(token in descriptor for token in ("as_is", "legacy", "deprecated", "historic", "archive")):
        return "historical"
    return "working_doc"


def _source_temporal_status(*, document: CollectedDocument) -> str:
    descriptor = " ".join([str(document.title or ""), str(document.path_hint or "")]).casefold()
    if any(token in descriptor for token in ("as_is", "legacy", "deprecated", "archive", "historic")):
        return "historical"
    if any(token in descriptor for token in ("target", "ssot", "reference")):
        return "target"
    return "current"


def _source_authority_from_governance_level(*, governance_level: str, temporal_status: str) -> str:
    normalized_governance = str(governance_level or "").strip()
    normalized_temporal = str(temporal_status or "").strip()
    if normalized_governance == "ssot":
        return "ssot"
    if normalized_governance == "governed":
        return "governed"
    if normalized_governance == "implementation":
        return "implementation"
    if normalized_governance == "historical" or normalized_temporal == "historical":
        return "historical"
    return "working_doc"


def _claim_assertion_status(*, matched_text: str, document: CollectedDocument) -> str:
    normalized = str(matched_text or "").casefold()
    descriptor = " ".join([str(document.title or ""), str(document.path_hint or ""), normalized]).casefold()
    if any(
        token in descriptor
        for token in ("deprecated", "legacy", "historic", "veraltet", "removed", "entfaellt", "entfällt")
    ):
        return "deprecated"
    if any(token in normalized for token in ("not ssot", "kein ssot", "nicht ssot")):
        return "not_ssot"
    if any(token in descriptor for token in ("secondary only", "sekundaer", "sekundär")):
        return "secondary_only"
    if any(token in normalized for token in ("excluded", "ausgeschlossen", "without ", "ohne ", "kein ", "keine ", "not ")) and "not null" not in normalized:
        return "excluded"
    return "asserted"


def _is_secondary_analysis_claim(
    *,
    document: CollectedDocument,
    subject_key: str,
    predicate: str,
    matched_text: str,
    extra_metadata: dict[str, object] | None,
) -> bool:
    path_hint = str(document.path_hint or document.source_id or "").replace("\\", "/").casefold()
    subject_root = str(subject_key or "").split(".", 1)[0]
    normalized_text = " ".join(
        [
            str(matched_text or ""),
            str(subject_key or ""),
            str(predicate or ""),
            *_metadata_text_fragments(metadata=extra_metadata),
        ]
    ).casefold()
    if (
        document.source_type == "github_file"
        and any(path_hint.endswith(hint) for hint in META_ANALYSIS_PATH_HINTS)
        and any(token in normalized_text for token in META_ANALYSIS_TEXT_HINTS)
    ):
        return True
    if (
        document.source_type == "local_doc"
        and (subject_key == "BSM.process" or subject_root in {"BSM_Phase", "BSM_Question"})
        and any(path_hint.endswith(hint) for hint in META_DOCUMENT_PATH_HINTS)
        and any(token in normalized_text for token in META_ANALYSIS_TEXT_HINTS)
    ):
        return True
    return False


def _path_variant_role(
    *,
    document: CollectedDocument,
    matched_text: str,
    section_path: str,
    extra_metadata: dict[str, object] | None,
) -> str:
    local_descriptor = " ".join(
        [
            str(document.title or ""),
            str(document.path_hint or ""),
            str(section_path or ""),
            str(matched_text or ""),
        ]
    ).casefold()
    extended_descriptor = " ".join(
        [
            local_descriptor,
            *_metadata_text_fragments(metadata=extra_metadata),
        ]
    ).casefold()
    if any(token in local_descriptor for token in ("deprecated", "legacy", "historic", "veraltet", "removed", "archive")):
        return "legacy"
    if any(token in local_descriptor for token in FALLBACK_PATH_HINTS):
        return "fallback"
    if any(token in local_descriptor for token in COMPAT_PATH_HINTS):
        return "compat"
    if any(token in local_descriptor for token in SECONDARY_PATH_HINTS):
        return "secondary"
    if any(token in local_descriptor for token in PRIMARY_PATH_HINTS):
        return "primary"
    if any(token in extended_descriptor for token in ("deprecated", "legacy", "historic", "veraltet", "removed", "archive")):
        return "legacy"
    if any(token in extended_descriptor for token in FALLBACK_PATH_HINTS):
        return "fallback"
    if any(token in extended_descriptor for token in COMPAT_PATH_HINTS):
        return "compat"
    if any(token in extended_descriptor for token in SECONDARY_PATH_HINTS):
        return "secondary"
    if any(token in extended_descriptor for token in PRIMARY_PATH_HINTS):
        return "primary"
    return ""


def _metadata_text_fragments(*, metadata: dict[str, object] | None) -> list[str]:
    if not metadata:
        return []
    fragments: list[str] = []
    for key in ("static_call_graph_paths", "static_call_graph_qualified_paths", "repository_adapters", "driver_adapters"):
        value = metadata.get(key)
        if isinstance(value, list):
            fragments.extend(str(item or "") for item in value)
        elif value:
            fragments.append(str(value))
    return [fragment.strip() for fragment in fragments if str(fragment or "").strip()]


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
    primary_text = str(text_fragments[-1] if text_fragments else "").strip()
    specs: list[tuple[str, str]] = []
    if subject and REVIEW_STATUS_PATTERN.search(combined):
        specs.append((f"{subject}.review_status", f"{predicate_prefix}_review_status"))
    if subject and APPROVAL_POLICY_PATTERN.search(combined):
        specs.append((f"{subject}.approval_policy", f"{predicate_prefix}_approval_policy"))
    if subject and SCOPE_POLICY_PATTERN.search(combined):
        specs.append((f"{subject}.scope_policy", f"{predicate_prefix}_scope_policy"))
    if _has_structural_process_signal(line_text=primary_text, context_fragments=text_fragments[:-1]):
        specs.append(("BSM.process", f"{predicate_prefix}_process"))
        if _extract_phase_count(text_fragments=text_fragments) is not None:
            specs.append(("BSM.process", "phase_count"))
        phase_key = _extract_phase_key(text_fragments=text_fragments, require_process_context=True) or default_phase_key
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


def _extract_phase_key(*, text_fragments: list[str], require_process_context: bool = False) -> str | None:
    if require_process_context and not _has_process_phase_context(text_fragments=text_fragments):
        return None
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


def _extract_phase_count(*, text_fragments: list[str]) -> str | None:
    for fragment in text_fragments:
        match = PHASE_COUNT_PATTERN.search(fragment)
        if match is None:
            continue
        value = str(match.group("count") or match.group("count_named") or "").strip()
        if value:
            return value
    return None


def _has_process_phase_context(*, text_fragments: list[str]) -> bool:
    normalized = " ".join(fragment for fragment in text_fragments if fragment).casefold()
    return any(token in normalized for token in ("bsm", "prozess", "process", "question", "frage"))


def _has_structural_process_signal(*, line_text: str, context_fragments: list[str]) -> bool:
    primary = str(line_text or "").strip()
    if not primary:
        return False
    normalized_primary = primary.casefold()
    normalized_context = " ".join([*context_fragments, primary]).casefold()
    if not any(token in normalized_context for token in ("bsm", "phase", "phasen", "question", "frage", "process", "prozess")):
        return False
    if any(
        pattern.search(primary) is not None
        for pattern in (PHASE_REFERENCE_PATTERN, QUESTION_REFERENCE_PATTERN, PHASE_ORDER_PATTERN, QUESTION_COUNT_PATTERN, PHASE_COUNT_PATTERN)
    ):
        return True
    if "bsm" in normalized_primary and any(token in normalized_primary for token in ("process", "prozess", "phase", "question", "frage")):
        return True
    if any(token in normalized_primary for token in ("process", "prozess")) and any(
        token in normalized_primary for token in ("phase", "phasen", "question", "frage")
    ):
        return True
    has_explicit_phase_or_question_context = any(
        PHASE_REFERENCE_PATTERN.search(fragment) is not None or QUESTION_REFERENCE_PATTERN.search(fragment) is not None
        for fragment in context_fragments
    )
    has_contract_signal = any(
        pattern.search(primary) is not None
        for pattern in (
            REVIEW_STATUS_PATTERN,
            APPROVAL_POLICY_PATTERN,
            SCOPE_POLICY_PATTERN,
            READ_LINE_PATTERN,
            WRITE_LINE_PATTERN,
            LIFECYCLE_LINE_PATTERN,
            POLICY_LINE_PATTERN,
        )
    )
    if has_explicit_phase_or_question_context and has_contract_signal:
        return True
    return False


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


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in values:
        normalized = str(item or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
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
