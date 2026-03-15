from __future__ import annotations

from uuid import uuid4

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import (
    AuditClaimEntry,
    AuditFinding,
    AuditLocation,
    AuditPosition,
    RetrievalSegment,
    AuditSourceSnapshot,
    JiraTicketAICodingBrief,
    SemanticEntity,
    SemanticRelation,
    TruthLedgerEntry,
)
from fin_ai_auditor.services.causal_attribution_service import attach_causal_attribution_to_findings
from fin_ai_auditor.services.causal_graph_service import build_causal_graph
from fin_ai_auditor.services.claim_extractor import extract_claim_records
from fin_ai_auditor.services.claim_semantics import semantic_values_aligned, semantic_values_conflict
from fin_ai_auditor.services.consensus_detector import detect_consensus_deviations
from fin_ai_auditor.services.documentation_gap_detector import detect_documentation_gaps
from fin_ai_auditor.services.finding_engine import generate_findings
from fin_ai_auditor.services.finding_engine import build_finding_links
from fin_ai_auditor.services.finding_prioritization import select_findings_for_retrieval
from fin_ai_auditor.services.schema_truth_registry_service import build_schema_truth_registry
from fin_ai_auditor.services.jira_ticket_writer import build_jira_issue_payload
from fin_ai_auditor.services.pipeline_models import CollectedDocument, ExtractedClaimEvidence, ExtractedClaimRecord
from fin_ai_auditor.services.retrieval_index_service import (
    attach_retrieval_insights_to_findings,
    build_recommendation_contexts,
    build_retrieval_index,
)
from fin_ai_auditor.services.semantic_graph_service import build_semantic_graph


def _record_for_consensus(
    *,
    source_type: str,
    source_id: str,
    title: str,
    path_hint: str,
    subject_key: str,
    predicate: str,
    normalized_value: str,
) -> ExtractedClaimRecord:
    location = AuditLocation(
        source_type=source_type,  # type: ignore[arg-type]
        source_id=source_id,
        title=title,
        path_hint=path_hint,
        position=AuditPosition(
            anchor_kind="file_line_range",
            anchor_value=f"{source_id}#L1",
            line_start=1,
            line_end=1,
        ),
    )
    claim = AuditClaimEntry(
        source_type=source_type,  # type: ignore[arg-type]
        source_id=source_id,
        subject_kind="object_property",
        subject_key=subject_key,
        predicate=predicate,
        normalized_value=normalized_value,
        scope_kind="project",
        scope_key="FINAI",
        confidence=0.9,
        fingerprint=f"{source_id}|{subject_key}|{predicate}|{normalized_value}",
        metadata={"title": title, "path_hint": path_hint},
    )
    return ExtractedClaimRecord(
        claim=claim,
        evidence=ExtractedClaimEvidence(location=location, matched_text=normalized_value),
    )


def test_claim_extractor_uses_finai_object_hints_and_policy_predicates() -> None:
    snapshot = AuditSourceSnapshot(source_type="github_file", source_id="src/statement_policy_service.py", content_hash="sha256:test")
    document = CollectedDocument(
        snapshot=snapshot,
        source_type="github_file",
        source_id="src/statement_policy_service.py",
        title="statement_policy_service.py",
        body="\n".join(
            [
                "def persist_statement():",
                "    # approval and allowlist are enforced here",
                "    return guarded_write()",
            ]
        ),
        path_hint="src/statement_policy_service.py",
    )

    records = extract_claim_records(documents=[document])
    subject_keys = {record.claim.subject_key for record in records}
    predicates = {record.claim.predicate for record in records}

    assert "Statement.write_path" in subject_keys
    assert "Statement.policy" in subject_keys
    assert "implemented_policy" in predicates


def test_claim_extractor_adds_ast_based_python_function_claims() -> None:
    snapshot = AuditSourceSnapshot(source_type="github_file", source_id="src/statement_router.py", content_hash="sha256:test")
    document = CollectedDocument(
        snapshot=snapshot,
        source_type="github_file",
        source_id="src/statement_router.py",
        title="statement_router.py",
        body="\n".join(
            [
                "class StatementService:",
                "    def persist_statement(self):",
                "        return guarded_write()",
                "",
                "@router.get('/statements')",
                "def list_statements():",
                "    return load_statements()",
            ]
        ),
        path_hint="src/statement_router.py",
    )

    records = extract_claim_records(documents=[document])
    ast_records = [record for record in records if record.claim.metadata.get("ast_extracted") is True]

    assert any(record.claim.subject_key == "Statement.write_path" for record in ast_records)
    assert any(record.claim.subject_key == "Statement.read_path" for record in ast_records)
    assert any(record.evidence.location.position and record.evidence.location.position.section_path == "StatementService.persist_statement" for record in ast_records)


def test_claim_extractor_builds_static_write_call_graph_and_persistence_metadata() -> None:
    snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/finai/services/statement_writer.py",
        content_hash="sha256:static",
    )
    document = CollectedDocument(
        snapshot=snapshot,
        source_type="github_file",
        source_id="src/finai/services/statement_writer.py",
        title="statement_writer.py",
        body="\n".join(
            [
                "class Neo4jDriver:",
                "    def execute_query(self, query: str, **params):",
                "        return True",
                "",
                "class StatementRepository:",
                "    def __init__(self):",
                "        self.driver = Neo4jDriver()",
                "",
                "    def save(self, statement):",
                "        return self.driver.execute_query(\"MERGE (s:Statement {id: $id}) SET s.payload = $payload\", statement=statement)",
                "",
                "class StatementWriter:",
                "    def __init__(self):",
                "        self.driver = Neo4jDriver()",
                "        self.repo = StatementRepository()",
                "",
                "    @retry",
                "    def persist_statement(self, statements):",
                "        with self.driver.session() as session:",
                "            for statement in statements:",
                "                self.repo.save(statement)",
            ]
        ),
        path_hint="src/finai/services/statement_writer.py",
    )

    records = extract_claim_records(documents=[document])
    persist_record = next(
        record
        for record in records
        if record.claim.subject_key == "Statement.write_path"
        and record.claim.metadata.get("evidence_section_path") == "StatementWriter.persist_statement"
    )

    assert "StatementWriter.persist_statement -> StatementRepository.save" in persist_record.claim.metadata["static_call_graph_paths"]
    assert any("Neo4jDriver.execute_query" in path for path in persist_record.claim.metadata["static_call_graph_paths"])
    assert "StatementRepository" in persist_record.claim.metadata["repository_adapters"]
    assert "Neo4jDriver" in persist_record.claim.metadata["driver_adapters"]
    assert "Neo4jDriver.session" in persist_record.claim.metadata["transaction_boundaries"]
    assert "@retry" in persist_record.claim.metadata["retry_paths"]
    assert "statement in statements" in persist_record.claim.metadata["batch_paths"]
    assert "StatementRepository.save" in persist_record.claim.metadata["db_write_api_calls"]
    assert "Neo4jDriver.execute_query" in persist_record.claim.metadata["db_write_api_calls"]
    assert "neo4j" in persist_record.claim.metadata["persistence_backends"]
    assert "neo4j_merge_node" in persist_record.claim.metadata["persistence_operation_types"]
    assert "neo4j_set_properties" in persist_record.claim.metadata["persistence_operation_types"]
    assert "Node:Statement" in persist_record.claim.metadata["persistence_schema_targets"]


def test_claim_extractor_resolves_intermodule_call_graph_and_constructor_injection() -> None:
    driver_snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/finai/infra/neo4j_driver.py",
        content_hash="sha256:driver",
    )
    driver_document = CollectedDocument(
        snapshot=driver_snapshot,
        source_type="github_file",
        source_id="src/finai/infra/neo4j_driver.py",
        title="neo4j_driver.py",
        body="\n".join(
            [
                "class Neo4jDriver:",
                "    def session(self):",
                "        return self",
                "",
                "    def execute_query(self, query: str, **params):",
                "        return True",
            ]
        ),
        path_hint="src/finai/infra/neo4j_driver.py",
    )
    repository_snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/finai/repositories/statement_repository.py",
        content_hash="sha256:repo",
    )
    repository_document = CollectedDocument(
        snapshot=repository_snapshot,
        source_type="github_file",
        source_id="src/finai/repositories/statement_repository.py",
        title="statement_repository.py",
        body="\n".join(
            [
                "from finai.infra.neo4j_driver import Neo4jDriver",
                "",
                "class StatementRepository:",
                "    def __init__(self, driver: Neo4jDriver):",
                "        self.driver = driver",
                "",
                "    def save(self, statement):",
                "        return self.driver.execute_query(\"MERGE (s:Statement {id: $id}) SET s.payload = $payload\", statement=statement)",
            ]
        ),
        path_hint="src/finai/repositories/statement_repository.py",
    )
    writer_snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/finai/services/statement_writer.py",
        content_hash="sha256:writer",
    )
    writer_document = CollectedDocument(
        snapshot=writer_snapshot,
        source_type="github_file",
        source_id="src/finai/services/statement_writer.py",
        title="statement_writer.py",
        body="\n".join(
            [
                "from finai.repositories.statement_repository import StatementRepository",
                "",
                "class StatementWriter:",
                "    def __init__(self, repo: StatementRepository):",
                "        self.repo = repo",
                "",
                "    def persist_statement(self, statement):",
                "        return self.repo.save(statement)",
            ]
        ),
        path_hint="src/finai/services/statement_writer.py",
    )

    records = extract_claim_records(documents=[writer_document, repository_document, driver_document])
    persist_record = next(
        record
        for record in records
        if record.claim.subject_key == "Statement.write_path"
        and record.claim.metadata.get("evidence_section_path") == "StatementWriter.persist_statement"
    )

    assert "StatementWriter.persist_statement -> StatementRepository.save" in persist_record.claim.metadata["static_call_graph_paths"]
    assert any(
        "Neo4jDriver.execute_query" in path
        for path in persist_record.claim.metadata["static_call_graph_paths"]
    )
    assert any(
        path.endswith("finai.repositories.statement_repository.StatementRepository.save")
        for path in persist_record.claim.metadata["static_call_graph_qualified_paths"]
    )
    assert any(
        "finai.infra.neo4j_driver.Neo4jDriver.execute_query" in path
        for path in persist_record.claim.metadata["static_call_graph_qualified_paths"]
    )
    assert "StatementRepository" in persist_record.claim.metadata["repository_adapters"]
    assert "Neo4jDriver" in persist_record.claim.metadata["driver_adapters"]
    assert "finai.repositories.statement_repository.StatementRepository" in persist_record.claim.metadata["repository_adapter_symbols"]
    assert "finai.infra.neo4j_driver.Neo4jDriver" in persist_record.claim.metadata["driver_adapter_symbols"]
    assert "repo=finai.repositories.statement_repository.StatementRepository" in persist_record.claim.metadata["constructor_injection_bindings"]
    assert "driver=finai.infra.neo4j_driver.Neo4jDriver" in persist_record.claim.metadata["constructor_injection_bindings"]


def test_claim_extractor_tracks_local_variable_flow_across_alias_driver_and_session() -> None:
    snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/finai/services/statement_writer.py",
        content_hash="sha256:alias-flow",
    )
    document = CollectedDocument(
        snapshot=snapshot,
        source_type="github_file",
        source_id="src/finai/services/statement_writer.py",
        title="statement_writer.py",
        body="\n".join(
            [
                "class Neo4jDriver:",
                "    def session(self):",
                "        return self",
                "",
                "    def execute_query(self, query: str, **params):",
                "        return True",
                "",
                "class StatementRepository:",
                "    def __init__(self):",
                "        self.driver = Neo4jDriver()",
                "",
                "class StatementWriter:",
                "    def __init__(self):",
                "        self.repo = StatementRepository()",
                "",
                "    def persist_statement(self, statement):",
                "        repo = self.repo",
                "        driver = repo.driver",
                "        session = driver.session()",
                "        return session.execute_query(\"MERGE (s:Statement {id: $id}) SET s.payload = $payload\", statement=statement)",
            ]
        ),
        path_hint="src/finai/services/statement_writer.py",
    )

    records = extract_claim_records(documents=[document])
    persist_record = next(
        record
        for record in records
        if record.claim.subject_key == "Statement.write_path"
        and record.claim.metadata.get("evidence_section_path") == "StatementWriter.persist_statement"
    )

    assert any(
        "Neo4jDriver.execute_query" in path
        for path in persist_record.claim.metadata["static_call_graph_paths"]
    )
    assert "Neo4jDriver" in persist_record.claim.metadata["driver_adapters"]
    assert "finai.services.statement_writer.Neo4jDriver" in persist_record.claim.metadata["driver_adapter_symbols"]
    assert "Neo4jDriver.session" in persist_record.claim.metadata["transaction_boundaries"]
    assert "Neo4jDriver.execute_query" in persist_record.claim.metadata["db_write_api_calls"]


def test_claim_extractor_resolves_protocol_and_inherited_repository_methods_across_files() -> None:
    driver_snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/finai/infra/neo4j_driver.py",
        content_hash="sha256:driver-protocol",
    )
    driver_document = CollectedDocument(
        snapshot=driver_snapshot,
        source_type="github_file",
        source_id="src/finai/infra/neo4j_driver.py",
        title="neo4j_driver.py",
        body="\n".join(
            [
                "class Neo4jDriver:",
                "    def execute_query(self, query: str, **params):",
                "        return True",
            ]
        ),
        path_hint="src/finai/infra/neo4j_driver.py",
    )
    contract_snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/finai/contracts/statement_repository.py",
        content_hash="sha256:contract-protocol",
    )
    contract_document = CollectedDocument(
        snapshot=contract_snapshot,
        source_type="github_file",
        source_id="src/finai/contracts/statement_repository.py",
        title="statement_repository.py",
        body="\n".join(
            [
                "from typing import Protocol",
                "",
                "class StatementRepositoryContract(Protocol):",
                "    def save(self, statement):",
                "        ...",
            ]
        ),
        path_hint="src/finai/contracts/statement_repository.py",
    )
    base_snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/finai/repositories/base_statement_repository.py",
        content_hash="sha256:base-protocol",
    )
    base_document = CollectedDocument(
        snapshot=base_snapshot,
        source_type="github_file",
        source_id="src/finai/repositories/base_statement_repository.py",
        title="base_statement_repository.py",
        body="\n".join(
            [
                "from finai.infra.neo4j_driver import Neo4jDriver",
                "",
                "class BaseStatementRepository:",
                "    def __init__(self, driver: Neo4jDriver):",
                "        self.driver = driver",
                "",
                "    def save(self, statement):",
                "        return self.driver.execute_query(\"MERGE (s:Statement {id: $id}) SET s.payload = $payload\", statement=statement)",
            ]
        ),
        path_hint="src/finai/repositories/base_statement_repository.py",
    )
    repository_snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/finai/repositories/statement_repository_impl.py",
        content_hash="sha256:repo-protocol",
    )
    repository_document = CollectedDocument(
        snapshot=repository_snapshot,
        source_type="github_file",
        source_id="src/finai/repositories/statement_repository_impl.py",
        title="statement_repository_impl.py",
        body="\n".join(
            [
                "from finai.contracts.statement_repository import StatementRepositoryContract",
                "from finai.infra.neo4j_driver import Neo4jDriver",
                "from finai.repositories.base_statement_repository import BaseStatementRepository",
                "",
                "class StatementRepository(BaseStatementRepository, StatementRepositoryContract):",
                "    def __init__(self, driver: Neo4jDriver):",
                "        super().__init__(driver)",
            ]
        ),
        path_hint="src/finai/repositories/statement_repository_impl.py",
    )
    writer_snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/finai/services/statement_writer.py",
        content_hash="sha256:writer-protocol",
    )
    writer_document = CollectedDocument(
        snapshot=writer_snapshot,
        source_type="github_file",
        source_id="src/finai/services/statement_writer.py",
        title="statement_writer.py",
        body="\n".join(
            [
                "from finai.contracts.statement_repository import StatementRepositoryContract",
                "",
                "class StatementWriter:",
                "    def __init__(self, repo: StatementRepositoryContract):",
                "        self.repo = repo",
                "",
                "    def persist_statement(self, statement):",
                "        repo = self.repo",
                "        return repo.save(statement)",
            ]
        ),
        path_hint="src/finai/services/statement_writer.py",
    )

    records = extract_claim_records(
        documents=[writer_document, repository_document, base_document, contract_document, driver_document]
    )
    persist_record = next(
        record
        for record in records
        if record.claim.subject_key == "Statement.write_path"
        and record.claim.metadata.get("evidence_section_path") == "StatementWriter.persist_statement"
    )

    assert any(
        "BaseStatementRepository.save" in path
        for path in persist_record.claim.metadata["static_call_graph_paths"]
    )
    assert any(
        "Neo4jDriver.execute_query" in path
        for path in persist_record.claim.metadata["static_call_graph_paths"]
    )
    assert "StatementRepositoryContract" in persist_record.claim.metadata["repository_adapters"]
    assert "StatementRepository" in persist_record.claim.metadata["repository_adapters"]
    assert "finai.contracts.statement_repository.StatementRepositoryContract" in persist_record.claim.metadata["repository_adapter_symbols"]
    assert "finai.repositories.statement_repository_impl.StatementRepository" in persist_record.claim.metadata["repository_adapter_symbols"]


def test_claim_extractor_validates_schema_targets_against_metamodel_catalog() -> None:
    meta_snapshot = AuditSourceSnapshot(source_type="metamodel", source_id="current_dump", content_hash="sha256:meta-schema")
    meta_document = CollectedDocument(
        snapshot=meta_snapshot,
        source_type="metamodel",
        source_id="current_dump",
        title="FIN-AI Metamodell Current Dump",
        body="""
[
  {"entity_kind": "metaclass", "metaclass_id": "mc_1", "metaclass_name": "Statement", "package_names": ["Governance"], "outbound_relation_types": ["CONTAINS"]},
  {"entity_kind": "label_summary", "label": "Statement", "node_count": 12},
  {"entity_kind": "relationship_summary", "relation_type": "CONTAINS", "relation_count": 8}
]
""".strip(),
        path_hint="data/metamodel/current_dump.json",
    )
    code_snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/finai/services/statement_writer.py",
        content_hash="sha256:code-schema",
    )
    code_document = CollectedDocument(
        snapshot=code_snapshot,
        source_type="github_file",
        source_id="src/finai/services/statement_writer.py",
        title="statement_writer.py",
        body="\n".join(
            [
                "class StatementRepository:",
                "    def save(self, statement):",
                "        return driver.execute_query(\"MERGE (s:Statement {id: $id}) SET s.payload = $payload\", statement=statement)",
            ]
        ),
        path_hint="src/finai/services/statement_writer.py",
    )

    records = extract_claim_records(documents=[meta_document, code_document])
    write_record = next(record for record in records if record.claim.subject_key == "Statement.write_path")

    assert write_record.claim.metadata["schema_validation_status"] == "ssot_confirmed"
    assert "Node:Statement" in write_record.claim.metadata["schema_validated_targets"]
    assert not write_record.claim.metadata["schema_unconfirmed_targets"]


def test_claim_extractor_adds_typescript_and_config_claims() -> None:
    ts_snapshot = AuditSourceSnapshot(source_type="github_file", source_id="web/src/services/statementApi.ts", content_hash="sha256:ts")
    ts_document = CollectedDocument(
        snapshot=ts_snapshot,
        source_type="github_file",
        source_id="web/src/services/statementApi.ts",
        title="statementApi.ts",
        body="\n".join(
            [
                "export async function updateStatement(statementId: string) {",
                "  return api.patch(`/statements/${statementId}`);",
                "}",
                "",
                "export async function loadStatement(statementId: string) {",
                "  return api.get(`/statements/${statementId}`);",
                "}",
            ]
        ),
        path_hint="web/src/services/statementApi.ts",
    )
    config_snapshot = AuditSourceSnapshot(source_type="github_file", source_id="config/contracts/write_allowlist.yaml", content_hash="sha256:yaml")
    config_document = CollectedDocument(
        snapshot=config_snapshot,
        source_type="github_file",
        source_id="config/contracts/write_allowlist.yaml",
        title="write_allowlist.yaml",
        body="\n".join(
            [
                "statement:",
                "  approval_policy: review_only",
                "  write_contract: guarded_write",
            ]
        ),
        path_hint="config/contracts/write_allowlist.yaml",
    )

    records = extract_claim_records(documents=[ts_document, config_document])
    subject_keys = {record.claim.subject_key for record in records}
    predicates = {record.claim.predicate for record in records}

    assert "Statement.write_path" in subject_keys
    assert "Statement.read_path" in subject_keys
    assert "Statement.policy" in subject_keys
    assert "implemented_write" in predicates
    assert "implemented_policy" in predicates
    assert any(record.claim.metadata.get("ts_extracted") is True for record in records)


def test_claim_extractor_adds_structured_claim_metadata() -> None:
    snapshot = AuditSourceSnapshot(source_type="github_file", source_id="src/statement_policy_service.py", content_hash="sha256:test")
    document = CollectedDocument(
        snapshot=snapshot,
        source_type="github_file",
        source_id="src/statement_policy_service.py",
        title="statement_policy_service.py",
        body="\n".join(
            [
                "def persist_statement():",
                "    # approval and allowlist are enforced here",
                "    return guarded_write()",
            ]
        ),
        path_hint="src/statement_policy_service.py",
    )

    records = extract_claim_records(documents=[document])
    policy_record = next(record for record in records if record.claim.subject_key == "Statement.policy")

    assert policy_record.claim.metadata["claim_subject_root"] == "Statement"
    assert policy_record.claim.metadata["claim_property"] == "policy"
    assert policy_record.claim.metadata["claim_scope_key"] == "Statement"
    assert policy_record.claim.metadata["claim_operator"] in {"requires", "describes"}
    assert str(policy_record.claim.metadata["claim_focus_value"]).strip()
    assert policy_record.claim.metadata["source_governance_level"] == "implementation"
    assert policy_record.claim.assertion_status == "asserted"
    assert policy_record.claim.source_authority == "implementation"


def test_bsm_domain_extractor_respects_metamodel_source_type_and_negation() -> None:
    target_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="_docs/bsm/target.md", content_hash="sha256:target"),
        source_type="local_doc",
        source_id="_docs/bsm/target.md",
        title="Target Architecture",
        body="summarisedAnswer entfaellt und soll kein eigenstaendiges Element mehr sein.\nrun_id ist kein SSOT und bleibt nur sekundaer.",
        path_hint="_docs/bsm/target.md",
    )
    puml_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="github_file", source_id="models/flow.puml", content_hash="sha256:puml"),
        source_type="github_file",
        source_id="models/flow.puml",
        title="flow.puml",
        body="note right of Workflow: No TO_MODIFY in target workflow",
        path_hint="models/flow.puml",
    )
    metamodel_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="metamodel", source_id="current_dump", content_hash="sha256:meta"),
        source_type="metamodel",
        source_id="current_dump",
        title="current_dump",
        body='[{"entity_kind":"metaclass","metaclass_name":"summarisedAnswer"}]',
        path_hint="data/metamodel/current_dump.json",
    )

    records = extract_claim_records(documents=[target_doc, puml_doc, metamodel_doc])

    role_record = next(record for record in records if record.claim.subject_key == "summarisedAnswer.role" and record.claim.source_type == "local_doc")
    run_record = next(record for record in records if record.claim.subject_key == "Run.model")
    to_modify_record = next(record for record in records if record.claim.subject_key == "TO_MODIFY.role")
    metamodel_record = next(record for record in records if record.claim.predicate == "metamodel_entity_exists")

    assert role_record.claim.assertion_status == "deprecated"
    assert role_record.claim.source_authority == "ssot"
    assert run_record.claim.normalized_value == "run_id_secondary_only"
    assert to_modify_record.claim.normalized_value == "excluded"
    assert metamodel_record.claim.source_type == "metamodel"


def test_schema_truth_registry_prefers_confirmed_targets_over_unconfirmed_inference() -> None:
    confirmed_claim = AuditClaimEntry(
        source_type="metamodel",
        source_id="current_dump",
        subject_kind="object_property",
        subject_key="Statement.write_path",
        predicate="metamodel_write_contract",
        normalized_value="Node:Statement",
        scope_kind="project",
        scope_key="FINAI",
        confidence=0.9,
        fingerprint="meta|statement",
        source_authority="ssot",
        metadata={
            "schema_validated_targets": ["Node:Statement"],
            "schema_validation_status": "ssot_confirmed",
        },
    )
    inferred_claim = AuditClaimEntry(
        source_type="github_file",
        source_id="src/finai/repositories/statement_repository.py",
        subject_kind="object_property",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="MERGE (s:Statement)",
        scope_kind="project",
        scope_key="FINAI",
        confidence=0.9,
        fingerprint="code|statement",
        source_authority="implementation",
        metadata={
            "schema_unconfirmed_targets": ["Node:Statement", "Node:StatementDraft"],
            "schema_validation_status": "unconfirmed",
        },
    )

    registry = build_schema_truth_registry(claims=[confirmed_claim, inferred_claim], truths=[])
    statement_entry = next(entry for entry in registry if entry.schema_key == "Node:Statement")
    draft_entry = next(entry for entry in registry if entry.schema_key == "Node:StatementDraft")

    assert statement_entry.status == "confirmed_ssot"
    assert statement_entry.source_kind == "metamodel"
    assert draft_entry.status == "code_only_inference"


def test_semantic_policy_variants_align_without_false_conflict() -> None:
    assert semantic_values_aligned(
        subject_key="Statement.policy",
        predicate="documented_policy",
        left_value="Write flow is approval-gated and review-only.",
        right_value="Persistence requires approval before save.",
    )
    assert not semantic_values_conflict(
        subject_key="Statement.policy",
        predicate="documented_policy",
        left_values={"write flow is approval-gated and review-only"},
        right_values={"persistence requires approval before save"},
    )


def test_claim_extractor_derives_process_phase_and_question_semantics_from_docs() -> None:
    snapshot = AuditSourceSnapshot(source_type="local_doc", source_id="_docs/bsm.md", content_hash="sha256:test")
    document = CollectedDocument(
        snapshot=snapshot,
        source_type="local_doc",
        source_id="_docs/bsm.md",
        title="BSM Contract",
        body="\n".join(
            [
                "# BSM",
                "Phase: Scoping has phase order 1 and 7 questions.",
                "Question: Problem Statement should stay mandatory.",
            ]
        ),
        path_hint="_docs/bsm.md",
    )

    records = extract_claim_records(documents=[document])
    subject_keys = {record.claim.subject_key for record in records}
    predicates = {record.claim.predicate for record in records}

    assert "BSM.process" in subject_keys
    assert "BSM.phase.scoping" in subject_keys
    assert "BSM.phase.scoping.question.problem_statement" in subject_keys
    assert "documented_phase_order" in predicates
    assert "documented_question_count" in predicates
    assert "documented_question_reference" in predicates


def test_claim_extractor_reads_extended_metamodel_entities() -> None:
    snapshot = AuditSourceSnapshot(source_type="metamodel", source_id="current_dump", content_hash="sha256:meta")
    document = CollectedDocument(
        snapshot=snapshot,
        source_type="metamodel",
        source_id="current_dump",
        title="FIN-AI Metamodell Current Dump",
        body="""
[
  {"entity_kind": "phase", "phase_id": "001", "phase_name": "Scoping", "phase_order": "001", "questions": []},
  {"entity_kind": "metaclass", "metaclass_id": "mc_1", "metaclass_name": "Statement", "package_names": ["Governance"], "outbound_relation_types": ["CONTAINS"]},
  {"entity_kind": "bsm_function", "function_id": "fn_1", "function_name": "Statement Extractor", "labels": ["bsmStatementExtractorFunction"], "relation_types": ["USES_EXTRACTOR_FN"], "question_keys": ["problem_statement"]},
  {"entity_kind": "label_summary", "label": "metaclass", "node_count": 42}
]
""".strip(),
        path_hint="data/metamodel/current_dump.json",
    )

    records = extract_claim_records(documents=[document])
    subject_keys = {record.claim.subject_key for record in records}
    predicates = {record.claim.predicate for record in records}

    assert "MetaClass.statement" in subject_keys
    assert "BSM.function.statement_extractor" in subject_keys
    assert "MetaModel.label.metaclass" in subject_keys
    assert "metamodel_metaclass" in predicates
    assert "metamodel_function" in predicates
    assert "label_count" in predicates


def test_semantic_process_variants_align_for_phase_reference_and_order() -> None:
    assert semantic_values_aligned(
        subject_key="BSM.process",
        predicate="phase_reference",
        left_value="Scoping",
        right_value="Phase: Scoping has phase order 1.",
    )
    assert semantic_values_aligned(
        subject_key="BSM.phase.scoping",
        predicate="phase_order",
        left_value="001",
        right_value="Phase order 1",
    )
    assert not semantic_values_conflict(
        subject_key="BSM.phase.scoping",
        predicate="phase_order",
        left_values={"001"},
        right_values={"Phase order 1"},
    )


def test_finding_engine_detects_process_semantic_conflicts_against_metamodel() -> None:
    metamodel_record = _claim_record(
        source_type="metamodel",
        source_id="metamodel_dump",
        title="Metamodell",
        subject_key="BSM.phase.scoping",
        predicate="phase_order",
        normalized_value="001",
        line_start=1,
    )
    doc_record = _claim_record(
        source_type="local_doc",
        source_id="_docs/bsm.md",
        title="BSM Contract",
        subject_key="BSM.phase.scoping",
        predicate="documented_phase_order",
        normalized_value="Phase order 2",
        line_start=5,
    )

    findings, _ = generate_findings(
        claim_records=[metamodel_record, doc_record],
        inherited_truths=[],
        impacted_scope_keys={"BSM.phase.scoping"},
    )

    contradictions = [finding for finding in findings if finding.category == "contradiction"]
    assert len(contradictions) == 1
    assert contradictions[0].metadata["delta_scope_affected"] is True
    assert any("phase_order" in signature for signature in contradictions[0].metadata["semantic_signatures"])


def test_finding_engine_avoids_unscoped_question_traceability_noise() -> None:
    orphan_question = _claim_record(
        source_type="metamodel",
        source_id="metamodel_dump",
        title="Metamodell",
        subject_key="BSM.phase.scoping.question.problem_statement",
        predicate="metamodel_question",
        normalized_value="Problem Statement",
        line_start=1,
    )

    findings, _ = generate_findings(
        claim_records=[orphan_question],
        inherited_truths=[],
        impacted_scope_keys=set(),
    )

    assert not any(
        finding.category == "traceability_gap"
        and finding.canonical_key == "BSM.phase.scoping.question.problem_statement"
        for finding in findings
    )


def test_finding_engine_flags_question_traceability_gap_when_phase_context_exists() -> None:
    metamodel_question = _claim_record(
        source_type="metamodel",
        source_id="metamodel_dump",
        title="Metamodell",
        subject_key="BSM.phase.scoping.question.problem_statement",
        predicate="metamodel_question",
        normalized_value="Problem Statement",
        line_start=1,
    )
    phase_doc = _claim_record(
        source_type="local_doc",
        source_id="_docs/bsm.md",
        title="BSM Contract",
        subject_key="BSM.phase.scoping",
        predicate="documented_phase_reference",
        normalized_value="Phase: Scoping",
        line_start=3,
    )

    findings, _ = generate_findings(
        claim_records=[metamodel_question, phase_doc],
        inherited_truths=[],
        impacted_scope_keys={"BSM.phase.scoping"},
    )

    assert any(
        finding.category == "traceability_gap"
        and finding.canonical_key == "BSM.phase.scoping.question.problem_statement"
        for finding in findings
    )


def test_document_claims_keep_heading_hierarchy_as_section_context() -> None:
    snapshot = AuditSourceSnapshot(source_type="confluence_page", source_id="123", content_hash="sha256:test")
    document = CollectedDocument(
        snapshot=snapshot,
        source_type="confluence_page",
        source_id="123",
        title="BSM Overview",
        body="\n".join(
            [
                "# Phase: Scoping",
                "## Question: Problem Statement",
                "Review status stays in draft until approval.",
            ]
        ),
        path_hint="Space FINAI / BSM Overview",
        metadata={"ancestor_titles": ["Governance", "BSM"]},
    )

    records = extract_claim_records(documents=[document])
    question_records = [record for record in records if record.claim.subject_key == "BSM.phase.scoping.question.problem_statement"]

    assert question_records
    assert all(
        record.evidence.location.position is not None
        and record.evidence.location.position.section_path == "Governance > BSM > BSM Overview > Phase: Scoping > Question: Problem Statement"
        for record in question_records
    )


def test_semantic_graph_builds_process_and_evidence_relations() -> None:
    phase_record = _claim_record(
        source_type="local_doc",
        source_id="_docs/bsm.md",
        title="BSM Contract",
        subject_key="BSM.phase.scoping",
        predicate="documented_phase_reference",
        normalized_value="Phase: Scoping",
        line_start=3,
    )
    question_record = _claim_record(
        source_type="github_file",
        source_id="src/statement_service.py",
        title="statement_service.py",
        subject_key="BSM.phase.scoping.question.problem_statement",
        predicate="implemented_question_reference",
        normalized_value="Question: Problem Statement",
        line_start=10,
    )

    graph = build_semantic_graph(
        run_id="audit_test",
        claim_records=[phase_record, question_record],
        truths=[],
    )

    entity_keys = {entity.canonical_key for entity in graph.semantic_entities}
    relation_types = {relation.relation_type for relation in graph.semantic_relations}

    assert "BSM.process" in entity_keys
    assert "BSM.phase.scoping" in entity_keys
    assert "BSM.phase.scoping.question.problem_statement" in entity_keys
    assert any(entity.entity_type == "documentation_section" for entity in graph.semantic_entities)
    assert any(entity.entity_type == "code_component" for entity in graph.semantic_entities)
    assert "belongs_to" in relation_types
    assert "documents" in relation_types
    assert "implements" in relation_types


def test_semantic_graph_builds_explicit_contract_chain_from_bsm_context() -> None:
    policy_location = AuditLocation(
        source_type="confluence_page",
        source_id="page-1",
        title="Statement Governance",
        path_hint="Space FINAI / Governance / Statement Governance",
        position=AuditPosition(
            anchor_kind="line",
            anchor_value="page-1:12",
            section_path="Governance > BSM > Statement Governance > Phase: Scoping > Question: Problem Statement",
            line_start=12,
            line_end=12,
        ),
    )
    write_location = AuditLocation(
        source_type="github_file",
        source_id="src/statement_service.py",
        title="statement_service.py",
        path_hint="src/statement_service.py",
        position=AuditPosition(
            anchor_kind="line",
            anchor_value="src/statement_service.py:48",
            section_path="StatementService.persist_statement",
            line_start=48,
            line_end=48,
        ),
    )
    policy_record = ExtractedClaimRecord(
        claim=AuditClaimEntry(
            source_snapshot_id=f"snapshot_{uuid4().hex}",
            source_type="confluence_page",
            source_id="page-1",
            subject_kind="object_property",
            subject_key="Statement.approval_policy",
            predicate="documented_approval_policy",
            normalized_value="Approval is required before save.",
            scope_kind="project",
            scope_key="FINAI",
            confidence=0.9,
            fingerprint=f"page-1:Statement.approval_policy:{uuid4().hex}",
            evidence_location_ids=[policy_location.location_id],
        ),
        evidence=ExtractedClaimEvidence(location=policy_location, matched_text="Approval is required before save."),
    )
    write_record = ExtractedClaimRecord(
        claim=AuditClaimEntry(
            source_snapshot_id=f"snapshot_{uuid4().hex}",
            source_type="github_file",
            source_id="src/statement_service.py",
            subject_kind="object_property",
            subject_key="Statement.write_path",
            predicate="implemented_write",
            normalized_value="persist_statement guarded_write",
            scope_kind="project",
            scope_key="FINAI",
            confidence=0.9,
            fingerprint=f"src/statement_service.py:Statement.write_path:{uuid4().hex}",
            evidence_location_ids=[write_location.location_id],
        ),
        evidence=ExtractedClaimEvidence(location=write_location, matched_text="persist_statement guarded_write"),
    )

    graph = build_semantic_graph(
        run_id="audit_test",
        claim_records=[policy_record, write_record],
        truths=[],
    )

    entity_by_key = {entity.canonical_key: entity for entity in graph.semantic_entities}
    policy_entity = entity_by_key["Statement.approval_policy"]
    write_entity = entity_by_key["Statement.write_path"]
    question_entity = entity_by_key["BSM.phase.scoping.question.problem_statement"]

    assert any(
        relation.source_entity_id == question_entity.entity_id
        and relation.relation_type == "governs"
        and relation.target_entity_id == policy_entity.entity_id
        for relation in graph.semantic_relations
    )
    assert any(
        relation.source_entity_id == policy_entity.entity_id
        and relation.relation_type == "governs"
        and relation.target_entity_id == write_entity.entity_id
        for relation in graph.semantic_relations
    )
    assert any("semantic_contract_paths" in claim.metadata for claim in graph.claims)
    assert any(
        "policy:Statement.approval_policy -> write_contract:Statement.write_path" in path
        for claim in graph.claims
        for path in claim.metadata.get("semantic_contract_paths", [])
    )


def test_causal_graph_builder_adds_write_decider_and_persistence_target_nodes() -> None:
    write_location = AuditLocation(
        source_type="github_file",
        source_id="src/finai/workers/job_worker.py",
        title="job_worker.py",
        path_hint="src/finai/workers/job_worker.py",
        position=AuditPosition(
            anchor_kind="line",
            anchor_value="src/finai/workers/job_worker.py:48",
            section_path="JobWorker.persist_statement",
            line_start=48,
            line_end=48,
        ),
    )
    write_record = ExtractedClaimRecord(
        claim=AuditClaimEntry(
            source_snapshot_id=f"snapshot_{uuid4().hex}",
            source_type="github_file",
            source_id="src/finai/workers/job_worker.py",
            subject_kind="object_property",
            subject_key="Statement.write_path",
            predicate="implemented_write",
            normalized_value="persist_statement guarded_write",
            scope_kind="project",
            scope_key="FINAI",
            confidence=0.9,
            fingerprint=f"src/finai/workers/job_worker.py:Statement.write_path:{uuid4().hex}",
            evidence_location_ids=[write_location.location_id],
            metadata={
                "evidence_section_path": "JobWorker.persist_statement",
                "path_hint": "src/finai/workers/job_worker.py",
            },
        ),
        evidence=ExtractedClaimEvidence(location=write_location, matched_text="persist_statement guarded_write"),
    )
    semantic_graph = build_semantic_graph(
        run_id="audit_test",
        claim_records=[write_record],
        truths=[],
    )

    causal_graph = build_causal_graph(
        run_id="audit_test",
        claims=semantic_graph.claims,
        truths=[],
        semantic_entities=semantic_graph.semantic_entities,
        semantic_relations=semantic_graph.semantic_relations,
    )

    node_types_by_label = {(node.node_type, node.label) for node in causal_graph.nodes}
    edge_types = {(edge.edge_type, edge.propagation_mode) for edge in causal_graph.edges}

    assert ("write_decider", "JobWorker.persist_statement") in node_types_by_label
    assert ("code_anchor", "persist") in node_types_by_label
    assert ("persistence_target", "CustomerGraph.Node.Statement") in node_types_by_label
    assert ("decides_write", "truth_and_delta") in edge_types
    assert ("writes_to", "truth_and_delta") in edge_types


def test_causal_attribution_exposes_write_decider_and_persistence_target_context() -> None:
    write_location = AuditLocation(
        source_type="github_file",
        source_id="src/finai/agents/a3b_agent.py",
        title="a3b_agent.py",
        path_hint="src/finai/agents/a3b_agent.py",
        position=AuditPosition(
            anchor_kind="line",
            anchor_value="src/finai/agents/a3b_agent.py:55",
            section_path="A3B.persist_statement",
            line_start=55,
            line_end=55,
        ),
    )
    write_record = ExtractedClaimRecord(
        claim=AuditClaimEntry(
            source_snapshot_id=f"snapshot_{uuid4().hex}",
            source_type="github_file",
            source_id="src/finai/agents/a3b_agent.py",
            subject_kind="object_property",
            subject_key="Statement.write_path",
            predicate="implemented_write",
            normalized_value="persist_statement guarded_write",
            scope_kind="project",
            scope_key="FINAI",
            confidence=0.9,
            fingerprint=f"src/finai/agents/a3b_agent.py:Statement.write_path:{uuid4().hex}",
            evidence_location_ids=[write_location.location_id],
            metadata={
                "evidence_section_path": "A3B.persist_statement",
                "path_hint": "src/finai/agents/a3b_agent.py",
            },
        ),
        evidence=ExtractedClaimEvidence(location=write_location, matched_text="persist_statement guarded_write"),
    )
    semantic_graph = build_semantic_graph(
        run_id="audit_test",
        claim_records=[write_record],
        truths=[],
    )
    causal_graph = build_causal_graph(
        run_id="audit_test",
        claims=semantic_graph.claims,
        truths=[],
        semantic_entities=semantic_graph.semantic_entities,
        semantic_relations=semantic_graph.semantic_relations,
    )
    finding = AuditFinding(
        severity="high",
        category="implementation_drift",
        title="Write drift",
        summary="Persistenzpfad weicht ab.",
        recommendation="Persistenzpfad angleichen.",
        canonical_key="Statement.write_path",
        metadata={"object_key": "Statement.write_path"},
    )

    attributed = attach_causal_attribution_to_findings(
        findings=[finding],
        claims=semantic_graph.claims,
        semantic_entities=semantic_graph.semantic_entities,
        semantic_relations=semantic_graph.semantic_relations,
        causal_graph=causal_graph,
    )[0]

    assert "CustomerGraph.Node.Statement" in attributed.metadata["causal_persistence_targets"]
    assert any("persist_statement" in label for label in attributed.metadata["causal_write_decider_labels"])
    assert "persist" in attributed.metadata["causal_write_apis"]


def test_causal_graph_builder_distinguishes_relationship_and_history_sinks() -> None:
    relationship_location = AuditLocation(
        source_type="github_file",
        source_id="src/finai/agents/a4_agent.py",
        title="a4_agent.py",
        path_hint="src/finai/agents/a4_agent.py",
        position=AuditPosition(
            anchor_kind="line",
            anchor_value="src/finai/agents/a4_agent.py:72",
            section_path="A4.merge_relationship",
            line_start=72,
            line_end=72,
        ),
    )
    history_location = AuditLocation(
        source_type="github_file",
        source_id="src/finai/services/statement_history_service.py",
        title="statement_history_service.py",
        path_hint="src/finai/services/statement_history_service.py",
        position=AuditPosition(
            anchor_kind="line",
            anchor_value="src/finai/services/statement_history_service.py:91",
            section_path="StatementHistoryService.persist_version",
            line_start=91,
            line_end=91,
        ),
    )
    relationship_record = ExtractedClaimRecord(
        claim=AuditClaimEntry(
            source_snapshot_id=f"snapshot_{uuid4().hex}",
            source_type="github_file",
            source_id="src/finai/agents/a4_agent.py",
            subject_kind="object_property",
            subject_key="Relationship.write_path",
            predicate="implemented_write",
            normalized_value="repo.merge_relationship(relationship)",
            scope_kind="project",
            scope_key="FINAI",
            confidence=0.9,
            fingerprint=f"src/finai/agents/a4_agent.py:Relationship.write_path:{uuid4().hex}",
            evidence_location_ids=[relationship_location.location_id],
            metadata={"evidence_section_path": "A4.merge_relationship", "path_hint": "src/finai/agents/a4_agent.py"},
        ),
        evidence=ExtractedClaimEvidence(location=relationship_location, matched_text="repo.merge_relationship(relationship)"),
    )
    history_record = ExtractedClaimRecord(
        claim=AuditClaimEntry(
            source_snapshot_id=f"snapshot_{uuid4().hex}",
            source_type="github_file",
            source_id="src/finai/services/statement_history_service.py",
            subject_kind="object_property",
            subject_key="Statement.lifecycle",
            predicate="implemented_write",
            normalized_value="history_repo.save(version)",
            scope_kind="project",
            scope_key="FINAI",
            confidence=0.9,
            fingerprint=f"src/finai/services/statement_history_service.py:Statement.lifecycle:{uuid4().hex}",
            evidence_location_ids=[history_location.location_id],
            metadata={"evidence_section_path": "StatementHistoryService.persist_version", "path_hint": "src/finai/services/statement_history_service.py"},
        ),
        evidence=ExtractedClaimEvidence(location=history_location, matched_text="history_repo.save(version)"),
    )
    semantic_graph = build_semantic_graph(
        run_id="audit_test",
        claim_records=[relationship_record, history_record],
        truths=[],
    )

    causal_graph = build_causal_graph(
        run_id="audit_test",
        claims=semantic_graph.claims,
        truths=[],
        semantic_entities=semantic_graph.semantic_entities,
        semantic_relations=semantic_graph.semantic_relations,
    )

    sink_nodes = {
        node.label: str(node.metadata.get("sink_kind") or "")
        for node in causal_graph.nodes
        if node.node_type == "persistence_target"
    }
    write_api_nodes = {
        node.label
        for node in causal_graph.nodes
        if node.metadata.get("api_kind") == "db_write_api"
    }

    assert sink_nodes["CustomerGraph.Relationship"] == "relationship_sink"
    assert sink_nodes["CustomerGraph.History.Statement"] == "history_sink"
    assert "repo.merge_relationship" in write_api_nodes
    assert "history_repo.save" in write_api_nodes


def test_causal_graph_builder_models_adapters_transactions_and_schema_targets() -> None:
    snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/finai/services/statement_writer.py",
        content_hash="sha256:static_graph",
    )
    document = CollectedDocument(
        snapshot=snapshot,
        source_type="github_file",
        source_id="src/finai/services/statement_writer.py",
        title="statement_writer.py",
        body="\n".join(
            [
                "class Neo4jDriver:",
                "    def execute_query(self, query: str, **params):",
                "        return True",
                "",
                "class StatementRepository:",
                "    def __init__(self):",
                "        self.driver = Neo4jDriver()",
                "",
                "    def save(self, statement):",
                "        return self.driver.execute_query(\"MERGE (s:Statement {id: $id}) SET s.payload = $payload\", statement=statement)",
                "",
                "class StatementWriter:",
                "    def __init__(self):",
                "        self.driver = Neo4jDriver()",
                "        self.repo = StatementRepository()",
                "",
                "    @retry",
                "    def persist_statement(self, statements):",
                "        with self.driver.session() as session:",
                "            for statement in statements:",
                "                self.repo.save(statement)",
            ]
        ),
        path_hint="src/finai/services/statement_writer.py",
    )
    semantic_graph = build_semantic_graph(
        run_id="audit_test",
        claim_records=extract_claim_records(documents=[document]),
        truths=[],
    )
    causal_graph = build_causal_graph(
        run_id="audit_test",
        claims=semantic_graph.claims,
        truths=[],
        semantic_entities=semantic_graph.semantic_entities,
        semantic_relations=semantic_graph.semantic_relations,
    )

    assert any(node.node_type == "repository_adapter" and node.label == "StatementRepository" for node in causal_graph.nodes)
    assert any(node.node_type == "driver_adapter" and node.label == "Neo4jDriver" for node in causal_graph.nodes)
    assert any(node.node_type == "transaction_boundary" and node.label == "Neo4jDriver.session" for node in causal_graph.nodes)
    repository_node = next(node for node in causal_graph.nodes if node.node_type == "repository_adapter" and node.label == "StatementRepository")
    driver_node = next(node for node in causal_graph.nodes if node.node_type == "driver_adapter" and node.label == "Neo4jDriver")
    assert repository_node.metadata["repository_adapter_symbol"].endswith("StatementRepository")
    assert driver_node.metadata["driver_adapter_symbol"].endswith("Neo4jDriver")
    persistence_node = next(node for node in causal_graph.nodes if node.label == "CustomerGraph.Node.Statement")
    assert "neo4j" in persistence_node.metadata["persistence_backends"]
    assert "neo4j_merge_node" in persistence_node.metadata["persistence_operation_types"]
    assert "Node:Statement" in persistence_node.metadata["persistence_schema_targets"]


def test_finding_engine_detects_semantic_policy_conflicts() -> None:
    code_record = _claim_record(
        source_type="github_file",
        source_id="src/statement_service.py",
        title="statement_service.py",
        subject_key="Statement.policy",
        predicate="implemented_policy",
        normalized_value="Direct write without approval is allowed.",
        line_start=10,
    )
    doc_record = _claim_record(
        source_type="local_doc",
        source_id="_docs/statement.md",
        title="Statement Contract",
        subject_key="Statement.policy",
        predicate="documented_policy",
        normalized_value="Write flow is approval-gated and review-only.",
        line_start=5,
    )

    findings, _ = generate_findings(
        claim_records=[code_record, doc_record],
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    policy_conflicts = [finding for finding in findings if finding.category == "policy_conflict"]
    assert len(policy_conflicts) == 1
    assert policy_conflicts[0].metadata["delta_scope_affected"] is True
    assert "approval" in " ".join(policy_conflicts[0].metadata["semantic_signatures"]).casefold()


def test_finding_engine_enforces_only_explicit_truths() -> None:
    code_record = _claim_record(
        source_type="github_file",
        source_id="src/statement_service.py",
        title="statement_service.py",
        subject_key="Statement.policy",
        predicate="implemented_policy",
        normalized_value="Direct write without approval is allowed.",
        line_start=10,
    )
    implicit_truth = TruthLedgerEntry(
        canonical_key="Statement.policy|phase_source",
        subject_kind="object_property",
        subject_key="Statement.policy",
        predicate="phase_source",
        normalized_value="Write flow is approval-gated and review-only.",
        scope_kind="project",
        scope_key="FINAI",
        source_kind="system_inference",
    )
    explicit_truth = implicit_truth.model_copy(
        update={
            "truth_id": "truth_explicit",
            "canonical_key": "Statement.policy|user_specification",
            "predicate": "user_specification",
            "source_kind": "user_specification",
        }
    )

    implicit_findings, _ = generate_findings(
        claim_records=[code_record],
        inherited_truths=[implicit_truth],
        impacted_scope_keys={"Statement"},
    )
    explicit_findings, _ = generate_findings(
        claim_records=[code_record],
        inherited_truths=[explicit_truth],
        impacted_scope_keys={"Statement"},
    )

    assert not any(f.metadata.get("truth_enforcement") is True for f in implicit_findings)
    assert any(f.metadata.get("truth_enforcement") is True for f in explicit_findings)


def test_causal_attribution_prefers_governing_policy_over_write_contract() -> None:
    write_entity = SemanticEntity(
        run_id="run_test",
        entity_type="write_contract",
        canonical_key="Statement.write_path",
        label="Statement.write_path",
        scope_key="Statement",
    )
    policy_entity = SemanticEntity(
        run_id="run_test",
        entity_type="policy",
        canonical_key="Statement.policy",
        label="Statement.policy",
        scope_key="Statement",
    )
    relation = SemanticRelation(
        run_id="run_test",
        source_entity_id=policy_entity.entity_id,
        target_entity_id=write_entity.entity_id,
        relation_type="governs",
        confidence=0.94,
    )
    claim = AuditClaimEntry(
        source_type="github_file",
        source_id="src/finai/workers/job_worker.py",
        subject_kind="object_property",
        subject_key="Statement.write_path",
        predicate="implemented_as",
        normalized_value="guarded_write",
        scope_kind="project",
        scope_key="FINAI",
        confidence=0.9,
        fingerprint="Statement.write_path|implemented_as|guarded_write|FINAI",
        metadata={"semantic_entity_ids": [write_entity.entity_id]},
    )
    finding = AuditFinding(
        severity="high",
        category="implementation_drift",
        title="Write path drift",
        summary="Write path weicht ab.",
        recommendation="Write path pruefen.",
        canonical_key="Statement.write_path",
        metadata={
            "object_key": "Statement.write_path",
            "semantic_entity_ids": [write_entity.entity_id],
        },
    )

    attributed = attach_causal_attribution_to_findings(
        findings=[finding],
        claims=[claim],
        semantic_entities=[write_entity, policy_entity],
        semantic_relations=[relation],
    )[0]

    assert attributed.metadata["causal_root_cause_bucket"] == "policy"
    assert "governs" in str(attributed.metadata["causal_root_cause_path"])


def test_retrieval_selection_expands_beyond_base_limit_for_missing_core_root_cause() -> None:
    findings: list[AuditFinding] = []
    for index in range(12):
        findings.append(
            AuditFinding(
                severity="high",
                category="policy_conflict",
                title=f"Policy root {index}",
                summary="Core-Problem im Policy-Bucket.",
                recommendation="Policy festziehen.",
                canonical_key=f"Statement.policy_{index}",
                metadata={
                    "object_key": f"Statement.policy_{index}",
                    "causal_root_cause_bucket": "policy",
                    "causal_root_cause_confidence": 0.91,
                },
            )
        )
    findings.append(
        AuditFinding(
            severity="high",
            category="contradiction",
            title="Lifecycle root cause",
            summary="Anderer Core-Bucket ausserhalb der Basisgrenze.",
            recommendation="Lifecycle konsolidieren.",
            canonical_key="Statement.lifecycle",
            metadata={
                "object_key": "Statement.lifecycle",
                "causal_root_cause_bucket": "lifecycle",
                "causal_root_cause_confidence": 0.93,
            },
        )
    )

    selected = select_findings_for_retrieval(
        findings=findings,
        base_max_findings=12,
        hard_cap_findings=20,
    )

    assert len(selected) == 13
    assert any(finding.title == "Lifecycle root cause" for finding in selected)


def test_build_recommendation_contexts_expands_snippet_budget_for_truth_findings(tmp_path) -> None:
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=tmp_path / "metamodel.json")
    finding = AuditFinding(
        severity="critical",
        category="contradiction",
        title="Truth mismatch",
        summary="Explizite Wahrheit wird nicht gespiegelt.",
        recommendation="Alle Quellen angleichen.",
        canonical_key="Statement.policy",
        metadata={
            "object_key": "Statement.policy",
            "truth_enforcement": True,
            "causal_root_cause_bucket": "truth",
        },
    )
    segments = [
        RetrievalSegment(
            run_id="run_test",
            source_type="local_doc",
            source_id=f"doc_{index}",
            title=f"Doc {index}",
            anchor_kind="document_line_range",
            anchor_value=f"doc_{index}#L1",
            ordinal=index,
            content="Statement policy must stay approval-gated and review-only.",
            segment_hash=f"hash_{index}",
            keywords=["statement", "policy", "approval", "review"],
        )
        for index in range(6)
    ]

    contexts = build_recommendation_contexts(
        settings=settings,
        findings=[finding],
        segments=segments,
        allow_remote_embeddings=False,
        lexical_search=lambda _query, _limit: [(segment.segment_id, 10.0 - index) for index, segment in enumerate(segments)],
        limit_per_finding=3,
        max_findings=1,
    )

    assert len(contexts[finding.canonical_key]) == 5


def test_retrieval_index_builds_segments_and_claim_links(tmp_path) -> None:
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=tmp_path / "metamodel.json")
    snapshot = AuditSourceSnapshot(source_type="local_doc", source_id="statement.md", content_hash="sha256:test")
    document = CollectedDocument(
        snapshot=snapshot,
        source_type="local_doc",
        source_id="statement.md",
        title="Statement Contract",
        body="\n".join(
            [
                "# Statement",
                "Write path is approval-gated and review-only.",
                "",
                "## Lifecycle",
                "Review status controls persistence.",
            ]
        ),
        path_hint="_docs/statement.md",
    )

    claim_records = extract_claim_records(documents=[document])
    result = build_retrieval_index(
        settings=settings,
        run_id="audit_test",
        documents=[document],
        claim_records=claim_records,
        previous_segments=[],
        allow_remote_embeddings=False,
    )

    assert len(result.segments) >= 1
    assert len(result.claim_links) >= 1
    assert any("Retrieval-Index aufgebaut" in note for note in result.notes)
    assert any(segment.delta_status == "added" for segment in result.segments)


def test_consensus_detector_weights_ssot_and_code_above_historical_doc() -> None:
    claim_records = [
        _record_for_consensus(
            source_type="local_doc",
            source_id="_docs/legacy_as_is_statement.md",
            title="Statement AS_IS",
            path_hint="_docs/legacy_as_is_statement.md",
            subject_key="Statement.policy",
            predicate="policy_state",
            normalized_value="direct write allowed",
        ),
        _record_for_consensus(
            source_type="local_doc",
            source_id="_docs/target_reference_statement.md",
            title="Statement Target SSOT",
            path_hint="_docs/target_reference_statement.md",
            subject_key="Statement.policy",
            predicate="policy_state",
            normalized_value="approval-gated",
        ),
        _record_for_consensus(
            source_type="github_file",
            source_id="src/finai/services/statement_policy_service.py",
            title="statement_policy_service.py",
            path_hint="src/finai/services/statement_policy_service.py",
            subject_key="Statement.policy",
            predicate="policy_state",
            normalized_value="approval-gated",
        ),
    ]

    findings = detect_consensus_deviations(claim_records=claim_records)

    assert not any(finding.category == "clarification_needed" for finding in findings)
    contradiction = next(finding for finding in findings if finding.category == "contradiction")
    assert contradiction.metadata["consensus_value"] == "approval-gated"
    assert contradiction.locations[0].source_id == "_docs/legacy_as_is_statement.md"


def test_documentation_gap_detector_surfaces_missing_subscope_when_root_is_documented() -> None:
    doc_record = _record_for_consensus(
        source_type="local_doc",
        source_id="_docs/statement.md",
        title="Statement",
        path_hint="_docs/statement.md",
        subject_key="Statement",
        predicate="documented_entity",
        normalized_value="Statement exists.",
    )
    code_record = _record_for_consensus(
        source_type="github_file",
        source_id="src/finai/services/statement_policy_service.py",
        title="statement_policy_service.py",
        path_hint="src/finai/services/statement_policy_service.py",
        subject_key="Statement.policy",
        predicate="implemented_policy",
        normalized_value="approval-gated",
    )
    documents = [
        CollectedDocument(
            snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="_docs/statement.md", content_hash="sha256:doc"),
            source_type="local_doc",
            source_id="_docs/statement.md",
            title="Statement",
            body="# Statement\nDieses Dokument beschreibt Statement im Überblick.\n" * 3,
            path_hint="_docs/statement.md",
        )
    ]

    findings = detect_documentation_gaps(claim_records=[doc_record, code_record], documents=documents)

    assert len(findings) == 1
    assert findings[0].canonical_key == "doc_gap:Statement.policy"
    assert findings[0].metadata["root_documented"] is True


def test_build_finding_links_uses_causal_group_dependencies() -> None:
    root = AuditFinding(
        severity="high",
        category="contradiction",
        title="BSM root",
        summary="Kernproblem im BSM-Prozess.",
        recommendation="Root Cause zuerst lösen.",
        canonical_key="BSM.process",
        metadata={
            "causal_group_key": "process:BSM.process",
            "causal_scope_keys": ["BSM.phase.scoping", "Statement"],
            "causal_root_cause_bucket": "process",
        },
    )
    detail = AuditFinding(
        severity="medium",
        category="missing_documentation",
        title="Worker-Doku",
        summary="Unterstützende Doku fehlt.",
        recommendation="Nachziehen.",
        canonical_key="job_worker.persist_statement",
        metadata={
            "causal_group_key": "process:BSM.process",
            "causal_scope_keys": ["BSM.phase.scoping", "Statement"],
            "causal_root_cause_bucket": "process",
        },
    )

    links = build_finding_links(findings=[root, detail])

    assert len(links) == 1
    assert links[0].relation_type == "depends_on"
    assert links[0].from_finding_id == detail.finding_id


def test_jira_issue_payload_contains_structured_ai_coding_sections() -> None:
    payload = build_jira_issue_payload(
        brief=JiraTicketAICodingBrief(
            ticket_key="FINAI-1",
            ticket_url="https://finius.atlassian.net/browse/FINAI-1",
            title="Statement Write Contract angleichen",
            problem_description="Code und Doku widersprechen sich beim Write-Vertrag.",
            reason="Der fachliche Vertrag driftet auseinander.",
            correction_measures=["Write-Logik konsolidieren"],
            target_state=["Ein konsistenter Write-Vertrag"],
            acceptance_criteria=["Tests belegen den neuen Vertrag"],
            implications=["Benachbarte Doku muss mitziehen"],
            affected_parts=["src/statement_service.py"],
            evidence=["Confluence Seite Statement Contract"],
            implementation_notes=["Guardrails beachten"],
            validation_steps=["pytest fuer betroffene Pfade"],
            ai_coding_prompt="Arbeite im FIN-AI Repo und gleiche den Write-Vertrag ab.",
        ),
        project_key="FINAI",
    )

    assert payload["fields"]["project"]["key"] == "FINAI"
    assert payload["fields"]["issuetype"]["name"] == "Story"
    description = payload["fields"]["description"]
    assert description["type"] == "doc"
    headings = [node for node in description["content"] if node.get("type") == "heading"]
    assert any(node.get("content", [{}])[0].get("text") == "Pruefbare Abnahmekriterien" for node in headings)


def test_retrieval_insights_mark_delta_relevant_findings(tmp_path) -> None:
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=tmp_path / "metamodel.json")
    snapshot = AuditSourceSnapshot(source_type="local_doc", source_id="statement.md", content_hash="sha256:test")
    document = CollectedDocument(
        snapshot=snapshot,
        source_type="local_doc",
        source_id="statement.md",
        title="Statement Contract",
        body="# Statement\nWrite path is approval-gated and review-only.\n",
        path_hint="_docs/statement.md",
    )
    claim_records = extract_claim_records(documents=[document])
    result = build_retrieval_index(
        settings=settings,
        run_id="audit_test",
        documents=[document],
        claim_records=claim_records,
        previous_segments=[],
        allow_remote_embeddings=False,
    )
    finding = AuditFinding(
        severity="high",
        category="implementation_drift",
        title="Statement Write Contract driftet",
        summary="Dokumentierter und technischer Write-Vertrag widersprechen sich.",
        recommendation="Write-Vertrag angleichen.",
        locations=[
            AuditLocation(
                source_type="local_doc",
                source_id="statement.md",
                title="Statement Contract",
                path_hint="_docs/statement.md",
                position=AuditPosition(
                    anchor_kind="document_section",
                    anchor_value=result.segments[0].anchor_value,
                    section_path=result.segments[0].section_path,
                ),
            )
        ],
        metadata={},
    )

    enriched = attach_retrieval_insights_to_findings(findings=[finding], segments=result.segments)[0]

    assert enriched.metadata["retrieval_segment_count"] >= 1
    assert enriched.metadata["delta_signal"] == "added"
    assert enriched.metadata["delta_summary"]


def _claim_record(
    *,
    source_type: str,
    source_id: str,
    title: str,
    subject_key: str,
    predicate: str,
    normalized_value: str,
    line_start: int,
) -> ExtractedClaimRecord:
    location = AuditLocation(
        source_type=source_type,  # type: ignore[arg-type]
        source_id=source_id,
        title=title,
        path_hint=source_id,
        position=AuditPosition(
            anchor_kind="line",
            anchor_value=f"{source_id}:{line_start}",
            section_path=subject_key,
            line_start=line_start,
            line_end=line_start,
        ),
    )
    claim = AuditClaimEntry(
        source_snapshot_id=f"snapshot_{uuid4().hex}",
        source_type=source_type,  # type: ignore[arg-type]
        source_id=source_id,
        subject_kind="object_property",
        subject_key=subject_key,
        predicate=predicate,
        normalized_value=normalized_value,
        scope_kind="project",
        scope_key="FINAI",
        confidence=0.9,
        fingerprint=f"{source_id}:{subject_key}:{predicate}:{uuid4().hex}",
        evidence_location_ids=[location.location_id],
    )
    return ExtractedClaimRecord(
        claim=claim,
        evidence=ExtractedClaimEvidence(location=location, matched_text=normalized_value),
    )
