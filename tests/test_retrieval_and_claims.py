from __future__ import annotations

import json
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
from fin_ai_auditor.services.bsm_domain_contradiction_detector import detect_bsm_domain_contradictions
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
from fin_ai_auditor.services.semantic_graph_service import attach_semantic_context_to_findings, build_semantic_graph


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


def test_bsm_domain_extractor_uses_note_block_context_for_puml_claims() -> None:
    target_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="_docs/bsm/status.md", content_hash="sha256:status"),
        source_type="local_doc",
        source_id="_docs/bsm/status.md",
        title="Statuslogik",
        body="Statement ist zentrales reviewbares Evidenzartefakt mit Accept/Reject/Modify.",
        path_hint="_docs/bsm/status.md",
    )
    puml_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="github_file", source_id="models/flow.puml", content_hash="sha256:puml-note"),
        source_type="github_file",
        source_id="models/flow.puml",
        title="flow.puml",
        body="\n".join(
            [
                "@startuml",
                "note right of Statement",
                "No HITL decisions in MVP",
                "end note",
                "@enduml",
            ]
        ),
        path_hint="models/flow.puml",
    )

    records = extract_claim_records(documents=[target_doc, puml_doc])
    puml_hitl_record = next(
        record
        for record in records
        if record.claim.subject_key == "Statement.hitl"
        and record.claim.predicate == "puml_hitl_decision"
    )
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert puml_hitl_record.claim.normalized_value == "excluded"
    assert any(f.metadata.get("subject_key") == "Statement.hitl" for f in findings)


def test_bsm_domain_detector_finds_chain_conflict_for_alias_based_cypher_target() -> None:
    target_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="_docs/bsm/target.md", content_hash="sha256:target-chain"),
        source_type="local_doc",
        source_id="_docs/bsm/target.md",
        title="Target Architecture",
        body="Die Evidenzkette ist bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element.",
        path_hint="_docs/bsm/target.md",
    )
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/bsm_statement_consolidation_service.py",
            content_hash="sha256:code-chain",
        ),
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        body="""
def persist_statement_chain():
    query = \"\"\"
    MATCH (sa:summarisedAnswer {id:$summary_id})
    CREATE (s:Statement {id:$statement_id})
    MERGE (s)-[:DERIVED_FROM]->(sa)
    \"\"\"
""".strip(),
        path_hint="src/finai/bsm_statement_consolidation_service.py",
    )

    records = extract_claim_records(documents=[target_doc, code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert any(f.metadata.get("subject_key") == "EvidenceChain.direction" for f in findings)


def test_bsm_domain_detector_emits_code_risk_findings_for_temporal_and_schema_gaps() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/router_mining.py",
            content_hash="sha256:code-risks",
        ),
        source_type="github_file",
        source_id="src/finai/router_mining.py",
        title="router_mining.py",
        body="""
def save_manual_answer():
    persist_answer(answer)
    enqueue_reaggregation(answer.id)

def rebuild_chain():
    supersede_old_statements()
    build_new_statements()

def build_response_schema():
    response_schema = {
        "text": {"type": "string"},
        "confidence": {"type": "number"},
        "rationale": {"type": "string"},
    }
    return response_schema
""".strip(),
        path_hint="src/finai/router_mining.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)
    titles = {finding.title for finding in findings}

    assert "Eventual-Consistency-Luecke im manuellen Antwortpfad" in titles
    assert "Reaggregation unterbricht die aktive BSM-Kette" in titles
    assert "Statement-Output-Schema verliert notwendige Unit-Zuordnung" in titles
    eventual_finding = next(f for f in findings if f.metadata.get("risk_predicate") == "code_eventual_consistency_risk")
    chain_finding = next(f for f in findings if f.metadata.get("risk_predicate") == "code_chain_interruption_risk")
    assert "code_temporal_sequence" in eventual_finding.metadata["support_predicates"]
    assert eventual_finding.metadata["eventual_consistency_subtype"] == "manual_answer_enqueue"
    assert eventual_finding.metadata["eventual_consistency_label"] == "Manueller Antwortpfad"
    assert eventual_finding.metadata["sequence_values"] == ["persist_before_enqueue"]
    assert eventual_finding.metadata["sequence_functions"] == ["save_manual_answer"]
    assert eventual_finding.metadata["sequence_line_windows"] == ["L2→L3"]
    assert eventual_finding.metadata["sequence_break_mode"] == "async_gap"
    assert eventual_finding.metadata["observed_sequence_path"] == ["persist", "enqueue"]
    assert eventual_finding.metadata["expected_sequence_path"] == ["persist", "protected_reaggregation", "enqueue"]
    assert eventual_finding.metadata["missing_sequence_segments"] == ["protected_reaggregation"]
    assert eventual_finding.metadata["sequence_break_before"] == "persist"
    assert eventual_finding.metadata["sequence_break_after"] == "enqueue"
    assert eventual_finding.metadata["sequence_rejoin_at"] == "enqueue"
    assert eventual_finding.metadata["matched_sequence_variants"] == [
        {
            "function_name": "save_manual_answer",
            "line_window": "L2→L3",
            "sequence_break_mode": "async_gap",
            "observed_sequence_path": ["persist", "enqueue"],
            "expected_sequence_path": ["persist", "protected_reaggregation", "enqueue"],
            "missing_sequence_segments": ["protected_reaggregation"],
            "missing_sequence_segment_path": "protected_reaggregation",
            "sequence_break_before": "persist",
            "sequence_break_after": "enqueue",
            "sequence_rejoin_at": "enqueue",
        }
    ]
    assert "code_temporal_sequence" in chain_finding.metadata["support_predicates"]
    assert chain_finding.metadata["sequence_values"] == ["supersede_before_rebuild"]
    assert chain_finding.metadata["sequence_functions"] == ["rebuild_chain"]
    assert chain_finding.metadata["sequence_line_windows"] == ["L6→L7"]
    assert chain_finding.metadata["sequence_break_mode"] == "replacement_gap"
    assert chain_finding.metadata["observed_sequence_path"] == ["supersede", "rebuild"]
    assert chain_finding.metadata["expected_sequence_path"] == ["supersede", "replacement_chain_available", "rebuild"]
    assert chain_finding.metadata["missing_sequence_segments"] == ["replacement_chain_available"]
    assert chain_finding.metadata["sequence_break_before"] == "supersede"
    assert chain_finding.metadata["sequence_break_after"] == "rebuild"
    assert chain_finding.metadata["sequence_rejoin_at"] == "rebuild"


def test_bsm_domain_detector_flags_missing_phase_run_id_on_refine_path() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/router_bsm_readiness.py",
            content_hash="sha256:refine-gap",
        ),
        source_type="github_file",
        source_id="src/finai/router_bsm_readiness.py",
        title="router_bsm_readiness.py",
        body="""
def refine_statement_version(summary_id, statement_id):
    payload = {
        "statement_id": statement_id,
        "target_label": "Statement",
    }
    rebuild_bsm_element_from_statement(payload)
""".strip(),
        path_hint="src/finai/router_bsm_readiness.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_field_propagation_gap"
        and finding.metadata.get("subject_key") == "Statement.field_propagation"
    )
    assert "code_missing_required_field" in finding.metadata["support_predicates"]
    assert "code_propagation_context" in finding.metadata["support_predicates"]
    assert finding.metadata["missing_fields"] == ["phase_run_id"]
    assert finding.metadata["propagation_contexts"] == ["name_context"]
    assert finding.metadata["propagation_break_mode"] == "field_drop"
    assert finding.metadata["propagation_path"] == ["refine_statement_version", "Statement"]
    assert finding.metadata["expected_propagation_fields"] == ["phase_run_id"]
    assert finding.metadata["missing_field_segments"] == ["phase_run_id"]
    assert finding.metadata["propagation_break_before"] == "refine_statement_version"
    assert finding.metadata["propagation_break_after"] == "Statement"
    assert finding.metadata["propagation_rejoin_at"] == "Statement"
    assert finding.metadata["matched_propagation_variants"] == [
        {
            "function_name": "refine_statement_version",
            "target_entity": "Statement",
            "propagation_break_mode": "field_drop",
            "propagation_path": ["refine_statement_version", "Statement"],
            "expected_propagation_fields": ["phase_run_id"],
            "missing_field_segments": ["phase_run_id"],
            "propagation_contexts": ["name_context"],
            "propagation_break_before": "refine_statement_version",
            "propagation_break_after": "Statement",
            "propagation_rejoin_at": "Statement",
        }
    ]


def test_bsm_domain_extractor_emits_support_claims_for_temporal_and_field_gaps() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/router_bsm_readiness.py",
            content_hash="sha256:support-claims",
        ),
        source_type="github_file",
        source_id="src/finai/router_bsm_readiness.py",
        title="router_bsm_readiness.py",
        body="""
def save_manual_answer():
    persist_answer(answer)
    enqueue_reaggregation(answer.id)

def refine_statement_version(summary_id, statement_id):
    payload = {
        "statement_id": statement_id,
        "target_label": "Statement",
    }
    rebuild_bsm_element_from_statement(payload)
""".strip(),
        path_hint="src/finai/router_bsm_readiness.py",
    )

    records = extract_claim_records(documents=[code_doc])
    support_claims = {
        (record.claim.subject_key, record.claim.predicate, record.claim.normalized_value)
        for record in records
        if record.claim.subject_kind == "bsm_domain"
    }

    assert (
        "TemporalConsistency.persist_then_enqueue",
        "code_temporal_sequence",
        "persist_before_enqueue",
    ) in support_claims
    assert (
        "Statement.field_propagation",
        "code_missing_required_field",
        "phase_run_id",
    ) in support_claims
    assert (
        "Statement.field_propagation",
        "code_propagation_context",
        "name_context",
    ) in support_claims


def test_bsm_domain_detector_follows_enqueue_helper_from_persisting_function() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/router_mining.py",
            content_hash="sha256:helper-enqueue",
        ),
        source_type="github_file",
        source_id="src/finai/router_mining.py",
        title="router_mining.py",
        body="""
def _enqueue_reaggregation_after_manual_answer():
    enqueue_bsm_reaggregation_job()

def save_phase_run_answer():
    write_cypher_guarded(
        rule_key="persist_bsm_answer_artifact",
        params={"id": "answer-1"},
    )
    _enqueue_reaggregation_after_manual_answer()
""".strip(),
        path_hint="src/finai/router_mining.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_eventual_consistency_risk"
    )
    assert finding.category == "read_write_gap"
    assert finding.metadata["eventual_consistency_subtype"] == "manual_answer_enqueue"
    assert finding.metadata["sequence_functions"] == ["save_phase_run_answer"]
    assert finding.metadata["sequence_values"] == ["persist_before_enqueue"]
    assert finding.metadata["sequence_break_mode"] == "async_gap"
    assert finding.metadata["missing_sequence_segments"] == ["protected_reaggregation"]


def test_bsm_domain_detector_classifies_ingestion_async_gap() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/api/routers/router_ingestion.py",
            content_hash="sha256:ingestion-async-gap",
        ),
        source_type="github_file",
        source_id="src/finai/api/routers/router_ingestion.py",
        title="router_ingestion.py",
        body="""
def ingest_stream():
    persist_uploaded_stream(stream)
    enqueue_ingestion_job(stream.id)
""".strip(),
        path_hint="src/finai/api/routers/router_ingestion.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_eventual_consistency_risk"
    )
    assert finding.title == "Eventual-Consistency-Luecke im Connector-/Ingestion-Pfad"
    assert finding.metadata["eventual_consistency_subtype"] == "connector_ingestion_enqueue"
    assert finding.metadata["eventual_consistency_label"] == "Connector-/Ingestion-Pfad"


def test_bsm_domain_detector_classifies_upload_async_gap() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/api/routers/router_ui_files.py",
            content_hash="sha256:upload-async-gap",
        ),
        source_type="github_file",
        source_id="src/finai/api/routers/router_ui_files.py",
        title="router_ui_files.py",
        body="""
def upload_files():
    persist_uploaded_file()
    enqueue_ingestion_job()
""".strip(),
        path_hint="src/finai/api/routers/router_ui_files.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_eventual_consistency_risk"
    )
    assert finding.title == "Eventual-Consistency-Luecke im Datei-/Upload-Pfad"
    assert finding.metadata["eventual_consistency_subtype"] == "upload_enqueue"
    assert finding.metadata["eventual_consistency_label"] == "Datei-/Upload-Pfad"


def test_bsm_domain_detector_does_not_classify_module_scope_storage_index_as_upload_path() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/core/services/storage_index_service.py",
            content_hash="sha256:storage-index-module-scope",
        ),
        source_type="github_file",
        source_id="src/finai/core/services/storage_index_service.py",
        title="storage_index_service.py",
        body="""
from finai.core.services.jobs.graph_job_queue_service import enqueue_job

async def upsert_entry():
    persist_storage_index()

helper_text = "enqueue later"
""".strip(),
        path_hint="src/finai/core/services/storage_index_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert [
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_eventual_consistency_risk"
    ] == []


def test_bsm_domain_detector_ignores_docstring_examples_for_eventual_consistency() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/core/services/storage_index_service.py",
            content_hash="sha256:storage-index-docstring-example",
        ),
        source_type="github_file",
        source_id="src/finai/core/services/storage_index_service.py",
        title="storage_index_service.py",
        body='''
"""
USAGE:
------
await svc.upsert_entry(
    storage_path="input/doc.pdf",
)
"""

def real_code():
    enqueue_job()
'''.strip(),
        path_hint="src/finai/core/services/storage_index_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert [
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_eventual_consistency_risk"
    ] == []


def test_bsm_domain_detector_classifies_storage_reconcile_async_gap() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/core/services/storage_index_service.py",
            content_hash="sha256:storage-index-reconcile-async-gap",
        ),
        source_type="github_file",
        source_id="src/finai/core/services/storage_index_service.py",
        title="storage_index_service.py",
        body="""
class StorageIndexService:
    async def reconcile_from_storage(self):
        def _flatten_storage_items():
            return []

        persist_storage_index()
        enqueue_job()
""".strip(),
        path_hint="src/finai/core/services/storage_index_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_eventual_consistency_risk"
    )
    assert finding.title == "Eventual-Consistency-Luecke im Storage-Reconcile-Pfad"
    assert finding.metadata["eventual_consistency_subtype"] == "storage_reconcile_enqueue"
    assert finding.metadata["eventual_consistency_label"] == "Storage-Reconcile-Pfad"


def test_bsm_domain_detector_classifies_workflow_registry_async_gap() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/core/services/ingestion/progress_tracker.py",
            content_hash="sha256:workflow-registry-async-gap",
        ),
        source_type="github_file",
        source_id="src/finai/core/services/ingestion/progress_tracker.py",
        title="progress_tracker.py",
        body="""
async def set_registry():
    persist_workflow_registry()
    enqueue_agent_notification()
""".strip(),
        path_hint="src/finai/core/services/ingestion/progress_tracker.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_eventual_consistency_risk"
    )
    assert finding.title == "Eventual-Consistency-Luecke im Workflow-Registry-Pfad"
    assert finding.metadata["eventual_consistency_subtype"] == "workflow_registry_enqueue"
    assert finding.metadata["eventual_consistency_label"] == "Workflow-Registry-Pfad"


def test_bsm_domain_detector_classifies_recovery_async_gap_separately() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/core/services/ingestion/recovery.py",
            content_hash="sha256:recovery-async-gap",
        ),
        source_type="github_file",
        source_id="src/finai/core/services/ingestion/recovery.py",
        title="recovery.py",
        body="""
async def recover_orphan_mining_tasks():
    persist_recovery_state()
    enqueue_embedding_inputsource_job()
""".strip(),
        path_hint="src/finai/core/services/ingestion/recovery.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_eventual_consistency_risk"
    )
    assert finding.title == "Eventual-Consistency-Luecke im Ingestion-Recovery-Pfad"
    assert finding.metadata["eventual_consistency_subtype"] == "ingestion_recovery_enqueue"
    assert finding.metadata["eventual_consistency_label"] == "Ingestion-Recovery-Pfad"


def test_bsm_domain_detector_groups_manual_answer_enqueue_paths() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/router_mining.py",
            content_hash="sha256:grouped-manual-answer-enqueue",
        ),
        source_type="github_file",
        source_id="src/finai/router_mining.py",
        title="router_mining.py",
        body="""
def save_phase_run_answer():
    persist_answer(answer)
    enqueue_reaggregation(answer.id)

def update_manual_answer():
    persist_answer(answer)
    enqueue_reaggregation(answer.id)

def delete_manual_answer():
    persist_answer(answer)
    enqueue_reaggregation(answer.id)
""".strip(),
        path_hint="src/finai/router_mining.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    eventual_findings = [
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_eventual_consistency_risk"
    ]
    assert len(eventual_findings) == 1
    finding = eventual_findings[0]
    assert finding.title == "Eventual-Consistency-Luecke im manuellen Antwortpfad"
    assert finding.metadata["eventual_consistency_subtype"] == "manual_answer_enqueue"
    assert finding.metadata["grouped_eventual_paths"] is True
    assert finding.metadata["eventual_path_type"] == "manual_answer_enqueue"
    assert finding.metadata["path_count"] == 3
    assert finding.metadata["sequence_functions"] == [
        "delete_manual_answer",
        "save_phase_run_answer",
        "update_manual_answer",
    ]


def test_bsm_domain_detector_groups_connector_ingestion_async_paths() -> None:
    records = extract_claim_records(
        documents=[
            CollectedDocument(
                snapshot=AuditSourceSnapshot(
                    source_type="github_file",
                    source_id="src/finai/api/routers/router_ingestion.py",
                    content_hash="sha256:grouped-connector-ingestion-router",
                ),
                source_type="github_file",
                source_id="src/finai/api/routers/router_ingestion.py",
                title="router_ingestion.py",
                body="""
def ingest_stream():
    persist_uploaded_stream(stream)
    enqueue_ingestion_job(stream.id)
""".strip(),
                path_hint="src/finai/api/routers/router_ingestion.py",
            ),
            CollectedDocument(
                snapshot=AuditSourceSnapshot(
                    source_type="github_file",
                    source_id="src/finai/core/services/atlassian_connector_ingest_helpers.py",
                    content_hash="sha256:grouped-connector-ingestion-helper",
                ),
                source_type="github_file",
                source_id="src/finai/core/services/atlassian_connector_ingest_helpers.py",
                title="atlassian_connector_ingest_helpers.py",
                body="""
def persist_and_ingest_markdown():
    persist_binary_artifact()
    enqueue_ingestion_job()

def persist_and_ingest_binary():
    persist_binary_artifact()
    enqueue_ingestion_job()
""".strip(),
                path_hint="src/finai/core/services/atlassian_connector_ingest_helpers.py",
            ),
            CollectedDocument(
                snapshot=AuditSourceSnapshot(
                    source_type="github_file",
                    source_id="src/finai/core/services/ingestion/connector_orchestrator.py",
                    content_hash="sha256:grouped-connector-ingestion-orchestrator",
                ),
                source_type="github_file",
                source_id="src/finai/core/services/ingestion/connector_orchestrator.py",
                title="connector_orchestrator.py",
                body="""
def execute_connector_sync():
    persist_connector_state()
    enqueue_ingestion_job()
""".strip(),
                path_hint="src/finai/core/services/ingestion/connector_orchestrator.py",
            ),
        ]
    )

    findings = detect_bsm_domain_contradictions(claim_records=records)

    eventual_findings = [
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_eventual_consistency_risk"
    ]
    assert len(eventual_findings) == 1
    finding = eventual_findings[0]
    assert finding.title == "Eventual-Consistency-Luecke im Connector-/Ingestion-Pfad"
    assert finding.metadata["eventual_consistency_subtype"] == "connector_ingestion_enqueue"
    assert finding.metadata["grouped_eventual_paths"] is True
    assert finding.metadata["eventual_path_type"] == "connector_ingestion_enqueue"
    assert finding.metadata["path_count"] == 4
    assert finding.metadata["sequence_functions"] == [
        "execute_connector_sync",
        "ingest_stream",
        "persist_and_ingest_binary",
        "persist_and_ingest_markdown",
    ]


def test_bsm_domain_detector_groups_phase_execution_async_paths() -> None:
    records = extract_claim_records(
        documents=[
            CollectedDocument(
                snapshot=AuditSourceSnapshot(
                    source_type="github_file",
                    source_id="src/finai/core/services/bsm_phase_execution_service.py",
                    content_hash="sha256:grouped-phase-execution-service",
                ),
                source_type="github_file",
                source_id="src/finai/core/services/bsm_phase_execution_service.py",
                title="bsm_phase_execution_service.py",
                body="""
def enqueue_phase_for_chunk():
    persist_phase_state()
    enqueue_phase_job()
""".strip(),
                path_hint="src/finai/core/services/bsm_phase_execution_service.py",
            ),
            CollectedDocument(
                snapshot=AuditSourceSnapshot(
                    source_type="github_file",
                    source_id="src/finai/workers/job_worker_bsm_phase_helpers.py",
                    content_hash="sha256:grouped-phase-execution-worker",
                ),
                source_type="github_file",
                source_id="src/finai/workers/job_worker_bsm_phase_helpers.py",
                title="job_worker_bsm_phase_helpers.py",
                body="""
def run_bsm_phase_followup():
    persist_phase_state()
    enqueue_phase_followup()
""".strip(),
                path_hint="src/finai/workers/job_worker_bsm_phase_helpers.py",
            ),
        ]
    )

    findings = detect_bsm_domain_contradictions(claim_records=records)

    eventual_findings = [
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_eventual_consistency_risk"
    ]
    assert len(eventual_findings) == 1
    finding = eventual_findings[0]
    assert finding.title == "Eventual-Consistency-Luecke im Phasen-Ausfuehrungspfad"
    assert finding.metadata["eventual_consistency_subtype"] == "phase_execution_enqueue"
    assert finding.metadata["grouped_eventual_paths"] is True
    assert finding.metadata["eventual_path_type"] == "phase_execution_enqueue"
    assert finding.metadata["path_count"] == 2
    assert finding.metadata["sequence_functions"] == [
        "enqueue_phase_for_chunk",
        "run_bsm_phase_followup",
    ]


def test_bsm_domain_detector_groups_reaggregation_chain_interruptions() -> None:
    records = extract_claim_records(
        documents=[
            CollectedDocument(
                snapshot=AuditSourceSnapshot(
                    source_type="github_file",
                    source_id="src/finai/router_bsm_readiness.py",
                    content_hash="sha256:chain-group-router",
                ),
                source_type="github_file",
                source_id="src/finai/router_bsm_readiness.py",
                title="router_bsm_readiness.py",
                body="""
def rebuild_review_chain():
    supersede_old_statements()
    rebuild_bsm_elements()
""".strip(),
                path_hint="src/finai/router_bsm_readiness.py",
            ),
            CollectedDocument(
                snapshot=AuditSourceSnapshot(
                    source_type="github_file",
                    source_id="src/finai/core/services/bsm_reaggregation_service.py",
                    content_hash="sha256:chain-group-service",
                ),
                source_type="github_file",
                source_id="src/finai/core/services/bsm_reaggregation_service.py",
                title="bsm_reaggregation_service.py",
                body="""
def rebuild_chain():
    supersede_old_statements()
    build_new_statements()
""".strip(),
                path_hint="src/finai/core/services/bsm_reaggregation_service.py",
            ),
            CollectedDocument(
                snapshot=AuditSourceSnapshot(
                    source_type="github_file",
                    source_id="src/finai/core/services/bsm_statement_consolidation_service.py",
                    content_hash="sha256:chain-group-consolidation",
                ),
                source_type="github_file",
                source_id="src/finai/core/services/bsm_statement_consolidation_service.py",
                title="bsm_statement_consolidation_service.py",
                body="""
def materialize_statement_chain():
    supersede_old_elements()
    materialize_new_elements()
""".strip(),
                path_hint="src/finai/core/services/bsm_statement_consolidation_service.py",
            ),
        ]
    )

    findings = detect_bsm_domain_contradictions(claim_records=records)

    chain_findings = [
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_chain_interruption_risk"
    ]
    assert len(chain_findings) == 1
    finding = chain_findings[0]
    assert finding.category == "read_write_gap"
    assert finding.metadata["grouped_chain_paths"] is True
    assert finding.metadata["chain_path_type"] == "reaggregation_rebuild_path"
    assert finding.metadata["path_count"] == 3
    assert finding.metadata["sequence_values"] == ["supersede_before_rebuild"]
    assert finding.metadata["sequence_functions"] == [
        "materialize_statement_chain",
        "rebuild_chain",
        "rebuild_review_chain",
    ]
    assert finding.metadata["sequence_line_windows"] == ["L2→L3"]
    assert finding.metadata["grouped_sequence_variants"] is True
    assert finding.metadata["sequence_break_mode"] == "replacement_gap"
    assert finding.metadata["missing_sequence_segments"] == ["replacement_chain_available"]
    assert len(finding.metadata["matched_sequence_variants"]) == 3


def test_bsm_domain_detector_ignores_non_statement_schema_when_statement_schema_is_complete() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/bsm_statement_consolidation_service.py",
            content_hash="sha256:statement-schema-complete",
        ),
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        body="""
def normalize_unit():
    output_schema = {
        "type": "object",
        "required": ["text", "normalized_text", "confidence"],
        "properties": {
            "text": {"type": "string"},
            "normalized_text": {"type": "string"},
            "confidence": {"type": "number"},
        },
    }
    return output_schema

def generate_statements():
    prompt = "Statements mit rationale und unit_ids"
    output_schema = {
        "type": "object",
        "required": ["statements"],
        "properties": {
            "statements": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["text", "confidence", "rationale", "unit_ids"],
                    "properties": {
                        "text": {"type": "string"},
                        "confidence": {"type": "number"},
                        "rationale": {"type": "string"},
                        "unit_ids": {"type": "array"},
                    },
                },
            },
        },
    }
    return output_schema
""".strip(),
        path_hint="src/finai/bsm_statement_consolidation_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert not any(
        finding.metadata.get("risk_predicate") == "code_schema_missing_fields"
        for finding in findings
    )


def test_bsm_domain_detector_extracts_supports_hop_from_yaml_blocks_and_puml_context() -> None:
    yaml_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="config/policies/write_allowlist.yaml",
            content_hash="sha256:yaml-support-hop",
        ),
        source_type="github_file",
        source_id="config/policies/write_allowlist.yaml",
        title="write_allowlist.yaml",
        body="""
allowed_writes:
  - key: "persist_bsm_element_proposal_artifact"
    cypher_template: |
      MATCH (st:Statement {id: $statement_id})
      MATCH (e:BSM_Element {id: $element_id})
      MERGE (st)-[:SUPPORTS]->(e)
""".strip(),
        path_hint="config/policies/write_allowlist.yaml",
    )
    puml_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="models/finai_meta_ssot_pipeline_v2.puml",
            content_hash="sha256:puml-support-hop",
        ),
        source_type="github_file",
        source_id="models/finai_meta_ssot_pipeline_v2.puml",
        title="finai_meta_ssot_pipeline_v2.puml",
        body="""
partition "Agent 3b - BSM Elements" {
  :Materialize BSM_Element proposals from PROPOSED Statement;
  :Persist SUPPORTS + IN_RUN;
}
""".strip(),
        path_hint="models/finai_meta_ssot_pipeline_v2.puml",
    )

    records = extract_claim_records(documents=[yaml_doc, puml_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert any(
        record.claim.predicate == "yaml_evidence_chain_hop"
        and record.claim.subject_key == "EvidenceChain.hop_statement_element"
        for record in records
    )
    assert any(
        finding.category == "architecture_observation"
        and finding.metadata.get("subject_key") == "EvidenceChain.hop_statement_element"
        for finding in findings
    )


def test_bsm_domain_detector_ignores_refine_context_without_write_activity() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/router_bsm_readiness.py",
            content_hash="sha256:refine-no-write",
        ),
        source_type="github_file",
        source_id="src/finai/router_bsm_readiness.py",
        title="router_bsm_readiness.py",
        body="""
def get_manual_refine_status():
    return {
        "mode": "manual",
        "target_label": "Statement",
    }
""".strip(),
        path_hint="src/finai/router_bsm_readiness.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert not any(
        finding.metadata.get("risk_predicate") == "code_field_propagation_gap"
        for finding in findings
    )


def test_bsm_domain_detector_ignores_non_bsm_infrastructure_write_context_for_phase_run_gap() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/core/db/graph_context.py",
            content_hash="sha256:infra-phase-run-gap",
        ),
        source_type="github_file",
        source_id="src/finai/core/db/graph_context.py",
        title="graph_context.py",
        body="""
def upsert_document_with_chunks():
    mode = "manual"
    statement_payload = {"target_label": "Statement"}
    persist_chunks(statement_payload)
""".strip(),
        path_hint="src/finai/core/db/graph_context.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert not any(
        finding.metadata.get("risk_predicate") == "code_field_propagation_gap"
        for finding in findings
    )


def test_bsm_domain_detector_ignores_comment_only_temporal_noise() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/router_mining.py",
            content_hash="sha256:comment-only-temporal-noise",
        ),
        source_type="github_file",
        source_id="src/finai/router_mining.py",
        title="router_mining.py",
        body="""
def save_manual_answer():
    # persist_answer(answer)
    update_summary_sync(answer.id)
    # enqueue_reaggregation(answer.id)
""".strip(),
        path_hint="src/finai/router_mining.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert not any(
        finding.metadata.get("risk_predicate") == "code_eventual_consistency_risk"
        for finding in findings
    )
    assert not any(
        finding.metadata.get("risk_predicate") == "code_chain_interruption_risk"
        for finding in findings
    )


def test_bsm_domain_detector_ignores_comment_only_manual_context_for_field_gap() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/core/services/bsm_service.py",
            content_hash="sha256:comment-only-field-noise",
        ),
        source_type="github_file",
        source_id="src/finai/core/services/bsm_service.py",
        title="bsm_service.py",
        body="""
def capture_answer(payload):
    # mode = "manual"
    # statement_payload = {"target_label": "Statement"}
    write_cypher_guarded({"target_label": "Document"})
""".strip(),
        path_hint="src/finai/core/services/bsm_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert not any(
        finding.metadata.get("risk_predicate") == "code_field_propagation_gap"
        for finding in findings
    )


def test_bsm_domain_detector_flags_nearby_manual_write_context_without_phase_run_id() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/core/services/bsm_service.py",
            content_hash="sha256:manual-nearby-gap",
        ),
        source_type="github_file",
        source_id="src/finai/core/services/bsm_service.py",
        title="bsm_service.py",
        body="""
def capture_answer(payload):
    mode = "manual"
    statement_payload = {"target_label": "Statement"}
    write_cypher_guarded(statement_payload)
""".strip(),
        path_hint="src/finai/core/services/bsm_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_field_propagation_gap"
    )
    assert finding.metadata["propagation_contexts"] == ["line_context"]
    assert finding.metadata["propagation_break_mode"] == "field_drop"
    assert finding.metadata["propagation_path"] == ["capture_answer", "Statement"]


def test_bsm_domain_detector_ignores_distant_legacy_comment_before_bsm_write() -> None:
    filler = "\n".join(f"    marker_{idx} = {idx}" for idx in range(30))
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/core/services/source_cleanup_service.py",
            content_hash="sha256:distant-legacy-no-gap",
        ),
        source_type="github_file",
        source_id="src/finai/core/services/source_cleanup_service.py",
        title="source_cleanup_service.py",
        body=f"""
def cleanup_source():
    # legacy cleanup marker
{filler}
    statement_payload = {{"target_label": "Statement"}}
    write_cypher_guarded(statement_payload)
""".strip(),
        path_hint="src/finai/core/services/source_cleanup_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert not any(
        finding.metadata.get("risk_predicate") == "code_field_propagation_gap"
        for finding in findings
    )


def test_bsm_domain_detector_ignores_manual_raw_chain_comment_without_manual_write_path() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/workers/job_worker.py",
            content_hash="sha256:manual-raw-comment",
        ),
        source_type="github_file",
        source_id="src/finai/workers/job_worker.py",
        title="job_worker.py",
        body="""
def finalize_phase():
    # raw/manual_raw -> summarisedAnswerUnit -> Statement
    statement_payload = {"target_label": "Statement"}
    write_cypher_guarded(statement_payload)
""".strip(),
        path_hint="src/finai/workers/job_worker.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert not any(
        finding.metadata.get("risk_predicate") == "code_field_propagation_gap"
        for finding in findings
    )


def test_bsm_domain_detector_does_not_leak_class_scope_into_top_level_manual_answer_path() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/api/routers/router_bsm_readiness.py",
            content_hash="sha256:top-level-scope-reset",
        ),
        source_type="github_file",
        source_id="src/finai/api/routers/router_bsm_readiness.py",
        title="router_bsm_readiness.py",
        body="""
class PhaseCompletionStatusResponse:
    statement_count: int = 0

@router.post("/answer")
async def save_ba_answer(payload):
    run_id = "manual:phase-1"
    write_cypher_guarded(
        {
            "id": f"bsm_answer_raw:{run_id}:manual:q1",
            "rule_key": "persist_bsm_answer_artifact",
        }
    )

class LaterResponse:
    target_label = "Statement"
""".strip(),
        path_hint="src/finai/api/routers/router_bsm_readiness.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    relevant = [
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_field_propagation_gap"
    ]
    assert len(relevant) == 1
    assert relevant[0].metadata["subject_key"] == "bsmAnswer.field_propagation"
    assert "PhaseCompletionStatusResponse.save_ba_answer" not in relevant[0].summary
    assert relevant[0].metadata["propagation_path"] == ["save_ba_answer", "bsmAnswer"]


def test_bsm_domain_detector_ignores_response_statement_labels_after_bsm_answer_write() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/api/routers/router_bsm_readiness.py",
            content_hash="sha256:response-statement-noise",
        ),
        source_type="github_file",
        source_id="src/finai/api/routers/router_bsm_readiness.py",
        title="router_bsm_readiness.py",
        body="""
async def save_ba_answer(payload):
    run_id = "manual:phase-1"
    out = write_cypher_guarded(
        rule_key="persist_bsm_answer_artifact",
        params={"id": f"bsm_answer_raw:{run_id}:manual:q1"},
    )
    return {
        "warning": "Automatische Statement-Neuberechnung nach Antwortspeicherung ist deaktiviert.",
        "statements": 0,
    }
""".strip(),
        path_hint="src/finai/api/routers/router_bsm_readiness.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    relevant = [
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_field_propagation_gap"
    ]
    assert len(relevant) == 1
    assert relevant[0].metadata["subject_key"] == "bsmAnswer.field_propagation"
    assert relevant[0].metadata["matched_propagation_variants"][0]["target_entity"] == "bsmAnswer"


def test_bsm_domain_detector_classifies_manual_bsm_answer_gap_as_boundary_path() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/core/services/bsm_service.py",
            content_hash="sha256:manual-boundary-gap",
        ),
        source_type="github_file",
        source_id="src/finai/core/services/bsm_service.py",
        title="bsm_service.py",
        body="""
def capture_ba_answer(payload):
    run_id = "manual:phase-1"
    out = write_cypher_guarded(
        rule_key="persist_bsm_answer_artifact",
        params={"id": f"bsm_answer_raw:{run_id}:manual:q1"},
    )
    return out
""".strip(),
        path_hint="src/finai/core/services/bsm_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_field_propagation_gap"
    )
    assert finding.category == "legacy_path_gap"
    assert finding.metadata["boundary_path_type"] == "manual_answer_entrypoint"
    assert finding.metadata["legacy_path_gap"] is True
    assert finding.metadata["propagation_break_mode"] == "field_drop"
    assert finding.metadata["missing_field_segments"] == ["phase_run_id"]


def test_bsm_domain_detector_groups_manual_boundary_paths_across_sources() -> None:
    router_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/api/routers/router_bsm_readiness.py",
            content_hash="sha256:manual-boundary-router",
        ),
        source_type="github_file",
        source_id="src/finai/api/routers/router_bsm_readiness.py",
        title="router_bsm_readiness.py",
        body="""
async def save_ba_answer(payload):
    run_id = "manual:phase-1"
    write_cypher_guarded(
        rule_key="persist_bsm_answer_artifact",
        params={"id": f"bsm_answer_raw:{run_id}:manual:q1"},
    )
""".strip(),
        path_hint="src/finai/api/routers/router_bsm_readiness.py",
    )
    service_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/core/services/bsm_service.py",
            content_hash="sha256:manual-boundary-service",
        ),
        source_type="github_file",
        source_id="src/finai/core/services/bsm_service.py",
        title="bsm_service.py",
        body="""
def capture_ba_answer(payload):
    run_id = "manual:phase-1"
    write_cypher_guarded(
        rule_key="persist_bsm_answer_artifact",
        params={"id": f"bsm_answer_raw:{run_id}:manual:q1"},
    )
""".strip(),
        path_hint="src/finai/core/services/bsm_service.py",
    )

    records = extract_claim_records(documents=[router_doc, service_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    legacy_findings = [finding for finding in findings if finding.category == "legacy_path_gap"]
    assert len(legacy_findings) == 1
    finding = legacy_findings[0]
    assert finding.metadata["grouped_boundary_paths"] is True
    assert finding.metadata["path_count"] == 2
    assert sorted(finding.metadata["boundary_function_names"]) == ["capture_ba_answer", "save_ba_answer"]
    assert finding.metadata["grouped_propagation_variants"] is True
    assert finding.metadata["propagation_break_mode"] == "field_drop"
    assert len(finding.metadata["matched_propagation_variants"]) == 2
    assert len(finding.locations) == 2


def test_normalize_claim_value_keeps_non_empty_fallback_for_structured_function_claims() -> None:
    from fin_ai_auditor.services.claim_extractor import _normalize_claim_value

    assert _normalize_claim_value(
        matched_text="jira_process_preflight [implemented_policy]: ",
        predicate="implemented_policy",
    ) == "jira_process_preflight [implemented_policy]:"


def test_bsm_domain_extractor_emits_explicit_evidence_chain_step_claims() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/bsm_statement_consolidation_service.py",
            content_hash="sha256:evidence-steps",
        ),
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        body="""
def materialize_statement_edges():
    query = \"\"\"
    MERGE (s:Statement)-[:DERIVED_FROM]->(u:summarisedAnswerUnit)
    MERGE (s:Statement)-[:SUPPORTS]->(e:BSM_Element)
    \"\"\"
    return query
""".strip(),
        path_hint="src/finai/bsm_statement_consolidation_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    hop_claims = {
        (
            record.claim.subject_key,
            record.claim.predicate,
            record.claim.normalized_value,
            str(record.claim.metadata.get("relationship_type") or ""),
        )
        for record in records
        if record.claim.subject_kind == "bsm_domain"
    }

    assert (
        "EvidenceChain.step.Statement.DERIVED_FROM.summarisedAnswerUnit",
        "code_evidence_chain_step",
        "Statement -[:DERIVED_FROM]-> summarisedAnswerUnit",
        "DERIVED_FROM",
    ) in hop_claims
    assert (
        "EvidenceChain.step.Statement.SUPPORTS.BSM_Element",
        "code_evidence_chain_step",
        "Statement -[:SUPPORTS]-> BSM_Element",
        "SUPPORTS",
    ) in hop_claims
    path_claims = {
        (record.claim.subject_key, record.claim.predicate, record.claim.normalized_value)
        for record in records
        if record.claim.subject_kind == "bsm_domain"
    }
    assert (
        "EvidenceChain.active_path",
        "code_evidence_chain_path",
        "summarisedAnswerUnit -> Statement -> BSM_Element",
    ) in path_claims
    assert (
        "EvidenceChain.full_path",
        "code_evidence_chain_full_path",
        "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element",
    ) in path_claims


def test_bsm_domain_extractor_emits_documented_active_evidence_chain_path() -> None:
    doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="local_doc",
            source_id="_docs/bsm/target.md",
            content_hash="sha256:doc-chain-path",
        ),
        source_type="local_doc",
        source_id="_docs/bsm/target.md",
        title="Target Architecture",
        body="Die Evidenzkette ist bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element.",
        path_hint="_docs/bsm/target.md",
    )

    records = extract_claim_records(documents=[doc])
    path_claims = {
        (record.claim.subject_key, record.claim.predicate, record.claim.normalized_value)
        for record in records
        if record.claim.subject_kind == "bsm_domain"
    }

    assert (
        "EvidenceChain.active_path",
        "documented_evidence_chain_path",
        "summarisedAnswerUnit -> Statement -> BSM_Element",
    ) in path_claims
    assert (
        "EvidenceChain.full_path",
        "documented_evidence_chain_full_path",
        "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element",
    ) in path_claims


def test_bsm_domain_detector_flags_missing_statement_to_element_hop() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/bsm_statement_consolidation_service.py",
            content_hash="sha256:missing-hop",
        ),
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        body="""
def persist_statement_chain():
    query = \"\"\"
    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})
    CREATE (s:Statement {id:$statement_id})
    MERGE (s)-[:DERIVED_FROM]->(u)
    \"\"\"
""".strip(),
        path_hint="src/finai/bsm_statement_consolidation_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_evidence_chain_break"
    )
    assert finding.category == "implementation_drift"
    assert finding.metadata["subject_key"] == "EvidenceChain.active_path"
    assert "Statement -[:DERIVED_FROM]-> summarisedAnswerUnit" in finding.metadata["chain_steps"]
    assert finding.metadata["observed_chain_path"] == ["Statement -[:DERIVED_FROM]-> summarisedAnswerUnit"]
    assert finding.metadata["expected_chain_path"] == [
        "Statement -[:DERIVED_FROM]-> summarisedAnswerUnit",
        "Statement -[:SUPPORTS]-> BSM_Element",
    ]
    assert finding.metadata["observed_chain_variants"] == ["summarisedAnswerUnit -> Statement"]
    assert finding.metadata["expected_chain_variants"] == ["summarisedAnswerUnit -> Statement -> BSM_Element"]
    assert finding.metadata["observed_full_chain_variants"] == ["bsmAnswer -> summarisedAnswerUnit -> Statement"]
    assert finding.metadata["expected_full_chain_variants"] == ["bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element"]
    assert finding.metadata["observed_full_chain_path"] == "bsmAnswer -> summarisedAnswerUnit -> Statement"
    assert finding.metadata["expected_full_chain_path"] == "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element"
    assert finding.metadata["chain_break_mode"] == "tail_gap"
    assert finding.metadata["chain_break_index"] == 3
    assert finding.metadata["chain_break_before"] == "Statement"
    assert finding.metadata["chain_break_after"] == "BSM_Element"
    assert finding.metadata["chain_rejoin_at"] is None
    assert finding.metadata["chain_break_at"] == "Statement.SUPPORTS"
    assert finding.metadata["missing_chain_segments"] == ["BSM_Element"]
    assert finding.metadata["missing_chain_segment_path"] == "BSM_Element"
    assert finding.metadata["remaining_expected_path"] == "BSM_Element"
    assert finding.metadata["common_prefix"] == ["bsmAnswer", "summarisedAnswerUnit", "Statement"]
    assert finding.metadata["common_suffix"] == []
    assert finding.metadata["matched_break_variants"] == [
        {
            "observed_path": "bsmAnswer -> summarisedAnswerUnit -> Statement",
            "expected_path": "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element",
            "score": (1, 3, 0, 3, -1),
            "break_mode": "tail_gap",
            "chain_break_index": 3,
            "chain_break_before": "Statement",
            "chain_break_after": "BSM_Element",
            "chain_rejoin_at": None,
            "missing_chain_segments": ["BSM_Element"],
            "missing_chain_segment_path": "BSM_Element",
            "remaining_expected_path": "BSM_Element",
            "common_prefix": ["bsmAnswer", "summarisedAnswerUnit", "Statement"],
            "common_suffix": [],
        }
    ]
    assert finding.metadata["derived_targets"] == ["summarisedAnswerUnit"]
    assert finding.metadata["missing_expected_step"] == "Statement -[:SUPPORTS]-> BSM_Element"
    assert finding.metadata["unmatched_observed_full_chain_variants"] == []
    assert finding.metadata["unmatched_expected_full_chain_variants"] == []


def test_bsm_domain_detector_tracks_multiple_incomplete_chain_variants() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/bsm_statement_consolidation_service.py",
            content_hash="sha256:missing-hop-multi-variants",
        ),
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        body="""
def persist_summary_chain():
    query = \"\"\"
    MATCH (sa:summarisedAnswer {id:$summary_id})
    CREATE (s:Statement {id:$statement_id})
    MERGE (s)-[:DERIVED_FROM]->(sa)
    \"\"\"

def persist_unit_chain():
    query = \"\"\"
    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})
    CREATE (s:Statement {id:$statement_id})
    MERGE (s)-[:DERIVED_FROM]->(u)
    \"\"\"
""".strip(),
        path_hint="src/finai/bsm_statement_consolidation_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_evidence_chain_break"
    )
    assert finding.metadata["observed_full_chain_variants"] == [
        "bsmAnswer -> summarisedAnswer -> Statement",
        "bsmAnswer -> summarisedAnswerUnit -> Statement",
    ]
    assert finding.metadata["expected_full_chain_variants"] == [
        "bsmAnswer -> summarisedAnswer -> Statement -> BSM_Element",
        "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element",
    ]
    assert len(finding.metadata["matched_break_variants"]) == 2
    assert finding.metadata["matched_break_variants"][0]["missing_chain_segment_path"] == "BSM_Element"
    assert finding.metadata["matched_break_variants"][1]["missing_chain_segment_path"] == "BSM_Element"
    assert finding.metadata["unmatched_observed_full_chain_variants"] == []
    assert finding.metadata["unmatched_expected_full_chain_variants"] == []


def test_bsm_domain_detector_does_not_flag_chain_break_when_supports_hop_exists() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/bsm_statement_consolidation_service.py",
            content_hash="sha256:complete-hop",
        ),
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        body="""
def persist_statement_chain():
    query = \"\"\"
    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})
    CREATE (s:Statement {id:$statement_id})
    MERGE (s)-[:DERIVED_FROM]->(u)
    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)
    \"\"\"
""".strip(),
        path_hint="src/finai/bsm_statement_consolidation_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert not any(
        finding.metadata.get("risk_predicate") == "code_evidence_chain_break"
        for finding in findings
    )


def test_bsm_domain_detector_flags_missing_statement_derivation_hop() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/bsm_statement_consolidation_service.py",
            content_hash="sha256:missing-derived-hop",
        ),
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        body="""
def persist_statement_chain():
    query = \"\"\"
    MATCH (s:Statement {id:$statement_id})
    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)
    \"\"\"
""".strip(),
        path_hint="src/finai/bsm_statement_consolidation_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_evidence_chain_break"
    )
    assert finding.metadata["observed_chain_path"] == ["Statement -[:SUPPORTS]-> BSM_Element"]
    assert finding.metadata["expected_chain_path"] == [
        "Statement -[:DERIVED_FROM]-> <summary_source>",
        "Statement -[:SUPPORTS]-> BSM_Element",
    ]
    assert finding.metadata["observed_chain_variants"] == ["Statement -> BSM_Element"]
    assert finding.metadata["expected_chain_variants"] == ["<summary_source> -> Statement -> BSM_Element"]
    assert finding.metadata["observed_full_chain_variants"] == ["Statement -> BSM_Element"]
    assert finding.metadata["expected_full_chain_variants"] == ["bsmAnswer -> <summary_source> -> Statement -> BSM_Element"]
    assert finding.metadata["observed_full_chain_path"] == "Statement -> BSM_Element"
    assert finding.metadata["expected_full_chain_path"] == "bsmAnswer -> <summary_source> -> Statement -> BSM_Element"
    assert finding.metadata["chain_break_mode"] == "prefix_gap"
    assert finding.metadata["chain_break_index"] == 2
    assert finding.metadata["chain_break_before"] == "<summary_source>"
    assert finding.metadata["chain_break_after"] == "Statement"
    assert finding.metadata["chain_rejoin_at"] == "Statement"
    assert finding.metadata["chain_break_at"] == "Statement.DERIVED_FROM"
    assert finding.metadata["missing_chain_segments"] == ["bsmAnswer", "<summary_source>"]
    assert finding.metadata["missing_chain_segment_path"] == "bsmAnswer -> <summary_source>"
    assert finding.metadata["remaining_expected_path"] == "Statement -> BSM_Element"
    assert finding.metadata["common_prefix"] == []
    assert finding.metadata["common_suffix"] == ["Statement", "BSM_Element"]
    assert finding.metadata["derived_targets"] == []
    assert finding.metadata["missing_expected_step"] == "Statement -[:DERIVED_FROM]-> <summary_source>"


def test_bsm_domain_detector_flags_parallel_active_chain_variants() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/bsm_statement_consolidation_service.py",
            content_hash="sha256:parallel-variants",
        ),
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        body="""
def persist_summary_chain():
    query = \"\"\"
    MATCH (sa:summarisedAnswer {id:$summary_id})
    CREATE (s:Statement {id:$statement_id})
    MERGE (s)-[:DERIVED_FROM]->(sa)
    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)
    \"\"\"

def persist_unit_chain():
    query = \"\"\"
    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})
    CREATE (s:Statement {id:$statement_id})
    MERGE (s)-[:DERIVED_FROM]->(u)
    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)
    \"\"\"
""".strip(),
        path_hint="src/finai/bsm_statement_consolidation_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_evidence_chain_variant_conflict"
    )
    assert finding.category == "implementation_drift"
    assert finding.metadata["subject_key"] == "EvidenceChain.active_path"
    assert finding.metadata["variant_families"] == ["summary_centric", "unit_centric"]
    assert "summarisedAnswer -> Statement -> BSM_Element" in finding.metadata["observed_chain_variants"]
    assert "summarisedAnswerUnit -> Statement -> BSM_Element" in finding.metadata["observed_chain_variants"]


def test_bsm_domain_detector_flags_documented_vs_implemented_active_path_conflict() -> None:
    doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="local_doc",
            source_id="_docs/bsm/target.md",
            content_hash="sha256:doc-path-conflict",
        ),
        source_type="local_doc",
        source_id="_docs/bsm/target.md",
        title="Target Architecture",
        body="Die Evidenzkette ist bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element.",
        path_hint="_docs/bsm/target.md",
    )
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/bsm_statement_consolidation_service.py",
            content_hash="sha256:code-path-conflict",
        ),
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        body="""
def persist_summary_chain():
    query = \"\"\"
    MATCH (sa:summarisedAnswer {id:$summary_id})
    CREATE (s:Statement {id:$statement_id})
    MERGE (s)-[:DERIVED_FROM]->(sa)
    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)
    \"\"\"
""".strip(),
        path_hint="src/finai/bsm_statement_consolidation_service.py",
    )

    records = extract_claim_records(documents=[doc, code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert any(
        finding.category == "contradiction"
        and finding.metadata.get("subject_key") == "EvidenceChain.active_path"
        for finding in findings
    )
    assert any(
        finding.category == "contradiction"
        and finding.metadata.get("subject_key") == "EvidenceChain.full_path"
        for finding in findings
    )
    full_path_finding = next(
        finding for finding in findings
        if finding.category == "contradiction"
        and finding.metadata.get("subject_key") == "EvidenceChain.full_path"
    )
    assert full_path_finding.metadata["documented_full_chain_variants"] == [
        "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element",
    ]
    assert full_path_finding.metadata["implemented_full_chain_variants"] == [
        "bsmAnswer -> summarisedAnswer -> Statement -> BSM_Element",
    ]
    assert full_path_finding.metadata["matched_full_chain_pairs"] == [
        {
            "documented_path": "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element",
            "implemented_path": "bsmAnswer -> summarisedAnswer -> Statement -> BSM_Element",
            "score": (0, 1, 2),
            "divergence_index": 1,
            "divergence_documented": "summarisedAnswerUnit",
            "divergence_implemented": "summarisedAnswer",
            "divergence_mode": "internal_substitution",
            "common_prefix": ["bsmAnswer"],
            "common_suffix": ["Statement", "BSM_Element"],
            "documented_gap_segments": ["summarisedAnswerUnit"],
            "implemented_gap_segments": ["summarisedAnswer"],
            "documented_gap_segment_path": "summarisedAnswerUnit",
            "implemented_gap_segment_path": "summarisedAnswer",
            "rejoin_at": "Statement",
            "documented_family": "unit_centric",
            "implemented_family": "summary_centric",
        }
    ]
    assert full_path_finding.metadata["unmatched_documented_full_chain_variants"] == []
    assert full_path_finding.metadata["unmatched_implemented_full_chain_variants"] == []
    assert full_path_finding.metadata["documented_full_chain_path"] == "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element"
    assert full_path_finding.metadata["implemented_full_chain_path"] == "bsmAnswer -> summarisedAnswer -> Statement -> BSM_Element"
    assert full_path_finding.metadata["documented_variant_family"] == "unit_centric"
    assert full_path_finding.metadata["implemented_variant_family"] == "summary_centric"
    assert full_path_finding.metadata["full_path_divergence_index"] == 1
    assert full_path_finding.metadata["full_path_divergence_documented"] == "summarisedAnswerUnit"
    assert full_path_finding.metadata["full_path_divergence_implemented"] == "summarisedAnswer"
    assert full_path_finding.metadata["full_path_divergence_mode"] == "internal_substitution"
    assert full_path_finding.metadata["full_path_divergence_prefix"] == ["bsmAnswer"]
    assert full_path_finding.metadata["full_path_common_suffix"] == ["Statement", "BSM_Element"]
    assert full_path_finding.metadata["full_path_documented_gap_segments"] == ["summarisedAnswerUnit"]
    assert full_path_finding.metadata["full_path_implemented_gap_segments"] == ["summarisedAnswer"]
    assert full_path_finding.metadata["full_path_rejoin_at"] == "Statement"


def test_bsm_domain_detector_matches_multiple_full_path_variants_and_tracks_unmatched() -> None:
    doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="local_doc",
            source_id="_docs/bsm/target.md",
            content_hash="sha256:doc-multi-path-conflict",
        ),
        source_type="local_doc",
        source_id="_docs/bsm/target.md",
        title="Target Architecture",
        body=(
            "Die Evidenzkette ist bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element.\n"
            "Alternativ beschreibt das Zielbild bsmAnswer -> summarisedAnswer -> Statement -> BSM_Element."
        ),
        path_hint="_docs/bsm/target.md",
    )
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/bsm_statement_consolidation_service.py",
            content_hash="sha256:code-multi-path-conflict",
        ),
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        body="""
def persist_summary_chain():
    query = \"\"\"
    MATCH (sa:summarisedAnswer {id:$summary_id})
    CREATE (s:Statement {id:$statement_id})
    MERGE (s)-[:DERIVED_FROM]->(sa)
    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)
    \"\"\"

def persist_fallback_chain():
    query = \"\"\"
    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})
    CREATE (s:Statement {id:$statement_id})
    MERGE (s)-[:DERIVED_FROM]->(u)
    \"\"\"
""".strip(),
        path_hint="src/finai/bsm_statement_consolidation_service.py",
    )

    records = extract_claim_records(documents=[doc, code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    full_path_finding = next(
        finding for finding in findings
        if finding.category == "contradiction"
        and finding.metadata.get("subject_key") == "EvidenceChain.full_path"
    )
    assert len(full_path_finding.metadata["matched_full_chain_pairs"]) == 2
    assert full_path_finding.metadata["matched_full_chain_pairs"][0]["documented_family"] == "summary_centric"
    assert full_path_finding.metadata["matched_full_chain_pairs"][0]["implemented_family"] == "summary_centric"
    assert full_path_finding.metadata["matched_full_chain_pairs"][1]["documented_family"] == "unit_centric"
    assert full_path_finding.metadata["matched_full_chain_pairs"][1]["implemented_family"] == "unit_centric"
    assert full_path_finding.metadata["unmatched_documented_full_chain_variants"] == []
    assert full_path_finding.metadata["unmatched_implemented_full_chain_variants"] == []


def test_bsm_domain_detector_classifies_documented_internal_full_path_gap() -> None:
    doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="local_doc",
            source_id="_docs/bsm/target.md",
            content_hash="sha256:doc-internal-gap",
        ),
        source_type="local_doc",
        source_id="_docs/bsm/target.md",
        title="Target Architecture",
        body="Die Evidenzkette ist bsmAnswer -> summarisedAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element.",
        path_hint="_docs/bsm/target.md",
    )
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/bsm_statement_consolidation_service.py",
            content_hash="sha256:code-internal-gap",
        ),
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        body="""
def persist_unit_chain():
    query = \"\"\"
    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})
    CREATE (s:Statement {id:$statement_id})
    MERGE (s)-[:DERIVED_FROM]->(u)
    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)
    \"\"\"
""".strip(),
        path_hint="src/finai/bsm_statement_consolidation_service.py",
    )

    records = extract_claim_records(documents=[doc, code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    full_path_finding = next(
        finding for finding in findings
        if finding.category == "contradiction"
        and finding.metadata.get("subject_key") == "EvidenceChain.full_path"
    )
    assert full_path_finding.metadata["full_path_divergence_mode"] == "implemented_internal_gap"
    assert full_path_finding.metadata["full_path_documented_gap_segments"] == ["summarisedAnswer"]
    assert full_path_finding.metadata["full_path_implemented_gap_segments"] == []
    assert full_path_finding.metadata["full_path_documented_gap_segment_path"] == "summarisedAnswer"
    assert full_path_finding.metadata["full_path_implemented_gap_segment_path"] == ""
    assert full_path_finding.metadata["full_path_rejoin_at"] == "summarisedAnswerUnit"


def test_bsm_domain_detector_does_not_flag_active_path_conflict_when_aligned() -> None:
    doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="local_doc",
            source_id="_docs/bsm/target.md",
            content_hash="sha256:doc-path-aligned",
        ),
        source_type="local_doc",
        source_id="_docs/bsm/target.md",
        title="Target Architecture",
        body="Die Evidenzkette ist bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element.",
        path_hint="_docs/bsm/target.md",
    )
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/bsm_statement_consolidation_service.py",
            content_hash="sha256:code-path-aligned",
        ),
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        body="""
def persist_unit_chain():
    query = \"\"\"
    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})
    CREATE (s:Statement {id:$statement_id})
    MERGE (s)-[:DERIVED_FROM]->(u)
    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)
    \"\"\"
""".strip(),
        path_hint="src/finai/bsm_statement_consolidation_service.py",
    )

    records = extract_claim_records(documents=[doc, code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert not any(
        finding.category == "contradiction"
        and finding.metadata.get("subject_key") == "EvidenceChain.active_path"
        for finding in findings
    )
    assert not any(
        finding.category == "contradiction"
        and finding.metadata.get("subject_key") == "EvidenceChain.full_path"
        for finding in findings
    )


def test_bsm_domain_detector_ignores_single_active_chain_variant() -> None:
    code_doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/finai/bsm_statement_consolidation_service.py",
            content_hash="sha256:single-variant",
        ),
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        body="""
def persist_unit_chain():
    query = \"\"\"
    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})
    CREATE (s:Statement {id:$statement_id})
    MERGE (s)-[:DERIVED_FROM]->(u)
    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)
    \"\"\"
""".strip(),
        path_hint="src/finai/bsm_statement_consolidation_service.py",
    )

    records = extract_claim_records(documents=[code_doc])
    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert not any(
        finding.metadata.get("risk_predicate") == "code_evidence_chain_variant_conflict"
        for finding in findings
    )


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


def test_semantic_confluence_writeback_policy_aligns_across_approval_gate() -> None:
    left_value = "kein externes Writeback auf Confluence ohne Freigabe"
    right_value = "externer Confluence-Writeback ist nach Approval erlaubt"

    assert semantic_values_aligned(
        subject_key="ConfluencePage.approval_policy",
        predicate="documented_approval_policy",
        left_value=left_value,
        right_value=right_value,
    )
    assert not semantic_values_conflict(
        subject_key="ConfluencePage.approval_policy",
        predicate="documented_approval_policy",
        left_values={left_value},
        right_values={right_value},
    )


def test_semantic_confluence_read_variants_align_without_false_conflict() -> None:
    left_value = "Confluence Read Collector"
    right_value = "aktueller Confluence-Live-Read bleibt getrennt vom Jira-Writeback-Scope"

    assert semantic_values_aligned(
        subject_key="ConfluencePage.read_path",
        predicate="documented_read",
        left_value=left_value,
        right_value=right_value,
    )
    assert not semantic_values_conflict(
        subject_key="ConfluencePage.read_path",
        predicate="documented_read",
        left_values={left_value},
        right_values={right_value},
    )


def test_semantic_confluence_read_only_and_approved_writeback_policy_do_not_conflict() -> None:
    left_value = "read-only Confluence-Collector"
    right_value = "Confluence writeback is allowed after approval"

    assert semantic_values_aligned(
        subject_key="ConfluencePage.policy",
        predicate="documented_policy",
        left_value=left_value,
        right_value=right_value,
    )
    assert not semantic_values_conflict(
        subject_key="ConfluencePage.policy",
        predicate="documented_policy",
        left_values={left_value},
        right_values={right_value},
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


def test_semantic_graph_builds_evidence_chain_step_relations() -> None:
    step_record = _claim_record(
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        subject_key="EvidenceChain.step.Statement.DERIVED_FROM.summarisedAnswerUnit",
        predicate="code_evidence_chain_step",
        normalized_value="Statement -[:DERIVED_FROM]-> summarisedAnswerUnit",
        line_start=22,
        metadata={
            "start_label": "Statement",
            "end_label": "summarisedAnswerUnit",
            "relationship_type": "DERIVED_FROM",
            "hop_kind": "derived_from",
        },
    )

    graph = build_semantic_graph(
        run_id="audit_test",
        claim_records=[step_record],
        truths=[],
    )

    entity_by_key = {entity.canonical_key: entity for entity in graph.semantic_entities}
    assert "Statement" in entity_by_key
    assert "summarisedAnswerUnit" in entity_by_key

    chain_relations = [
        relation
        for relation in graph.semantic_relations
        if relation.metadata.get("evidence_chain_step") is True
    ]
    assert len(chain_relations) == 1
    relation = chain_relations[0]
    assert relation.relation_type == "references"
    assert relation.metadata["relationship_type"] == "DERIVED_FROM"
    assert entity_by_key["Statement"].entity_id == relation.source_entity_id
    assert entity_by_key["summarisedAnswerUnit"].entity_id == relation.target_entity_id
    assert any(
        "object:Statement -[DERIVED_FROM]-> object:summarisedAnswerUnit" in path
        for claim in graph.claims
        for path in claim.metadata.get("semantic_evidence_chain_paths", [])
    )


def test_semantic_context_attaches_evidence_chain_paths_to_f08_findings() -> None:
    doc_record = _claim_record(
        source_type="local_doc",
        source_id="_docs/bsm/target.md",
        title="Target Architecture",
        subject_key="EvidenceChain.direction",
        predicate="evidence_chain_type",
        normalized_value="unit_centric",
        line_start=2,
    )
    code_record = _claim_record(
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        subject_key="EvidenceChain.step.Statement.DERIVED_FROM.summarisedAnswer",
        predicate="code_evidence_chain_step",
        normalized_value="Statement -[:DERIVED_FROM]-> summarisedAnswer",
        line_start=18,
        metadata={
            "start_label": "Statement",
            "end_label": "summarisedAnswer",
            "relationship_type": "DERIVED_FROM",
            "hop_kind": "derived_from",
        },
    )
    graph = build_semantic_graph(
        run_id="audit_test",
        claim_records=[doc_record, code_record],
        truths=[],
    )
    finding = AuditFinding(
        severity="high",
        category="contradiction",
        title="EvidenceChain direction drift",
        summary="direction mismatch",
        recommendation="align",
        canonical_key="bsm_contradiction|EvidenceChain.direction|evidence_chain_type",
        metadata={"subject_key": "EvidenceChain.direction"},
    )

    enriched = attach_semantic_context_to_findings(
        findings=[finding],
        claims=graph.claims,
        semantic_entities=graph.semantic_entities,
        semantic_relations=graph.semantic_relations,
    )[0]

    assert any(
        "object:Statement -[DERIVED_FROM]-> object:summarisedAnswer" in path
        for path in enriched.metadata.get("semantic_evidence_chain_paths", [])
    )


def test_semantic_graph_attaches_evidence_chain_full_paths_to_claims() -> None:
    doc_record = _claim_record(
        source_type="local_doc",
        source_id="_docs/bsm/target.md",
        title="Target Architecture",
        subject_key="EvidenceChain.full_path",
        predicate="documented_evidence_chain_full_path",
        normalized_value="bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element",
        line_start=2,
        metadata={"chain_path": "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element"},
    )
    code_record = _claim_record(
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        subject_key="EvidenceChain.full_path",
        predicate="code_evidence_chain_full_path",
        normalized_value="bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element",
        line_start=18,
        metadata={"chain_path": "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element"},
    )

    graph = build_semantic_graph(
        run_id="audit_test",
        claim_records=[doc_record, code_record],
        truths=[],
    )

    assert any(
        "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element" in path
        for claim in graph.claims
        for path in claim.metadata.get("semantic_evidence_chain_full_paths", [])
    )


def test_semantic_graph_does_not_attach_unrelated_full_paths_across_clusters() -> None:
    evidence_chain_record = _claim_record(
        source_type="local_doc",
        source_id="_docs/bsm/target.md",
        title="Target Architecture",
        subject_key="EvidenceChain.full_path",
        predicate="documented_evidence_chain_full_path",
        normalized_value="bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element",
        line_start=2,
        metadata={"chain_path": "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element"},
    )
    statement_record = _claim_record(
        source_type="github_file",
        source_id="src/statement_service.py",
        title="statement_service.py",
        subject_key="Statement.write_path",
        predicate="implemented_write_path",
        normalized_value="writes Statement",
        line_start=18,
    )

    graph = build_semantic_graph(
        run_id="audit_test",
        claim_records=[evidence_chain_record, statement_record],
        truths=[],
    )

    claim_by_subject = {claim.subject_key: claim for claim in graph.claims}
    assert claim_by_subject["EvidenceChain.full_path"].metadata.get("semantic_evidence_chain_full_paths") == [
        "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element"
    ]
    assert claim_by_subject["Statement.write_path"].metadata.get("semantic_evidence_chain_full_paths") == []


def test_semantic_context_attaches_evidence_chain_full_paths_to_f08_findings() -> None:
    doc_record = _claim_record(
        source_type="local_doc",
        source_id="_docs/bsm/target.md",
        title="Target Architecture",
        subject_key="EvidenceChain.full_path",
        predicate="documented_evidence_chain_full_path",
        normalized_value="bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element",
        line_start=2,
        metadata={"chain_path": "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element"},
    )
    code_record = _claim_record(
        source_type="github_file",
        source_id="src/finai/bsm_statement_consolidation_service.py",
        title="bsm_statement_consolidation_service.py",
        subject_key="EvidenceChain.full_path",
        predicate="code_evidence_chain_full_path",
        normalized_value="bsmAnswer -> summarisedAnswer -> Statement -> BSM_Element",
        line_start=18,
        metadata={"chain_path": "bsmAnswer -> summarisedAnswer -> Statement -> BSM_Element"},
    )

    graph = build_semantic_graph(
        run_id="audit_test",
        claim_records=[doc_record, code_record],
        truths=[],
    )
    finding = AuditFinding(
        severity="high",
        category="contradiction",
        title="EvidenceChain full path drift",
        summary="full path mismatch",
        recommendation="align",
        canonical_key="bsm_contradiction|EvidenceChain.full_path|evidence_chain_full_path",
        metadata={"subject_key": "EvidenceChain.full_path"},
    )

    enriched = attach_semantic_context_to_findings(
        findings=[finding],
        claims=graph.claims,
        semantic_entities=graph.semantic_entities,
        semantic_relations=graph.semantic_relations,
    )[0]

    assert sorted(enriched.metadata.get("semantic_evidence_chain_full_paths", [])) == [
        "bsmAnswer -> summarisedAnswer -> Statement -> BSM_Element",
        "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element",
    ]


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


def test_finding_engine_detects_legacy_path_gap_against_primary_target() -> None:
    primary_doc = _claim_record(
        source_type="confluence_page",
        source_id="page-legacy-1",
        title="Primary Statement Policy",
        subject_key="Statement.policy",
        predicate="documented_policy",
        normalized_value="Primary path requires approval and phase_run_id propagation.",
        line_start=6,
    )
    legacy_code = _claim_record(
        source_type="github_file",
        source_id="src/legacy_statement_service.py",
        title="legacy_statement_service.py",
        subject_key="Statement.policy",
        predicate="implemented_policy",
        normalized_value="Direct publish without approval is allowed.",
        line_start=12,
        path_hint="src/legacy_statement_service.py",
        metadata={
            "assertion_status": "deprecated",
            "source_authority": "historical",
            "source_governance_level": "historical",
            "source_temporal_status": "historical",
        },
    )

    findings, _ = generate_findings(
        claim_records=[primary_doc, legacy_code],
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    legacy_gaps = [finding for finding in findings if finding.category == "legacy_path_gap"]
    assert len(legacy_gaps) == 1
    assert legacy_gaps[0].canonical_key == "Statement.policy"
    assert legacy_gaps[0].metadata["legacy_path_gap"] is True
    assert legacy_gaps[0].metadata["delta_scope_affected"] is True


def test_claim_extractor_emits_metamodel_lifecycle_claims() -> None:
    snapshot = AuditSourceSnapshot(
        source_type="metamodel",
        source_id="metamodel_dump",
        content_hash="sha256:metamodel_lifecycle",
    )
    document = CollectedDocument(
        snapshot=snapshot,
        source_type="metamodel",
        source_id="metamodel_dump",
        title="current_dump",
        body=json.dumps(
            [
                {
                    "entity_kind": "metaclass",
                    "metaclass_name": "Statement",
                    "initial_status": "draft",
                    "lifecycle": "review required before release",
                }
            ]
        ),
    )

    records = extract_claim_records(documents=[document])
    lifecycle_claims = {(record.claim.subject_key, record.claim.predicate, record.claim.normalized_value) for record in records}

    assert ("Statement.review_status", "metamodel_review_status", "draft") in lifecycle_claims
    assert ("Statement.lifecycle", "metamodel_lifecycle", "review required before release") in lifecycle_claims


def test_claim_extractor_tags_primary_and_fallback_path_roles() -> None:
    primary_snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/primary_statement_service.py",
        content_hash="sha256:primary_statement",
    )
    fallback_snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/fallback_statement_service.py",
        content_hash="sha256:fallback_statement",
    )
    documents = [
        CollectedDocument(
            snapshot=primary_snapshot,
            source_type="github_file",
            source_id="src/primary_statement_service.py",
            title="primary_statement_service.py",
            body="def persist_primary_statement(statement):\n    approval_required_before_save(statement)\n",
            path_hint="src/primary_statement_service.py",
        ),
        CollectedDocument(
            snapshot=fallback_snapshot,
            source_type="github_file",
            source_id="src/fallback_statement_service.py",
            title="fallback_statement_service.py",
            body="def persist_fallback_statement(statement):\n    direct_write_without_approval(statement)\n",
            path_hint="src/fallback_statement_service.py",
        ),
    ]

    records = extract_claim_records(documents=documents)
    roles = {
        (record.claim.source_id, record.claim.predicate): str(record.claim.metadata.get("path_variant_role") or "")
        for record in records
        if record.claim.subject_key in {"Statement.write_path", "Statement.policy"}
    }

    assert ("src/primary_statement_service.py", "implemented_write") in roles
    assert ("src/fallback_statement_service.py", "implemented_write") in roles
    assert roles[("src/primary_statement_service.py", "implemented_write")] == "primary"
    assert roles[("src/fallback_statement_service.py", "implemented_write")] == "fallback"
    assert any(
        record.claim.subject_key == "Statement.write_path"
        and record.claim.predicate == "implemented_path_variant_role"
        and record.claim.normalized_value == "primary"
        for record in records
    )
    assert any(
        record.claim.subject_key == "Statement.write_path"
        and record.claim.predicate == "implemented_path_variant_role"
        and record.claim.normalized_value == "fallback"
        for record in records
    )
    assert any(
        record.claim.subject_key == "Statement.write_path"
        and record.claim.predicate == "implemented_path_delegate"
        and record.claim.normalized_value == "persist_primary_statement"
        for record in records
    )
    assert any(
        record.claim.subject_key == "Statement.write_path"
        and record.claim.predicate == "implemented_path_delegate"
        and record.claim.normalized_value == "persist_fallback_statement"
        for record in records
    )
    assert any(
        record.claim.subject_key == "Statement.write_path"
        and record.claim.predicate == "implemented_path_family_group"
        and record.claim.normalized_value == "persist_primary_statement"
        for record in records
    )
    strength_claims = [
        record
        for record in records
        if record.claim.subject_key == "Statement.write_path"
        and record.claim.predicate == "implemented_path_strength_score"
    ]
    assert len(strength_claims) >= 2
    assert any(int(str(record.claim.normalized_value)) > 0 for record in strength_claims if str(record.claim.normalized_value).lstrip("-").isdigit())
    assert any(int(str(record.claim.normalized_value)) < 0 for record in strength_claims if str(record.claim.normalized_value).lstrip("-").isdigit())


def test_claim_extractor_emits_inference_support_claims_for_unmarked_paths() -> None:
    primary_snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/statement_service.py",
        content_hash="sha256:primary_unmarked",
    )
    side_snapshot = AuditSourceSnapshot(
        source_type="github_file",
        source_id="src/statement_service_bypass.py",
        content_hash="sha256:side_unmarked",
    )
    documents = [
        CollectedDocument(
            snapshot=primary_snapshot,
            source_type="github_file",
            source_id="src/statement_service.py",
            title="statement_service.py",
                body=(
                    "class StatementService:\n"
                    "    def persist(self, statement):\n"
                    "        approval_required_before_save(statement)\n"
                    "        phase_run_id = statement.phase_run_id\n"
                    "        return StatementWriter().save_review_path(statement)\n"
                ),
                path_hint="src/statement_service.py",
            ),
        CollectedDocument(
            snapshot=side_snapshot,
            source_type="github_file",
            source_id="src/statement_service_bypass.py",
            title="statement_service_bypass.py",
            body=(
                "class StatementBypass:\n"
                "    def publish_direct(self, statement):\n"
                "        direct_write_without_approval(statement)\n"
                "        return ManualWriter().save_raw_path(statement)\n"
            ),
            path_hint="src/statement_service_bypass.py",
        ),
    ]

    records = extract_claim_records(documents=documents)
    family_group_claims = [
        record
        for record in records
        if record.claim.subject_key == "Statement.write_path"
        and record.claim.predicate == "implemented_path_family_group"
    ]
    inference_claims = [
        record
        for record in records
        if record.claim.subject_key == "Statement.write_path"
        and record.claim.predicate == "implemented_path_inference_signal"
    ]
    strength_claims = [
        record
        for record in records
        if record.claim.subject_key == "Statement.write_path"
        and record.claim.predicate == "implemented_path_strength_score"
    ]

    assert any("StatementService" in record.claim.normalized_value for record in family_group_claims)
    assert any("StatementBypass" in record.claim.normalized_value for record in family_group_claims)
    assert any(record.claim.normalized_value == "likely_primary" for record in inference_claims)
    assert any(record.claim.normalized_value == "likely_side_path" for record in inference_claims)
    assert any(int(str(record.claim.normalized_value)) >= 2 for record in strength_claims if str(record.claim.normalized_value).lstrip("-").isdigit())
    assert any(int(str(record.claim.normalized_value)) <= -2 for record in strength_claims if str(record.claim.normalized_value).lstrip("-").isdigit())


def test_f12_support_claims_fall_back_to_adapter_and_injection_families() -> None:
    strong_path = _claim_record(
        source_type="github_file",
        source_id="src/statement_pipeline.py",
        title="statement_pipeline.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="approval required before save with review gate",
        line_start=12,
        metadata={
            "repository_adapters": ["StatementRepository"],
            "repository_adapter_symbols": ["finai.repo.StatementRepository.save"],
            "driver_adapters": ["Neo4jDriver"],
            "driver_adapter_symbols": ["finai.graph.Neo4jDriver.execute_query"],
            "constructor_injection_bindings": ["finai.pipeline.StatementPipeline"],
        },
    )
    weak_path = _claim_record(
        source_type="github_file",
        source_id="src/statement_manual_writer.py",
        title="statement_manual_writer.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="manual direct write without approval",
        line_start=28,
        metadata={
            "repository_adapters": ["ManualStatementRepository"],
            "repository_adapter_symbols": ["finai.manual.ManualStatementRepository.save_raw"],
            "driver_adapters": ["ManualGraphDriver"],
            "driver_adapter_symbols": ["finai.manual.ManualGraphDriver.execute_raw"],
            "constructor_injection_bindings": ["finai.manual.StatementManualWriter"],
        },
    )

    findings, _ = generate_findings(
        claim_records=[strong_path, weak_path],
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    legacy_gap = next(finding for finding in findings if finding.category == "legacy_path_gap")
    assert legacy_gap.metadata["inferred_path_role_inference"] is True
    assert legacy_gap.metadata["inferred_primary_group_keys"] == [
        "finai.repo.StatementRepository|finai.graph.Neo4jDriver|finai.pipeline"
    ]
    assert legacy_gap.metadata["inferred_variant_group_keys"] == [
        "finai.manual.ManualStatementRepository|finai.manual.ManualGraphDriver|finai.manual"
    ]


def test_f12_no_finding_when_adapter_based_paths_are_equally_strong() -> None:
    strong_path = _claim_record(
        source_type="github_file",
        source_id="src/statement_pipeline.py",
        title="statement_pipeline.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="approval required before save with review gate",
        line_start=12,
        metadata={
            "repository_adapters": ["StatementRepository"],
            "repository_adapter_symbols": ["finai.repo.StatementRepository.save"],
            "driver_adapters": ["Neo4jDriver"],
            "driver_adapter_symbols": ["finai.graph.Neo4jDriver.execute_query"],
            "constructor_injection_bindings": ["finai.pipeline.StatementPipeline"],
        },
    )
    equally_strong_path = _claim_record(
        source_type="github_file",
        source_id="src/statement_api_pipeline.py",
        title="statement_api_pipeline.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="approval required before save with review gate",
        line_start=32,
        metadata={
            "repository_adapters": ["ApiStatementRepository"],
            "repository_adapter_symbols": ["finai.api.ApiStatementRepository.save"],
            "driver_adapters": ["ApiNeo4jDriver"],
            "driver_adapter_symbols": ["finai.api.ApiNeo4jDriver.execute_query"],
            "constructor_injection_bindings": ["finai.api.StatementPipeline"],
        },
    )

    findings, _ = generate_findings(
        claim_records=[strong_path, equally_strong_path],
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    assert [finding for finding in findings if finding.category == "legacy_path_gap"] == []


def test_finding_engine_detects_legacy_path_gap_via_static_call_graph_roles() -> None:
    primary_code = _claim_record(
        source_type="github_file",
        source_id="src/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="approval required before save",
        line_start=12,
        metadata={
            "static_call_graph_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "static_call_graph_qualified_paths": [
                "finai.dispatch.StatementDispatcher.persist -> finai.pipeline.StatementPipeline.run_primary_path"
            ],
            "path_variant_role": "primary",
        },
    )
    fallback_code = _claim_record(
        source_type="github_file",
        source_id="src/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="direct write without approval",
        line_start=24,
        metadata={
            "static_call_graph_paths": ["StatementDispatcher.persist_degraded -> StatementPipeline.run_fallback_path"],
            "static_call_graph_qualified_paths": [
                "finai.dispatch.StatementDispatcher.persist_degraded -> finai.pipeline.StatementPipeline.run_fallback_path"
            ],
            "path_variant_role": "fallback",
        },
    )

    findings, _ = generate_findings(
        claim_records=[primary_code, fallback_code],
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    legacy_gaps = [finding for finding in findings if finding.category == "legacy_path_gap"]
    assert len(legacy_gaps) == 1
    assert legacy_gaps[0].canonical_key == "Statement.write_path"
    assert "fallback" in [str(item).casefold() for item in legacy_gaps[0].metadata["variant_roles"]]
    assert legacy_gaps[0].metadata["primary_source_ids"] == ["src/statement_dispatcher.py"]
    assert legacy_gaps[0].metadata["variant_source_ids"] == ["src/statement_dispatcher.py"]
    assert "StatementDispatcher.persist -> StatementPipeline.run_primary_path" in legacy_gaps[0].metadata["primary_delegate_paths"]
    assert "StatementDispatcher.persist_degraded -> StatementPipeline.run_fallback_path" in legacy_gaps[0].metadata["variant_delegate_paths"]
    assert legacy_gaps[0].metadata["primary_family_keys"] == ["StatementDispatcher", "StatementPipeline"]
    assert legacy_gaps[0].metadata["variant_family_keys"] == ["StatementDispatcher", "StatementPipeline"]
    assert legacy_gaps[0].metadata["qualified_primary_family_keys"] == ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"]
    assert legacy_gaps[0].metadata["qualified_variant_family_keys"] == ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"]
    assert legacy_gaps[0].metadata["primary_family_groups"] == [
        {
            "primary_group_key": "finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline",
            "primary_source_ids": ["src/statement_dispatcher.py"],
            "primary_source_types": ["github_file"],
            "primary_roles": ["primary"],
            "primary_values": ["approval required before save"],
            "primary_delegate_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
        }
    ]
    assert legacy_gaps[0].metadata["variant_family_groups"] == [
        {
            "variant_role": "fallback",
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "variant_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "qualified_variant_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "variant_source_ids": ["src/statement_dispatcher.py"],
            "family_overlap_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_family_overlap_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "family_alignment": "shared_qualified_service_family",
        }
    ]
    assert legacy_gaps[0].metadata["matched_primary_variant_groups"] == [
        {
            "primary_group_key": "finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline",
            "primary_source_ids": ["src/statement_dispatcher.py"],
            "primary_source_types": ["github_file"],
            "primary_roles": ["primary"],
            "primary_values": ["approval required before save"],
            "primary_delegate_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "variant_roles": ["fallback"],
            "variant_source_ids": ["src/statement_dispatcher.py"],
            "variant_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_variant_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "family_alignment_states": ["shared_qualified_service_family"],
            "comparison_count": 1,
        }
    ]
    assert legacy_gaps[0].metadata["unmatched_primary_family_groups"] == []
    assert legacy_gaps[0].metadata["matched_variant_family_groups"] == [
        {
            "variant_role": "fallback",
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "variant_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "qualified_variant_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "variant_source_ids": ["src/statement_dispatcher.py"],
            "family_overlap_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_family_overlap_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "family_alignment": "shared_qualified_service_family",
        }
    ]
    assert legacy_gaps[0].metadata["unmatched_variant_family_groups"] == []
    assert legacy_gaps[0].metadata["variant_comparisons"] == [
        {
            "primary_group_key": "finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline",
            "primary_source_id": "src/statement_dispatcher.py",
            "primary_source_type": "github_file",
            "primary_role": "primary",
            "primary_value": "approval required before save",
            "primary_delegate_path": "StatementDispatcher.persist -> StatementPipeline.run_primary_path",
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "variant_source_id": "src/statement_dispatcher.py",
            "variant_source_type": "github_file",
            "variant_role": "fallback",
            "variant_value": "direct write without approval",
            "variant_delegate_path": "StatementDispatcher.persist_degraded -> StatementPipeline.run_fallback_path",
            "variant_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_variant_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "family_overlap_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_family_overlap_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "qualified_chain_similarity": [2, 2, 2, 2],
            "chain_alignment_prefix": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "chain_alignment_suffix": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "family_alignment": "shared_qualified_service_family",
        }
    ]


def test_finding_engine_tracks_multiple_variant_path_comparisons() -> None:
    primary_code = _claim_record(
        source_type="github_file",
        source_id="src/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="approval required before save",
        line_start=12,
        metadata={
            "static_call_graph_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "static_call_graph_qualified_paths": [
                "finai.dispatch.StatementDispatcher.persist -> finai.pipeline.StatementPipeline.run_primary_path"
            ],
            "path_variant_role": "primary",
        },
    )
    fallback_code = _claim_record(
        source_type="github_file",
        source_id="src/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="direct write without approval",
        line_start=24,
        metadata={
            "static_call_graph_paths": ["StatementDispatcher.persist_degraded -> StatementPipeline.run_fallback_path"],
            "static_call_graph_qualified_paths": [
                "finai.dispatch.StatementDispatcher.persist_degraded -> finai.pipeline.StatementPipeline.run_fallback_path"
            ],
            "path_variant_role": "fallback",
        },
    )
    compat_code = _claim_record(
        source_type="github_file",
        source_id="src/statement_compat.py",
        title="statement_compat.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="legacy api write without phase_run_id",
        line_start=8,
        metadata={
            "static_call_graph_paths": ["CompatFacade.persist -> CompatWriter.run_compat_path"],
            "static_call_graph_qualified_paths": [
                "finai.compat.CompatFacade.persist -> finai.compat.CompatWriter.run_compat_path"
            ],
            "path_variant_role": "compat",
        },
    )

    findings, _ = generate_findings(
        claim_records=[primary_code, fallback_code, compat_code],
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    legacy_gap = next(finding for finding in findings if finding.category == "legacy_path_gap")
    comparisons = legacy_gap.metadata["variant_comparisons"]
    assert len(comparisons) == 2
    assert comparisons[0]["variant_role"] == "compat"
    assert comparisons[1]["variant_role"] == "fallback"
    assert comparisons[0]["primary_delegate_path"] == "StatementDispatcher.persist -> StatementPipeline.run_primary_path"
    assert comparisons[1]["primary_delegate_path"] == "StatementDispatcher.persist -> StatementPipeline.run_primary_path"
    assert comparisons[0]["primary_family_keys"] == ["StatementDispatcher", "StatementPipeline"]
    assert comparisons[0]["variant_family_keys"] == ["CompatFacade", "CompatWriter"]
    assert comparisons[0]["qualified_primary_family_keys"] == ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"]
    assert comparisons[0]["qualified_variant_family_keys"] == ["finai.compat.CompatFacade", "finai.compat.CompatWriter"]
    assert comparisons[0]["family_overlap_keys"] == []
    assert comparisons[0]["qualified_family_overlap_keys"] == []
    assert comparisons[0]["family_alignment"] == "isolated_service_family"
    assert comparisons[1]["variant_family_keys"] == ["StatementDispatcher", "StatementPipeline"]
    assert comparisons[1]["qualified_variant_family_keys"] == ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"]
    assert comparisons[1]["family_overlap_keys"] == ["StatementDispatcher", "StatementPipeline"]
    assert comparisons[1]["qualified_family_overlap_keys"] == ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"]
    assert comparisons[1]["family_alignment"] == "shared_qualified_service_family"
    assert legacy_gap.metadata["variant_source_ids"] == ["src/statement_compat.py", "src/statement_dispatcher.py"]
    assert legacy_gap.metadata["primary_family_keys"] == ["StatementDispatcher", "StatementPipeline"]
    assert legacy_gap.metadata["variant_family_keys"] == ["CompatFacade", "CompatWriter", "StatementDispatcher", "StatementPipeline"]
    assert legacy_gap.metadata["qualified_primary_family_keys"] == ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"]
    assert legacy_gap.metadata["qualified_variant_family_keys"] == [
        "finai.compat.CompatFacade",
        "finai.compat.CompatWriter",
        "finai.dispatch.StatementDispatcher",
        "finai.pipeline.StatementPipeline",
    ]
    assert legacy_gap.metadata["primary_family_groups"] == [
        {
            "primary_group_key": "finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline",
            "primary_source_ids": ["src/statement_dispatcher.py"],
            "primary_source_types": ["github_file"],
            "primary_roles": ["primary"],
            "primary_values": ["approval required before save"],
            "primary_delegate_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
        }
    ]
    assert legacy_gap.metadata["variant_family_groups"] == [
        {
            "variant_role": "compat",
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "variant_family_keys": ["CompatFacade", "CompatWriter"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "qualified_variant_family_keys": ["finai.compat.CompatFacade", "finai.compat.CompatWriter"],
            "variant_source_ids": ["src/statement_compat.py"],
            "family_overlap_keys": [],
            "qualified_family_overlap_keys": [],
            "family_alignment": "isolated_service_family",
        },
        {
            "variant_role": "fallback",
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "variant_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "qualified_variant_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "variant_source_ids": ["src/statement_dispatcher.py"],
            "family_overlap_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_family_overlap_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "family_alignment": "shared_qualified_service_family",
        },
    ]
    assert legacy_gap.metadata["matched_variant_family_groups"] == [
        {
            "variant_role": "fallback",
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "variant_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "qualified_variant_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "variant_source_ids": ["src/statement_dispatcher.py"],
            "family_overlap_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_family_overlap_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "family_alignment": "shared_qualified_service_family",
        }
    ]
    assert legacy_gap.metadata["unmatched_variant_family_groups"] == [
        {
            "variant_role": "compat",
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "variant_family_keys": ["CompatFacade", "CompatWriter"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "qualified_variant_family_keys": ["finai.compat.CompatFacade", "finai.compat.CompatWriter"],
            "variant_source_ids": ["src/statement_compat.py"],
            "family_overlap_keys": [],
            "qualified_family_overlap_keys": [],
            "family_alignment": "isolated_service_family",
        }
    ]
    assert legacy_gap.metadata["matched_primary_variant_groups"] == [
        {
            "primary_group_key": "finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline",
            "primary_source_ids": ["src/statement_dispatcher.py"],
            "primary_source_types": ["github_file"],
            "primary_roles": ["primary"],
            "primary_values": ["approval required before save"],
            "primary_delegate_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "variant_roles": ["compat", "fallback"],
            "variant_source_ids": ["src/statement_compat.py", "src/statement_dispatcher.py"],
            "variant_family_keys": ["CompatFacade", "CompatWriter", "StatementDispatcher", "StatementPipeline"],
            "qualified_variant_family_keys": [
                "finai.compat.CompatFacade",
                "finai.compat.CompatWriter",
                "finai.dispatch.StatementDispatcher",
                "finai.pipeline.StatementPipeline",
            ],
            "family_alignment_states": ["isolated_service_family", "shared_qualified_service_family"],
            "comparison_count": 2,
        }
    ]
    assert legacy_gap.metadata["unmatched_primary_family_groups"] == []


def test_finding_engine_distinguishes_module_name_collision_via_qualified_variant_families() -> None:
    primary_code = _claim_record(
        source_type="github_file",
        source_id="src/finai/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="approval required before save",
        line_start=12,
        metadata={
            "static_call_graph_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "static_call_graph_qualified_paths": [
                "finai.dispatch.StatementDispatcher.persist -> finai.pipeline.StatementPipeline.run_primary_path"
            ],
            "path_variant_role": "primary",
        },
    )
    colliding_variant = _claim_record(
        source_type="github_file",
        source_id="src/legacy/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="direct write without approval",
        line_start=24,
        metadata={
            "static_call_graph_paths": ["StatementDispatcher.persist -> StatementPipeline.run_fallback_path"],
            "static_call_graph_qualified_paths": [
                "legacy.dispatch.StatementDispatcher.persist -> legacy.pipeline.StatementPipeline.run_fallback_path"
            ],
            "path_variant_role": "fallback",
        },
    )

    findings, _ = generate_findings(
        claim_records=[primary_code, colliding_variant],
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    legacy_gap = next(finding for finding in findings if finding.category == "legacy_path_gap")
    comparison = legacy_gap.metadata["variant_comparisons"][0]
    assert comparison["family_overlap_keys"] == ["StatementDispatcher", "StatementPipeline"]
    assert comparison["qualified_family_overlap_keys"] == []
    assert comparison["qualified_chain_similarity"] == [0, 0, 0, 2]
    assert comparison["chain_alignment_prefix"] == []
    assert comparison["chain_alignment_suffix"] == []
    assert comparison["family_alignment"] == "shared_name_only_service_family"
    assert legacy_gap.metadata["matched_variant_family_groups"] == [
        {
            "variant_role": "fallback",
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "variant_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "qualified_variant_family_keys": ["legacy.dispatch.StatementDispatcher", "legacy.pipeline.StatementPipeline"],
            "variant_source_ids": ["src/legacy/statement_dispatcher.py"],
            "family_overlap_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_family_overlap_keys": [],
            "family_alignment": "shared_name_only_service_family",
        }
    ]
    assert legacy_gap.metadata["unmatched_variant_family_groups"] == []
    assert legacy_gap.metadata["matched_primary_variant_groups"] == [
        {
            "primary_group_key": "finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline",
            "primary_source_ids": ["src/finai/statement_dispatcher.py"],
            "primary_source_types": ["github_file"],
            "primary_roles": ["primary"],
            "primary_values": ["approval required before save"],
            "primary_delegate_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "variant_roles": ["fallback"],
            "variant_source_ids": ["src/legacy/statement_dispatcher.py"],
            "variant_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_variant_family_keys": ["legacy.dispatch.StatementDispatcher", "legacy.pipeline.StatementPipeline"],
            "family_alignment_states": ["shared_name_only_service_family"],
            "comparison_count": 1,
        }
    ]
    assert legacy_gap.metadata["unmatched_primary_family_groups"] == []


def test_finding_engine_assigns_variants_to_best_matching_primary_family() -> None:
    dispatcher_primary = _claim_record(
        source_type="github_file",
        source_id="src/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="approval required before save",
        line_start=12,
        metadata={
            "static_call_graph_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "static_call_graph_qualified_paths": [
                "finai.dispatch.StatementDispatcher.persist -> finai.pipeline.StatementPipeline.run_primary_path"
            ],
            "path_variant_role": "primary",
        },
    )
    api_primary = _claim_record(
        source_type="github_file",
        source_id="src/api_statement_dispatcher.py",
        title="api_statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="api primary path requires approval and audit envelope",
        line_start=40,
        metadata={
            "static_call_graph_paths": ["ApiStatementDispatcher.persist -> ApiStatementPipeline.run_primary_path"],
            "static_call_graph_qualified_paths": [
                "finai.api.ApiStatementDispatcher.persist -> finai.api.ApiStatementPipeline.run_primary_path"
            ],
            "path_variant_role": "primary",
        },
    )
    dispatcher_fallback = _claim_record(
        source_type="github_file",
        source_id="src/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="direct fallback write without approval",
        line_start=24,
        metadata={
            "static_call_graph_paths": ["StatementDispatcher.persist_degraded -> StatementPipeline.run_fallback_path"],
            "static_call_graph_qualified_paths": [
                "finai.dispatch.StatementDispatcher.persist_degraded -> finai.pipeline.StatementPipeline.run_fallback_path"
            ],
            "path_variant_role": "fallback",
        },
    )
    api_compat = _claim_record(
        source_type="github_file",
        source_id="src/api_statement_compat.py",
        title="api_statement_compat.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="compat api write without audit envelope",
        line_start=18,
        metadata={
            "static_call_graph_paths": ["ApiStatementDispatcher.persist_compat -> ApiStatementPipeline.run_compat_path"],
            "static_call_graph_qualified_paths": [
                "finai.api.ApiStatementDispatcher.persist_compat -> finai.api.ApiStatementPipeline.run_compat_path"
            ],
            "path_variant_role": "compat",
        },
    )

    findings, _ = generate_findings(
        claim_records=[dispatcher_primary, api_primary, dispatcher_fallback, api_compat],
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    legacy_gap = next(finding for finding in findings if finding.category == "legacy_path_gap")
    comparisons = legacy_gap.metadata["variant_comparisons"]
    assert len(comparisons) == 2
    fallback_comparison = next(item for item in comparisons if item["variant_role"] == "fallback")
    compat_comparison = next(item for item in comparisons if item["variant_role"] == "compat")

    assert fallback_comparison["primary_group_key"] == "finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline"
    assert fallback_comparison["primary_source_id"] == "src/statement_dispatcher.py"
    assert compat_comparison["primary_group_key"] == "finai.api.ApiStatementDispatcher|finai.api.ApiStatementPipeline"
    assert compat_comparison["primary_source_id"] == "src/api_statement_dispatcher.py"
    assert compat_comparison["qualified_chain_similarity"] == [2, 2, 2, 2]

    assert legacy_gap.metadata["primary_family_groups"] == [
        {
            "primary_group_key": "finai.api.ApiStatementDispatcher|finai.api.ApiStatementPipeline",
            "primary_source_ids": ["src/api_statement_dispatcher.py"],
            "primary_source_types": ["github_file"],
            "primary_roles": ["primary"],
            "primary_values": ["api primary path requires approval and audit envelope"],
            "primary_delegate_paths": ["ApiStatementDispatcher.persist -> ApiStatementPipeline.run_primary_path"],
            "primary_family_keys": ["ApiStatementDispatcher", "ApiStatementPipeline"],
            "qualified_primary_family_keys": ["finai.api.ApiStatementDispatcher", "finai.api.ApiStatementPipeline"],
        },
        {
            "primary_group_key": "finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline",
            "primary_source_ids": ["src/statement_dispatcher.py"],
            "primary_source_types": ["github_file"],
            "primary_roles": ["primary"],
            "primary_values": ["approval required before save"],
            "primary_delegate_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
        },
    ]
    assert legacy_gap.metadata["matched_primary_variant_groups"] == [
        {
            "primary_group_key": "finai.api.ApiStatementDispatcher|finai.api.ApiStatementPipeline",
            "primary_source_ids": ["src/api_statement_dispatcher.py"],
            "primary_source_types": ["github_file"],
            "primary_roles": ["primary"],
            "primary_values": ["api primary path requires approval and audit envelope"],
            "primary_delegate_paths": ["ApiStatementDispatcher.persist -> ApiStatementPipeline.run_primary_path"],
            "primary_family_keys": ["ApiStatementDispatcher", "ApiStatementPipeline"],
            "qualified_primary_family_keys": ["finai.api.ApiStatementDispatcher", "finai.api.ApiStatementPipeline"],
            "variant_roles": ["compat"],
            "variant_source_ids": ["src/api_statement_compat.py"],
            "variant_family_keys": ["ApiStatementDispatcher", "ApiStatementPipeline"],
            "qualified_variant_family_keys": ["finai.api.ApiStatementDispatcher", "finai.api.ApiStatementPipeline"],
            "family_alignment_states": ["shared_qualified_service_family"],
            "comparison_count": 1,
        },
        {
            "primary_group_key": "finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline",
            "primary_source_ids": ["src/statement_dispatcher.py"],
            "primary_source_types": ["github_file"],
            "primary_roles": ["primary"],
            "primary_values": ["approval required before save"],
            "primary_delegate_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "variant_roles": ["fallback"],
            "variant_source_ids": ["src/statement_dispatcher.py"],
            "variant_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_variant_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "family_alignment_states": ["shared_qualified_service_family"],
            "comparison_count": 1,
        },
    ]
    assert legacy_gap.metadata["unmatched_primary_family_groups"] == []


def test_finding_engine_keeps_same_role_variants_as_separate_family_match_groups() -> None:
    primary_code = _claim_record(
        source_type="github_file",
        source_id="src/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="approval required before save",
        line_start=12,
        metadata={
            "static_call_graph_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "static_call_graph_qualified_paths": [
                "finai.dispatch.StatementDispatcher.persist -> finai.pipeline.StatementPipeline.run_primary_path"
            ],
            "path_variant_role": "primary",
        },
    )
    shared_fallback = _claim_record(
        source_type="github_file",
        source_id="src/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="direct fallback write without approval",
        line_start=24,
        metadata={
            "static_call_graph_paths": ["StatementDispatcher.persist_degraded -> StatementPipeline.run_fallback_path"],
            "static_call_graph_qualified_paths": [
                "finai.dispatch.StatementDispatcher.persist_degraded -> finai.pipeline.StatementPipeline.run_fallback_path"
            ],
            "path_variant_role": "fallback",
        },
    )
    isolated_fallback = _claim_record(
        source_type="github_file",
        source_id="src/compat/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="compat fallback write without approval envelope",
        line_start=30,
        metadata={
            "static_call_graph_paths": ["CompatStatementDispatcher.persist_degraded -> CompatStatementPipeline.run_fallback_path"],
            "static_call_graph_qualified_paths": [
                "finai.compat.CompatStatementDispatcher.persist_degraded -> finai.compat.CompatStatementPipeline.run_fallback_path"
            ],
            "path_variant_role": "fallback",
        },
    )

    findings, _ = generate_findings(
        claim_records=[primary_code, shared_fallback, isolated_fallback],
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    legacy_gap = next(finding for finding in findings if finding.category == "legacy_path_gap")
    match_groups = legacy_gap.metadata["variant_family_match_groups"]
    assert match_groups == [
        {
            "variant_group_key": "fallback|finai.compat.CompatStatementDispatcher|finai.compat.CompatStatementPipeline",
            "variant_role": "fallback",
            "primary_group_keys": ["finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline"],
            "primary_source_ids": ["src/statement_dispatcher.py"],
            "variant_source_ids": ["src/compat/statement_dispatcher.py"],
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "variant_family_keys": ["CompatStatementDispatcher", "CompatStatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "qualified_variant_family_keys": ["finai.compat.CompatStatementDispatcher", "finai.compat.CompatStatementPipeline"],
            "family_overlap_keys": [],
            "qualified_family_overlap_keys": [],
            "family_alignment_states": ["isolated_service_family"],
            "chain_similarity_scores": [[0, 0, 0, 0]],
            "chain_alignment_prefixes": [],
            "chain_alignment_suffixes": [],
        },
        {
            "variant_group_key": "fallback|finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline",
            "variant_role": "fallback",
            "primary_group_keys": ["finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline"],
            "primary_source_ids": ["src/statement_dispatcher.py"],
            "variant_source_ids": ["src/statement_dispatcher.py"],
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "variant_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "qualified_variant_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "family_overlap_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_family_overlap_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "family_alignment_states": ["shared_qualified_service_family"],
            "chain_similarity_scores": [[2, 2, 2, 2]],
            "chain_alignment_prefixes": [["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"]],
            "chain_alignment_suffixes": [["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"]],
        },
    ]
    assert legacy_gap.metadata["shared_variant_family_match_groups"] == [match_groups[1]]
    assert legacy_gap.metadata["isolated_variant_family_match_groups"] == [match_groups[0]]
    primary_matches = legacy_gap.metadata["primary_variant_family_matches"]
    assert primary_matches == [
        {
            "primary_group_key": "finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline",
            "primary_source_ids": ["src/statement_dispatcher.py"],
            "primary_source_types": ["github_file"],
            "primary_roles": ["primary"],
            "primary_values": ["approval required before save"],
            "primary_delegate_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "matched_variant_group_keys": [
                "fallback|finai.compat.CompatStatementDispatcher|finai.compat.CompatStatementPipeline",
                "fallback|finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline",
            ],
            "matched_variant_roles": ["fallback"],
            "matched_variant_source_ids": ["src/compat/statement_dispatcher.py", "src/statement_dispatcher.py"],
            "match_group_count": 2,
        }
    ]


def test_finding_engine_reports_unmatched_primary_family_groups_for_multi_primary_scope() -> None:
    dispatcher_primary = _claim_record(
        source_type="github_file",
        source_id="src/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="approval required before save",
        line_start=12,
        metadata={
            "static_call_graph_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "static_call_graph_qualified_paths": [
                "finai.dispatch.StatementDispatcher.persist -> finai.pipeline.StatementPipeline.run_primary_path"
            ],
            "path_variant_role": "primary",
        },
    )
    api_primary = _claim_record(
        source_type="github_file",
        source_id="src/api_statement_dispatcher.py",
        title="api_statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="api primary path requires approval and audit envelope",
        line_start=40,
        metadata={
            "static_call_graph_paths": ["ApiStatementDispatcher.persist -> ApiStatementPipeline.run_primary_path"],
            "static_call_graph_qualified_paths": [
                "finai.api.ApiStatementDispatcher.persist -> finai.api.ApiStatementPipeline.run_primary_path"
            ],
            "path_variant_role": "primary",
        },
    )
    dispatcher_fallback = _claim_record(
        source_type="github_file",
        source_id="src/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="direct fallback write without approval",
        line_start=24,
        metadata={
            "static_call_graph_paths": ["StatementDispatcher.persist_degraded -> StatementPipeline.run_fallback_path"],
            "static_call_graph_qualified_paths": [
                "finai.dispatch.StatementDispatcher.persist_degraded -> finai.pipeline.StatementPipeline.run_fallback_path"
            ],
            "path_variant_role": "fallback",
        },
    )

    findings, _ = generate_findings(
        claim_records=[dispatcher_primary, api_primary, dispatcher_fallback],
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    legacy_gap = next(finding for finding in findings if finding.category == "legacy_path_gap")
    assert legacy_gap.metadata["unmatched_primary_family_groups"] == [
        {
            "primary_group_key": "finai.api.ApiStatementDispatcher|finai.api.ApiStatementPipeline",
            "primary_source_ids": ["src/api_statement_dispatcher.py"],
            "primary_source_types": ["github_file"],
            "primary_roles": ["primary"],
            "primary_values": ["api primary path requires approval and audit envelope"],
            "primary_delegate_paths": ["ApiStatementDispatcher.persist -> ApiStatementPipeline.run_primary_path"],
            "primary_family_keys": ["ApiStatementDispatcher", "ApiStatementPipeline"],
            "qualified_primary_family_keys": ["finai.api.ApiStatementDispatcher", "finai.api.ApiStatementPipeline"],
        }
    ]
    assert legacy_gap.metadata["matched_primary_variant_groups"] == [
        {
            "primary_group_key": "finai.dispatch.StatementDispatcher|finai.pipeline.StatementPipeline",
            "primary_source_ids": ["src/statement_dispatcher.py"],
            "primary_source_types": ["github_file"],
            "primary_roles": ["primary"],
            "primary_values": ["approval required before save"],
            "primary_delegate_paths": ["StatementDispatcher.persist -> StatementPipeline.run_primary_path"],
            "primary_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_primary_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "variant_roles": ["fallback"],
            "variant_source_ids": ["src/statement_dispatcher.py"],
            "variant_family_keys": ["StatementDispatcher", "StatementPipeline"],
            "qualified_variant_family_keys": ["finai.dispatch.StatementDispatcher", "finai.pipeline.StatementPipeline"],
            "family_alignment_states": ["shared_qualified_service_family"],
            "comparison_count": 1,
        }
    ]


def test_finding_engine_infers_unmarked_weaker_side_path_from_strength_and_family_drift() -> None:
    primary_code = _claim_record(
        source_type="github_file",
        source_id="src/statement_service.py",
        title="statement_service.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="approval required before save with phase_run_id propagation",
        line_start=12,
        metadata={
            "static_call_graph_paths": ["StatementService.persist -> StatementWriter.save_primary_path"],
            "static_call_graph_qualified_paths": [
                "finai.statement.StatementService.persist -> finai.statement.StatementWriter.save_primary_path"
            ],
        },
    )
    weaker_side_path = _claim_record(
        source_type="github_file",
        source_id="src/statement_service_bypass.py",
        title="statement_service_bypass.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="direct write without approval through manual publish path",
        line_start=28,
        metadata={
            "static_call_graph_paths": ["StatementBypass.publish_direct -> ManualWriter.save_raw_path"],
            "static_call_graph_qualified_paths": [
                "finai.bypass.StatementBypass.publish_direct -> finai.manual.ManualWriter.save_raw_path"
            ],
        },
    )

    findings, _ = generate_findings(
        claim_records=[primary_code, weaker_side_path],
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    legacy_gap = next(finding for finding in findings if finding.category == "legacy_path_gap")
    assert legacy_gap.metadata["inferred_path_role_inference"] is True
    assert legacy_gap.metadata["inferred_primary_group_keys"] == [
        "finai.statement.StatementService|finai.statement.StatementWriter"
    ]
    assert legacy_gap.metadata["inferred_variant_group_keys"] == [
        "finai.bypass.StatementBypass|finai.manual.ManualWriter"
    ]
    assert "inferred_variant" in legacy_gap.metadata["variant_roles"]
    assert legacy_gap.metadata["primary_family_groups"] == [
        {
            "primary_group_key": "finai.statement.StatementService|finai.statement.StatementWriter",
            "primary_source_ids": ["src/statement_service.py"],
            "primary_source_types": ["github_file"],
            "primary_roles": ["documented_primary"],
            "primary_values": ["approval required before save with phase_run_id propagation"],
            "primary_delegate_paths": ["StatementService.persist -> StatementWriter.save_primary_path"],
            "primary_family_keys": ["StatementService", "StatementWriter"],
            "qualified_primary_family_keys": ["finai.statement.StatementService", "finai.statement.StatementWriter"],
        }
    ]


def test_finding_engine_does_not_infer_side_path_when_unmarked_paths_are_equally_strong() -> None:
    first_primary = _claim_record(
        source_type="github_file",
        source_id="src/statement_service.py",
        title="statement_service.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="approval required before save with phase_run_id propagation",
        line_start=12,
        metadata={
            "static_call_graph_paths": ["StatementService.persist -> StatementWriter.save_primary_path"],
            "static_call_graph_qualified_paths": [
                "finai.statement.StatementService.persist -> finai.statement.StatementWriter.save_primary_path"
            ],
        },
    )
    second_primary = _claim_record(
        source_type="github_file",
        source_id="src/api_statement_service.py",
        title="api_statement_service.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="approval required before save with phase_run_id propagation",
        line_start=40,
        metadata={
            "static_call_graph_paths": ["ApiStatementService.persist -> ApiStatementWriter.save_primary_path"],
            "static_call_graph_qualified_paths": [
                "finai.api.ApiStatementService.persist -> finai.api.ApiStatementWriter.save_primary_path"
            ],
        },
    )

    findings, _ = generate_findings(
        claim_records=[first_primary, second_primary],
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    legacy_gaps = [finding for finding in findings if finding.category == "legacy_path_gap"]
    assert legacy_gaps == []


def test_finding_engine_ignores_variant_support_claims_without_behavior_drift() -> None:
    primary_code = _claim_record(
        source_type="github_file",
        source_id="src/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="approval required before save",
        line_start=12,
        metadata={"path_variant_role": "primary"},
    )
    fallback_code = _claim_record(
        source_type="github_file",
        source_id="src/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_write",
        normalized_value="approval required before save",
        line_start=24,
        metadata={"path_variant_role": "fallback"},
    )
    primary_variant_role = _claim_record(
        source_type="github_file",
        source_id="src/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_path_variant_role",
        normalized_value="primary",
        line_start=12,
        metadata={"path_variant_role": "primary", "path_variant_claim": True},
    )
    fallback_variant_role = _claim_record(
        source_type="github_file",
        source_id="src/statement_dispatcher.py",
        title="statement_dispatcher.py",
        subject_key="Statement.write_path",
        predicate="implemented_path_variant_role",
        normalized_value="fallback",
        line_start=24,
        metadata={"path_variant_role": "fallback", "path_variant_claim": True},
    )

    findings, _ = generate_findings(
        claim_records=[primary_code, fallback_code, primary_variant_role, fallback_variant_role],
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    legacy_gaps = [finding for finding in findings if finding.category == "legacy_path_gap"]
    assert legacy_gaps == []


def test_finding_engine_detects_lifecycle_drift_against_metamodel() -> None:
    doc_record = _claim_record(
        source_type="local_doc",
        source_id="_docs/statement-lifecycle.md",
        title="Statement Lifecycle",
        subject_key="Statement.review_status",
        predicate="documented_review_status",
        normalized_value="released immediately",
        line_start=7,
    )
    metamodel_record = _claim_record(
        source_type="metamodel",
        source_id="metamodel_dump",
        title="current_dump",
        subject_key="Statement.review_status",
        predicate="metamodel_review_status",
        normalized_value="draft",
        line_start=1,
    )

    findings, _ = generate_findings(
        claim_records=[doc_record, metamodel_record],
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    lifecycle_conflicts = [
        finding for finding in findings
        if finding.category == "contradiction" and finding.canonical_key == "Statement.review_status"
    ]
    assert len(lifecycle_conflicts) == 1
    assert "Metamodell" in lifecycle_conflicts[0].title


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


def test_retrieval_index_reports_progress_for_segmentation_and_linking(tmp_path) -> None:
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
    progress_events: list[tuple[str, int, int, str]] = []

    build_retrieval_index(
        settings=settings,
        run_id="audit_test",
        documents=[document],
        claim_records=claim_records,
        previous_segments=[],
        allow_remote_embeddings=False,
        progress_callback=lambda stage, current, total, message: progress_events.append((stage, current, total, message)),
    )

    assert any(stage == "segment_documents" for stage, _, _, _ in progress_events)
    assert any(stage == "annotate_deltas" for stage, _, _, _ in progress_events)
    assert any(stage == "link_claims" for stage, _, _, _ in progress_events)
    assert all(current >= 1 for _, current, _, _ in progress_events)
    assert all(total >= current for _, current, total, _ in progress_events)
    assert all(message for _, _, _, message in progress_events)


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


def test_consensus_detector_aligns_semantic_phase_count_values() -> None:
    claim_records = [
        _record_for_consensus(
            source_type="metamodel",
            source_id="current_dump",
            title="current_dump",
            path_hint="data/metamodel/current_dump.json",
            subject_key="BSM.process",
            predicate="phase_count",
            normalized_value="3",
        ),
        _record_for_consensus(
            source_type="confluence_page",
            source_id="page-process",
            title="Process Definition",
            path_hint="wiki/process-definition",
            subject_key="BSM.process",
            predicate="phase_count",
            normalized_value="BSM process has 3 phases.",
        ),
    ]

    findings = detect_consensus_deviations(claim_records=claim_records)

    assert not any(
        str(finding.canonical_key or "").startswith("consensus_deviation:BSM.process:phase_count")
        for finding in findings
    )
    assert not any(
        str(finding.canonical_key or "") == "consensus_ambiguous:BSM.process:phase_count"
        for finding in findings
    )


def test_consensus_detector_skips_doc_only_gap_for_process_structure_without_code() -> None:
    claim_records = [
        _record_for_consensus(
            source_type="metamodel",
            source_id="current_dump",
            title="current_dump",
            path_hint="data/metamodel/current_dump.json",
            subject_key="BSM.process",
            predicate="phase_count",
            normalized_value="3",
        ),
        _record_for_consensus(
            source_type="confluence_page",
            source_id="page-process",
            title="Process Definition",
            path_hint="wiki/process-definition",
            subject_key="BSM.process",
            predicate="phase_count",
            normalized_value="BSM process has 3 phases.",
        ),
    ]

    findings = detect_consensus_deviations(claim_records=claim_records)

    assert not any(
        str(finding.canonical_key or "") == "coverage_gap:doc_only:BSM.process:phase_count"
        for finding in findings
    )


def test_document_claim_extractor_skips_generic_meta_subject_noise() -> None:
    document = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="docs/architecture.md", content_hash="sha256:architecture"),
        source_type="local_doc",
        source_id="docs/architecture.md",
        title="architecture.md",
        body="\n".join(
            [
                "# Architektur",
                "- externe Ressourcen bis zu einer expliziten User-Entscheidung strikt read-only",
                "- FIN-AI Metamodell direkt read-only aus Neo4j",
            ]
        ),
        path_hint="docs/architecture.md",
    )

    records = extract_claim_records(documents=[document])
    subject_keys = {record.claim.subject_key for record in records}

    assert "Architektur.read_path" not in subject_keys
    assert "Architektur.policy" not in subject_keys
    assert not any(
        record.claim.subject_key == "BSM.process" and record.claim.predicate == "documented_process"
        for record in records
    )


def test_document_claim_extractor_does_not_infer_process_from_heading_only() -> None:
    document = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="docs/process-plan.md", content_hash="sha256:process-heading"),
        source_type="local_doc",
        source_id="docs/process-plan.md",
        title="process-plan.md",
        body="\n".join(
            [
                "# Prozess",
                "- Pflicht:",
                "- Snapshot Chain",
            ]
        ),
        path_hint="docs/process-plan.md",
    )

    records = extract_claim_records(documents=[document])

    assert not any(
        record.claim.subject_key == "BSM.process" and record.claim.predicate == "documented_process"
        for record in records
    )


def test_document_claim_extractor_skips_generic_document_subject_fallback_noise() -> None:
    document = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="docs/architecture.md", content_hash="sha256:subject-noise"),
        source_type="local_doc",
        source_id="docs/architecture.md",
        title="architecture.md",
        body="\n".join(
            [
                "# Architektur",
                "- FIN-AI Metamodell direkt read-only aus Neo4j",
                "- Doku behauptet Promotion-Regel Y",
            ]
        ),
        path_hint="docs/architecture.md",
    )

    records = extract_claim_records(documents=[document])
    subject_keys = {record.claim.subject_key for record in records}

    assert "FIN.read_path" not in subject_keys
    assert "FIN.policy" not in subject_keys
    assert "Doku.lifecycle" not in subject_keys


def test_document_claim_extractor_keeps_explicit_backtick_subjects_without_object_hints() -> None:
    document = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="docs/custom-graph.md", content_hash="sha256:explicit-subject"),
        source_type="local_doc",
        source_id="docs/custom-graph.md",
        title="custom-graph.md",
        body="\n".join(
            [
                "# Graph",
                "- save `CausalGraphTruthBinding` after review",
            ]
        ),
        path_hint="docs/custom-graph.md",
    )

    records = extract_claim_records(documents=[document])

    assert any(
        record.claim.subject_key == "CausalGraphTruthBinding.write_path"
        and record.claim.predicate == "documented_write"
        for record in records
    )


def test_document_claim_extractor_does_not_treat_generic_phase_headings_as_bsm_phases() -> None:
    document = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="docs/product-scope.md", content_hash="sha256:phase-heading-noise"),
        source_type="local_doc",
        source_id="docs/product-scope.md",
        title="product-scope.md",
        body="\n".join(
            [
                "# Produkt-Scope",
                "### Phase 3",
                "- Freigabe- und Publishing-Workflow",
            ]
        ),
        path_hint="docs/product-scope.md",
    )

    records = extract_claim_records(documents=[document])

    assert not any(record.claim.subject_key.startswith("BSM.phase.") for record in records)


def test_document_claim_extractor_does_not_match_run_keyword_inside_unrelated_words() -> None:
    document = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="docs/product-scope.md", content_hash="sha256:run-keyword-noise"),
        source_type="local_doc",
        source_id="docs/product-scope.md",
        title="product-scope.md",
        body="\n".join(
            [
                "# Produkt-Scope",
                "- Confluence-Patch-Preview mit Review-Markierungen, weiterhin nur lokal",
            ]
        ),
        path_hint="docs/product-scope.md",
    )

    records = extract_claim_records(documents=[document])

    assert not any(record.claim.subject_key.startswith("FINAI_Run.") for record in records)


def test_document_claim_extractor_does_not_treat_review_patch_notes_as_lifecycle() -> None:
    document = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="docs/architecture.md", content_hash="sha256:review-note"),
        source_type="local_doc",
        source_id="docs/architecture.md",
        title="architecture.md",
        body="\n".join(
            [
                "# Architektur",
                "- externer Confluence-Writeback fuehrt nach Approval section-anchored Review-Patches ueber die API aus",
            ]
        ),
        path_hint="docs/architecture.md",
    )

    records = extract_claim_records(documents=[document])

    assert not any(record.claim.subject_key == "ConfluencePage.lifecycle" for record in records)


def test_document_claim_extractor_does_not_treat_status_node_mentions_as_lifecycle() -> None:
    document = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="docs/architecture.md", content_hash="sha256:status-node"),
        source_type="local_doc",
        source_id="docs/architecture.md",
        title="architecture.md",
        body="\n".join(
            [
                "# Architektur",
                "- Confluence-Reads/-Writes behandeln auch Tabellen, Makros, Attachments, Status- und Card-Knoten strukturierter",
            ]
        ),
        path_hint="docs/architecture.md",
    )

    records = extract_claim_records(documents=[document])

    assert not any(record.claim.subject_key == "ConfluencePage.lifecycle" for record in records)


def test_code_claim_extractor_does_not_treat_generic_source_symbols_as_input_source() -> None:
    document = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="github_file", source_id="src/example.py", content_hash="sha256:source-symbols"),
        source_type="github_file",
        source_id="src/example.py",
        title="example.py",
        body="\n".join(
            [
                "def summarize_source(source_id: str, source_type: str) -> dict[str, str]:",
                "    current_source = source_id.strip()",
                "    return {'source_id': current_source, 'source_type': source_type}",
            ]
        ),
        path_hint="src/example.py",
    )

    records = extract_claim_records(documents=[document])

    assert not any(record.claim.subject_key.startswith("InputSource.") for record in records)


def test_code_claim_extractor_keeps_explicit_input_source_subject() -> None:
    document = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="github_file", source_id="src/input_source.py", content_hash="sha256:inputsource"),
        source_type="github_file",
        source_id="src/input_source.py",
        title="input_source.py",
        body="\n".join(
            [
                "class InputSourceService:",
                "    def read_inputsource(self) -> str:",
                "        return load_inputsource()",
            ]
        ),
        path_hint="src/input_source.py",
    )

    records = extract_claim_records(documents=[document])

    assert any(record.claim.subject_key.startswith("InputSource.") for record in records)


def test_meta_analysis_code_claims_are_marked_secondary_only() -> None:
    document = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/fin_ai_auditor/services/claim_extractor.py",
            content_hash="sha256:meta-claim",
        ),
        source_type="github_file",
        source_id="src/fin_ai_auditor/services/claim_extractor.py",
        title="claim_extractor.py",
        body="\n".join(
            [
                "def build_meta_claim() -> None:",
                '    subject_key = "BSM.process"',
                '    predicate = "implemented_process"',
                '    anchor_value = "BSM.process.phase_count"',
                "    return None",
            ]
        ),
        path_hint="src/fin_ai_auditor/services/claim_extractor.py",
    )

    records = extract_claim_records(documents=[document])
    bsm_process_records = [record for record in records if record.claim.subject_key == "BSM.process"]

    assert bsm_process_records
    assert all(record.claim.assertion_status == "secondary_only" for record in bsm_process_records)


def test_meta_analysis_code_claims_do_not_trigger_process_drift_or_doc_gap() -> None:
    doc = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="docs/process.md", content_hash="sha256:process-doc"),
        source_type="local_doc",
        source_id="docs/process.md",
        title="process.md",
        body="\n".join(
            [
                "# Process",
                "BSM process has 3 phases.",
            ]
        ),
        path_hint="docs/process.md",
    )
    code = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/fin_ai_auditor/services/claim_extractor.py",
            content_hash="sha256:process-meta-code",
        ),
        source_type="github_file",
        source_id="src/fin_ai_auditor/services/claim_extractor.py",
        title="claim_extractor.py",
        body="\n".join(
            [
                "def build_meta_claim() -> None:",
                '    subject_key = "BSM.process"',
                '    predicate = "implemented_process"',
                '    anchor_value = "BSM.process.phase_count"',
                "    return None",
            ]
        ),
        path_hint="src/fin_ai_auditor/services/claim_extractor.py",
    )

    records = extract_claim_records(documents=[doc, code])
    findings, _ = generate_findings(claim_records=records, inherited_truths=[])
    doc_gap_findings = detect_documentation_gaps(claim_records=records, documents=[doc, code])
    consensus_findings = detect_consensus_deviations(claim_records=records)

    assert not any(str(finding.canonical_key or "") == "BSM.process" for finding in findings)
    assert not any("doc_gap:BSM.process" == str(finding.canonical_key or "") for finding in doc_gap_findings)
    assert not any("BSM.process" in str(finding.canonical_key or "") for finding in consensus_findings)


def test_meta_process_documentation_claims_are_marked_secondary_only() -> None:
    document = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="docs/architecture.md", content_hash="sha256:meta-process-doc"),
        source_type="local_doc",
        source_id="docs/architecture.md",
        title="architecture.md",
        body="\n".join(
            [
                "# Architektur",
                "- BSM-Prozessclaims werden tiefer normalisiert: Phase-/Frage-Referenzen und question_count.",
            ]
        ),
        path_hint="docs/architecture.md",
    )

    records = extract_claim_records(documents=[document])
    bsm_process_records = [record for record in records if record.claim.subject_key == "BSM.process"]

    assert bsm_process_records
    assert all(record.claim.assertion_status == "secondary_only" for record in bsm_process_records)


def test_meta_bsm_phase_documentation_claims_are_marked_secondary_only() -> None:
    document = CollectedDocument(
        snapshot=AuditSourceSnapshot(source_type="local_doc", source_id="docs/architecture.md", content_hash="sha256:meta-phase-doc"),
        source_type="local_doc",
        source_id="docs/architecture.md",
        title="architecture.md",
        body="\n".join(
            [
                "# Architektur",
                "- BSM-Prozessclaims werden tiefer normalisiert: Phase-/Frage-Referenzen, `phase_order`, `question_count` sowie Review-/Approval-Unterclaims werden kanonisch abgeleitet.",
            ]
        ),
        path_hint="docs/architecture.md",
    )

    records = extract_claim_records(documents=[document])
    bsm_phase_records = [record for record in records if record.claim.subject_key.startswith("BSM_Phase.")]

    assert bsm_phase_records
    assert all(record.claim.assertion_status == "secondary_only" for record in bsm_phase_records)


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


def test_generate_findings_reports_progress_for_scan_and_linking() -> None:
    records = [
        _claim_record(
            source_type="confluence_page",
            source_id="123",
            title="Confluence Statement",
            subject_key="Statement.policy",
            predicate="policy_state",
            normalized_value="approval-gated",
            line_start=10,
        ),
        _claim_record(
            source_type="local_doc",
            source_id="_docs/statement.md",
            title="Local Statement",
            subject_key="Statement.policy",
            predicate="policy_state",
            normalized_value="direct-write",
            line_start=14,
            path_hint="_docs/statement.md",
        ),
    ]
    progress_events: list[tuple[str, int, int, str]] = []

    findings, links = generate_findings(
        claim_records=records,
        inherited_truths=[],
        progress_callback=lambda stage, current, total, message: progress_events.append((stage, current, total, message)),
    )

    assert findings
    assert any(stage == "scan_subject_groups" for stage, _, _, _ in progress_events)
    assert any(stage == "truth_conflicts" for stage, _, _, _ in progress_events)
    assert any(stage == "link_findings" for stage, _, _, _ in progress_events)
    assert links
    assert all(current >= 0 for _, current, _, _ in progress_events)
    assert all(total >= 1 for _, _, total, _ in progress_events)
    assert all(message for _, _, _, message in progress_events)


def test_embedding_contradiction_detector_reports_similarity_chunk_completion(monkeypatch, tmp_path) -> None:
    from fin_ai_auditor.config import Settings
    from fin_ai_auditor.services import embedding_contradiction_detector as detector

    class FakeEmbedder:
        def embed_documents(self, batch):
            return [[1.0, 0.0] for _ in batch]

    records = [
        _claim_record(
            source_type="confluence_page",
            source_id="123",
            title="Confluence Statement",
            subject_key="Statement.policy",
            predicate="policy_state",
            normalized_value="approval-gated persistent policy",
            line_start=10,
        ),
        _claim_record(
            source_type="local_doc",
            source_id="_docs/statement.md",
            title="Local Statement",
            subject_key="Statement.policy",
            predicate="policy_state",
            normalized_value="direct-write persistent policy",
            line_start=14,
            path_hint="_docs/statement.md",
        ),
    ]
    progress_events: list[str] = []
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=tmp_path / "metamodel.json")

    monkeypatch.setattr(detector, "_find_embedding_slot", lambda settings: 1)
    monkeypatch.setattr(detector, "get_embeddings_from_llm_slot", lambda settings, llm_slot: FakeEmbedder())

    findings = detector.detect_cross_document_contradictions(
        settings=settings,
        claim_records=records,
        allow_remote_embeddings=True,
        progress_callback=progress_events.append,
    )

    assert findings
    assert any("Embedding-Cache:" in message for message in progress_events)
    assert any("Similarity-Vergleich: Chunk 1/1 (0/2 Vektoren)" in message for message in progress_events)
    assert any("Similarity-Vergleich: Chunk 1/1 abgeschlossen (2/2 Vektoren" in message for message in progress_events)


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
    path_hint: str | None = None,
    metadata: dict[str, object] | None = None,
) -> ExtractedClaimRecord:
    location = AuditLocation(
        source_type=source_type,  # type: ignore[arg-type]
        source_id=source_id,
        title=title,
        path_hint=path_hint or source_id,
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
        metadata=dict(metadata or {}),
    )
    return ExtractedClaimRecord(
        claim=claim,
        evidence=ExtractedClaimEvidence(location=location, matched_text=normalized_value),
    )
