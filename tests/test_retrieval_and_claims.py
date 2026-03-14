from __future__ import annotations

from uuid import uuid4

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import (
    AuditClaimEntry,
    AuditFinding,
    AuditLocation,
    AuditPosition,
    AuditSourceSnapshot,
    JiraTicketAICodingBrief,
)
from fin_ai_auditor.services.claim_extractor import extract_claim_records
from fin_ai_auditor.services.claim_semantics import semantic_values_aligned, semantic_values_conflict
from fin_ai_auditor.services.finding_engine import generate_findings
from fin_ai_auditor.services.jira_ticket_writer import build_jira_issue_payload
from fin_ai_auditor.services.pipeline_models import CollectedDocument, ExtractedClaimEvidence, ExtractedClaimRecord
from fin_ai_auditor.services.retrieval_index_service import (
    attach_retrieval_insights_to_findings,
    build_retrieval_index,
)
from fin_ai_auditor.services.semantic_graph_service import build_semantic_graph


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
