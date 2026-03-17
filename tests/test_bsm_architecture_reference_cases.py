from __future__ import annotations

from fin_ai_auditor.domain.models import AuditSourceSnapshot
from fin_ai_auditor.services.bsm_domain_contradiction_detector import detect_bsm_domain_contradictions
from fin_ai_auditor.services.claim_extractor import extract_claim_records
from fin_ai_auditor.services.pipeline_models import CollectedDocument


def _doc(
    *,
    source_type: str,
    source_id: str,
    body: str,
    path_hint: str | None = None,
    title: str | None = None,
) -> CollectedDocument:
    return CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type=source_type,  # type: ignore[arg-type]
            source_id=source_id,
            content_hash=f"sha256:{source_id}",
        ),
        source_type=source_type,  # type: ignore[arg-type]
        source_id=source_id,
        title=title or source_id,
        body=body,
        path_hint=path_hint or source_id,
    )


def test_reference_case_1_main_chain_uses_summary_not_unit() -> None:
    records = extract_claim_records(
        documents=[
            _doc(
                source_type="local_doc",
                source_id="_docs/bsm/target.md",
                body="Die Evidenzkette ist bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element.",
            ),
            _doc(
                source_type="github_file",
                source_id="src/finai/bsm_statement_consolidation_service.py",
                body="""
def persist_statement_chain():
    query = \"\"\"
    MATCH (sa:summarisedAnswer {id:$summary_id})
    CREATE (s:Statement {id:$statement_id})
    MERGE (s)-[:DERIVED_FROM]->(sa)
    \"\"\"
""".strip(),
                path_hint="src/finai/bsm_statement_consolidation_service.py",
            ),
            _doc(
                source_type="github_file",
                source_id="src/finai/write_allowlist.yaml",
                body="- (:Statement)-[:DERIVED_FROM]->(:summarisedAnswer)",
                path_hint="src/finai/write_allowlist.yaml",
            ),
        ]
    )

    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert any(
        finding.category == "contradiction"
        and finding.metadata.get("subject_key") == "EvidenceChain.direction"
        for finding in findings
    )


def test_reference_case_2_statement_generator_lacks_unit_ids() -> None:
    records = extract_claim_records(
        documents=[
            _doc(
                source_type="github_file",
                source_id="src/finai/bsm_statement_consolidation_service.py",
                body="""
def build_response_schema():
    response_schema = {
        "text": {"type": "string"},
        "confidence": {"type": "number"},
        "rationale": {"type": "string"},
    }
    return response_schema
""".strip(),
                path_hint="src/finai/bsm_statement_consolidation_service.py",
            )
        ]
    )

    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert any(
        finding.category == "implementation_drift"
        and finding.metadata.get("risk_predicate") == "code_schema_missing_fields"
        for finding in findings
    )


def test_reference_case_3_statement_to_element_hop_is_confirmed() -> None:
    records = extract_claim_records(
        documents=[
            _doc(
                source_type="github_file",
                source_id="src/finai/bsm_statement_consolidation_service.py",
                body="""
def materialize_elements():
    query = \"\"\"
    MATCH (s:Statement {id:$id})
    MERGE (e:BSM_Element {id:$eid})
    MERGE (s)-[:SUPPORTS]->(e)
    \"\"\"
""".strip(),
                path_hint="src/finai/bsm_statement_consolidation_service.py",
            ),
            _doc(
                source_type="github_file",
                source_id="src/finai/write_allowlist.yaml",
                body="- (:Statement)-[:SUPPORTS]->(:BSM_Element)",
                path_hint="src/finai/write_allowlist.yaml",
            ),
        ]
    )

    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert any(
        finding.category == "architecture_observation"
        and finding.metadata.get("observation_kind") == "confirmed_architecture_path"
        and finding.metadata.get("subject_key") == "EvidenceChain.hop_statement_element"
        for finding in findings
    )


def test_reference_case_3b_missing_statement_to_element_hop_is_flagged() -> None:
    records = extract_claim_records(
        documents=[
            _doc(
                source_type="github_file",
                source_id="src/finai/bsm_statement_consolidation_service.py",
                body="""
def materialize_elements():
    query = \"\"\"
    MATCH (u:summarisedAnswerUnit {id:$id})
    MERGE (s:Statement {id:$sid})
    MERGE (s)-[:DERIVED_FROM]->(u)
    \"\"\"
""".strip(),
                path_hint="src/finai/bsm_statement_consolidation_service.py",
            ),
        ]
    )

    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert any(
        finding.category == "implementation_drift"
        and finding.metadata.get("risk_predicate") == "code_evidence_chain_break"
        for finding in findings
    )
    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_evidence_chain_break"
    )
    assert finding.metadata["chain_break_at"] == "Statement.SUPPORTS"
    assert finding.metadata["chain_break_mode"] == "tail_gap"
    assert finding.metadata["chain_break_before"] == "Statement"
    assert finding.metadata["chain_break_after"] == "BSM_Element"
    assert finding.metadata["missing_chain_segments"] == ["BSM_Element"]
    assert finding.metadata["matched_break_variants"][0]["missing_chain_segment_path"] == "BSM_Element"
    assert finding.metadata["expected_chain_path"][-1] == "Statement -[:SUPPORTS]-> BSM_Element"


def test_reference_case_3c_missing_statement_derivation_hop_is_flagged() -> None:
    records = extract_claim_records(
        documents=[
            _doc(
                source_type="github_file",
                source_id="src/finai/bsm_statement_consolidation_service.py",
                body="""
def materialize_elements():
    query = \"\"\"
    MATCH (s:Statement {id:$sid})
    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)
    \"\"\"
""".strip(),
                path_hint="src/finai/bsm_statement_consolidation_service.py",
            ),
        ]
    )

    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_evidence_chain_break"
    )
    assert finding.metadata["chain_break_at"] == "Statement.DERIVED_FROM"
    assert finding.metadata["chain_break_mode"] == "prefix_gap"
    assert finding.metadata["chain_break_before"] == "<summary_source>"
    assert finding.metadata["chain_break_after"] == "Statement"
    assert finding.metadata["missing_chain_segments"] == ["bsmAnswer", "<summary_source>"]
    assert finding.metadata["chain_rejoin_at"] == "Statement"
    assert finding.metadata["missing_expected_step"] == "Statement -[:DERIVED_FROM]-> <summary_source>"


def test_reference_case_3d_multiple_incomplete_chain_variants_are_grouped() -> None:
    records = extract_claim_records(
        documents=[
            _doc(
                source_type="github_file",
                source_id="src/finai/bsm_statement_consolidation_service.py",
                body="""
def persist_summary_chain():
    query = \"\"\"
    MATCH (sa:summarisedAnswer {id:$summary_id})
    MERGE (s:Statement {id:$sid})
    MERGE (s)-[:DERIVED_FROM]->(sa)
    \"\"\"

def persist_unit_chain():
    query = \"\"\"
    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})
    MERGE (s:Statement {id:$sid})
    MERGE (s)-[:DERIVED_FROM]->(u)
    \"\"\"
""".strip(),
                path_hint="src/finai/bsm_statement_consolidation_service.py",
            ),
        ]
    )

    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_evidence_chain_break"
    )
    assert len(finding.metadata["matched_break_variants"]) == 2
    assert finding.metadata["unmatched_observed_full_chain_variants"] == []
    assert finding.metadata["unmatched_expected_full_chain_variants"] == []


def test_reference_case_3d_parallel_active_chain_variants_are_flagged() -> None:
    records = extract_claim_records(
        documents=[
            _doc(
                source_type="github_file",
                source_id="src/finai/bsm_statement_consolidation_service.py",
                body="""
def persist_summary_chain():
    query = \"\"\"
    MATCH (sa:summarisedAnswer {id:$summary_id})
    MERGE (s:Statement {id:$sid})
    MERGE (s)-[:DERIVED_FROM]->(sa)
    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)
    \"\"\"

def persist_unit_chain():
    query = \"\"\"
    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})
    MERGE (s:Statement {id:$sid})
    MERGE (s)-[:DERIVED_FROM]->(u)
    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)
    \"\"\"
""".strip(),
                path_hint="src/finai/bsm_statement_consolidation_service.py",
            ),
        ]
    )

    findings = detect_bsm_domain_contradictions(claim_records=records)

    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_evidence_chain_variant_conflict"
    )
    assert finding.metadata["variant_families"] == ["summary_centric", "unit_centric"]


def test_reference_case_4_manual_answer_path_is_eventually_consistent() -> None:
    records = extract_claim_records(
        documents=[
            _doc(
                source_type="github_file",
                source_id="src/finai/router_mining.py",
                body="""
def save_manual_answer():
    persist_answer(answer)
    enqueue_reaggregation(answer.id)
""".strip(),
                path_hint="src/finai/router_mining.py",
            )
        ]
    )

    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert any(
        finding.category == "read_write_gap"
        and finding.metadata.get("risk_predicate") == "code_eventual_consistency_risk"
        for finding in findings
    )
    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_eventual_consistency_risk"
    )
    assert finding.metadata["sequence_break_mode"] == "async_gap"
    assert finding.metadata["missing_sequence_segments"] == ["protected_reaggregation"]
    assert finding.metadata["sequence_rejoin_at"] == "enqueue"


def test_reference_case_5_reaggregation_temporarily_breaks_the_chain() -> None:
    records = extract_claim_records(
        documents=[
            _doc(
                source_type="github_file",
                source_id="src/finai/bsm_reaggregation_service.py",
                body="""
def rebuild_chain():
    supersede_old_statements()
    build_new_statements()
""".strip(),
                path_hint="src/finai/bsm_reaggregation_service.py",
            )
        ]
    )

    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert any(
        finding.category == "read_write_gap"
        and finding.metadata.get("risk_predicate") == "code_chain_interruption_risk"
        for finding in findings
    )
    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_chain_interruption_risk"
    )
    assert finding.metadata["sequence_break_mode"] == "replacement_gap"
    assert finding.metadata["missing_sequence_segments"] == ["replacement_chain_available"]
    assert finding.metadata["sequence_rejoin_at"] == "rebuild"
    assert not any(
        finding.metadata.get("risk_predicate") == "code_field_propagation_gap"
        for finding in findings
    )


def test_reference_case_6_refine_path_drops_phase_run_id() -> None:
    records = extract_claim_records(
        documents=[
            _doc(
                source_type="github_file",
                source_id="src/finai/router_bsm_readiness.py",
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
        ]
    )

    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert any(
        finding.category == "implementation_drift"
        and finding.metadata.get("risk_predicate") == "code_field_propagation_gap"
        for finding in findings
    )
    finding = next(
        finding for finding in findings
        if finding.metadata.get("risk_predicate") == "code_field_propagation_gap"
        and finding.metadata.get("subject_key") == "Statement.field_propagation"
    )
    assert finding.metadata["propagation_break_mode"] == "field_drop"
    assert finding.metadata["missing_field_segments"] == ["phase_run_id"]
    assert finding.metadata["propagation_rejoin_at"] == "Statement"


def test_reference_case_7_legacy_manual_service_path_is_weaker() -> None:
    records = extract_claim_records(
        documents=[
            _doc(
                source_type="github_file",
                source_id="src/finai/bsm_service.py",
                body="""
def legacy_manual_statement_save(statement_id):
    payload = {
        "statement_id": statement_id,
        "target_label": "Statement",
    }
    persist_statement(payload)
""".strip(),
                path_hint="src/finai/bsm_service.py",
            )
        ]
    )

    findings = detect_bsm_domain_contradictions(claim_records=records)

    assert any(
        finding.category == "implementation_drift"
        and finding.metadata.get("risk_predicate") == "code_field_propagation_gap"
        for finding in findings
    )
