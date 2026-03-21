from __future__ import annotations

from fin_ai_auditor.domain.models import AuditSourceSnapshot
from fin_ai_auditor.services.bsm_domain_contradiction_detector import detect_bsm_domain_contradictions
from fin_ai_auditor.services.claim_extractor import extract_claim_records
from fin_ai_auditor.services.consensus_detector import detect_consensus_deviations
from fin_ai_auditor.services.gold_set_benchmark import (
    GoldSetCase,
    build_reference_gold_set_cases,
    build_reference_delta_gold_set_cases,
    build_reference_gold_set_documents,
    build_reference_gold_set_synthetic_findings,
    evaluate_reference_gold_set,
    evaluate_reference_delta_gold_set,
    GoldSetFindingExpectation,
    evaluate_gold_set_cases,
    evaluate_delta_gold_set_cases,
    _detect_case_findings as detect_gold_set_case_findings,
)
from fin_ai_auditor.services.pipeline_models import CollectedDocument


def test_gold_set_benchmark_reaches_required_precision_and_recall() -> None:
    cases = [
        GoldSetCase(
            case_id="summarised_answer_role_conflict",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="summarisedAnswer.role",
                    title_contains="summarisedAnswer",
                    must_have_jira_action=True,
                    must_be_top_finding=True,
                )
            ],
        ),
        GoldSetCase(
            case_id="statement_hitl_conflict",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="Statement.hitl",
                    title_contains="Statement",
                    must_be_top_finding=True,
                )
            ],
        ),
        GoldSetCase(
            case_id="run_model_conflict",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="Run.model",
                    title_contains="Run",
                )
            ],
        ),
        GoldSetCase(
            case_id="to_modify_negative_case",
            forbidden_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="TO_MODIFY.role",
                    title_contains="TO_MODIFY",
                )
            ],
        ),
    ]

    findings_by_case = {
        "summarised_answer_role_conflict": _detect_case_findings(
            _doc(
                source_type="local_doc",
                source_id="_docs/bsm/target.md",
                title="Target Architecture",
                body="\n".join(
                    [
                        "# Zielbild",
                        "summarisedAnswer entfaellt und soll kein eigenstaendiges Element mehr sein.",
                        "Die Evidenzkette ist bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element.",
                    ]
                ),
            ),
            _doc(
                source_type="github_file",
                source_id="models/finai_meta_ssot_pipeline_v2.puml",
                title="finai_meta_ssot_pipeline_v2.puml",
                body="\n".join(
                    [
                        "@startuml",
                        "summarisedAnswer : Traceability-Node",
                        "summarisedAnswer : IN_RUN",
                        "summarisedAnswer bucket root in Agent 2",
                        "@enduml",
                    ]
                ),
                path_hint="models/finai_meta_ssot_pipeline_v2.puml",
            ),
            _doc(
                source_type="metamodel",
                source_id="current_dump",
                title="current_dump",
                body='[{"entity_kind":"metaclass","metaclass_name":"summarisedAnswer"}]',
                path_hint="data/metamodel/current_dump.json",
            ),
        ),
        "statement_hitl_conflict": _detect_case_findings(
            _doc(
                source_type="local_doc",
                source_id="_docs/bsm/status.md",
                title="Statuslogik",
                body="\n".join(
                    [
                        "# Statement",
                        "Statement ist zentrales reviewbares Evidenzartefakt mit Accept/Reject/Modify.",
                    ]
                ),
            ),
            _doc(
                source_type="github_file",
                source_id="models/finai_meta_ssot_pipeline_v2.puml",
                title="finai_meta_ssot_pipeline_v2.puml",
                body="\n".join(
                    [
                        "@startuml",
                        "note right of Statement: No HITL decisions on Statements in MVP",
                        "@enduml",
                    ]
                ),
                path_hint="models/finai_meta_ssot_pipeline_v2.puml",
            ),
        ),
        "run_model_conflict": _detect_case_findings(
            _doc(
                source_type="local_doc",
                source_id="_docs/bsm/run-ssot.md",
                title="Run SSOT",
                body="\n".join(
                    [
                        "# Run SSOT",
                        "FINAI_AnalysisRun -> FINAI_PhaseRun -> FINAI_ChunkPhaseRun ist fuehrend.",
                        "run_id ist kein SSOT und bleibt nur sekundaer.",
                    ]
                ),
            ),
            _doc(
                source_type="github_file",
                source_id="models/finai_meta_ssot_pipeline_v2.puml",
                title="finai_meta_ssot_pipeline_v2.puml",
                body="\n".join(
                    [
                        "@startuml",
                        "analysisRun is run_id centric",
                        "@enduml",
                    ]
                ),
                path_hint="models/finai_meta_ssot_pipeline_v2.puml",
            ),
        ),
        "to_modify_negative_case": _detect_case_findings(
            _doc(
                source_type="local_doc",
                source_id="_docs/bsm/status.md",
                title="Statuslogik",
                body="\n".join(
                    [
                        "# TO_MODIFY",
                        "TO_MODIFY ist nicht Teil des Zielbilds.",
                    ]
                ),
            ),
            _doc(
                source_type="github_file",
                source_id="models/finai_meta_ssot_pipeline_v2.puml",
                title="finai_meta_ssot_pipeline_v2.puml",
                body="\n".join(
                    [
                        "@startuml",
                        "note right of Workflow: No TO_MODIFY in target workflow",
                        "@enduml",
                    ]
                ),
                path_hint="models/finai_meta_ssot_pipeline_v2.puml",
            ),
        ),
    }

    evaluation = evaluate_gold_set_cases(cases=cases, findings_by_case=findings_by_case)

    assert evaluation.recall == 1.0, evaluation.missing_expectation_labels
    assert evaluation.precision >= 0.9, evaluation.false_positive_labels
    assert evaluation.false_positives == 0, evaluation.false_positive_labels


def test_reference_gold_set_gate_is_green() -> None:
    gate = evaluate_reference_gold_set()

    assert gate.passed is True
    assert gate.evaluation.recall == 1.0
    assert gate.evaluation.precision >= 0.9
    assert gate.evaluation.false_positives == 0


def test_reference_gold_set_covers_process_phase_count_doc_to_metamodel_conflict() -> None:
    documents_by_case = {
        case.case_id: documents
        for case, documents in build_reference_gold_set_documents()
    }

    positive_findings = detect_gold_set_case_findings(*documents_by_case["process_phase_count_metamodel_conflict"])
    negative_findings = detect_gold_set_case_findings(*documents_by_case["process_phase_count_negative_case"])

    expectation = GoldSetFindingExpectation(
        category="contradiction",
        canonical_key="BSM.process",
        title_contains="Metamodell",
    )

    assert any(
        finding.category == expectation.category
        and str(finding.canonical_key or "") == expectation.canonical_key
        and expectation.title_contains.casefold() in finding.title.casefold()
        for finding in positive_findings
    )
    assert not any(
        finding.category == expectation.category
        and str(finding.canonical_key or "") == expectation.canonical_key
        and expectation.title_contains.casefold() in finding.title.casefold()
        for finding in negative_findings
    )
    assert not any(
        str(finding.canonical_key or "").startswith("consensus_deviation:BSM.process:phase_count")
        for finding in negative_findings
    )
    assert not any(
        str(finding.canonical_key or "") == "coverage_gap:doc_only:BSM.process:phase_count"
        for finding in negative_findings
    )


def test_reference_gold_set_synthetic_findings_cover_policy_and_doc_gap() -> None:
    findings_by_case = build_reference_gold_set_synthetic_findings()

    policy_findings = findings_by_case["statement_policy_conflict"]
    doc_gap_findings = findings_by_case["statement_policy_doc_gap"]

    assert any(finding.category == "policy_conflict" for finding in policy_findings)
    assert any(finding.category == "missing_documentation" for finding in doc_gap_findings)
    assert any(str(finding.proposed_confluence_action or "").strip() for finding in doc_gap_findings)


def test_reference_delta_gold_set_gate_is_green() -> None:
    gate = evaluate_reference_delta_gold_set()

    assert gate.passed is True
    assert gate.evaluation.recall == 1.0
    assert gate.evaluation.precision >= 0.9
    assert gate.evaluation.false_positives == 0


def test_reference_gold_set_builders_stay_consistent() -> None:
    cases = build_reference_gold_set_cases()
    case_ids = {case.case_id for case in cases}
    document_case_ids = {case.case_id for case, _documents in build_reference_gold_set_documents()}
    synthetic_case_ids = set(build_reference_gold_set_synthetic_findings())

    assert case_ids == document_case_ids.union(synthetic_case_ids)


def test_reference_delta_gold_set_builders_stay_consistent() -> None:
    evaluation = evaluate_delta_gold_set_cases(cases=build_reference_delta_gold_set_cases())

    assert evaluation.total_expectations >= 3
    assert evaluation.false_positives == 0


def _detect_case_findings(*documents: CollectedDocument) -> list:
    claim_records = extract_claim_records(documents=list(documents))
    findings = detect_bsm_domain_contradictions(claim_records=claim_records)
    findings.extend(detect_consensus_deviations(claim_records=claim_records, confirmed_truths=[]))
    return findings


def _doc(
    *,
    source_type: str,
    source_id: str,
    title: str,
    body: str,
    path_hint: str | None = None,
) -> CollectedDocument:
    snapshot = AuditSourceSnapshot(
        source_type=source_type,  # type: ignore[arg-type]
        source_id=source_id,
        content_hash=f"sha256:{source_id}",
    )
    return CollectedDocument(
        snapshot=snapshot,
        source_type=source_type,  # type: ignore[arg-type]
        source_id=source_id,
        title=title,
        body=body,
        path_hint=path_hint or source_id,
    )
