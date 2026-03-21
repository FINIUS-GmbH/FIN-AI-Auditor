from __future__ import annotations

from dataclasses import dataclass, field

from fin_ai_auditor.domain.models import (
    AuditClaimEntry,
    AuditFinding,
    AuditLocation,
    AuditPosition,
    AuditSourceSnapshot,
    TruthLedgerEntry,
)
from fin_ai_auditor.services.bsm_domain_contradiction_detector import detect_bsm_domain_contradictions
from fin_ai_auditor.services.claim_extractor import extract_claim_records
from fin_ai_auditor.services.consensus_detector import detect_consensus_deviations
from fin_ai_auditor.services.documentation_gap_detector import detect_documentation_gaps
from fin_ai_auditor.services.finding_engine import generate_findings
from fin_ai_auditor.services.finding_prioritization import prioritize_findings
from fin_ai_auditor.services.pipeline_service import _derive_impacted_scope_keys
from fin_ai_auditor.services.pipeline_models import (
    CollectedDocument,
    ExtractedClaimEvidence,
    ExtractedClaimRecord,
)
from fin_ai_auditor.services.causal_graph_models import (
    CausalGraph,
    CausalGraphEdge,
    CausalGraphNode,
    CausalGraphTruthBinding,
)

GOLD_SET_REQUIRED_RECALL = 1.0
GOLD_SET_REQUIRED_PRECISION = 0.9
GOLD_SET_MAX_FALSE_POSITIVES = 0


@dataclass(frozen=True)
class GoldSetFindingExpectation:
    category: str
    subject_key: str | None = None
    canonical_key: str | None = None
    title_contains: str | None = None
    must_have_jira_action: bool = False
    must_have_confluence_action: bool = False
    must_be_top_finding: bool = False


@dataclass(frozen=True)
class GoldSetCase:
    case_id: str
    expected_findings: list[GoldSetFindingExpectation] = field(default_factory=list)
    forbidden_findings: list[GoldSetFindingExpectation] = field(default_factory=list)


@dataclass(frozen=True)
class GoldSetDeltaCase:
    case_id: str
    claims: list[AuditClaimEntry] = field(default_factory=list)
    inherited_truths: list[TruthLedgerEntry] = field(default_factory=list)
    causal_graph: CausalGraph | None = None
    expected_impacted_scope_keys: list[str] = field(default_factory=list)
    forbidden_impacted_scope_keys: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class GoldSetEvaluation:
    matched_expectations: int
    total_expectations: int
    false_positives: int
    precision: float
    recall: float
    missing_expectation_labels: list[str]
    false_positive_labels: list[str]


@dataclass(frozen=True)
class GoldSetGateResult:
    evaluation: GoldSetEvaluation
    required_recall: float
    required_precision: float
    max_false_positives: int
    passed: bool
    failure_reasons: list[str]


def evaluate_gold_set_cases(
    *,
    cases: list[GoldSetCase],
    findings_by_case: dict[str, list[AuditFinding]],
) -> GoldSetEvaluation:
    matched = 0
    total = 0
    false_positives = 0
    missing_labels: list[str] = []
    false_positive_labels: list[str] = []

    for case in cases:
        findings = findings_by_case.get(case.case_id, [])
        top_finding = findings[0] if findings else None
        for expectation in case.expected_findings:
            total += 1
            hit = next((finding for finding in findings if _matches(finding=finding, expectation=expectation)), None)
            if hit is None or (expectation.must_be_top_finding and top_finding is not hit):
                missing_labels.append(_expectation_label(case_id=case.case_id, expectation=expectation))
                continue
            matched += 1
        for forbidden in case.forbidden_findings:
            if any(_matches(finding=finding, expectation=forbidden) for finding in findings):
                false_positives += 1
                false_positive_labels.append(_expectation_label(case_id=case.case_id, expectation=forbidden))

    precision = matched / max(matched + false_positives, 1)
    recall = matched / max(total, 1)
    return GoldSetEvaluation(
        matched_expectations=matched,
        total_expectations=total,
        false_positives=false_positives,
        precision=precision,
        recall=recall,
        missing_expectation_labels=missing_labels,
        false_positive_labels=false_positive_labels,
    )


def evaluate_reference_gold_set() -> GoldSetGateResult:
    cases = build_reference_gold_set_cases()
    findings_by_case = {
        case.case_id: _detect_case_findings(*documents)
        for case, documents in build_reference_gold_set_documents()
    }
    findings_by_case.update(build_reference_gold_set_synthetic_findings())
    evaluation = evaluate_gold_set_cases(cases=cases, findings_by_case=findings_by_case)
    return build_gold_set_gate_result(evaluation=evaluation)


def evaluate_reference_delta_gold_set() -> GoldSetGateResult:
    evaluation = evaluate_delta_gold_set_cases(cases=build_reference_delta_gold_set_cases())
    return build_gold_set_gate_result(evaluation=evaluation)


def build_gold_set_gate_result(*, evaluation: GoldSetEvaluation) -> GoldSetGateResult:
    failure_reasons: list[str] = []
    if evaluation.recall < GOLD_SET_REQUIRED_RECALL:
        failure_reasons.append(
            f"Recall unter Schwellwert: {evaluation.recall:.2f} < {GOLD_SET_REQUIRED_RECALL:.2f}"
        )
    if evaluation.precision < GOLD_SET_REQUIRED_PRECISION:
        failure_reasons.append(
            f"Precision unter Schwellwert: {evaluation.precision:.2f} < {GOLD_SET_REQUIRED_PRECISION:.2f}"
        )
    if evaluation.false_positives > GOLD_SET_MAX_FALSE_POSITIVES:
        failure_reasons.append(
            f"Zu viele False Positives: {evaluation.false_positives} > {GOLD_SET_MAX_FALSE_POSITIVES}"
        )
    return GoldSetGateResult(
        evaluation=evaluation,
        required_recall=GOLD_SET_REQUIRED_RECALL,
        required_precision=GOLD_SET_REQUIRED_PRECISION,
        max_false_positives=GOLD_SET_MAX_FALSE_POSITIVES,
        passed=not failure_reasons,
        failure_reasons=failure_reasons,
    )


def build_reference_gold_set_cases() -> list[GoldSetCase]:
    return [
        GoldSetCase(
            case_id="summarised_answer_role_conflict",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="summarisedAnswer.role",
                    title_contains="summarisedAnswer",
                    must_have_jira_action=True,
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
            case_id="statement_hitl_artifact_routing",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="Statement.hitl",
                    title_contains="Statement",
                    must_have_jira_action=True,
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
            case_id="statement_status_canon_conflict",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="Statement.status_canon",
                    title_contains="Statement",
                )
            ],
        ),
        GoldSetCase(
            case_id="bsm_element_initial_state_conflict",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="BSM_Element.initial_state",
                    title_contains="BSM_Element",
                )
            ],
        ),
        GoldSetCase(
            case_id="relationship_lifecycle_conflict",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="Relationship.lifecycle",
                    title_contains="Relationship",
                )
            ],
        ),
        GoldSetCase(
            case_id="relationship_change_model_conflict",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="Relationship.change_model",
                    title_contains="Relationship",
                )
            ],
        ),
        GoldSetCase(
            case_id="phase_scope_conflict",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="Phase.scope_distinction",
                    title_contains="Phase",
                )
            ],
        ),
        GoldSetCase(
            case_id="process_phase_count_metamodel_conflict",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="BSM.process",
                    title_contains="Metamodell",
                )
            ],
        ),
        GoldSetCase(
            case_id="process_phase_count_negative_case",
            forbidden_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="BSM.process",
                    title_contains="Metamodell",
                )
            ],
        ),
        GoldSetCase(
            case_id="evidence_chain_conflict",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="EvidenceChain.direction",
                    title_contains="EvidenceChain",
                )
            ],
        ),
        GoldSetCase(
            case_id="to_modify_positive_conflict",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="TO_MODIFY.role",
                    title_contains="TO_MODIFY",
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
        GoldSetCase(
            case_id="run_model_negative_case",
            forbidden_findings=[
                GoldSetFindingExpectation(
                    category="contradiction",
                    subject_key="Run.model",
                    title_contains="Run",
                )
            ],
        ),
        GoldSetCase(
            case_id="statement_policy_conflict",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="policy_conflict",
                    subject_key="Statement.policy",
                    title_contains="Policy",
                    must_be_top_finding=True,
                )
            ],
        ),
        GoldSetCase(
            case_id="statement_policy_doc_gap",
            expected_findings=[
                GoldSetFindingExpectation(
                    category="missing_documentation",
                    canonical_key="doc_gap:Statement.policy",
                    title_contains="Dokumentation",
                    must_have_confluence_action=True,
                )
            ],
        ),
    ]


def build_reference_gold_set_documents() -> list[tuple[GoldSetCase, list[CollectedDocument]]]:
    cases = {case.case_id: case for case in build_reference_gold_set_cases()}
    return [
        (
            cases["summarised_answer_role_conflict"],
            [
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
            ],
        ),
        (
            cases["statement_hitl_conflict"],
            [
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
            ],
        ),
        (
            cases["statement_hitl_artifact_routing"],
            [
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
            ],
        ),
        (
            cases["run_model_conflict"],
            [
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
            ],
        ),
        (
            cases["statement_status_canon_conflict"],
            [
                _doc(
                    source_type="local_doc",
                    source_id="_docs/bsm/status-target.md",
                    title="Status Target",
                    body="\n".join(
                        [
                            "# Statement",
                            "Statement verwendet PROPOSED, VALIDATED, REJECTED und HISTORIC:MODIFIED.",
                        ]
                    ),
                ),
                _doc(
                    source_type="github_file",
                    source_id="models/finai_meta_ssot_pipeline_as_is.puml",
                    title="finai_meta_ssot_pipeline_as_is.puml",
                    body="\n".join(
                        [
                            "@startuml",
                            "Statement : VERIFIED",
                            "Statement : REJECTED",
                            "Statement : REFINED",
                            "@enduml",
                        ]
                    ),
                    path_hint="models/finai_meta_ssot_pipeline_as_is.puml",
                ),
            ],
        ),
        (
            cases["bsm_element_initial_state_conflict"],
            [
                _doc(
                    source_type="local_doc",
                    source_id="_docs/bsm/target-architecture-unit-run-scope.md",
                    title="Target Unit Run Scope",
                    body="\n".join(
                        [
                            "| Entity | Initial |",
                            "| --- | --- |",
                            "| BSM_Element | STAGED |",
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
                            "BSM_Element startet als PROPOSED",
                            "@enduml",
                        ]
                    ),
                    path_hint="models/finai_meta_ssot_pipeline_v2.puml",
                ),
            ],
        ),
        (
            cases["relationship_lifecycle_conflict"],
            [
                _doc(
                    source_type="local_doc",
                    source_id="_docs/bsm/status-target.md",
                    title="Status Target",
                    body="\n".join(
                        [
                            "# Relationship",
                            "Relationship state = STAGED | ACTIVE | REJECTED.",
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
                            "Relationship state = PROPOSED | ACTIVE | REJECTED",
                            "@enduml",
                        ]
                    ),
                    path_hint="models/finai_meta_ssot_pipeline_v2.puml",
                ),
            ],
        ),
        (
            cases["relationship_change_model_conflict"],
            [
                _doc(
                    source_type="local_doc",
                    source_id="_docs/bsm/status-target.md",
                    title="Status Target",
                    body="\n".join(
                        [
                            "# Relationship",
                            "Relationship-Aenderungen erfolgen ueber Versionierung und immutable snapshots.",
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
                            "Relationship state wechsel direkt auf der Relationship mit to_modify",
                            "@enduml",
                        ]
                    ),
                    path_hint="models/finai_meta_ssot_pipeline_v2.puml",
                ),
            ],
        ),
        (
            cases["phase_scope_conflict"],
            [
                _doc(
                    source_type="local_doc",
                    source_id="_docs/bsm/scope-matrix.md",
                    title="Scope Matrix",
                    body="\n".join(
                        [
                            "# Scope Matrix",
                            "ui_phase_id ist UI-only, phase_id ist fachlicher BSM-Phasen-Scope.",
                        ]
                    ),
                ),
                _doc(
                    source_type="github_file",
                    source_id="models/finai_meta_ssot_pipeline_as_is.puml",
                    title="finai_meta_ssot_pipeline_as_is.puml",
                    body="\n".join(
                        [
                            "@startuml",
                            "UI kennt 2 Fachphasen: ingestion und genai_ba",
                            "@enduml",
                        ]
                    ),
                    path_hint="models/finai_meta_ssot_pipeline_as_is.puml",
                ),
            ],
        ),
        (
            cases["process_phase_count_metamodel_conflict"],
            [
                _doc(
                    source_type="metamodel",
                    source_id="current_dump",
                    title="current_dump",
                    body=(
                        '[{"phase_id":"draft","phase_name":"Draft"},'
                        '{"phase_id":"review","phase_name":"Review"},'
                        '{"phase_id":"release","phase_name":"Release"}]'
                    ),
                    path_hint="data/metamodel/current_dump.json",
                ),
                _doc(
                    source_type="confluence_page",
                    source_id="page-process-definition",
                    title="Process Definition",
                    body="\n".join(
                        [
                            "# Process",
                            "BSM process has 4 phases.",
                        ]
                    ),
                    path_hint="wiki/process-definition",
                ),
            ],
        ),
        (
            cases["process_phase_count_negative_case"],
            [
                _doc(
                    source_type="metamodel",
                    source_id="current_dump",
                    title="current_dump",
                    body=(
                        '[{"phase_id":"draft","phase_name":"Draft"},'
                        '{"phase_id":"review","phase_name":"Review"},'
                        '{"phase_id":"release","phase_name":"Release"}]'
                    ),
                    path_hint="data/metamodel/current_dump.json",
                ),
                _doc(
                    source_type="confluence_page",
                    source_id="page-process-definition",
                    title="Process Definition",
                    body="\n".join(
                        [
                            "# Process",
                            "BSM process has 3 phases.",
                        ]
                    ),
                    path_hint="wiki/process-definition",
                ),
            ],
        ),
        (
            cases["evidence_chain_conflict"],
            [
                _doc(
                    source_type="local_doc",
                    source_id="_docs/bsm/target-architecture-unit-run-scope.md",
                    title="Target Unit Run Scope",
                    body="\n".join(
                        [
                            "# Evidenzkette",
                            "bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element",
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
                            "summarisedAnswer bucket root in Agent 2",
                            "@enduml",
                        ]
                    ),
                    path_hint="models/finai_meta_ssot_pipeline_v2.puml",
                ),
            ],
        ),
        (
            cases["to_modify_positive_conflict"],
            [
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
                            "Workflow :TO_MODIFY",
                            "@enduml",
                        ]
                    ),
                    path_hint="models/finai_meta_ssot_pipeline_v2.puml",
                ),
            ],
        ),
        (
            cases["to_modify_negative_case"],
            [
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
            ],
        ),
        (
            cases["run_model_negative_case"],
            [
                _doc(
                    source_type="local_doc",
                    source_id="_docs/bsm/run-ssot.md",
                    title="Run SSOT",
                    body="\n".join(
                        [
                            "# Run SSOT",
                            "run_id ist kein SSOT und bleibt nur sekundaer.",
                        ]
                    ),
                ),
                _doc(
                    source_type="github_file",
                    source_id="models/run-target-notes.puml",
                    title="run-target-notes.puml",
                    body="\n".join(
                        [
                            "@startuml",
                            "run_id ist kein SSOT und bleibt sekundaer",
                            "@enduml",
                        ]
                    ),
                    path_hint="models/run-target-notes.puml",
                ),
            ],
        ),
    ]


def _matches(*, finding: AuditFinding, expectation: GoldSetFindingExpectation) -> bool:
    if finding.category != expectation.category:
        return False
    if expectation.canonical_key and str(finding.canonical_key or "").strip() != expectation.canonical_key:
        return False
    subject_key = str(finding.metadata.get("subject_key") or finding.metadata.get("object_key") or "")
    if expectation.subject_key and subject_key != expectation.subject_key:
        return False
    if expectation.title_contains and expectation.title_contains.casefold() not in finding.title.casefold():
        return False
    if expectation.must_have_jira_action and not str(finding.proposed_jira_action or "").strip():
        return False
    if expectation.must_have_confluence_action and not str(finding.proposed_confluence_action or "").strip():
        return False
    return True


def _expectation_label(*, case_id: str, expectation: GoldSetFindingExpectation) -> str:
    suffix = f" [{expectation.title_contains}]" if expectation.title_contains else ""
    subject_or_key = expectation.subject_key or expectation.canonical_key or "?"
    return f"{case_id}:{expectation.category}:{subject_or_key}{suffix}"


def _detect_case_findings(*documents: CollectedDocument) -> list[AuditFinding]:
    claim_records = extract_claim_records(documents=list(documents))
    findings = detect_bsm_domain_contradictions(claim_records=claim_records)
    findings.extend(detect_consensus_deviations(claim_records=claim_records, confirmed_truths=[]))
    generated_findings, _ = generate_findings(
        claim_records=claim_records,
        inherited_truths=[],
        impacted_scope_keys=None,
    )
    findings.extend(generated_findings)
    findings.extend(detect_documentation_gaps(claim_records=claim_records, documents=list(documents)))
    return sorted(prioritize_findings(findings=findings), key=_gold_set_sort_key)


def build_reference_gold_set_synthetic_findings() -> dict[str, list[AuditFinding]]:
    policy_records = [
        _record(
            source_type="github_file",
            source_id="src/finai/services/statement_policy_service.py",
            title="statement_policy_service.py",
            subject_key="Statement.policy",
            predicate="implemented_policy",
            normalized_value="Direct write without approval is allowed.",
            path_hint="src/finai/services/statement_policy_service.py",
        ),
        _record(
            source_type="local_doc",
            source_id="_docs/statement-policy.md",
            title="Statement Policy",
            subject_key="Statement.policy",
            predicate="documented_policy",
            normalized_value="Write flow is approval-gated and review-only.",
            path_hint="_docs/statement-policy.md",
            source_authority="ssot",
        ),
    ]
    policy_findings, _ = generate_findings(
        claim_records=policy_records,
        inherited_truths=[],
        impacted_scope_keys={"Statement"},
    )

    doc_gap_documents = [
        _doc(
            source_type="local_doc",
            source_id="_docs/statement.md",
            title="Statement",
            body="# Statement\nDieses Dokument beschreibt Statement im Ueberblick.\n" * 3,
            path_hint="_docs/statement.md",
        )
    ]
    doc_gap_records = [
        _record(
            source_type="local_doc",
            source_id="_docs/statement.md",
            title="Statement",
            subject_key="Statement",
            predicate="documented_entity",
            normalized_value="Statement exists.",
            path_hint="_docs/statement.md",
            source_authority="ssot",
        ),
        _record(
            source_type="github_file",
            source_id="src/finai/services/statement_policy_service.py",
            title="statement_policy_service.py",
            subject_key="Statement.policy",
            predicate="implemented_policy",
            normalized_value="approval-gated",
            path_hint="src/finai/services/statement_policy_service.py",
        ),
    ]
    doc_gap_findings = detect_documentation_gaps(
        claim_records=doc_gap_records,
        documents=doc_gap_documents,
    )

    return {
        "statement_policy_conflict": sorted(prioritize_findings(findings=policy_findings), key=_gold_set_sort_key),
        "statement_policy_doc_gap": sorted(prioritize_findings(findings=doc_gap_findings), key=_gold_set_sort_key),
    }


def evaluate_delta_gold_set_cases(*, cases: list[GoldSetDeltaCase]) -> GoldSetEvaluation:
    matched = 0
    total = 0
    false_positives = 0
    missing_labels: list[str] = []
    false_positive_labels: list[str] = []

    for case in cases:
        impacted = _derive_impacted_scope_keys(
            claims=case.claims,
            inherited_truths=case.inherited_truths,
            causal_graph=case.causal_graph,
        )
        for scope_key in case.expected_impacted_scope_keys:
            total += 1
            if scope_key not in impacted:
                missing_labels.append(f"{case.case_id}:expected:{scope_key}")
                continue
            matched += 1
        for scope_key in case.forbidden_impacted_scope_keys:
            if scope_key in impacted:
                false_positives += 1
                false_positive_labels.append(f"{case.case_id}:forbidden:{scope_key}")

    precision = matched / max(matched + false_positives, 1)
    recall = matched / max(total, 1)
    return GoldSetEvaluation(
        matched_expectations=matched,
        total_expectations=total,
        false_positives=false_positives,
        precision=precision,
        recall=recall,
        missing_expectation_labels=missing_labels,
        false_positive_labels=false_positive_labels,
    )


def build_reference_delta_gold_set_cases() -> list[GoldSetDeltaCase]:
    truth_scope_node = CausalGraphNode(
        run_id="gold_set",
        node_type="policy",
        layer="governance",
        canonical_key="Statement.policy",
        label="Statement.policy",
        scope_key="Statement.policy",
        decision_relevant=True,
    )
    write_scope_node = CausalGraphNode(
        run_id="gold_set",
        node_type="write_contract",
        layer="runtime",
        canonical_key="Statement.write_path",
        label="Statement.write_path",
        scope_key="Statement.write_path",
        write_relevant=True,
        decision_relevant=True,
    )
    causal_graph = CausalGraph(
        run_id="gold_set",
        nodes=[truth_scope_node, write_scope_node],
        edges=[
            CausalGraphEdge(
                run_id="gold_set",
                source_node_id=truth_scope_node.node_id,
                target_node_id=write_scope_node.node_id,
                edge_type="propagates_truth_to",
                propagation_mode="truth_and_delta",
                truth_relevant=True,
            )
        ],
    )
    truth = TruthLedgerEntry(
        truth_id="truth_statement_policy",
        canonical_key="Statement.policy|user_specification",
        subject_kind="object_property",
        subject_key="Statement.policy",
        predicate="user_specification",
        normalized_value="approval-gated",
        scope_kind="project",
        scope_key="FINAI",
        source_kind="user_specification",
        metadata={"truth_delta_retrigger": True},
    )
    causal_graph.truth_bindings.append(
        CausalGraphTruthBinding(
            truth_id=truth.truth_id,
            truth_canonical_key=truth.canonical_key,
            bound_node_id=truth_scope_node.node_id,
            predicate="user_specification",
            propagation_mode="truth_and_delta",
        )
    )

    return [
        GoldSetDeltaCase(
            case_id="truth_retrigger_propagates_to_write_path",
            claims=[
                _claim(
                    source_type="github_file",
                    source_id="src/finai/services/statement_policy_service.py",
                    subject_key="Statement.policy",
                    predicate="implemented_policy",
                    normalized_value="approval-gated",
                    metadata={
                        "delta_status": "unchanged",
                        "delta_scope_key": "Statement.policy",
                        "semantic_cluster_keys": ["Statement.policy", "Statement.write_path"],
                    },
                )
            ],
            inherited_truths=[truth],
            causal_graph=causal_graph,
            expected_impacted_scope_keys=["Statement.policy", "Statement.write_path"],
        ),
        GoldSetDeltaCase(
            case_id="stale_truth_without_trigger_stays_idle",
            claims=[
                _claim(
                    source_type="github_file",
                    source_id="src/finai/services/statement_policy_service.py",
                    subject_key="Statement.policy",
                    predicate="implemented_policy",
                    normalized_value="approval-gated",
                    metadata={"delta_status": "unchanged", "delta_scope_key": "Statement.policy"},
                )
            ],
            inherited_truths=[
                truth.model_copy(
                    update={
                        "truth_id": "truth_statement_policy_idle",
                        "metadata": {},
                    }
                )
            ],
            forbidden_impacted_scope_keys=["Statement.policy", "Statement.write_path"],
        ),
        GoldSetDeltaCase(
            case_id="transitive_delta_cluster_expands_neighbors",
            claims=[
                _claim(
                    source_type="github_file",
                    source_id="src/finai/services/statement_policy_service.py",
                    subject_key="Statement.policy",
                    predicate="implemented_policy",
                    normalized_value="approval-gated",
                    metadata={
                        "delta_status": "changed",
                        "delta_scope_key": "Statement.policy",
                        "semantic_cluster_keys": ["Statement.policy", "Statement.write_path"],
                    },
                ),
                _claim(
                    source_type="github_file",
                    source_id="src/finai/workers/job_worker.py",
                    subject_key="Statement.write_path",
                    predicate="implemented_as",
                    normalized_value="guarded_write",
                    metadata={
                        "delta_status": "unchanged",
                        "delta_scope_key": "Statement.write_path",
                        "semantic_cluster_keys": ["Statement.write_path", "BSM.process"],
                    },
                ),
            ],
            expected_impacted_scope_keys=["Statement.policy", "Statement.write_path", "BSM.process"],
        ),
    ]


def _record(
    *,
    source_type: str,
    source_id: str,
    title: str,
    subject_key: str,
    predicate: str,
    normalized_value: str,
    path_hint: str,
    source_authority: str | None = None,
) -> ExtractedClaimRecord:
    claim = _claim(
        source_type=source_type,
        source_id=source_id,
        subject_key=subject_key,
        predicate=predicate,
        normalized_value=normalized_value,
        metadata={
            "claim_focus_value": normalized_value,
            **({"claim_source_authority": source_authority} if source_authority else {}),
        },
    )
    return ExtractedClaimRecord(
        claim=claim,
        evidence=ExtractedClaimEvidence(
            location=AuditLocation(
                source_type=source_type,  # type: ignore[arg-type]
                source_id=source_id,
                title=title,
                path_hint=path_hint,
                position=AuditPosition(anchor_kind="document_line_range", anchor_value=f"{source_id}#L1", line_start=1, line_end=1),
            ),
            matched_text=normalized_value,
        ),
    )


def _claim(
    *,
    source_type: str,
    source_id: str,
    subject_key: str,
    predicate: str,
    normalized_value: str,
    metadata: dict[str, object] | None = None,
) -> AuditClaimEntry:
    claim_metadata = {
        "claim_operator": "=",
        "claim_focus_value": normalized_value,
        **(metadata or {}),
    }
    return AuditClaimEntry(
        source_type=source_type,  # type: ignore[arg-type]
        source_id=source_id,
        subject_kind="object_property",
        subject_key=subject_key,
        predicate=predicate,
        normalized_value=normalized_value,
        scope_kind="project",
        scope_key="FINAI",
        confidence=0.9,
        fingerprint=f"{source_type}|{source_id}|{subject_key}|{predicate}|{normalized_value}",
        metadata=claim_metadata,
    )


def _gold_set_sort_key(finding: AuditFinding) -> tuple[int, int, str]:
    category_priority = {
        "contradiction": 0,
        "policy_conflict": 0,
        "implementation_drift": 1,
        "missing_documentation": 2,
        "traceability_gap": 2,
        "clarification_needed": 3,
    }.get(finding.category, 2)
    return (
        category_priority,
        0 if str(finding.proposed_jira_action or finding.proposed_confluence_action or "").strip() else 1,
        str(finding.metadata.get("subject_key") or finding.metadata.get("object_key") or finding.canonical_key or finding.title),
    )


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
