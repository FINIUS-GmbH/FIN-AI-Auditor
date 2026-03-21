from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from fin_ai_auditor.domain.models import AuditClaimEntry, AuditLocation, AuditPosition, AuditSourceSnapshot
from fin_ai_auditor.services.bsm_domain_contradiction_detector import detect_bsm_domain_contradictions
from fin_ai_auditor.services.claim_extractor import extract_claim_records
from fin_ai_auditor.services.documentation_gap_detector import detect_documentation_gaps
from fin_ai_auditor.services.finding_engine import generate_findings
from fin_ai_auditor.services.pipeline_models import (
    CollectedDocument,
    ExtractedClaimEvidence,
    ExtractedClaimRecord,
)


@dataclass(frozen=True, slots=True)
class ForensicReferenceExpectation:
    category: str
    canonical_key: str | None = None
    subject_key: str | None = None
    title_contains: str | None = None
    metadata_key: str | None = None
    metadata_value: str | None = None


@dataclass(frozen=True, slots=True)
class ForensicReferenceCase:
    case_id: str
    class_id: str
    polarity: str
    title: str
    detector: str
    current_status: str
    notes: str | None = None
    documents: tuple[CollectedDocument, ...] = ()
    claim_records: tuple[ExtractedClaimRecord, ...] = ()
    expected_findings: tuple[ForensicReferenceExpectation, ...] = ()
    forbidden_findings: tuple[ForensicReferenceExpectation, ...] = ()

    def is_executable(self) -> bool:
        return self.detector != "pending"


def build_forensic_reference_cases() -> tuple[ForensicReferenceCase, ...]:
    return (
        ForensicReferenceCase(
            case_id="F01_pos_doc_doc_conflict",
            class_id="F01",
            polarity="positive",
            title="Confluence und lokale Doku widersprechen sich zur Statement-Policy",
            detector="finding_engine",
            current_status="covered",
            claim_records=(
                _claim_record(
                    source_type="confluence_page",
                    source_id="page-1",
                    title="Statement Policy",
                    subject_key="Statement.policy",
                    predicate="documented_policy",
                    normalized_value="approval-gated and review-only",
                    line_start=5,
                ),
                _claim_record(
                    source_type="local_doc",
                    source_id="_docs/statement.md",
                    title="Statement Policy Draft",
                    subject_key="Statement.policy",
                    predicate="documented_policy",
                    normalized_value="direct write is allowed",
                    line_start=8,
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="contradiction",
                    canonical_key="Statement.policy",
                    title_contains="widersprechen",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F01_neg_doc_doc_alignment",
            class_id="F01",
            polarity="negative",
            title="Confluence und lokale Doku sind zur Statement-Policy konsistent",
            detector="finding_engine",
            current_status="covered",
            claim_records=(
                _claim_record(
                    source_type="confluence_page",
                    source_id="page-1",
                    title="Statement Policy",
                    subject_key="Statement.policy",
                    predicate="documented_policy",
                    normalized_value="approval-gated and review-only",
                    line_start=5,
                ),
                _claim_record(
                    source_type="local_doc",
                    source_id="_docs/statement.md",
                    title="Statement Policy Draft",
                    subject_key="Statement.policy",
                    predicate="documented_policy",
                    normalized_value="approval-gated and review-only",
                    line_start=8,
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(category="contradiction", canonical_key="Statement.policy"),
            ),
        ),
        ForensicReferenceCase(
            case_id="F02_pos_doc_metamodel_conflict",
            class_id="F02",
            polarity="positive",
            title="Dokumentation und Metamodell widersprechen sich zur Phasenanzahl",
            detector="finding_engine",
            current_status="covered",
            claim_records=(
                _claim_record(
                    source_type="metamodel",
                    source_id="current_dump",
                    title="current_dump",
                    subject_key="Statement.phase_count",
                    predicate="phase_count",
                    normalized_value="3",
                    line_start=1,
                ),
                _claim_record(
                    source_type="confluence_page",
                    source_id="page-1",
                    title="Process Definition",
                    subject_key="Statement.phase_count",
                    predicate="documented_process",
                    normalized_value="4",
                    line_start=12,
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="contradiction",
                    canonical_key="Statement.phase_count",
                    title_contains="Metamodell",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F02_neg_doc_metamodel_alignment",
            class_id="F02",
            polarity="negative",
            title="Dokumentation und Metamodell stimmen zur Phasenanzahl ueberein",
            detector="finding_engine",
            current_status="covered",
            claim_records=(
                _claim_record(
                    source_type="metamodel",
                    source_id="current_dump",
                    title="current_dump",
                    subject_key="Statement.phase_count",
                    predicate="phase_count",
                    normalized_value="3",
                    line_start=1,
                ),
                _claim_record(
                    source_type="confluence_page",
                    source_id="page-1",
                    title="Process Definition",
                    subject_key="Statement.phase_count",
                    predicate="documented_process",
                    normalized_value="3",
                    line_start=12,
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(category="contradiction", canonical_key="Statement.phase_count"),
            ),
        ),
        ForensicReferenceCase(
            case_id="F03_pos_doc_code_drift",
            class_id="F03",
            polarity="positive",
            title="Dokumentierter und implementierter Write-Pfad driften auseinander",
            detector="finding_engine",
            current_status="covered",
            claim_records=(
                _claim_record(
                    source_type="local_doc",
                    source_id="_docs/statement.md",
                    title="Statement Contract",
                    subject_key="Statement.write_path",
                    predicate="documented_write",
                    normalized_value="write path goes over review service",
                    line_start=5,
                ),
                _claim_record(
                    source_type="github_file",
                    source_id="src/statement_service.py",
                    title="statement_service.py",
                    subject_key="Statement.write_path",
                    predicate="implemented_write",
                    normalized_value="direct write through worker queue",
                    line_start=10,
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="implementation_drift",
                    canonical_key="Statement.write_path",
                    title_contains="weicht",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F03_neg_doc_code_alignment",
            class_id="F03",
            polarity="negative",
            title="Dokumentierter und implementierter Write-Pfad sind konsistent",
            detector="finding_engine",
            current_status="covered",
            claim_records=(
                _claim_record(
                    source_type="local_doc",
                    source_id="_docs/statement.md",
                    title="Statement Contract",
                    subject_key="Statement.write_path",
                    predicate="documented_write",
                    normalized_value="approval gated review service",
                    line_start=5,
                ),
                _claim_record(
                    source_type="github_file",
                    source_id="src/statement_service.py",
                    title="statement_service.py",
                    subject_key="Statement.write_path",
                    predicate="implemented_write",
                    normalized_value="approval gated review service",
                    line_start=10,
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(category="implementation_drift", canonical_key="Statement.write_path"),
            ),
        ),
        ForensicReferenceCase(
            case_id="F04_pos_documented_path_missing_in_code",
            class_id="F04",
            polarity="positive",
            title="Dokumentierter Write-Pfad ist ohne Codebeleg",
            detector="finding_engine",
            current_status="covered",
            documents=(
                _document(
                    source_type="confluence_page",
                    source_id="page-2",
                    title="Statement Write Contract",
                    body="# Statement\nWrite path for Statement goes over StatementService.persist and repository save.",
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="implementation_drift",
                    canonical_key="Statement.write_path",
                    title_contains="nicht implementiert",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F04_neg_documented_path_has_code_support",
            class_id="F04",
            polarity="negative",
            title="Dokumentierter Write-Pfad hat einen Codebeleg",
            detector="finding_engine",
            current_status="covered",
            documents=(
                _document(
                    source_type="confluence_page",
                    source_id="page-2",
                    title="Statement Write Contract",
                    body="# Statement\nWrite path for Statement is approval guarded.",
                ),
                _document(
                    source_type="github_file",
                    source_id="src/statement_service.py",
                    title="statement_service.py",
                    body="def persist_statement():\n    save_statement()\n",
                    path_hint="src/statement_service.py",
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(category="implementation_drift", canonical_key="Statement.write_path"),
            ),
        ),
        ForensicReferenceCase(
            case_id="F05_pos_code_path_not_documented",
            class_id="F05",
            polarity="positive",
            title="Implementierte Policy-Details sind fachlich nicht dokumentiert",
            detector="documentation_gap",
            current_status="covered",
            claim_records=(
                _claim_record(
                    source_type="confluence_page",
                    source_id="page-5",
                    title="Statement Overview",
                    subject_key="Statement",
                    predicate="definition",
                    normalized_value="Statement is the review artifact.",
                    line_start=3,
                ),
                _claim_record(
                    source_type="github_file",
                    source_id="src/statement_policy_service.py",
                    title="statement_policy_service.py",
                    subject_key="Statement.policy",
                    predicate="implemented_policy",
                    normalized_value="approval token required before publish",
                    line_start=9,
                ),
                _claim_record(
                    source_type="github_file",
                    source_id="src/statement_policy_service.py",
                    title="statement_policy_service.py",
                    subject_key="Statement.policy",
                    predicate="approval_guard",
                    normalized_value="preflight checks run before publish",
                    line_start=15,
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="missing_documentation",
                    canonical_key="doc_gap:Statement.policy",
                    title_contains="Fehlende",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F05_neg_code_path_documented",
            class_id="F05",
            polarity="negative",
            title="Implementierte Policy-Details sind fachlich dokumentiert",
            detector="documentation_gap",
            current_status="covered",
            claim_records=(
                _claim_record(
                    source_type="confluence_page",
                    source_id="page-5",
                    title="Statement Overview",
                    subject_key="Statement",
                    predicate="definition",
                    normalized_value="Statement is the review artifact.",
                    line_start=3,
                ),
                _claim_record(
                    source_type="local_doc",
                    source_id="_docs/statement-policy.md",
                    title="Statement Policy",
                    subject_key="Statement.policy",
                    predicate="documented_policy",
                    normalized_value="approval token required before publish",
                    line_start=6,
                ),
                _claim_record(
                    source_type="github_file",
                    source_id="src/statement_policy_service.py",
                    title="statement_policy_service.py",
                    subject_key="Statement.policy",
                    predicate="implemented_policy",
                    normalized_value="approval token required before publish",
                    line_start=9,
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="missing_documentation",
                    canonical_key="doc_gap:Statement.policy",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F06_pos_policy_violation",
            class_id="F06",
            polarity="positive",
            title="Implementierte Policy verletzt die dokumentierte Approval-Regel",
            detector="finding_engine",
            current_status="covered",
            claim_records=(
                _claim_record(
                    source_type="local_doc",
                    source_id="_docs/statement.md",
                    title="Statement Contract",
                    subject_key="Statement.policy",
                    predicate="documented_policy",
                    normalized_value="write flow is approval-gated and review-only",
                    line_start=5,
                ),
                _claim_record(
                    source_type="github_file",
                    source_id="src/statement_service.py",
                    title="statement_service.py",
                    subject_key="Statement.policy",
                    predicate="implemented_policy",
                    normalized_value="direct write without approval is allowed",
                    line_start=10,
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="policy_conflict",
                    canonical_key="Statement.policy",
                    title_contains="Policy",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F06_neg_policy_alignment",
            class_id="F06",
            polarity="negative",
            title="Implementierte Policy folgt der dokumentierten Approval-Regel",
            detector="finding_engine",
            current_status="covered",
            claim_records=(
                _claim_record(
                    source_type="local_doc",
                    source_id="_docs/statement.md",
                    title="Statement Contract",
                    subject_key="Statement.policy",
                    predicate="documented_policy",
                    normalized_value="write flow is approval-gated and review-only",
                    line_start=5,
                ),
                _claim_record(
                    source_type="github_file",
                    source_id="src/statement_service.py",
                    title="statement_service.py",
                    subject_key="Statement.policy",
                    predicate="implemented_policy",
                    normalized_value="write flow is approval-gated and review-only",
                    line_start=10,
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(category="policy_conflict", canonical_key="Statement.policy"),
            ),
        ),
        ForensicReferenceCase(
            case_id="F07_pos_lifecycle_doc_conflict",
            class_id="F07",
            polarity="positive",
            title="Lifecycle-Regeln widersprechen sich zwischen Dokumentquellen",
            detector="finding_engine",
            current_status="covered",
            notes="Doku-interner Lifecycle-Konflikt als Basisfall.",
            claim_records=(
                _claim_record(
                    source_type="confluence_page",
                    source_id="page-7",
                    title="Statement Lifecycle",
                    subject_key="Statement.review_status",
                    predicate="documented_review_status",
                    normalized_value="in review",
                    line_start=11,
                ),
                _claim_record(
                    source_type="local_doc",
                    source_id="_docs/statement-lifecycle.md",
                    title="Statement Lifecycle Draft",
                    subject_key="Statement.review_status",
                    predicate="documented_review_status",
                    normalized_value="released immediately",
                    line_start=8,
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="contradiction",
                    canonical_key="Statement.review_status",
                    title_contains="Lifecycle",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F07_neg_lifecycle_doc_alignment",
            class_id="F07",
            polarity="negative",
            title="Lifecycle-Regeln sind zwischen Dokumentquellen konsistent",
            detector="finding_engine",
            current_status="covered",
            notes="Negativfall fuer den dokumentinternen Lifecycle-Abgleich.",
            claim_records=(
                _claim_record(
                    source_type="confluence_page",
                    source_id="page-7",
                    title="Statement Lifecycle",
                    subject_key="Statement.review_status",
                    predicate="documented_review_status",
                    normalized_value="in review",
                    line_start=11,
                ),
                _claim_record(
                    source_type="local_doc",
                    source_id="_docs/statement-lifecycle.md",
                    title="Statement Lifecycle Draft",
                    subject_key="Statement.review_status",
                    predicate="documented_review_status",
                    normalized_value="in review",
                    line_start=8,
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="contradiction",
                    canonical_key="Statement.review_status",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F07_pos_lifecycle_doc_code_drift",
            class_id="F07",
            polarity="positive",
            title="Lifecycle-Regeln driften zwischen Doku und Code",
            detector="finding_engine",
            current_status="covered",
            notes="Cross-Source-Fall fuer Doku gegen Code.",
            claim_records=(
                _claim_record(
                    source_type="local_doc",
                    source_id="_docs/statement-lifecycle.md",
                    title="Statement Lifecycle",
                    subject_key="Statement.review_status",
                    predicate="documented_review_status",
                    normalized_value="in review",
                    line_start=7,
                ),
                _claim_record(
                    source_type="github_file",
                    source_id="src/statement_status_service.py",
                    title="statement_status_service.py",
                    subject_key="Statement.review_status",
                    predicate="implemented_review_status",
                    normalized_value="released immediately",
                    line_start=14,
                    path_hint="src/statement_status_service.py",
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="implementation_drift",
                    canonical_key="Statement.review_status",
                    title_contains="weicht",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F07_neg_lifecycle_doc_code_alignment",
            class_id="F07",
            polarity="negative",
            title="Lifecycle-Regeln sind zwischen Doku und Code konsistent",
            detector="finding_engine",
            current_status="covered",
            notes="Negativfall fuer Doku gegen Code.",
            claim_records=(
                _claim_record(
                    source_type="local_doc",
                    source_id="_docs/statement-lifecycle.md",
                    title="Statement Lifecycle",
                    subject_key="Statement.review_status",
                    predicate="documented_review_status",
                    normalized_value="in review",
                    line_start=7,
                ),
                _claim_record(
                    source_type="github_file",
                    source_id="src/statement_status_service.py",
                    title="statement_status_service.py",
                    subject_key="Statement.review_status",
                    predicate="implemented_review_status",
                    normalized_value="in review",
                    line_start=14,
                    path_hint="src/statement_status_service.py",
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="implementation_drift",
                    canonical_key="Statement.review_status",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F07_pos_lifecycle_doc_metamodel_drift",
            class_id="F07",
            polarity="positive",
            title="Lifecycle-Regeln driften zwischen Doku und Metamodell",
            detector="finding_engine",
            current_status="covered",
            notes="Cross-Source-Fall fuer Doku gegen Metamodell.",
            claim_records=(
                _claim_record(
                    source_type="local_doc",
                    source_id="_docs/statement-lifecycle.md",
                    title="Statement Lifecycle",
                    subject_key="Statement.review_status",
                    predicate="documented_review_status",
                    normalized_value="released immediately",
                    line_start=7,
                ),
                _claim_record(
                    source_type="metamodel",
                    source_id="metamodel_dump",
                    title="current_dump",
                    subject_key="Statement.review_status",
                    predicate="metamodel_review_status",
                    normalized_value="draft",
                    line_start=1,
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="contradiction",
                    canonical_key="Statement.review_status",
                    title_contains="Metamodell",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F07_neg_lifecycle_doc_metamodel_alignment",
            class_id="F07",
            polarity="negative",
            title="Lifecycle-Regeln sind zwischen Doku und Metamodell konsistent",
            detector="finding_engine",
            current_status="covered",
            notes="Negativfall fuer Doku gegen Metamodell.",
            claim_records=(
                _claim_record(
                    source_type="local_doc",
                    source_id="_docs/statement-lifecycle.md",
                    title="Statement Lifecycle",
                    subject_key="Statement.review_status",
                    predicate="documented_review_status",
                    normalized_value="draft",
                    line_start=7,
                ),
                _claim_record(
                    source_type="metamodel",
                    source_id="metamodel_dump",
                    title="current_dump",
                    subject_key="Statement.review_status",
                    predicate="metamodel_review_status",
                    normalized_value="draft",
                    line_start=1,
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="contradiction",
                    canonical_key="Statement.review_status",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F08_pos_evidence_chain_conflict",
            class_id="F08",
            polarity="positive",
            title="Dokumentierte und implementierte Evidenzkette widersprechen sich",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="local_doc",
                    source_id="_docs/bsm/target.md",
                    title="Target Architecture",
                    body="Die Evidenzkette ist bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element.",
                ),
                _document(
                    source_type="github_file",
                    source_id="src/finai/bsm_statement_consolidation_service.py",
                    title="bsm_statement_consolidation_service.py",
                    body=(
                        "def persist_statement_chain():\n"
                        "    query = \"\"\"\n"
                        "    MATCH (sa:summarisedAnswer {id:$summary_id})\n"
                        "    CREATE (s:Statement {id:$statement_id})\n"
                        "    MERGE (s)-[:DERIVED_FROM]->(sa)\n"
                        "    \"\"\"\n"
                    ),
                    path_hint="src/finai/bsm_statement_consolidation_service.py",
                ),
                _document(
                    source_type="github_file",
                    source_id="src/finai/write_allowlist.yaml",
                    title="write_allowlist.yaml",
                    body="- (:Statement)-[:DERIVED_FROM]->(:summarisedAnswer)",
                    path_hint="src/finai/write_allowlist.yaml",
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="contradiction",
                    subject_key="EvidenceChain.direction",
                    title_contains="EvidenceChain",
                ),
                ForensicReferenceExpectation(
                    category="contradiction",
                    subject_key="EvidenceChain.active_path",
                ),
                ForensicReferenceExpectation(
                    category="contradiction",
                    subject_key="EvidenceChain.full_path",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F08_neg_evidence_chain_alignment",
            class_id="F08",
            polarity="negative",
            title="Dokumentierte und implementierte Evidenzkette stimmen ueberein",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="local_doc",
                    source_id="_docs/bsm/target.md",
                    title="Target Architecture",
                    body="Die Evidenzkette ist bsmAnswer -> summarisedAnswer -> Statement -> BSM_Element.",
                ),
                _document(
                    source_type="github_file",
                    source_id="src/finai/bsm_statement_consolidation_service.py",
                    title="bsm_statement_consolidation_service.py",
                    body=(
                        "def persist_statement_chain():\n"
                        "    query = \"\"\"\n"
                        "    MATCH (sa:summarisedAnswer {id:$summary_id})\n"
                        "    CREATE (s:Statement {id:$statement_id})\n"
                        "    MERGE (s)-[:DERIVED_FROM]->(sa)\n"
                        "    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)\n"
                        "    \"\"\"\n"
                    ),
                    path_hint="src/finai/bsm_statement_consolidation_service.py",
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(category="contradiction", subject_key="EvidenceChain.direction"),
                ForensicReferenceExpectation(category="contradiction", subject_key="EvidenceChain.active_path"),
                ForensicReferenceExpectation(category="contradiction", subject_key="EvidenceChain.full_path"),
            ),
        ),
        ForensicReferenceCase(
            case_id="F08_pos_missing_statement_element_hop",
            class_id="F08",
            polarity="positive",
            title="Implementierte Evidenzkette endet ohne Statement-zu-BSM-Element-Hop",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/finai/bsm_statement_consolidation_service.py",
                    title="bsm_statement_consolidation_service.py",
                    body=(
                        "def persist_statement_chain():\n"
                        "    query = \"\"\"\n"
                        "    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})\n"
                        "    CREATE (s:Statement {id:$statement_id})\n"
                        "    MERGE (s)-[:DERIVED_FROM]->(u)\n"
                        "    \"\"\"\n"
                    ),
                    path_hint="src/finai/bsm_statement_consolidation_service.py",
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="implementation_drift",
                    metadata_key="risk_predicate",
                    metadata_value="code_evidence_chain_break",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F08_neg_statement_element_hop_present",
            class_id="F08",
            polarity="negative",
            title="Aktive Evidenzkette enthaelt den Statement-zu-BSM-Element-Hop",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/finai/bsm_statement_consolidation_service.py",
                    title="bsm_statement_consolidation_service.py",
                    body=(
                        "def persist_statement_chain():\n"
                        "    query = \"\"\"\n"
                        "    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})\n"
                        "    CREATE (s:Statement {id:$statement_id})\n"
                        "    MERGE (s)-[:DERIVED_FROM]->(u)\n"
                        "    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)\n"
                        "    \"\"\"\n"
                    ),
                    path_hint="src/finai/bsm_statement_consolidation_service.py",
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="implementation_drift",
                    metadata_key="risk_predicate",
                    metadata_value="code_evidence_chain_break",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F08_pos_missing_statement_derivation_hop",
            class_id="F08",
            polarity="positive",
            title="Aktive Evidenzkette enthaelt nur den Statement-zu-BSM-Element-Hop",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/finai/bsm_statement_consolidation_service.py",
                    title="bsm_statement_consolidation_service.py",
                    body=(
                        "def persist_statement_chain():\n"
                        "    query = \"\"\"\n"
                        "    MATCH (s:Statement {id:$statement_id})\n"
                        "    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)\n"
                        "    \"\"\"\n"
                    ),
                    path_hint="src/finai/bsm_statement_consolidation_service.py",
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="implementation_drift",
                    metadata_key="risk_predicate",
                    metadata_value="code_evidence_chain_break",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F08_pos_parallel_active_chain_variants",
            class_id="F08",
            polarity="positive",
            title="Aktive Implementierung fuehrt summary- und unit-zentrierte Ketten parallel",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/finai/bsm_statement_consolidation_service.py",
                    title="bsm_statement_consolidation_service.py",
                    body=(
                        "def persist_summary_chain():\n"
                        "    query = \"\"\"\n"
                        "    MATCH (sa:summarisedAnswer {id:$summary_id})\n"
                        "    CREATE (s:Statement {id:$statement_id})\n"
                        "    MERGE (s)-[:DERIVED_FROM]->(sa)\n"
                        "    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)\n"
                        "    \"\"\"\n\n"
                        "def persist_unit_chain():\n"
                        "    query = \"\"\"\n"
                        "    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})\n"
                        "    CREATE (s:Statement {id:$statement_id})\n"
                        "    MERGE (s)-[:DERIVED_FROM]->(u)\n"
                        "    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)\n"
                        "    \"\"\"\n"
                    ),
                    path_hint="src/finai/bsm_statement_consolidation_service.py",
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="implementation_drift",
                    metadata_key="risk_predicate",
                    metadata_value="code_evidence_chain_variant_conflict",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F08_pos_multiple_incomplete_chain_variants",
            class_id="F08",
            polarity="positive",
            title="Mehrere aktive Evidenzkettenvarianten enden vor dem Statement-zu-BSM-Element-Hop",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/finai/bsm_statement_consolidation_service.py",
                    title="bsm_statement_consolidation_service.py",
                    body=(
                        "def persist_summary_chain():\n"
                        "    query = \"\"\"\n"
                        "    MATCH (sa:summarisedAnswer {id:$summary_id})\n"
                        "    CREATE (s:Statement {id:$statement_id})\n"
                        "    MERGE (s)-[:DERIVED_FROM]->(sa)\n"
                        "    \"\"\"\n\n"
                        "def persist_unit_chain():\n"
                        "    query = \"\"\"\n"
                        "    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})\n"
                        "    CREATE (s:Statement {id:$statement_id})\n"
                        "    MERGE (s)-[:DERIVED_FROM]->(u)\n"
                        "    \"\"\"\n"
                    ),
                    path_hint="src/finai/bsm_statement_consolidation_service.py",
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="implementation_drift",
                    metadata_key="risk_predicate",
                    metadata_value="code_evidence_chain_break",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F08_neg_single_active_chain_variant",
            class_id="F08",
            polarity="negative",
            title="Aktive Implementierung fuehrt nur eine unit-zentrierte Kette",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/finai/bsm_statement_consolidation_service.py",
                    title="bsm_statement_consolidation_service.py",
                    body=(
                        "def persist_unit_chain():\n"
                        "    query = \"\"\"\n"
                        "    MATCH (u:summarisedAnswerUnit {id:$summary_unit_id})\n"
                        "    CREATE (s:Statement {id:$statement_id})\n"
                        "    MERGE (s)-[:DERIVED_FROM]->(u)\n"
                        "    MERGE (s)-[:SUPPORTS]->(e:BSM_Element)\n"
                        "    \"\"\"\n"
                    ),
                    path_hint="src/finai/bsm_statement_consolidation_service.py",
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="implementation_drift",
                    metadata_key="risk_predicate",
                    metadata_value="code_evidence_chain_variant_conflict",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F09_pos_eventual_consistency_gap",
            class_id="F09",
            polarity="positive",
            title="Persistenz und Reaggregation sind nur eventual consistent gekoppelt",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/finai/router_mining.py",
                    title="router_mining.py",
                    body="def save_manual_answer():\n    persist_answer(answer)\n    enqueue_reaggregation(answer.id)\n",
                    path_hint="src/finai/router_mining.py",
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="read_write_gap",
                    metadata_key="risk_predicate",
                    metadata_value="code_eventual_consistency_risk",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F09_neg_no_eventual_consistency_gap",
            class_id="F09",
            polarity="negative",
            title="Persistenzpfad zeigt keine asynchrone Luecke",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/finai/router_mining.py",
                    title="router_mining.py",
                    body="def save_manual_answer():\n    persist_answer(answer)\n    update_summary_sync(answer.id)\n",
                    path_hint="src/finai/router_mining.py",
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="read_write_gap",
                    metadata_key="risk_predicate",
                    metadata_value="code_eventual_consistency_risk",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F09_neg_comment_only_temporal_noise",
            class_id="F09",
            polarity="negative",
            title="Kommentierte Persist-/Enqueue-Hinweise erzeugen keinen Temporalbefund",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/finai/router_mining.py",
                    title="router_mining.py",
                    body=(
                        "def save_manual_answer():\n"
                        "    # persist_answer(answer)\n"
                        "    update_summary_sync(answer.id)\n"
                        "    # enqueue_reaggregation(answer.id)\n"
                    ),
                    path_hint="src/finai/router_mining.py",
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="read_write_gap",
                    metadata_key="risk_predicate",
                    metadata_value="code_eventual_consistency_risk",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F10_pos_chain_interruption_gap",
            class_id="F10",
            polarity="positive",
            title="Reaggregation unterbricht die aktive Kette durch Supersede vor Rebuild",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/finai/bsm_reaggregation_service.py",
                    title="bsm_reaggregation_service.py",
                    body="def rebuild_chain():\n    supersede_old_statements()\n    build_new_statements()\n",
                    path_hint="src/finai/bsm_reaggregation_service.py",
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="read_write_gap",
                    metadata_key="risk_predicate",
                    metadata_value="code_chain_interruption_risk",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F10_neg_no_chain_interruption_gap",
            class_id="F10",
            polarity="negative",
            title="Rebuild ohne Supersede erzeugt keinen aktiven Kettenbruch",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/finai/bsm_reaggregation_service.py",
                    title="bsm_reaggregation_service.py",
                    body="def rebuild_chain():\n    load_current_state()\n    build_new_statements()\n",
                    path_hint="src/finai/bsm_reaggregation_service.py",
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="read_write_gap",
                    metadata_key="risk_predicate",
                    metadata_value="code_chain_interruption_risk",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F11_pos_field_propagation_gap",
            class_id="F11",
            polarity="positive",
            title="Refine-Pfad verliert phase_run_id",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/finai/router_bsm_readiness.py",
                    title="router_bsm_readiness.py",
                    body=(
                        "def refine_statement_version(summary_id, statement_id):\n"
                        "    payload = {\n"
                        "        \"statement_id\": statement_id,\n"
                        "        \"target_label\": \"Statement\",\n"
                        "    }\n"
                        "    rebuild_bsm_element_from_statement(payload)\n"
                    ),
                    path_hint="src/finai/router_bsm_readiness.py",
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="implementation_drift",
                    metadata_key="risk_predicate",
                    metadata_value="code_field_propagation_gap",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F11_neg_field_propagation_ok",
            class_id="F11",
            polarity="negative",
            title="Refine-Pfad propagiert phase_run_id sauber weiter",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/finai/router_bsm_readiness.py",
                    title="router_bsm_readiness.py",
                    body=(
                        "def refine_statement_version(summary_id, statement_id, phase_run_id):\n"
                        "    payload = {\n"
                        "        \"statement_id\": statement_id,\n"
                        "        \"phase_run_id\": phase_run_id,\n"
                        "        \"target_label\": \"Statement\",\n"
                        "    }\n"
                        "    rebuild_bsm_element_from_statement(payload)\n"
                    ),
                    path_hint="src/finai/router_bsm_readiness.py",
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="implementation_drift",
                    metadata_key="risk_predicate",
                    metadata_value="code_field_propagation_gap",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F11_neg_comment_only_manual_noise",
            class_id="F11",
            polarity="negative",
            title="Kommentierte Manual-/Statement-Hinweise erzeugen keinen Propagationsbefund",
            detector="bsm_domain",
            current_status="covered",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/finai/core/services/bsm_service.py",
                    title="bsm_service.py",
                    body=(
                        "def capture_answer(payload):\n"
                        "    # mode = \"manual\"\n"
                        "    # statement_payload = {\"target_label\": \"Statement\"}\n"
                        "    write_cypher_guarded({\"target_label\": \"Document\"})\n"
                    ),
                    path_hint="src/finai/core/services/bsm_service.py",
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="implementation_drift",
                    metadata_key="risk_predicate",
                    metadata_value="code_field_propagation_gap",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F12_pos_legacy_path_weaker_than_primary",
            class_id="F12",
            polarity="positive",
            title="Legacy-Pfad ist schwaecher als der dokumentierte Hauptpfad",
            detector="finding_engine",
            current_status="covered",
            notes=(
                "Aktuell abgedeckt ist der doc- oder zielpfadgestuetzte Vergleich gegen historisch/sekundaer "
                "markierte Nebenpfade. Ein voll generischer Pfadfamilienvergleich fehlt weiterhin."
            ),
            claim_records=(
                _claim_record(
                    source_type="confluence_page",
                    source_id="page-12",
                    title="Primary Write Path",
                    subject_key="Statement.policy",
                    predicate="documented_policy",
                    normalized_value="primary path requires approval and phase_run_id propagation",
                    line_start=4,
                ),
                _claim_record(
                    source_type="github_file",
                    source_id="src/legacy_statement_service.py",
                    title="legacy_statement_service.py",
                    subject_key="Statement.policy",
                    predicate="implemented_policy",
                    normalized_value="direct publish without approval is allowed",
                    line_start=8,
                    path_hint="src/legacy_statement_service.py",
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="legacy_path_gap",
                    canonical_key="Statement.policy",
                    title_contains="Legacy",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F12_pos_primary_vs_fallback_code_path",
            class_id="F12",
            polarity="positive",
            title="Expliziter Primary- und Fallback-Pfad driften im Code auseinander",
            detector="finding_engine",
            current_status="covered",
            notes=(
                "Code-only-Fall fuer explizit markierte Pfadrollen. Das deckt noch nicht alle "
                "Pfadfamilien ab, aber erweitert F12 ueber rein historische Marker hinaus."
            ),
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/primary_statement_service.py",
                    title="primary_statement_service.py",
                    body=(
                        "def persist_primary_statement(statement):\n"
                        "    # primary path\n"
                        "    approval_required_before_save(statement)\n"
                    ),
                    path_hint="src/primary_statement_service.py",
                ),
                _document(
                    source_type="github_file",
                    source_id="src/fallback_statement_service.py",
                    title="fallback_statement_service.py",
                    body=(
                        "def persist_fallback_statement(statement):\n"
                        "    direct_write_without_approval(statement)\n"
                    ),
                    path_hint="src/fallback_statement_service.py",
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="legacy_path_gap",
                    canonical_key="Statement.write_path",
                    title_contains="Nebenpfad",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F12_neg_no_weaker_legacy_path",
            class_id="F12",
            polarity="negative",
            title="Legacy-Pfad ist zum Hauptpfad fachlich gleichwertig",
            detector="finding_engine",
            current_status="covered",
            notes=(
                "Negativfall fuer den aktuell implementierten Teilpfad: historisch markierter Nebenpfad, "
                "der dem fuehrenden Pfad nicht widerspricht."
            ),
            claim_records=(
                _claim_record(
                    source_type="confluence_page",
                    source_id="page-12",
                    title="Primary Write Path",
                    subject_key="Statement.policy",
                    predicate="documented_policy",
                    normalized_value="primary path requires approval and phase_run_id propagation",
                    line_start=4,
                ),
                _claim_record(
                    source_type="github_file",
                    source_id="src/legacy_statement_service.py",
                    title="legacy_statement_service.py",
                    subject_key="Statement.policy",
                    predicate="implemented_policy",
                    normalized_value="primary path requires approval and phase_run_id propagation",
                    line_start=8,
                    path_hint="src/legacy_statement_service.py",
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="legacy_path_gap",
                    canonical_key="Statement.policy",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F12_neg_primary_vs_fallback_code_alignment",
            class_id="F12",
            polarity="negative",
            title="Expliziter Primary- und Fallback-Pfad sind im Code konsistent",
            detector="finding_engine",
            current_status="covered",
            notes="Negativfall fuer explizit markierte code-only Pfadrollen.",
            documents=(
                _document(
                    source_type="github_file",
                    source_id="src/primary_statement_service.py",
                    title="primary_statement_service.py",
                    body=(
                        "def persist_primary_statement(statement):\n"
                        "    approval_required_before_save(statement)\n"
                    ),
                    path_hint="src/primary_statement_service.py",
                ),
                _document(
                    source_type="github_file",
                    source_id="src/fallback_statement_service.py",
                    title="fallback_statement_service.py",
                    body=(
                        "def persist_fallback_statement(statement):\n"
                        "    approval_required_before_save(statement)\n"
                    ),
                    path_hint="src/fallback_statement_service.py",
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="legacy_path_gap",
                    canonical_key="Statement.write_path",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F12_pos_multi_primary_family_assignment",
            class_id="F12",
            polarity="positive",
            title="Mehrere Primärpfade ordnen Nebenpfade qualifizierten Servicefamilien zu",
            detector="finding_engine",
            current_status="covered",
            notes=(
                "Deckt den neu hinzugekommenen Teilpfad ab, in dem mehrere Primärpfadfamilien "
                "gegen mehrere Nebenpfade über qualifizierte Delegationsketten zugeordnet werden."
            ),
            claim_records=(
                _claim_record(
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
                ),
                _claim_record(
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
                ),
                _claim_record(
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
                ),
                _claim_record(
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
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="legacy_path_gap",
                    canonical_key="Statement.write_path",
                    title_contains="Nebenpfad",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F12_neg_module_collision_without_behavior_drift",
            class_id="F12",
            polarity="negative",
            title="Namenskollision allein erzeugt ohne Verhaltensdrift keinen Legacy-Befund",
            detector="finding_engine",
            current_status="covered",
            notes=(
                "Negativfall für denselben Namensraumkonflikt über Module hinweg, wenn beide Pfade "
                "fachlich dieselbe Policy tragen."
            ),
            claim_records=(
                _claim_record(
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
                ),
                _claim_record(
                    source_type="github_file",
                    source_id="src/legacy/statement_dispatcher.py",
                    title="statement_dispatcher.py",
                    subject_key="Statement.write_path",
                    predicate="implemented_write",
                    normalized_value="approval required before save",
                    line_start=24,
                    metadata={
                        "static_call_graph_paths": ["StatementDispatcher.persist -> StatementPipeline.run_fallback_path"],
                        "static_call_graph_qualified_paths": [
                            "legacy.dispatch.StatementDispatcher.persist -> legacy.pipeline.StatementPipeline.run_fallback_path"
                        ],
                        "path_variant_role": "fallback",
                    },
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="legacy_path_gap",
                    canonical_key="Statement.write_path",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F12_pos_same_role_variants_split_into_family_matches",
            class_id="F12",
            polarity="positive",
            title="Mehrere Fallback-Pfade bleiben trotz gleicher Rolle als getrennte Familien sichtbar",
            detector="finding_engine",
            current_status="covered",
            notes=(
                "Deckt den erweiterten Teilpfad ab, in dem Varianten gleicher Rolle nicht mehr "
                "nur nach Rolle zusammenfallen, sondern als eigene Familien-Match-Gruppen erhalten bleiben."
            ),
            claim_records=(
                _claim_record(
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
                ),
                _claim_record(
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
                ),
                _claim_record(
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
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="legacy_path_gap",
                    canonical_key="Statement.write_path",
                    title_contains="Nebenpfad",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F12_pos_unmarked_weaker_side_path_inferred_from_code",
            class_id="F12",
            polarity="positive",
            title="Ungemarkierter Nebenpfad wird aus Drift und Delegationskette inferiert",
            detector="finding_engine",
            current_status="covered",
            notes=(
                "Deckt den neuen Teilpfad ab, in dem `F12` auch ohne explizite Rollenmarker greift, "
                "wenn ein technisch schwächerer Nebenpfad über Drift und Delegationskette erkennbar ist."
            ),
            claim_records=(
                _claim_record(
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
                ),
                _claim_record(
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
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="legacy_path_gap",
                    canonical_key="Statement.write_path",
                    title_contains="Nebenpfad",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F12_neg_unmarked_parallel_primary_paths",
            class_id="F12",
            polarity="negative",
            title="Ungemarkierte parallele Primärpfade erzeugen ohne Drift keinen Legacy-Befund",
            detector="finding_engine",
            current_status="covered",
            notes=(
                "Negativfall für mehrere starke Implementierungspfade ohne Rollenmarker. "
                "Solange kein schwächerer Nebenpfad erkennbar ist, darf `F12` nicht auslösen."
            ),
            claim_records=(
                _claim_record(
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
                ),
                _claim_record(
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
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="legacy_path_gap",
                    canonical_key="Statement.write_path",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F12_pos_adapter_based_side_path_inference",
            class_id="F12",
            polarity="positive",
            title="Nebenpfad wird auch ohne Call-Graph über Adapter- und Injected-Familien erkannt",
            detector="finding_engine",
            current_status="covered",
            notes=(
                "Deckt den verbleibenden F12-Restpfad ab, in dem keine statische Call-Graph-Kette "
                "vorliegt, aber Repository-/Driver-/Injection-Signale den schwächeren Nebenpfad "
                "gegen den stärkeren Hauptpfad abgrenzen."
            ),
            claim_records=(
                _claim_record(
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
                ),
                _claim_record(
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
                ),
            ),
            expected_findings=(
                ForensicReferenceExpectation(
                    category="legacy_path_gap",
                    canonical_key="Statement.write_path",
                    title_contains="Nebenpfad",
                ),
            ),
        ),
        ForensicReferenceCase(
            case_id="F12_neg_adapter_based_parallel_primary_paths",
            class_id="F12",
            polarity="negative",
            title="Adapter-/Injection-Familien allein erzeugen ohne Drift keinen Legacy-Befund",
            detector="finding_engine",
            current_status="covered",
            notes=(
                "Negativfall für mehrere starke Implementierungspfade ohne Call-Graph, wenn weder "
                "fachlicher Drift noch ein schwächerer Nebenpfad erkennbar ist."
            ),
            claim_records=(
                _claim_record(
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
                ),
                _claim_record(
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
                ),
            ),
            forbidden_findings=(
                ForensicReferenceExpectation(
                    category="legacy_path_gap",
                    canonical_key="Statement.write_path",
                ),
            ),
        ),
    )


def detect_reference_case_findings(*, case: ForensicReferenceCase):
    if case.detector == "finding_engine":
        records = list(case.claim_records)
        if case.documents:
            records = extract_claim_records(documents=list(case.documents))
        findings, _ = generate_findings(claim_records=records, inherited_truths=[])
        return findings
    if case.detector == "bsm_domain":
        records = extract_claim_records(documents=list(case.documents))
        return detect_bsm_domain_contradictions(claim_records=records)
    if case.detector == "documentation_gap":
        return detect_documentation_gaps(
            claim_records=list(case.claim_records),
            documents=list(case.documents),
        )
    if case.detector == "pending":
        return []
    raise ValueError(f"Unbekannter Detector fuer Referenzfall: {case.detector}")


def _document(
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
    descriptor = " ".join([title, str(path_hint or ""), normalized_value]).casefold()
    is_legacy = any(token in descriptor for token in ("legacy", "deprecated", "historic", "archive", "veraltet"))
    location = AuditLocation(
        source_type=source_type,  # type: ignore[arg-type]
        source_id=source_id,
        title=title,
        path_hint=path_hint,
        position=AuditPosition(
            anchor_kind="file_line_range",
            anchor_value=f"{source_id}#L{line_start}",
            line_start=line_start,
            line_end=line_start,
        ),
    )
    claim_metadata: dict[str, object] = dict(metadata or {})
    if is_legacy:
        claim_metadata.update(
            {
                "assertion_status": "deprecated",
                "source_authority": "historical",
                "source_governance_level": "historical",
                "source_temporal_status": "historical",
            }
        )
    claim = AuditClaimEntry(
        claim_id=f"claim_{uuid4().hex}",
        source_snapshot_id=f"snapshot_{uuid4().hex}",
        source_type=source_type,  # type: ignore[arg-type]
        source_id=source_id,
        subject_kind="object",
        subject_key=subject_key,
        predicate=predicate,
        normalized_value=normalized_value,
        scope_kind="global",
        scope_key="FINAI",
        fingerprint=f"{subject_key}|{predicate}|{normalized_value}|FINAI",
        metadata=claim_metadata,
    )
    return ExtractedClaimRecord(
        claim=claim,
        evidence=ExtractedClaimEvidence(location=location, matched_text=normalized_value),
    )
