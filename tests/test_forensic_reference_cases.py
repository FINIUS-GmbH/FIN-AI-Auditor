from __future__ import annotations

from fin_ai_auditor.domain.models import AuditSourceSnapshot
from fin_ai_auditor.services.claim_extractor import extract_claim_records
from fin_ai_auditor.services.documentation_gap_detector import detect_documentation_gaps
from fin_ai_auditor.services.finding_engine import generate_findings
from fin_ai_auditor.services.pipeline_models import CollectedDocument

from tests.forensic_reference_cases import (
    ForensicReferenceExpectation,
    build_forensic_reference_cases,
    detect_reference_case_findings,
)


def test_forensic_reference_cases_cover_all_p0_classes_with_pos_and_neg() -> None:
    cases = build_forensic_reference_cases()
    by_class: dict[str, set[str]] = {}
    for case in cases:
        by_class.setdefault(case.class_id, set()).add(case.polarity)

    assert set(by_class) == {"F01", "F02", "F03", "F04", "F05", "F06", "F07", "F08", "F09", "F10", "F11", "F12"}
    for polarities in by_class.values():
        assert polarities == {"positive", "negative"}


def test_forensic_reference_cases_expose_honest_status_matrix() -> None:
    cases = build_forensic_reference_cases()
    by_class: dict[str, set[str]] = {}
    for case in cases:
        by_class.setdefault(case.class_id, set()).add(case.current_status)

    assert {class_id for class_id, statuses in by_class.items() if statuses == {"covered"}} == {
        "F01",
        "F02",
        "F03",
        "F04",
        "F05",
        "F06",
        "F07",
        "F08",
        "F09",
        "F10",
        "F11",
        "F12",
    }
    assert {class_id for class_id, statuses in by_class.items() if statuses == {"partial"}} == set()
    assert {class_id for class_id, statuses in by_class.items() if statuses == {"open"}} == set()


def test_forensic_reference_cases_behave_as_expected_for_executable_subset() -> None:
    cases = build_forensic_reference_cases()
    for case in cases:
        if not case.is_executable():
            continue
        findings = detect_reference_case_findings(case=case)
        for expectation in case.expected_findings:
            assert any(_matches(finding, expectation) for finding in findings), case.case_id
        for expectation in case.forbidden_findings:
            assert not any(_matches(finding, expectation) for finding in findings), case.case_id


def test_forensic_policy_conflict_closes_document_to_claim_to_finding_path() -> None:
    documents = [
        _document(
            source_type="local_doc",
            source_id="_docs/statement.md",
            title="Statement Contract",
            body="# Statement\nWrite flow is approval-gated and review-only.\n",
            content_hash="sha256:doc-policy",
        ),
        _document(
            source_type="github_file",
            source_id="src/statement_service.py",
            title="statement_service.py",
            body="def persist_statement():\n    # direct write without approval\n    return save_statement()\n",
            content_hash="sha256:code-policy",
        ),
    ]

    claim_records, findings = _claims_and_findings_from_documents(documents)

    assert any(record.claim.predicate == "documented_policy" for record in claim_records)
    assert any(record.claim.predicate == "implemented_policy" for record in claim_records)

    assert any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="policy_conflict",
                canonical_key="Statement.policy",
                title_contains="Policy",
            ),
        )
        for finding in findings
    )


def test_forensic_policy_alignment_does_not_raise_false_positive_over_document_path() -> None:
    documents = [
        _document(
            source_type="local_doc",
            source_id="_docs/statement.md",
            title="Statement Contract",
            body="# Statement\nWrite flow is approval-gated and review-only.\n",
            content_hash="sha256:doc-policy-ok",
        ),
        _document(
            source_type="github_file",
            source_id="src/statement_service.py",
            title="statement_service.py",
            body="def persist_statement():\n    return guarded_review_write()\n",
            content_hash="sha256:code-policy-ok",
        ),
    ]

    claim_records, findings = _claims_and_findings_from_documents(documents)

    assert any(record.claim.predicate == "documented_policy" for record in claim_records)
    assert any(record.claim.predicate == "implemented_policy" for record in claim_records)

    assert not any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="policy_conflict",
                canonical_key="Statement.policy",
            ),
        )
        for finding in findings
    )


def test_forensic_doc_doc_conflict_closes_document_to_claim_to_finding_path() -> None:
    positive_documents = [
        _document(
            source_type="confluence_page",
            source_id="page-1",
            title="Statement Policy",
            body="# Statement Policy\nWrite flow is approval-gated and review-only.\n",
            content_hash="sha256:f01-doc-pos-a",
        ),
        _document(
            source_type="local_doc",
            source_id="_docs/statement.md",
            title="Statement Policy Draft",
            body="# Statement Policy\nDirect write is allowed.\n",
            content_hash="sha256:f01-doc-pos-b",
        ),
    ]
    negative_documents = [
        _document(
            source_type="confluence_page",
            source_id="page-1",
            title="Statement Policy",
            body="# Statement Policy\nWrite flow is approval-gated and review-only.\n",
            content_hash="sha256:f01-doc-neg-a",
        ),
        _document(
            source_type="local_doc",
            source_id="_docs/statement.md",
            title="Statement Policy Draft",
            body="# Statement Policy\nWrite flow is approval-gated and review-only.\n",
            content_hash="sha256:f01-doc-neg-b",
        ),
    ]

    positive_claims, positive_findings = _claims_and_findings_from_documents(positive_documents)
    negative_claims, negative_findings = _claims_and_findings_from_documents(negative_documents)

    assert any(record.claim.predicate == "documented_write" for record in positive_claims)
    assert any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="contradiction",
                canonical_key="Statement.write_path",
                title_contains="widersprechen",
            ),
        )
        for finding in positive_findings
    )
    assert any(record.claim.predicate == "documented_write" for record in negative_claims)
    assert not any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="contradiction",
                canonical_key="Statement.write_path",
            ),
        )
        for finding in negative_findings
    )


def test_forensic_doc_metamodel_phase_count_closes_document_to_claim_to_finding_path() -> None:
    positive_documents = [
        _document(
            source_type="metamodel",
            source_id="current_dump",
            title="current_dump",
            body='[{"phase_id":"draft","phase_name":"Draft"},{"phase_id":"review","phase_name":"Review"},{"phase_id":"release","phase_name":"Release"}]',
            content_hash="sha256:f02-meta-pos",
        ),
        _document(
            source_type="confluence_page",
            source_id="page-2",
            title="Process Definition",
            body="# Process\nBSM process has 4 phases.\n",
            content_hash="sha256:f02-doc-pos",
        ),
    ]
    negative_documents = [
        _document(
            source_type="metamodel",
            source_id="current_dump",
            title="current_dump",
            body='[{"phase_id":"draft","phase_name":"Draft"},{"phase_id":"review","phase_name":"Review"},{"phase_id":"release","phase_name":"Release"}]',
            content_hash="sha256:f02-meta-neg",
        ),
        _document(
            source_type="confluence_page",
            source_id="page-2",
            title="Process Definition",
            body="# Process\nBSM process has 3 phases.\n",
            content_hash="sha256:f02-doc-neg",
        ),
    ]

    positive_claims, positive_findings = _claims_and_findings_from_documents(positive_documents)
    negative_claims, negative_findings = _claims_and_findings_from_documents(negative_documents)

    assert any(
        record.claim.subject_key == "BSM.process" and record.claim.predicate == "phase_count"
        for record in positive_claims
    )
    assert any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="contradiction",
                canonical_key="BSM.process",
                title_contains="Metamodell",
            ),
        )
        for finding in positive_findings
    )
    assert any(
        record.claim.subject_key == "BSM.process" and record.claim.predicate == "phase_count"
        for record in negative_claims
    )
    assert not any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="contradiction",
                canonical_key="BSM.process",
            ),
        )
        for finding in negative_findings
    )


def test_forensic_doc_code_drift_closes_document_to_claim_to_finding_path() -> None:
    positive_documents = [
        _document(
            source_type="local_doc",
            source_id="_docs/statement.md",
            title="Statement Contract",
            body="# Statement\nWrite path goes over review service.\n",
            content_hash="sha256:f03-doc-pos",
        ),
        _document(
            source_type="github_file",
            source_id="src/statement_service.py",
            title="statement_service.py",
            body="def persist_statement():\n    # direct write through worker queue\n    return enqueue_and_save()\n",
            content_hash="sha256:f03-code-pos",
        ),
    ]
    negative_documents = [
        _document(
            source_type="local_doc",
            source_id="_docs/statement.md",
            title="Statement Contract",
            body="# Statement\nWrite path is approval-gated and review-only.\n",
            content_hash="sha256:f03-doc-neg",
        ),
        _document(
            source_type="github_file",
            source_id="src/statement_service.py",
            title="statement_service.py",
            body="def persist_statement():\n    return guarded_review_write()\n",
            content_hash="sha256:f03-code-neg",
        ),
    ]

    positive_claims, positive_findings = _claims_and_findings_from_documents(positive_documents)
    negative_claims, negative_findings = _claims_and_findings_from_documents(negative_documents)

    assert any(record.claim.predicate == "documented_write" for record in positive_claims)
    assert any(record.claim.predicate == "implemented_write" for record in positive_claims)
    assert any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="implementation_drift",
                canonical_key="Statement.write_path",
            ),
        )
        for finding in positive_findings
    )
    assert any(record.claim.predicate == "documented_write" for record in negative_claims)
    assert any(record.claim.predicate == "implemented_write" for record in negative_claims)
    assert not any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="implementation_drift",
                canonical_key="Statement.write_path",
            ),
        )
        for finding in negative_findings
    )


def test_forensic_documented_path_missing_in_code_closes_document_to_claim_to_finding_path() -> None:
    positive_documents = [
        _document(
            source_type="confluence_page",
            source_id="page-2",
            title="Statement Write Contract",
            body="# Statement\nWrite path for Statement goes over StatementService.persist and repository save.\n",
            content_hash="sha256:f04-doc-pos",
        ),
    ]
    negative_documents = [
        _document(
            source_type="confluence_page",
            source_id="page-2",
            title="Statement Write Contract",
            body="# Statement\nWrite path for Statement is approval guarded.\n",
            content_hash="sha256:f04-doc-neg",
        ),
        _document(
            source_type="github_file",
            source_id="src/statement_service.py",
            title="statement_service.py",
            body="def persist_statement():\n    save_statement()\n",
            content_hash="sha256:f04-code-neg",
        ),
    ]

    positive_claims, positive_findings = _claims_and_findings_from_documents(positive_documents)
    negative_claims, negative_findings = _claims_and_findings_from_documents(negative_documents)

    assert any(record.claim.predicate == "documented_write" for record in positive_claims)
    assert any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="implementation_drift",
                canonical_key="Statement.write_path",
                title_contains="nicht implementiert",
            ),
        )
        for finding in positive_findings
    )
    assert any(record.claim.predicate == "documented_write" for record in negative_claims)
    assert any(record.claim.predicate == "implemented_write" for record in negative_claims)
    assert not any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="implementation_drift",
                canonical_key="Statement.write_path",
            ),
        )
        for finding in negative_findings
    )


def test_forensic_missing_documentation_closes_document_to_claim_to_finding_path() -> None:
    positive_documents = [
        _document(
            source_type="confluence_page",
            source_id="page-5",
            title="Statement Overview",
            body="# Statement\nStatement is the review artifact.\n",
            content_hash="sha256:f05-doc-pos-a",
        ),
        _document(
            source_type="github_file",
            source_id="src/statement_policy_service.py",
            title="statement_policy_service.py",
            body=(
                "def publish_statement():\n"
                "    # approval token required before publish\n"
                "    preflight_checks()\n"
                "    return publish()\n"
            ),
            content_hash="sha256:f05-code-pos",
        ),
    ]
    negative_documents = [
        _document(
            source_type="confluence_page",
            source_id="page-5",
            title="Statement Overview",
            body="# Statement\nStatement is the review artifact.\n",
            content_hash="sha256:f05-doc-neg-a",
        ),
        _document(
            source_type="local_doc",
            source_id="_docs/statement-policy.md",
            title="Statement Policy",
            body="# Statement Policy\nWrite path is approval guarded.\napproval token required before publish\n",
            content_hash="sha256:f05-doc-neg-b",
        ),
        _document(
            source_type="github_file",
            source_id="src/statement_policy_service.py",
            title="statement_policy_service.py",
            body="def publish_statement():\n    # approval token required before publish\n    return publish()\n",
            content_hash="sha256:f05-code-neg",
        ),
    ]

    positive_claims, positive_gaps = _claims_and_doc_gaps_from_documents(positive_documents)
    negative_claims, negative_gaps = _claims_and_doc_gaps_from_documents(negative_documents)

    assert any(record.claim.predicate == "implemented_policy" for record in positive_claims)
    assert any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="missing_documentation",
                canonical_key="doc_gap:Statement.policy",
            ),
        )
        for finding in positive_gaps
    )
    assert any(record.claim.predicate == "documented_policy" for record in negative_claims)
    assert any(record.claim.predicate == "documented_write" for record in negative_claims)
    assert any(record.claim.predicate == "implemented_policy" for record in negative_claims)
    assert not any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="missing_documentation",
                canonical_key="doc_gap:Statement.policy",
            ),
        )
        for finding in negative_gaps
    )


def test_forensic_lifecycle_paths_close_document_to_claim_to_finding_path() -> None:
    doc_doc_positive = [
        _document(
            source_type="confluence_page",
            source_id="page-7",
            title="Statement Lifecycle",
            body="# Lifecycle\nStatement remains in review.\n",
            content_hash="sha256:f07-docdoc-pos-a",
        ),
        _document(
            source_type="local_doc",
            source_id="_docs/statement-lifecycle.md",
            title="Statement Lifecycle Draft",
            body="# Lifecycle\nStatement is released immediately.\n",
            content_hash="sha256:f07-docdoc-pos-b",
        ),
    ]
    doc_doc_negative = [
        _document(
            source_type="confluence_page",
            source_id="page-7",
            title="Statement Lifecycle",
            body="# Lifecycle\nStatement remains in review.\n",
            content_hash="sha256:f07-docdoc-neg-a",
        ),
        _document(
            source_type="local_doc",
            source_id="_docs/statement-lifecycle.md",
            title="Statement Lifecycle Draft",
            body="# Lifecycle\nStatement remains in review.\n",
            content_hash="sha256:f07-docdoc-neg-b",
        ),
    ]
    doc_code_positive = [
        _document(
            source_type="local_doc",
            source_id="_docs/statement-lifecycle.md",
            title="Statement Lifecycle",
            body="# Lifecycle\nStatement remains in review.\n",
            content_hash="sha256:f07-doccode-pos-a",
        ),
        _document(
            source_type="github_file",
            source_id="src/statement_status_service.py",
            title="statement_status_service.py",
            body="def release_statement():\n    status = 'released'\n    return status\n",
            content_hash="sha256:f07-doccode-pos-b",
        ),
    ]
    doc_code_negative = [
        _document(
            source_type="local_doc",
            source_id="_docs/statement-lifecycle.md",
            title="Statement Lifecycle",
            body="# Lifecycle\nStatement remains in review.\n",
            content_hash="sha256:f07-doccode-neg-a",
        ),
        _document(
            source_type="github_file",
            source_id="src/statement_status_service.py",
            title="statement_status_service.py",
            body="def keep_statement_in_review():\n    status = 'in review'\n    return status\n",
            content_hash="sha256:f07-doccode-neg-b",
        ),
    ]
    doc_meta_positive = [
        _document(
            source_type="local_doc",
            source_id="_docs/statement-lifecycle.md",
            title="Statement Lifecycle",
            body="# Lifecycle\nStatement is released immediately.\n",
            content_hash="sha256:f07-docmeta-pos-a",
        ),
        _document(
            source_type="metamodel",
            source_id="metamodel_dump",
            title="current_dump",
            body='[{"entity_kind":"metaclass","metaclass_name":"Statement","initial_status":"draft","lifecycle":"review required before release"}]',
            content_hash="sha256:f07-docmeta-pos-b",
        ),
    ]
    doc_meta_negative = [
        _document(
            source_type="local_doc",
            source_id="_docs/statement-lifecycle.md",
            title="Statement Lifecycle",
            body="# Lifecycle\nStatement is draft.\n",
            content_hash="sha256:f07-docmeta-neg-a",
        ),
        _document(
            source_type="metamodel",
            source_id="metamodel_dump",
            title="current_dump",
            body='[{"entity_kind":"metaclass","metaclass_name":"Statement","initial_status":"draft","lifecycle":"review required before release"}]',
            content_hash="sha256:f07-docmeta-neg-b",
        ),
    ]

    _, doc_doc_positive_findings = _claims_and_findings_from_documents(doc_doc_positive)
    _, doc_doc_negative_findings = _claims_and_findings_from_documents(doc_doc_negative)
    _, doc_code_positive_findings = _claims_and_findings_from_documents(doc_code_positive)
    _, doc_code_negative_findings = _claims_and_findings_from_documents(doc_code_negative)
    _, doc_meta_positive_findings = _claims_and_findings_from_documents(doc_meta_positive)
    _, doc_meta_negative_findings = _claims_and_findings_from_documents(doc_meta_negative)

    assert any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="contradiction",
                canonical_key="Statement.review_status",
            ),
        )
        for finding in doc_doc_positive_findings
    )
    assert not any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="contradiction",
                canonical_key="Statement.review_status",
            ),
        )
        for finding in doc_doc_negative_findings
    )
    assert any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="implementation_drift",
                canonical_key="Statement.review_status",
            ),
        )
        for finding in doc_code_positive_findings
    )
    assert not any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="implementation_drift",
                canonical_key="Statement.review_status",
            ),
        )
        for finding in doc_code_negative_findings
    )
    assert any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="contradiction",
                canonical_key="Statement.review_status",
                title_contains="Metamodell",
            ),
        )
        for finding in doc_meta_positive_findings
    )
    assert not any(
        _matches(
            finding,
            ForensicReferenceExpectation(
                category="contradiction",
                canonical_key="Statement.review_status",
            ),
        )
        for finding in doc_meta_negative_findings
    )
def test_partial_reference_cases_are_explicitly_marked() -> None:
    cases = build_forensic_reference_cases()
    partial_cases = [case for case in cases if case.current_status == "partial"]

    assert partial_cases == []


def _matches(finding, expectation: ForensicReferenceExpectation) -> bool:
    if finding.category != expectation.category:
        return False
    if expectation.canonical_key and str(finding.canonical_key or "").strip() != expectation.canonical_key:
        return False
    if expectation.subject_key and str(finding.metadata.get("subject_key") or "").strip() != expectation.subject_key:
        return False
    if expectation.title_contains and expectation.title_contains.casefold() not in finding.title.casefold():
        return False
    if expectation.metadata_key:
        value = str(finding.metadata.get(expectation.metadata_key) or "").strip()
        if value != str(expectation.metadata_value or "").strip():
            return False
    return True


def _document(
    *,
    source_type: str,
    source_id: str,
    title: str,
    body: str,
    content_hash: str,
) -> CollectedDocument:
    return CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type=source_type,  # type: ignore[arg-type]
            source_id=source_id,
            content_hash=content_hash,
        ),
        source_type=source_type,  # type: ignore[arg-type]
        source_id=source_id,
        title=title,
        body=body,
        path_hint=source_id,
    )


def _claims_and_findings_from_documents(documents: list[CollectedDocument]):
    claim_records = extract_claim_records(documents=documents)
    findings, _ = generate_findings(claim_records=claim_records, inherited_truths=[])
    return claim_records, findings


def _claims_and_doc_gaps_from_documents(documents: list[CollectedDocument]):
    claim_records = extract_claim_records(documents=documents)
    findings = detect_documentation_gaps(claim_records=claim_records, documents=documents)
    return claim_records, findings
