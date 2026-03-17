from __future__ import annotations

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
