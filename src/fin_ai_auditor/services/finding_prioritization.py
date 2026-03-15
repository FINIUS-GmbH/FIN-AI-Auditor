from __future__ import annotations

from fin_ai_auditor.domain.models import AuditFinding


ROOT_CAUSE_PRIORITY: dict[str, int] = {
    "truth": 0,
    "write_contract": 1,
    "policy": 2,
    "lifecycle": 3,
    "process": 4,
    "implementation": 5,
    "documentation": 6,
    "clarification": 7,
    "misc": 8,
}
ROOT_CAUSE_LABELS: dict[str, str] = {
    "truth": "Wahrheitsabgleich",
    "write_contract": "Write-Contract",
    "policy": "Policy",
    "lifecycle": "Lifecycle",
    "process": "Prozessbruch",
    "implementation": "Implementierung",
    "documentation": "Dokumentation",
    "clarification": "Klaerung",
    "misc": "Sonstiges",
}
CORE_ROOT_CAUSE_BUCKETS: frozenset[str] = frozenset(
    {"truth", "write_contract", "policy", "lifecycle", "process"}
)
SUPPORTING_DETAIL_CATEGORIES: frozenset[str] = frozenset(
    {
        "missing_definition",
        "missing_documentation",
        "traceability_gap",
        "ownership_gap",
        "low_confidence_review",
        "obsolete_documentation",
        "stale_source",
        "clarification_needed",
        "terminology_collision",
        "open_decision",
    }
)


def severity_rank(*, severity: str) -> int:
    return {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(str(severity), 9)


def root_cause_priority(*, bucket: str) -> int:
    return ROOT_CAUSE_PRIORITY.get(str(bucket or "").strip(), 99)


def root_cause_label(*, bucket: str) -> str:
    normalized = str(bucket or "").strip()
    return ROOT_CAUSE_LABELS.get(normalized, normalized or "Sonstiges")


def is_core_root_cause_bucket(*, bucket: str) -> bool:
    return str(bucket or "").strip() in CORE_ROOT_CAUSE_BUCKETS


def finding_root_cause_bucket(*, finding: AuditFinding) -> str:
    metadata = finding.metadata or {}
    object_key = str(metadata.get("object_key") or finding.canonical_key or "").strip()
    normalized_text = " ".join(
        [
            object_key,
            str(finding.category or ""),
            str(finding.title or ""),
            str(finding.summary or ""),
            str(finding.recommendation or ""),
            " ".join(_string_list(metadata.get("semantic_contract_paths"))),
            " ".join(_string_list(metadata.get("semantic_context"))),
        ]
    ).casefold()

    if bool(metadata.get("truth_enforcement")):
        return "truth"
    if (
        object_key.endswith((".write_path", ".read_path"))
        or finding.category == "read_write_gap"
        or any(token in normalized_text for token in ("write contract", "read contract", "write path", "read path"))
        or any(
            "write_contract" in path.casefold() or "read_contract" in path.casefold()
            for path in _string_list(metadata.get("semantic_contract_paths"))
        )
    ):
        return "write_contract"
    if (
        object_key.endswith((".policy", ".approval_policy", ".scope_policy"))
        or finding.category == "policy_conflict"
        or any(token in normalized_text for token in ("approval", "guardrail", "policy"))
    ):
        return "policy"
    if (
        object_key.endswith((".lifecycle", ".review_status"))
        or any(token in normalized_text for token in ("lifecycle", "review status", "status-kanon", "status canon"))
    ):
        return "lifecycle"
    if (
        object_key == "BSM.process"
        or object_key.startswith("BSM.phase.")
        or object_key.startswith("BSM.process.")
        or any(token in normalized_text for token in ("bsm.process", "bsm process", "prozess", "phase"))
    ):
        return "process"
    if finding.category in {"implementation_drift"}:
        return "implementation"
    if finding.category in {
        "missing_definition",
        "missing_documentation",
        "traceability_gap",
        "ownership_gap",
        "low_confidence_review",
        "obsolete_documentation",
        "stale_source",
        "terminology_collision",
    }:
        return "documentation"
    if finding.category in {"clarification_needed", "open_decision"}:
        return "clarification"
    return "misc"


def assigned_root_cause_bucket(
    *,
    finding: AuditFinding,
    available_core_buckets: set[str],
) -> str:
    own_bucket = finding_root_cause_bucket(finding=finding)
    if not available_core_buckets:
        return own_bucket
    if own_bucket in available_core_buckets:
        return own_bucket
    return min(available_core_buckets, key=lambda bucket: (root_cause_priority(bucket=bucket), bucket))


def prioritize_findings(*, findings: list[AuditFinding]) -> list[AuditFinding]:
    return sorted(findings, key=_global_finding_sort_key)


def select_primary_finding(
    *,
    findings: list[AuditFinding],
    package_bucket: str,
) -> AuditFinding | None:
    if not findings:
        return None
    return sorted(findings, key=lambda finding: _package_finding_sort_key(finding=finding, package_bucket=package_bucket))[0]


def order_package_findings(
    *,
    findings: list[AuditFinding],
    package_bucket: str,
) -> list[AuditFinding]:
    primary = select_primary_finding(findings=findings, package_bucket=package_bucket)
    primary_finding_id = primary.finding_id if primary is not None else None
    return sorted(
        findings,
        key=lambda finding: (
            0 if finding.finding_id == primary_finding_id else 1,
            severity_rank(severity=finding.severity),
            0 if finding_root_cause_bucket(finding=finding) == package_bucket else 1,
            _detail_penalty(finding=finding),
            str(finding.metadata.get("object_key") or finding.canonical_key or finding.title or ""),
            finding.title,
        ),
    )


def _global_finding_sort_key(finding: AuditFinding) -> tuple[int, int, int, int, str, str]:
    bucket = finding_root_cause_bucket(finding=finding)
    return (
        root_cause_priority(bucket=bucket),
        severity_rank(severity=finding.severity),
        _detail_penalty(finding=finding),
        0 if bool(finding.metadata.get("delta_scope_affected")) else 1,
        str(finding.metadata.get("object_key") or finding.canonical_key or finding.title or ""),
        finding.title,
    )


def _package_finding_sort_key(*, finding: AuditFinding, package_bucket: str) -> tuple[int, int, int, int, str]:
    return (
        0 if finding_root_cause_bucket(finding=finding) == package_bucket else 1,
        0 if bool(finding.metadata.get("truth_enforcement")) else 1,
        _detail_penalty(finding=finding),
        severity_rank(severity=finding.severity),
        finding.title,
    )


def _detail_penalty(*, finding: AuditFinding) -> int:
    return 1 if finding.category in SUPPORTING_DETAIL_CATEGORIES else 0


def _string_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    return []
