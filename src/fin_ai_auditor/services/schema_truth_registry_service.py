from __future__ import annotations

from collections import defaultdict
from typing import Literal

from fin_ai_auditor.domain.models import (
    AuditClaimEntry,
    SchemaTruthEntry,
    SchemaTruthSourceKind,
    TruthLedgerEntry,
)


_STATUS_PRIORITY: dict[str, int] = {
    "rejected_target": 5,
    "confirmed_ssot": 4,
    "provisional_target": 3,
    "observed_only": 2,
    "code_only_inference": 1,
}


def build_schema_truth_registry(
    *,
    claims: list[AuditClaimEntry],
    truths: list[TruthLedgerEntry],
) -> list[SchemaTruthEntry]:
    grouped_entries: dict[str, list[SchemaTruthEntry]] = defaultdict(list)

    for claim in claims:
        metadata = claim.metadata or {}
        target_buckets = {
            "confirmed_ssot": _string_list(metadata.get("schema_validated_targets")),
            "observed_only": _string_list(metadata.get("schema_observed_only_targets")),
            "code_only_inference": _string_list(metadata.get("schema_unconfirmed_targets")),
        }
        status_hint = str(metadata.get("schema_validation_status") or "").strip()
        if status_hint in {"ssot_confirmed", "observed_only", "unconfirmed"}:
            normalized_status = {
                "ssot_confirmed": "confirmed_ssot",
                "observed_only": "observed_only",
                "unconfirmed": "code_only_inference",
            }[status_hint]
            hint_targets = _string_list(metadata.get("persistence_schema_targets"))
            if not target_buckets[normalized_status] and hint_targets:
                target_buckets[normalized_status] = hint_targets

        for status, targets in target_buckets.items():
            for target in targets:
                grouped_entries[target].append(
                    SchemaTruthEntry(
                        schema_key=target,
                        schema_kind=_schema_kind(target),
                        target_label=target,
                        status=status,  # type: ignore[arg-type]
                        source_kind=_source_kind_from_claim(claim=claim),
                        source_authority=claim.source_authority,
                        source_ids=[claim.source_id],
                        evidence_claim_ids=[claim.claim_id],
                        metadata={
                            "derived_from_claim_subject": claim.subject_key,
                            "derived_from_predicate": claim.predicate,
                            "claim_scope_key": claim.scope_key,
                        },
                    )
                )

    for truth in truths:
        metadata = truth.metadata or {}
        schema_key = str(metadata.get("schema_key") or metadata.get("schema_target") or "").strip()
        if not schema_key:
            continue
        schema_status = str(metadata.get("schema_truth_status") or "").strip()
        if schema_status not in _STATUS_PRIORITY:
            continue
        grouped_entries[schema_key].append(
            SchemaTruthEntry(
                schema_key=schema_key,
                schema_kind=_schema_kind(schema_key),
                target_label=schema_key,
                status=schema_status,  # type: ignore[arg-type]
                source_kind="truth_ledger",
                source_authority=truth.source_authority,
                source_ids=[truth.canonical_key],
                evidence_claim_ids=[],
                related_truth_ids=[truth.truth_id],
                metadata={
                    "derived_from_truth": truth.canonical_key,
                    "truth_scope_key": truth.scope_key,
                },
            )
        )

    registry: list[SchemaTruthEntry] = []
    for schema_key, entries in grouped_entries.items():
        chosen = sorted(
            entries,
            key=lambda entry: (
                -_STATUS_PRIORITY.get(entry.status, 0),
                -len(entry.related_truth_ids),
                -len(entry.evidence_claim_ids),
                entry.target_label,
            ),
        )[0]
        registry.append(
            chosen.model_copy(
                update={
                    "source_ids": _dedupe_preserve_order(
                        item for entry in entries for item in entry.source_ids
                    ),
                    "evidence_claim_ids": _dedupe_preserve_order(
                        item for entry in entries for item in entry.evidence_claim_ids
                    ),
                    "related_truth_ids": _dedupe_preserve_order(
                        item for entry in entries for item in entry.related_truth_ids
                    ),
                    "metadata": {
                        **chosen.metadata,
                        "status_candidates": _dedupe_preserve_order(entry.status for entry in entries),
                        "source_authorities": _dedupe_preserve_order(entry.source_authority for entry in entries),
                    },
                }
            )
        )

    return sorted(registry, key=lambda entry: (entry.schema_key, -_STATUS_PRIORITY.get(entry.status, 0)))


def _source_kind_from_claim(*, claim: AuditClaimEntry) -> SchemaTruthSourceKind:
    if claim.source_type == "metamodel":
        return "metamodel"
    if claim.source_type in {"confluence_page", "local_doc"}:
        return "documentation"
    if claim.source_authority == "runtime_observation":
        return "runtime_observation"
    return "implementation_inference"


def _schema_kind(target: str) -> Literal["node", "relationship", "property", "unknown"]:
    normalized = str(target or "").strip()
    if normalized.startswith("Node:"):
        return "node"
    if normalized.startswith("Relationship:"):
        return "relationship"
    if "." in normalized:
        return "property"
    return "unknown"


def _string_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    return [text] if text else []


def _dedupe_preserve_order(values: list[str] | tuple[str, ...] | object) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values if isinstance(values, (list, tuple)) else [values]:
        value = str(raw or "").strip()
        if not value or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    return normalized
