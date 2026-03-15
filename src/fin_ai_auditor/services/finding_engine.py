from __future__ import annotations

import logging
from collections import defaultdict
from itertools import combinations
from typing import Iterable

from fin_ai_auditor.domain.models import AuditFinding, AuditFindingLink, TruthLedgerEntry
from fin_ai_auditor.services.claim_semantics import (
    normalize_claim_value,
    package_scope_key,
    semantic_signature_for_claim,
    semantic_values_conflict,
)
from fin_ai_auditor.services.finding_prioritization import (
    SUPPORTING_DETAIL_CATEGORIES,
    finding_root_cause_bucket,
    is_core_root_cause_bucket,
    severity_rank,
)
from fin_ai_auditor.services.pipeline_models import ExtractedClaimRecord

logger = logging.getLogger(__name__)


def derive_truths(
    *,
    inherited_truths: list[TruthLedgerEntry],
    claim_records: list[ExtractedClaimRecord],
) -> list[TruthLedgerEntry]:
    truths = [_normalize_truth_for_storage(truth=truth) for truth in inherited_truths]
    if any(truth.canonical_key == "BSM.process.phase_source" and truth.truth_status == "active" for truth in truths):
        return truths
    if any(record.claim.subject_key == "BSM.process" and record.claim.predicate == "phase_count" for record in claim_records):
        truths.append(
            TruthLedgerEntry(
                canonical_key="BSM.process.phase_source",
                subject_kind="process",
                subject_key="BSM.process",
                predicate="phase_source",
                normalized_value="metamodel_current_dump",
                scope_kind="global",
                scope_key="FINAI",
                source_kind="system_inference",
                metadata={"origin": "pipeline"},
            )
        )
    return truths


def _normalize_truth_for_storage(*, truth: TruthLedgerEntry) -> TruthLedgerEntry:
    metadata = {**truth.metadata}
    metadata.pop("truth_delta_retrigger", None)
    return truth.model_copy(deep=True, update={"metadata": metadata})


def generate_findings(
    *,
    claim_records: list[ExtractedClaimRecord],
    inherited_truths: list[TruthLedgerEntry],
    impacted_scope_keys: set[str] | None = None,
) -> tuple[list[AuditFinding], list[AuditFindingLink]]:
    """Spec-driven finding generation.

    Priority order:
    1. Documentation ↔ Documentation contradictions (Confluence pages, local docs)
    2. Documentation contradictions validated against Metamodel
    3. Documentation spec vs Code implementation (doc is SSOT → code must follow)
    4. Truth ledger conflicts

    NOT a finding:
    - Code exists without documentation coverage (code follows docs, not vice versa)
    - Metamodel exists without runtime representation (low-priority gap hint only)
    """
    findings: list[AuditFinding] = []
    groups = _group_claims(claim_records=claim_records)
    effective_impacted_scope_keys = set(impacted_scope_keys or set())
    logger.info("finding_generation_start", extra={"event_name": "finding_generation_start", "event_payload": {"claim_groups": len(groups), "total_claims": len(claim_records), "impacted_scopes": len(effective_impacted_scope_keys)}})

    for subject_key, records in groups.items():
        code_records = [record for record in records if record.claim.source_type == "github_file"]
        doc_records = [
            record for record in records if record.claim.source_type in {"confluence_page", "local_doc"}
        ]
        confluence_records = [record for record in records if record.claim.source_type == "confluence_page"]
        local_doc_records = [record for record in records if record.claim.source_type == "local_doc"]
        metamodel_records = [record for record in records if record.claim.source_type == "metamodel"]
        delta_scope_affected = _is_subject_impacted(subject_key=subject_key, impacted_scope_keys=effective_impacted_scope_keys)

        # ── PHASE 1: Doc ↔ Doc contradictions (PRIMARY findings) ──────────
        # Confluence pages contradicting each other
        if len(confluence_records) > 1 and _values_internal_conflict(confluence_records, subject_key=subject_key):
            findings.append(
                _build_finding(
                    subject_key=subject_key,
                    category="contradiction",
                    severity="high",
                    title="Confluence-Seiten widersprechen sich",
                    summary=(
                        "Mindestens zwei Confluence-Seiten enthalten fuer denselben Scope "
                        "widerspruechliche Aussagen. Die Dokumentation muss konsolidiert werden."
                    ),
                    recommendation=(
                        "Die betroffenen Confluence-Seiten vergleichen, die kanonische Aussage "
                        "fuer diesen Scope festlegen und die abweichenden Seiten aktualisieren."
                    ),
                    records=confluence_records[:3],
                    delta_scope_affected=delta_scope_affected,
                )
            )

        # Confluence vs local docs contradictions
        if confluence_records and local_doc_records and _values_conflict(
            confluence_records, local_doc_records, subject_key=subject_key
        ):
            findings.append(
                _build_finding(
                    subject_key=subject_key,
                    category="contradiction",
                    severity="high",
                    title="Confluence-Doku und lokale Arbeitsdokumente widersprechen sich",
                    summary=(
                        "Fuer denselben Scope sagen Confluence-Seiten und lokale _docs-Dokumente "
                        "unterschiedliche Dinge. Eine Quelle muss korrigiert werden."
                    ),
                    recommendation=(
                        "Confluence als fuehrende Quelle festlegen und die lokale Doku anpassen, "
                        "oder umgekehrt — dann die Confluence-Seite aktualisieren."
                    ),
                    records=[*confluence_records[:2], *local_doc_records[:2]],
                    delta_scope_affected=delta_scope_affected,
                )
            )

        # Local docs contradicting each other
        if len(local_doc_records) > 1 and _values_internal_conflict(local_doc_records, subject_key=subject_key):
            findings.append(
                _build_finding(
                    subject_key=subject_key,
                    category="contradiction",
                    severity="medium",
                    title="Lokale Arbeitsdokumente widersprechen sich untereinander",
                    summary=(
                        "Mindestens zwei lokale Dokumente enthalten fuer denselben Scope "
                        "widerspruechliche Aussagen."
                    ),
                    recommendation=(
                        "Lokale Dokumentquellen konsolidieren und eine eindeutige SSOT-Aussage "
                        "fuer diesen Scope festlegen."
                    ),
                    records=local_doc_records[:3],
                    delta_scope_affected=delta_scope_affected,
                )
            )

        # ── PHASE 2: Doc contradictions vs Metamodel ──────────────────────
        if metamodel_records and doc_records and _values_conflict(
            metamodel_records, doc_records, subject_key=subject_key
        ):
            findings.append(
                _build_finding(
                    subject_key=subject_key,
                    category="contradiction",
                    severity="high",
                    title="Dokumentation weicht vom Metamodell ab",
                    summary=(
                        "Die Dokumentation beschreibt fuer diesen Scope etwas anderes als das "
                        "aktuelle Metamodell vorgibt. Die Dokumentation muss an das Metamodell "
                        "angepasst werden oder das Metamodell aktualisiert werden."
                    ),
                    recommendation=(
                        "Metamodell-Vorgaben fuer diesen Scope pruefen und die Dokumentation "
                        "entsprechend anpassen. Falls das Metamodell veraltet ist, dort zuerst aendern."
                    ),
                    records=[*metamodel_records[:2], *doc_records[:2]],
                    delta_scope_affected=delta_scope_affected,
                )
            )

        # ── PHASE 3: Doc spec vs Code implementation ──────────────────────
        # Only raise a finding if documentation SPECIFIES something and code CONTRADICTS it.
        # Code without doc coverage is NOT a finding (spec-driven: docs lead).
        if doc_records and code_records and _values_conflict(
            doc_records, code_records, subject_key=subject_key
        ):
            if _is_policy_scope(subject_key=subject_key):
                findings.append(
                    _build_finding(
                        subject_key=subject_key,
                        category="policy_conflict",
                        severity="high",
                        title="Implementierte Policy widerspricht der Dokumentation",
                        summary=(
                            "Die dokumentierte Policy fuer diesen Scope kollidiert mit der aktuell "
                            "implementierten Policy im Code. Der Zielzustand muss zuerst fachlich "
                            "festgezogen und danach in allen Quellen konsistent gespiegelt werden."
                        ),
                        recommendation=(
                            "Die dokumentierte Policy als Zielbild gegen Code, Guardrails und Metamodell pruefen. "
                            "Danach muessen alle abweichenden Code- und Doku-Stellen auf denselben Policy-Zustand gebracht werden."
                        ),
                        records=[*doc_records[:2], *code_records[:2]],
                        delta_scope_affected=delta_scope_affected,
                    )
                )
            else:
                findings.append(
                    _build_finding(
                        subject_key=subject_key,
                        category="implementation_drift",
                        severity="high",
                        title="Code-Implementierung weicht von der Dokumentation ab",
                        summary=(
                            "Die Dokumentation spezifiziert fuer diesen Scope ein bestimmtes Verhalten, "
                            "das im Code anders implementiert ist. Die Dokumentation ist die fuehrende "
                            "Quelle — der Code muss angepasst werden."
                        ),
                        recommendation=(
                            "Dokumentations-Spezifikation fuer diesen Scope pruefen. Wenn die Doku "
                            "korrekt ist, Code anpassen. Wenn der Code korrekt ist, Doku aktualisieren."
                        ),
                        records=[*doc_records[:2], *code_records[:2]],
                        delta_scope_affected=delta_scope_affected,
                    )
                )

        # Doc specifies something that code doesn't implement at all
        if subject_key.endswith((".write_path", ".read_path")):
            if doc_records and not code_records:
                findings.append(
                    _build_finding(
                        subject_key=subject_key,
                        category="implementation_drift",
                        severity="medium",
                        title="Dokumentierte Operation ist im Code nicht implementiert",
                        summary=(
                            "Die Doku beschreibt fuer diesen Scope einen Read-/Write-Pfad, "
                            "der im aktuellen Code nicht gefunden wurde. "
                            "Entweder fehlt die Implementierung oder die Doku ist veraltet."
                        ),
                        recommendation=(
                            "Pruefen ob die Implementierung fehlt (dann Code nachziehen) "
                            "oder ob die Doku veraltet ist (dann Doku korrigieren)."
                        ),
                        records=doc_records[:2],
                        delta_scope_affected=delta_scope_affected,
                    )
                )

        # ── Lifecycle: only doc-internal contradictions matter ────────────
        if _is_lifecycle_scope(subject_key=subject_key):
            if len(doc_records) > 1 and _values_internal_conflict(doc_records, subject_key=subject_key):
                findings.append(
                    _build_finding(
                        subject_key=subject_key,
                        category="contradiction",
                        severity="medium",
                        title="Lifecycle-Beschreibung ist zwischen Dokumentquellen widerspruechlich",
                        summary=(
                            "Mehrere Dokumentquellen beschreiben Lifecycle, Status oder Review-Regeln "
                            "fuer denselben Scope unterschiedlich."
                        ),
                        recommendation="Dokumentquellen konsolidieren und eine kanonische Lifecycle-Aussage festziehen.",
                        records=doc_records[:3],
                        delta_scope_affected=delta_scope_affected,
                    )
                )

        # ── Policy: doc contradictions and doc-vs-code drift ──────────────
        if _is_policy_scope(subject_key=subject_key):
            if len(doc_records) > 1 and _values_internal_conflict(doc_records, subject_key=subject_key):
                findings.append(
                    _build_finding(
                        subject_key=subject_key,
                        category="policy_conflict",
                        severity="high",
                        title="Policy-Dokumentation ist in sich widerspruechlich",
                        summary=(
                            "Mehrere Dokumentquellen beschreiben Policy-, Approval- oder Guardrail-Regeln "
                            "fuer denselben Scope unterschiedlich."
                        ),
                        recommendation=(
                            "Policy-Dokumentation konsolidieren und eine eindeutige Policy-Aussage festziehen."
                        ),
                        records=doc_records[:3],
                        delta_scope_affected=delta_scope_affected,
                    )
                )

        # ── Process/BSM scope: primarily doc-vs-metamodel ─────────────────
        if _is_process_scope(subject_key=subject_key):
            # Metamodel vs docs already handled in Phase 2 above.
            # Only add: doc-internal process contradictions
            runtime_doc_records = [r for r in records if r.claim.source_type in {"confluence_page", "local_doc"}]
            if len(runtime_doc_records) > 1 and _values_internal_conflict(runtime_doc_records, subject_key=subject_key):
                findings.append(
                    _build_finding(
                        subject_key=subject_key,
                        category="contradiction",
                        severity="medium",
                        title="Prozessbeschreibung ist zwischen Dokumentquellen widerspruechlich",
                        summary=(
                            "Dokumentierte Prozesssemantik kollidiert fuer diesen Scope "
                            "bereits innerhalb der Dokumentquellen."
                        ),
                        recommendation=(
                            "Die betroffenen Doku-Claims fuer diesen Prozessscope konsolidieren und "
                            "danach erst gegen das Metamodell freigeben."
                        ),
                        records=runtime_doc_records[:3],
                        delta_scope_affected=delta_scope_affected,
                    )
                )
            # Metamodel without ANY doc coverage: low-severity gap hint
            if metamodel_records and not doc_records and not code_records:
                if _is_question_process_scope(subject_key=subject_key) and not _has_related_runtime_process_context(
                    groups=groups,
                    subject_key=subject_key,
                ):
                    continue
                findings.append(
                    _build_finding(
                        subject_key=subject_key,
                        category="traceability_gap",
                        severity="low",
                        title="Metamodell-Scope hat keine Dokumentationsabdeckung",
                        summary=(
                            "Das Metamodell definiert fuer diesen Scope eine Struktur, "
                            "die in der Dokumentation nicht beschrieben wird."
                        ),
                        recommendation=(
                            "Pruefen ob dieser Metamodell-Scope in der Dokumentation erwähnt werden sollte "
                            "oder ob er nur interne Infrastruktur betrifft."
                        ),
                        records=metamodel_records[:2],
                        delta_scope_affected=delta_scope_affected,
                    )
                )

    # ── Truth ledger conflicts ────────────────────────────────────────────
    truth_findings = _find_truth_conflicts(
        claim_records=claim_records,
        inherited_truths=inherited_truths,
        impacted_scope_keys=effective_impacted_scope_keys,
    )
    findings.extend(truth_findings)

    links = _build_links(findings=findings)
    from collections import Counter
    cat_counts = dict(Counter(f.category for f in findings))
    logger.info("finding_generation_done", extra={"event_name": "finding_generation_done", "event_payload": {"total_findings": len(findings), "links": len(links), "by_category": cat_counts}})
    return findings, links


def build_finding_links(*, findings: list[AuditFinding]) -> list[AuditFindingLink]:
    return _build_links(findings=findings)


def _group_claims(*, claim_records: list[ExtractedClaimRecord]) -> dict[str, list[ExtractedClaimRecord]]:
    grouped: dict[str, list[ExtractedClaimRecord]] = defaultdict(list)
    for record in claim_records:
        grouped[record.claim.subject_key].append(record)
    return dict(grouped)


def _values_conflict(
    left: Iterable[ExtractedClaimRecord],
    right: Iterable[ExtractedClaimRecord],
    *,
    subject_key: str,
) -> bool:
    left_records = list(left)
    right_records = list(right)
    left_values = {normalize_claim_value(record.claim.normalized_value) for record in left_records}
    right_values = {normalize_claim_value(record.claim.normalized_value) for record in right_records}
    if not left_values or not right_values:
        return False
    representative_predicate = _representative_predicate(records=[*left_records, *right_records])
    return semantic_values_conflict(
        subject_key=subject_key,
        left_values=left_values,
        right_values=right_values,
        predicate=representative_predicate,
    )


def _values_internal_conflict(records: list[ExtractedClaimRecord], *, subject_key: str) -> bool:
    if len(records) <= 1:
        return False
    for left_record, right_record in combinations(records, 2):
        left_value = normalize_claim_value(left_record.claim.normalized_value)
        right_value = normalize_claim_value(right_record.claim.normalized_value)
        if left_value == right_value:
            continue
        if semantic_values_conflict(
            subject_key=subject_key,
            predicate=left_record.claim.predicate,
            left_values={left_value},
            right_values={right_value},
        ):
            return True
    return False


def _has_metamodel_claims(*, groups: dict[str, list[ExtractedClaimRecord]]) -> bool:
    return any(
        any(record.claim.source_type == "metamodel" for record in records)
        for records in groups.values()
    )


def _first_metamodel_records(*, groups: dict[str, list[ExtractedClaimRecord]]) -> list[ExtractedClaimRecord]:
    out: list[ExtractedClaimRecord] = []
    for records in groups.values():
        for record in records:
            if record.claim.source_type == "metamodel":
                out.append(record)
            if len(out) >= 2:
                return out
    return out


def _find_truth_conflicts(
    *,
    claim_records: list[ExtractedClaimRecord],
    inherited_truths: list[TruthLedgerEntry],
    impacted_scope_keys: set[str],
) -> list[AuditFinding]:
    """Enforce confirmed truths across ALL sources.

    Core principle: A confirmed truth MUST be reflected in EVERY document
    and code fragment.  Each source that contradicts a confirmed truth
    gets its OWN finding so it can be individually tracked and resolved.
    """
    findings: list[AuditFinding] = []
    active_truths = [
        truth
        for truth in inherited_truths
        if truth.truth_status == "active" and _is_explicit_truth(truth=truth)
    ]
    if not active_truths:
        return findings

    grouped = _group_claims(claim_records=claim_records)

    for truth in active_truths:
        truth_value = normalize_claim_value(truth.normalized_value)
        if not truth_value:
            continue

        # Find ALL records that match by subject_key OR by subject_key prefix
        relevant_records: list[ExtractedClaimRecord] = []
        for key, records in grouped.items():
            if key == truth.subject_key or key.startswith(f"{truth.subject_key}."):
                relevant_records.extend(records)

        if not relevant_records:
            continue

        # Group conflicting records BY SOURCE — each source gets its own finding
        conflicting_by_source: dict[str, list[ExtractedClaimRecord]] = {}
        for record in relevant_records:
            claim_value = normalize_claim_value(record.claim.normalized_value)
            if not claim_value:
                continue
            if semantic_values_conflict(
                subject_key=truth.subject_key,
                left_values={truth_value},
                right_values={claim_value},
                predicate=truth.predicate,
            ):
                # Key by source type + source id for per-source findings
                source_key = f"{record.claim.source_type}:{record.evidence.location.source_id or ''}"
                if source_key not in conflicting_by_source:
                    conflicting_by_source[source_key] = []
                conflicting_by_source[source_key].append(record)

        # Generate ONE finding per conflicting source
        for source_key, conflict_records in conflicting_by_source.items():
            source_type = conflict_records[0].claim.source_type
            source_label = {
                "github_file": "Code",
                "confluence_page": "Confluence",
                "local_doc": "Lokales Dokument",
                "metamodel": "Metamodell",
            }.get(source_type, source_type)
            source_path = conflict_records[0].evidence.location.path_hint or conflict_records[0].evidence.location.source_id or ""
            source_short = source_path.split("/")[-1] if "/" in source_path else source_path

            wrong_values = sorted({r.claim.normalized_value for r in conflict_records})

            findings.append(
                _build_finding(
                    subject_key=truth.subject_key,
                    category="policy_conflict",
                    severity="critical",  # Confirmed truths are critical
                    title=f"Explizit bestaetigte Wahrheit nicht umgesetzt: {source_label} — {source_short}",
                    summary=(
                        f"Die bestaetigte Wahrheit '{truth.subject_key}' "
                        f"definiert den Wert '{truth.normalized_value}', "
                        f"aber {source_label} '{source_short}' verwendet abweichende Werte: "
                        f"{', '.join(wrong_values[:5])}.\n\n"
                        f"Bestaetigte Wahrheit: {truth.predicate} = {truth.normalized_value}\n"
                        f"Quelle der Wahrheit: {truth.source_kind}"
                    ),
                    recommendation=(
                        f"{source_label} '{source_short}' muss angepasst werden, "
                        f"um die bestaetigte Wahrheit '{truth.normalized_value}' fuer "
                        f"'{truth.subject_key}/{truth.predicate}' korrekt widerzuspiegeln. "
                        f"Nach der Anpassung muss ein Delta-Abgleich erfolgen."
                    ),
                    records=conflict_records[:5],
                    delta_scope_affected=True,  # Always delta-relevant for confirmed truths
                    extra_metadata={
                        "truth_enforcement": True,
                        "truth_id": truth.truth_id,
                        "confirmed_value": truth.normalized_value,
                        "wrong_values": wrong_values[:10],
                        "source_type": source_type,
                        "requires_delta_recalculation": True,
                    },
                )
            )

    return findings


def _is_explicit_truth(*, truth: TruthLedgerEntry) -> bool:
    return truth.source_kind in {"user_specification", "user_acceptance"}


def _build_finding(
    *,
    subject_key: str,
    category: AuditFinding.__annotations__["category"],
    severity: AuditFinding.__annotations__["severity"],
    title: str,
    summary: str,
    recommendation: str,
    records: list[ExtractedClaimRecord],
    delta_scope_affected: bool,
    extra_metadata: dict[str, object] | None = None,
) -> AuditFinding:
    semantic_signatures = sorted(
        {
            "|".join(
                semantic_signature_for_claim(
                    subject_key=subject_key,
                    predicate=record.claim.predicate,
                    value=record.claim.normalized_value,
                )
            )
            for record in records
            if semantic_signature_for_claim(
                subject_key=subject_key,
                predicate=record.claim.predicate,
                value=record.claim.normalized_value,
            )
        }
    )
    # Build concrete evidence quotes grouped by source type
    evidence_quotes = _build_evidence_quotes(records)
    enriched_summary = f"{summary}\n\n{evidence_quotes}" if evidence_quotes else summary
    metadata: dict[str, object] = {
        "object_key": subject_key,
        "generated_by": "deterministic_finding_engine",
        "delta_scope_key": package_scope_key(subject_key),
        "delta_scope_affected": delta_scope_affected,
        "semantic_signatures": semantic_signatures,
    }
    if extra_metadata:
        metadata.update(extra_metadata)
    return AuditFinding(
        severity=severity,
        category=category,
        title=title,
        summary=enriched_summary,
        recommendation=recommendation,
        canonical_key=subject_key,
        locations=[record.evidence.location for record in records if record.evidence.location is not None],
        metadata=metadata,
    )


_SOURCE_LABEL: dict[str, str] = {
    "github_file": "Code",
    "confluence_page": "Confluence",
    "local_doc": "Lokales Dokument",
    "metamodel": "Metamodell",
}


def _build_evidence_quotes(records: list[ExtractedClaimRecord]) -> str:
    """Build a concise evidence summary quoting what each source actually says."""
    lines: list[str] = []
    for record in records[:4]:
        src_label = _SOURCE_LABEL.get(record.claim.source_type, record.claim.source_type)
        matched = str(record.evidence.matched_text or "").strip()
        title = record.evidence.location.title if record.evidence.location else ""
        path = record.evidence.location.path_hint or "" if record.evidence.location else ""
        loc_hint = path or title
        # Truncate long quotes
        if len(matched) > 120:
            matched = matched[:117] + "..."
        if matched and loc_hint:
            lines.append(f"{src_label} ({loc_hint}): \u00ab{matched}\u00bb")
        elif matched:
            lines.append(f"{src_label}: \u00ab{matched}\u00bb")
    return "\n".join(lines)


def _build_links(*, findings: list[AuditFinding]) -> list[AuditFindingLink]:
    links: list[AuditFindingLink] = []
    for index, left in enumerate(findings):
        for right in findings[index + 1 :]:
            classified = _classify_finding_link(left=left, right=right)
            if classified is None:
                continue
            relation, rationale, confidence, source, target = classified
            links.append(
                AuditFindingLink(
                    from_finding_id=source.finding_id,
                    to_finding_id=target.finding_id,
                    relation_type=relation,
                    rationale=rationale,
                    confidence=confidence,
                    metadata={
                        "scope_overlap": sorted(_finding_scope_keys(source).intersection(_finding_scope_keys(target))),
                        "group_key": str(source.metadata.get("causal_group_key") or target.metadata.get("causal_group_key") or ""),
                    },
                )
            )
    return links


def _classify_finding_link(
    *,
    left: AuditFinding,
    right: AuditFinding,
) -> tuple[str, str, float, AuditFinding, AuditFinding] | None:
    left_scopes = _finding_scope_keys(left)
    right_scopes = _finding_scope_keys(right)
    shared_scopes = left_scopes.intersection(right_scopes)
    left_group = str(left.metadata.get("causal_group_key") or "").strip()
    right_group = str(right.metadata.get("causal_group_key") or "").strip()
    shared_truths = set(_string_list(left.metadata.get("causal_related_truth_ids"))).intersection(
        _string_list(right.metadata.get("causal_related_truth_ids"))
    )

    if (
        left.canonical_key
        and right.canonical_key
        and left.canonical_key == right.canonical_key
        and finding_root_cause_bucket(finding=left) == finding_root_cause_bucket(finding=right)
    ):
        return (
            "duplicates",
            "Beide Findings verweisen auf denselben kanonischen Kernkonflikt und sollten zusammengelegt oder gemeinsam bewertet werden.",
            0.92,
            left,
            right,
        )

    if left_group and left_group == right_group:
        detail_left = _is_supporting_detail_finding(finding=left)
        detail_right = _is_supporting_detail_finding(finding=right)
        if detail_left != detail_right:
            detail = left if detail_left else right
            root = right if detail_left else left
            return (
                "depends_on",
                "Das Detail-Finding haengt kausal am selben Root-Cause-Cluster und sollte erst nach Klaerung des Kernproblems abgeschlossen werden.",
                0.86,
                detail,
                root,
            )
        if finding_root_cause_bucket(finding=left) != finding_root_cause_bucket(finding=right):
            source, target = _ordered_by_root_cause(left=left, right=right)
            return (
                "depends_on",
                "Beide Findings teilen sich denselben kausalen Cluster, betreffen aber unterschiedliche Root-Cause-Ebenen in derselben Wirkungskette.",
                0.82,
                source,
                target,
            )
        return (
            "supports",
            "Beide Findings werden durch denselben kausalen Cluster getragen und verdichten gemeinsam dasselbe Kernproblem.",
            0.84,
            left,
            right,
        )

    if shared_truths:
        source, target = _ordered_by_root_cause(left=left, right=right)
        return (
            "depends_on",
            "Beide Findings haengen an derselben bestaetigten Wahrheit und sollten entlang derselben Delta-Kette bewertet werden.",
            0.8,
            source,
            target,
        )

    if shared_scopes:
        relation = "gap_hint" if left.category == "missing_definition" or right.category == "missing_definition" else "supports"
        return (
            relation,
            "Beide Findings ueberlappen im selben fachlichen Scope und liefern gemeinsame Evidenz fuer denselben Bereich.",
            0.72,
            left,
            right,
        )
    return None


def _base_scope(finding: AuditFinding) -> str:
    canonical = str(finding.canonical_key or finding.title)
    return package_scope_key(canonical)


def _finding_scope_keys(finding: AuditFinding) -> set[str]:
    scope_keys = {
        package_scope_key(str(scope_key))
        for scope_key in _string_list(finding.metadata.get("causal_scope_keys"))
        if str(scope_key).strip()
    }
    base_scope = _base_scope(finding)
    if base_scope:
        scope_keys.add(base_scope)
    return {scope_key for scope_key in scope_keys if str(scope_key or "").strip()}


def _is_supporting_detail_finding(*, finding: AuditFinding) -> bool:
    return finding.category in SUPPORTING_DETAIL_CATEGORIES


def _ordered_by_root_cause(*, left: AuditFinding, right: AuditFinding) -> tuple[AuditFinding, AuditFinding]:
    left_key = (
        0 if bool(left.metadata.get("truth_enforcement")) else 1,
        severity_rank(severity=left.severity),
        0 if is_core_root_cause_bucket(bucket=finding_root_cause_bucket(finding=left)) else 1,
        left.title,
    )
    right_key = (
        0 if bool(right.metadata.get("truth_enforcement")) else 1,
        severity_rank(severity=right.severity),
        0 if is_core_root_cause_bucket(bucket=finding_root_cause_bucket(finding=right)) else 1,
        right.title,
    )
    return (left, right) if left_key <= right_key else (right, left)


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _is_subject_impacted(*, subject_key: str, impacted_scope_keys: set[str]) -> bool:
    if not impacted_scope_keys:
        return False
    scope_key = package_scope_key(subject_key)
    return scope_key in impacted_scope_keys or subject_key in impacted_scope_keys


def _is_lifecycle_scope(*, subject_key: str) -> bool:
    return subject_key.endswith((".lifecycle", ".review_status"))


def _is_policy_scope(*, subject_key: str) -> bool:
    return subject_key.endswith((".policy", ".approval_policy", ".scope_policy"))


def _is_process_scope(*, subject_key: str) -> bool:
    return subject_key == "BSM.process" or subject_key.startswith("BSM.phase.") or subject_key.startswith("BSM.process.question.")


def _representative_predicate(*, records: list[ExtractedClaimRecord]) -> str:
    for record in records:
        predicate = str(record.claim.predicate or "").strip()
        if predicate:
            return predicate
    return ""


def _is_question_process_scope(*, subject_key: str) -> bool:
    return ".question." in subject_key


def _has_related_runtime_process_context(
    *,
    groups: dict[str, list[ExtractedClaimRecord]],
    subject_key: str,
) -> bool:
    related_scope_keys: set[str] = {"BSM.process"}
    if subject_key.startswith("BSM.phase."):
        phase_scope = ".".join(subject_key.split(".")[:3])
        related_scope_keys.add(phase_scope)
    for scope_key in related_scope_keys:
        for record in groups.get(scope_key, []):
            if record.claim.source_type in {"github_file", "confluence_page", "local_doc"}:
                return True
    return False
