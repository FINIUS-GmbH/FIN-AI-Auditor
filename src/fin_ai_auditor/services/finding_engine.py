from __future__ import annotations

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
from fin_ai_auditor.services.pipeline_models import ExtractedClaimRecord


def derive_truths(
    *,
    inherited_truths: list[TruthLedgerEntry],
    claim_records: list[ExtractedClaimRecord],
) -> list[TruthLedgerEntry]:
    truths = [truth.model_copy(deep=True) for truth in inherited_truths]
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
    findings: list[AuditFinding] = []
    active_truths = [truth for truth in inherited_truths if truth.truth_status == "active"]
    grouped = _group_claims(claim_records=claim_records)
    for truth in active_truths:
        relevant_records = grouped.get(truth.subject_key, [])
        if not relevant_records:
            continue
        claim_values = {normalize_claim_value(record.claim.normalized_value) for record in relevant_records}
        truth_value = normalize_claim_value(truth.normalized_value)
        if truth_value and semantic_values_conflict(
            subject_key=truth.subject_key,
            left_values={truth_value},
            right_values=claim_values,
            predicate=truth.predicate,
        ):
            findings.append(
                _build_finding(
                    subject_key=truth.subject_key,
                    category="policy_conflict",
                    severity="high",
                    title="Gespeicherte Wahrheit kollidiert mit der aktuellen Evidenz",
                    summary=(
                        "Eine bereits lokal gespeicherte User- oder System-Wahrheit passt nicht mehr "
                        "zu den aktuell gelesenen Claims."
                    ),
                    recommendation=(
                        "Die Wahrheit pruefen, neu bestaetigen oder bewusst supersedieren und danach "
                        "die betroffenen Pakete neu bewerten."
                    ),
                    records=relevant_records[:3],
                    delta_scope_affected=_is_subject_impacted(
                        subject_key=truth.subject_key,
                        impacted_scope_keys=impacted_scope_keys,
                    ),
                )
            )
    return findings


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
    return AuditFinding(
        severity=severity,
        category=category,
        title=title,
        summary=summary,
        recommendation=recommendation,
        canonical_key=subject_key,
        locations=[record.evidence.location for record in records if record.evidence.location is not None],
        metadata={
            "object_key": subject_key,
            "generated_by": "deterministic_finding_engine",
            "delta_scope_key": package_scope_key(subject_key),
            "delta_scope_affected": delta_scope_affected,
            "semantic_signatures": semantic_signatures,
        },
    )


def _build_links(*, findings: list[AuditFinding]) -> list[AuditFindingLink]:
    links: list[AuditFindingLink] = []
    for index, left in enumerate(findings):
        left_scope = _base_scope(left)
        for right in findings[index + 1 :]:
            if left_scope != _base_scope(right):
                continue
            relation = "gap_hint" if left.category == "missing_definition" or right.category == "missing_definition" else "supports"
            links.append(
                AuditFindingLink(
                    from_finding_id=left.finding_id,
                    to_finding_id=right.finding_id,
                    relation_type=relation,
                    rationale="Beide Findings beziehen sich auf denselben Scope-Cluster und sollten gemeinsam bewertet werden.",
                    confidence=0.74,
                    metadata={"scope": left_scope},
                )
            )
    return links


def _base_scope(finding: AuditFinding) -> str:
    canonical = str(finding.canonical_key or finding.title)
    return package_scope_key(canonical)


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
