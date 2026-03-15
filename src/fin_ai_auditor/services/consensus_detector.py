"""Consensus-based BSM target state analysis.

Core question: "What is the intended BSM process according to the MAJORITY
of documents, and what is missing in docs or code to describe and implement
that target state completely and without contradictions?"

Strategy:
1. For each subject+predicate, determine the MAJORITY VALUE across all sources
2. Flag any source that deviates from consensus as a finding
3. Identify subject/predicate gaps where coverage is incomplete
   (e.g. concept exists in code but not in docs, or vice versa)
4. Generate findings with concrete actions to reach full consistency

Finding categories produced:
- contradiction:          source deviates from consensus
- missing_documentation:  concept exists in code but missing from docs
- implementation_drift:   concept exists in docs but missing from code
- clarification_needed:   no consensus exists (equal split)
"""
from __future__ import annotations

import logging
from collections import Counter, defaultdict
from typing import Sequence

from fin_ai_auditor.domain.models import AuditFinding, AuditLocation, TruthLedgerEntry
from fin_ai_auditor.services.pipeline_models import ExtractedClaimRecord

logger = logging.getLogger(__name__)

# At least this many claims must exist for a subject to be analyzed
_MIN_CLAIMS_FOR_CONSENSUS = 2
# Source types considered "documentation"
_DOC_SOURCES = frozenset({"confluence_page", "local_doc", "metamodel"})
# Source types considered "code/implementation"
_CODE_SOURCES = frozenset({"github_file"})

_SOURCE_LABELS: dict[str, str] = {
    "github_file": "Code",
    "confluence_page": "Confluence",
    "local_doc": "Lokales Dokument",
    "metamodel": "Metamodell",
}


def detect_consensus_deviations(
    *,
    claim_records: list[ExtractedClaimRecord],
    confirmed_truths: Sequence[TruthLedgerEntry] | None = None,
) -> list[AuditFinding]:
    """Analyze all claims to find the consensus target state and flag deviations.

    Confirmed truths OVERRIDE the majority vote — they are the definitive answer.
    Each confirmed truth sharpens the target picture and increases the confidence
    of the overall analysis until a coherent, complete picture emerges.

    Returns findings for:
    - Sources that deviate from the majority-defined target state
    - Concepts that lack full coverage across docs and code
    - Ambiguous concepts where no clear consensus exists
    """
    logger.info(
        "consensus_analysis_start",
        extra={"event_name": "consensus_analysis_start", "event_payload": {"total_claims": len(claim_records)}},
    )

    # Build truth index: (subject_key, predicate) → confirmed value
    # Confirmed truths are the ULTIMATE authority — they override majority vote
    truth_overrides: dict[tuple[str, str], str] = {}
    if confirmed_truths:
        for truth in confirmed_truths:
            if truth.truth_status == "active" and _is_explicit_truth(truth=truth):
                truth_overrides[(truth.subject_key, truth.predicate)] = truth.normalized_value
        logger.info(
            "consensus_truths_loaded",
            extra={"event_name": "consensus_truths_loaded", "event_payload": {"active_truths": len(truth_overrides)}},
        )

    # 1. Group claims by (subject_key, predicate) → list of (value, source_type, record)
    claims_by_aspect: dict[tuple[str, str], list[tuple[str, str, ExtractedClaimRecord]]] = defaultdict(list)
    for record in claim_records:
        key = (record.claim.subject_key, record.claim.predicate)
        value = record.claim.normalized_value.strip()
        if value:
            claims_by_aspect[key].append((value, record.claim.source_type, record))

    findings: list[AuditFinding] = []
    consensus_stats = {
        "total_aspects": 0, "with_consensus": 0, "deviations": 0,
        "coverage_gaps": 0, "ambiguous": 0, "truth_overrides": len(truth_overrides),
        "target_confidence_pct": 0,
    }
    aspects_with_fixed_target = 0

    for (subject_key, predicate), entries in claims_by_aspect.items():
        if len(entries) < _MIN_CLAIMS_FOR_CONSENSUS:
            continue
        consensus_stats["total_aspects"] += 1
        doc_entries = [entry for entry in entries if entry[1] in _DOC_SOURCES]
        target_entries = doc_entries or entries

        # 2. Determine target value:
        #    - If a confirmed truth exists → it IS the target (100% confidence)
        #    - Otherwise → die Dokument-Mehrheit bestimmt den Zielzustand
        truth_key = (subject_key, predicate)
        # Also check prefix matches for truths
        truth_value_override = truth_overrides.get(truth_key)
        if not truth_value_override:
            for tk, tv in truth_overrides.items():
                if subject_key.startswith(f"{tk[0]}.") and predicate == tk[1]:
                    truth_value_override = tv
                    break

        if truth_value_override:
            # Confirmed truth — this is the definitive target
            consensus_display = truth_value_override
            most_common_value = truth_value_override.casefold()
            consensus_ratio = 1.0  # 100% confidence — truth is absolute
            is_truth_fixed = True
            aspects_with_fixed_target += 1
        else:
            # No explicit truth — determine by majority of documents if available.
            value_counter: Counter[str] = Counter()
            for value, _src_type, _rec in target_entries:
                value_counter[value.casefold()] += 1

            total_votes = sum(value_counter.values())
            most_common_value, most_common_count = value_counter.most_common(1)[0]
            consensus_ratio = most_common_count / total_votes
            is_truth_fixed = False

            # Find the original-case version of the consensus value
            consensus_display = next(
                (v for v, _st, _r in target_entries if v.casefold() == most_common_value),
                most_common_value,
            )

        total_votes = len(target_entries)

        # 3. Determine source coverage
        source_types_present = {src_type for _, src_type, _ in entries}
        has_doc = bool(source_types_present & _DOC_SOURCES)
        has_code = bool(source_types_present & _CODE_SOURCES)

        # 4a. Clear target (consensus >50% OR confirmed truth) — flag all deviations
        if consensus_ratio > 0.5:
            consensus_stats["with_consensus"] += 1
            if is_truth_fixed:
                continue

            # Find records that deviate from consensus
            deviating_by_source: dict[str, list[tuple[str, ExtractedClaimRecord]]] = defaultdict(list)
            for value, src_type, record in entries:
                if value.casefold() != most_common_value:
                    source_key = f"{src_type}:{record.evidence.location.source_id or ''}"
                    deviating_by_source[source_key].append((value, record))

            for source_key, devs in deviating_by_source.items():
                consensus_stats["deviations"] += 1
                dev_values = sorted({v for v, _ in devs})
                dev_record = devs[0][1]
                source_label = _SOURCE_LABELS.get(dev_record.claim.source_type, dev_record.claim.source_type)
                source_path = dev_record.evidence.location.path_hint or dev_record.evidence.location.source_id or ""
                source_short = source_path.split("/")[-1] if "/" in source_path else source_path

                # Truth-fixed targets are critical; majority-derived are high
                severity = "critical" if is_truth_fixed else "high"
                target_label = "bestaetigte Wahrheit" if is_truth_fixed else f"Mehrheitskonsens ({int(consensus_ratio * 100)}%)"

                findings.append(AuditFinding(
                    severity=severity,
                    category="contradiction",
                    title=f"Abweichung von {target_label}: {source_label} — {source_short}",
                    summary=(
                        f"Fuer '{subject_key}/{predicate}' definiert {target_label} "
                        f"den Zielwert '{consensus_display}'. "
                        f"{source_label} '{source_short}' verwendet stattdessen: {', '.join(dev_values)}.\n\n"
                        f"Zielzustand: {predicate} = {consensus_display}\n"
                        f"Abweichender Wert: {', '.join(dev_values)}\n"
                        f"Konfidenz: {'100% (bestätigt)' if is_truth_fixed else f'{int(consensus_ratio * 100)}% ({total_votes} Quellen)'}"
                    ),
                    recommendation=(
                        f"{source_label} '{source_short}' muss angepasst werden, "
                        f"um den {'bestaetigten ' if is_truth_fixed else ''}Zielwert '{consensus_display}' "
                        f"fuer '{subject_key}/{predicate}' korrekt widerzuspiegeln."
                    ),
                    canonical_key=f"consensus_deviation:{subject_key}:{predicate}:{source_key}",
                    locations=[r.evidence.location for _, r in devs[:5]],
                    metadata={
                        "generated_by": "consensus_detector",
                        "consensus_value": consensus_display,
                        "consensus_ratio": round(consensus_ratio, 2),
                        "is_truth_fixed": is_truth_fixed,
                        "deviating_values": dev_values,
                        "source_type": dev_record.claim.source_type,
                        "total_sources": total_votes,
                        "requires_delta_recalculation": is_truth_fixed,
                    },
                ))

        # 4b. No clear consensus — ambiguous, needs clarification
        elif len(value_counter) > 1 and consensus_ratio <= 0.5:
            consensus_stats["ambiguous"] += 1
            top_2 = value_counter.most_common(2)
            all_records = [r for _, _, r in entries]

            findings.append(AuditFinding(
                severity="medium",
                category="clarification_needed",
                title=f"Kein Konsens: {subject_key}/{predicate}",
                summary=(
                    f"Fuer '{subject_key}/{predicate}' gibt es keinen eindeutigen Konsens. "
                    f"Die haeufigsten Werte sind: '{top_2[0][0]}' ({top_2[0][1]}x) "
                    f"und '{top_2[1][0]}' ({top_2[1][1]}x) von insgesamt {total_votes} Quellen.\n\n"
                    f"Eine explizite Entscheidung ist erforderlich, welcher Wert der richtige ist."
                ),
                recommendation=(
                    f"Fuer '{subject_key}/{predicate}' muss eine definitive Entscheidung getroffen "
                    f"und als bestaetigte Wahrheit gespeichert werden. "
                    f"Danach wird die Wahrheit automatisch in allen Quellen durchgesetzt."
                ),
                canonical_key=f"consensus_ambiguous:{subject_key}:{predicate}",
                locations=[r.evidence.location for r in all_records[:5]],
                metadata={
                    "generated_by": "consensus_detector",
                    "value_distribution": dict(value_counter.most_common(5)),
                    "total_sources": total_votes,
                },
            ))

        # 5. Coverage gap analysis: concept in code but not docs, or vice versa
        if has_code and not has_doc:
            consensus_stats["coverage_gaps"] += 1
            code_records = [r for _, st, r in entries if st in _CODE_SOURCES]

            findings.append(AuditFinding(
                severity="medium",
                category="missing_documentation",
                title=f"Nur im Code definiert: {subject_key}/{predicate}",
                summary=(
                    f"'{subject_key}/{predicate}' wird im Code definiert "
                    f"(Wert: '{consensus_display}'), aber in keiner Dokumentation "
                    f"(Confluence, Metamodell, lokale Docs) beschrieben. "
                    f"Der BSM-Prozess ist dadurch unvollstaendig dokumentiert."
                ),
                recommendation=(
                    f"Eine Confluence-Seite oder ein Metamodell-Eintrag fuer "
                    f"'{subject_key}/{predicate}' erstellen, der den Wert "
                    f"'{consensus_display}' dokumentiert."
                ),
                canonical_key=f"coverage_gap:code_only:{subject_key}:{predicate}",
                locations=[r.evidence.location for r in code_records[:5]],
                metadata={
                    "generated_by": "consensus_detector",
                    "gap_type": "code_only",
                    "consensus_value": consensus_display,
                },
            ))

        elif has_doc and not has_code:
            consensus_stats["coverage_gaps"] += 1
            doc_records = [r for _, st, r in entries if st in _DOC_SOURCES]

            findings.append(AuditFinding(
                severity="medium",
                category="implementation_drift",
                title=f"Nur in Doku definiert: {subject_key}/{predicate}",
                summary=(
                    f"'{subject_key}/{predicate}' wird in der Dokumentation definiert "
                    f"(Wert: '{consensus_display}'), aber im Code nicht implementiert. "
                    f"Der BSM-Prozess ist dadurch unvollstaendig umgesetzt."
                ),
                recommendation=(
                    f"Die Implementierung fuer '{subject_key}/{predicate}' "
                    f"mit dem dokumentierten Wert '{consensus_display}' im Code ergaenzen."
                ),
                canonical_key=f"coverage_gap:doc_only:{subject_key}:{predicate}",
                locations=[r.evidence.location for r in doc_records[:5]],
                metadata={
                    "generated_by": "consensus_detector",
                    "gap_type": "doc_only",
                    "consensus_value": consensus_display,
                },
            ))

    # Compute target confidence: % of aspects with a definitive target (truth-fixed + clear consensus)
    if consensus_stats["total_aspects"] > 0:
        fixed_and_clear = aspects_with_fixed_target + (consensus_stats["with_consensus"] - aspects_with_fixed_target)
        consensus_stats["target_confidence_pct"] = round(
            (fixed_and_clear / consensus_stats["total_aspects"]) * 100, 1
        )
    else:
        consensus_stats["target_confidence_pct"] = 0

    logger.info(
        "consensus_analysis_done",
        extra={"event_name": "consensus_analysis_done", "event_payload": consensus_stats},
    )
    return findings


def _is_explicit_truth(*, truth: TruthLedgerEntry) -> bool:
    return truth.source_kind in {"user_specification", "user_acceptance"}
