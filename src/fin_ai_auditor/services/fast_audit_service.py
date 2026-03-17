from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from dataclasses import dataclass
from typing import Callable

from pydantic import BaseModel, Field

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import (
    AuditFinding,
    AuditLocation,
    AuditPosition,
    ReviewCard,
    ReviewCardCoverageSummary,
)
from fin_ai_auditor.llm import ChatMessage, GenerationConfig, LiteLLMClient
from fin_ai_auditor.services.bsm_domain_contradiction_detector import detect_bsm_domain_contradictions
from fin_ai_auditor.services.claim_extractor import extract_claim_records
from fin_ai_auditor.services.pipeline_models import CollectedDocument, ExtractedClaimRecord

logger = logging.getLogger(__name__)

FAST_AUDIT_BUDGET_SECONDS = 600
MAX_PRIORITIZED_SECTIONS = 48
MAX_COMPARISON_CANDIDATES = 14
MAX_SECTIONS_PER_DOCUMENT = 6
LLM_BATCH_SIZE = 4

PRIORITY_TERMS = (
    "approval",
    "review",
    "write",
    "read",
    "policy",
    "status",
    "phase",
    "question",
    "required",
    "must",
    "workflow",
    "owner",
    "schnittstelle",
    "prozess",
)
STOPWORDS = {
    "the",
    "and",
    "oder",
    "und",
    "mit",
    "without",
    "from",
    "into",
    "fuer",
    "für",
    "der",
    "die",
    "das",
    "ein",
    "eine",
    "ist",
    "are",
    "for",
    "this",
    "that",
    "page",
    "section",
}
SOURCE_LABELS = {
    "github_file": "Code",
    "confluence_page": "Confluence",
    "metamodel": "Metamodell",
    "local_doc": "Lokale Doku",
    "jira_ticket": "Jira",
}


@dataclass(frozen=True)
class FastAuditSection:
    section_id: str
    source_type: str
    source_id: str
    title: str
    path_hint: str | None
    url: str | None
    heading: str
    body: str
    delta_status: str
    priority_score: int
    topic_tokens: set[str]
    location: AuditLocation
    explicit_focus: bool = False


@dataclass(frozen=True)
class ComparisonCandidate:
    candidate_id: str
    left: FastAuditSection
    right: FastAuditSection | None
    score: int
    comparison_reason: str


class _LLMReviewCardItem(BaseModel):
    candidate_id: str
    should_create_card: bool = False
    title: str = ""
    deviation_type: str = "unclear"
    summary: str = ""
    why_it_matters: str = ""
    recommended_decision: str = ""
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    priority: str = "medium"
    source_a_evidence: list[str] = Field(default_factory=list)
    source_b_evidence: list[str] = Field(default_factory=list)


class _LLMReviewCardBatch(BaseModel):
    items: list[_LLMReviewCardItem] = Field(default_factory=list)


@dataclass(frozen=True)
class FastAuditResult:
    review_cards: list[ReviewCard]
    findings: list[AuditFinding]
    claims: list[object]
    analysis_notes: list[str]
    summary: str
    coverage_summary: ReviewCardCoverageSummary
    budget_limited: bool
    llm_usage: dict


class FastAuditService:
    def __init__(self, *, settings: Settings, allow_remote_calls: bool) -> None:
        self._settings = settings
        self._allow_remote_calls = allow_remote_calls

    async def analyze(
        self,
        *,
        documents: list[CollectedDocument],
        progress_callback: Callable[[str, int, int, str], None] | None = None,
        time_budget_seconds: int = FAST_AUDIT_BUDGET_SECONDS,
    ) -> FastAuditResult:
        started_at = time.monotonic()
        sections = _extract_sections(documents=documents)
        prioritized_sections = sorted(sections, key=lambda section: (-section.priority_score, section.section_id))[
            :MAX_PRIORITIZED_SECTIONS
        ]
        if progress_callback is not None:
            progress_callback(
                "section_profiling",
                len(prioritized_sections),
                max(len(sections), 1),
                f"{len(prioritized_sections)} priorisierte Sektionen fuer den Fast Audit vorbereitet.",
            )

        candidates, candidate_notes, budget_limited = _build_candidates(
            prioritized_sections=prioritized_sections,
            started_at=started_at,
            time_budget_seconds=time_budget_seconds,
        )
        llm_usage: dict = {"by_model": {}, "total_prompt_tokens": 0, "total_completion_tokens": 0, "total_cost_usd": 0.0, "total_cost_eur": 0.0}
        if progress_callback is not None:
            progress_callback(
                "candidate_comparison",
                len(candidates),
                max(len(candidates), 1),
                f"{len(candidates)} priorisierte Vergleichskandidaten werden gespiegelt.",
            )

        review_cards: list[ReviewCard]
        llm_notes: list[str]
        deterministic_candidates = [candidate for candidate in candidates if candidate.right is None]
        llm_candidates = [candidate for candidate in candidates if candidate.right is not None]
        review_cards = [card for card in (_heuristic_card(candidate=candidate) for candidate in deterministic_candidates) if card is not None]
        llm_notes = []
        if llm_candidates and self._allow_remote_calls and _select_fast_audit_slot(settings=self._settings) is not None:
            llm_cards, llm_notes, llm_usage = await self._compare_with_llm(candidates=llm_candidates)
            review_cards.extend(llm_cards)
        else:
            review_cards.extend(
                card for card in (_heuristic_card(candidate=candidate) for candidate in llm_candidates) if card is not None
            )
            llm_notes = [
                "Fast Audit lief ohne Remote-LLM und nutzte heuristische Review-Kartenableitung."
                if candidates
                else "Keine priorisierten Vergleichskandidaten fuer Review-Karten vorhanden."
            ]
        if deterministic_candidates:
            llm_notes.append(
                f"{len(deterministic_candidates)} Kandidaten ohne belastbares Gegenstueck wurden deterministisch als Spiegelungs-/Abdeckungskarten behandelt."
            )

        fast_claim_documents = _select_fast_claim_documents(documents=documents)
        fast_claim_records: list[ExtractedClaimRecord] = []
        bsm_findings: list[AuditFinding] = []
        if fast_claim_documents:
            fast_claim_records = extract_claim_records(documents=fast_claim_documents)
            bsm_findings = detect_bsm_domain_contradictions(claim_records=fast_claim_records)
            if bsm_findings:
                review_cards.extend(_review_card_from_bsm_finding(finding=finding) for finding in bsm_findings)
                llm_notes.append(
                    f"Leichter BSM-Claim-Check fand {len(bsm_findings)} strukturelle Widersprueche in {len(fast_claim_documents)} Architekturquellen."
                )
            else:
                llm_notes.append(
                    f"Leichter BSM-Claim-Check pruefte {len(fast_claim_documents)} Architekturquellen ohne strukturellen Widerspruch."
                )

        review_cards = _dedupe_review_cards(review_cards=review_cards)
        review_card_findings = [_review_card_to_finding(card=card) for card in review_cards]
        findings = review_card_findings
        review_cards = [
            card.model_copy(update={"related_finding_ids": [review_card_findings[index].finding_id]})
            for index, card in enumerate(review_cards)
        ]

        if progress_callback is not None:
            progress_callback(
                "review_cards",
                len(review_cards),
                max(len(review_cards), 1),
                f"{len(review_cards)} Review-Karten wurden fuer die Bewertung vorbereitet.",
            )

        prioritized_section_ids = {item.section_id for item in prioritized_sections}
        coverage_summary = ReviewCardCoverageSummary(
            total_documents=len(documents),
            total_sections=len(sections),
            prioritized_sections=len(prioritized_sections),
            compared_pairs=len(candidates),
            skipped_sections_due_to_prioritization=max(0, len(sections) - len(prioritized_sections)),
            skipped_pairs_due_to_budget=max(0, len(prioritized_sections) - len(candidates)),
            source_type_counts=_source_type_counts(documents=documents),
            prioritized_scope_labels=_scope_labels_for_sections(sections=prioritized_sections),
            compared_scope_labels=_scope_labels_for_candidates(candidates=candidates),
            deferred_scope_labels=_scope_labels_for_sections(
                sections=[section for section in sections if section.section_id not in prioritized_section_ids]
            ),
            notes=[*candidate_notes, *llm_notes],
        )
        summary = (
            f"Fast Audit abgeschlossen: {len(review_cards)} Review-Karten aus {len(candidates)} "
            f"priorisierten Vergleichskandidaten bei {len(documents)} Quellen."
        )
        notes = [
            f"Fast Audit nutzte {len(prioritized_sections)} priorisierte Sektionen statt eines Vollvergleichs.",
            f"{len(review_cards)} Review-Karten wurden als primaere Review-Artefakte erzeugt.",
            *coverage_summary.notes,
        ]
        return FastAuditResult(
            review_cards=review_cards,
            findings=findings,
            claims=[record.claim for record in fast_claim_records],
            analysis_notes=notes,
            summary=summary,
            coverage_summary=coverage_summary,
            budget_limited=budget_limited,
            llm_usage=llm_usage,
        )

    async def _compare_with_llm(
        self,
        *,
        candidates: list[ComparisonCandidate],
    ) -> tuple[list[ReviewCard], list[str], dict]:
        slot = _select_fast_audit_slot(settings=self._settings)
        if slot is None:
            return [], ["Kein geeigneter LLM-Slot fuer den Fast-Audit-Vergleich konfiguriert."], {}
        client = LiteLLMClient(settings=self._settings, default_slot=slot)
        review_cards: list[ReviewCard] = []
        usage: dict = {"by_model": {}, "total_prompt_tokens": 0, "total_completion_tokens": 0, "total_cost_usd": 0.0, "total_cost_eur": 0.0}
        notes: list[str] = []

        for index in range(0, len(candidates), LLM_BATCH_SIZE):
            batch = candidates[index : index + LLM_BATCH_SIZE]
            try:
                parsed, response = await client.structured_output_with_usage(
                    messages=_build_fast_audit_messages(candidates=batch),
                    schema=_LLMReviewCardBatch,
                    config=GenerationConfig(slot=slot, max_tokens=2200, temperature=0.1, timeout_s=45.0),
                )
                _merge_usage(total=usage, response=response)
                review_cards.extend(
                    card
                    for item in parsed.items
                    for card in [_llm_item_to_review_card(item=item, candidates=batch)]
                    if card is not None
                )
            except Exception as exc:
                logger.warning("fast_audit_llm_batch_failed", extra={"event_payload": {"error": str(exc)}})
                notes.append(
                    f"LLM-Vergleich fuer Batch {index // LLM_BATCH_SIZE + 1} fehlgeschlagen; heuristische Ableitung wurde verwendet."
                )
                review_cards.extend(
                    card for card in (_heuristic_card(candidate=candidate) for candidate in batch) if card is not None
                )

        if not notes:
            notes.append(f"Fast Audit nutzte strukturierte KI-Vergleiche fuer {len(candidates)} Kandidaten.")
        return review_cards, notes, usage


def _extract_sections(*, documents: list[CollectedDocument]) -> list[FastAuditSection]:
    sections: list[FastAuditSection] = []
    for document in documents:
        chunks = _split_document_into_sections(document=document)[:MAX_SECTIONS_PER_DOCUMENT]
        for index, (heading, body) in enumerate(chunks, start=1):
            normalized_body = _normalize_text(body)
            if len(normalized_body) < 60:
                continue
            topic_tokens = _topic_tokens(f"{document.title}\n{heading}\n{normalized_body}")
            delta_status = str(document.snapshot.metadata.get("delta_status") or "changed").strip() or "changed"
            location = AuditLocation(
                snapshot_id=document.snapshot.snapshot_id,
                source_type=document.source_type,
                source_id=document.source_id,
                title=document.title,
                path_hint=document.path_hint,
                url=document.url,
                position=AuditPosition(
                    anchor_kind="section",
                    anchor_value=heading or document.title,
                    section_path=heading or document.title,
                    content_hash=document.snapshot.content_hash,
                ),
                metadata={"delta_status": delta_status},
            )
            priority_score = _priority_score(document=document, heading=heading, body=normalized_body, delta_status=delta_status)
            sections.append(
                FastAuditSection(
                    section_id=_section_id(document=document, index=index, heading=heading),
                    source_type=document.source_type,
                    source_id=document.source_id,
                    title=document.title,
                    path_hint=document.path_hint,
                    url=document.url,
                    heading=heading or document.title,
                    body=normalized_body[:1400],
                    delta_status=delta_status,
                    priority_score=priority_score,
                    topic_tokens=topic_tokens,
                    location=location,
                    explicit_focus=bool(document.metadata.get("explicitly_selected")),
                )
            )
    return sections


def _split_document_into_sections(*, document: CollectedDocument) -> list[tuple[str, str]]:
    text = str(document.body or "").strip()
    if not text:
        return []
    if document.source_type == "github_file":
        pattern = re.compile(
            r"(?m)^(class\s+\w+|async\s+def\s+\w+|def\s+\w+|interface\s+\w+|function\s+\w+|const\s+\w+\s*=)"
        )
        matches = list(pattern.finditer(text))
        if matches:
            sections: list[tuple[str, str]] = []
            for index, match in enumerate(matches):
                start = match.start()
                end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
                heading = match.group(1).strip()
                sections.append((heading, text[start:end].strip()))
            return sections
    heading_pattern = re.compile(r"(?m)^(#{1,6}\s+.+)$")
    matches = list(heading_pattern.finditer(text))
    if matches:
        sections = []
        for index, match in enumerate(matches):
            start = match.end()
            end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
            heading = match.group(1).lstrip("#").strip()
            body = text[start:end].strip()
            if body:
                sections.append((heading, body))
        if sections:
            return sections
    paragraphs = [chunk.strip() for chunk in re.split(r"\n\s*\n", text) if chunk.strip()]
    if not paragraphs:
        return [(document.title, text[:1400])]
    merged: list[tuple[str, str]] = []
    buffer: list[str] = []
    for paragraph in paragraphs:
        buffer.append(paragraph)
        if sum(len(item) for item in buffer) >= 900:
            merged.append((document.title, "\n\n".join(buffer)))
            buffer = []
    if buffer:
        merged.append((document.title, "\n\n".join(buffer)))
    return merged


def _build_candidates(
    *,
    prioritized_sections: list[FastAuditSection],
    started_at: float,
    time_budget_seconds: int,
) -> tuple[list[ComparisonCandidate], list[str], bool]:
    candidates: list[ComparisonCandidate] = []
    used_pairs: set[tuple[str, str]] = set()
    budget_limited = False
    focused_sections = [section for section in prioritized_sections if section.explicit_focus]
    regular_sections = [section for section in prioritized_sections if not section.explicit_focus]

    for section in [*focused_sections, *regular_sections]:
        if time.monotonic() - started_at >= time_budget_seconds:
            budget_limited = True
            break
        possible_matches = [
            other
            for other in prioritized_sections
            if other.section_id != section.section_id
            and other.source_id != section.source_id
        ]
        scored_matches = sorted(
            (
                (_candidate_score(left=section, right=other), other)
                for other in possible_matches
            ),
            key=lambda item: (-item[0], item[1].section_id),
        )
        chosen_matches: list[FastAuditSection] = []
        for score, other in scored_matches:
            if score < 3:
                continue
            if section.explicit_focus and not _has_anchor_alignment(left=section, right=other):
                continue
            chosen_matches.append(other)
            if len(chosen_matches) >= 2:
                break
        if not chosen_matches and section.priority_score >= 8:
            candidate_key = tuple(sorted((section.section_id, "missing")))
            if candidate_key in used_pairs:
                continue
            used_pairs.add(candidate_key)
            candidates.append(
                ComparisonCandidate(
                    candidate_id=f"cand_{hashlib.sha1(candidate_key[0].encode('utf-8')).hexdigest()[:10]}",
                    left=section,
                    right=None,
                    score=section.priority_score,
                    comparison_reason=(
                        "Explizit ausgewaehlte Fokus-Sektion ohne belastbares Gegenstueck."
                        if section.explicit_focus
                        else "Hohe Prioritaet ohne belastbares Gegenstueck."
                    ),
                )
            )
            continue
        for other in chosen_matches:
            pair_key = tuple(sorted((section.section_id, other.section_id)))
            if pair_key in used_pairs:
                continue
            used_pairs.add(pair_key)
            candidates.append(
                ComparisonCandidate(
                    candidate_id=f"cand_{hashlib.sha1('|'.join(pair_key).encode('utf-8')).hexdigest()[:10]}",
                    left=section,
                    right=other,
                    score=_candidate_score(left=section, right=other),
                    comparison_reason="Gemeinsame Themen- und Vertragsbegriffe.",
                )
            )
            if len(candidates) >= MAX_COMPARISON_CANDIDATES:
                budget_limited = len(prioritized_sections) > len(candidates)
                break
        if len(candidates) >= MAX_COMPARISON_CANDIDATES:
            break
    notes = [
        f"{len(prioritized_sections)} priorisierte Sektionen wurden fuer den Fast Audit vorbereitet.",
        f"{len(candidates)} Vergleichskandidaten wurden fuer den schnellen KI-Vergleich ausgewaehlt.",
    ]
    if budget_limited:
        notes.append("Fast Audit hat das Vergleichsbudget begrenzt und den Rest bewusst zurueckgestellt.")
    return candidates, notes, budget_limited


def _candidate_score(*, left: FastAuditSection, right: FastAuditSection) -> int:
    overlap = len(left.topic_tokens.intersection(right.topic_tokens))
    title_overlap = len(_topic_tokens(left.heading).intersection(_topic_tokens(right.heading)))
    delta_bonus = 2 if left.delta_status != "unchanged" or right.delta_status != "unchanged" else 0
    return overlap * 2 + title_overlap * 3 + delta_bonus


def _has_anchor_alignment(*, left: FastAuditSection, right: FastAuditSection) -> bool:
    left_anchor_tokens = _topic_tokens(left.heading)
    if not left_anchor_tokens:
        return False
    right_anchor_tokens = _topic_tokens("\n".join(part for part in [right.title, right.heading, right.path_hint or ""] if part))
    return bool(left_anchor_tokens.intersection(right_anchor_tokens))


def _heuristic_card(*, candidate: ComparisonCandidate) -> ReviewCard | None:
    left = candidate.left
    right = candidate.right
    if right is None:
        source_a_claim = _statement_excerpt(left.body)
        source_b_claim = "Kein belastbares Gegenstueck innerhalb des priorisierten Fast-Audit-Budgets gefunden."
        deviation_type = "gap"
        return ReviewCard(
            title=f"Spiegelungsluecke bei {left.heading}",
            deviation_type=deviation_type,
            summary=(
                f"Fuer {left.heading} wurde in diesem Fast-Audit-Lauf kein belastbares Gegenstueck "
                "innerhalb der priorisierten Vergleichsmenge gefunden."
            ),
            source_a=_source_label(section=left),
            source_b="Kein priorisierter Gegenstand",
            source_a_evidence=[source_a_claim],
            source_b_evidence=[source_b_claim],
            source_a_locations=[left.location],
            source_b_locations=[],
            why_it_matters=(
                "Die Aussage wurde in diesem Run nicht gegen eine zweite Quelle gespiegelt. "
                "Das ist noch kein nachgewiesener fachlicher Drift, sondern zunaechst eine Abdeckungs- oder Scope-Luecke."
            ),
            recommended_decision=(
                "Pruefen, ob fuer diesen Abschnitt ueberhaupt ein passendes Soll-Gegenstueck im Scope existiert "
                "oder ob der Fast Audit hier nur am Priorisierungsbudget geendet hat."
            ),
            confidence=0.62,
            priority="medium",
            follow_up_capabilities=[],
            metadata=_review_card_metadata(
                left=left,
                right=right,
                deviation_type=deviation_type,
                source_a_claim=source_a_claim,
                source_b_claim=source_b_claim,
            ),
        )
    left_tokens = left.topic_tokens
    right_tokens = right.topic_tokens
    shared = left_tokens.intersection(right_tokens)
    if len(shared) < 2:
        return None
    if _looks_obsolete(text=right.body) or _looks_obsolete(text=left.body):
        deviation_type = "obsolete"
        why = "Eine der beiden Aussagen wirkt zeitlich oder fachlich veraltet."
        recommended = "Festlegen, welche Quelle den aktuellen Stand repraesentiert, und die veraltete Aussage entfernen."
        priority = "medium"
    elif _contains_gap_signal(left.body) or _contains_gap_signal(right.body):
        deviation_type = "gap"
        why = "Mindestens eine Seite beschreibt das Thema nur unvollstaendig oder indirekt."
        recommended = "Fehlende Definition oder Prozessregel explizit nachziehen."
        priority = "high"
    elif _contains_conflict_signal(left.body, right.body):
        deviation_type = "error"
        why = "Die beiden Abschnitte sprechen mit hoher Wahrscheinlichkeit ueber denselben Sachverhalt, aber nicht deckungsgleich."
        recommended = "Pruefen, welche Quelle gilt, und die abweichende Aussage korrigieren."
        priority = "high"
    else:
        deviation_type = "misunderstanding"
        why = "Die beiden Beschreibungen scheinen dasselbe Thema unterschiedlich zu rahmen oder zu interpretieren."
        recommended = "Begriffe und Erwartungshaltung vereinheitlichen, bevor Folgeaktionen ausgelöst werden."
        priority = "medium"
    source_a_claim = _statement_excerpt(left.body)
    source_b_claim = _statement_excerpt(right.body)
    return ReviewCard(
        title=f"{left.heading} vs {right.heading}",
        deviation_type=deviation_type,
        summary=f"{_source_label(section=left)} und {_source_label(section=right)} weichen im priorisierten Vergleich voneinander ab.",
        source_a=_source_label(section=left),
        source_b=_source_label(section=right),
        source_a_evidence=[source_a_claim],
        source_b_evidence=[source_b_claim],
        source_a_locations=[left.location],
        source_b_locations=[right.location],
        why_it_matters=why,
        recommended_decision=recommended,
        confidence=0.58 if deviation_type == "misunderstanding" else 0.68,
        priority=priority,
        follow_up_capabilities=_follow_up_capabilities(source_types={left.source_type, right.source_type}),
        metadata=_review_card_metadata(
            left=left,
            right=right,
            deviation_type=deviation_type,
            source_a_claim=source_a_claim,
            source_b_claim=source_b_claim,
        ),
    )


def _build_fast_audit_messages(*, candidates: list[ComparisonCandidate]) -> list[ChatMessage]:
    candidate_lines: list[str] = []
    for candidate in candidates:
        right_label = _source_label(section=candidate.right) if candidate.right is not None else "Kein Gegenstueck"
        right_body = candidate.right.body[:600] if candidate.right is not None else "Kein passender Gegenabschnitt im Budget."
        candidate_lines.append(
            "\n".join(
                [
                    f"candidate_id={candidate.candidate_id}",
                    f"reason={candidate.comparison_reason}",
                    f"source_a={_source_label(section=candidate.left)}",
                    f"source_a_heading={candidate.left.heading}",
                    f"source_a_text={candidate.left.body[:600]}",
                    f"source_b={right_label}",
                    f"source_b_heading={candidate.right.heading if candidate.right is not None else 'N/A'}",
                    f"source_b_text={right_body}",
                ]
            )
        )
    return [
        ChatMessage(
            role="system",
            content=(
                "Du bist ein Auditor fuer schnellen Soll/Ist-Vergleich. "
                "Vergleiche nur die gelieferten Kandidaten und liefere ausschliesslich JSON. "
                "Erzeuge nur dann eine Review-Karte, wenn eine inhaltlich relevante Abweichung vorliegt. "
                "Nutze deviation_type nur aus: error, gap, misunderstanding, obsolete, unclear."
            ),
        ),
        ChatMessage(
            role="user",
            content=(
                "Vergleiche die folgenden Kandidaten und liefere fuer jeden Kandidaten optional eine Review-Karte.\n\n"
                + "\n\n---\n\n".join(candidate_lines)
            ),
        ),
    ]


def _llm_item_to_review_card(
    *,
    item: _LLMReviewCardItem,
    candidates: list[ComparisonCandidate],
) -> ReviewCard | None:
    if not item.should_create_card:
        return None
    candidate = next((candidate for candidate in candidates if candidate.candidate_id == item.candidate_id), None)
    if candidate is None:
        return None
    left = candidate.left
    right = candidate.right
    source_a_claim = item.source_a_evidence[0] if item.source_a_evidence else _statement_excerpt(left.body)
    source_b_claim = (
        item.source_b_evidence[0]
        if item.source_b_evidence
        else (_statement_excerpt(right.body) if right is not None else "Kein Gegenstueck im Budget gefunden.")
    )
    deviation_type = item.deviation_type if item.deviation_type in {"error", "gap", "misunderstanding", "obsolete", "unclear"} else "unclear"
    return ReviewCard(
        title=item.title or f"{left.heading} Abweichung",
        deviation_type=deviation_type,
        summary=item.summary or f"{_source_label(section=left)} und {_source_label(section=right) if right is not None else 'kein Gegenstueck'} weichen ab.",
        source_a=_source_label(section=left),
        source_b=_source_label(section=right) if right is not None else "Kein priorisierter Gegenstand",
        source_a_evidence=item.source_a_evidence or [source_a_claim],
        source_b_evidence=item.source_b_evidence or [source_b_claim],
        source_a_locations=[left.location],
        source_b_locations=[right.location] if right is not None else [],
        why_it_matters=item.why_it_matters or "Die Abweichung beeinflusst die inhaltliche Konsistenz des Auditmaterials.",
        recommended_decision=item.recommended_decision or "Die beiden Aussagen fachlich gegeneinander abgleichen und den gueltigen Stand festhalten.",
        confidence=item.confidence,
        priority=item.priority if item.priority in {"high", "medium", "low"} else "medium",
        follow_up_capabilities=_follow_up_capabilities(
            source_types={left.source_type, *( [right.source_type] if right is not None else [] )}
        ),
        metadata=_review_card_metadata(
            left=left,
            right=right,
            deviation_type=deviation_type,
            source_a_claim=source_a_claim,
            source_b_claim=source_b_claim,
        ),
    )


def _review_card_to_finding(*, card: ReviewCard) -> AuditFinding:
    category_map = {
        "error": "contradiction",
        "gap": "missing_documentation",
        "misunderstanding": "clarification_needed",
        "obsolete": "obsolete_documentation",
        "unclear": "open_decision",
    }
    severity_map = {
        "high": "high",
        "medium": "medium",
        "low": "low",
    }
    locations = [*card.source_a_locations, *card.source_b_locations]
    is_budget_gap = bool(card.metadata.get("is_budget_gap"))
    return AuditFinding(
        severity=severity_map.get(card.priority, "medium"),
        category="open_decision" if is_budget_gap else category_map.get(card.deviation_type, "open_decision"),
        title=card.title,
        summary=card.summary,
        recommendation=card.recommended_decision,
        canonical_key=f"fast_review|{card.card_id}",
        locations=locations,
        proposed_confluence_action=(
            "Doku-Abweichung nach User-Entscheidung korrigieren."
            if "confluence_page_update" in card.follow_up_capabilities
            else None
        ),
        proposed_jira_action=(
            "Code- oder Artefaktfolge nach User-Entscheidung als Jira-Ticket ausleiten."
            if "jira_ticket_create" in card.follow_up_capabilities
            else None
        ),
        metadata={
            "review_card_id": card.card_id,
            "analysis_mode": "fast",
            "source_types": [location.source_type for location in locations],
        },
    )


def _review_card_from_bsm_finding(*, finding: AuditFinding) -> ReviewCard:
    locations = finding.locations[:]
    source_a_location = locations[0] if locations else None
    source_b_location = locations[1] if len(locations) > 1 else None
    source_a = _location_label(source_a_location) if source_a_location is not None else "Mehrere Architekturquellen"
    source_b = _location_label(source_b_location) if source_b_location is not None else "Weitere Architekturquelle"
    evidence_lines = [line.strip() for line in finding.summary.splitlines() if line.strip()]
    intro_line = evidence_lines[0] if evidence_lines else finding.summary
    detail_lines = evidence_lines[1:] if len(evidence_lines) > 1 else [finding.summary]
    source_a_evidence = [detail_lines[0]] if detail_lines else [intro_line]
    source_b_evidence = [detail_lines[1]] if len(detail_lines) > 1 else ([detail_lines[0]] if detail_lines else [finding.recommendation])
    deviation_type = _deviation_type_for_bsm_finding(finding=finding)
    metadata = _finding_review_card_metadata(
        finding=finding,
        source_a_label=source_a,
        source_b_label=source_b,
        source_a_evidence=source_a_evidence[0],
        source_b_evidence=source_b_evidence[0],
    )
    follow_up_capabilities = _follow_up_capabilities(source_types={location.source_type for location in locations}) or ["confluence_page_update", "jira_ticket_create"]
    priority = "high" if finding.severity in {"critical", "high"} else "medium"
    return ReviewCard(
        title=finding.title,
        deviation_type=deviation_type,
        summary=intro_line,
        source_a=source_a,
        source_b=source_b,
        source_a_evidence=source_a_evidence,
        source_b_evidence=source_b_evidence,
        source_a_locations=[source_a_location] if source_a_location is not None else [],
        source_b_locations=[source_b_location] if source_b_location is not None else [],
        why_it_matters=finding.summary if finding.summary else finding.recommendation,
        recommended_decision=finding.recommendation,
        confidence=0.82 if finding.category == "contradiction" else 0.7,
        priority=priority,
        follow_up_capabilities=follow_up_capabilities,
        metadata=metadata,
    )


def _priority_score(*, document: CollectedDocument, heading: str, body: str, delta_status: str) -> int:
    source_weight = {
        "confluence_page": 4,
        "metamodel": 5,
        "local_doc": 4,
        "github_file": 3,
        "jira_ticket": 2,
    }.get(document.source_type, 1)
    delta_weight = {"added": 4, "changed": 3, "unchanged": 0}.get(delta_status, 2)
    keyword_weight = sum(1 for term in PRIORITY_TERMS if term in f"{heading}\n{body}".casefold())
    explicit_focus_bonus = 12 if bool(document.metadata.get("explicitly_selected")) else 0
    return source_weight + delta_weight + keyword_weight + explicit_focus_bonus


def _source_label(*, section: FastAuditSection | None) -> str:
    if section is None:
        return "Kein priorisierter Gegenstand"
    label = SOURCE_LABELS.get(section.source_type, section.source_type)
    anchor = section.heading or section.title
    return f"{label}: {section.title} · {anchor}"


def _scope_label(*, source_type: str, title: str, heading: str) -> str:
    label = SOURCE_LABELS.get(source_type, source_type)
    return f"{label}: {title} · {heading or title}"


def _scope_labels_for_sections(*, sections: list[FastAuditSection], limit: int = 8) -> list[str]:
    labels: list[str] = []
    seen: set[str] = set()
    for section in sections:
        label = _scope_label(source_type=section.source_type, title=section.title, heading=section.heading)
        if label in seen:
            continue
        seen.add(label)
        labels.append(label)
        if len(labels) >= limit:
            break
    return labels


def _scope_labels_for_candidates(*, candidates: list[ComparisonCandidate], limit: int = 8) -> list[str]:
    labels: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        for section in (candidate.left, candidate.right):
            if section is None:
                continue
            label = _scope_label(source_type=section.source_type, title=section.title, heading=section.heading)
            if label in seen:
                continue
            seen.add(label)
            labels.append(label)
            if len(labels) >= limit:
                return labels
    return labels


def _source_type_counts(*, documents: list[CollectedDocument]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for document in documents:
        key = str(document.source_type)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _location_label(location: AuditLocation | None) -> str:
    if location is None:
        return "Unbekannte Quelle"
    label = SOURCE_LABELS.get(location.source_type, location.source_type)
    anchor = (
        location.position.section_path
        if location.position is not None and location.position.section_path
        else location.title
    )
    return f"{label}: {location.title} · {anchor}"


def _topic_tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-zA-Z0-9_]{4,}", text.casefold())
        if token not in STOPWORDS
    }


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _statement_excerpt(text: str, *, max_sentences: int = 2, max_chars: int = 420) -> str:
    normalized = _normalize_text(text)
    if not normalized:
        return ""
    sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", normalized) if part.strip()]
    excerpt_parts: list[str] = []
    current_length = 0
    for sentence in sentences:
        addition = len(sentence) + (1 if excerpt_parts else 0)
        if excerpt_parts and current_length + addition > max_chars:
            break
        excerpt_parts.append(sentence)
        current_length += addition
        if len(excerpt_parts) >= max_sentences:
            break
    excerpt = " ".join(excerpt_parts) if excerpt_parts else normalized[:max_chars].strip()
    if len(excerpt) >= len(normalized):
        return excerpt
    trimmed = excerpt[:max_chars].rstrip()
    return f"{trimmed} ..."


def _decision_question(*, left: FastAuditSection, right: FastAuditSection | None, deviation_type: str) -> str:
    focus = left.heading or left.title
    if deviation_type == "gap":
        if right is None:
            return f"Soll fuer '{focus}' ein belastbares Gegenstueck oder eine ergaenzende Soll-Quelle nachgezogen werden?"
        return f"Soll fuer '{focus}' die fehlende oder unvollstaendige Aussage in einer der beiden Quellen explizit nachgezogen werden?"
    if deviation_type == "error":
        return f"Welche der beiden Aussagen soll fuer '{focus}' kuenftig als gueltig gelten?"
    if deviation_type == "obsolete":
        return f"Welche Aussage ist fuer '{focus}' aktuell, und welche Quelle soll als veraltet markiert werden?"
    if deviation_type == "misunderstanding":
        return f"Beschreiben beide Quellen fuer '{focus}' denselben Sachverhalt, oder liegt ein Begriffs- bzw. Scope-Missverstaendnis vor?"
    return f"Reicht die Evidenz fuer '{focus}' fuer eine belastbare Entscheidung aus, oder braucht es zuerst eine Rueckfrage?"


def _decision_action_labels(*, deviation_type: str) -> dict[str, str]:
    if deviation_type == "gap":
        return {
            "accept_label": "Luecke bestaetigen",
            "reject_label": "Nicht als Luecke werten",
            "clarify_label": "Scope zuerst klaeren",
        }
    if deviation_type == "error":
        return {
            "accept_label": "Abweichung bestaetigen",
            "reject_label": "Kein belastbarer Konflikt",
            "clarify_label": "Zustaendigkeit klaeren",
        }
    if deviation_type == "obsolete":
        return {
            "accept_label": "Veraltete Aussage bestaetigen",
            "reject_label": "Beide Aussagen noch gueltig",
            "clarify_label": "Aktuellen Stand klaeren",
        }
    if deviation_type == "misunderstanding":
        return {
            "accept_label": "Missverstaendnis bestaetigen",
            "reject_label": "Aussagen als vereinbar werten",
            "clarify_label": "Begriffe klaeren",
        }
    return {
        "accept_label": "Klaerungsbedarf bestaetigen",
        "reject_label": "Kein separater Befund",
        "clarify_label": "Weitere Evidenz anfordern",
    }


def _decision_consequences(
    *,
    deviation_type: str,
    follow_up_capabilities: list[str],
) -> dict[str, list[str]]:
    accept = [
        f"Die Karte wird als {deviation_type} akzeptiert und aus dem offenen Review entfernt.",
        "Zugehoerige Findings werden als akzeptiert markiert.",
    ]
    if "confluence_page_update" in follow_up_capabilities:
        accept.append("Confluence- oder Doku-Folgeaktionen koennen danach freigegeben werden.")
    if "jira_ticket_create" in follow_up_capabilities:
        accept.append("Jira-Folgeaktionen fuer Code- oder Artefaktanpassungen koennen danach freigegeben werden.")
    reject = [
        "Die Karte wird als abgelehnt geschlossen und aus dem offenen Review entfernt.",
        "Zugehoerige Findings werden verworfen und nicht in Folgefreigaben uebernommen.",
    ]
    clarify = [
        "Die Karte bleibt als Klaerfall markiert und wird nicht in Folgefreigaben uebernommen.",
        "Zugehoerige Findings bleiben offen, bis eine Rueckfrage oder manuelle Einordnung vorliegt.",
    ]
    return {
        "accept_consequences": accept,
        "reject_consequences": reject,
        "clarify_consequences": clarify,
    }


def _lane_metadata(
    *,
    subject_key: str,
    left: FastAuditSection | None,
    right: FastAuditSection | None,
    deviation_type: str,
) -> dict[str, object]:
    lane_key, lane_label, lane_rank = _determine_lane(
        subject_key=subject_key,
        left=left,
        right=right,
        deviation_type=deviation_type,
    )
    return {
        "decision_lane": lane_key,
        "decision_lane_label": lane_label,
        "decision_lane_rank": lane_rank,
        "decision_independence": True,
        "decision_independence_note": (
            "Diese Karte basiert auf dem stabilen Snapshot dieses Runs. "
            "Ihre Entscheidung aendert andere offene Karten nicht sofort; eine Neuberechnung passiert erst in einem neuen oder explizit aktualisierten Run."
        ),
    }


def _determine_lane(
    *,
    subject_key: str,
    left: FastAuditSection | None,
    right: FastAuditSection | None,
    deviation_type: str,
) -> tuple[str, str, int]:
    subject = str(subject_key or "").casefold()
    combined = "\n".join(
        part for part in [
            subject,
            left.heading if left is not None else "",
            left.title if left is not None else "",
            left.body if left is not None else "",
            right.heading if right is not None else "",
            right.title if right is not None else "",
            right.body if right is not None else "",
        ] if part
    ).casefold()
    if any(token in subject for token in ("status_canon", "initial_state", "field_propagation")) or any(
        token in combined for token in ("pflichtfeld", "mandatory", "required field", "property", "feld", "schema", "metaclass")
    ):
        return ("property_schema", "Properties & Schema", 0)
    if any(token in subject for token in ("relationship.lifecycle", "relationship.change_model", "evidencechain", "summarisedanswer.role")) or any(
        token in combined for token in ("relationship", "kante", "node", "knoten", "label", "lifecycle", "staged", "proposed", "active", "rejected")
    ):
        return ("object_lifecycle", "Objekte, Kanten & Lifecycle", 1)
    if any(token in subject for token in ("run.model", "statement.hitl", "phase.scope_distinction", "to_modify.role")) or any(
        token in combined for token in ("run", "in_run", "phase", "ui_phase_id", "phase_id", "chunkphaserun", "phaserun", "hitl", "workflow", "scope")
    ):
        return ("process_scope", "Prozess, Run & Scope", 2)
    if (
        left is not None
        and right is None
        and left.source_type in {"confluence_page", "local_doc"}
    ) or any(token in combined for token in ("confluence", "seite", "kapitel", "gliederung", "struktur", "inhaltsueberblick")):
        return ("documentation_structure", "Doku-Struktur & Konsolidierung", 3)
    if deviation_type == "gap":
        return ("documentation_structure", "Doku-Struktur & Konsolidierung", 3)
    return ("process_scope", "Prozess, Run & Scope", 2)


def _deviation_type_for_bsm_finding(*, finding: AuditFinding) -> str:
    if finding.category == "contradiction":
        return "error"
    if finding.category in {"implementation_drift", "obsolete_documentation"}:
        return "obsolete"
    if finding.category in {"clarification_needed", "open_decision", "architecture_observation"}:
        return "unclear"
    if finding.category in {"missing_documentation", "traceability_gap"}:
        return "gap"
    return "misunderstanding"


def _finding_review_card_metadata(
    *,
    finding: AuditFinding,
    source_a_label: str,
    source_b_label: str,
    source_a_evidence: str,
    source_b_evidence: str,
) -> dict[str, object]:
    subject_key = str(finding.metadata.get("subject_key") or finding.metadata.get("object_key") or finding.canonical_key or finding.title)
    deviation_type = _deviation_type_for_bsm_finding(finding=finding)
    labels = _decision_action_labels(deviation_type=deviation_type)
    capabilities = _follow_up_capabilities(source_types={location.source_type for location in finding.locations})
    consequences = _decision_consequences(
        deviation_type=deviation_type,
        follow_up_capabilities=capabilities,
    )
    return {
        "comparison_focus": subject_key,
        "subject_key": subject_key,
        "source_a_heading": source_a_label,
        "source_b_heading": source_b_label,
        "source_a_claim": source_a_evidence,
        "source_b_claim": source_b_evidence,
        "source_a_full_text": finding.summary,
        "source_b_full_text": finding.recommendation,
        "decision_question": f"Soll der Widerspruch zu '{subject_key}' als echter Architekturkonflikt akzeptiert und konsolidiert werden?",
        "accept_label": labels["accept_label"],
        "reject_label": labels["reject_label"],
        "clarify_label": labels["clarify_label"],
        "rationale_summary": (
            "Diese Karte liegt in einer eigenstaendigen Entscheidungslane. "
            "Sie kann ohne sofortige globale Neuberechnung der restlichen offenen Karten bewertet werden."
        ),
        **_lane_metadata(
            subject_key=subject_key,
            left=None,
            right=None,
            deviation_type=deviation_type,
        ),
        **consequences,
    }


def _select_fast_claim_documents(*, documents: list[CollectedDocument]) -> list[CollectedDocument]:
    selected: list[CollectedDocument] = []
    for document in documents:
        body_lower = str(document.body or "").casefold()
        path_lower = f"{document.source_id} {document.title} {document.path_hint or ''}".casefold()
        is_structured_arch_doc = any(
            token in path_lower
            for token in (
                ".puml",
                "metamodel_export.json",
                "current_dump.json",
                "ssot",
                "status",
                "scope",
                "run",
                "pipeline",
            )
        )
        has_bsm_signal = any(
            token in body_lower
            for token in (
                "summarisedanswer",
                "summarisedanswerunit",
                "statement",
                "bsm_element",
                "to_modify",
                "in_run",
                "phase_id",
                "ui_phase_id",
                "phaserun",
                "chunkphaserun",
                "proposed",
                "staged",
                "verified",
                "refined",
            )
        )
        if bool(document.metadata.get("explicitly_selected")):
            selected.append(document)
            continue
        if document.source_type == "metamodel":
            selected.append(document)
            continue
        if document.source_type in {"confluence_page", "local_doc"} and has_bsm_signal:
            selected.append(document)
            continue
        if document.source_type == "github_file" and (is_structured_arch_doc or has_bsm_signal):
            selected.append(document)
    return selected


def _review_card_metadata(
    *,
    left: FastAuditSection,
    right: FastAuditSection | None,
    deviation_type: str,
    source_a_claim: str,
    source_b_claim: str,
) -> dict[str, object]:
    is_budget_gap = deviation_type == "gap" and right is None
    if is_budget_gap:
        labels = {
            "accept_label": "Als Abdeckungsluecke akzeptieren",
            "reject_label": "Nicht als fachlichen Befund werten",
            "clarify_label": "Scope zuerst klaeren",
        }
        consequences = {
            "accept_consequences": [
                "Die Karte wird als Abdeckungs- bzw. Spiegelungsluecke akzeptiert und aus dem offenen Review entfernt.",
                "Zugehoerige Findings bleiben als Scope-/Coverage-Hinweis dokumentiert, nicht als nachgewiesene fachliche Abweichung.",
            ],
            "reject_consequences": [
                "Die Karte wird geschlossen und nicht als fachlicher Befund weitergefuehrt.",
                "Es werden keine Folgefreigaben fuer Doku- oder Code-Korrekturen erzeugt.",
            ],
            "clarify_consequences": [
                "Die Karte bleibt als Klaerfall markiert, bis Scope oder Gegenquelle explizit geklaert sind.",
                "Erst nach einer Nachspiegelung oder klaren Scope-Entscheidung ist eine fachliche Bewertung belastbar.",
            ],
        }
        decision_question = (
            f"Soll fuer '{left.heading or left.title}' zuerst Scope bzw. Gegenquelle geklaert werden, "
            "bevor ueber einen fachlichen Befund entschieden wird?"
        )
        rationale_summary = (
            "Diese Karte zeigt primar eine fehlende Spiegelung im priorisierten Fast-Audit-Scope. "
            "Ohne Gegenquelle ist daraus noch kein fachlicher Drift ableitbar."
        )
    else:
        labels = _decision_action_labels(deviation_type=deviation_type)
        capabilities = _follow_up_capabilities(
            source_types={left.source_type, *( [right.source_type] if right is not None else [] )}
        )
        consequences = _decision_consequences(
            deviation_type=deviation_type,
            follow_up_capabilities=capabilities,
        )
        decision_question = _decision_question(left=left, right=right, deviation_type=deviation_type)
        rationale_summary = (
            "Diese Karte ist als atomare Entscheidung innerhalb eines stabilen Run-Snapshots modelliert. "
            "Andere offene Karten werden nicht sofort neu berechnet."
        )
    return {
        "comparison_focus": left.heading or left.title,
        "gap_class": "coverage_gap" if is_budget_gap else ("content_gap" if deviation_type == "gap" else ""),
        "is_budget_gap": is_budget_gap,
        "source_a_heading": left.heading,
        "source_b_heading": right.heading if right is not None else "",
        "source_a_claim": source_a_claim,
        "source_b_claim": source_b_claim,
        "source_a_full_text": _normalize_text(left.body),
        "source_b_full_text": _normalize_text(right.body) if right is not None else source_b_claim,
        "decision_question": decision_question,
        "accept_label": labels["accept_label"],
        "reject_label": labels["reject_label"],
        "clarify_label": labels["clarify_label"],
        "rationale_summary": rationale_summary,
        **_lane_metadata(
            subject_key=left.heading or left.title,
            left=left,
            right=right,
            deviation_type=deviation_type,
        ),
        **consequences,
    }


def _section_id(*, document: CollectedDocument, index: int, heading: str) -> str:
    base = f"{document.source_type}|{document.source_id}|{index}|{heading}".encode("utf-8")
    return hashlib.sha1(base).hexdigest()[:16]


def _contains_conflict_signal(left_text: str, right_text: str) -> bool:
    approvals = ("approval", "review", "required", "must", "mandatory")
    bypass = ("without approval", "direct write", "manual", "bypass", "optional")
    left_lower = left_text.casefold()
    right_lower = right_text.casefold()
    return (
        any(term in left_lower for term in approvals)
        and any(term in right_lower for term in bypass)
    ) or (
        any(term in right_lower for term in approvals)
        and any(term in left_lower for term in bypass)
    )


def _contains_gap_signal(text: str) -> bool:
    lowered = text.casefold()
    return any(token in lowered for token in ("tbd", "todo", "missing", "not documented", "noch offen", "unklar"))


def _looks_obsolete(text: str) -> bool:
    lowered = text.casefold()
    return any(token in lowered for token in ("deprecated", "legacy", "obsolete", "veraltet", "old path"))


def _follow_up_capabilities(*, source_types: set[str]) -> list[str]:
    capabilities: list[str] = []
    if source_types.intersection({"confluence_page", "local_doc", "metamodel"}):
        capabilities.append("confluence_page_update")
    if source_types.intersection({"github_file", "jira_ticket"}):
        capabilities.append("jira_ticket_create")
    return capabilities


def _dedupe_review_cards(*, review_cards: list[ReviewCard]) -> list[ReviewCard]:
    seen: set[str] = set()
    deduped: list[ReviewCard] = []
    for card in review_cards:
        key = hashlib.sha1(
            f"{card.deviation_type}|{card.title.casefold()}|{card.source_a.casefold()}|{card.source_b.casefold()}".encode("utf-8")
        ).hexdigest()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(card)
    return deduped


def _select_fast_audit_slot(*, settings: Settings) -> int | None:
    for slot in settings.get_configured_llm_slots():
        model_hint = f"{slot.model} {slot.deployment or ''}".casefold()
        if "embedding" in model_hint or "document-ai" in model_hint or "ocr" in model_hint:
            continue
        return int(slot.slot)
    return None


def _merge_usage(*, total: dict, response: object) -> None:
    usage = getattr(response, "usage", {}) or {}
    model = getattr(response, "model", "unknown") or "unknown"
    prompt_tokens = int(usage.get("prompt_tokens") or 0)
    completion_tokens = int(usage.get("completion_tokens") or 0)
    total["total_prompt_tokens"] += prompt_tokens
    total["total_completion_tokens"] += completion_tokens
    by_model = total.setdefault("by_model", {})
    previous = by_model.get(
        model,
        {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "cost_usd": 0.0, "cost_eur": 0.0},
    )
    by_model[model] = {
        "calls": previous["calls"] + 1,
        "prompt_tokens": previous["prompt_tokens"] + prompt_tokens,
        "completion_tokens": previous["completion_tokens"] + completion_tokens,
        "cost_usd": previous["cost_usd"],
        "cost_eur": previous["cost_eur"],
    }
