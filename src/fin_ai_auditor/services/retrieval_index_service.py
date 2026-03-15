from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from typing import Callable
from dataclasses import dataclass

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import AuditFinding, RetrievalSegment, RetrievalSegmentClaimLink
from fin_ai_auditor.llm import get_embeddings_from_llm_slot, select_embedding_slot
from fin_ai_auditor.services.pipeline_models import CollectedDocument, ExtractedClaimRecord


_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_]{3,}")
_STOPWORDS = {
    "the",
    "and",
    "oder",
    "und",
    "der",
    "die",
    "das",
    "eine",
    "einer",
    "eines",
    "with",
    "from",
    "fuer",
    "werden",
    "wird",
    "auch",
    "eine",
    "this",
    "that",
    "into",
    "only",
    "oder",
    "path",
    "scope",
}
_HEADING_PATTERN = re.compile(r"^\s{0,3}(?P<hashes>#{1,6})\s+(?P<title>.+?)\s*$")
_CODE_BLOCK_START = re.compile(r"^\s*(class|def)\s+[A-Za-z0-9_]+|^\s*@router\.(get|post|put|patch|delete)\(")
_PHASE_SEGMENT_LIMIT = 1200


@dataclass(frozen=True)
class RetrievalIndexBuildResult:
    segments: list[RetrievalSegment]
    claim_links: list[RetrievalSegmentClaimLink]
    notes: list[str]


def build_retrieval_index(
    *,
    settings: Settings,
    run_id: str,
    documents: list[CollectedDocument],
    claim_records: list[ExtractedClaimRecord],
    previous_segments: list[RetrievalSegment],
    allow_remote_embeddings: bool,
) -> RetrievalIndexBuildResult:
    raw_segments: list[RetrievalSegment] = []
    for document in documents:
        raw_segments.extend(_segment_document(run_id=run_id, document=document))

    segments = _annotate_segment_deltas(segments=raw_segments, previous_segments=previous_segments)
    embedding_notes: list[str] = []
    if allow_remote_embeddings and segments:
        segments, embedding_notes = _attach_embeddings(settings=settings, segments=segments)
    else:
        embedding_notes.append("Retrieval-Embeddings wurden fuer diesen Lauf nicht remote berechnet.")

    claim_links = _link_claims_to_segments(segments=segments, claim_records=claim_records)
    notes = [
        f"Retrieval-Index aufgebaut: {len(segments)} Segmente, {len(claim_links)} Claim-Verknuepfungen.",
        *_build_delta_notes(segments=segments),
        *embedding_notes,
    ]
    return RetrievalIndexBuildResult(segments=segments, claim_links=claim_links, notes=notes)


def build_recommendation_contexts(
    *,
    settings: Settings,
    findings: list[AuditFinding],
    segments: list[RetrievalSegment],
    allow_remote_embeddings: bool,
    lexical_search: Callable[[str, int], list[tuple[str, float]]] | None = None,
    limit_per_finding: int = 3,
    max_findings: int = 12,
) -> dict[str, list[str]]:
    target_findings = findings[: max(1, int(max_findings))]
    if not target_findings or not segments:
        return {}

    query_vectors: dict[str, list[float]] = {}
    local_fts_connection = _build_local_fts_connection(segments=segments)
    if allow_remote_embeddings:
        slot = _select_embedding_slot(settings=settings)
        if slot is not None:
            try:
                embedder = get_embeddings_from_llm_slot(settings=settings, llm_slot=int(slot))
                for finding in target_findings:
                    query = _query_text_for_finding(finding=finding)
                    if query:
                        query_vectors[_finding_key(finding)] = embedder.embed_query(query)
            except Exception:
                query_vectors = {}

    contexts: dict[str, list[str]] = {}
    for finding in target_findings:
        query_text = _query_text_for_finding(finding=finding)
        query_keywords = _keywords(query_text, limit=10)
        lexical_hits = (
            lexical_search(query_text, 14)
            if lexical_search is not None and query_text
            else _search_local_fts(connection=local_fts_connection, query_text=query_text, limit=14)
        )
        lexical_scores = {segment_id: score for segment_id, score in lexical_hits}
        ranked = sorted(
            (
                (
                    _segment_score(
                        segment=segment,
                        finding=finding,
                        query_keywords=query_keywords,
                        query_vectors=query_vectors,
                        lexical_scores=lexical_scores,
                    ),
                    segment,
                )
                for segment in segments
            ),
            key=lambda item: item[0],
            reverse=True,
        )
        snippets: list[str] = []
        for score, segment in ranked:
            if score <= 0.0:
                continue
            snippets.append(
                f"{segment.source_type}:{segment.title}:{segment.anchor_value} :: {_truncate(segment.content, limit=260)}"
            )
            if len(snippets) >= int(limit_per_finding):
                break
        if snippets:
            contexts[_finding_key(finding)] = snippets
    if local_fts_connection is not None:
        local_fts_connection.close()
    return contexts


def attach_retrieval_context_to_findings(
    *,
    findings: list[AuditFinding],
    contexts: dict[str, list[str]],
) -> list[AuditFinding]:
    enriched: list[AuditFinding] = []
    for finding in findings:
        retrieval_context = contexts.get(_finding_key(finding), [])
        enriched.append(
            finding.model_copy(
                update={
                    "metadata": {
                        **finding.metadata,
                        "retrieval_context": retrieval_context,
                    }
                }
            )
        )
    return enriched


def attach_retrieval_insights_to_findings(
    *,
    findings: list[AuditFinding],
    segments: list[RetrievalSegment],
) -> list[AuditFinding]:
    enriched: list[AuditFinding] = []
    for finding in findings:
        matched_segments = _match_segments_for_finding(finding=finding, segments=segments)
        delta_statuses = sorted({segment.delta_status for segment in matched_segments if segment.delta_status != "unchanged"})
        delta_reasons = [
            f"{segment.delta_status}: {segment.source_type}:{segment.title}:{segment.anchor_value}"
            for segment in matched_segments
            if segment.delta_status != "unchanged"
        ]
        delta_summary: list[str] = []
        if matched_segments:
            delta_summary.append(f"{len(matched_segments)} Retrieval-Segmente sind direkt mit dem Problemelement verknuepft.")
        if delta_reasons:
            delta_summary.append(
                f"{sum(1 for segment in matched_segments if segment.delta_status == 'changed')} geaenderte und "
                f"{sum(1 for segment in matched_segments if segment.delta_status == 'added')} neue Segmente beeinflussen die Neubewertung."
            )
        metadata = {
            **finding.metadata,
            "retrieval_anchor_values": [segment.anchor_value for segment in matched_segments[:6]],
            "retrieval_segment_count": len(matched_segments),
            "delta_statuses": delta_statuses,
            "delta_reasons": delta_reasons[:6],
            "delta_summary": delta_summary,
            "delta_signal": delta_statuses[0] if len(delta_statuses) == 1 else ("mixed" if delta_statuses else "stable"),
        }
        enriched.append(finding.model_copy(update={"metadata": metadata}))
    return enriched


def _segment_document(*, run_id: str, document: CollectedDocument) -> list[RetrievalSegment]:
    if document.source_type == "github_file":
        return _segment_code_document(run_id=run_id, document=document)
    if document.source_type == "metamodel":
        return _segment_metamodel_document(run_id=run_id, document=document)
    return _segment_text_document(run_id=run_id, document=document)


def _match_segments_for_finding(
    *,
    finding: AuditFinding,
    segments: list[RetrievalSegment],
) -> list[RetrievalSegment]:
    by_exact_anchor: list[RetrievalSegment] = []
    by_source_match: list[RetrievalSegment] = []
    for location in finding.locations:
        for segment in segments:
            if segment.source_type != location.source_type or segment.source_id != location.source_id:
                continue
            if _segment_matches_location(segment=segment, location=location):
                by_exact_anchor.append(segment)
            else:
                by_source_match.append(segment)
    deduped_exact = _dedupe_segments(by_exact_anchor)
    if deduped_exact:
        return deduped_exact[:6]
    return _dedupe_segments(by_source_match)[:4]


def _segment_matches_location(*, segment: RetrievalSegment, location: object) -> bool:
    position = getattr(location, "position", None)
    if position is not None and segment.anchor_value == position.anchor_value:
        return True
    section_path = getattr(position, "section_path", None) if position is not None else None
    if section_path and segment.section_path and section_path == segment.section_path:
        return True
    location_path_hint = str(getattr(location, "path_hint", "") or "").strip()
    return bool(location_path_hint and segment.path_hint and location_path_hint == segment.path_hint)


def _dedupe_segments(segments: list[RetrievalSegment]) -> list[RetrievalSegment]:
    seen: set[str] = set()
    deduped: list[RetrievalSegment] = []
    for segment in segments:
        if segment.segment_id in seen:
            continue
        seen.add(segment.segment_id)
        deduped.append(segment)
    return deduped


def _segment_code_document(*, run_id: str, document: CollectedDocument) -> list[RetrievalSegment]:
    lines = document.body.splitlines()
    segments: list[RetrievalSegment] = []
    block_lines: list[str] = []
    block_start = 1
    current_section = document.title
    ordinal = 0
    for line_no, raw_line in enumerate(lines, start=1):
        if _CODE_BLOCK_START.match(raw_line) and block_lines:
            segment = _build_segment(
                run_id=run_id,
                document=document,
                ordinal=ordinal,
                anchor_kind="file_line_range",
                anchor_value=f"{document.source_id}#L{block_start}-L{line_no - 1}",
                section_path=current_section,
                content="\n".join(block_lines).strip(),
                metadata={"line_start": block_start, "line_end": line_no - 1},
            )
            if segment is not None:
                segments.append(segment)
                ordinal += 1
            block_lines = []
            block_start = line_no
        stripped = raw_line.strip()
        if stripped:
            if _CODE_BLOCK_START.match(raw_line):
                current_section = stripped.split("(", 1)[0][:120]
            block_lines.append(raw_line)
        elif block_lines and len(block_lines) >= 14:
            segment = _build_segment(
                run_id=run_id,
                document=document,
                ordinal=ordinal,
                anchor_kind="file_line_range",
                anchor_value=f"{document.source_id}#L{block_start}-L{line_no}",
                section_path=current_section,
                content="\n".join(block_lines).strip(),
                metadata={"line_start": block_start, "line_end": line_no},
            )
            if segment is not None:
                segments.append(segment)
                ordinal += 1
            block_lines = []
            block_start = line_no + 1
    if block_lines:
        segment = _build_segment(
            run_id=run_id,
            document=document,
            ordinal=ordinal,
            anchor_kind="file_line_range",
            anchor_value=f"{document.source_id}#L{block_start}-L{len(lines)}",
            section_path=current_section,
            content="\n".join(block_lines).strip(),
            metadata={"line_start": block_start, "line_end": len(lines)},
        )
        if segment is not None:
            segments.append(segment)
    return segments


def _segment_text_document(*, run_id: str, document: CollectedDocument) -> list[RetrievalSegment]:
    segments: list[RetrievalSegment] = []
    ordinal = 0
    base_context = _document_base_context(document=document)
    heading_stack: list[str] = []
    current_heading = _document_section_path(base_context=base_context, heading_stack=heading_stack)
    buffer: list[str] = []
    for raw_line in document.body.splitlines():
        heading_match = _HEADING_PATTERN.match(raw_line)
        if heading_match is not None and buffer:
            segment = _build_segment(
                run_id=run_id,
                document=document,
                ordinal=ordinal,
                anchor_kind="document_section",
                anchor_value=f"{document.source_id}#section:{_slugify(current_heading)}:{ordinal}",
                section_path=current_heading,
                content="\n".join(buffer).strip(),
                metadata=_text_segment_metadata(
                    document=document,
                    section_path=current_heading,
                    heading_depth=len(heading_stack),
                ),
            )
            if segment is not None:
                segments.append(segment)
                ordinal += 1
            buffer = []
        if heading_match is not None:
            heading_stack = _updated_heading_stack(
                stack=heading_stack,
                level=len(heading_match.group("hashes")),
                title=heading_match.group("title").strip(),
            )
            current_heading = _document_section_path(base_context=base_context, heading_stack=heading_stack)
            continue
        stripped = raw_line.strip()
        if not stripped:
            if sum(len(item) for item in buffer) >= 700:
                segment = _build_segment(
                    run_id=run_id,
                    document=document,
                    ordinal=ordinal,
                    anchor_kind="document_section",
                    anchor_value=f"{document.source_id}#section:{_slugify(current_heading)}:{ordinal}",
                    section_path=current_heading,
                    content="\n".join(buffer).strip(),
                    metadata=_text_segment_metadata(
                        document=document,
                        section_path=current_heading,
                        heading_depth=len(heading_stack),
                    ),
                )
                if segment is not None:
                    segments.append(segment)
                    ordinal += 1
                buffer = []
            continue
        buffer.append(stripped)
    if buffer:
        segment = _build_segment(
            run_id=run_id,
            document=document,
            ordinal=ordinal,
            anchor_kind="document_section",
            anchor_value=f"{document.source_id}#section:{_slugify(current_heading)}:{ordinal}",
            section_path=current_heading,
            content="\n".join(buffer).strip(),
            metadata=_text_segment_metadata(
                document=document,
                section_path=current_heading,
                heading_depth=len(heading_stack),
            ),
        )
        if segment is not None:
            segments.append(segment)
    return segments


def _segment_metamodel_document(*, run_id: str, document: CollectedDocument) -> list[RetrievalSegment]:
    segments: list[RetrievalSegment] = []
    try:
        rows = json.loads(document.body)
    except ValueError:
        rows = []
    if not isinstance(rows, list):
        rows = []
    summary_segment = _build_segment(
        run_id=run_id,
        document=document,
        ordinal=0,
        anchor_kind="metamodel_summary",
        anchor_value="metamodel:summary",
        section_path="current_dump",
        content=f"Metamodell-Dump mit {len(rows)} Phasen.",
        metadata={"row_count": len(rows)},
    )
    if summary_segment is not None:
        segments.append(summary_segment)
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        phase_id = str(row.get("phase_id") or row.get("phase_public_id") or row.get("id") or f"phase_{index}")
        phase_name = str(row.get("phase_name") or row.get("name") or phase_id)
        content = json.dumps(row, ensure_ascii=False, sort_keys=True)
        segment = _build_segment(
            run_id=run_id,
            document=document,
            ordinal=index,
            anchor_kind="metamodel_phase",
            anchor_value=f"metamodel:{phase_id}",
            section_path=phase_name,
            content=content[:_PHASE_SEGMENT_LIMIT],
            metadata={"phase_id": phase_id, "phase_name": phase_name},
        )
        if segment is not None:
            segments.append(segment)
    return segments


def _build_segment(
    *,
    run_id: str,
    document: CollectedDocument,
    ordinal: int,
    anchor_kind: str,
    anchor_value: str,
    section_path: str | None,
    content: str,
    metadata: dict[str, object],
) -> RetrievalSegment | None:
    cleaned = content.strip()
    if len(cleaned) < 16:
        return None
    segment_hash = _sha256_text(cleaned)
    keyword_source = " ".join(
        part
        for part in (
            section_path or "",
            " ".join(_metadata_context_tokens(metadata=metadata)),
            cleaned,
        )
        if part
    )
    return RetrievalSegment(
        run_id=run_id,
        snapshot_id=document.snapshot.snapshot_id,
        source_type=document.source_type,
        source_id=document.source_id,
        title=document.title,
        path_hint=document.path_hint,
        url=document.url,
        anchor_kind=anchor_kind,
        anchor_value=anchor_value,
        section_path=section_path,
        ordinal=int(ordinal),
        content=cleaned,
        content_hash=document.snapshot.content_hash,
        segment_hash=segment_hash,
        token_count=len(cleaned.split()),
        keywords=_keywords(keyword_source, limit=24),
        metadata=metadata,
    )


def _annotate_segment_deltas(
    *,
    segments: list[RetrievalSegment],
    previous_segments: list[RetrievalSegment],
) -> list[RetrievalSegment]:
    previous_map = {
        (segment.source_type, segment.source_id, segment.anchor_value): segment for segment in previous_segments
    }
    out: list[RetrievalSegment] = []
    for segment in segments:
        previous = previous_map.get((segment.source_type, segment.source_id, segment.anchor_value))
        delta_status = "added"
        if previous is not None:
            delta_status = "unchanged" if previous.segment_hash == segment.segment_hash else "changed"
        out.append(segment.model_copy(update={"delta_status": delta_status}))
    return out


def _attach_embeddings(
    *,
    settings: Settings,
    segments: list[RetrievalSegment],
) -> tuple[list[RetrievalSegment], list[str]]:
    slot = _select_embedding_slot(settings=settings)
    if slot is None:
        return segments, ["Kein Embedding-faehiger LLM-Slot fuer den Retrieval-Index konfiguriert."]
    try:
        embedder = get_embeddings_from_llm_slot(settings=settings, llm_slot=int(slot))
        texts = [segment.content for segment in segments]
        embeddings = _embed_in_batches(embedder=embedder, texts=texts, batch_size=24)
    except Exception as exc:
        return segments, [f"Retrieval-Embeddings konnten nicht berechnet werden: {type(exc).__name__}: {exc}"]

    enriched: list[RetrievalSegment] = []
    embedded_count = 0
    for segment, vector in zip(segments, embeddings, strict=False):
        embedding = vector if vector else None
        if embedding:
            embedded_count += 1
        enriched.append(segment.model_copy(update={"embedding": embedding}))
    return enriched, [f"Retrieval-Embeddings wurden fuer {embedded_count} Segmente berechnet (Slot {slot})."]


def _embed_in_batches(*, embedder: object, texts: list[str], batch_size: int) -> list[list[float]]:
    vectors: list[list[float]] = []
    for start in range(0, len(texts), max(1, batch_size)):
        batch = texts[start : start + max(1, batch_size)]
        batch_vectors = embedder.embed_documents(batch)
        vectors.extend(batch_vectors)
    return vectors


def _select_embedding_slot(*, settings: Settings) -> int | None:
    return select_embedding_slot(settings=settings)


def _link_claims_to_segments(
    *,
    segments: list[RetrievalSegment],
    claim_records: list[ExtractedClaimRecord],
) -> list[RetrievalSegmentClaimLink]:
    by_source: dict[tuple[str, str], list[RetrievalSegment]] = {}
    for segment in segments:
        by_source.setdefault((segment.source_type, segment.source_id), []).append(segment)

    links: dict[tuple[str, str, str], RetrievalSegmentClaimLink] = {}
    for record in claim_records:
        location = record.evidence.location
        candidates = by_source.get((location.source_type, location.source_id), [])
        for segment in candidates:
            score = _claim_segment_score(segment=segment, record=record)
            if score <= 0.0:
                continue
            relation_type = "evidence" if score >= 0.95 else "scope_match"
            key = (segment.segment_id, record.claim.claim_id, relation_type)
            existing = links.get(key)
            if existing is None or existing.score < score:
                links[key] = RetrievalSegmentClaimLink(
                    segment_id=segment.segment_id,
                    claim_id=record.claim.claim_id,
                    relation_type=relation_type,
                    score=score,
                    metadata={"subject_key": record.claim.subject_key, "predicate": record.claim.predicate},
                )
    return list(links.values())


def _claim_segment_score(*, segment: RetrievalSegment, record: ExtractedClaimRecord) -> float:
    location = record.evidence.location
    if location.position is not None:
        line_start = location.position.line_start
        if line_start is not None:
            seg_start = int(segment.metadata.get("line_start", -1) or -1)
            seg_end = int(segment.metadata.get("line_end", -1) or -1)
            if seg_start > 0 and seg_end >= seg_start and seg_start <= int(line_start) <= seg_end:
                return 1.0
        if location.position.section_path and segment.section_path:
            if str(location.position.section_path).strip().casefold() == str(segment.section_path).strip().casefold():
                return 0.96
    overlap = _keyword_overlap(
        left=_keywords(record.claim.subject_key, limit=8),
        right=segment.keywords,
    )
    if overlap > 0.0:
        return min(0.9, 0.45 + overlap)
    return 0.0


def _build_delta_notes(*, segments: list[RetrievalSegment]) -> list[str]:
    if not segments:
        return ["Retrieval-Index enthaelt fuer diesen Lauf noch keine Segmente."]
    added = sum(1 for segment in segments if segment.delta_status == "added")
    changed = sum(1 for segment in segments if segment.delta_status == "changed")
    unchanged = sum(1 for segment in segments if segment.delta_status == "unchanged")
    return [
        f"Retrieval-Segment-Delta: {added} neu, {changed} geaendert, {unchanged} unveraendert.",
    ]


def _segment_score(
    *,
    segment: RetrievalSegment,
    finding: AuditFinding,
    query_keywords: list[str],
    query_vectors: dict[str, list[float]],
    lexical_scores: dict[str, float],
) -> float:
    score = 0.0
    if _finding_has_direct_segment_match(finding=finding, segment=segment):
        score += 1.2
    score += lexical_scores.get(segment.segment_id, 0.0) * 1.1
    score += _keyword_overlap(left=query_keywords, right=segment.keywords)
    query_vector = query_vectors.get(_finding_key(finding))
    if query_vector and segment.embedding:
        score += max(0.0, _cosine(query_vector, segment.embedding))
    return score


def _finding_has_direct_segment_match(*, finding: AuditFinding, segment: RetrievalSegment) -> bool:
    for location in finding.locations:
        if location.source_type != segment.source_type or location.source_id != segment.source_id:
            continue
        if location.position is None:
            continue
        if location.position.anchor_value == segment.anchor_value:
            return True
        if location.position.line_start is not None:
            seg_start = int(segment.metadata.get("line_start", -1) or -1)
            seg_end = int(segment.metadata.get("line_end", -1) or -1)
            if seg_start > 0 and seg_end >= seg_start and seg_start <= int(location.position.line_start) <= seg_end:
                return True
        if location.position.section_path and segment.section_path:
            if str(location.position.section_path).strip().casefold() == str(segment.section_path).strip().casefold():
                return True
    return False


def _query_text_for_finding(*, finding: AuditFinding) -> str:
    return " ".join(
        part
        for part in (
            str(finding.canonical_key or "").strip(),
            str(finding.title or "").strip(),
            str(finding.summary or "").strip(),
            " ".join(_metadata_string_list(finding.metadata.get("semantic_context"))[:4]),
            " ".join(_metadata_string_list(finding.metadata.get("semantic_relation_summaries"))[:3]),
            " ".join(_metadata_string_list(finding.metadata.get("semantic_contract_paths"))[:3]),
            " ".join(_metadata_string_list(finding.metadata.get("semantic_section_paths"))[:3]),
        )
        if part
    )


def _keyword_overlap(*, left: list[str], right: list[str]) -> float:
    left_set = set(left)
    right_set = set(right)
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / max(1, len(left_set))


def _keywords(text: str, *, limit: int) -> list[str]:
    seen: list[str] = []
    for token in _TOKEN_PATTERN.findall(str(text or "").casefold()):
        if token in _STOPWORDS:
            continue
        if token not in seen:
            seen.append(token)
        if len(seen) >= max(1, limit):
            break
    return seen


def _document_base_context(*, document: CollectedDocument) -> list[str]:
    ancestor_titles = document.metadata.get("ancestor_titles")
    normalized_ancestors = (
        [str(item).strip() for item in ancestor_titles if str(item).strip()]
        if isinstance(ancestor_titles, list)
        else []
    )
    return [*normalized_ancestors, document.title]


def _updated_heading_stack(*, stack: list[str], level: int, title: str) -> list[str]:
    normalized_level = max(1, int(level))
    trimmed = list(stack[: normalized_level - 1])
    return [*trimmed, title]


def _document_section_path(*, base_context: list[str], heading_stack: list[str]) -> str:
    return " > ".join(
        str(item).strip()
        for item in [*base_context, *heading_stack]
        if str(item).strip()
    )


def _metadata_context_tokens(*, metadata: dict[str, object]) -> list[str]:
    tokens: list[str] = []
    for key in ("heading_level", "heading_path", "space_key"):
        value = metadata.get(key)
        if value is not None:
            tokens.append(str(value))
    ancestor_titles = metadata.get("ancestor_titles")
    if isinstance(ancestor_titles, list):
        tokens.extend(str(item) for item in ancestor_titles if str(item).strip())
    return tokens


def _text_segment_metadata(
    *,
    document: CollectedDocument,
    section_path: str,
    heading_depth: int,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "heading_path": section_path,
        "heading_level": heading_depth,
    }
    ancestor_titles = document.metadata.get("ancestor_titles")
    if isinstance(ancestor_titles, list):
        metadata["ancestor_titles"] = [str(item).strip() for item in ancestor_titles if str(item).strip()]
    if "space_key" in document.metadata:
        metadata["space_key"] = document.metadata.get("space_key")
    return metadata


def _cosine(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    dot = 0.0
    left_norm = 0.0
    right_norm = 0.0
    for left_value, right_value in zip(left, right, strict=False):
        dot += float(left_value) * float(right_value)
        left_norm += float(left_value) * float(left_value)
        right_norm += float(right_value) * float(right_value)
    if left_norm <= 0.0 or right_norm <= 0.0:
        return 0.0
    return dot / math.sqrt(left_norm * right_norm)


def _sha256_text(value: str) -> str:
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _slugify(value: str) -> str:
    tokens = _TOKEN_PATTERN.findall(str(value or "").casefold())
    if not tokens:
        return "section"
    return "-".join(tokens[:6])


def _truncate(value: str, *, limit: int) -> str:
    compact = " ".join(str(value or "").split())
    if len(compact) <= max(16, limit):
        return compact
    return f"{compact[: max(16, limit) - 3]}..."


def _finding_key(finding: AuditFinding) -> str:
    return str(finding.canonical_key or finding.finding_id)


def _metadata_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _build_local_fts_connection(*, segments: list[RetrievalSegment]) -> sqlite3.Connection | None:
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.execute(
            """
            CREATE VIRTUAL TABLE segments_fts USING fts5(
                segment_id UNINDEXED,
                title,
                section_path,
                keywords,
                content,
                tokenize = 'unicode61'
            )
            """
        )
        for segment in segments:
            connection.execute(
                """
                INSERT INTO segments_fts(segment_id, title, section_path, keywords, content)
                VALUES(?, ?, ?, ?, ?)
                """,
                (
                    segment.segment_id,
                    segment.title,
                    segment.section_path,
                    " ".join(segment.keywords),
                    segment.content,
                ),
            )
        return connection
    except sqlite3.Error:
        try:
            if connection is not None:
                connection.close()
        except Exception:
            pass
        return None


def _search_local_fts(
    *,
    connection: sqlite3.Connection | None,
    query_text: str,
    limit: int,
) -> list[tuple[str, float]]:
    if connection is None:
        return []
    fts_query = _fts_query_from_text(query_text)
    if not fts_query:
        return []
    try:
        rows = connection.execute(
            """
            SELECT segment_id, bm25(segments_fts, 6.0, 4.0, 2.0, 1.0) AS rank
            FROM segments_fts
            WHERE segments_fts MATCH ?
            ORDER BY rank ASC
            LIMIT ?
            """,
            (fts_query, max(1, int(limit))),
        ).fetchall()
    except sqlite3.Error:
        return []
    return [
        (str(row["segment_id"]), _normalize_bm25_score(float(row["rank"])))
        for row in rows
    ]


def _fts_query_from_text(query_text: str) -> str:
    tokens = _keywords(query_text, limit=8)
    return " OR ".join(f'"{token}"' for token in tokens)


def _normalize_bm25_score(rank: float) -> float:
    value = abs(float(rank))
    return 1.0 / (1.0 + value)
