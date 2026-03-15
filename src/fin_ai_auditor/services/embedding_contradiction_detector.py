"""Embedding-based cross-document contradiction detection.

Uses embeddings to find semantically similar but contradictory claims across
different document sources (Confluence, Code, Metamodel, local docs).

Quality first: Every contradiction must be found; false positives are acceptable
as they will be reviewed by the auditor.
"""
from __future__ import annotations

import logging
import math
from typing import Callable, Sequence

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import (
    AuditFinding,
    AuditLocation,
    AuditPosition,
    new_location_id,
)
from fin_ai_auditor.llm import get_embeddings_from_llm_slot, select_embedding_slot
from fin_ai_auditor.services.pipeline_models import ExtractedClaimRecord

logger = logging.getLogger(__name__)

# Similarity thresholds
SIMILARITY_HIGH = 0.85     # Very similar claims — likely about the same thing
SIMILARITY_MEDIUM = 0.70   # Moderately similar — worth checking
BATCH_SIZE = 200           # Embeddings per API call
EMBED_PARALLELISM = 3      # Concurrent embedding API calls (reduced to avoid rate limits)


def detect_cross_document_contradictions(
    *,
    settings: Settings,
    claim_records: list[ExtractedClaimRecord],
    allow_remote_embeddings: bool,
    progress_callback: Callable[[str], None] | None = None,
) -> list[AuditFinding]:
    """Find semantically similar but value-conflicting claims across sources.

    Strategy:
    1. Embed all claim texts (parallelized API calls)
    2. Find high-similarity pairs across different source types (numpy vectorized)
    3. Check if their normalized values conflict
    4. Generate findings for confirmed contradictions
    """
    if not allow_remote_embeddings:
        logger.info("embedding_contradiction_skipped", extra={
            "event_name": "embedding_contradiction_skipped",
            "event_payload": {"reason": "remote_calls_disabled"},
        })
        return []

    # Filter to claims with meaningful text
    eligible = [
        r for r in claim_records
        if r.evidence.matched_text and len(str(r.evidence.matched_text).strip()) > 10
    ]
    if len(eligible) < 2:
        return []

    # Select embedding slot
    embedding_slot = _find_embedding_slot(settings=settings)
    if embedding_slot is None:
        logger.info("no_embedding_slot_available")
        return []

    try:
        client = get_embeddings_from_llm_slot(settings=settings, llm_slot=embedding_slot)
    except Exception as exc:
        logger.warning("embedding_client_init_failed", extra={"event_payload": {"error": str(exc)}})
        return []

    # Build claim texts for embedding
    claim_texts = [
        f"{r.claim.subject_key}: {r.claim.predicate} = {r.claim.normalized_value} "
        f"({str(r.evidence.matched_text or '')[:200]})"
        for r in eligible
    ]

    # Embed in parallel batches
    from concurrent.futures import ThreadPoolExecutor, as_completed

    batches = [claim_texts[i:i + BATCH_SIZE] for i in range(0, len(claim_texts), BATCH_SIZE)]
    batch_results: dict[int, list[list[float]]] = {}

    def _embed_batch(batch_idx: int, batch: list[str]) -> tuple[int, list[list[float]]]:
        try:
            return batch_idx, client.embed_documents(batch)
        except Exception as exc:
            logger.warning("embedding_batch_failed", extra={"event_payload": {"batch": batch_idx, "error": str(exc)}})
            return batch_idx, [[] for _ in batch]

    with ThreadPoolExecutor(max_workers=EMBED_PARALLELISM) as pool:
        futures = {pool.submit(_embed_batch, idx, batch): idx for idx, batch in enumerate(batches)}
        completed_batches = 0
        total_claims = len(claim_texts)
        for future in as_completed(futures):
            batch_idx, embeddings = future.result()
            batch_results[batch_idx] = embeddings
            completed_batches += 1
            embedded_so_far = min(completed_batches * BATCH_SIZE, total_claims)
            if progress_callback:
                progress_callback(f"Embedding: {embedded_so_far}/{total_claims} Claims verarbeitet ({completed_batches}/{len(batches)} Batches)")

    # Reassemble in order
    all_embeddings: list[list[float]] = []
    for idx in range(len(batches)):
        all_embeddings.extend(batch_results.get(idx, [[] for _ in batches[idx]]))

    if not all_embeddings or not any(all_embeddings):
        return []

    logger.info(
        "embedding_contradiction_search",
        extra={
            "event_name": "embedding_contradiction_search",
            "event_payload": {"claims_embedded": len(all_embeddings)},
        },
    )

    # ── Vectorized similarity search using numpy ──
    import numpy as np

    # Build embedding matrix, skipping empty vectors
    dim = max(len(e) for e in all_embeddings if e)
    valid_indices: list[int] = []
    vectors: list[list[float]] = []
    for idx, emb in enumerate(all_embeddings):
        if emb and len(emb) == dim:
            valid_indices.append(idx)
            vectors.append(emb)

    if len(valid_indices) < 2:
        return []

    mat = np.array(vectors, dtype=np.float32)  # (N, D)
    # L2 normalize rows for cosine similarity via dot product
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    mat_normed = mat / norms

    # Build source type array for cross-source filtering
    source_types = [eligible[vi].claim.source_type for vi in valid_indices]
    n = len(valid_indices)

    # Compute similarities in chunks to control memory (chunk rows, full columns)
    findings: list[AuditFinding] = []
    seen_pairs: set[frozenset[str]] = set()
    chunk_rows = 500  # process 500 rows at a time
    total_chunks = math.ceil(n / chunk_rows)

    for chunk_idx, row_start in enumerate(range(0, n, chunk_rows)):
        row_end = min(row_start + chunk_rows, n)
        if progress_callback:
            progress_callback(f"Similarity-Vergleich: Chunk {chunk_idx + 1}/{total_chunks} ({row_start}/{n} Vektoren)")
        sim_block = mat_normed[row_start:row_end] @ mat_normed.T  # (chunk, N)

        # Find pairs above threshold
        for local_i in range(row_end - row_start):
            global_i = row_start + local_i
            # Only check j > global_i to avoid duplicates
            start_j = global_i + 1
            if start_j >= n:
                continue
            sims = sim_block[local_i, start_j:]

            # Find indices above SIMILARITY_MEDIUM
            above = np.where(sims >= SIMILARITY_MEDIUM)[0]
            for offset in above:
                global_j = start_j + int(offset)
                sim_val = float(sims[offset])

                vi = valid_indices[global_i]
                vj = valid_indices[global_j]
                rec_i = eligible[vi]
                rec_j = eligible[vj]

                # Must be from different source types
                if source_types[global_i] == source_types[global_j]:
                    continue

                # Must have different normalized values
                if rec_i.claim.normalized_value.strip().casefold() == rec_j.claim.normalized_value.strip().casefold():
                    continue

                # Deduplicate
                pair_key = frozenset([rec_i.claim.claim_id, rec_j.claim.claim_id])
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)

                severity = "high" if sim_val >= SIMILARITY_HIGH else "medium"
                src_label_i = _SOURCE_LABELS.get(rec_i.claim.source_type, rec_i.claim.source_type)
                src_label_j = _SOURCE_LABELS.get(rec_j.claim.source_type, rec_j.claim.source_type)
                text_i = str(rec_i.evidence.matched_text or "")[:120]
                text_j = str(rec_j.evidence.matched_text or "")[:120]

                finding = AuditFinding(
                    severity=severity,
                    category="contradiction",
                    title=f"Semantischer Widerspruch: {src_label_i} vs. {src_label_j}",
                    summary=(
                        f"Zwei inhaltlich verwandte Aussagen ({int(sim_val * 100)}% semantische Aehnlichkeit) "
                        f"widersprechen sich zwischen {src_label_i} und {src_label_j}.\n\n"
                        f"{src_label_i}: \u00ab{text_i}\u00bb\n"
                        f"{src_label_j}: \u00ab{text_j}\u00bb"
                    ),
                    recommendation=(
                        f"Die Aussagen in {src_label_i} und {src_label_j} zum Thema "
                        f"'{rec_i.claim.subject_key}' muessen konsolidiert werden. "
                        f"Pruefen welche Quelle die fuehrende ist und die andere anpassen."
                    ),
                    canonical_key=f"embedding_contradiction|{rec_i.claim.subject_key}|{rec_j.claim.subject_key}",
                    locations=[
                        loc for loc in [rec_i.evidence.location, rec_j.evidence.location] if loc is not None
                    ],
                    metadata={
                        "generated_by": "embedding_contradiction_detector",
                        "similarity_score": round(sim_val, 3),
                        "source_types": [rec_i.claim.source_type, rec_j.claim.source_type],
                        "subject_keys": [rec_i.claim.subject_key, rec_j.claim.subject_key],
                    },
                )
                findings.append(finding)

    logger.info(
        "embedding_contradictions_found",
        extra={
            "event_name": "embedding_contradictions_found",
            "event_payload": {"count": len(findings), "pairs_checked": len(seen_pairs)},
        },
    )
    return findings


_SOURCE_LABELS: dict[str, str] = {
    "github_file": "Code",
    "confluence_page": "Confluence",
    "local_doc": "Lokales Dokument",
    "metamodel": "Metamodell",
}


def _find_embedding_slot(*, settings: Settings) -> int | None:
    """Find the slot used consistently for embedding-backed analysis."""
    return select_embedding_slot(settings=settings)

