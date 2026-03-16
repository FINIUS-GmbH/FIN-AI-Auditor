"""Embedding-based cross-document contradiction detection.

Uses embeddings to find semantically similar but contradictory claims across
different document sources (Confluence, Code, Metamodel, local docs).

Quality first: Every contradiction must be found; false positives are acceptable
as they will be reviewed by the auditor.
"""
from __future__ import annotations

import gc
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
SIMILARITY_CHUNK_ROWS = 100


def detect_cross_document_contradictions(
    *,
    settings: Settings,
    claim_records: list[ExtractedClaimRecord],
    allow_remote_embeddings: bool,
    progress_callback: Callable[[str], None] | None = None,
    db_path: str | None = None,
) -> list[AuditFinding]:
    """Find semantically similar but value-conflicting claims across sources.

    Strategy:
    1. Check embedding cache for already-embedded claims (delta optimization)
    2. Embed only NEW claim texts (parallelized API calls)
    3. Store new embeddings in cache for next run
    4. Find high-similarity pairs across different source types (numpy vectorized)
    5. Check if their normalized values conflict
    6. Generate findings for confirmed contradictions
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

    # ── Delta-caching: only embed uncached claims ──
    import hashlib
    text_hashes = [hashlib.sha256(t.encode("utf-8")).hexdigest()[:24] for t in claim_texts]
    cached_embeddings: dict[str, list[float]] = {}
    cache_svc = None

    if db_path:
        from pathlib import Path
        from fin_ai_auditor.services.pipeline_cache_service import PipelineCacheService
        cache_svc = PipelineCacheService(db_path=Path(db_path))
        cached_embeddings = cache_svc.get_cached_embeddings(text_hashes=text_hashes)

    # Identify which texts need embedding
    uncached_indices = [i for i, h in enumerate(text_hashes) if h not in cached_embeddings]
    cached_count = len(claim_texts) - len(uncached_indices)
    total_claims = len(claim_texts)

    if progress_callback:
        progress_callback(f"Embedding-Cache: {cached_count}/{total_claims} Claims aus Cache geladen, {len(uncached_indices)} neu zu berechnen")
    logger.info("embedding_cache_stats", extra={"event_name": "embedding_cache_stats", "event_payload": {"total": total_claims, "cached": cached_count, "to_embed": len(uncached_indices)}})

    # Build the full embedding list — fill cached ones directly
    all_embeddings: list[list[float]] = [[] for _ in claim_texts]
    for i, h in enumerate(text_hashes):
        if h in cached_embeddings:
            all_embeddings[i] = cached_embeddings[h]

    # Embed only uncached claims
    if uncached_indices:
        uncached_texts = [claim_texts[i] for i in uncached_indices]

        from concurrent.futures import ThreadPoolExecutor, as_completed

        batches = [uncached_texts[i:i + BATCH_SIZE] for i in range(0, len(uncached_texts), BATCH_SIZE)]
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
            for future in as_completed(futures):
                batch_idx, embeddings = future.result()
                batch_results[batch_idx] = embeddings
                completed_batches += 1
                embedded_so_far = min(completed_batches * BATCH_SIZE, len(uncached_texts))
                if progress_callback:
                    progress_callback(f"Embedding: {embedded_so_far}/{len(uncached_texts)} neue Claims verarbeitet ({completed_batches}/{len(batches)} Batches)")

        # Reassemble uncached embeddings into full list
        uncached_flat: list[list[float]] = []
        for idx in range(len(batches)):
            uncached_flat.extend(batch_results.get(idx, [[] for _ in batches[idx]]))

        # Store new embeddings in cache
        new_cache_entries: dict[str, list[float]] = {}
        for local_idx, global_idx in enumerate(uncached_indices):
            emb = uncached_flat[local_idx] if local_idx < len(uncached_flat) else []
            all_embeddings[global_idx] = emb
            if emb:  # Only cache non-empty embeddings
                new_cache_entries[text_hashes[global_idx]] = emb

        if cache_svc and new_cache_entries:
            cache_svc.set_cached_embeddings(entries=new_cache_entries)
            if progress_callback:
                progress_callback(f"{len(new_cache_entries)} neue Embeddings im Cache gespeichert")

    if not any(all_embeddings):
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
    source_type_arr = np.array(source_types)
    n = len(valid_indices)

    # Large Python lists are no longer needed once the dense matrix exists.
    del all_embeddings
    del vectors
    del cached_embeddings
    gc.collect()

    # Compute similarities in chunks to control memory (chunk rows, full columns)
    findings: list[AuditFinding] = []
    seen_pairs: set[frozenset[str]] = set()
    chunk_rows = max(25, min(SIMILARITY_CHUNK_ROWS, n))
    total_chunks = math.ceil(n / chunk_rows)

    for chunk_idx, row_start in enumerate(range(0, n, chunk_rows)):
        row_end = min(row_start + chunk_rows, n)
        if progress_callback:
            progress_callback(f"Similarity-Vergleich: Chunk {chunk_idx + 1}/{total_chunks} ({row_start}/{n} Vektoren)")
        sim_block = mat_normed[row_start:row_end] @ mat_normed.T  # (chunk, N)

        # Zero out lower triangle + self to avoid duplicates (only j > global_i)
        for local_i in range(row_end - row_start):
            global_i = row_start + local_i
            sim_block[local_i, :global_i + 1] = 0.0

        # Find ALL above-threshold pairs in this chunk at once (vectorized)
        pairs = np.argwhere(sim_block >= SIMILARITY_MEDIUM)
        if len(pairs) == 0:
            if progress_callback:
                progress_callback(
                    f"Similarity-Vergleich: Chunk {chunk_idx + 1}/{total_chunks} abgeschlossen "
                    f"({row_end}/{n} Vektoren, {len(findings)} Findings)"
                )
            del sim_block
            gc.collect()
            continue

        # Vectorized cross-source filter
        global_is = pairs[:, 0] + row_start
        global_js = pairs[:, 1]
        src_i = source_type_arr[global_is]
        src_j = source_type_arr[global_js]
        cross_mask = src_i != src_j

        cross_pairs = pairs[cross_mask]
        cross_global_is = global_is[cross_mask]
        cross_global_js = global_js[cross_mask]

        for k in range(len(cross_pairs)):
            gi = int(cross_global_is[k])
            gj = int(cross_global_js[k])
            sim_val = float(sim_block[cross_pairs[k, 0], cross_pairs[k, 1]])

            vi = valid_indices[gi]
            vj = valid_indices[gj]
            rec_i = eligible[vi]
            rec_j = eligible[vj]
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

        if progress_callback:
            progress_callback(
                f"Similarity-Vergleich: Chunk {chunk_idx + 1}/{total_chunks} abgeschlossen "
                f"({row_end}/{n} Vektoren, {len(findings)} Findings)"
            )
        del pairs
        del cross_pairs
        del cross_global_is
        del cross_global_js
        del sim_block
        gc.collect()

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
