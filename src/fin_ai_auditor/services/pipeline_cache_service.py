"""Pipeline-level caching for context summaries and LLM responses.

Three cache types:
 - repo_summary: Cached per content hash of all code files
 - llm_response: Cached per finding fingerprint + context hash
 - context_summary: Cached per content hash of collected documents
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Sequence

from fin_ai_auditor.domain.models import AuditFinding, utc_now_iso
from fin_ai_auditor.services.audit_database import connect_database, ensure_schema
from fin_ai_auditor.services.pipeline_models import CollectedDocument

logger = logging.getLogger(__name__)


class PipelineCacheService:
    """Manages pipeline-level caches in SQLite."""

    def __init__(self, *, db_path: Path) -> None:
        self._db_path = db_path

    def _connection(self) -> sqlite3.Connection:
        conn = connect_database(db_path=self._db_path)
        ensure_schema(connection=conn)
        return conn

    # ── Repo Summary Cache ──────────────────────────────────────────

    def get_repo_summary(self, *, content_hash: str) -> str | None:
        """Retrieve cached repo summary by content hash (immutable — no TTL)."""
        with self._connection() as conn:
            row = conn.execute(
                "SELECT value FROM pipeline_cache WHERE cache_key = ? AND cache_type = 'repo_summary'",
                (f"repo:{content_hash}",),
            ).fetchone()
            return row["value"] if row else None

    def set_repo_summary(self, *, content_hash: str, summary: str) -> None:
        with self._connection() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO pipeline_cache (cache_key, cache_type, value, content_hash, created_at)
                   VALUES (?, 'repo_summary', ?, ?, ?)""",
                (f"repo:{content_hash}", summary, content_hash, utc_now_iso()),
            )

    # ── Context Summary Cache ───────────────────────────────────────

    def get_context_summary(self, *, cache_type: str, content_hash: str) -> str | None:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT value FROM pipeline_cache WHERE cache_key = ? AND cache_type = ?",
                (f"{cache_type}:{content_hash}", cache_type),
            ).fetchone()
            return row["value"] if row else None

    def set_context_summary(self, *, cache_type: str, content_hash: str, summary: str) -> None:
        with self._connection() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO pipeline_cache (cache_key, cache_type, value, content_hash, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (f"{cache_type}:{content_hash}", cache_type, summary, content_hash, utc_now_iso()),
            )

    # ── LLM Response Cache ──────────────────────────────────────────

    def get_llm_response(self, *, cache_key: str) -> str | None:
        """Retrieve cached LLM response (24h TTL)."""
        with self._connection() as conn:
            row = conn.execute(
                """SELECT value FROM pipeline_cache
                   WHERE cache_key = ? AND cache_type = 'llm_response'
                   AND (expires_at IS NULL OR expires_at > ?)""",
                (f"llm:{cache_key}", utc_now_iso()),
            ).fetchone()
            if row:
                logger.info("llm_cache_hit", extra={"event_name": "llm_cache_hit", "event_payload": {"key": cache_key[:32]}})
            return row["value"] if row else None

    def set_llm_response(self, *, cache_key: str, response_json: str) -> None:
        expires = (datetime.now(UTC) + timedelta(hours=24)).isoformat()
        with self._connection() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO pipeline_cache
                   (cache_key, cache_type, value, content_hash, created_at, expires_at)
                   VALUES (?, 'llm_response', ?, ?, ?, ?)""",
                (f"llm:{cache_key}", response_json, cache_key, utc_now_iso(), expires),
            )

    def cleanup_expired(self) -> int:
        """Remove expired cache entries."""
        with self._connection() as conn:
            result = conn.execute(
                "DELETE FROM pipeline_cache WHERE expires_at IS NOT NULL AND expires_at < ?",
                (utc_now_iso(),),
            )
            return result.rowcount

    # ── Cache Key Builders ──────────────────────────────────────────

    @staticmethod
    def build_content_hash(documents: Sequence[CollectedDocument], source_type: str) -> str:
        """Build a hash from document content for cache keying."""
        filtered = sorted(
            (d for d in documents if d.source_type == source_type),
            key=lambda d: d.source_id or "",
        )
        parts = [
            f"{d.source_id}:{PipelineCacheService._document_content_hash(document=d)}"
            for d in filtered
        ]
        return hashlib.sha256("|".join(parts).encode()).hexdigest()[:32]

    @staticmethod
    def build_llm_cache_key(
        findings: Sequence[AuditFinding],
        context_hash: str,
    ) -> str:
        """Build a cache key from finding fingerprints + context hash."""
        fingerprints = sorted(
            str(f.canonical_key or f.finding_id)
            for f in findings
        )
        combined = f"{','.join(fingerprints)}|{context_hash}"
        return hashlib.sha256(combined.encode()).hexdigest()[:32]

    @staticmethod
    def _document_content_hash(*, document: CollectedDocument) -> str:
        snapshot_hash = str(document.snapshot.content_hash or "").strip()
        if snapshot_hash:
            return snapshot_hash
        return f"sha256:{hashlib.sha256(document.body.encode('utf-8')).hexdigest()}"
