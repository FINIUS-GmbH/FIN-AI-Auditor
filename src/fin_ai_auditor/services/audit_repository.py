from __future__ import annotations

import json
import re
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Sequence, TypedDict, cast
from uuid import uuid4

from fin_ai_auditor.domain.models import (
    AtomicFactEntry,
    AuditClaimEntry,
    AuditFindingLink,
    AuditLocation,
    AuditPosition,
    AuditRun,
    AuditSourceSnapshot,
    AtlassianOAuthStateRecord,
    AtlassianOAuthTokenRecord,
    DecisionPackage,
    DecisionProblemElement,
    DecisionRecord,
    RetrievalSegment,
    RetrievalSegmentClaimLink,
    SchemaTruthEntry,
    SemanticEntity,
    SemanticRelation,
    TruthLedgerEntry,
    WritebackApprovalRequest,
    utc_now_iso,
)
from fin_ai_auditor.services.audit_database import connect_database, ensure_schema
from fin_ai_auditor.services.pipeline_models import CachedCollectedDocument, CollectedDocument
from fin_ai_auditor.services.secret_store import SecretStore


_EXTERNAL_SECRET_SENTINEL = "__external_secret__"
_ATLASSIAN_ACCESS_SECRET_KEY = "atlassian/access_token"
_ATLASSIAN_REFRESH_SECRET_KEY = "atlassian/refresh_token"


class _DecisionPackagePayload(TypedDict):
    package_id: str
    title: str
    category: object
    severity_summary: object
    scope_summary: str
    decision_state: object
    decision_required: bool
    rerender_required_after_decision: bool
    recommendation_summary: str
    related_finding_ids: list[str]
    problem_elements: list[DecisionProblemElement]
    metadata: dict[str, object]


class SQLiteAuditRepository:
    _CONFLUENCE_CACHE_KEEP_RECENT_REVISIONS = 3
    _CONFLUENCE_CACHE_MAX_AGE_DAYS = 30
    _CONFLUENCE_ATTACHMENT_POLICY = "metadata_only"

    def __init__(self, *, db_path: Path, secret_store: SecretStore | None = None) -> None:
        self._db_path = Path(db_path)
        self._secret_store = secret_store
        connection = connect_database(db_path=self._db_path)
        try:
            ensure_schema(connection=connection)
        finally:
            connection.close()

    def get_secret_storage_summary(self) -> dict[str, object]:
        store = self._secret_store
        if store is None:
            return {
                "mode": "database_legacy",
                "available": True,
                "secure": False,
                "backend": "legacy_database",
                "notes": ["Secrets werden weiterhin im lokalen SQLite-Store gehalten."],
            }
        status = store.status()
        notes = list(status.notes)
        if status.mode == "database_legacy":
            notes.append("Secrets werden weiterhin im lokalen SQLite-Store gehalten.")
        elif status.available:
            notes.append("Secrets werden ausserhalb der SQLite-Datenbank gehalten.")
        else:
            notes.append("Externer Secret-Store ist aktuell nicht verfuegbar.")
        return {
            "mode": status.mode,
            "available": status.available,
            "secure": status.secure,
            "backend": status.backend,
            "notes": notes,
        }

    def list_runs(self) -> list[AuditRun]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT run_id, status, target_json, created_at, updated_at, started_at, finished_at,
                       analysis_mode, progress_json, review_cards_json, coverage_summary_json, budget_limited,
                       analysis_log_json, implemented_changes_json, clarification_threads_json,
                       llm_usage_json, summary, error
                FROM audit_runs
                ORDER BY created_at DESC
                """
            ).fetchall()
            return [self._load_run(connection=connection, row=row) for row in rows]

    def get_run(self, *, run_id: str) -> AuditRun | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT run_id, status, target_json, created_at, updated_at, started_at, finished_at,
                       analysis_mode, progress_json, review_cards_json, coverage_summary_json, budget_limited,
                       analysis_log_json, implemented_changes_json, clarification_threads_json,
                       llm_usage_json, summary, error
                FROM audit_runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            if row is None:
                return None
            return self._load_run(connection=connection, row=row)

    def upsert_run(self, *, run: AuditRun) -> AuditRun:
        with self._connection() as connection, connection:
            connection.execute(
                """
                INSERT INTO audit_runs(
                    run_id, status, analysis_mode, target_json, created_at, updated_at, started_at, finished_at,
                    progress_json, review_cards_json, coverage_summary_json, budget_limited,
                    analysis_log_json, implemented_changes_json, clarification_threads_json,
                    llm_usage_json, summary, error
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    status = excluded.status,
                    analysis_mode = excluded.analysis_mode,
                    target_json = excluded.target_json,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at,
                    started_at = excluded.started_at,
                    finished_at = excluded.finished_at,
                    progress_json = excluded.progress_json,
                    review_cards_json = excluded.review_cards_json,
                    coverage_summary_json = excluded.coverage_summary_json,
                    budget_limited = excluded.budget_limited,
                    analysis_log_json = excluded.analysis_log_json,
                    implemented_changes_json = excluded.implemented_changes_json,
                    clarification_threads_json = excluded.clarification_threads_json,
                    llm_usage_json = excluded.llm_usage_json,
                    summary = excluded.summary,
                    error = excluded.error
                """,
                (
                    run.run_id,
                    run.status,
                    run.analysis_mode,
                    run.target.model_dump_json(),
                    run.created_at,
                    run.updated_at,
                    run.started_at,
                    run.finished_at,
                    run.progress.model_dump_json(),
                    _dump_json_list(run.review_cards),
                    _dump_json(run.coverage_summary.model_dump(mode="json") if run.coverage_summary is not None else {}),
                    1 if run.budget_limited else 0,
                    _dump_json_list(run.analysis_log),
                    _dump_json_list(run.implemented_changes),
                    _dump_json_list(run.clarification_threads),
                    json.dumps(run.llm_usage),
                    run.summary,
                    run.error,
                ),
            )

            connection.execute("DELETE FROM decision_records WHERE run_id = ?", (run.run_id,))
            connection.execute("DELETE FROM writeback_approval_requests WHERE run_id = ?", (run.run_id,))
            connection.execute("DELETE FROM decision_packages WHERE run_id = ?", (run.run_id,))
            connection.execute("DELETE FROM atomic_fact_entries WHERE run_id = ?", (run.run_id,))
            connection.execute("DELETE FROM schema_truth_entries WHERE run_id = ?", (run.run_id,))
            connection.execute("DELETE FROM truth_entries WHERE run_id = ?", (run.run_id,))
            connection.execute("DELETE FROM audit_claims WHERE run_id = ?", (run.run_id,))
            connection.execute("DELETE FROM semantic_relations WHERE run_id = ?", (run.run_id,))
            connection.execute("DELETE FROM semantic_entities WHERE run_id = ?", (run.run_id,))
            connection.execute("DELETE FROM finding_links WHERE run_id = ?", (run.run_id,))
            connection.execute("DELETE FROM source_snapshots WHERE run_id = ?", (run.run_id,))
            connection.execute("DELETE FROM audit_findings WHERE run_id = ?", (run.run_id,))

            for snapshot in run.source_snapshots:
                connection.execute(
                    """
                    INSERT INTO source_snapshots(
                        snapshot_id, run_id, source_type, source_id, revision_id, content_hash,
                        sync_token, parent_snapshot_id, collected_at, metadata_json
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot.snapshot_id,
                        run.run_id,
                        snapshot.source_type,
                        snapshot.source_id,
                        snapshot.revision_id,
                        snapshot.content_hash,
                        snapshot.sync_token,
                        snapshot.parent_snapshot_id,
                        snapshot.collected_at,
                        _dump_json(snapshot.metadata),
                    ),
                )

            for claim in run.claims:
                claim_metadata = {
                    **claim.metadata,
                    "claim_operator": claim.operator,
                    "claim_constraint": claim.constraint,
                    "claim_focus_value": claim.focus_value,
                    "assertion_status": claim.assertion_status,
                    "source_authority": claim.source_authority,
                }
                connection.execute(
                    """
                    INSERT INTO audit_claims(
                        claim_id, run_id, source_snapshot_id, source_type, source_id,
                        subject_kind, subject_key, predicate, normalized_value, scope_kind,
                        scope_key, confidence, fingerprint, status, evidence_location_ids_json, metadata_json
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        claim.claim_id,
                        run.run_id,
                        claim.source_snapshot_id,
                        claim.source_type,
                        claim.source_id,
                        claim.subject_kind,
                        claim.subject_key,
                        claim.predicate,
                        claim.normalized_value,
                        claim.scope_kind,
                        claim.scope_key,
                        claim.confidence,
                        claim.fingerprint,
                        claim.status,
                        _dump_json(claim.evidence_location_ids),
                        _dump_json(claim_metadata),
                    ),
                )

            for truth in run.truths:
                truth_metadata = {
                    **truth.metadata,
                    "source_authority": truth.source_authority,
                }
                connection.execute(
                    """
                    INSERT INTO truth_entries(
                        truth_id, run_id, canonical_key, subject_kind, subject_key, predicate,
                        normalized_value, scope_kind, scope_key, truth_status, source_kind,
                        created_from_problem_id, supersedes_truth_id, valid_from_snapshot_id, metadata_json
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        truth.truth_id,
                        run.run_id,
                        truth.canonical_key,
                        truth.subject_kind,
                        truth.subject_key,
                        truth.predicate,
                        truth.normalized_value,
                        truth.scope_kind,
                        truth.scope_key,
                        truth.truth_status,
                        truth.source_kind,
                        truth.created_from_problem_id,
                        truth.supersedes_truth_id,
                        truth.valid_from_snapshot_id,
                        _dump_json(truth_metadata),
                    ),
                )

            for schema_truth in run.schema_truths:
                connection.execute(
                    """
                    INSERT INTO schema_truth_entries(
                        schema_truth_id, run_id, schema_key, schema_kind, target_label, status,
                        source_kind, source_authority, source_ids_json, evidence_claim_ids_json,
                        related_truth_ids_json, metadata_json
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        schema_truth.schema_truth_id,
                        run.run_id,
                        schema_truth.schema_key,
                        schema_truth.schema_kind,
                        schema_truth.target_label,
                        schema_truth.status,
                        schema_truth.source_kind,
                        schema_truth.source_authority,
                        _dump_json(schema_truth.source_ids),
                        _dump_json(schema_truth.evidence_claim_ids),
                        _dump_json(schema_truth.related_truth_ids),
                        _dump_json(schema_truth.metadata),
                    ),
                )

            for atomic_fact in run.atomic_facts:
                connection.execute(
                    """
                    INSERT INTO atomic_fact_entries(
                        atomic_fact_id, run_id, fact_key, summary, status, action_lane,
                        primary_package_id, primary_problem_id, related_package_ids_json,
                        related_problem_ids_json, related_finding_ids_json, source_types_json,
                        source_ids_json, subject_keys_json, predicates_json, claim_ids_json,
                        truth_ids_json, metadata_json
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        atomic_fact.atomic_fact_id,
                        run.run_id,
                        atomic_fact.fact_key,
                        atomic_fact.summary,
                        atomic_fact.status,
                        atomic_fact.action_lane,
                        atomic_fact.primary_package_id,
                        atomic_fact.primary_problem_id,
                        _dump_json(atomic_fact.related_package_ids),
                        _dump_json(atomic_fact.related_problem_ids),
                        _dump_json(atomic_fact.related_finding_ids),
                        _dump_json(atomic_fact.source_types),
                        _dump_json(atomic_fact.source_ids),
                        _dump_json(atomic_fact.subject_keys),
                        _dump_json(atomic_fact.predicates),
                        _dump_json(atomic_fact.claim_ids),
                        _dump_json(atomic_fact.truth_ids),
                        _dump_json(atomic_fact.metadata),
                    ),
                )

            for entity in run.semantic_entities:
                connection.execute(
                    """
                    INSERT INTO semantic_entities(
                        entity_id, run_id, entity_type, canonical_key, label, scope_key,
                        source_ids_json, metadata_json
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entity.entity_id,
                        run.run_id,
                        entity.entity_type,
                        entity.canonical_key,
                        entity.label,
                        entity.scope_key,
                        _dump_json(entity.source_ids),
                        _dump_json(entity.metadata),
                    ),
                )

            for relation in run.semantic_relations:
                connection.execute(
                    """
                    INSERT INTO semantic_relations(
                        relation_id, run_id, source_entity_id, target_entity_id, relation_type,
                        confidence, metadata_json
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        relation.relation_id,
                        run.run_id,
                        relation.source_entity_id,
                        relation.target_entity_id,
                        relation.relation_type,
                        relation.confidence,
                        _dump_json(relation.metadata),
                    ),
                )

            for package in run.decision_packages:
                connection.execute(
                    """
                    INSERT INTO decision_packages(
                        package_id, run_id, title, category, severity_summary, scope_summary,
                        decision_state, decision_required, rerender_required_after_decision,
                        recommendation_summary, related_finding_ids_json, metadata_json
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        package.package_id,
                        run.run_id,
                        package.title,
                        package.category,
                        package.severity_summary,
                        package.scope_summary,
                        package.decision_state,
                        int(package.decision_required),
                        int(package.rerender_required_after_decision),
                        package.recommendation_summary,
                        _dump_json(package.related_finding_ids),
                        _dump_json(package.metadata),
                    ),
                )
                for problem in package.problem_elements:
                    connection.execute(
                        """
                        INSERT INTO decision_problems(
                            problem_id, package_id, finding_id, category, severity, scope_summary,
                            short_explanation, recommendation, confidence, affected_claim_ids_json,
                            affected_truth_ids_json, evidence_json, metadata_json
                        )
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            problem.problem_id,
                            package.package_id,
                            problem.finding_id,
                            problem.category,
                            problem.severity,
                            problem.scope_summary,
                            problem.short_explanation,
                            problem.recommendation,
                            problem.confidence,
                            _dump_json(problem.affected_claim_ids),
                            _dump_json(problem.affected_truth_ids),
                            _dump_json([location.model_dump(mode="json") for location in problem.evidence_locations]),
                            _dump_json(problem.metadata),
                        ),
                    )

            for decision in run.decision_records:
                connection.execute(
                    """
                    INSERT INTO decision_records(
                        decision_id, run_id, package_id, action, created_at, comment_text,
                        created_truth_ids_json, impacted_package_ids_json, metadata_json
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        decision.decision_id,
                        run.run_id,
                        decision.package_id,
                        decision.action,
                        decision.created_at,
                        decision.comment_text,
                        _dump_json(decision.created_truth_ids),
                        _dump_json(decision.impacted_package_ids),
                        _dump_json(decision.metadata),
                    ),
                )

            for approval in run.approval_requests:
                connection.execute(
                    """
                    INSERT INTO writeback_approval_requests(
                        approval_request_id, run_id, created_at, target_type, status, title, summary,
                        target_url, related_package_ids_json, related_finding_ids_json, payload_preview_json,
                        decided_at, decision_comment, metadata_json
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        approval.approval_request_id,
                        run.run_id,
                        approval.created_at,
                        approval.target_type,
                        approval.status,
                        approval.title,
                        approval.summary,
                        approval.target_url,
                        _dump_json(approval.related_package_ids),
                        _dump_json(approval.related_finding_ids),
                        _dump_json(approval.payload_preview),
                        approval.decided_at,
                        approval.decision_comment,
                        _dump_json(approval.metadata),
                    ),
                )

            for finding in run.findings:
                connection.execute(
                    """
                    INSERT INTO audit_findings(
                        finding_id, run_id, severity, category, title, summary, recommendation,
                        canonical_key, resolution_state, proposed_confluence_action,
                        proposed_jira_action, metadata_json
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        finding.finding_id,
                        run.run_id,
                        finding.severity,
                        finding.category,
                        finding.title,
                        finding.summary,
                        finding.recommendation,
                        finding.canonical_key,
                        finding.resolution_state,
                        finding.proposed_confluence_action,
                        finding.proposed_jira_action,
                        _dump_json(finding.metadata),
                    ),
                )
                for location in finding.locations:
                    position = location.position
                    connection.execute(
                        """
                        INSERT OR REPLACE INTO finding_locations(
                            location_id, finding_id, snapshot_id, source_type, source_id, title, path_hint, url,
                            anchor_kind, anchor_value, section_path, line_start, line_end, char_start, char_end,
                            snippet_hash, content_hash, metadata_json
                        )
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            location.location_id,
                            finding.finding_id,
                            location.snapshot_id,
                            location.source_type,
                            location.source_id,
                            location.title,
                            location.path_hint,
                            location.url,
                            position.anchor_kind if position else None,
                            position.anchor_value if position else None,
                            position.section_path if position else None,
                            position.line_start if position else None,
                            position.line_end if position else None,
                            position.char_start if position else None,
                            position.char_end if position else None,
                            position.snippet_hash if position else None,
                            position.content_hash if position else None,
                            _dump_json(location.metadata),
                        ),
                    )

            for link in run.finding_links:
                connection.execute(
                    """
                    INSERT INTO finding_links(
                        link_id, run_id, from_finding_id, to_finding_id, relation_type,
                        rationale, confidence, metadata_json
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        link.link_id,
                        run.run_id,
                        link.from_finding_id,
                        link.to_finding_id,
                        link.relation_type,
                        link.rationale,
                        link.confidence,
                        _dump_json(link.metadata),
                    ),
                )

        reloaded = self.get_run(run_id=run.run_id)
        if reloaded is None:
            raise RuntimeError(f"Audit-Run konnte nach dem Speichern nicht geladen werden: {run.run_id}")
        return reloaded

    def list_by_status(self, *, status: str) -> list[AuditRun]:
        return [run for run in self.list_runs() if run.status == status]

    def reset_all_runs(self) -> None:
        """Delete all audit runs and related data. Keeps OAuth tokens and document cache."""
        with self._connection() as connection, connection:
            connection.execute("DELETE FROM decision_problems")
            connection.execute("DELETE FROM decision_records")
            connection.execute("DELETE FROM writeback_approval_requests")
            connection.execute("DELETE FROM decision_packages")
            connection.execute("DELETE FROM atomic_fact_entries")
            connection.execute("DELETE FROM schema_truth_entries")
            connection.execute("DELETE FROM truth_entries")
            connection.execute("DELETE FROM audit_claims")
            connection.execute("DELETE FROM semantic_relations")
            connection.execute("DELETE FROM semantic_entities")
            connection.execute("DELETE FROM finding_links")
            connection.execute("DELETE FROM finding_locations")
            connection.execute("DELETE FROM audit_findings")
            connection.execute("DELETE FROM source_snapshots")
            connection.execute("DELETE FROM retrieval_segment_claim_links")
            connection.execute("DELETE FROM retrieval_segments")
            connection.execute("DELETE FROM retrieval_segments_fts")
            connection.execute("DELETE FROM audit_runs")

    def claim_next_planned_run(
        self,
        *,
        worker_id: str,
        lease_expires_at: str,
        now_iso: str,
    ) -> AuditRun | None:
        for _ in range(3):
            with self._connection() as connection, connection:
                candidate = connection.execute(
                    """
                    SELECT run_id
                    FROM audit_runs
                    WHERE status = 'planned'
                       OR (
                           status = 'running'
                           AND lease_expires_at IS NOT NULL
                           AND lease_expires_at < ?
                       )
                    ORDER BY CASE WHEN status = 'planned' THEN 0 ELSE 1 END ASC, created_at ASC
                    LIMIT 1
                    """,
                    (now_iso,),
                ).fetchone()
                if candidate is None:
                    return None
                claimed = connection.execute(
                    """
                    UPDATE audit_runs
                    SET status = 'running',
                        started_at = COALESCE(started_at, ?),
                        updated_at = ?,
                        lease_owner = ?,
                        lease_expires_at = ?,
                        last_heartbeat_at = ?
                    WHERE run_id = ?
                      AND (
                          status = 'planned'
                          OR (
                              status = 'running'
                              AND lease_expires_at IS NOT NULL
                              AND lease_expires_at < ?
                          )
                      )
                    RETURNING run_id
                    """,
                    (
                        now_iso,
                        now_iso,
                        worker_id,
                        lease_expires_at,
                        now_iso,
                        str(candidate["run_id"]),
                        now_iso,
                    ),
                ).fetchone()
                if claimed is None:
                    continue
                return self.get_run(run_id=str(claimed["run_id"]))
        return None

    def heartbeat_run_lease(
        self,
        *,
        run_id: str,
        lease_expires_at: str,
        now_iso: str,
        worker_id: str | None = None,
    ) -> bool:
        with self._connection() as connection, connection:
            if worker_id is None:
                cursor = connection.execute(
                    """
                    UPDATE audit_runs
                    SET lease_expires_at = ?,
                        last_heartbeat_at = ?,
                        updated_at = CASE
                            WHEN status = 'running' THEN ?
                            ELSE updated_at
                        END
                    WHERE run_id = ? AND status = 'running'
                    """,
                    (lease_expires_at, now_iso, now_iso, run_id),
                )
            else:
                cursor = connection.execute(
                    """
                    UPDATE audit_runs
                    SET lease_expires_at = ?,
                        last_heartbeat_at = ?,
                        updated_at = CASE
                            WHEN status = 'running' THEN ?
                            ELSE updated_at
                        END
                    WHERE run_id = ? AND status = 'running' AND lease_owner = ?
                    """,
                    (lease_expires_at, now_iso, now_iso, run_id, worker_id),
                )
        return bool(cursor.rowcount)

    def clear_run_lease(self, *, run_id: str, worker_id: str | None = None) -> bool:
        with self._connection() as connection, connection:
            if worker_id is None:
                cursor = connection.execute(
                    """
                    UPDATE audit_runs
                    SET lease_owner = NULL,
                        lease_expires_at = NULL,
                        last_heartbeat_at = NULL
                    WHERE run_id = ?
                    """,
                    (run_id,),
                )
            else:
                cursor = connection.execute(
                    """
                    UPDATE audit_runs
                    SET lease_owner = NULL,
                        lease_expires_at = NULL,
                        last_heartbeat_at = NULL
                    WHERE run_id = ? AND (lease_owner = ? OR lease_owner IS NULL)
                    """,
                    (run_id, worker_id),
                )
        return bool(cursor.rowcount)

    def get_stale_run_recovery_summary(self, *, now_iso: str) -> dict[str, object]:
        with self._connection() as connection:
            reclaimable_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM audit_runs
                    WHERE status = 'running'
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at < ?
                    """,
                    (now_iso,),
                ).fetchone()["count"]
            )
            latest_reclaimable = connection.execute(
                """
                SELECT run_id, lease_owner, lease_expires_at, last_heartbeat_at, updated_at
                FROM audit_runs
                WHERE status = 'running'
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < ?
                ORDER BY lease_expires_at ASC
                LIMIT 5
                """,
                (now_iso,),
            ).fetchall()
        return {
            "reclaimable_run_count": reclaimable_count,
            "reclaimable_runs": [
                {
                    "run_id": row["run_id"],
                    "lease_owner": row["lease_owner"],
                    "lease_expires_at": row["lease_expires_at"],
                    "last_heartbeat_at": row["last_heartbeat_at"],
                    "updated_at": row["updated_at"],
                }
                for row in latest_reclaimable
            ],
        }

    def replace_retrieval_index(
        self,
        *,
        run_id: str,
        segments: list[RetrievalSegment],
        claim_links: list[RetrievalSegmentClaimLink],
    ) -> None:
        with self._connection() as connection, connection:
            connection.execute(
                """
                DELETE FROM retrieval_segment_claim_links
                WHERE segment_id IN (
                    SELECT segment_id FROM retrieval_segments WHERE run_id = ?
                )
                """,
                (run_id,),
            )
            connection.execute("DELETE FROM retrieval_segments WHERE run_id = ?", (run_id,))
            connection.execute("DELETE FROM retrieval_segments_fts WHERE run_id = ?", (run_id,))
            for segment in segments:
                connection.execute(
                    """
                    INSERT INTO retrieval_segments(
                        segment_id, run_id, snapshot_id, source_type, source_id, title, path_hint, url,
                        anchor_kind, anchor_value, section_path, ordinal, content, content_hash,
                        segment_hash, token_count, delta_status, keywords_json, embedding_json, metadata_json
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        segment.segment_id,
                        segment.run_id,
                        segment.snapshot_id,
                        segment.source_type,
                        segment.source_id,
                        segment.title,
                        segment.path_hint,
                        segment.url,
                        segment.anchor_kind,
                        segment.anchor_value,
                        segment.section_path,
                        segment.ordinal,
                        segment.content,
                        segment.content_hash,
                        segment.segment_hash,
                        segment.token_count,
                        segment.delta_status,
                        _dump_json(segment.keywords),
                        _dump_json(segment.embedding) if segment.embedding is not None else None,
                        _dump_json(segment.metadata),
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO retrieval_segments_fts(segment_id, run_id, title, section_path, keywords, content)
                    VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    (
                        segment.segment_id,
                        run_id,
                        segment.title,
                        segment.section_path,
                        " ".join(segment.keywords),
                        segment.content,
                    ),
                )
            for link in claim_links:
                connection.execute(
                    """
                    INSERT INTO retrieval_segment_claim_links(
                        segment_id, claim_id, relation_type, score, metadata_json
                    )
                    VALUES(?, ?, ?, ?, ?)
                    """,
                    (
                        link.segment_id,
                        link.claim_id,
                        link.relation_type,
                        link.score,
                        _dump_json(link.metadata),
                    ),
                )

    def cache_documents(self, *, documents: list[CollectedDocument]) -> None:
        now_iso = utc_now_iso()
        with self._connection() as connection, connection:
            touched_confluence_pages: dict[str, CollectedDocument] = {}
            for document in documents:
                content_hash = str(document.snapshot.content_hash or "").strip()
                if not content_hash:
                    continue
                connection.execute(
                    """
                    INSERT INTO collected_document_cache(
                        source_type, source_id, content_hash, title, body, path_hint, url, metadata_json, cached_at
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_type, source_id, content_hash) DO UPDATE SET
                        title = excluded.title,
                        body = excluded.body,
                        path_hint = excluded.path_hint,
                        url = excluded.url,
                        metadata_json = excluded.metadata_json,
                        cached_at = excluded.cached_at
                    """,
                    (
                        document.source_type,
                        document.source_id,
                        content_hash,
                        document.title,
                        document.body,
                        document.path_hint,
                        document.url,
                        _dump_json(document.metadata),
                        now_iso,
                    ),
                )
                if document.source_type == "confluence_page":
                    touched_confluence_pages[document.source_id] = document
            for document in touched_confluence_pages.values():
                self._upsert_confluence_page_registry(
                    connection=connection,
                    document=document,
                    now_iso=now_iso,
                )
            if touched_confluence_pages:
                self._prune_confluence_document_cache(connection=connection, now_iso=now_iso)

    def get_cached_document(
        self,
        *,
        source_type: str,
        source_id: str,
        content_hash: str,
    ) -> CachedCollectedDocument | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT source_type, source_id, content_hash, title, body, path_hint, url, metadata_json, cached_at
                FROM collected_document_cache
                WHERE source_type = ? AND source_id = ? AND content_hash = ?
                """,
                (source_type, source_id, content_hash),
            ).fetchone()
            if row is None:
                return None
            return CachedCollectedDocument(
                source_type=row["source_type"],
                source_id=row["source_id"],
                content_hash=row["content_hash"],
                title=row["title"],
                body=row["body"],
                cached_at=row["cached_at"],
                path_hint=row["path_hint"],
                url=row["url"],
                metadata=json.loads(row["metadata_json"] or "{}"),
            )

    def get_latest_cached_document(
        self,
        *,
        source_type: str,
        source_id: str,
    ) -> CachedCollectedDocument | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT source_type, source_id, content_hash, title, body, path_hint, url, metadata_json, cached_at
                FROM collected_document_cache
                WHERE source_type = ? AND source_id = ?
                ORDER BY cached_at DESC
                LIMIT 1
                """,
                (source_type, source_id),
            ).fetchone()
            if row is None:
                return None
            return CachedCollectedDocument(
                source_type=row["source_type"],
                source_id=row["source_id"],
                content_hash=row["content_hash"],
                title=row["title"],
                body=row["body"],
                cached_at=row["cached_at"],
                path_hint=row["path_hint"],
                url=row["url"],
                metadata=json.loads(row["metadata_json"] or "{}"),
            )

    def get_confluence_analysis_cache_summary(self) -> dict[str, object]:
        with self._connection() as connection:
            page_count = int(
                connection.execute("SELECT COUNT(*) AS count FROM confluence_page_registry").fetchone()["count"]
            )
            cache_entry_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM collected_document_cache
                    WHERE source_type = 'confluence_page'
                    """
                ).fetchone()["count"]
            )
            restricted_page_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM confluence_page_registry
                    WHERE restriction_state = 'restricted'
                    """
                ).fetchone()["count"]
            )
            unknown_restriction_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM confluence_page_registry
                    WHERE restriction_state = 'unknown'
                    """
                ).fetchone()["count"]
            )
            unknown_sensitivity_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM confluence_page_registry
                    WHERE sensitivity_level = 'unknown'
                    """
                ).fetchone()["count"]
            )
            rows = connection.execute(
                """
                SELECT page_id, space_key, title, url, current_revision_id, latest_cached_at,
                       restriction_state, sensitivity_level, attachment_count, structured_block_count, metadata_json
                FROM confluence_page_registry
                ORDER BY latest_cached_at DESC, page_id ASC
                LIMIT 8
                """
            ).fetchall()
        return {
            "mode": "analysis_cache",
            "page_count": page_count,
            "cache_entry_count": cache_entry_count,
            "retention_policy": {
                "keep_recent_revisions": self._CONFLUENCE_CACHE_KEEP_RECENT_REVISIONS,
                "max_age_days": self._CONFLUENCE_CACHE_MAX_AGE_DAYS,
                "attachment_policy": self._CONFLUENCE_ATTACHMENT_POLICY,
            },
            "restricted_page_count": restricted_page_count,
            "unknown_restriction_page_count": unknown_restriction_count,
            "unknown_sensitivity_page_count": unknown_sensitivity_count,
            "recent_pages": [
                {
                    "page_id": row["page_id"],
                    "space_key": row["space_key"],
                    "title": row["title"],
                    "url": row["url"],
                    "current_revision_id": row["current_revision_id"],
                    "latest_cached_at": row["latest_cached_at"],
                    "restriction_state": row["restriction_state"],
                    "sensitivity_level": row["sensitivity_level"],
                    "attachment_count": int(row["attachment_count"] or 0),
                    "structured_block_count": int(row["structured_block_count"] or 0),
                    "metadata": json.loads(row["metadata_json"] or "{}"),
                }
                for row in rows
            ],
        }

    def record_runtime_trace_span(
        self,
        *,
        trace_id: str,
        run_id: str | None,
        worker_id: str | None,
        span_name: str,
        status: str,
        started_at: str,
        finished_at: str,
        duration_ms: float,
        metadata: dict[str, object],
    ) -> None:
        with self._connection() as connection, connection:
            connection.execute(
                """
                INSERT INTO runtime_trace_spans(
                    span_id, trace_id, run_id, worker_id, span_name, status, started_at, finished_at, duration_ms, metadata_json
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"span_{uuid4().hex}",
                    trace_id,
                    run_id,
                    worker_id,
                    span_name,
                    status,
                    started_at,
                    finished_at,
                    float(duration_ms),
                    _dump_json(metadata),
                ),
            )

    def record_runtime_metric_sample(
        self,
        *,
        metric_key: str,
        metric_kind: str,
        value: float,
        run_id: str | None,
        worker_id: str | None,
        observed_at: str,
        labels: dict[str, object],
    ) -> None:
        with self._connection() as connection, connection:
            connection.execute(
                """
                INSERT INTO runtime_metric_samples(
                    sample_id, metric_key, metric_kind, value, run_id, worker_id, observed_at, labels_json
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"metric_{uuid4().hex}",
                    metric_key,
                    metric_kind,
                    float(value),
                    run_id,
                    worker_id,
                    observed_at,
                    _dump_json(labels),
                ),
            )

    def get_runtime_observability_summary(self) -> dict[str, object]:
        with self._connection() as connection:
            trace_count = int(
                connection.execute("SELECT COUNT(*) AS count FROM runtime_trace_spans").fetchone()["count"]
            )
            metric_count = int(
                connection.execute("SELECT COUNT(*) AS count FROM runtime_metric_samples").fetchone()["count"]
            )
            recent_error_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM runtime_trace_spans
                    WHERE status = 'error'
                      AND started_at >= datetime('now', '-1 day')
                    """
                ).fetchone()["count"]
            )
            latest = connection.execute(
                """
                SELECT trace_id, run_id, worker_id, span_name, status, started_at, finished_at, duration_ms, metadata_json
                FROM runtime_trace_spans
                ORDER BY started_at DESC
                LIMIT 5
                """
            ).fetchall()
        return {
            "trace_count": trace_count,
            "metric_sample_count": metric_count,
            "recent_error_span_count": recent_error_count,
            "recent_spans": [
                {
                    "trace_id": row["trace_id"],
                    "run_id": row["run_id"],
                    "worker_id": row["worker_id"],
                    "span_name": row["span_name"],
                    "status": row["status"],
                    "started_at": row["started_at"],
                    "finished_at": row["finished_at"],
                    "duration_ms": float(row["duration_ms"]),
                    "metadata": json.loads(row["metadata_json"] or "{}"),
                }
                for row in latest
            ],
        }

    def get_atomic_fact_registry_summary(self) -> dict[str, object]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT af.atomic_fact_id, af.fact_key, af.summary, af.status, af.action_lane, af.metadata_json,
                       af.related_package_ids_json, af.related_problem_ids_json, af.related_finding_ids_json,
                       af.source_types_json, af.source_ids_json, af.subject_keys_json, af.predicates_json,
                       af.claim_ids_json, af.truth_ids_json,
                       ar.run_id, ar.updated_at
                FROM atomic_fact_entries af
                JOIN audit_runs ar ON ar.run_id = af.run_id
                WHERE ar.status = 'completed'
                ORDER BY ar.updated_at DESC, af.fact_key ASC, af.atomic_fact_id ASC
                """
            ).fetchall()
        total_entries = len(rows)
        latest_by_key: dict[str, dict[str, object]] = {}
        run_ids_by_key: dict[str, set[str]] = {}
        reopened_count = 0
        for row in rows:
            fact_key = str(row["fact_key"])
            metadata = json.loads(row["metadata_json"] or "{}")
            run_ids_by_key.setdefault(fact_key, set()).add(str(row["run_id"]))
            if fact_key in latest_by_key:
                continue
            latest_by_key[fact_key] = {
                "fact_key": fact_key,
                "summary": row["summary"],
                "status": row["status"],
                "action_lane": row["action_lane"],
                "run_id": row["run_id"],
                "updated_at": row["updated_at"],
                "occurrence_count": int(metadata.get("occurrence_count") or 1),
                "carry_over_mode": str(metadata.get("carry_over_mode") or "").strip() or None,
                "previous_run_id": str(metadata.get("previous_run_id") or "").strip() or None,
                "source_types": json.loads(row["source_types_json"] or "[]"),
                "source_ids": json.loads(row["source_ids_json"] or "[]"),
                "subject_keys": json.loads(row["subject_keys_json"] or "[]"),
                "predicates": json.loads(row["predicates_json"] or "[]"),
                "related_package_ids": json.loads(row["related_package_ids_json"] or "[]"),
                "related_problem_ids": json.loads(row["related_problem_ids_json"] or "[]"),
                "related_finding_ids": json.loads(row["related_finding_ids_json"] or "[]"),
                "claim_count": len(json.loads(row["claim_ids_json"] or "[]")),
                "truth_count": len(json.loads(row["truth_ids_json"] or "[]")),
                "root_cause_bucket": str(metadata.get("root_cause_bucket") or "").strip() or None,
                "scope_summary": str(metadata.get("scope_summary") or "").strip() or None,
                "last_status_comment": str(metadata.get("last_status_comment") or "").strip() or None,
                "seen_run_ids": list(metadata.get("seen_run_ids") or []),
            }
            if str(metadata.get("carry_over_mode") or "").strip() == "reopened":
                reopened_count += 1
        latest_entries = list(latest_by_key.values())
        latest_status_counts: dict[str, int] = {}
        for entry in latest_entries:
            status = str(entry["status"])
            latest_status_counts[status] = latest_status_counts.get(status, 0) + 1
        recurring_count = sum(1 for run_ids in run_ids_by_key.values() if len(run_ids) > 1)
        return {
            "total_entries": total_entries,
            "unique_fact_count": len(latest_entries),
            "recurring_fact_count": recurring_count,
            "reopened_fact_count": reopened_count,
            "latest_status_counts": latest_status_counts,
            "latest_facts": latest_entries[:12],
        }

    def _upsert_confluence_page_registry(
        self,
        *,
        connection: sqlite3.Connection,
        document: CollectedDocument,
        now_iso: str,
    ) -> None:
        metadata = dict(document.metadata)
        connection.execute(
            """
            INSERT INTO confluence_page_registry(
                page_id, space_key, title, url, current_revision_id, current_content_hash,
                last_seen_at, latest_cached_at, restriction_state, sensitivity_level,
                attachment_policy, attachment_count, structured_block_count, metadata_json
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(page_id) DO UPDATE SET
                space_key = excluded.space_key,
                title = excluded.title,
                url = excluded.url,
                current_revision_id = excluded.current_revision_id,
                current_content_hash = excluded.current_content_hash,
                last_seen_at = excluded.last_seen_at,
                latest_cached_at = excluded.latest_cached_at,
                restriction_state = excluded.restriction_state,
                sensitivity_level = excluded.sensitivity_level,
                attachment_policy = excluded.attachment_policy,
                attachment_count = excluded.attachment_count,
                structured_block_count = excluded.structured_block_count,
                metadata_json = excluded.metadata_json
            """,
            (
                document.source_id,
                str(metadata.get("space_key") or "").strip() or "UNKNOWN",
                document.title,
                document.url,
                document.snapshot.revision_id,
                document.snapshot.content_hash,
                now_iso,
                now_iso,
                str(metadata.get("restriction_state") or "unknown"),
                str(metadata.get("sensitivity_level") or "unknown"),
                self._CONFLUENCE_ATTACHMENT_POLICY,
                _coerce_int(metadata.get("attachment_count")),
                _coerce_int(metadata.get("structured_block_count")),
                _dump_json(metadata),
            ),
        )

    def _prune_confluence_document_cache(
        self,
        *,
        connection: sqlite3.Connection,
        now_iso: str,
    ) -> None:
        cutoff = (
            datetime.fromisoformat(now_iso).astimezone(UTC) - timedelta(days=self._CONFLUENCE_CACHE_MAX_AGE_DAYS)
        ).astimezone(UTC).isoformat()
        source_rows = connection.execute(
            """
            SELECT DISTINCT source_id
            FROM collected_document_cache
            WHERE source_type = 'confluence_page'
            """
        ).fetchall()
        for row in source_rows:
            source_id = str(row["source_id"])
            stale_rows = connection.execute(
                """
                SELECT content_hash
                FROM collected_document_cache
                WHERE source_type = 'confluence_page'
                  AND source_id = ?
                ORDER BY cached_at DESC
                LIMIT -1 OFFSET ?
                """,
                (source_id, self._CONFLUENCE_CACHE_KEEP_RECENT_REVISIONS),
            ).fetchall()
            if stale_rows:
                stale_hashes = [str(stale_row["content_hash"]) for stale_row in stale_rows]
                for content_hash in stale_hashes:
                    connection.execute(
                        """
                        DELETE FROM collected_document_cache
                        WHERE source_type = 'confluence_page'
                          AND source_id = ?
                          AND content_hash = ?
                          AND cached_at < ?
                        """,
                        (source_id, content_hash, cutoff),
                    )

    def list_retrieval_segments(self, *, run_id: str) -> list[RetrievalSegment]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT segment_id, run_id, snapshot_id, source_type, source_id, title, path_hint, url,
                       anchor_kind, anchor_value, section_path, ordinal, content, content_hash,
                       segment_hash, token_count, delta_status, keywords_json, embedding_json, metadata_json
                FROM retrieval_segments
                WHERE run_id = ?
                ORDER BY source_type ASC, source_id ASC, ordinal ASC, segment_id ASC
                """,
                (run_id,),
            ).fetchall()
            return [
                RetrievalSegment(
                    segment_id=row["segment_id"],
                    run_id=row["run_id"],
                    snapshot_id=row["snapshot_id"],
                    source_type=row["source_type"],
                    source_id=row["source_id"],
                    title=row["title"],
                    path_hint=row["path_hint"],
                    url=row["url"],
                    anchor_kind=row["anchor_kind"],
                    anchor_value=row["anchor_value"],
                    section_path=row["section_path"],
                    ordinal=int(row["ordinal"]),
                    content=row["content"],
                    content_hash=row["content_hash"],
                    segment_hash=row["segment_hash"],
                    token_count=int(row["token_count"]),
                    delta_status=row["delta_status"],
                    keywords=json.loads(row["keywords_json"] or "[]"),
                    embedding=json.loads(row["embedding_json"]) if row["embedding_json"] else None,
                    metadata=json.loads(row["metadata_json"] or "{}"),
                )
                for row in rows
            ]

    def search_retrieval_segments(
        self,
        *,
        run_id: str,
        query_text: str,
        limit: int = 12,
    ) -> list[tuple[str, float]]:
        fts_query = _fts_query_from_text(query_text)
        if not fts_query:
            return []
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT segment_id, bm25(retrieval_segments_fts, 6.0, 4.0, 2.0, 1.0) AS rank
                FROM retrieval_segments_fts
                WHERE retrieval_segments_fts MATCH ? AND run_id = ?
                ORDER BY rank ASC
                LIMIT ?
                """,
                (fts_query, run_id, max(1, int(limit))),
            ).fetchall()
        return [
            (str(row["segment_id"]), _normalize_bm25_score(float(row["rank"])))
            for row in rows
        ]

    def save_atlassian_oauth_state(self, *, state: AtlassianOAuthStateRecord) -> AtlassianOAuthStateRecord:
        with self._connection() as connection, connection:
            connection.execute(
                """
                INSERT INTO atlassian_oauth_states(
                    state_id, created_at, expires_at, redirect_uri, status, scope, metadata_json
                )
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(state_id) DO UPDATE SET
                    created_at = excluded.created_at,
                    expires_at = excluded.expires_at,
                    redirect_uri = excluded.redirect_uri,
                    status = excluded.status,
                    scope = excluded.scope,
                    metadata_json = excluded.metadata_json
                """,
                (
                    state.state_id,
                    state.created_at,
                    state.expires_at,
                    state.redirect_uri,
                    state.status,
                    state.scope,
                    _dump_json(state.metadata),
                ),
            )
        loaded = self.get_atlassian_oauth_state(state_id=state.state_id)
        if loaded is None:
            raise RuntimeError(f"OAuth-State konnte nach dem Speichern nicht geladen werden: {state.state_id}")
        return loaded

    def get_atlassian_oauth_state(self, *, state_id: str) -> AtlassianOAuthStateRecord | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT state_id, created_at, expires_at, redirect_uri, status, scope, metadata_json
                FROM atlassian_oauth_states
                WHERE state_id = ?
                """,
                (state_id,),
            ).fetchone()
            if row is None:
                return None
            return AtlassianOAuthStateRecord(
                state_id=row["state_id"],
                created_at=row["created_at"],
                expires_at=row["expires_at"],
                redirect_uri=row["redirect_uri"],
                status=row["status"],
                scope=row["scope"],
                metadata=json.loads(row["metadata_json"] or "{}"),
            )

    def upsert_atlassian_token(self, *, token: AtlassianOAuthTokenRecord) -> AtlassianOAuthTokenRecord:
        token_metadata = dict(token.metadata or {})
        access_token_value = token.access_token
        refresh_token_value = token.refresh_token
        if self._secret_store is not None and self._secret_store.mode != "database_legacy":
            self._secret_store.set_secret(key=_ATLASSIAN_ACCESS_SECRET_KEY, value=token.access_token)
            if token.refresh_token:
                self._secret_store.set_secret(key=_ATLASSIAN_REFRESH_SECRET_KEY, value=token.refresh_token)
            else:
                self._secret_store.delete_secret(key=_ATLASSIAN_REFRESH_SECRET_KEY)
            access_token_value = _EXTERNAL_SECRET_SENTINEL
            refresh_token_value = _EXTERNAL_SECRET_SENTINEL if token.refresh_token else None
            token_metadata.update(
                {
                    "secret_storage_mode": self._secret_store.mode,
                    "access_token_secret_key": _ATLASSIAN_ACCESS_SECRET_KEY,
                    "refresh_token_secret_key": _ATLASSIAN_REFRESH_SECRET_KEY if token.refresh_token else None,
                }
            )
        else:
            token_metadata.setdefault("secret_storage_mode", "database_legacy")
        with self._connection() as connection, connection:
            connection.execute(
                """
                INSERT INTO atlassian_oauth_tokens(
                    provider, access_token, refresh_token, scope, token_type, obtained_at, expires_at, metadata_json
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider) DO UPDATE SET
                    access_token = excluded.access_token,
                    refresh_token = excluded.refresh_token,
                    scope = excluded.scope,
                    token_type = excluded.token_type,
                    obtained_at = excluded.obtained_at,
                    expires_at = excluded.expires_at,
                    metadata_json = excluded.metadata_json
                """,
                (
                    token.provider,
                    access_token_value,
                    refresh_token_value,
                    token.scope,
                    token.token_type,
                    token.obtained_at,
                    token.expires_at,
                    _dump_json(token_metadata),
                ),
            )
        loaded = self.get_atlassian_token()
        if loaded is None:
            raise RuntimeError("Atlassian-Token konnte nach dem Speichern nicht geladen werden.")
        return loaded

    def delete_atlassian_token(self) -> None:
        existing = self.get_atlassian_token()
        if existing is not None and self._secret_store is not None:
            metadata = dict(existing.metadata or {})
            if metadata.get("secret_storage_mode") != "database_legacy":
                access_key = str(metadata.get("access_token_secret_key") or _ATLASSIAN_ACCESS_SECRET_KEY)
                refresh_key = str(metadata.get("refresh_token_secret_key") or _ATLASSIAN_REFRESH_SECRET_KEY)
                self._secret_store.delete_secret(key=access_key)
                self._secret_store.delete_secret(key=refresh_key)
        with self._connection() as connection, connection:
            connection.execute("DELETE FROM atlassian_oauth_tokens WHERE provider = 'atlassian'")

    def get_atlassian_token(self) -> AtlassianOAuthTokenRecord | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT provider, access_token, refresh_token, scope, token_type, obtained_at, expires_at, metadata_json
                FROM atlassian_oauth_tokens
                WHERE provider = 'atlassian'
                """,
            ).fetchone()
            if row is None:
                return None
            metadata = json.loads(row["metadata_json"] or "{}")
            secret_storage_mode = str(metadata.get("secret_storage_mode") or "database_legacy").strip() or "database_legacy"
            access_token = row["access_token"]
            refresh_token = row["refresh_token"]
            if secret_storage_mode != "database_legacy":
                if self._secret_store is None:
                    return None
                access_key = str(metadata.get("access_token_secret_key") or _ATLASSIAN_ACCESS_SECRET_KEY)
                refresh_key = str(metadata.get("refresh_token_secret_key") or _ATLASSIAN_REFRESH_SECRET_KEY)
                access_token = self._secret_store.get_secret(key=access_key)
                refresh_token = self._secret_store.get_secret(key=refresh_key) if refresh_key else None
                if not access_token:
                    return None
            return AtlassianOAuthTokenRecord(
                provider=row["provider"],
                access_token=access_token,
                refresh_token=refresh_token,
                scope=row["scope"],
                token_type=row["token_type"],
                obtained_at=row["obtained_at"],
                expires_at=row["expires_at"],
                metadata=metadata,
            )

    def _load_run(self, *, connection: sqlite3.Connection, row: sqlite3.Row) -> AuditRun:
        run_id = str(row["run_id"])
        findings_by_id = self._load_findings(connection=connection, run_id=run_id)
        snapshots = self._load_snapshots(connection=connection, run_id=run_id)
        return AuditRun.model_validate(
            {
                "run_id": run_id,
                "status": row["status"],
                "analysis_mode": row["analysis_mode"] if "analysis_mode" in row.keys() else "fast",
                "target": json.loads(row["target_json"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "progress": json.loads(row["progress_json"] or "{}"),
                "review_cards": json.loads(row["review_cards_json"] or "[]") if "review_cards_json" in row.keys() else [],
                "coverage_summary": (
                    json.loads(row["coverage_summary_json"] or "{}")
                    if "coverage_summary_json" in row.keys() and str(row["coverage_summary_json"] or "").strip() not in {"", "{}"}
                    else None
                ),
                "budget_limited": bool(row["budget_limited"]) if "budget_limited" in row.keys() else False,
                "analysis_log": json.loads(row["analysis_log_json"] or "[]"),
                "implemented_changes": json.loads(row["implemented_changes_json"] or "[]"),
                "clarification_threads": json.loads(
                    row["clarification_threads_json"] or "[]"
                ) if "clarification_threads_json" in row.keys() else [],
                "claims": self._load_claims(connection=connection, run_id=run_id),
                "truths": self._load_truths(connection=connection, run_id=run_id),
                "schema_truths": self._load_schema_truths(connection=connection, run_id=run_id),
                "atomic_facts": self._load_atomic_facts(connection=connection, run_id=run_id),
                "semantic_entities": self._load_semantic_entities(connection=connection, run_id=run_id),
                "semantic_relations": self._load_semantic_relations(connection=connection, run_id=run_id),
                "decision_packages": self._load_decision_packages(connection=connection, run_id=run_id),
                "decision_records": self._load_decision_records(connection=connection, run_id=run_id),
                "approval_requests": self._load_approval_requests(connection=connection, run_id=run_id),
                "summary": row["summary"],
                "error": row["error"],
                "llm_usage": json.loads(row["llm_usage_json"] or "{}") if "llm_usage_json" in row.keys() else {},
                "source_snapshots": snapshots,
                "findings": list(findings_by_id.values()),
                "finding_links": self._load_links(connection=connection, run_id=run_id),
            }
        )

    def _load_claims(self, *, connection: sqlite3.Connection, run_id: str) -> list[AuditClaimEntry]:
        rows = connection.execute(
            """
            SELECT claim_id, source_snapshot_id, source_type, source_id, subject_kind, subject_key, predicate,
                   normalized_value, scope_kind, scope_key, confidence, fingerprint, status,
                   evidence_location_ids_json, metadata_json
            FROM audit_claims
            WHERE run_id = ?
            ORDER BY subject_key ASC, predicate ASC, claim_id ASC
            """,
            (run_id,),
        ).fetchall()
        return [
            AuditClaimEntry(
                claim_id=row["claim_id"],
                source_snapshot_id=row["source_snapshot_id"],
                source_type=row["source_type"],
                source_id=row["source_id"],
                subject_kind=row["subject_kind"],
                subject_key=row["subject_key"],
                predicate=row["predicate"],
                normalized_value=row["normalized_value"],
                scope_kind=row["scope_kind"],
                scope_key=row["scope_key"],
                confidence=float(row["confidence"]),
                fingerprint=row["fingerprint"],
                status=row["status"],
                evidence_location_ids=json.loads(row["evidence_location_ids_json"] or "[]"),
                metadata=json.loads(row["metadata_json"] or "{}"),
            )
            for row in rows
        ]

    def _load_truths(self, *, connection: sqlite3.Connection, run_id: str) -> list[TruthLedgerEntry]:
        rows = connection.execute(
            """
            SELECT truth_id, canonical_key, subject_kind, subject_key, predicate, normalized_value,
                   scope_kind, scope_key, truth_status, source_kind, created_from_problem_id,
                   supersedes_truth_id, valid_from_snapshot_id, metadata_json
            FROM truth_entries
            WHERE run_id = ?
            ORDER BY canonical_key ASC, truth_id ASC
            """,
            (run_id,),
        ).fetchall()
        return [
            TruthLedgerEntry(
                truth_id=row["truth_id"],
                canonical_key=row["canonical_key"],
                subject_kind=row["subject_kind"],
                subject_key=row["subject_key"],
                predicate=row["predicate"],
                normalized_value=row["normalized_value"],
                scope_kind=row["scope_kind"],
                scope_key=row["scope_key"],
                truth_status=row["truth_status"],
                source_kind=row["source_kind"],
                created_from_problem_id=row["created_from_problem_id"],
                supersedes_truth_id=row["supersedes_truth_id"],
                valid_from_snapshot_id=row["valid_from_snapshot_id"],
                metadata=json.loads(row["metadata_json"] or "{}"),
            )
            for row in rows
        ]

    def _load_schema_truths(self, *, connection: sqlite3.Connection, run_id: str) -> list[SchemaTruthEntry]:
        rows = connection.execute(
            """
            SELECT schema_truth_id, schema_key, schema_kind, target_label, status, source_kind,
                   source_authority, source_ids_json, evidence_claim_ids_json, related_truth_ids_json, metadata_json
            FROM schema_truth_entries
            WHERE run_id = ?
            ORDER BY schema_key ASC, status ASC, schema_truth_id ASC
            """,
            (run_id,),
        ).fetchall()
        return [
            SchemaTruthEntry(
                schema_truth_id=row["schema_truth_id"],
                schema_key=row["schema_key"],
                schema_kind=row["schema_kind"],
                target_label=row["target_label"],
                status=row["status"],
                source_kind=row["source_kind"],
                source_authority=row["source_authority"],
                source_ids=json.loads(row["source_ids_json"] or "[]"),
                evidence_claim_ids=json.loads(row["evidence_claim_ids_json"] or "[]"),
                related_truth_ids=json.loads(row["related_truth_ids_json"] or "[]"),
                metadata=json.loads(row["metadata_json"] or "{}"),
            )
            for row in rows
        ]

    def _load_atomic_facts(self, *, connection: sqlite3.Connection, run_id: str) -> list[AtomicFactEntry]:
        rows = connection.execute(
            """
            SELECT atomic_fact_id, fact_key, summary, status, action_lane, primary_package_id,
                   primary_problem_id, related_package_ids_json, related_problem_ids_json,
                   related_finding_ids_json, source_types_json, source_ids_json, subject_keys_json,
                   predicates_json, claim_ids_json, truth_ids_json, metadata_json
            FROM atomic_fact_entries
            WHERE run_id = ?
            ORDER BY fact_key ASC, atomic_fact_id ASC
            """,
            (run_id,),
        ).fetchall()
        return [
            AtomicFactEntry(
                atomic_fact_id=row["atomic_fact_id"],
                fact_key=row["fact_key"],
                summary=row["summary"],
                status=row["status"],
                action_lane=row["action_lane"],
                primary_package_id=row["primary_package_id"],
                primary_problem_id=row["primary_problem_id"],
                related_package_ids=json.loads(row["related_package_ids_json"] or "[]"),
                related_problem_ids=json.loads(row["related_problem_ids_json"] or "[]"),
                related_finding_ids=json.loads(row["related_finding_ids_json"] or "[]"),
                source_types=json.loads(row["source_types_json"] or "[]"),
                source_ids=json.loads(row["source_ids_json"] or "[]"),
                subject_keys=json.loads(row["subject_keys_json"] or "[]"),
                predicates=json.loads(row["predicates_json"] or "[]"),
                claim_ids=json.loads(row["claim_ids_json"] or "[]"),
                truth_ids=json.loads(row["truth_ids_json"] or "[]"),
                metadata=json.loads(row["metadata_json"] or "{}"),
            )
            for row in rows
        ]

    def _load_semantic_entities(self, *, connection: sqlite3.Connection, run_id: str) -> list[SemanticEntity]:
        rows = connection.execute(
            """
            SELECT entity_id, run_id, entity_type, canonical_key, label, scope_key, source_ids_json, metadata_json
            FROM semantic_entities
            WHERE run_id = ?
            ORDER BY scope_key ASC, entity_type ASC, label ASC, entity_id ASC
            """,
            (run_id,),
        ).fetchall()
        return [
            SemanticEntity(
                entity_id=row["entity_id"],
                run_id=row["run_id"],
                entity_type=row["entity_type"],
                canonical_key=row["canonical_key"],
                label=row["label"],
                scope_key=row["scope_key"],
                source_ids=json.loads(row["source_ids_json"] or "[]"),
                metadata=json.loads(row["metadata_json"] or "{}"),
            )
            for row in rows
        ]

    def _load_semantic_relations(self, *, connection: sqlite3.Connection, run_id: str) -> list[SemanticRelation]:
        rows = connection.execute(
            """
            SELECT relation_id, run_id, source_entity_id, target_entity_id, relation_type, confidence, metadata_json
            FROM semantic_relations
            WHERE run_id = ?
            ORDER BY relation_type ASC, source_entity_id ASC, target_entity_id ASC, relation_id ASC
            """,
            (run_id,),
        ).fetchall()
        return [
            SemanticRelation(
                relation_id=row["relation_id"],
                run_id=row["run_id"],
                source_entity_id=row["source_entity_id"],
                target_entity_id=row["target_entity_id"],
                relation_type=row["relation_type"],
                confidence=float(row["confidence"]),
                metadata=json.loads(row["metadata_json"] or "{}"),
            )
            for row in rows
        ]

    def _load_decision_packages(self, *, connection: sqlite3.Connection, run_id: str) -> list[DecisionPackage]:
        def _severity_order(value: str) -> int:
            return {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(str(value), 9)

        package_rows = connection.execute(
            """
            SELECT package_id, title, category, severity_summary, scope_summary, decision_state,
                   decision_required, rerender_required_after_decision, recommendation_summary,
                   related_finding_ids_json, metadata_json
            FROM decision_packages
            WHERE run_id = ?
            ORDER BY severity_summary DESC, package_id ASC
            """,
            (run_id,),
        ).fetchall()
        packages: dict[str, _DecisionPackagePayload] = {}
        for row in package_rows:
            package_id = str(row["package_id"])
            packages[package_id] = {
                "package_id": package_id,
                "title": row["title"],
                "category": row["category"],
                "severity_summary": row["severity_summary"],
                "scope_summary": row["scope_summary"],
                "decision_state": row["decision_state"],
                "decision_required": bool(row["decision_required"]),
                "rerender_required_after_decision": bool(row["rerender_required_after_decision"]),
                "recommendation_summary": row["recommendation_summary"],
                "related_finding_ids": json.loads(row["related_finding_ids_json"] or "[]"),
                "problem_elements": [],
                "metadata": json.loads(row["metadata_json"] or "{}"),
            }

        if not packages:
            return []

        problem_rows = connection.execute(
            """
            SELECT problem_id, package_id, finding_id, category, severity, scope_summary,
                   short_explanation, recommendation, confidence, affected_claim_ids_json,
                   affected_truth_ids_json, evidence_json, metadata_json
            FROM decision_problems
            WHERE package_id IN (
                SELECT package_id FROM decision_packages WHERE run_id = ?
            )
            ORDER BY package_id ASC, problem_id ASC
            """,
            (run_id,),
        ).fetchall()
        for row in problem_rows:
            package = packages.get(str(row["package_id"]))
            if package is None:
                continue
            package["problem_elements"].append(
                DecisionProblemElement(
                    problem_id=row["problem_id"],
                    finding_id=row["finding_id"],
                    category=row["category"],
                    severity=row["severity"],
                    scope_summary=row["scope_summary"],
                    short_explanation=row["short_explanation"],
                    recommendation=row["recommendation"],
                    confidence=float(row["confidence"]),
                    affected_claim_ids=json.loads(row["affected_claim_ids_json"] or "[]"),
                    affected_truth_ids=json.loads(row["affected_truth_ids_json"] or "[]"),
                    evidence_locations=[
                        AuditLocation.model_validate(item) for item in json.loads(row["evidence_json"] or "[]")
                    ],
                    metadata=json.loads(row["metadata_json"] or "{}"),
                )
            )

        loaded_packages: list[DecisionPackage] = []
        for payload in packages.values():
            payload["problem_elements"].sort(
                key=lambda problem: (
                    0 if str(problem.metadata.get("root_cause_role") or "") == "primary" else 1,
                    _severity_order(str(problem.severity)),
                    str(problem.scope_summary),
                    str(problem.problem_id),
                )
            )
            loaded_packages.append(DecisionPackage.model_validate(payload))
        return sorted(
            loaded_packages,
            key=lambda package: (
                _coerce_int(package.metadata.get("package_sort_rank"), fallback=999),
                _severity_order(str(package.severity_summary)),
                package.title,
            ),
        )

    def _load_decision_records(self, *, connection: sqlite3.Connection, run_id: str) -> list[DecisionRecord]:
        rows = connection.execute(
            """
            SELECT decision_id, package_id, action, created_at, comment_text,
                   created_truth_ids_json, impacted_package_ids_json, metadata_json
            FROM decision_records
            WHERE run_id = ?
            ORDER BY created_at ASC, decision_id ASC
            """,
            (run_id,),
        ).fetchall()
        return [
            DecisionRecord(
                decision_id=row["decision_id"],
                package_id=row["package_id"],
                action=row["action"],
                created_at=row["created_at"],
                comment_text=row["comment_text"],
                created_truth_ids=json.loads(row["created_truth_ids_json"] or "[]"),
                impacted_package_ids=json.loads(row["impacted_package_ids_json"] or "[]"),
                metadata=json.loads(row["metadata_json"] or "{}"),
            )
            for row in rows
        ]

    def _load_approval_requests(
        self,
        *,
        connection: sqlite3.Connection,
        run_id: str,
    ) -> list[WritebackApprovalRequest]:
        rows = connection.execute(
            """
            SELECT approval_request_id, created_at, target_type, status, title, summary, target_url,
                   related_package_ids_json, related_finding_ids_json, payload_preview_json,
                   decided_at, decision_comment, metadata_json
            FROM writeback_approval_requests
            WHERE run_id = ?
            ORDER BY created_at DESC, approval_request_id DESC
            """,
            (run_id,),
        ).fetchall()
        return [
            WritebackApprovalRequest(
                approval_request_id=row["approval_request_id"],
                created_at=row["created_at"],
                target_type=row["target_type"],
                status=row["status"],
                title=row["title"],
                summary=row["summary"],
                target_url=row["target_url"],
                related_package_ids=json.loads(row["related_package_ids_json"] or "[]"),
                related_finding_ids=json.loads(row["related_finding_ids_json"] or "[]"),
                payload_preview=json.loads(row["payload_preview_json"] or "[]"),
                decided_at=row["decided_at"],
                decision_comment=row["decision_comment"],
                metadata=json.loads(row["metadata_json"] or "{}"),
            )
            for row in rows
        ]

    def _load_findings(self, *, connection: sqlite3.Connection, run_id: str) -> dict[str, dict[str, object]]:
        rows = connection.execute(
            """
            SELECT finding_id, severity, category, title, summary, recommendation, canonical_key,
                   resolution_state, proposed_confluence_action, proposed_jira_action, metadata_json
            FROM audit_findings
            WHERE run_id = ?
            ORDER BY severity DESC, finding_id ASC
            """,
            (run_id,),
        ).fetchall()
        findings: dict[str, dict[str, object]] = {}
        for row in rows:
            finding_id = str(row["finding_id"])
            findings[finding_id] = {
                "finding_id": finding_id,
                "severity": row["severity"],
                "category": row["category"],
                "title": row["title"],
                "summary": row["summary"],
                "recommendation": row["recommendation"],
                "canonical_key": row["canonical_key"],
                "resolution_state": row["resolution_state"],
                "proposed_confluence_action": row["proposed_confluence_action"],
                "proposed_jira_action": row["proposed_jira_action"],
                "metadata": json.loads(row["metadata_json"] or "{}"),
                "locations": [],
            }

        location_rows = connection.execute(
            """
            SELECT location_id, finding_id, snapshot_id, source_type, source_id, title, path_hint, url,
                   anchor_kind, anchor_value, section_path, line_start, line_end, char_start, char_end,
                   snippet_hash, content_hash, metadata_json
            FROM finding_locations
            WHERE finding_id IN (
                SELECT finding_id
                FROM audit_findings
                WHERE run_id = ?
            )
            ORDER BY location_id ASC
            """,
            (run_id,),
        ).fetchall()
        for row in location_rows:
            finding = findings.get(str(row["finding_id"]))
            if finding is None:
                continue
            position: AuditPosition | None = None
            anchor_kind = row["anchor_kind"]
            anchor_value = row["anchor_value"]
            if anchor_kind and anchor_value:
                position = AuditPosition(
                    anchor_kind=anchor_kind,
                    anchor_value=anchor_value,
                    section_path=row["section_path"],
                    line_start=row["line_start"],
                    line_end=row["line_end"],
                    char_start=row["char_start"],
                    char_end=row["char_end"],
                    snippet_hash=row["snippet_hash"],
                    content_hash=row["content_hash"],
                )
            cast(list[AuditLocation], finding["locations"]).append(
                AuditLocation(
                    location_id=row["location_id"],
                    snapshot_id=row["snapshot_id"],
                    source_type=row["source_type"],
                    source_id=row["source_id"],
                    title=row["title"],
                    path_hint=row["path_hint"],
                    url=row["url"],
                    position=position,
                    metadata=json.loads(row["metadata_json"] or "{}"),
                )
            )
        return findings

    def _load_links(self, *, connection: sqlite3.Connection, run_id: str) -> list[AuditFindingLink]:
        rows = connection.execute(
            """
            SELECT link_id, from_finding_id, to_finding_id, relation_type, rationale, confidence, metadata_json
            FROM finding_links
            WHERE run_id = ?
            ORDER BY link_id ASC
            """,
            (run_id,),
        ).fetchall()
        return [
            AuditFindingLink(
                link_id=row["link_id"],
                from_finding_id=row["from_finding_id"],
                to_finding_id=row["to_finding_id"],
                relation_type=row["relation_type"],
                rationale=row["rationale"],
                confidence=float(row["confidence"]),
                metadata=json.loads(row["metadata_json"] or "{}"),
            )
            for row in rows
        ]

    def _load_snapshots(self, *, connection: sqlite3.Connection, run_id: str) -> list[AuditSourceSnapshot]:
        rows = connection.execute(
            """
            SELECT snapshot_id, source_type, source_id, revision_id, content_hash, sync_token,
                   parent_snapshot_id, collected_at, metadata_json
            FROM source_snapshots
            WHERE run_id = ?
            ORDER BY collected_at ASC, snapshot_id ASC
            """,
            (run_id,),
        ).fetchall()
        return [
            AuditSourceSnapshot(
                snapshot_id=row["snapshot_id"],
                source_type=row["source_type"],
                source_id=row["source_id"],
                revision_id=row["revision_id"],
                content_hash=row["content_hash"],
                sync_token=row["sync_token"],
                parent_snapshot_id=row["parent_snapshot_id"],
                collected_at=row["collected_at"],
                metadata=json.loads(row["metadata_json"] or "{}"),
            )
            for row in rows
        ]

    def _connection(self) -> sqlite3.Connection:
        return connect_database(db_path=self._db_path)


def _dump_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def _dump_json_list(items: Sequence[object]) -> str:
    payload = [item.model_dump(mode="json") if hasattr(item, "model_dump") else item for item in items]
    return _dump_json(payload)


def _coerce_int(value: object, *, fallback: int = 0) -> int:
    try:
        return int(cast(int | float | str, value))
    except (TypeError, ValueError):
        return fallback


_FTS_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_]{2,}")
_FTS_STOPWORDS = {
    "the",
    "and",
    "oder",
    "und",
    "der",
    "die",
    "das",
    "for",
    "mit",
    "von",
    "eine",
    "einer",
    "this",
    "that",
}


def _fts_query_from_text(query_text: str) -> str:
    tokens: list[str] = []
    for token in _FTS_TOKEN_PATTERN.findall(str(query_text or "").casefold()):
        if token in _FTS_STOPWORDS:
            continue
        if token not in tokens:
            tokens.append(token)
        if len(tokens) >= 8:
            break
    return " OR ".join(f'"{token}"' for token in tokens)


def _normalize_bm25_score(rank: float) -> float:
    value = abs(float(rank))
    return 1.0 / (1.0 + value)
