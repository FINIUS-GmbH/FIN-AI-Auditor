from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA_VERSION = 18


def connect_database(*, db_path: Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def ensure_schema(*, connection: sqlite3.Connection) -> None:
    current = _current_schema_version(connection=connection)
    if current == SCHEMA_VERSION:
        return

    with connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_runs (
                run_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                analysis_mode TEXT NOT NULL DEFAULT 'fast',
                target_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                progress_json TEXT NOT NULL DEFAULT '{}',
                review_cards_json TEXT NOT NULL DEFAULT '[]',
                coverage_summary_json TEXT NOT NULL DEFAULT '{}',
                budget_limited INTEGER NOT NULL DEFAULT 0,
                analysis_log_json TEXT NOT NULL DEFAULT '[]',
                implemented_changes_json TEXT NOT NULL DEFAULT '[]',
                clarification_threads_json TEXT NOT NULL DEFAULT '[]',
                lease_owner TEXT,
                lease_expires_at TEXT,
                last_heartbeat_at TEXT,
                summary TEXT,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS source_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                revision_id TEXT,
                content_hash TEXT,
                sync_token TEXT,
                parent_snapshot_id TEXT,
                collected_at TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS audit_findings (
                finding_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                severity TEXT NOT NULL,
                category TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                recommendation TEXT NOT NULL,
                canonical_key TEXT,
                resolution_state TEXT NOT NULL,
                proposed_confluence_action TEXT,
                proposed_jira_action TEXT,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS finding_locations (
                location_id TEXT PRIMARY KEY,
                finding_id TEXT NOT NULL,
                snapshot_id TEXT,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                title TEXT NOT NULL,
                path_hint TEXT,
                url TEXT,
                anchor_kind TEXT,
                anchor_value TEXT,
                section_path TEXT,
                line_start INTEGER,
                line_end INTEGER,
                char_start INTEGER,
                char_end INTEGER,
                snippet_hash TEXT,
                content_hash TEXT,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(finding_id) REFERENCES audit_findings(finding_id) ON DELETE CASCADE,
                FOREIGN KEY(snapshot_id) REFERENCES source_snapshots(snapshot_id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS finding_links (
                link_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                from_finding_id TEXT NOT NULL,
                to_finding_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                rationale TEXT NOT NULL,
                confidence REAL NOT NULL,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE,
                FOREIGN KEY(from_finding_id) REFERENCES audit_findings(finding_id) ON DELETE CASCADE,
                FOREIGN KEY(to_finding_id) REFERENCES audit_findings(finding_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS audit_claims (
                claim_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                source_snapshot_id TEXT,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                subject_kind TEXT NOT NULL,
                subject_key TEXT NOT NULL,
                predicate TEXT NOT NULL,
                normalized_value TEXT NOT NULL,
                scope_kind TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                confidence REAL NOT NULL,
                fingerprint TEXT NOT NULL,
                status TEXT NOT NULL,
                evidence_location_ids_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE,
                FOREIGN KEY(source_snapshot_id) REFERENCES source_snapshots(snapshot_id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS truth_entries (
                truth_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                canonical_key TEXT NOT NULL,
                subject_kind TEXT NOT NULL,
                subject_key TEXT NOT NULL,
                predicate TEXT NOT NULL,
                normalized_value TEXT NOT NULL,
                scope_kind TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                truth_status TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                created_from_problem_id TEXT,
                supersedes_truth_id TEXT,
                valid_from_snapshot_id TEXT,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS schema_truth_entries (
                schema_truth_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                schema_key TEXT NOT NULL,
                schema_kind TEXT NOT NULL,
                target_label TEXT NOT NULL,
                status TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                source_authority TEXT NOT NULL,
                source_ids_json TEXT NOT NULL,
                evidence_claim_ids_json TEXT NOT NULL,
                related_truth_ids_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS atomic_fact_entries (
                atomic_fact_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                fact_key TEXT NOT NULL,
                summary TEXT NOT NULL,
                status TEXT NOT NULL,
                action_lane TEXT NOT NULL,
                primary_package_id TEXT,
                primary_problem_id TEXT,
                related_package_ids_json TEXT NOT NULL,
                related_problem_ids_json TEXT NOT NULL,
                related_finding_ids_json TEXT NOT NULL,
                source_types_json TEXT NOT NULL,
                source_ids_json TEXT NOT NULL,
                subject_keys_json TEXT NOT NULL,
                predicates_json TEXT NOT NULL,
                claim_ids_json TEXT NOT NULL,
                truth_ids_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS decision_packages (
                package_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                title TEXT NOT NULL,
                category TEXT NOT NULL,
                severity_summary TEXT NOT NULL,
                scope_summary TEXT NOT NULL,
                decision_state TEXT NOT NULL,
                decision_required INTEGER NOT NULL,
                rerender_required_after_decision INTEGER NOT NULL,
                recommendation_summary TEXT NOT NULL,
                related_finding_ids_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS decision_problems (
                problem_id TEXT PRIMARY KEY,
                package_id TEXT NOT NULL,
                finding_id TEXT,
                category TEXT NOT NULL,
                severity TEXT NOT NULL,
                scope_summary TEXT NOT NULL,
                short_explanation TEXT NOT NULL,
                recommendation TEXT NOT NULL,
                confidence REAL NOT NULL,
                affected_claim_ids_json TEXT NOT NULL,
                affected_truth_ids_json TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(package_id) REFERENCES decision_packages(package_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS decision_records (
                decision_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                package_id TEXT NOT NULL,
                action TEXT NOT NULL,
                created_at TEXT NOT NULL,
                comment_text TEXT,
                created_truth_ids_json TEXT NOT NULL,
                impacted_package_ids_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE,
                FOREIGN KEY(package_id) REFERENCES decision_packages(package_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS writeback_approval_requests (
                approval_request_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                target_type TEXT NOT NULL,
                status TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                target_url TEXT,
                related_package_ids_json TEXT NOT NULL,
                related_finding_ids_json TEXT NOT NULL,
                payload_preview_json TEXT NOT NULL,
                decided_at TEXT,
                decision_comment TEXT,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS retrieval_segments (
                segment_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                snapshot_id TEXT,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                title TEXT NOT NULL,
                path_hint TEXT,
                url TEXT,
                anchor_kind TEXT NOT NULL,
                anchor_value TEXT NOT NULL,
                section_path TEXT,
                ordinal INTEGER NOT NULL,
                content TEXT NOT NULL,
                content_hash TEXT,
                segment_hash TEXT NOT NULL,
                token_count INTEGER NOT NULL,
                delta_status TEXT NOT NULL,
                keywords_json TEXT NOT NULL,
                embedding_json TEXT,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE,
                FOREIGN KEY(snapshot_id) REFERENCES source_snapshots(snapshot_id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS retrieval_segment_claim_links (
                segment_id TEXT NOT NULL,
                claim_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                score REAL NOT NULL,
                metadata_json TEXT NOT NULL,
                PRIMARY KEY(segment_id, claim_id, relation_type),
                FOREIGN KEY(segment_id) REFERENCES retrieval_segments(segment_id) ON DELETE CASCADE,
                FOREIGN KEY(claim_id) REFERENCES audit_claims(claim_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS collected_document_cache (
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                path_hint TEXT,
                url TEXT,
                metadata_json TEXT NOT NULL,
                cached_at TEXT NOT NULL,
                PRIMARY KEY(source_type, source_id, content_hash)
            );

            CREATE TABLE IF NOT EXISTS runtime_trace_spans (
                span_id TEXT PRIMARY KEY,
                trace_id TEXT NOT NULL,
                run_id TEXT,
                worker_id TEXT,
                span_name TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                duration_ms REAL NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runtime_metric_samples (
                sample_id TEXT PRIMARY KEY,
                metric_key TEXT NOT NULL,
                metric_kind TEXT NOT NULL,
                value REAL NOT NULL,
                run_id TEXT,
                worker_id TEXT,
                observed_at TEXT NOT NULL,
                labels_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS confluence_page_registry (
                page_id TEXT PRIMARY KEY,
                space_key TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT,
                current_revision_id TEXT,
                current_content_hash TEXT,
                last_seen_at TEXT NOT NULL,
                latest_cached_at TEXT NOT NULL,
                restriction_state TEXT NOT NULL,
                sensitivity_level TEXT NOT NULL,
                attachment_policy TEXT NOT NULL,
                attachment_count INTEGER NOT NULL DEFAULT 0,
                structured_block_count INTEGER NOT NULL DEFAULT 0,
                metadata_json TEXT NOT NULL
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS retrieval_segments_fts USING fts5(
                segment_id UNINDEXED,
                run_id UNINDEXED,
                title,
                section_path,
                keywords,
                content,
                tokenize = 'unicode61'
            );

            CREATE TABLE IF NOT EXISTS semantic_entities (
                entity_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                canonical_key TEXT NOT NULL,
                label TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                source_ids_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS semantic_relations (
                relation_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                source_entity_id TEXT NOT NULL,
                target_entity_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                confidence REAL NOT NULL,
                metadata_json TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE,
                FOREIGN KEY(source_entity_id) REFERENCES semantic_entities(entity_id) ON DELETE CASCADE,
                FOREIGN KEY(target_entity_id) REFERENCES semantic_entities(entity_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS atlassian_oauth_states (
                state_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                redirect_uri TEXT NOT NULL,
                status TEXT NOT NULL,
                scope TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS atlassian_oauth_tokens (
                provider TEXT PRIMARY KEY,
                access_token TEXT NOT NULL,
                refresh_token TEXT,
                scope TEXT,
                token_type TEXT NOT NULL,
                obtained_at TEXT NOT NULL,
                expires_at TEXT,
                metadata_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_runs_status ON audit_runs(status, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_findings_run ON audit_findings(run_id);
            CREATE INDEX IF NOT EXISTS idx_locations_finding ON finding_locations(finding_id);
            CREATE INDEX IF NOT EXISTS idx_locations_source ON finding_locations(source_type, source_id);
            CREATE INDEX IF NOT EXISTS idx_links_run ON finding_links(run_id);
            CREATE INDEX IF NOT EXISTS idx_snapshots_run ON source_snapshots(run_id);
            CREATE INDEX IF NOT EXISTS idx_claims_run ON audit_claims(run_id);
            CREATE INDEX IF NOT EXISTS idx_claims_fingerprint ON audit_claims(fingerprint);
            CREATE INDEX IF NOT EXISTS idx_truths_run ON truth_entries(run_id);
            CREATE INDEX IF NOT EXISTS idx_truths_canonical_key ON truth_entries(canonical_key);
            CREATE INDEX IF NOT EXISTS idx_schema_truths_run ON schema_truth_entries(run_id, status);
            CREATE INDEX IF NOT EXISTS idx_schema_truths_key ON schema_truth_entries(schema_key);
            CREATE INDEX IF NOT EXISTS idx_atomic_facts_run ON atomic_fact_entries(run_id, status);
            CREATE INDEX IF NOT EXISTS idx_atomic_facts_key ON atomic_fact_entries(fact_key);
            CREATE INDEX IF NOT EXISTS idx_packages_run ON decision_packages(run_id);
            CREATE INDEX IF NOT EXISTS idx_problems_package ON decision_problems(package_id);
            CREATE INDEX IF NOT EXISTS idx_decisions_run ON decision_records(run_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_approvals_run ON writeback_approval_requests(run_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_segments_run ON retrieval_segments(run_id, source_type, source_id);
            CREATE INDEX IF NOT EXISTS idx_segments_anchor ON retrieval_segments(source_type, source_id, anchor_value);
            CREATE INDEX IF NOT EXISTS idx_segment_claim_links_claim ON retrieval_segment_claim_links(claim_id);
            CREATE INDEX IF NOT EXISTS idx_semantic_entities_run ON semantic_entities(run_id, scope_key, entity_type);
            CREATE INDEX IF NOT EXISTS idx_semantic_entities_canonical ON semantic_entities(canonical_key);
            CREATE INDEX IF NOT EXISTS idx_semantic_relations_run ON semantic_relations(run_id, relation_type);
            CREATE INDEX IF NOT EXISTS idx_atlassian_oauth_states_status ON atlassian_oauth_states(status, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_document_cache_source ON collected_document_cache(source_type, source_id, cached_at DESC);
            CREATE INDEX IF NOT EXISTS idx_runtime_trace_run ON runtime_trace_spans(run_id, started_at DESC);
            CREATE INDEX IF NOT EXISTS idx_runtime_metric_key ON runtime_metric_samples(metric_key, observed_at DESC);
            CREATE INDEX IF NOT EXISTS idx_confluence_page_registry_space ON confluence_page_registry(space_key, latest_cached_at DESC);
            """
        )
        if current < 2:
            _ensure_progress_column(connection=connection)
        if current < 3:
            _ensure_analysis_log_column(connection=connection)
        if current < 4:
            _ensure_implemented_changes_column(connection=connection)
        if current < 10:
            _ensure_run_lease_columns(connection=connection)
            _ensure_document_cache_table(connection=connection)
        if current < 11:
            _ensure_runtime_observability_tables(connection=connection)
        if current < 12:
            _ensure_confluence_analysis_cache_tables(connection=connection)
        if current < 13:
            _ensure_pipeline_cache_table(connection=connection)
        if current < 14:
            _ensure_llm_usage_column(connection=connection)
        if current < 15:
            _ensure_schema_truth_entries_table(connection=connection)
        if current < 16:
            _ensure_atomic_fact_entries_table(connection=connection)
        if current < 17:
            _ensure_clarification_threads_column(connection=connection)
        if current < 18:
            _ensure_fast_audit_columns(connection=connection)
        connection.execute(
            """
            INSERT INTO schema_meta(key, value)
            VALUES('schema_version', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (str(SCHEMA_VERSION),),
        )


def _current_schema_version(*, connection: sqlite3.Connection) -> int:
    table_exists = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'schema_meta'
        """
    ).fetchone()
    if table_exists is None:
        return 0

    row = connection.execute(
        """
        SELECT value
        FROM schema_meta
        WHERE key = 'schema_version'
        """
    ).fetchone()
    if row is None:
        return 0
    try:
        return int(row["value"])
    except (TypeError, ValueError):
        return 0


def _ensure_progress_column(*, connection: sqlite3.Connection) -> None:
    if _column_exists(connection=connection, table_name="audit_runs", column_name="progress_json"):
        return
    connection.execute("ALTER TABLE audit_runs ADD COLUMN progress_json TEXT NOT NULL DEFAULT '{}'")


def _ensure_analysis_log_column(*, connection: sqlite3.Connection) -> None:
    if _column_exists(connection=connection, table_name="audit_runs", column_name="analysis_log_json"):
        return
    connection.execute("ALTER TABLE audit_runs ADD COLUMN analysis_log_json TEXT NOT NULL DEFAULT '[]'")


def _ensure_implemented_changes_column(*, connection: sqlite3.Connection) -> None:
    if _column_exists(connection=connection, table_name="audit_runs", column_name="implemented_changes_json"):
        return
    connection.execute("ALTER TABLE audit_runs ADD COLUMN implemented_changes_json TEXT NOT NULL DEFAULT '[]'")


def _ensure_run_lease_columns(*, connection: sqlite3.Connection) -> None:
    if not _column_exists(connection=connection, table_name="audit_runs", column_name="lease_owner"):
        connection.execute("ALTER TABLE audit_runs ADD COLUMN lease_owner TEXT")
    if not _column_exists(connection=connection, table_name="audit_runs", column_name="lease_expires_at"):
        connection.execute("ALTER TABLE audit_runs ADD COLUMN lease_expires_at TEXT")
    if not _column_exists(connection=connection, table_name="audit_runs", column_name="last_heartbeat_at"):
        connection.execute("ALTER TABLE audit_runs ADD COLUMN last_heartbeat_at TEXT")


def _ensure_llm_usage_column(*, connection: sqlite3.Connection) -> None:
    if _column_exists(connection=connection, table_name="audit_runs", column_name="llm_usage_json"):
        return
    connection.execute("ALTER TABLE audit_runs ADD COLUMN llm_usage_json TEXT NOT NULL DEFAULT '{}'")


def _ensure_fast_audit_columns(*, connection: sqlite3.Connection) -> None:
    if not _column_exists(connection=connection, table_name="audit_runs", column_name="analysis_mode"):
        connection.execute("ALTER TABLE audit_runs ADD COLUMN analysis_mode TEXT NOT NULL DEFAULT 'fast'")
    if not _column_exists(connection=connection, table_name="audit_runs", column_name="review_cards_json"):
        connection.execute("ALTER TABLE audit_runs ADD COLUMN review_cards_json TEXT NOT NULL DEFAULT '[]'")
    if not _column_exists(connection=connection, table_name="audit_runs", column_name="coverage_summary_json"):
        connection.execute("ALTER TABLE audit_runs ADD COLUMN coverage_summary_json TEXT NOT NULL DEFAULT '{}'")
    if not _column_exists(connection=connection, table_name="audit_runs", column_name="budget_limited"):
        connection.execute("ALTER TABLE audit_runs ADD COLUMN budget_limited INTEGER NOT NULL DEFAULT 0")


def _ensure_schema_truth_entries_table(*, connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS schema_truth_entries (
            schema_truth_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            schema_key TEXT NOT NULL,
            schema_kind TEXT NOT NULL,
            target_label TEXT NOT NULL,
            status TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            source_authority TEXT NOT NULL,
            source_ids_json TEXT NOT NULL,
            evidence_claim_ids_json TEXT NOT NULL,
            related_truth_ids_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_schema_truths_run
            ON schema_truth_entries(run_id, status);
        CREATE INDEX IF NOT EXISTS idx_schema_truths_key
            ON schema_truth_entries(schema_key);
        """
    )


def _ensure_atomic_fact_entries_table(*, connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS atomic_fact_entries (
            atomic_fact_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            fact_key TEXT NOT NULL,
            summary TEXT NOT NULL,
            status TEXT NOT NULL,
            action_lane TEXT NOT NULL,
            primary_package_id TEXT,
            primary_problem_id TEXT,
            related_package_ids_json TEXT NOT NULL,
            related_problem_ids_json TEXT NOT NULL,
            related_finding_ids_json TEXT NOT NULL,
            source_types_json TEXT NOT NULL,
            source_ids_json TEXT NOT NULL,
            subject_keys_json TEXT NOT NULL,
            predicates_json TEXT NOT NULL,
            claim_ids_json TEXT NOT NULL,
            truth_ids_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES audit_runs(run_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_atomic_facts_run
            ON atomic_fact_entries(run_id, status);
        CREATE INDEX IF NOT EXISTS idx_atomic_facts_key
            ON atomic_fact_entries(fact_key);
        """
    )


def _ensure_document_cache_table(*, connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS collected_document_cache (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            path_hint TEXT,
            url TEXT,
            metadata_json TEXT NOT NULL,
            cached_at TEXT NOT NULL,
            PRIMARY KEY(source_type, source_id, content_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_document_cache_source
            ON collected_document_cache(source_type, source_id, cached_at DESC);
        """
    )


def _ensure_runtime_observability_tables(*, connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS runtime_trace_spans (
            span_id TEXT PRIMARY KEY,
            trace_id TEXT NOT NULL,
            run_id TEXT,
            worker_id TEXT,
            span_name TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            duration_ms REAL NOT NULL,
            metadata_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runtime_metric_samples (
            sample_id TEXT PRIMARY KEY,
            metric_key TEXT NOT NULL,
            metric_kind TEXT NOT NULL,
            value REAL NOT NULL,
            run_id TEXT,
            worker_id TEXT,
            observed_at TEXT NOT NULL,
            labels_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_runtime_trace_run
            ON runtime_trace_spans(run_id, started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_runtime_metric_key
            ON runtime_metric_samples(metric_key, observed_at DESC);
        """
    )


def _ensure_confluence_analysis_cache_tables(*, connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS confluence_page_registry (
            page_id TEXT PRIMARY KEY,
            space_key TEXT NOT NULL,
            title TEXT NOT NULL,
            url TEXT,
            current_revision_id TEXT,
            current_content_hash TEXT,
            last_seen_at TEXT NOT NULL,
            latest_cached_at TEXT NOT NULL,
            restriction_state TEXT NOT NULL,
            sensitivity_level TEXT NOT NULL,
            attachment_policy TEXT NOT NULL,
            attachment_count INTEGER NOT NULL DEFAULT 0,
            structured_block_count INTEGER NOT NULL DEFAULT 0,
            metadata_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_confluence_page_registry_space
            ON confluence_page_registry(space_key, latest_cached_at DESC);
        """
    )


def _ensure_pipeline_cache_table(*, connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS pipeline_cache (
            cache_key TEXT PRIMARY KEY,
            cache_type TEXT NOT NULL,
            value TEXT NOT NULL,
            content_hash TEXT,
            created_at TEXT NOT NULL,
            expires_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pipeline_cache_type
            ON pipeline_cache(cache_type, created_at DESC);
        """
    )


def _column_exists(*, connection: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row["name"]) == column_name for row in rows)


def _ensure_clarification_threads_column(*, connection: sqlite3.Connection) -> None:
    if _column_exists(connection=connection, table_name="audit_runs", column_name="clarification_threads_json"):
        return
    connection.execute("ALTER TABLE audit_runs ADD COLUMN clarification_threads_json TEXT NOT NULL DEFAULT '[]'")
