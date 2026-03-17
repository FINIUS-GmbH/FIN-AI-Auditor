import sqlite3
from pathlib import Path

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import (
    AtomicFactEntry,
    AuditFinding,
    AuditFindingLink,
    AuditLocation,
    AuditPosition,
    AuditRun,
    AuditSourceSnapshot,
    AuditTarget,
    AtlassianOAuthTokenRecord,
    CreateAuditRunRequest,
    DecisionCommentAnalysis,
    DecisionPackage,
    DecisionProblemElement,
    RetrievalSegment,
    ReviewCard,
    SchemaTruthEntry,
)
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository
from fin_ai_auditor.services.audit_service import AuditService
from fin_ai_auditor.services.pipeline_models import CollectedDocument
from fin_ai_auditor.services.secret_store import MemorySecretStore
from fin_ai_auditor.services import audit_repository as audit_repository_module


def test_repository_persists_findings_with_positions_and_links(tmp_path: Path) -> None:
    repository = SQLiteAuditRepository(db_path=tmp_path / "auditor.db")
    run = AuditRun(
        target=AuditTarget(local_repo_path="/Users/martinwaelter/GitHub/FIN-AI"),
        progress={
            "progress_pct": 42,
            "phase_key": "confluence_check",
            "phase_label": "Confluence-Pruefung",
            "current_activity": "Confluence-Seiten werden lesend geprueft.",
            "steps": [
                {"step_key": "metamodel_check", "label": "Metamodell-Pruefung", "status": "completed"},
                {"step_key": "confluence_check", "label": "Confluence-Pruefung", "status": "running"},
            ],
        },
        source_snapshots=[
            AuditSourceSnapshot(
                snapshot_id="snapshot_repo_1",
                source_type="github_file",
                source_id="src/finai/example.py",
                revision_id="main",
                content_hash="sha256:repo-v1",
                sync_token="git:main:example.py",
                metadata={"repo_path": "/Users/martinwaelter/GitHub/FIN-AI"},
            )
        ],
        findings=[
            AuditFinding(
                finding_id="finding_1",
                severity="high",
                category="contradiction",
                title="Write-Vertrag widerspricht der Doku",
                summary="Die Doku nennt einen anderen Write-Pfad als der Code.",
                recommendation="Claim-Normalisierung und Doku-Abschnitt angleichen.",
                canonical_key="Statement.write_contract",
                locations=[
                    AuditLocation(
                        location_id="location_1",
                        snapshot_id="snapshot_repo_1",
                        source_type="github_file",
                        source_id="src/finai/example.py",
                        title="Persistenz-Service",
                        path_hint="src/finai/example.py",
                        position=AuditPosition(
                            anchor_kind="file_line_range",
                            anchor_value="src/finai/example.py#L10-L18",
                            section_path="PersistService.write",
                            line_start=10,
                            line_end=18,
                            snippet_hash="sha256:snippet-v1",
                            content_hash="sha256:repo-v1",
                        ),
                        metadata={"role": "implemented_path"},
                    )
                ],
                metadata={"object_key": "Statement.write_contract"},
            ),
            AuditFinding(
                finding_id="finding_2",
                severity="medium",
                category="missing_definition",
                title="Lifecycle unvollstaendig",
                summary="Eine fachliche Lifecycle-Definition fehlt.",
                recommendation="Lifecycle-Abschnitt in der SSOT-Doku ergaenzen.",
            ),
        ],
        finding_links=[
            AuditFindingLink(
                link_id="link_1",
                from_finding_id="finding_1",
                to_finding_id="finding_2",
                relation_type="gap_hint",
                rationale="Der fehlende Lifecycle erklaert die Write-Abweichung zumindest teilweise.",
                confidence=0.81,
                metadata={"cluster": "statement-governance"},
            )
        ],
    )

    saved = repository.upsert_run(run=run)
    loaded = repository.get_run(run_id=saved.run_id)

    assert loaded is not None
    assert loaded.run_id == saved.run_id
    assert loaded.progress.progress_pct == 42
    assert loaded.progress.phase_key == "confluence_check"
    assert len(loaded.analysis_log) == 0
    assert len(loaded.claims) == 0
    assert len(loaded.truths) == 0
    assert len(loaded.decision_packages) == 0
    assert len(loaded.decision_records) == 0
    assert len(loaded.approval_requests) == 0
    assert len(loaded.implemented_changes) == 0
    assert len(loaded.source_snapshots) == 1
    assert loaded.source_snapshots[0].content_hash == "sha256:repo-v1"
    assert len(loaded.findings) == 2
    assert len(loaded.finding_links) == 1

    finding_by_id = {finding.finding_id: finding for finding in loaded.findings}
    assert finding_by_id["finding_1"].canonical_key == "Statement.write_contract"
    assert finding_by_id["finding_1"].locations[0].position is not None
    assert finding_by_id["finding_1"].locations[0].position.anchor_value == "src/finai/example.py#L10-L18"
    assert loaded.finding_links[0].relation_type == "gap_hint"


def test_repository_persists_fast_audit_review_cards(tmp_path: Path) -> None:
    repository = SQLiteAuditRepository(db_path=tmp_path / "auditor.db")
    run = AuditRun(
        analysis_mode="fast",
        target=AuditTarget(local_repo_path="/Users/martinwaelter/GitHub/FIN-AI"),
        review_cards=[
            ReviewCard(
                card_id="review_demo_1",
                title="Statement Review fehlt",
                deviation_type="gap",
                summary="Die priorisierte Vergleichsmenge enthaelt kein belastbares Gegenstueck.",
                source_a="Confluence: Statement Contract",
                source_b="Kein priorisierter Gegenstand",
                source_a_evidence=["Statement Contract beschreibt den Review-Status."],
                source_b_evidence=["Kein passender Gegenabschnitt innerhalb des Budgets gefunden."],
                why_it_matters="Die Aussage ist nicht ausreichend gegengeprueft.",
                recommended_decision="Fehlende Quelle oder Dokumentationsluecke klaeren.",
                priority="medium",
            )
        ],
        budget_limited=True,
        coverage_summary={
            "total_documents": 5,
            "total_sections": 18,
            "prioritized_sections": 8,
            "compared_pairs": 4,
            "skipped_sections_due_to_prioritization": 10,
            "skipped_pairs_due_to_budget": 3,
            "source_type_counts": {"confluence_page": 2, "github_file": 2, "metamodel": 1},
            "prioritized_scope_labels": ["Confluence: Statement Contract · Review"],
            "compared_scope_labels": ["Confluence: Statement Contract · Review", "Code: statement_writer.py · def write_statement_directly"],
            "deferred_scope_labels": ["Lokale Doku: Legacy Guide · Optional Write Path"],
            "notes": ["Fast Audit hat restliche Vergleiche zurueckgestellt."],
        },
    )

    saved = repository.upsert_run(run=run)
    loaded = repository.get_run(run_id=saved.run_id)

    assert loaded is not None
    assert loaded.analysis_mode == "fast"
    assert loaded.budget_limited is True
    assert loaded.coverage_summary is not None
    assert loaded.coverage_summary.compared_pairs == 4
    assert loaded.coverage_summary.skipped_sections_due_to_prioritization == 10
    assert loaded.coverage_summary.source_type_counts["confluence_page"] == 2
    assert loaded.coverage_summary.deferred_scope_labels[0] == "Lokale Doku: Legacy Guide · Optional Write Path"
    assert loaded.review_cards[0].card_id == "review_demo_1"


def test_repository_persists_schema_truth_registry(tmp_path: Path) -> None:
    repository = SQLiteAuditRepository(db_path=tmp_path / "auditor.db")
    run = AuditRun(
        target=AuditTarget(local_repo_path="/Users/martinwaelter/GitHub/FIN-AI"),
        schema_truths=[
            SchemaTruthEntry(
                schema_truth_id="schema_truth_1",
                schema_key="Node:Statement",
                schema_kind="node",
                target_label="Node:Statement",
                status="confirmed_ssot",
                source_kind="metamodel",
                source_authority="ssot",
                source_ids=["current_dump"],
                evidence_claim_ids=["claim_1"],
                related_truth_ids=["truth_1"],
                metadata={"status_candidates": ["confirmed_ssot", "code_only_inference"]},
            )
        ],
    )

    saved = repository.upsert_run(run=run)
    loaded = repository.get_run(run_id=saved.run_id)

    assert loaded is not None
    assert len(loaded.schema_truths) == 1
    assert loaded.schema_truths[0].schema_key == "Node:Statement"
    assert loaded.schema_truths[0].status == "confirmed_ssot"
    assert loaded.schema_truths[0].source_authority == "ssot"


def test_repository_persists_atomic_fact_registry(tmp_path: Path) -> None:
    repository = SQLiteAuditRepository(db_path=tmp_path / "auditor.db")
    run = AuditRun(
        target=AuditTarget(local_repo_path="/Users/martinwaelter/GitHub/FIN-AI"),
        atomic_facts=[
            AtomicFactEntry(
                atomic_fact_id="atomic_fact_1",
                fact_key="Statement.write_path",
                summary="Statement.write_path: Confluence vs. Code widersprechen sich oder lassen denselben Sachverhalt unvollstaendig.",
                status="open",
                action_lane="confluence_and_jira",
                primary_package_id="package_1",
                primary_problem_id="problem_1",
                related_package_ids=["package_1"],
                related_problem_ids=["problem_1"],
                related_finding_ids=["finding_1"],
                source_types=["confluence_page", "github_file"],
                source_ids=["page-1", "src/statement.py"],
                subject_keys=["Statement.write_path"],
                predicates=["documented_as", "implemented_as"],
                claim_ids=["claim_1", "claim_2"],
                truth_ids=["truth_1"],
                metadata={"root_cause_bucket": "write_contract"},
            )
        ],
    )

    saved = repository.upsert_run(run=run)
    loaded = repository.get_run(run_id=saved.run_id)

    assert loaded is not None
    assert len(loaded.atomic_facts) == 1
    assert loaded.atomic_facts[0].fact_key == "Statement.write_path"
    assert loaded.atomic_facts[0].action_lane == "confluence_and_jira"
    assert loaded.atomic_facts[0].source_types == ["confluence_page", "github_file"]


def test_repository_summarizes_global_atomic_fact_registry(tmp_path: Path) -> None:
    repository = SQLiteAuditRepository(db_path=tmp_path / "auditor.db")
    target = AuditTarget(local_repo_path="/Users/martinwaelter/GitHub/FIN-AI")
    repository.upsert_run(
        run=AuditRun(
            run_id="run_a",
            status="completed",
            target=target,
            atomic_facts=[
                AtomicFactEntry(
                    atomic_fact_id="atomic_fact_a",
                    fact_key="Statement.write_path",
                    summary="Statement.write_path driftet.",
                    status="confirmed",
                    action_lane="jira_code",
                    metadata={"occurrence_count": 1},
                )
            ],
        )
    )
    repository.upsert_run(
        run=AuditRun(
            run_id="run_b",
            status="completed",
            target=target,
            atomic_facts=[
                AtomicFactEntry(
                    atomic_fact_id="atomic_fact_b",
                    fact_key="Statement.write_path",
                    summary="Statement.write_path driftet erneut.",
                    status="open",
                    action_lane="jira_code",
                    related_package_ids=["package_write"],
                    source_types=["github_file", "local_doc"],
                    subject_keys=["Statement.write_path"],
                    predicates=["implemented_as", "documented_as"],
                    claim_ids=["claim_a", "claim_b"],
                    truth_ids=["truth_a"],
                    metadata={
                        "occurrence_count": 2,
                        "carry_over_mode": "reopened",
                        "previous_run_id": "run_a",
                        "root_cause_bucket": "write_contract",
                        "scope_summary": "Statement · Write path",
                        "last_status_comment": "Erneut offen wegen neuem Drift.",
                        "seen_run_ids": ["run_a", "run_b"],
                    },
                ),
                AtomicFactEntry(
                    atomic_fact_id="atomic_fact_c",
                    fact_key="Statement.policy",
                    summary="Statement.policy kollidiert.",
                    status="resolved",
                    action_lane="confluence_doc",
                    metadata={"occurrence_count": 1},
                ),
            ],
        )
    )

    summary = repository.get_atomic_fact_registry_summary()

    assert summary["total_entries"] == 3
    assert summary["unique_fact_count"] == 2
    assert summary["recurring_fact_count"] == 1
    assert summary["reopened_fact_count"] == 1
    assert summary["latest_status_counts"]["open"] == 1
    assert summary["latest_status_counts"]["resolved"] == 1
    write_path_entry = next(item for item in summary["latest_facts"] if item["fact_key"] == "Statement.write_path")
    assert write_path_entry["previous_run_id"] == "run_a"
    assert write_path_entry["root_cause_bucket"] == "write_contract"
    assert write_path_entry["source_types"] == ["github_file", "local_doc"]
    assert write_path_entry["subject_keys"] == ["Statement.write_path"]
    assert write_path_entry["claim_count"] == 2
    assert write_path_entry["truth_count"] == 1
    assert write_path_entry["related_package_ids"] == ["package_write"]
    assert write_path_entry["scope_summary"] == "Statement · Write path"
    assert write_path_entry["last_status_comment"] == "Erneut offen wegen neuem Drift."
    assert write_path_entry["seen_run_ids"] == ["run_a", "run_b"]


def test_service_enforces_fixed_sources_and_metamodel_policy(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        fixed_confluence_space_key="FINAI",
        fixed_jira_project_key="FINAI",
        confluence_home_url="https://finius.atlassian.net/wiki/home",
        jira_board_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)

    run = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="deep",
            target=AuditTarget(
                local_repo_path="/Users/martinwaelter/GitHub/FIN-AI",
                github_ref="main",
                confluence_space_keys=["OTHER"],
                confluence_page_ids=["page-2", "page-2", " page-3 "],
                jira_project_keys=["OTHER"],
                include_metamodel=False,
                include_local_docs=True,
            )
        )
    )

    assert run.target.confluence_space_keys == ["FINAI"]
    assert run.target.confluence_page_ids == ["page-2", "page-3"]
    assert run.target.jira_project_keys == ["FINAI"]
    assert run.target.include_metamodel is True
    assert run.progress.progress_pct == 0
    assert run.progress.phase_key == "queued"
    assert len(run.progress.steps) >= 7
    assert len(run.analysis_log) == 1
    assert run.analysis_log[0].source_type == "system"


def test_service_progress_advances_through_pipeline_steps(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        fixed_confluence_space_key="FINAI",
        fixed_jira_project_key="FINAI",
        confluence_home_url="https://finius.atlassian.net/wiki/home",
        jira_board_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)

    created = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="deep",
            target=AuditTarget(
                local_repo_path="/Users/martinwaelter/GitHub/FIN-AI",
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )

    claimed = service.claim_next_planned_run()
    assert claimed is not None
    assert claimed.run_id == created.run_id
    assert claimed.progress.phase_key == "starting"

    updated = service.update_run_progress(
        run_id=created.run_id,
        step_key="confluence_check",
        progress_pct=36,
        current_activity="Confluence-Seiten werden lesend geladen.",
        step_status="running",
        detail="Confluence-Seiten werden lesend geladen.",
    )

    assert updated.progress.progress_pct == 36
    assert updated.progress.phase_key == "confluence_check"
    steps_by_key = {step.step_key: step for step in updated.progress.steps}
    assert steps_by_key["metamodel_check"].status == "completed"
    assert steps_by_key["finai_code_check"].status == "completed"
    assert steps_by_key["confluence_check"].status == "running"
    assert len(updated.analysis_log) == 3
    assert updated.analysis_log[-1].source_type == "pipeline"
    assert updated.analysis_log[-1].metadata["step_key"] == "confluence_check"


def test_service_records_decision_comment_effects(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        fixed_confluence_space_key="FINAI",
        fixed_jira_project_key="FINAI",
        confluence_home_url="https://finius.atlassian.net/wiki/home",
        jira_board_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)

    created = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="deep",
            target=AuditTarget(
                local_repo_path="/Users/martinwaelter/GitHub/FIN-AI",
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )

    updated = service.record_decision_comment_effects(
        run_id=created.run_id,
        comment_text="Statement darf nur im Review-Status geschrieben werden.",
        normalized_truths=["Truth gespeichert: Statement.write_allowed_when=review_status"],
        derived_changes=["Betroffene Claim-Gruppe Statement.write_path wird neu bewertet."],
        impact_summary=["Entscheidungspaket fuer Write-Vertrag muss neu generiert werden."],
        related_finding_ids=["finding_demo_1"],
        related_scope_keys=["Statement.write_path"],
    )

    assert len(updated.analysis_log) == 2
    latest = updated.analysis_log[-1]
    assert latest.source_type == "decision_comment"
    assert latest.message == "Statement darf nur im Review-Status geschrieben werden."
    assert latest.related_scope_keys == ["Statement.write_path"]
    assert latest.impact_summary == ["Entscheidungspaket fuer Write-Vertrag muss neu generiert werden."]


def test_repository_indexes_retrieval_segments_via_fts(tmp_path: Path) -> None:
    repository = SQLiteAuditRepository(db_path=tmp_path / "auditor.db")
    repository.upsert_run(
        run=AuditRun(
            run_id="audit_test",
            target=AuditTarget(local_repo_path="/Users/martinwaelter/GitHub/FIN-AI"),
        )
    )
    repository.replace_retrieval_index(
        run_id="audit_test",
        segments=[
            RetrievalSegment(
                run_id="audit_test",
                source_type="local_doc",
                source_id="_docs/statement.md",
                title="Statement Contract",
                path_hint="_docs/statement.md",
                anchor_kind="document_section",
                anchor_value="statement#review",
                section_path="Statement > Review",
                ordinal=0,
                content="Statement write path is approval gated and review only.",
                content_hash="sha256:statement",
                segment_hash="sha256:segment_1",
                token_count=9,
                keywords=["statement", "write", "approval", "review"],
            )
        ],
        claim_links=[],
    )

    hits = repository.search_retrieval_segments(
        run_id="audit_test",
        query_text="approval gated statement write",
        limit=5,
    )

    assert hits
    assert hits[0][0]
    assert hits[0][1] > 0.0


def test_repository_caches_documents_and_service_claim_is_single_consumer(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        fixed_confluence_space_key="FINAI",
        fixed_jira_project_key="FINAI",
        confluence_home_url="https://finius.atlassian.net/wiki/home",
        jira_board_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)
    created = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="deep",
            target=AuditTarget(local_repo_path="/Users/martinwaelter/GitHub/FIN-AI")
        )
    )
    repository.cache_documents(
        documents=[
            CollectedDocument(
                snapshot=AuditSourceSnapshot(
                    source_type="github_file",
                    source_id="src/finai/example.py",
                    content_hash="sha256:cached",
                ),
                source_type="github_file",
                source_id="src/finai/example.py",
                title="example",
                body="def load_statement():\n    return True\n",
                path_hint="src/finai/example.py",
                metadata={"char_count": 36},
            )
        ]
    )

    first_claim = service.claim_next_planned_run(worker_id="worker-a")
    second_claim = service.claim_next_planned_run(worker_id="worker-b")
    cached = service.get_cached_document(
        source_type="github_file",
        source_id="src/finai/example.py",
        content_hash="sha256:cached",
    )

    assert first_claim is not None
    assert first_claim.run_id == created.run_id
    assert second_claim is None
    assert cached is not None
    assert "load_statement" in cached.body


def test_service_rejects_heartbeat_from_foreign_worker(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        fixed_confluence_space_key="FINAI",
        fixed_jira_project_key="FINAI",
        confluence_home_url="https://finius.atlassian.net/wiki/home",
        jira_board_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)
    created = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="deep",
            target=AuditTarget(local_repo_path="/Users/martinwaelter/GitHub/FIN-AI")
        )
    )

    claimed = service.claim_next_planned_run(worker_id="worker-a")

    assert claimed is not None
    try:
        service.update_run_progress(
            run_id=created.run_id,
            step_key="metamodel_check",
            progress_pct=10,
            current_activity="Metamodell wird geprueft.",
            worker_id="worker-b",
        )
    except RuntimeError as exc:
        assert "nicht mehr Lease-Owner" in str(exc)
    else:
        raise AssertionError("Fremder Worker darf den Lease-Heartbeat nicht verlaengern.")


def test_repository_manages_confluence_analysis_cache_registry_and_retention(monkeypatch, tmp_path: Path) -> None:
    repository = SQLiteAuditRepository(db_path=tmp_path / "auditor.db")
    timestamps = iter(
        [
            "2026-01-01T00:00:00+00:00",
            "2026-01-10T00:00:00+00:00",
            "2026-01-20T00:00:00+00:00",
            "2026-02-15T00:00:00+00:00",
        ]
    )
    monkeypatch.setattr(audit_repository_module, "utc_now_iso", lambda: next(timestamps))

    for revision, content_hash, changed_sections in [
        ("1", "sha256:v1", []),
        ("2", "sha256:v2", ["Statement / Review"]),
        ("3", "sha256:v3", ["Statement / Approval"]),
        ("4", "sha256:v4", ["Statement / Write"]),
    ]:
        repository.cache_documents(
            documents=[
                CollectedDocument(
                    snapshot=AuditSourceSnapshot(
                        source_type="confluence_page",
                        source_id="page-1",
                        revision_id=revision,
                        content_hash=content_hash,
                    ),
                    source_type="confluence_page",
                    source_id="page-1",
                    title="Statement Contract",
                    body=f"# Statement\nRevision {revision}",
                    path_hint="Space FINAI / Statement Contract",
                    url="https://finius.atlassian.net/wiki/spaces/FINAI/pages/page-1/Statement+Contract",
                    metadata={
                        "space_key": "FINAI",
                        "restriction_state": "unknown",
                        "sensitivity_level": "unknown",
                        "structured_block_count": 4,
                        "attachment_count": 1,
                        "changed_section_paths": changed_sections,
                    },
                )
            ]
        )

    latest = repository.get_latest_cached_document(source_type="confluence_page", source_id="page-1")
    summary = repository.get_confluence_analysis_cache_summary()

    assert latest is not None
    assert latest.content_hash == "sha256:v4"
    assert summary["page_count"] == 1
    assert summary["cache_entry_count"] == 3
    assert summary["retention_policy"]["keep_recent_revisions"] == 3
    assert summary["recent_pages"][0]["metadata"]["changed_section_paths"] == ["Statement / Write"]


def test_service_processes_decision_comment_into_follow_up_logs(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        fixed_confluence_space_key="FINAI",
        fixed_jira_project_key="FINAI",
        confluence_home_url="https://finius.atlassian.net/wiki/home",
        jira_board_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)

    created = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="deep",
            target=AuditTarget(
                local_repo_path="/Users/martinwaelter/GitHub/FIN-AI",
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )

    updated = service.process_decision_comment(
        run_id=created.run_id,
        comment_text="Statement darf nur im Review-Status geschrieben werden und Confluence muss das genauso sagen.",
        related_finding_ids=["finding_demo_1"],
    )

    assert len(updated.analysis_log) == 4
    assert updated.analysis_log[-3].source_type == "decision_comment"
    assert updated.analysis_log[-2].source_type == "truth_update"
    assert updated.analysis_log[-1].source_type == "recommendation_regeneration"
    assert "Statement.review_status" in updated.analysis_log[-1].related_scope_keys
    assert "Confluence.FINAI" in updated.analysis_log[-1].related_scope_keys
    assert any(
        "Read-/Write-Vertraege" in change for change in updated.analysis_log[-1].derived_changes
    )


def test_complete_run_seeds_claims_truths_and_decision_packages(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        fixed_confluence_space_key="FINAI",
        fixed_jira_project_key="FINAI",
        confluence_home_url="https://finius.atlassian.net/wiki/home",
        jira_board_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)

    created = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="deep",
            target=AuditTarget(
                local_repo_path="/Users/martinwaelter/GitHub/FIN-AI",
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )

    completed = service.complete_run_with_demo_findings(run_id=created.run_id)

    assert completed.status == "completed"
    assert len(completed.claims) >= 3
    assert len(completed.truths) >= 1
    assert completed.truths[0].canonical_key == "BSM.process.phase_source"
    assert len(completed.decision_packages) >= 1
    assert completed.decision_packages[0].problem_elements
    assert completed.decision_packages[0].metadata["cluster_key"] == "Statement"
    assert completed.summary is not None


def test_package_specification_creates_truths_and_marks_impacted_packages(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        fixed_confluence_space_key="FINAI",
        fixed_jira_project_key="FINAI",
        confluence_home_url="https://finius.atlassian.net/wiki/home",
        jira_board_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)

    created = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="deep",
            target=AuditTarget(
                local_repo_path="/Users/martinwaelter/GitHub/FIN-AI",
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    completed = service.complete_run_with_demo_findings(run_id=created.run_id)
    package = completed.decision_packages[0]

    updated = service.apply_package_decision(
        run_id=completed.run_id,
        package_id=package.package_id,
        action="specify",
        comment_text="Statement darf nur im Review-Status geschrieben werden und Confluence muss das genauso sagen.",
    )

    specified_package = next(item for item in updated.decision_packages if item.package_id == package.package_id)
    assert specified_package.decision_state == "specified"
    assert specified_package.rerender_required_after_decision is True
    assert updated.decision_records[-1].action == "specify"
    assert updated.decision_records[-1].created_truth_ids
    assert any(truth.source_kind == "user_specification" for truth in updated.truths)
    assert any(
        truth.canonical_key == "Confluence.FINAI|user_specification" and truth.truth_status == "active"
        for truth in updated.truths
    )
    assert any(entry.source_type == "truth_update" for entry in updated.analysis_log)


def test_service_records_jira_ticket_with_ai_coding_brief(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        fixed_confluence_space_key="FINAI",
        fixed_jira_project_key="FINAI",
        confluence_home_url="https://finius.atlassian.net/wiki/home",
        jira_board_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)

    created = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="deep",
            target=AuditTarget(
                local_repo_path="/Users/martinwaelter/GitHub/FIN-AI",
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    completed = service.complete_run_with_demo_findings(run_id=created.run_id)
    package = completed.decision_packages[0]

    pending = service.create_writeback_approval_request(
        run_id=completed.run_id,
        target_type="jira_ticket_create",
        title=f"Jira-Writeback fuer {package.title}",
        summary="Lokale Freigabeanfrage fuer das Jira-Ticket.",
        target_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
        related_package_ids=[package.package_id],
        related_finding_ids=package.related_finding_ids,
        payload_preview=["Prompt-Entwurf", "Abnahmekriterien", "Betroffene Teile"],
    )
    approved_request = pending.approval_requests[0]
    assert approved_request.metadata["jira_issue_payload"]["fields"]["project"]["key"] == "FINAI"
    assert "AI-Coding-Prompt" in " ".join(approved_request.payload_preview)
    approved = service.resolve_writeback_approval_request(
        run_id=pending.run_id,
        approval_request_id=approved_request.approval_request_id,
        decision="approve",
        comment_text="AI-Coding-Brief darf erstellt werden.",
    )

    updated = service.record_jira_ticket_created(
        run_id=approved.run_id,
        approval_request_id=approved_request.approval_request_id,
        ticket_key="FINAI-999",
        ticket_url="https://finius.atlassian.net/browse/FINAI-999",
        related_finding_ids=[approved.findings[0].finding_id],
    )

    assert len(updated.implemented_changes) == 1
    change = updated.implemented_changes[0]
    assert change.change_type == "jira_ticket_created"
    assert change.title == "Jira Ticket FINAI-999 zur FIN-AI Codeanpassung erstellt"
    assert change.jira_ticket is not None
    assert change.jira_ticket.ticket_key == "FINAI-999"
    assert "Problem:" in change.jira_ticket.ai_coding_prompt
    assert any("Abnahmekriterien" in line for line in change.jira_ticket.ai_coding_prompt.splitlines())
    assert len(change.jira_ticket.acceptance_criteria) >= 3
    assert len(change.jira_ticket.affected_parts) >= 1
    assert updated.approval_requests[0].status == "executed"


def test_fast_review_card_acceptance_can_create_jira_approval(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        fixed_confluence_space_key="FINAI",
        fixed_jira_project_key="FINAI",
        confluence_home_url="https://finius.atlassian.net/wiki/home",
        jira_board_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
    )
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)

    created = service.create_run(
        payload=CreateAuditRunRequest(
            analysis_mode="fast",
            target=AuditTarget(
                local_repo_path="/Users/martinwaelter/GitHub/FIN-AI",
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    snapshot_ids = service._build_demo_snapshot_ids()
    findings = service._build_demo_findings(run=created, snapshot_ids=snapshot_ids)
    review_card = ReviewCard(
        title="Statement Write-Vertrag pruefen",
        deviation_type="error",
        summary="Code und Doku weichen fuer den Statement-Write-Vertrag voneinander ab.",
        source_a="Confluence: Statement Contract",
        source_b="Code: StatementService.write",
        source_a_evidence=["Die Dokumentation verlangt eine Review-Freigabe vor dem Write."],
        source_b_evidence=["Der Code ermoeglicht einen direkten Write-Pfad."],
        source_a_locations=findings[0].locations[:1],
        source_b_locations=findings[0].locations[1:2],
        why_it_matters="Die gueltige Write-Regel ist aktuell nicht eindeutig.",
        recommended_decision="Gueltigen Write-Pfad festlegen und die abweichende Quelle angleichen.",
        priority="high",
        follow_up_capabilities=["jira_ticket_create"],
        related_finding_ids=[findings[0].finding_id],
    )
    run = service._repository.upsert_run(
        run=created.model_copy(
            update={
                "status": "completed",
                "source_snapshots": service._build_demo_snapshots(run=created, snapshot_ids=snapshot_ids),
                "findings": findings,
                "review_cards": [review_card],
            }
        )
    )

    decided = service.apply_review_card_decision(
        run_id=run.run_id,
        card_id=review_card.card_id,
        action="accept",
        comment_text="Diese Abweichung soll in ein Jira-Ticket ueberfuehrt werden.",
    )

    assert decided.review_cards[0].decision_state == "accepted"
    decided_finding = next(item for item in decided.findings if item.finding_id == findings[0].finding_id)
    assert decided_finding.resolution_state == "accepted"

    updated = service.create_writeback_approval_request(
        run_id=decided.run_id,
        target_type="jira_ticket_create",
        title="Jira-Writeback fuer Statement Write-Vertrag",
        summary="Akzeptierte Review-Karte soll als Jira-Ticket vorbereitet werden.",
        target_url="https://finius.atlassian.net/jira/software/projects/FINAI/boards/67",
        related_review_card_ids=[review_card.card_id],
        related_package_ids=[],
        related_finding_ids=[],
        payload_preview=["Review-Karte akzeptiert", "Jira-Folgeaktion vorbereiten"],
    )

    approval = updated.approval_requests[0]
    assert approval.related_finding_ids == [findings[0].finding_id]
    assert approval.metadata["related_review_card_ids"] == [review_card.card_id]
    assert approval.metadata["jira_issue_payload"]["fields"]["project"]["key"] == "FINAI"


def test_merge_truths_from_specification_never_uses_package_scope_summary_as_subject_key(tmp_path: Path) -> None:
    settings = Settings(database_path=tmp_path / "auditor.db")
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    service = AuditService(repository=repository, settings=settings)

    package = DecisionPackage(
        title="Statement · Policy konsolidieren",
        category="contradiction",
        severity_summary="high",
        scope_summary="Statement · Policy · 2 Problemelemente",
        recommendation_summary="Policy und Write-Contract angleichen.",
        problem_elements=[
            DecisionProblemElement(
                category="contradiction",
                severity="high",
                scope_summary="Statement.policy",
                short_explanation="Dokumentation und Code widersprechen sich.",
                recommendation="Kanonischen Policy-Zustand festziehen.",
            )
        ],
        metadata={},
    )
    analysis = DecisionCommentAnalysis(
        normalized_truths=["User-Wahrheit: Statement bleibt review-pflichtig."],
        related_scope_keys=["Statement"],
    )

    updated_truths, created_truths = service._merge_truths_from_specification(
        truths=[],
        package=package,
        analysis=analysis,
    )

    assert len(updated_truths) == 1
    assert len(created_truths) == 1
    assert created_truths[0].subject_key == "Statement"
    assert created_truths[0].canonical_key == "Statement|user_specification"
    assert "Problemelemente" not in created_truths[0].subject_key


def test_repository_keeps_atlassian_tokens_out_of_sqlite_when_secret_store_is_external(tmp_path: Path) -> None:
    repository = SQLiteAuditRepository(
        db_path=tmp_path / "auditor.db",
        secret_store=MemorySecretStore(),
    )

    saved = repository.upsert_atlassian_token(
        token=AtlassianOAuthTokenRecord(
            access_token="access-token",
            refresh_token="refresh-token",
            scope="read:page:confluence write:jira-work",
            expires_at="2099-01-01T00:00:00+00:00",
        )
    )

    assert saved.access_token == "access-token"
    assert saved.refresh_token == "refresh-token"
    with sqlite3.connect(tmp_path / "auditor.db") as connection:
        row = connection.execute(
            "SELECT access_token, refresh_token, metadata_json FROM atlassian_oauth_tokens WHERE provider = 'atlassian'"
        ).fetchone()
    assert row is not None
    assert row[0] == "__external_secret__"
    assert row[1] == "__external_secret__"
    assert '"secret_storage_mode": "memory"' in row[2]
