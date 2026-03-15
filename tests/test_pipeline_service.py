import json
from pathlib import Path

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import (
    AuditClaimEntry,
    AuditFinding,
    AuditRun,
    AuditSourceSnapshot,
    AuditTarget,
    CreateAuditRunRequest,
    TruthLedgerEntry,
)
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository
from fin_ai_auditor.services.audit_service import AuditService
from fin_ai_auditor.services.connectors.github_connector import GitHubSnapshotConnector, GitHubSnapshotRequest
from fin_ai_auditor.services.connectors import github_connector
from fin_ai_auditor.services.connectors import metamodel_connector
from fin_ai_auditor.services.connectors.metamodel_connector import MetaModelConnector
from fin_ai_auditor.services import pipeline_service as pipeline_service_module
from fin_ai_auditor.services.pipeline_cache_service import PipelineCacheService
from fin_ai_auditor.services.pipeline_models import CollectedDocument
from fin_ai_auditor.services.pipeline_service import (
    AuditPipelineService,
    _annotate_claim_deltas,
    _derive_impacted_scope_keys,
    _prepare_section_level_confluence_regeneration,
)


def test_github_connector_collects_code_and_local_docs(tmp_path: Path) -> None:
    repo_path = tmp_path / "repo"
    repo_path.mkdir()
    (repo_path / "src").mkdir()
    (repo_path / "_docs").mkdir()
    (repo_path / "src" / "service.py").write_text("def load_statement():\n    return True\n", encoding="utf-8")
    (repo_path / "_docs" / "process.md").write_text("# Statement\nWrite path is documented.\n", encoding="utf-8")

    connector = GitHubSnapshotConnector()
    bundle = connector.collect_snapshot(
        request=GitHubSnapshotRequest(local_repo_path=str(repo_path), git_ref="main")
    )

    assert len(bundle.documents) == 2
    assert {document.source_type for document in bundle.documents} == {"github_file", "local_doc"}
    assert all(snapshot.content_hash for snapshot in bundle.snapshots)


def test_metamodel_connector_uses_local_dump_fallback(monkeypatch, tmp_path: Path) -> None:
    dump_path = tmp_path / "metamodel.json"
    dump_path.write_text(
        json.dumps(
            {
                "collected_at": "2026-03-14T00:00:00+00:00",
                "source": "local_dump",
                "rows": [{"phase_id": "001", "phase_name": "Scoping", "questions": []}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(Settings, "_collect_external_env_map", lambda self: {})
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=dump_path,
        mothership_url="",
        license_key="",
        license_tenant="",
    )

    connector = MetaModelConnector(settings=settings)
    bundle = connector.collect_catalog()

    assert len(bundle.documents) == 1
    assert bundle.documents[0].source_type == "metamodel"
    assert "Fallback" in " ".join(bundle.analysis_notes)


def test_metamodel_connector_prefers_direct_source(monkeypatch, tmp_path: Path) -> None:
    dump_path = tmp_path / "metamodel.json"
    settings = Settings(database_path=tmp_path / "auditor.db", metamodel_dump_path=dump_path)
    monkeypatch.setattr(
        Settings,
        "_collect_external_env_map",
        lambda self: {
            "FINAI_META_SOURCE": "DIRECT",
            "FINAI_META_MODEL_URI": "neo4j+s://example.databases.neo4j.io",
            "FINAI_META_MODEL_USERNAME": "neo4j",
            "FINAI_META_MODEL_PASSWORD": "secret",
            "FINAI_META_MODEL_DATABASE": "neo4j",
        },
    )

    def fake_fetch_direct_catalog(*, config: object) -> list[dict[str, object]]:
        return [{"phase_id": "001", "phase_name": "Scoping", "phase_order": "001", "phase_properties": {}, "questions": []}]

    monkeypatch.setattr(metamodel_connector, "_fetch_direct_catalog", fake_fetch_direct_catalog)

    connector = MetaModelConnector(settings=settings)
    bundle = connector.collect_catalog()

    assert len(bundle.documents) == 1
    assert bundle.documents[0].metadata["source"] == "direct_neo4j"
    assert "direkt aus Neo4j" in " ".join(bundle.analysis_notes)


def test_pipeline_service_executes_with_local_repo_only(monkeypatch, tmp_path: Path) -> None:
    repo_path = tmp_path / "repo"
    repo_path.mkdir()
    (repo_path / "src").mkdir()
    (repo_path / "_docs").mkdir()
    (repo_path / "src" / "statement_service.py").write_text(
        "\n".join(
            [
                "class StatementService:",
                "    def load_statement(self):",
                "        return self.query_statement()",
                "",
                "    def persist_statement(self):",
                "        return self.save_statement()",
            ]
        ),
        encoding="utf-8",
    )
    (repo_path / "_docs" / "statement.md").write_text(
        "\n".join(
            [
                "# Statement",
                "The Statement lifecycle is under review.",
                "The documented write flow is handled by a review writer.",
            ]
        ),
        encoding="utf-8",
    )
    dump_path = tmp_path / "metamodel.json"
    dump_path.write_text(
        json.dumps(
            {
                "collected_at": "2026-03-14T00:00:00+00:00",
                "source": "local_dump",
                "rows": [{"phase_id": "001", "phase_name": "Scoping", "questions": []}],
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=dump_path,
        mothership_url="",
        license_key="",
        license_tenant="",
    )
    monkeypatch.setattr(Settings, "_collect_external_env_map", lambda self: {})
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    audit_service = AuditService(repository=repository, settings=settings)
    created = audit_service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path=str(repo_path),
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    claimed = audit_service.claim_next_planned_run()
    assert claimed is not None

    pipeline = AuditPipelineService(
        audit_service=audit_service,
        settings=settings,
        allow_remote_llm=False,
    )
    completed = pipeline.execute_run(run_id=created.run_id)

    assert completed.status == "completed"
    assert len(completed.source_snapshots) >= 3
    assert len(completed.claims) >= 3
    assert len(completed.findings) >= 1
    assert len(completed.decision_packages) >= 1
    assert any("Confluence-Collector wurde uebersprungen" in entry.message for entry in completed.analysis_log)


def test_pipeline_cache_build_content_hash_falls_back_to_document_body() -> None:
    snapshot = AuditSourceSnapshot(source_type="github_file", source_id="src/service.py", content_hash=None)
    document = CollectedDocument(
        snapshot=snapshot,
        source_type="github_file",
        source_id="src/service.py",
        title="service.py",
        body="def load_statement():\n    return True\n",
    )

    content_hash = PipelineCacheService.build_content_hash([document], "github_file")

    assert isinstance(content_hash, str)
    assert len(content_hash) == 32


def test_pipeline_service_can_inherit_truths_across_runs(monkeypatch, tmp_path: Path) -> None:
    repo_path = tmp_path / "repo"
    repo_path.mkdir()
    (repo_path / "src").mkdir()
    (repo_path / "_docs").mkdir()
    (repo_path / "src" / "statement_service.py").write_text(
        "def persist_statement():\n    return save_statement()\n",
        encoding="utf-8",
    )
    (repo_path / "_docs" / "statement.md").write_text(
        "# Statement\nStatement review status is documented.\n",
        encoding="utf-8",
    )
    dump_path = tmp_path / "metamodel.json"
    dump_path.write_text(
        json.dumps(
            {
                "collected_at": "2026-03-14T00:00:00+00:00",
                "source": "local_dump",
                "rows": [{"phase_id": "001", "phase_name": "Scoping", "questions": []}],
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=dump_path,
        mothership_url="",
        license_key="",
        license_tenant="",
    )
    monkeypatch.setattr(Settings, "_collect_external_env_map", lambda self: {})
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    audit_service = AuditService(repository=repository, settings=settings)
    pipeline = AuditPipelineService(audit_service=audit_service, settings=settings, allow_remote_llm=False)

    first_run = audit_service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path=str(repo_path),
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    audit_service.claim_next_planned_run()
    completed_first = pipeline.execute_run(run_id=first_run.run_id)

    second_run = audit_service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path=str(repo_path),
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    audit_service.claim_next_planned_run()
    completed_second = pipeline.execute_run(run_id=second_run.run_id)

    assert completed_first.truths
    assert completed_second.truths
    assert completed_first.truths[0].truth_id != completed_second.truths[0].truth_id


def test_claim_delta_annotation_distinguishes_textual_and_semantic_changes() -> None:
    previous_claim = AuditClaimEntry(
        source_type="local_doc",
        source_id="_docs/statement.md",
        subject_kind="object_property",
        subject_key="Statement.policy",
        predicate="documented_policy",
        normalized_value="Write flow is approval-gated and review-only.",
        scope_kind="project",
        scope_key="FINAI",
        confidence=0.9,
        fingerprint="claim_previous",
    )
    textual_current = previous_claim.model_copy(
        update={
            "claim_id": "claim_textual",
            "fingerprint": "claim_textual",
            "normalized_value": "Persistence requires approval before save.",
        }
    )
    semantic_current = previous_claim.model_copy(
        update={
            "claim_id": "claim_semantic",
            "fingerprint": "claim_semantic",
            "normalized_value": "Direct write without approval is allowed.",
        }
    )

    annotated = _annotate_claim_deltas(current=[textual_current, semantic_current], previous=[previous_claim])

    assert annotated[0].metadata["delta_change_type"] == "textual_only"
    assert annotated[1].metadata["delta_change_type"] == "semantic"
    assert annotated[0].metadata["delta_scope_key"] == "Statement"


def test_claim_delta_annotation_treats_phase_order_format_changes_as_textual_only() -> None:
    previous_claim = AuditClaimEntry(
        source_type="metamodel",
        source_id="metamodel_dump",
        subject_kind="process",
        subject_key="BSM.phase.scoping",
        predicate="phase_order",
        normalized_value="001",
        scope_kind="global",
        scope_key="FINAI",
        confidence=0.92,
        fingerprint="phase_order_previous",
    )
    current_claim = previous_claim.model_copy(
        update={
            "claim_id": "phase_order_current",
            "fingerprint": "phase_order_current",
            "normalized_value": "1",
        }
    )

    annotated = _annotate_claim_deltas(current=[current_claim], previous=[previous_claim])

    assert annotated[0].metadata["delta_status"] == "changed"
    assert annotated[0].metadata["delta_change_type"] == "textual_only"
    assert annotated[0].metadata["delta_scope_key"] == "BSM.phase.scoping"


def test_impacted_scope_keys_include_explicit_truth_scopes() -> None:
    changed_claim = AuditClaimEntry(
        source_type="local_doc",
        source_id="_docs/statement.md",
        subject_kind="object_property",
        subject_key="Statement.policy",
        predicate="documented_policy",
        normalized_value="Direct write without approval is allowed.",
        scope_kind="project",
        scope_key="FINAI",
        confidence=0.9,
        fingerprint="claim_statement_changed",
        metadata={"delta_status": "changed", "delta_scope_key": "Statement"},
    )
    unchanged_claim = AuditClaimEntry(
        source_type="metamodel",
        source_id="metamodel_dump",
        subject_kind="process",
        subject_key="BSM.process",
        predicate="phase_count",
        normalized_value="25",
        scope_kind="global",
        scope_key="FINAI",
        confidence=1.0,
        fingerprint="claim_bsm_unchanged",
        metadata={"delta_status": "unchanged", "delta_scope_key": "BSM.process"},
    )
    related_truth = TruthLedgerEntry(
        canonical_key="Statement.policy|user_specification",
        subject_kind="object_property",
        subject_key="Statement.policy",
        predicate="user_specification",
        normalized_value="User-Wahrheit: Write path bleibt review-pflichtig.",
        scope_kind="project",
        scope_key="FINAI",
        source_kind="user_specification",
    )
    unrelated_truth = TruthLedgerEntry(
        canonical_key="BSM.process|user_specification",
        subject_kind="process",
        subject_key="BSM.process",
        predicate="user_specification",
        normalized_value="User-Wahrheit: BSM Prozess bleibt unveraendert.",
        scope_kind="project",
        scope_key="FINAI",
        source_kind="user_specification",
    )

    impacted = _derive_impacted_scope_keys(
        claims=[changed_claim, unchanged_claim],
        inherited_truths=[related_truth, unrelated_truth],
    )

    assert impacted == {"Statement", "BSM.process"}


def test_impacted_scope_keys_expand_transitively_over_semantic_clusters() -> None:
    changed_claim = AuditClaimEntry(
        source_type="confluence_page",
        source_id="page-1",
        subject_kind="object_property",
        subject_key="Statement.policy",
        predicate="documented_policy",
        normalized_value="Approval gated",
        scope_kind="project",
        scope_key="FINAI",
        confidence=0.9,
        fingerprint="claim_statement_policy",
        metadata={
            "delta_status": "changed",
            "delta_scope_key": "Statement",
            "semantic_cluster_keys": ["Statement", "BSM.phase.scoping"],
        },
    )
    related_unchanged_claim = AuditClaimEntry(
        source_type="metamodel",
        source_id="metamodel_dump",
        subject_kind="process",
        subject_key="BSM.phase.scoping.question.problem_statement",
        predicate="question_reference",
        normalized_value="Problem Statement",
        scope_kind="global",
        scope_key="FINAI",
        confidence=0.92,
        fingerprint="claim_problem_statement",
        metadata={
            "delta_status": "unchanged",
            "delta_scope_key": "BSM.phase.scoping.question.problem_statement",
            "semantic_cluster_keys": ["BSM.phase.scoping", "BSM.phase.scoping.question.problem_statement"],
        },
    )

    impacted = _derive_impacted_scope_keys(
        claims=[changed_claim, related_unchanged_claim],
        inherited_truths=[],
    )

    assert impacted == {"Statement", "BSM.phase.scoping", "BSM.phase.scoping.question.problem_statement"}


def test_section_level_confluence_regeneration_reuses_unchanged_section_claims() -> None:
    current_document = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="confluence_page",
            source_id="page-1",
            revision_id="8",
            content_hash="sha256:new",
        ),
        source_type="confluence_page",
        source_id="page-1",
        title="Statement Contract",
        body="# Statement\n## Write\nApproval gated.\n",
        path_hint="Space FINAI / Contracts / Statement Contract",
        url="https://finius.atlassian.net/wiki/spaces/FINAI/pages/page-1/Statement+Contract",
        metadata={
            "ancestor_titles": ["Contracts"],
            "section_delta_status": "changed",
            "changed_section_paths": ["Statement / Write"],
            "added_section_paths": [],
            "removed_section_paths": [],
            "structured_blocks": [
                {"kind": "heading", "text": "Statement", "section_path": "Statement"},
                {"kind": "heading", "text": "Write", "section_path": "Statement / Write"},
                {"kind": "text", "text": "Approval gated.", "section_path": "Statement / Write"},
                {"kind": "heading", "text": "Read", "section_path": "Statement / Read"},
                {"kind": "text", "text": "Read path stays query-only.", "section_path": "Statement / Read"},
            ],
        },
    )
    previous_run = AuditRun(
        target=AuditTarget(local_repo_path="/tmp/repo"),
        claims=[
            AuditClaimEntry(
                source_type="confluence_page",
                source_id="page-1",
                subject_kind="object_property",
                subject_key="Statement.read_path",
                predicate="documented_read",
                normalized_value="Read path stays query-only.",
                scope_kind="project",
                scope_key="FINAI",
                confidence=0.8,
                fingerprint="claim_read",
                metadata={
                    "matched_text": "Read path stays query-only.",
                    "evidence_anchor_kind": "document_line_range",
                    "evidence_anchor_value": "page-1#L5",
                    "evidence_section_path": "Contracts > Statement Contract > Statement > Read",
                    "evidence_line_start": 5,
                    "evidence_line_end": 5,
                    "evidence_url": current_document.url,
                    "title": current_document.title,
                    "path_hint": current_document.path_hint,
                },
            ),
            AuditClaimEntry(
                source_type="confluence_page",
                source_id="page-1",
                subject_kind="object_property",
                subject_key="Statement.write_path",
                predicate="documented_write",
                normalized_value="Approval gated.",
                scope_kind="project",
                scope_key="FINAI",
                confidence=0.8,
                fingerprint="claim_write",
                metadata={
                    "matched_text": "Approval gated.",
                    "evidence_anchor_kind": "document_line_range",
                    "evidence_anchor_value": "page-1#L3",
                    "evidence_section_path": "Contracts > Statement Contract > Statement > Write",
                    "evidence_line_start": 3,
                    "evidence_line_end": 3,
                    "evidence_url": current_document.url,
                    "title": current_document.title,
                    "path_hint": current_document.path_hint,
                },
            ),
        ],
    )

    regular_documents, regenerated_documents, reused_claim_records, notes = _prepare_section_level_confluence_regeneration(
        previous_run=previous_run,
        documents=[current_document],
    )

    assert regular_documents == []
    assert len(regenerated_documents) == 1
    assert regenerated_documents[0].metadata["section_regeneration_target"] == "Statement > Write"
    assert len(reused_claim_records) == 1
    assert reused_claim_records[0].claim.subject_key == "Statement.read_path"
    assert any("Claims aus unveraenderten Sektionen wiederverwendet" in note for note in notes)


def test_section_level_confluence_regeneration_drops_removed_section_claims_without_full_fallback() -> None:
    current_document = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="confluence_page",
            source_id="page-1",
            revision_id="9",
            content_hash="sha256:newer",
        ),
        source_type="confluence_page",
        source_id="page-1",
        title="Statement Contract",
        body="# Statement\n## Active\nOnly active section remains.\n",
        path_hint="Space FINAI / Contracts / Statement Contract",
        url="https://finius.atlassian.net/wiki/spaces/FINAI/pages/page-1/Statement+Contract",
        metadata={
            "ancestor_titles": ["Contracts"],
            "section_delta_status": "changed",
            "changed_section_paths": [],
            "added_section_paths": [],
            "removed_section_paths": ["Statement / Legacy"],
            "structured_blocks": [
                {"kind": "heading", "text": "Statement", "section_path": "Statement"},
                {"kind": "heading", "text": "Active", "section_path": "Statement / Active"},
                {"kind": "text", "text": "Only active section remains.", "section_path": "Statement / Active"},
            ],
        },
    )
    previous_run = AuditRun(
        target=AuditTarget(local_repo_path="/tmp/repo"),
        claims=[
            AuditClaimEntry(
                source_type="confluence_page",
                source_id="page-1",
                subject_kind="object_property",
                subject_key="Statement.lifecycle",
                predicate="documented_lifecycle",
                normalized_value="Only active section remains.",
                scope_kind="project",
                scope_key="FINAI",
                confidence=0.8,
                fingerprint="claim_active",
                metadata={
                    "matched_text": "Only active section remains.",
                    "evidence_anchor_kind": "document_line_range",
                    "evidence_anchor_value": "page-1#L3",
                    "evidence_section_path": "Contracts > Statement Contract > Statement > Active",
                    "evidence_line_start": 3,
                    "evidence_line_end": 3,
                    "evidence_url": current_document.url,
                    "title": current_document.title,
                    "path_hint": current_document.path_hint,
                },
            ),
            AuditClaimEntry(
                source_type="confluence_page",
                source_id="page-1",
                subject_kind="object_property",
                subject_key="Statement.policy",
                predicate="documented_policy",
                normalized_value="Legacy section exists.",
                scope_kind="project",
                scope_key="FINAI",
                confidence=0.8,
                fingerprint="claim_legacy",
                metadata={
                    "matched_text": "Legacy section exists.",
                    "evidence_anchor_kind": "document_line_range",
                    "evidence_anchor_value": "page-1#L6",
                    "evidence_section_path": "Contracts > Statement Contract > Statement > Legacy",
                    "evidence_line_start": 6,
                    "evidence_line_end": 6,
                    "evidence_url": current_document.url,
                    "title": current_document.title,
                    "path_hint": current_document.path_hint,
                },
            ),
        ],
    )

    regular_documents, regenerated_documents, reused_claim_records, notes = _prepare_section_level_confluence_regeneration(
        previous_run=previous_run,
        documents=[current_document],
    )

    assert regular_documents == []
    assert regenerated_documents == []
    assert [record.claim.subject_key for record in reused_claim_records] == ["Statement.lifecycle"]
    assert any("0 Abschnittsdokumente" in note for note in notes)


def test_pipeline_service_marks_semantic_delta_scopes_between_runs(monkeypatch, tmp_path: Path) -> None:
    repo_path = tmp_path / "repo"
    repo_path.mkdir()
    (repo_path / "src").mkdir()
    (repo_path / "_docs").mkdir()
    (repo_path / "src" / "statement_policy_service.py").write_text(
        "\n".join(
            [
                "def persist_statement():",
                "    # approval and allowlist are enforced here",
                "    return guarded_write()",
            ]
        ),
        encoding="utf-8",
    )
    doc_path = repo_path / "_docs" / "statement.md"
    doc_path.write_text(
        "# Statement\nWrite flow is approval-gated and review-only.\n",
        encoding="utf-8",
    )
    dump_path = tmp_path / "metamodel.json"
    dump_path.write_text(
        json.dumps(
            {
                "collected_at": "2026-03-14T00:00:00+00:00",
                "source": "local_dump",
                "rows": [{"phase_id": "001", "phase_name": "Scoping", "questions": []}],
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=dump_path,
        mothership_url="",
        license_key="",
        license_tenant="",
    )
    monkeypatch.setattr(Settings, "_collect_external_env_map", lambda self: {})
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    audit_service = AuditService(repository=repository, settings=settings)
    pipeline = AuditPipelineService(audit_service=audit_service, settings=settings, allow_remote_llm=False)

    first_run = audit_service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path=str(repo_path),
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    audit_service.claim_next_planned_run()
    pipeline.execute_run(run_id=first_run.run_id)

    doc_path.write_text(
        "# Statement\nDirect write without approval is allowed.\n",
        encoding="utf-8",
    )

    second_run = audit_service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path=str(repo_path),
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    audit_service.claim_next_planned_run()
    completed_second = pipeline.execute_run(run_id=second_run.run_id)

    policy_conflicts = [finding for finding in completed_second.findings if finding.category == "policy_conflict"]

    assert any(claim.metadata.get("delta_change_type") == "semantic" for claim in completed_second.claims)
    assert any(finding.metadata.get("delta_scope_affected") is True for finding in policy_conflicts)
    assert any("Neubewertung fokussiert auf" in entry.message for entry in completed_second.analysis_log)


def test_explicit_truth_scope_forces_recalculation_without_source_change() -> None:
    unchanged_claim = AuditClaimEntry(
        source_snapshot_id="snapshot_1",
        source_type="github_file",
        source_id="src/statement_service.py",
        subject_kind="object_property",
        subject_key="Statement.policy",
        predicate="implemented_policy",
        normalized_value="approval-gated",
        scope_kind="project",
        scope_key="FINAI",
        confidence=0.9,
        fingerprint="Statement.policy|implemented_policy|approval-gated|FINAI",
        metadata={"delta_status": "unchanged", "delta_scope_key": "Statement"},
    )
    explicit_truth = TruthLedgerEntry(
        canonical_key="Statement.policy|user_specification",
        subject_kind="object_property",
        subject_key="Statement.policy",
        predicate="user_specification",
        normalized_value="approval-gated",
        scope_kind="project",
        scope_key="FINAI",
        source_kind="user_specification",
    )

    impacted = _derive_impacted_scope_keys(
        claims=[unchanged_claim],
        inherited_truths=[explicit_truth],
    )

    assert "Statement" in impacted


def test_pipeline_service_reuses_unchanged_repo_documents_from_cache(monkeypatch, tmp_path: Path) -> None:
    repo_path = tmp_path / "repo"
    repo_path.mkdir()
    (repo_path / "src").mkdir()
    (repo_path / "_docs").mkdir()
    code_file = repo_path / "src" / "statement_service.py"
    doc_file = repo_path / "_docs" / "statement.md"
    code_file.write_text("def load_statement():\n    return query_statement()\n", encoding="utf-8")
    doc_file.write_text("# Statement\nWrite flow is approval-gated.\n", encoding="utf-8")
    dump_path = tmp_path / "metamodel.json"
    dump_path.write_text(
        json.dumps(
            {
                "collected_at": "2026-03-14T00:00:00+00:00",
                "source": "local_dump",
                "rows": [{"phase_id": "001", "phase_name": "Scoping", "questions": []}],
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=dump_path,
        mothership_url="",
        license_key="",
        license_tenant="",
    )
    monkeypatch.setattr(Settings, "_collect_external_env_map", lambda self: {})
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    audit_service = AuditService(repository=repository, settings=settings)
    pipeline = AuditPipelineService(audit_service=audit_service, settings=settings, allow_remote_llm=False)

    first_run = audit_service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path=str(repo_path),
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    audit_service.claim_next_planned_run()
    pipeline.execute_run(run_id=first_run.run_id)

    def fail_on_reread(*, file_path: Path) -> str | None:
        raise AssertionError(f"Unveraenderte Datei sollte aus dem Cache wiederverwendet werden: {file_path}")

    monkeypatch.setattr(github_connector, "_read_text_file", fail_on_reread)
    second_run = audit_service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path=str(repo_path),
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    audit_service.claim_next_planned_run()
    completed_second = pipeline.execute_run(run_id=second_run.run_id)

    assert completed_second.status == "completed"
    assert any("Inkrementelle Repo-Wiederverwendung" in entry.message for entry in completed_second.analysis_log)


def test_pipeline_service_reuses_cached_claims_and_findings_for_unchanged_follow_up_run(monkeypatch, tmp_path: Path) -> None:
    repo_path = tmp_path / "repo"
    repo_path.mkdir()
    (repo_path / "src").mkdir()
    (repo_path / "_docs").mkdir()
    (repo_path / "src" / "statement_service.py").write_text(
        "def persist_statement():\n    return save_statement()\n",
        encoding="utf-8",
    )
    (repo_path / "_docs" / "statement.md").write_text(
        "# Statement\nWrite flow is approval-gated and review-only.\n",
        encoding="utf-8",
    )
    dump_path = tmp_path / "metamodel.json"
    dump_path.write_text(
        json.dumps(
            {
                "collected_at": "2026-03-14T00:00:00+00:00",
                "source": "local_dump",
                "rows": [{"phase_id": "001", "phase_name": "Scoping", "questions": []}],
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=dump_path,
        mothership_url="",
        license_key="",
        license_tenant="",
    )
    monkeypatch.setattr(Settings, "_collect_external_env_map", lambda self: {})
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    audit_service = AuditService(repository=repository, settings=settings)
    pipeline = AuditPipelineService(audit_service=audit_service, settings=settings, allow_remote_llm=False)

    first_run = audit_service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path=str(repo_path),
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    audit_service.claim_next_planned_run()
    first_completed = pipeline.execute_run(run_id=first_run.run_id)
    assert first_completed.findings

    observed_source_types: list[set[str]] = []
    original_extract_claims = pipeline_service_module.extract_claim_records

    def observe_extract_claims(*, documents: list[object]) -> list[object]:
        observed_source_types.append({getattr(document, "source_type", "unknown") for document in documents})
        return original_extract_claims(documents=documents)

    def fail_if_called(*args: object, **kwargs: object) -> tuple[list[object], list[object]]:
        raise AssertionError("Finding-Neugenerierung sollte bei unveraendertem Folge-Lauf nicht erneut stattfinden.")

    monkeypatch.setattr(pipeline_service_module, "extract_claim_records", observe_extract_claims)
    monkeypatch.setattr(pipeline_service_module, "generate_findings", fail_if_called)

    second_run = audit_service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path=str(repo_path),
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    audit_service.claim_next_planned_run()
    second_completed = pipeline.execute_run(run_id=second_run.run_id)

    assert second_completed.findings
    assert len(second_completed.findings) == len(first_completed.findings)
    assert observed_source_types
    assert all(source_types <= {"metamodel"} for source_types in observed_source_types)
    assert any("Claim-Reuse:" in entry.message for entry in second_completed.analysis_log)


def test_pipeline_service_persists_semantic_graph_and_enriches_packages(monkeypatch, tmp_path: Path) -> None:
    repo_path = tmp_path / "repo"
    repo_path.mkdir()
    (repo_path / "src").mkdir()
    (repo_path / "_docs").mkdir()
    (repo_path / "src" / "statement_service.py").write_text(
        "\n".join(
            [
                "def persist_statement():",
                "    # without approval direct write is allowed",
                "    return save_statement()",
            ]
        ),
        encoding="utf-8",
    )
    (repo_path / "_docs" / "bsm.md").write_text(
        "\n".join(
            [
                "# Phase: Scoping",
                "## Question: Problem Statement",
                "Question: Problem Statement remains mandatory.",
            ]
        ),
        encoding="utf-8",
    )
    (repo_path / "_docs" / "statement.md").write_text(
        "# Statement\nWrite flow is approval-gated and review-only.\n",
        encoding="utf-8",
    )
    dump_path = tmp_path / "metamodel.json"
    dump_path.write_text(
        json.dumps(
            {
                "collected_at": "2026-03-14T00:00:00+00:00",
                "source": "local_dump",
                "rows": [
                    {
                        "phase_id": "001",
                        "phase_name": "Scoping",
                        "phase_order": "001",
                        "questions": [{"question_id": "q1", "question_text": "Problem Statement"}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=dump_path,
        mothership_url="",
        license_key="",
        license_tenant="",
    )
    monkeypatch.setattr(Settings, "_collect_external_env_map", lambda self: {})
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    audit_service = AuditService(repository=repository, settings=settings)
    pipeline = AuditPipelineService(audit_service=audit_service, settings=settings, allow_remote_llm=False)

    created = audit_service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path=str(repo_path),
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    audit_service.claim_next_planned_run()
    completed = pipeline.execute_run(run_id=created.run_id)

    assert completed.semantic_entities
    assert completed.semantic_relations
    assert any(entity.entity_type == "phase" for entity in completed.semantic_entities)
    assert any(entity.entity_type == "question" for entity in completed.semantic_entities)
    assert any(relation.relation_type == "belongs_to" for relation in completed.semantic_relations)
    assert completed.decision_packages
    assert any(package.metadata.get("semantic_context") for package in completed.decision_packages)
    assert any("Semantikfokus:" in package.recommendation_summary for package in completed.decision_packages)


def test_pipeline_service_rebuilds_finding_links_after_embedding_findings(monkeypatch, tmp_path: Path) -> None:
    repo_path = tmp_path / "repo"
    repo_path.mkdir()
    (repo_path / "src").mkdir()
    (repo_path / "_docs").mkdir()
    (repo_path / "src" / "statement_service.py").write_text(
        "def persist_statement():\n    return save_statement()\n",
        encoding="utf-8",
    )
    (repo_path / "_docs" / "statement.md").write_text(
        "# Statement\nWrite flow is approval-gated and review-only.\n",
        encoding="utf-8",
    )
    dump_path = tmp_path / "metamodel.json"
    dump_path.write_text(
        json.dumps(
            {
                "collected_at": "2026-03-14T00:00:00+00:00",
                "source": "local_dump",
                "rows": [{"phase_id": "001", "phase_name": "Scoping", "questions": []}],
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        database_path=tmp_path / "auditor.db",
        metamodel_dump_path=dump_path,
        mothership_url="",
        license_key="",
        license_tenant="",
    )
    monkeypatch.setattr(Settings, "_collect_external_env_map", lambda self: {})
    repository = SQLiteAuditRepository(db_path=settings.database_path)
    audit_service = AuditService(repository=repository, settings=settings)
    pipeline = AuditPipelineService(audit_service=audit_service, settings=settings, allow_remote_llm=False)

    first_run = audit_service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path=str(repo_path),
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    audit_service.claim_next_planned_run()
    first_completed = pipeline.execute_run(run_id=first_run.run_id)
    assert first_completed.findings

    embedding_finding = AuditFinding(
        severity="high",
        category="contradiction",
        title="Embedding-Widerspruch",
        summary="Zusätzlicher semantischer Widerspruch fuer denselben Scope.",
        recommendation="Scope gemeinsam pruefen.",
        canonical_key=str(first_completed.findings[0].canonical_key),
    )

    monkeypatch.setattr(
        "fin_ai_auditor.services.embedding_contradiction_detector.detect_cross_document_contradictions",
        lambda **kwargs: [embedding_finding],
    )

    second_run = audit_service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path=str(repo_path),
                github_ref="main",
                confluence_space_keys=["FINAI"],
                jira_project_keys=["FINAI"],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    audit_service.claim_next_planned_run()
    second_completed = pipeline.execute_run(run_id=second_run.run_id)

    assert any(
        embedding_finding.finding_id in {link.from_finding_id, link.to_finding_id}
        for link in second_completed.finding_links
    )
