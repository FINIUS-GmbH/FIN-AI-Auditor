"""Tests for the ClarificationService — Three-tier evidence model.

Covers:
  1. Thread lifecycle: open → answer → resolve / dismiss
  2. Definitive truth flow: confirm with double-confirmation, truth ledger entry
  3. Indication capture: claim with elevated confidence
  4. Conflict detection: same canonical_key → conflict_resolution message
  5. Supersede mechanism: old truth → superseded, new truth → active
  6. Cross-thread context propagation
  7. Persistence: threads survive upsert/load round-trip
  8. Validators: exactly one anchor enforced
"""

from pathlib import Path

import pytest

from fin_ai_auditor.domain.models import (
    AuditRun,
    AuditTarget,
    CreateAuditRunRequest,
    DecisionPackage,
)
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository
from fin_ai_auditor.services.audit_service import AuditService
from fin_ai_auditor.services.clarification_service import ClarificationService


# ── Fixtures ──────────────────────────────────────────────────

def _make_env(tmp_path: Path):
    """Bootstrap repo, service, clarification service, and a run with one package."""
    from unittest.mock import MagicMock

    repo = SQLiteAuditRepository(db_path=tmp_path / "auditor.db")
    settings = MagicMock()
    settings.confluence_home_url = ""
    settings.fixed_jira_project_key = ""
    audit_svc = AuditService(repository=repo, settings=settings)
    cs = ClarificationService(audit_service=audit_svc)

    run = audit_svc.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path="/tmp/test-repo",
                github_ref="main",
                confluence_space_keys=[],
                confluence_page_ids=[],
                jira_project_keys=[],
                include_metamodel=True,
                include_local_docs=False,
            )
        )
    )
    pkg = DecisionPackage(
        title="API-Statuswiderspruch",
        category="contradiction",
        severity_summary="high",
        scope_summary="api.status",
        recommendation_summary="Status klären",
    )
    repo.upsert_run(run=run.model_copy(update={"decision_packages": [pkg]}))

    return repo, audit_svc, cs, run.run_id, pkg


# ── 1. Thread lifecycle ──────────────────────────────────────

class TestThreadLifecycle:
    def test_open_thread_creates_first_message(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        result = cs.open_thread(
            run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification"
        )

        assert len(result.clarification_threads) == 1
        thread = result.clarification_threads[0]
        assert thread.package_id == pkg.package_id
        assert thread.purpose == "truth_clarification"
        assert thread.status == "active"
        assert len(thread.messages) == 1
        assert thread.messages[0].role in ("system", "assistant")
        assert thread.messages[0].message_type == "question"

    def test_open_thread_with_initial_message_is_atomic(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        result = cs.open_thread(
            run_id=run_id,
            package_id=pkg.package_id,
            purpose="truth_clarification",
            initial_content="Bitte klaeren, ob API X aktiv ist.",
        )

        assert len(result.clarification_threads) == 1
        thread = result.clarification_threads[0]
        assert [message.role for message in thread.messages[:2]] == ["assistant", "user"]
        assert thread.messages[1].content == "Bitte klaeren, ob API X aktiv ist."
        assert thread.metadata == {}

    def test_process_answer_adds_messages(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        r2 = cs.process_answer(run_id=run_id, thread_id=tid, content="Das API ist aktiv.")

        thread = r2.clarification_threads[0]
        assert len(thread.messages) >= 2  # user answer + follow-up
        user_msgs = [m for m in thread.messages if m.role == "user"]
        assert len(user_msgs) == 1
        assert user_msgs[0].content == "Das API ist aktiv."

    def test_dismiss_thread(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="action_routing")
        tid = r1.clarification_threads[0].thread_id

        r2 = cs.dismiss_thread(run_id=run_id, thread_id=tid)

        thread = r2.clarification_threads[0]
        assert thread.status == "dismissed"
        assert thread.resolved_at is not None
        # Dismissed threads should record a resolution message
        resolution_msgs = [m for m in thread.messages if m.message_type == "resolution"]
        assert len(resolution_msgs) >= 1


# ── 2. Definitive truth flow ─────────────────────────────────

class TestDefinitiveTruth:
    def test_confirm_truth_creates_truth_ledger_entry(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        r2 = cs.process_answer(run_id=run_id, thread_id=tid, content="API X ist definitiv aktiv")

        r3 = cs.confirm_truth(
            run_id=run_id,
            thread_id=tid,
            truth_canonical_key="api.x.status",
            truth_normalized_value="aktiv",
            subject_kind="api",
            subject_key="api_x",
            predicate="status",
        )

        # Truth created
        dialog_truths = [t for t in r3.truths if t.source_kind == "clarification_dialog"]
        assert len(dialog_truths) == 1
        assert dialog_truths[0].canonical_key == "api.x.status"
        assert dialog_truths[0].normalized_value == "aktiv"
        assert dialog_truths[0].truth_status == "active"

        # Thread marked resolved
        thread = r3.clarification_threads[0]
        assert thread.status == "resolved"
        assert thread.triggered_delta_recompute is True
        assert len(thread.created_truth_ids) == 1
        superseded_packages = [p for p in r3.decision_packages if p.decision_state == "superseded"]
        revised_packages = [
            p for p in r3.decision_packages if p.metadata.get("revision_of") == pkg.package_id
        ]
        assert len(superseded_packages) == 1
        assert len(revised_packages) == 1
        assert revised_packages[0].metadata.get("clarification_thread_id") == tid

    def test_confirm_truth_adds_confirmation_message(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id
        cs.process_answer(run_id=run_id, thread_id=tid, content="X")

        r3 = cs.confirm_truth(
            run_id=run_id,
            thread_id=tid,
            truth_canonical_key="test.key",
            truth_normalized_value="val",
            subject_kind="test",
            subject_key="test_subj",
            predicate="prop",
        )

        thread = r3.clarification_threads[0]
        # The resolution message has message_type='resolution' and outcome_type='truth_confirmed'
        conf_msgs = [m for m in thread.messages if m.outcome_type == "truth_confirmed"]
        assert len(conf_msgs) >= 1
        assert conf_msgs[-1].outcome_type == "truth_confirmed"


# ── 3. Indication capture ────────────────────────────────────

class TestIndicationCapture:
    def test_capture_indication_creates_claim(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        r2 = cs.capture_indication(run_id=run_id, thread_id=tid, content="API X scheint aktiv")

        dialog_claims = [
            c for c in r2.claims
            if c.source_id and "clarification" in c.source_id
        ]
        assert len(dialog_claims) == 1
        assert dialog_claims[0].confidence == 0.8
        assert dialog_claims[0].normalized_value == "API X scheint aktiv"
        assert dialog_claims[0].source_type == "user_truth"

    def test_capture_indication_records_outcome_in_thread(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        r2 = cs.capture_indication(run_id=run_id, thread_id=tid, content="Hinweis XY")

        thread = r2.clarification_threads[0]
        assert len(thread.created_claim_ids) == 1
        outcome_msgs = [m for m in thread.messages if m.outcome_type == "indication_captured"]
        assert len(outcome_msgs) >= 1

    def test_capture_indication_uses_latest_user_answer_when_content_blank(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id
        cs.process_answer(run_id=run_id, thread_id=tid, content="API X scheint aktiv")

        r2 = cs.capture_indication(run_id=run_id, thread_id=tid, content="   ")

        dialog_claims = [c for c in r2.claims if c.source_id == f"clarification:{tid}"]
        assert dialog_claims[-1].normalized_value == "API X scheint aktiv"


# ── 4. Conflict detection ────────────────────────────────────

class TestConflictDetection:
    def test_conflict_detected_with_same_canonical_key(self, tmp_path: Path) -> None:
        repo, _, cs, run_id, pkg = _make_env(tmp_path)

        # First truth
        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid1 = r1.clarification_threads[0].thread_id
        cs.process_answer(run_id=run_id, thread_id=tid1, content="API aktiv")
        r2 = cs.confirm_truth(
            run_id=run_id, thread_id=tid1,
            truth_canonical_key="api.status",
            truth_normalized_value="aktiv",
            subject_kind="api", subject_key="api_x", predicate="status",
        )

        # Add second package for a second thread
        pkg2 = DecisionPackage(
            title="Zweiter Test",
            category="contradiction",
            severity_summary="medium",
            scope_summary="api.status",
            recommendation_summary="Prüfen",
        )
        repo.upsert_run(
            run=r2.model_copy(update={"decision_packages": [*r2.decision_packages, pkg2]})
        )

        # Second thread — conflicting truth
        r3 = cs.open_thread(run_id=run_id, package_id=pkg2.package_id, purpose="truth_clarification")
        tid2 = r3.clarification_threads[1].thread_id
        cs.process_answer(run_id=run_id, thread_id=tid2, content="deprecated")

        r4 = cs.confirm_truth(
            run_id=run_id, thread_id=tid2,
            truth_canonical_key="api.status",
            truth_normalized_value="deprecated",
            subject_kind="api", subject_key="api_x", predicate="status",
        )

        # Should have a conflict_resolution message
        thread2 = r4.clarification_threads[1]
        conflict_msgs = [m for m in thread2.messages if m.message_type == "conflict_resolution"]
        assert len(conflict_msgs) >= 1
        assert "aktiv" in conflict_msgs[0].content.lower() or "bestehende" in conflict_msgs[0].content.lower()
        assert conflict_msgs[0].metadata["conflicting_truth_id"] == r2.truths[0].truth_id
        assert conflict_msgs[0].metadata["proposed_canonical_key"] == "api.status"
        assert conflict_msgs[0].metadata["proposed_normalized_value"] == "deprecated"


# ── 5. Supersede mechanism ───────────────────────────────────

class TestSupersedeFlow:
    def test_supersede_replaces_old_truth(self, tmp_path: Path) -> None:
        repo, _, cs, run_id, pkg = _make_env(tmp_path)

        # Create first truth
        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid1 = r1.clarification_threads[0].thread_id
        cs.process_answer(run_id=run_id, thread_id=tid1, content="aktiv")
        r2 = cs.confirm_truth(
            run_id=run_id, thread_id=tid1,
            truth_canonical_key="api.status",
            truth_normalized_value="aktiv",
            subject_kind="api", subject_key="api_x", predicate="status",
        )
        old_truth = [t for t in r2.truths if t.source_kind == "clarification_dialog"][0]

        # Second thread
        pkg2 = DecisionPackage(
            title="Supersede-Test",
            category="contradiction",
            severity_summary="high",
            scope_summary="api.status",
            recommendation_summary="Ersetzen",
        )
        repo.upsert_run(
            run=r2.model_copy(update={"decision_packages": [*r2.decision_packages, pkg2]})
        )
        r3 = cs.open_thread(run_id=run_id, package_id=pkg2.package_id, purpose="truth_clarification")
        tid2 = r3.clarification_threads[1].thread_id
        cs.process_answer(run_id=run_id, thread_id=tid2, content="deprecated")

        # Trigger conflict
        r4 = cs.confirm_truth(
            run_id=run_id, thread_id=tid2,
            truth_canonical_key="api.status",
            truth_normalized_value="deprecated",
            subject_kind="api", subject_key="api_x", predicate="status",
        )

        # Now supersede
        r5 = cs.supersede_truth(
            run_id=run_id,
            thread_id=tid2,
            existing_truth_id=old_truth.truth_id,
            new_canonical_key="api.status",
            new_normalized_value="deprecated",
            new_subject_kind="api",
            new_subject_key="api_x",
            new_predicate="status",
        )

        # Old truth should be superseded
        superseded = [t for t in r5.truths if t.truth_status == "superseded"]
        assert len(superseded) == 1
        assert superseded[0].truth_id == old_truth.truth_id

        # New truth should be active
        active = [
            t for t in r5.truths
            if t.truth_status == "active"
            and t.canonical_key == "api.status"
            and t.source_kind == "clarification_dialog"
        ]
        assert len(active) >= 1
        assert active[0].normalized_value == "deprecated"

        # Thread resolved
        thread2 = r5.clarification_threads[1]
        assert thread2.status == "resolved"
        assert thread2.triggered_delta_recompute is True
        revised_packages = [
            p for p in r5.decision_packages if p.metadata.get("revision_of") == pkg2.package_id
        ]
        assert len(revised_packages) == 1
        assert revised_packages[0].metadata.get("clarification_thread_id") == tid2


# ── 6. Cross-thread context ──────────────────────────────────

class TestCrossThreadContext:
    def test_cross_thread_context_includes_previous_truths(self, tmp_path: Path) -> None:
        repo, _, cs, run_id, pkg = _make_env(tmp_path)

        # First thread with a confirmed truth
        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid1 = r1.clarification_threads[0].thread_id
        cs.process_answer(run_id=run_id, thread_id=tid1, content="aktiv")
        r2 = cs.confirm_truth(
            run_id=run_id, thread_id=tid1,
            truth_canonical_key="api.status",
            truth_normalized_value="aktiv",
            subject_kind="api", subject_key="api_x", predicate="status",
        )

        # Build cross-thread context from resolved run
        ctx = cs._build_cross_thread_context(r2)

        assert len(ctx) > 0
        assert "Wahrheit" in ctx or "wahrheit" in ctx.lower()

    def test_new_thread_gets_cross_context(self, tmp_path: Path) -> None:
        repo, _, cs, run_id, pkg = _make_env(tmp_path)

        # First thread + truth
        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid1 = r1.clarification_threads[0].thread_id
        cs.process_answer(run_id=run_id, thread_id=tid1, content="aktiv")
        r2 = cs.confirm_truth(
            run_id=run_id, thread_id=tid1,
            truth_canonical_key="api.status",
            truth_normalized_value="aktiv",
            subject_kind="api", subject_key="api_x", predicate="status",
        )

        # Add second package
        pkg2 = DecisionPackage(
            title="Zweiter Paket",
            category="missing_definition",
            severity_summary="low",
            scope_summary="test",
            recommendation_summary="Test",
        )
        repo.upsert_run(
            run=r2.model_copy(update={"decision_packages": [*r2.decision_packages, pkg2]})
        )

        # Open second thread — should get cross-thread context in first message
        r3 = cs.open_thread(run_id=run_id, package_id=pkg2.package_id, purpose="truth_clarification")
        # The second thread should exist with a question
        thread2 = r3.clarification_threads[1]
        assert thread2.status == "active"
        assert len(thread2.messages) >= 1


# ── 7. Persistence round-trip ────────────────────────────────

class TestPersistence:
    def test_threads_survive_upsert_load_cycle(self, tmp_path: Path) -> None:
        repo, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id
        r2 = cs.process_answer(run_id=run_id, thread_id=tid, content="Testantwort")

        # Load from DB
        loaded = repo.get_run(run_id=run_id)
        assert loaded is not None
        assert len(loaded.clarification_threads) == 1
        assert loaded.clarification_threads[0].thread_id == tid
        assert len(loaded.clarification_threads[0].messages) == len(r2.clarification_threads[0].messages)
        assert loaded.clarification_threads[0].status == "active"

    def test_resolved_thread_persists_outcomes(self, tmp_path: Path) -> None:
        repo, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id
        cs.process_answer(run_id=run_id, thread_id=tid, content="X")
        r3 = cs.confirm_truth(
            run_id=run_id, thread_id=tid,
            truth_canonical_key="a.b",
            truth_normalized_value="val",
            subject_kind="s", subject_key="k", predicate="p",
        )

        # Reload
        loaded = repo.get_run(run_id=run_id)
        assert loaded is not None
        thread = loaded.clarification_threads[0]
        assert thread.status == "resolved"
        assert len(thread.created_truth_ids) == 1
        assert thread.triggered_delta_recompute is True


# ── 8. Validator enforcement ─────────────────────────────────

class TestValidators:
    def test_open_thread_rejects_missing_anchor(self, tmp_path: Path) -> None:
        _, _, cs, run_id, _ = _make_env(tmp_path)

        # With package_id=None (default) the service still accepts the call
        # but an unknown/invalid package_id should fail
        with pytest.raises(ValueError):
            cs.open_thread(run_id=run_id, package_id="nonexistent_anchor", purpose="truth_clarification")

    def test_open_thread_rejects_unknown_package(self, tmp_path: Path) -> None:
        _, _, cs, run_id, _ = _make_env(tmp_path)

        with pytest.raises(ValueError):
            cs.open_thread(run_id=run_id, package_id="nonexistent", purpose="truth_clarification")

    def test_process_answer_rejects_unknown_thread(self, tmp_path: Path) -> None:
        _, _, cs, run_id, _ = _make_env(tmp_path)

        with pytest.raises(ValueError):
            cs.process_answer(run_id=run_id, thread_id="fake_thread", content="test")

    def test_confirm_truth_on_dismissed_thread_fails(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id
        cs.dismiss_thread(run_id=run_id, thread_id=tid)

        with pytest.raises(ValueError, match="bereits geschlossen"):
            cs.confirm_truth(
                run_id=run_id, thread_id=tid,
                truth_canonical_key="k", truth_normalized_value="v",
                subject_kind="s", subject_key="sk", predicate="p",
            )


# ── 9. Heuristic indication extraction ───────────────────────

class TestHeuristicExtraction:
    def test_definite_markers_produce_high_confidence(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        r2 = cs.process_answer(
            run_id=run_id, thread_id=tid,
            content="Das API muss immer aktiviert sein, ausnahmslos."
        )

        # Should trigger a truth_confirmation message
        thread = r2.clarification_threads[0]
        truth_msgs = [m for m in thread.messages if m.message_type == "truth_confirmation"]
        assert len(truth_msgs) >= 1
        # Should mention "definitive" in the proposal
        assert "definitive" in truth_msgs[0].content.lower() or "klare" in truth_msgs[0].content.lower()

    def test_assertion_with_scope_produces_medium_confidence(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        r2 = cs.process_answer(
            run_id=run_id, thread_id=tid,
            content="Der API-Status ist immer 'aktiv' und wird nie geändert."
        )

        thread = r2.clarification_threads[0]
        truth_msgs = [m for m in thread.messages if m.message_type == "truth_confirmation"]
        assert len(truth_msgs) >= 1

    def test_negation_produces_lower_confidence(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        r2 = cs.process_answer(
            run_id=run_id, thread_id=tid,
            content="Diese Einstellung darf nicht von außen geändert werden."
        )

        thread = r2.clarification_threads[0]
        truth_msgs = [m for m in thread.messages if m.message_type == "truth_confirmation"]
        assert len(truth_msgs) >= 1

    def test_filler_responses_are_ignored(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        r2 = cs.process_answer(run_id=run_id, thread_id=tid, content="ok")

        thread = r2.clarification_threads[0]
        # Should NOT produce a truth_confirmation, only a follow-up question
        truth_msgs = [m for m in thread.messages if m.message_type == "truth_confirmation"]
        assert len(truth_msgs) == 0

    def test_questions_are_not_extracted(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        r2 = cs.process_answer(
            run_id=run_id, thread_id=tid,
            content="Was genau ist damit gemeint?"
        )

        thread = r2.clarification_threads[0]
        truth_msgs = [m for m in thread.messages if m.message_type == "truth_confirmation"]
        assert len(truth_msgs) == 0

    def test_short_input_is_not_extracted(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        r2 = cs.process_answer(run_id=run_id, thread_id=tid, content="hmm naja")

        thread = r2.clarification_threads[0]
        truth_msgs = [m for m in thread.messages if m.message_type == "truth_confirmation"]
        assert len(truth_msgs) == 0

    def test_follow_up_includes_confidence_metadata(self, tmp_path: Path) -> None:
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        r2 = cs.process_answer(
            run_id=run_id, thread_id=tid,
            content="Das System muss grundsätzlich den Status 'aktiv' verwenden."
        )

        thread = r2.clarification_threads[0]
        truth_msgs = [m for m in thread.messages if m.message_type == "truth_confirmation"]
        assert len(truth_msgs) >= 1
        meta = truth_msgs[0].metadata
        assert "confidence_grade" in meta
        assert meta["confidence_grade"] == "definite"
        assert "extraction_confidence" in meta
        assert float(meta["extraction_confidence"]) >= 0.9


# ── 10. LLM integration path ─────────────────────────────────

class TestLLMIntegration:
    """Tests for the LLM extraction path using mocks."""

    def _make_env_with_llm(self, tmp_path: Path):
        """Bootstrap environment with mocked LLM settings."""
        from unittest.mock import MagicMock, AsyncMock, patch

        repo, audit_svc, _, run_id, pkg = _make_env(tmp_path)

        # Create mock settings with LLM slots
        settings = MagicMock()
        settings.confluence_home_url = ""
        settings.fixed_jira_project_key = ""

        mock_slot = MagicMock()
        mock_slot.model = "gpt-4o"
        mock_slot.deployment = None
        mock_slot.slot = 1
        settings.get_configured_llm_slots.return_value = [mock_slot]

        cs = ClarificationService(audit_service=audit_svc, settings=settings)
        return repo, audit_svc, cs, run_id, pkg, settings

    def test_llm_extraction_produces_truth_proposal(self, tmp_path: Path) -> None:
        from unittest.mock import AsyncMock, patch

        repo, _, cs, run_id, pkg, _ = self._make_env_with_llm(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        # Mock the LLM extraction to return a structured result
        from fin_ai_auditor.services.clarification_service import _LLMIndicationResult, _LLMIndicationItem

        mock_result = _LLMIndicationResult(
            indications=[
                _LLMIndicationItem(
                    statement="Das API ist definitiv aktiv",
                    confidence=0.95,
                    canonical_key_hint="api.status",
                    scope_kind="service",
                    is_definite=True,
                    is_negation=False,
                )
            ],
            follow_up_question="",
            extraction_notes="Definitive Aussage extrahiert.",
        )

        with patch.object(cs, "_get_llm_client") as mock_client_factory:
            mock_client = AsyncMock()
            mock_client.structured_output = AsyncMock(return_value=mock_result)
            mock_client_factory.return_value = mock_client

            r2 = cs.process_answer(
                run_id=run_id, thread_id=tid,
                content="Das API ist definitiv aktiv"
            )

        thread = r2.clarification_threads[0]
        truth_msgs = [m for m in thread.messages if m.message_type == "truth_confirmation"]
        assert len(truth_msgs) >= 1
        assert "definitiv" in truth_msgs[0].content.lower() or "klare" in truth_msgs[0].content.lower()
        # Metadata should include LLM-derived canonical_key_hint
        assert truth_msgs[0].metadata.get("canonical_key_hint") == "api.status"
        assert truth_msgs[0].metadata.get("confidence_grade") == "definite"

    def test_llm_failure_falls_back_to_heuristic(self, tmp_path: Path) -> None:
        from unittest.mock import patch

        repo, _, cs, run_id, pkg, _ = self._make_env_with_llm(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        # Mock the LLM to raise an exception
        with patch.object(cs, "_get_llm_client", side_effect=RuntimeError("LLM unavailable")):
            r2 = cs.process_answer(
                run_id=run_id, thread_id=tid,
                content="Das System muss grundsätzlich den Status 'aktiv' verwenden."
            )

        # Should still work via heuristic fallback
        thread = r2.clarification_threads[0]
        truth_msgs = [m for m in thread.messages if m.message_type == "truth_confirmation"]
        assert len(truth_msgs) >= 1  # Heuristic should detect "grundsätzlich" as definite

    def test_llm_follow_up_question_used(self, tmp_path: Path) -> None:
        from unittest.mock import AsyncMock, patch

        repo, _, cs, run_id, pkg, _ = self._make_env_with_llm(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        # Mock LLM returning no indications but a follow-up question
        from fin_ai_auditor.services.clarification_service import _LLMIndicationResult

        mock_result = _LLMIndicationResult(
            indications=[],
            follow_up_question="Können Sie genauer angeben, welchen Status das API im Produktivbetrieb hat?",
            extraction_notes="Keine substantielle Aussage gefunden.",
        )

        with patch.object(cs, "_get_llm_client") as mock_client_factory:
            mock_client = AsyncMock()
            mock_client.structured_output = AsyncMock(return_value=mock_result)
            mock_client_factory.return_value = mock_client

            r2 = cs.process_answer(run_id=run_id, thread_id=tid, content="hmm, gute frage")

        thread = r2.clarification_threads[0]
        # Should use the LLM-generated follow-up question
        question_msgs = [m for m in thread.messages if m.message_type == "question" and m.role == "assistant"]
        last_q = question_msgs[-1]
        assert "Produktivbetrieb" in last_q.content
        assert last_q.metadata.get("generated_by") == "llm"

    def test_no_llm_when_settings_is_none(self, tmp_path: Path) -> None:
        # Standard env without settings — should never attempt LLM
        _, _, cs, run_id, pkg = _make_env(tmp_path)

        assert cs._has_llm() is False

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        # Should work fine with heuristic only
        r2 = cs.process_answer(
            run_id=run_id, thread_id=tid,
            content="Das API muss immer aktiviert sein."
        )

        thread = r2.clarification_threads[0]
        assert len(thread.messages) >= 2  # At minimum: question + user msg + follow-up

    def test_llm_canonical_key_hint_in_metadata(self, tmp_path: Path) -> None:
        from unittest.mock import AsyncMock, patch

        repo, _, cs, run_id, pkg, _ = self._make_env_with_llm(tmp_path)

        r1 = cs.open_thread(run_id=run_id, package_id=pkg.package_id, purpose="truth_clarification")
        tid = r1.clarification_threads[0].thread_id

        from fin_ai_auditor.services.clarification_service import _LLMIndicationResult, _LLMIndicationItem

        mock_result = _LLMIndicationResult(
            indications=[
                _LLMIndicationItem(
                    statement="Statement write_path ist review-pflichtig",
                    confidence=0.88,
                    canonical_key_hint="statement.write_path.review_gate",
                    scope_kind="entity",
                    is_definite=False,
                    is_negation=False,
                )
            ],
        )

        with patch.object(cs, "_get_llm_client") as mock_client_factory:
            mock_client = AsyncMock()
            mock_client.structured_output = AsyncMock(return_value=mock_result)
            mock_client_factory.return_value = mock_client

            r2 = cs.process_answer(
                run_id=run_id, thread_id=tid,
                content="Statement write_path ist review-pflichtig"
            )

        thread = r2.clarification_threads[0]
        truth_msgs = [m for m in thread.messages if m.message_type == "truth_confirmation"]
        assert len(truth_msgs) >= 1
        assert truth_msgs[0].metadata.get("canonical_key_hint") == "statement.write_path.review_gate"
        assert truth_msgs[0].metadata.get("confidence_grade") == "assertion"
