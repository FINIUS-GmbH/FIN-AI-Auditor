"""Clarification Dialog Service — paketgebundener Klärungsdialog.

Drei-Stufen-Evidenzmodell:
  💎 Definitive Wahrheit — doppelte Bestätigung, ausnahmslos
  🔹 Indiz — erhöhte Confidence, kein Override
  💬 Kontextwissen — nur Protokoll

Der Dialog ist lokal an eine Karte gebunden,
aber jede Erkenntnis fließt global in das Gesamtsystem.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field

from fin_ai_auditor.domain.models import (
    AuditAnalysisLogEntry,
    AuditClaimEntry,
    AuditRun,
    ClarificationMessage,
    ClarificationOutcomeType,
    ClarificationThread,
    DecisionPackage,
    TruthLedgerEntry,
    utc_now_iso,
    new_claim_id,
    new_truth_id,
)

if TYPE_CHECKING:
    from fin_ai_auditor.config import Settings
    from fin_ai_auditor.services.audit_service import AuditService

logger = logging.getLogger(__name__)

# ── Constants ──
MAX_MESSAGES_PER_THREAD = 10
MAX_QUESTIONS_PER_STEP = 3
INDICATION_CONFIDENCE = 0.80
LLM_EXTRACTION_TIMEOUT_S = 30.0


# ── Pydantic schemas for LLM structured output ──

class _LLMIndicationItem(BaseModel):
    """A single extracted indication from user input."""
    statement: str = Field(description="The core factual assertion from the user")
    confidence: float = Field(ge=0.0, le=1.0, description="Extraction confidence 0.0-1.0")
    canonical_key_hint: str = Field(default="", description="Suggested canonical key, e.g. 'api.status'")
    scope_kind: str = Field(default="global", description="Scope category: global, module, service, entity")
    is_definite: bool = Field(default=False, description="True if user expressed absolute certainty")
    is_negation: bool = Field(default=False, description="True if user expressed an exclusion/prohibition")


class _LLMIndicationResult(BaseModel):
    """Structured output from LLM indication extraction."""
    indications: list[_LLMIndicationItem] = Field(default_factory=list)
    follow_up_question: str = Field(
        default="",
        description="A context-aware follow-up question to ask the user, or empty if extraction is sufficient",
    )
    extraction_notes: str = Field(default="", description="Internal extraction notes")
    resolution_status: Literal["needs_more", "resolved"] = Field(
        default="needs_more",
        description="'resolved' wenn alle Fragen geklärt sind und eine überarbeitete Empfehlung formuliert werden kann. 'needs_more' wenn noch Rückfragen nötig sind."
    )
    resolution_summary: str = Field(
        default="",
        description="Nur wenn resolved: Zusammenfassung aller geklärten Punkte"
    )


class _LLMFollowUpResult(BaseModel):
    """Structured output from LLM follow-up question generation."""
    question: str = Field(description="The follow-up question to ask")
    question_type: str = Field(default="clarification", description="Type: clarification, confirmation, summary")


class ClarificationService:
    """Manages clarification dialogs bound to decision packages or atomic facts."""

    def __init__(
        self,
        *,
        audit_service: AuditService,
        settings: Settings | None = None,
    ) -> None:
        self._audit = audit_service
        self._settings = settings
        self._llm_client = None  # Lazy-initialized

    # ──────────────────────────────────────
    # Public API
    # ──────────────────────────────────────

    def open_thread(
        self,
        *,
        run_id: str,
        package_id: str | None = None,
        atomic_fact_id: str | None = None,
        review_card_id: str | None = None,
        purpose: str,
        initial_content: str | None = None,
    ) -> AuditRun:
        """Open a new clarification thread and generate the first question."""
        run = self._require_run(run_id)
        self._validate_anchor(run, package_id=package_id, atomic_fact_id=atomic_fact_id)

        # Build context from the package/fact and all prior threads
        context_summary = self._build_cross_thread_context(run)
        anchor_context = self._build_anchor_context(run, package_id=package_id, atomic_fact_id=atomic_fact_id)
        first_question = self._generate_initial_question(
            purpose=purpose,
            anchor_context=anchor_context,
            global_context=context_summary,
        )

        thread = ClarificationThread(
            run_id=run_id,
            package_id=package_id,
            atomic_fact_id=atomic_fact_id,
            purpose=purpose,
            messages=[first_question],
            metadata={
                "review_card_id": review_card_id,
            }
        )

        if initial_content and initial_content.strip():
            thread = self._process_answer_in_thread(
                run=run,
                thread=thread,
                content=initial_content.strip(),
            )

        log_entry = AuditAnalysisLogEntry(
            source_type="clarification_dialog",
            title="Klärungsdialog gestartet",
            message=f"Neuer Klärungsdialog ({purpose}) für "
                    f"{'Paket ' + (package_id or '') if package_id else 'Fakt ' + (atomic_fact_id or '')}.",
            derived_changes=[f"Thread {thread.thread_id} wurde eröffnet."],
            impact_summary=["Erkenntnisse aus diesem Dialog können alle Bewertungen beeinflussen."],
            metadata={
                "thread_id": thread.thread_id,
                "purpose": purpose,
                "package_id": package_id,
                "atomic_fact_id": atomic_fact_id,
                "initial_message_provided": bool(initial_content and initial_content.strip()),
                "review_card_id": review_card_id,
                "clarification_confirmed": False,
            },
        )

        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "clarification_threads": [*run.clarification_threads, thread],
                "analysis_log": [*run.analysis_log, log_entry],
            }
        )
        persisted = self._audit.repository.upsert_run(run=updated)
        if thread.status == "resolved":
            return self._regenerate_package_if_needed(run=persisted, thread=thread)
        return persisted

    def process_answer(
        self,
        *,
        run_id: str,
        thread_id: str,
        content: str,
    ) -> AuditRun:
        """Process a user answer and optionally generate follow-up question or truth proposal.

        Tries LLM-based extraction first, falls back to heuristic if unavailable.
        """
        run = self._require_run(run_id)
        thread = self._require_thread(run, thread_id)
        self._check_message_limit(thread)
        updated_thread = self._process_answer_in_thread(run=run, thread=thread, content=content)
        persisted = self._update_thread_in_run(run, updated_thread)
        if updated_thread.status == "resolved":
            return self._regenerate_package_if_needed(run=persisted, thread=updated_thread)
        return persisted

    def confirm_truth(
        self,
        *,
        run_id: str,
        thread_id: str,
        truth_canonical_key: str,
        truth_normalized_value: str,
        subject_kind: str,
        subject_key: str,
        predicate: str,
        scope_kind: str = "global",
        scope_key: str = "*",
    ) -> AuditRun:
        """Confirm a definitive truth after double-confirmation (100% sure, no exceptions)."""
        run = self._require_run(run_id)
        thread = self._require_thread(run, thread_id)

        # Check for conflicts with existing truths
        conflicts = self._find_truth_conflicts(
            run=run,
            canonical_key=truth_canonical_key,
        )

        if conflicts:
            # Don't create truth yet — return a conflict resolution prompt
            primary_conflict = conflicts[0]
            conflict_msg = ClarificationMessage(
                role="assistant",
                message_type="conflict_resolution",
                content=self._format_conflict_prompt(conflicts, truth_canonical_key, truth_normalized_value),
                referenced_truth_ids=[t.truth_id for t in conflicts],
                metadata={
                    "conflicting_truth_id": primary_conflict.truth_id,
                    "proposed_canonical_key": truth_canonical_key,
                    "proposed_normalized_value": truth_normalized_value,
                    "proposed_subject_kind": subject_kind,
                    "proposed_subject_key": subject_key,
                    "proposed_predicate": predicate,
                    "proposed_scope_kind": scope_kind,
                    "proposed_scope_key": scope_key,
                },
            )
            updated_thread = thread.model_copy(
                update={"messages": [*thread.messages, conflict_msg]}
            )
            return self._update_thread_in_run(run, updated_thread)

        # No conflicts — create truth directly
        return self._create_truth_from_dialog(
            run=run,
            thread=thread,
            canonical_key=truth_canonical_key,
            normalized_value=truth_normalized_value,
            subject_kind=subject_kind,
            subject_key=subject_key,
            predicate=predicate,
            scope_kind=scope_kind,
            scope_key=scope_key,
        )

    def supersede_truth(
        self,
        *,
        run_id: str,
        thread_id: str,
        existing_truth_id: str,
        new_canonical_key: str,
        new_normalized_value: str,
        new_subject_kind: str,
        new_subject_key: str,
        new_predicate: str,
        new_scope_kind: str = "global",
        new_scope_key: str = "*",
    ) -> AuditRun:
        """Supersede an existing truth with a new one from clarification dialog."""
        run = self._require_run(run_id)
        thread = self._require_thread(run, thread_id)

        # Find the truth being superseded
        old_truth = next((t for t in run.truths if t.truth_id == existing_truth_id), None)
        if old_truth is None:
            raise ValueError(f"Wahrheit nicht gefunden: {existing_truth_id}")

        # Create new truth
        new_truth = TruthLedgerEntry(
            canonical_key=new_canonical_key,
            subject_kind=new_subject_kind,
            subject_key=new_subject_key,
            predicate=new_predicate,
            normalized_value=new_normalized_value,
            scope_kind=new_scope_kind,
            scope_key=new_scope_key,
            source_kind="clarification_dialog",
            supersedes_truth_id=existing_truth_id,
            metadata={
                "clarification_thread_id": thread.thread_id,
                "superseded_truth_id": existing_truth_id,
                "confirmed_absolute": True,
            },
        )

        # Mark old truth as superseded
        updated_truths = []
        for t in run.truths:
            if t.truth_id == existing_truth_id:
                updated_truths.append(t.model_copy(update={"truth_status": "superseded"}))
            else:
                updated_truths.append(t)
        updated_truths.append(new_truth)

        # Resolution message
        resolution_msg = ClarificationMessage(
            role="assistant",
            message_type="resolution",
            content=f"Wahrheit ersetzt: '{old_truth.predicate} = {old_truth.normalized_value}' "
                    f"→ '{new_predicate} = {new_normalized_value}'.",
            outcome_type="truth_superseded",
            referenced_truth_ids=[existing_truth_id, new_truth.truth_id],
            created_truth_id=new_truth.truth_id,
            superseded_truth_id=existing_truth_id,
        )

        updated_thread = thread.model_copy(
            update={
                "messages": [*thread.messages, resolution_msg],
                "status": "resolved",
                "resolved_at": utc_now_iso(),
                "resolution_summary": resolution_msg.content,
                "created_truth_ids": [*thread.created_truth_ids, new_truth.truth_id],
                "superseded_truth_ids": [*thread.superseded_truth_ids, existing_truth_id],
                "triggered_delta_recompute": True,
            }
        )

        log_entry = AuditAnalysisLogEntry(
            source_type="clarification_dialog",
            title="Wahrheit ersetzt durch Klärungsdialog",
            message=f"'{old_truth.predicate} = {old_truth.normalized_value}' → '{new_predicate} = {new_normalized_value}'",
            related_finding_ids=list(thread.messages[0].referenced_finding_ids) if thread.messages else [],
            derived_changes=[
                f"Wahrheit {existing_truth_id} wurde auf 'superseded' gesetzt.",
                f"Neue Wahrheit {new_truth.truth_id} wurde angelegt.",
            ],
            impact_summary=[
                "Alle Entscheidungspakete werden anhand der neuen Wahrheit neu bewertet.",
            ],
            metadata={
                "thread_id": thread.thread_id,
                "old_truth_id": existing_truth_id,
                "new_truth_id": new_truth.truth_id,
                "review_card_id": str(thread.metadata.get("review_card_id") or "") or None,
                "clarification_confirmed": True,
            },
        )

        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "truths": updated_truths,
                "clarification_threads": self._replace_thread(run.clarification_threads, updated_thread),
                "analysis_log": [*run.analysis_log, log_entry],
            }
        )
        persisted = self._audit.repository.upsert_run(run=updated)
        return self._regenerate_package_if_needed(run=persisted, thread=updated_thread)

    def capture_indication(
        self,
        *,
        run_id: str,
        thread_id: str,
        content: str,
    ) -> AuditRun:
        """Capture user statement as an indication (elevated confidence claim, not truth)."""
        run = self._require_run(run_id)
        thread = self._require_thread(run, thread_id)
        indication_content = content.strip() or self._latest_user_message_content(thread)
        if not indication_content:
            raise ValueError("Es liegt keine Nutzeräußerung vor, die als Indiz gespeichert werden kann.")

        # Create an indication claim
        subject_key = self._derive_subject_key(thread, run)
        fingerprint = f"clarification:{thread.thread_id}:{indication_content[:50]}"
        indication_claim = AuditClaimEntry(
            claim_id=new_claim_id(),
            source_type="user_truth",
            source_id=f"clarification:{thread.thread_id}",
            subject_kind="clarification_indication",
            subject_key=subject_key,
            predicate="user_indication",
            normalized_value=indication_content,
            scope_kind="global",
            scope_key="*",
            confidence=INDICATION_CONFIDENCE,
            fingerprint=fingerprint,
            status="active",
            assertion_status="asserted",
            metadata={
                "clarification_thread_id": thread.thread_id,
                "indication_type": "user_statement",
                "not_confirmed_as_truth": True,
            },
        )

        indication_msg = ClarificationMessage(
            role="assistant",
            message_type="resolution",
            content=f"Als Indiz gespeichert: \"{indication_content}\". Fließt in Bewertung ein, überschreibt aber keine bestehende Wahrheit.",
            outcome_type="indication_captured",
            created_claim_id=indication_claim.claim_id,
        )

        updated_thread = thread.model_copy(
            update={
                "messages": [*thread.messages, indication_msg],
                "created_claim_ids": [*thread.created_claim_ids, indication_claim.claim_id],
            }
        )

        log_entry = AuditAnalysisLogEntry(
            source_type="clarification_dialog",
            title="Indiz aus Klärungsdialog erfasst",
            message=(
                f"Nutzeraussage als Indiz gespeichert: "
                f"\"{indication_content[:100]}{'...' if len(indication_content) > 100 else ''}\""
            ),
            derived_changes=[f"Claim {indication_claim.claim_id} mit Confidence {INDICATION_CONFIDENCE} angelegt."],
            impact_summary=["Beeinflusst Scoring, überschreibt keine Wahrheiten."],
            metadata={
                "thread_id": thread.thread_id,
                "claim_id": indication_claim.claim_id,
                "review_card_id": str(thread.metadata.get("review_card_id") or "") or None,
                "clarification_confirmed": False,
            },
        )

        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "claims": [*run.claims, indication_claim],
                "clarification_threads": self._replace_thread(run.clarification_threads, updated_thread),
                "analysis_log": [*run.analysis_log, log_entry],
            }
        )
        return self._audit.repository.upsert_run(run=updated)

    def dismiss_thread(
        self,
        *,
        run_id: str,
        thread_id: str,
    ) -> AuditRun:
        """Close a thread without results. History is preserved as context."""
        run = self._require_run(run_id)
        thread = self._require_thread(run, thread_id)

        dismiss_msg = ClarificationMessage(
            role="system",
            message_type="resolution",
            content="Klärungsdialog wurde ohne Ergebnis geschlossen. Gesprächsprotokoll bleibt als Kontextwissen erhalten.",
            outcome_type="context_only",
        )

        updated_thread = thread.model_copy(
            update={
                "messages": [*thread.messages, dismiss_msg],
                "status": "dismissed",
                "resolved_at": utc_now_iso(),
            }
        )

        log_entry = AuditAnalysisLogEntry(
            source_type="clarification_dialog",
            title="Klärungsdialog geschlossen",
            message="Klärung ohne Ergebnis abgeschlossen.",
            impact_summary=["Keine neue Wahrheit gesetzt, keine automatische Neuberechnung."],
            metadata={
                "thread_id": thread.thread_id,
                "review_card_id": str(thread.metadata.get("review_card_id") or "") or None,
                "clarification_confirmed": False,
            },
        )

        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "clarification_threads": self._replace_thread(run.clarification_threads, updated_thread),
                "analysis_log": [*run.analysis_log, log_entry],
            }
        )
        return self._audit.repository.upsert_run(run=updated)

    # ──────────────────────────────────────
    # Private helpers
    # ──────────────────────────────────────

    def _require_run(self, run_id: str) -> AuditRun:
        run = self._audit.get_run(run_id=run_id)
        if run is None:
            raise ValueError(f"Audit-Run nicht gefunden: {run_id}")
        return run

    def _require_thread(self, run: AuditRun, thread_id: str) -> ClarificationThread:
        thread = next(
            (t for t in run.clarification_threads if t.thread_id == thread_id),
            None,
        )
        if thread is None:
            raise ValueError(f"Klärungsdialog nicht gefunden: {thread_id}")
        if thread.status != "active":
            raise ValueError(f"Klärungsdialog ist bereits geschlossen: {thread.status}")
        return thread

    def _validate_anchor(
        self,
        run: AuditRun,
        *,
        package_id: str | None,
        atomic_fact_id: str | None,
    ) -> None:
        if package_id:
            if not any(p.package_id == package_id for p in run.decision_packages):
                raise ValueError(f"Entscheidungspaket nicht gefunden: {package_id}")
        if atomic_fact_id:
            if not any(f.atomic_fact_id == atomic_fact_id for f in run.atomic_facts):
                raise ValueError(f"Atomarer Fakt nicht gefunden: {atomic_fact_id}")

    def _check_message_limit(self, thread: ClarificationThread) -> None:
        if len(thread.messages) >= MAX_MESSAGES_PER_THREAD:
            raise ValueError(
                f"Maximale Nachrichtenanzahl ({MAX_MESSAGES_PER_THREAD}) erreicht. "
                f"Bitte schließen Sie den Dialog ab."
            )

    def _update_thread_in_run(self, run: AuditRun, updated_thread: ClarificationThread) -> AuditRun:
        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "clarification_threads": self._replace_thread(
                    run.clarification_threads, updated_thread
                ),
            }
        )
        return self._audit.repository.upsert_run(run=updated)

    @staticmethod
    def _replace_thread(
        threads: list[ClarificationThread],
        updated: ClarificationThread,
    ) -> list[ClarificationThread]:
        return [
            updated if t.thread_id == updated.thread_id else t
            for t in threads
        ]

    # ── Context Building ──

    def _build_cross_thread_context(self, run: AuditRun) -> str:
        """Collect global knowledge from all prior threads for LLM context injection."""
        lines: list[str] = []

        # Confirmed truths from all threads
        for thread in run.clarification_threads:
            if thread.status != "resolved":
                continue
            lines.append(f"Klärung #{thread.thread_id[:12]} ({thread.purpose}):")
            if thread.created_truth_ids:
                for tid in thread.created_truth_ids:
                    truth = next((t for t in run.truths if t.truth_id == tid), None)
                    if truth:
                        lines.append(f"  → Wahrheit: {truth.predicate} = {truth.normalized_value}")
            if thread.superseded_truth_ids:
                lines.append(f"  → {len(thread.superseded_truth_ids)} Wahrheit(en) ersetzt")
            if thread.created_claim_ids:
                lines.append(f"  → {len(thread.created_claim_ids)} Indiz(ien) erfasst")
            if thread.resolution_summary:
                lines.append(f"  Ergebnis: {thread.resolution_summary}")

        # Include indications from clarification dialogs
        dialog_claims = [
            c for c in run.claims
            if c.source_id and c.source_id.startswith("clarification:")
        ]
        if dialog_claims:
            lines.append(f"\n{len(dialog_claims)} Indizien aus vorherigen Dialogen:")
            for claim in dialog_claims[:5]:
                lines.append(f"  🔹 {claim.predicate}: {claim.normalized_value}")

        return "\n".join(lines) if lines else "Keine vorherigen Klärungen vorhanden."

    def _build_anchor_context(
        self,
        run: AuditRun,
        *,
        package_id: str | None,
        atomic_fact_id: str | None,
    ) -> str:
        """Build context specific to the anchored package or fact."""
        lines: list[str] = []

        if package_id:
            pkg = next((p for p in run.decision_packages if p.package_id == package_id), None)
            if pkg:
                lines.append(f"Paket: {pkg.title}")
                lines.append(f"Kategorie: {pkg.category} | Severity: {pkg.severity_summary}")
                lines.append(f"Scope: {pkg.scope_summary}")
                lines.append(f"Empfehlung: {pkg.recommendation_summary}")

                # Related claims
                related_claims = [
                    c for c in run.claims
                    if any(fid in (c.metadata.get("finding_id", "") if isinstance(c.metadata, dict) else "")
                           for fid in pkg.related_finding_ids)
                ]
                if related_claims:
                    lines.append(f"\nZugehörige Claims ({len(related_claims)}):")
                    for claim in related_claims[:5]:
                        lines.append(f"  - {claim.predicate}: {claim.object_value} (Quelle: {claim.source_type})")

                # Related findings
                related_findings = [
                    f for f in run.findings
                    if f.finding_id in pkg.related_finding_ids
                ]
                if related_findings:
                    lines.append(f"\nZugehörige Findings ({len(related_findings)}):")
                    for finding in related_findings[:5]:
                        lines.append(f"  - [{finding.severity}] {finding.title}")

                # Problem elements
                if pkg.problem_elements:
                    lines.append(f"\nProblemelemente ({len(pkg.problem_elements)}):")
                    for pe in pkg.problem_elements[:3]:
                        lines.append(f"  - {pe.short_explanation} (Confidence: {pe.confidence:.0%})")

        elif atomic_fact_id:
            fact = next((f for f in run.atomic_facts if f.atomic_fact_id == atomic_fact_id), None)
            if fact:
                lines.append(f"Fakt: {fact.fact_key}")
                lines.append(f"Zusammenfassung: {fact.summary}")
                lines.append(f"Status: {fact.status} | Aktionsspur: {fact.action_lane}")

        return "\n".join(lines) if lines else "Kein Kontext verfügbar."

    # ── Question Generation ──

    def _generate_initial_question(
        self,
        *,
        purpose: str,
        anchor_context: str,
        global_context: str,
    ) -> ClarificationMessage:
        """Generate the first question for a new clarification thread.

        Uses template-based generation with global context injection.
        Future: LLM-based with slot-filling from context.
        """
        # Inject global context if available
        context_block = ""
        if global_context and global_context != "Keine vorherigen Klärungen vorhanden.":
            context_block = (
                "\n\n━━━ Bisherige Erkenntnisse ━━━\n"
                f"{global_context}\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            )

        if purpose == "truth_clarification":
            content = (
                "In diesem Paket gibt es widersprüchliche Aussagen. "
                "Bitte klären Sie, welche der folgenden Aussagen fachlich korrekt ist.\n\n"
                f"{anchor_context}"
                f"{context_block}"
            )
        elif purpose == "rating_explanation":
            content = (
                "Sie möchten verstehen, warum dieses Paket so bewertet wurde. "
                "Hier ist der Kontext der Bewertung:\n\n"
                f"{anchor_context}"
                f"{context_block}"
            )
        elif purpose == "action_routing":
            content = (
                "Für dieses Paket muss eine Folgeaktion festgelegt werden. "
                "Soll die Korrektur in Dokumentation, Code oder als Artefakt erfolgen?\n\n"
                f"{anchor_context}"
                f"{context_block}"
            )
        else:
            content = f"Klärungsdialog gestartet.\n\n{anchor_context}{context_block}"

        return ClarificationMessage(
            role="assistant",
            message_type="question",
            content=content,
        )

    def _generate_follow_up(
        self,
        *,
        thread: ClarificationThread,
        user_answer: str,
        indications: list[dict[str, str]],
        run: AuditRun,
        llm_follow_up_question: str = "",
    ) -> list[ClarificationMessage]:
        """Generate follow-up messages based on user answer.

        Returns 1-3 messages: optionally an explanation + a follow-up question or truth proposal.
        """
        messages: list[ClarificationMessage] = []

        # If we extracted indications, propose them
        if indications:
            for indication in indications[:MAX_QUESTIONS_PER_STEP]:
                confidence_grade = indication.get("confidence_grade", "general")
                statement = indication.get("statement", user_answer)

                # Build truth proposal with confidence context
                if confidence_grade == "definite":
                    prefix = "Sie haben eine klare, definitive Aussage gemacht"
                elif confidence_grade == "assertion":
                    prefix = "Sie haben eine fachliche Feststellung getroffen"
                elif confidence_grade == "negation":
                    prefix = "Sie haben eine Ausschlusskriterie benannt"
                else:
                    prefix = "Sie haben eine relevante Angabe gemacht"

                messages.append(ClarificationMessage(
                    role="assistant",
                    message_type="truth_confirmation",
                    content=(
                        f"{prefix}: \"{statement}\"\n\n"
                        f"Soll das als DEFINITIVE Wahrheit gelten?\n"
                        f"Das heißt: Diese Aussage gilt ausnahmslos und beeinflusst ALLE Bewertungen.\n\n"
                        f"💎 Wenn ja: Bestätigen Sie über 'Als Wahrheit bestätigen'\n"
                        f"🔹 Wenn nein: Speichern Sie es über 'Nur als Hinweis'"
                    ),
                    metadata={
                        "proposed_statement": statement,
                        "confidence_grade": confidence_grade,
                        "extraction_confidence": float(indication.get("confidence", "0.65")),
                        "detected_patterns": indication.get("detected_patterns", []),
                        **self._truth_metadata_from_indication(
                            thread=thread,
                            run=run,
                            indication=indication,
                            statement=statement,
                        ),
                    },
                ))
                break  # Only one truth proposal at a time

        # If no indication was extracted, ask a follow-up
        if not messages:
            question_count = sum(1 for m in thread.messages if m.message_type == "question")
            if question_count < MAX_QUESTIONS_PER_STEP:
                # Use LLM-generated follow-up if available
                if llm_follow_up_question:
                    messages.append(ClarificationMessage(
                        role="assistant",
                        message_type="question",
                        content=llm_follow_up_question,
                        metadata={"generated_by": "llm"},
                    ))
                else:
                    messages.append(ClarificationMessage(
                        role="assistant",
                        message_type="question",
                        content=(
                            "Verstanden. Gibt es noch weitere Aspekte, die Sie zu diesem Punkt klären möchten? "
                            "Oder möchten Sie den Dialog abschließen?"
                        ),
                    ))

        return messages

    # ── Indication Extraction ──

    # --- Pattern sets for heuristic classification ---
    _DEFINITE_MARKERS = frozenset([
        "immer", "grundsätzlich", "ausnahmslos", "definitiv", "zwingend",
        "muss", "darf nicht", "niemals", "in jedem fall", "stets",
        "verpflichtend", "unbedingt", "obligatorisch", "auf jeden fall",
    ])
    _ASSERTION_MARKERS = frozenset([
        "ist", "sind", "bedeutet", "heißt", "soll", "wird", "war",
        "haben", "hat", "verwendet", "nutzt", "basiert auf", "gilt",
        "entspricht", "erfordert", "benötigt", "liefert", "erzeugt",
    ])
    _NEGATION_MARKERS = frozenset([
        "nicht", "kein", "keiner", "keine", "nie", "weder", "ohne",
        "darf nicht", "soll nicht", "ist nicht", "gibt es nicht",
    ])
    _SCOPE_MARKERS = frozenset([
        "api", "status", "modul", "service", "klasse", "tabelle",
        "endpunkt", "endpoint", "feld", "spalte", "column", "schema",
        "workflow", "prozess", "phase", "schritt",
    ])
    _FILLER_RESPONSES = frozenset([
        "ja", "nein", "ok", "okay", "danke", "verstanden", "klar",
        "genau", "richtig", "stimmt", "passt", "alles klar",
        "weiter", "fertig", "abschließen", "schließen",
    ])

    def _extract_indications(
        self,
        content: str,
        run: AuditRun,
        thread: ClarificationThread,
    ) -> list[dict[str, str]]:
        """Extract potential truth-like statements from user answer.

        Uses multi-pattern heuristic analysis to classify statement confidence:
          - definite:  contains absolute qualifiers → 0.95
          - assertion: contains declarative verbs → 0.85
          - negation:  contains clear exclusions → 0.75
          - general:   substantive but not clearly assertive → 0.65
        """
        indications: list[dict[str, str]] = []
        content_stripped = content.strip()
        content_lower = content_stripped.lower()

        # Skip empty, too short, questions, and filler responses
        if len(content_stripped) <= 10:
            return indications
        if content_stripped.endswith("?"):
            return indications
        if content_lower in self._FILLER_RESPONSES:
            return indications
        # Also check for filler phrases at start
        for filler in self._FILLER_RESPONSES:
            if content_lower == filler or (content_lower.startswith(filler) and len(content_lower) <= len(filler) + 3):
                return indications

        # Detect patterns
        detected_patterns: list[str] = []

        # 1. Definite pattern: absolute qualifiers
        has_definite = any(marker in content_lower for marker in self._DEFINITE_MARKERS)
        if has_definite:
            detected_patterns.append("definite_qualifier")

        # 2. Assertion pattern: declarative verbs
        words = set(content_lower.split())
        has_assertion = bool(words & self._ASSERTION_MARKERS)
        if has_assertion:
            detected_patterns.append("declarative_verb")

        # 3. Negation pattern: exclusions
        has_negation = any(marker in content_lower for marker in self._NEGATION_MARKERS)
        if has_negation:
            detected_patterns.append("negation")

        # 4. Scope reference: mentions technical terms from the package context
        scope_key = ""
        if thread.package_id:
            pkg = next(
                (p for p in run.decision_packages if p.package_id == thread.package_id),
                None,
            )
            if pkg:
                scope_key = pkg.scope_summary.lower()
        elif thread.atomic_fact_id:
            fact = next(
                (f for f in run.atomic_facts if f.atomic_fact_id == thread.atomic_fact_id),
                None,
            )
            if fact:
                scope_key = fact.fact_key.lower()

        has_scope_ref = any(marker in content_lower for marker in self._SCOPE_MARKERS)
        # Also check scope key terms
        if scope_key:
            scope_terms = [t for t in scope_key.replace(".", " ").replace("_", " ").split() if len(t) > 2]
            if any(term in content_lower for term in scope_terms):
                has_scope_ref = True
                detected_patterns.append("scope_reference")

        if has_scope_ref and "scope_reference" not in detected_patterns:
            detected_patterns.append("technical_term")

        # Classify confidence grade
        if has_definite:
            confidence_grade = "definite"
            confidence = 0.95
        elif has_assertion and (has_scope_ref or has_negation):
            confidence_grade = "assertion"
            confidence = 0.85
        elif has_negation:
            confidence_grade = "negation"
            confidence = 0.75
        elif has_assertion or len(content_stripped) > 30:
            confidence_grade = "general"
            confidence = 0.65
        else:
            # Too short / too vague — not an indication
            return indications

        indications.append({
            "statement": content_stripped,
            "source": "heuristic_extraction",
            "confidence_grade": confidence_grade,
            "confidence": str(confidence),
            "detected_patterns": detected_patterns,
            "extracted_by": "heuristic_v2",
        })

        return indications

    # ── LLM Integration ──

    def _has_llm(self) -> bool:
        """Check if LLM extraction is available."""
        if self._settings is None:
            return False
        slot = self._select_clarification_slot()
        return slot is not None

    def _select_clarification_slot(self) -> int | None:
        """Select a suitable LLM slot for clarification (non-embedding, non-OCR)."""
        if self._settings is None:
            return None
        for slot in self._settings.get_configured_llm_slots():
            model_hint = f"{slot.model} {slot.deployment or ''}".casefold()
            if "embedding" in model_hint or "document-ai" in model_hint or "ocr" in model_hint:
                continue
            return int(slot.slot)
        return None

    def _get_llm_client(self):
        """Lazy-initialize and return LLM client."""
        if self._llm_client is None:
            from fin_ai_auditor.llm import LiteLLMClient
            slot = self._select_clarification_slot()
            if slot is None or self._settings is None:
                raise RuntimeError("Kein LLM-Slot für Klärungsdialoge konfiguriert.")
            self._llm_client = LiteLLMClient(settings=self._settings, default_slot=slot)
        return self._llm_client

    @staticmethod
    def _run_async(coro):
        """Run an async coroutine from sync context."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            # Already inside an event loop (e.g. FastAPI) — use nest_asyncio or run in thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, coro).result(timeout=LLM_EXTRACTION_TIMEOUT_S + 5)
        return asyncio.run(coro)

    async def _llm_extract_indications(
        self,
        content: str,
        run: AuditRun,
        thread: ClarificationThread,
    ) -> dict | None:
        """LLM-based indication extraction.

        Returns a dict with 'indications' (list) and 'follow_up_question' (str),
        or None if extraction fails / no LLM available.
        """
        from fin_ai_auditor.llm import ChatMessage, GenerationConfig

        client = self._get_llm_client()
        slot = self._select_clarification_slot()

        # Build context
        anchor_context = self._build_anchor_context(
            run, package_id=thread.package_id, atomic_fact_id=thread.atomic_fact_id
        )
        dialog_history = "\n".join(
            f"[{m.role}] {m.content[:200]}" for m in thread.messages[-6:]
        )
        cross_context = self._build_cross_thread_context(run)

        system_prompt = (
            "Du bist ein Fachexperte für Governance-Klärungsdialoge im FIN-AI Auditor.\n\n"
            "=== Specification Driven Development ===\n"
            "FIN-AI folgt dem Prinzip Specification Driven Development:\n"
            "Die Dokumentation (Confluence, Architektur-Docs, Metamodell) ist die EINZIGE SSOT.\n"
            "Code dient nur als INDIZ für den Umsetzungsstand, nicht als Zieldefinition.\n"
            "Explizit bestätigte User-Wahrheiten haben höchste Priorität.\n\n"
            "Deine Aufgabe ist es, aus der Nutzerantwort fachliche Aussagen zu extrahieren, "
            "die als potenzielle Wahrheiten oder Indizien gespeichert werden könnten.\n\n"
            "Regeln:\n"
            "1. Extrahiere nur SUBSTANTIELLE fachliche Aussagen — keine Höflichkeitsfloskeln, "
            "Fragen oder unklare Äußerungen.\n"
            "2. Bewerte die Konfidenz: 0.9-1.0 für absolute Aussagen (immer, ausnahmslos, zwingend), "
            "0.7-0.89 für klare Feststellungen, 0.5-0.69 für Vermutungen/Tendenzen.\n"
            "3. Schlage einen kanonischen Schlüssel vor (z.B. 'api.status', 'statement.write_path').\n"
            "4. Markiere ob die Aussage definitiv (is_definite=true) oder eine Negation (is_negation=true) ist.\n"
            "5. Wenn die Antwort keine extrahierbare Aussage enthält, liefere eine leere Liste "
            "und generiere stattdessen eine gezielte Rückfrage als follow_up_question.\n"
            "6. Die Rückfrage soll den Nutzer gezielt zu einer klärenden Aussage führen — "
            "nicht allgemein, sondern direkt auf den fachlichen Streitpunkt bezogen.\n"
            "7. Priorisiere Aussagen, die die DOKUMENTATION betreffen — das ist die SSOT.\n"
            "8. Aussagen über Code-Verhalten sind Indizien, nicht Wahrheiten über das Zielbild.\n\n"
            "Antworte ausschließlich als gültiges JSON passend zum Schema."
        )

        user_prompt = (
            f"=== Paket-Kontext ===\n{anchor_context}\n\n"
            f"=== Bisherige Erkenntnisse ===\n{cross_context}\n\n"
            f"=== Dialogverlauf ===\n{dialog_history}\n\n"
            f"=== Aktuelle Nutzerantwort ===\n{content}\n\n"
            f"Extrahiere fachliche Aussagen und/oder generiere eine gezielte Rückfrage."
        )

        messages = [
            ChatMessage(role="system", content=system_prompt),
            ChatMessage(role="user", content=user_prompt),
        ]

        config = GenerationConfig(
            slot=slot,
            max_tokens=800,
            temperature=0.1,
            timeout_s=LLM_EXTRACTION_TIMEOUT_S,
        )

        result: _LLMIndicationResult = await client.structured_output(
            messages=messages,
            schema=_LLMIndicationResult,
            config=config,
        )

        logger.info(
            "llm_clarification_extraction",
            extra={
                "event_name": "llm_clarification_extraction",
                "event_payload": {
                    "indication_count": len(result.indications),
                    "has_follow_up": bool(result.follow_up_question),
                    "notes": result.extraction_notes[:100] if result.extraction_notes else "",
                },
            },
        )

        # Convert to dict format compatible with heuristic output
        indications = []
        for item in result.indications:
            confidence_grade = "definite" if item.is_definite else (
                "negation" if item.is_negation else (
                    "assertion" if item.confidence >= 0.7 else "general"
                )
            )
            indications.append({
                "statement": item.statement,
                "source": "llm_extraction",
                "confidence_grade": confidence_grade,
                "confidence": str(item.confidence),
                "canonical_key_hint": item.canonical_key_hint,
                "scope_kind": item.scope_kind,
                "detected_patterns": [
                    *((["definite_qualifier"] if item.is_definite else [])),
                    *((["negation"] if item.is_negation else [])),
                    "llm_extracted",
                ],
                "extracted_by": "llm_v1",
            })

        return {
            "indications": indications if indications else None,
            "follow_up_question": result.follow_up_question,
            "resolution_status": result.resolution_status,
            "resolution_summary": result.resolution_summary,
        }

    # ── Truth Conflict Detection ──

    def _find_truth_conflicts(
        self,
        *,
        run: AuditRun,
        canonical_key: str,
    ) -> list[TruthLedgerEntry]:
        """Find existing active truths that would conflict with a new one."""
        return [
            t for t in run.truths
            if t.canonical_key == canonical_key
            and t.truth_status == "active"
        ]

    def _format_conflict_prompt(
        self,
        conflicts: list[TruthLedgerEntry],
        new_key: str,
        new_value: str,
    ) -> str:
        lines = [
            f"⚠️ Es gibt bereits eine bestehende Wahrheit zum Thema '{new_key}':\n"
        ]
        for truth in conflicts:
            lines.append(
                f"  Bestehend: {truth.predicate} = {truth.normalized_value} "
                f"(Quelle: {truth.source_kind})"
            )
        lines.append(f"\n  Neu vorgeschlagen: {new_key} = {new_value}")
        lines.append("\nSoll die bestehende Wahrheit ersetzt werden?")
        return "\n".join(lines)

    # ── Truth Creation ──

    def _create_truth_from_dialog(
        self,
        *,
        run: AuditRun,
        thread: ClarificationThread,
        canonical_key: str,
        normalized_value: str,
        subject_kind: str,
        subject_key: str,
        predicate: str,
        scope_kind: str,
        scope_key: str,
    ) -> AuditRun:
        """Create a definitive truth from clarification dialog."""
        new_truth = TruthLedgerEntry(
            canonical_key=canonical_key,
            subject_kind=subject_kind,
            subject_key=subject_key,
            predicate=predicate,
            normalized_value=normalized_value,
            scope_kind=scope_kind,
            scope_key=scope_key,
            source_kind="clarification_dialog",
            metadata={
                "clarification_thread_id": thread.thread_id,
                "confirmed_absolute": True,
            },
        )

        resolution_msg = ClarificationMessage(
            role="assistant",
            message_type="resolution",
            content=f"✅ Definitive Wahrheit bestätigt: '{predicate} = {normalized_value}'. "
                    f"Gilt ausnahmslos. Alle Bewertungen werden aktualisiert.",
            outcome_type="truth_confirmed",
            created_truth_id=new_truth.truth_id,
            referenced_truth_ids=[new_truth.truth_id],
        )

        updated_thread = thread.model_copy(
            update={
                "messages": [*thread.messages, resolution_msg],
                "status": "resolved",
                "resolved_at": utc_now_iso(),
                "resolution_summary": f"{predicate} = {normalized_value}",
                "created_truth_ids": [*thread.created_truth_ids, new_truth.truth_id],
                "triggered_delta_recompute": True,
            }
        )

        log_entry = AuditAnalysisLogEntry(
            source_type="clarification_dialog",
            title="Wahrheit aus Klärungsdialog bestätigt",
            message=f"'{predicate} = {normalized_value}' wurde als definitive Wahrheit bestätigt.",
            derived_changes=[f"Wahrheit {new_truth.truth_id} wurde im Truth Ledger angelegt."],
            impact_summary=["Alle Entscheidungspakete werden anhand der neuen Wahrheit neu bewertet."],
            metadata={
                "thread_id": thread.thread_id,
                "truth_id": new_truth.truth_id,
                "canonical_key": canonical_key,
                "review_card_id": str(thread.metadata.get("review_card_id") or "") or None,
                "clarification_confirmed": True,
            },
        )

        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "truths": [*run.truths, new_truth],
                "clarification_threads": self._replace_thread(run.clarification_threads, updated_thread),
                "analysis_log": [*run.analysis_log, log_entry],
            }
        )
        persisted = self._audit.repository.upsert_run(run=updated)
        return self._regenerate_package_if_needed(run=persisted, thread=updated_thread)

    def _process_answer_in_thread(
        self,
        *,
        run: AuditRun,
        thread: ClarificationThread,
        content: str,
    ) -> ClarificationThread:
        user_msg = ClarificationMessage(
            role="user",
            message_type="answer",
            content=content,
        )

        indications: list[dict[str, str]] | None = None
        llm_follow_up_question = ""
        resolution_status = "needs_more"
        resolution_summary = ""

        if self._has_llm():
            try:
                llm_result = self._run_async(
                    self._llm_extract_indications(content, run, thread)
                )
                if llm_result is not None:
                    indications = llm_result.get("indications")
                    llm_follow_up_question = llm_result.get("follow_up_question", "")
                    resolution_status = llm_result.get("resolution_status", "needs_more")
                    resolution_summary = llm_result.get("resolution_summary", "")
            except Exception as exc:
                logger.warning(
                    "LLM indication extraction failed, using heuristic fallback: %s", exc
                )

        if indications is None:
            indications = self._extract_indications(content, run, thread)

        if resolution_status == "resolved":
            resolution_msg = ClarificationMessage(
                role="assistant",
                message_type="resolution",
                content=f"Dialog abgeschlossen. Ergebnis: {resolution_summary}",
                outcome_type="context_only",
            )
            return thread.model_copy(
                update={
                    "messages": [*thread.messages, user_msg, resolution_msg],
                    "status": "resolved",
                    "resolved_at": utc_now_iso(),
                    "resolution_summary": resolution_summary,
                    "triggered_delta_recompute": True,
                }
            )

        follow_up = self._generate_follow_up(
            thread=thread,
            user_answer=content,
            indications=indications,
            run=run,
            llm_follow_up_question=llm_follow_up_question,
        )
        return thread.model_copy(
            update={"messages": [*thread.messages, user_msg, *follow_up]}
        )

    def _regenerate_package_if_needed(
        self,
        *,
        run: AuditRun,
        thread: ClarificationThread,
    ) -> AuditRun:
        if not thread.package_id:
            return run
        return self._audit.regenerate_package_from_clarification(
            run_id=run.run_id,
            package_id=thread.package_id,
            thread_id=thread.thread_id,
        )

    def _truth_metadata_from_indication(
        self,
        *,
        thread: ClarificationThread,
        run: AuditRun,
        indication: dict[str, str],
        statement: str,
    ) -> dict[str, object]:
        subject_key = self._derive_subject_key(thread, run)
        canonical_key_hint = str(indication.get("canonical_key_hint", "")).strip()
        predicate = canonical_key_hint.split(".")[-1] if canonical_key_hint else "clarification_truth"
        return {
            "canonical_key_hint": canonical_key_hint,
            "proposed_canonical_key": canonical_key_hint or subject_key,
            "proposed_normalized_value": statement,
            "proposed_subject_kind": "package" if thread.package_id else "atomic_fact",
            "proposed_subject_key": subject_key,
            "proposed_predicate": predicate,
            "proposed_scope_kind": indication.get("scope_kind", "global") or "global",
            "proposed_scope_key": "*",
        }

    @staticmethod
    def _latest_user_message_content(thread: ClarificationThread) -> str:
        for message in reversed(thread.messages):
            if message.role == "user" and message.message_type == "answer":
                return message.content.strip()
        return ""

    # ── Helpers ──

    @staticmethod
    def _derive_subject_key(thread: ClarificationThread, run: AuditRun) -> str:
        """Derive a subject_key from the thread's anchor."""
        if thread.package_id:
            pkg = next((p for p in run.decision_packages if p.package_id == thread.package_id), None)
            return pkg.scope_summary if pkg else thread.package_id
        if thread.atomic_fact_id:
            fact = next((f for f in run.atomic_facts if f.atomic_fact_id == thread.atomic_fact_id), None)
            return fact.fact_key if fact else thread.atomic_fact_id
        return "unknown"
