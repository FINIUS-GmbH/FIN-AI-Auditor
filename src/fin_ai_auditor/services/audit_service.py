from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
import hashlib
import json
import re
from typing import Final
from urllib.parse import urlparse
from uuid import uuid4

import httpx

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import (
    AnalysisMode,
    AtomicFactEntry,
    AuditAnalysisLogEntry,
    AuditClaimEntry,
    AuditFinding,
    AuditFindingLink,
    AuditImplementedChange,
    ConfluencePatchPreview,
    JiraTicketAICodingBrief,
    AuditLocation,
    AuditPosition,
    AuditProgressStep,
    AuditRun,
    AuditRunProgress,
    AuditSourceSnapshot,
    AuditTarget,
    CreateAuditRunRequest,
    DecisionCommentAnalysis,
    DecisionPackage,
    DecisionProblemElement,
    DecisionRecord,
    ReviewCard,
    ReviewCardCoverageSummary,
    RetrievalSegment,
    RetrievalSegmentClaimLink,
    SchemaTruthEntry,
    SemanticEntity,
    SemanticRelation,
    TruthLedgerEntry,
    WritebackApprovalRequest,
    utc_now_iso,
    new_package_id,
)
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository
from fin_ai_auditor.services.atlassian_oauth_service import AtlassianOAuthService
from fin_ai_auditor.services.change_payloads import build_confluence_update_details, build_jira_ticket_brief
from fin_ai_auditor.services.claim_semantics import package_scope_key
from fin_ai_auditor.services.confluence_patch_service import (
    build_confluence_patch_preview,
    build_confluence_payload_preview,
)
from fin_ai_auditor.services.connectors.confluence_connector import (
    ConfluencePageTarget,
    ConfluencePageWriteConnector,
)
from fin_ai_auditor.services.connectors.jira_connector import JiraTicketTarget, JiraTicketingConnector
from fin_ai_auditor.services.finding_prioritization import (
    assigned_root_cause_bucket,
    finding_root_cause_bucket,
    is_core_root_cause_bucket,
    order_package_findings,
    prioritize_findings,
    root_cause_label,
    root_cause_priority,
    select_primary_finding,
    severity_rank,
)
from fin_ai_auditor.services.jira_ticket_writer import build_jira_issue_payload
from fin_ai_auditor.services.pipeline_models import CachedCollectedDocument, CollectedDocument


AUDIT_PIPELINE_STEPS: Final[tuple[tuple[str, str], ...]] = (
    ("metamodel_check", "Metamodell-Pruefung"),
    ("finai_code_check", "FIN-AI Code-Pruefung"),
    ("confluence_check", "Confluence-Pruefung"),
    ("local_docs_check", "Lokale Doku-Pruefung"),
    ("delta_reconciliation", "Delta-Abgleich"),
    ("retrieval_indexing", "Retrieval-Indexierung"),
    ("finding_generation", "Finding-Generierung"),
    ("llm_recommendations", "LLM-Empfehlungen"),
    ("decision_packages", "Entscheidungspakete"),
)

FAST_AUDIT_PIPELINE_STEPS: Final[tuple[tuple[str, str], ...]] = (
    ("source_collection", "Quellen laden"),
    ("section_profiling", "Sektionen profilieren"),
    ("candidate_comparison", "Kandidaten vergleichen"),
    ("review_cards", "Review-Karten bereitstellen"),
    ("follow_up_preparation", "Folgeaktionen vorbereiten"),
)

STEP_LOG_BLUEPRINTS: Final[dict[str, dict[str, list[str] | str]]] = {
    "metamodel_check": {
        "title": "Metamodell wird abgeglichen",
        "derived_changes": [
            "Aktueller Metamodell-Dump wird lokal referenziert.",
            "Metamodell-Aenderungen werden als Delta-Basis fuer spaetere Claim-Pruefungen vorbereitet.",
        ],
        "impact_summary": [
            "Nachfolgende Code- und Doku-Pruefung arbeiten gegen denselben Metamodell-Stand.",
        ],
    },
    "finai_code_check": {
        "title": "FIN-AI Codebasis wird analysiert",
        "derived_changes": [
            "Relevante Lese- und Schreibpfade werden lokal fuer den aktuellen Lauf eingeordnet.",
        ],
        "impact_summary": [
            "Abweichungen gegen Doku und Metamodell koennen spaeter auf konkrete Codeanker gezeigt werden.",
        ],
    },
    "confluence_check": {
        "title": "Confluence-Aussagen werden verdichtet",
        "derived_changes": [
            "Fachliche Aussagen werden aus den relevanten Confluence-Bloecken extrahiert.",
        ],
        "impact_summary": [
            "Widersprueche und fehlende Definitionen koennen gegen echte Seitenanker gespiegelt werden.",
        ],
    },
    "local_docs_check": {
        "title": "Lokale Dokumente werden einbezogen",
        "derived_changes": [
            "Repo-nahe Doku wird als zusaetzliche Evidenzschicht aufgenommen.",
        ],
        "impact_summary": [
            "Unterschiede zwischen Arbeitsdoku und Confluence werden spaeter im Delta sichtbar.",
        ],
    },
    "delta_reconciliation": {
        "title": "Delta-Abgleich laeuft",
        "derived_changes": [
            "Quellaenderungen werden vorhandenen Anchors und bekannten Wahrheiten zugeordnet.",
        ],
        "impact_summary": [
            "Nur betroffene Cluster muessen spaeter neu bewertet werden.",
        ],
    },
    "retrieval_indexing": {
        "title": "Retrieval-Index wird aktualisiert",
        "derived_changes": [
            "Segmente, Claim-Verknuepfungen und Suchanker werden lokal fuer schnelle Kontextbildung gespeichert.",
        ],
        "impact_summary": [
            "Spaetere Delta-Neubewertung und LLM-Empfehlungen koennen gezielter auf relevante Textfenster zugreifen.",
        ],
    },
    "finding_generation": {
        "title": "Problemelemente werden neu generiert",
        "derived_changes": [
            "Atomare Widersprueche, Luecken und Drift-Signale werden aus den Claims abgeleitet.",
        ],
        "impact_summary": [
            "Die spaetere Entscheidungspaket-Bildung arbeitet auf kleineren, konkreten Problemelementen.",
        ],
    },
    "llm_recommendations": {
        "title": "LLM-Empfehlungen werden vorbereitet",
        "derived_changes": [
            "Evidenzen werden in kurze, pruefbare Handlungsempfehlungen ueberfuehrt.",
        ],
        "impact_summary": [
            "User sieht spaeter pro Problemelement eine direkte Vorschlagslogik.",
        ],
    },
    "decision_packages": {
        "title": "Entscheidungspakete werden gebildet",
        "derived_changes": [
            "Verwandte Problemelemente werden zu kleinen, bearbeitbaren Paketen gruppiert.",
        ],
        "impact_summary": [
            "Der User muss nicht ueber rohe Findings entscheiden, sondern ueber fachlich zusammenhaengende Pakete.",
        ],
    },
    "source_collection": {
        "title": "Analysequellen werden gesammelt",
        "derived_changes": [
            "Read-only-Quellen werden in einen gemeinsamen Fast-Audit-Arbeitsstand uebernommen.",
        ],
        "impact_summary": [
            "Nur priorisierte Inhalte gehen anschliessend in den schnellen Vergleichspfad.",
        ],
    },
    "section_profiling": {
        "title": "Sektionen werden profiliert",
        "derived_changes": [
            "Dokumente werden in vergleichbare, entscheidungsrelevante Abschnitte zerlegt.",
        ],
        "impact_summary": [
            "Der spaetere KI-Vergleich arbeitet nur auf priorisierten Abschnittskandidaten.",
        ],
    },
    "candidate_comparison": {
        "title": "Priorisierte Kandidaten werden verglichen",
        "derived_changes": [
            "Passende Abschnitte aus Doku, Code und Metamodell werden direkt gegeneinander gespiegelt.",
        ],
        "impact_summary": [
            "Der schnelle Vergleich ersetzt teure Vollgraph- und Retrieval-Pfade.",
        ],
    },
    "review_cards": {
        "title": "Review-Karten werden vorbereitet",
        "derived_changes": [
            "Abweichungen werden als wenige, entscheidbare Review-Karten bereitgestellt.",
        ],
        "impact_summary": [
            "Der User arbeitet direkt auf Review-Karten statt auf tiefen Forensik-Artefakten.",
        ],
    },
    "follow_up_preparation": {
        "title": "Folgeaktionen werden vorbereitet",
        "derived_changes": [
            "Akzeptierbare Folgeaktionen werden pro Review-Karte vorbereitet, aber noch nicht ausgefuehrt.",
        ],
        "impact_summary": [
            "Jira- und Confluence-Folgepfade koennen nach einer User-Entscheidung direkt anschliessen.",
        ],
    },
}

DECISION_SCOPE_HINTS: Final[tuple[tuple[tuple[str, ...], str], ...]] = (
    (("statement",), "Statement"),
    (("write", "schreib", "persist"), "Statement.write_path"),
    (("read", "les"), "Statement.read_path"),
    (("review", "freigabe", "status"), "Statement.review_status"),
    (("metamodell", "metamodel"), "Metamodel.current_dump"),
    (("confluence", "wiki", "seite"), "Confluence.FINAI"),
    (("jira", "ticket", "board"), "Jira.ticket_creation"),
    (("bsm", "prozess", "process"), "BSM.process"),
    (("fin-ai", "finai", "repo", "code"), "FINAI.codebase"),
    (("doku", "dokumentation", "ssot"), "Documentation.contract"),
)


class AuditService:
    def __init__(
        self,
        *,
        repository: SQLiteAuditRepository,
        settings: Settings,
        atlassian_oauth_service: AtlassianOAuthService | None = None,
        confluence_page_write_connector: ConfluencePageWriteConnector | None = None,
        jira_ticketing_connector: JiraTicketingConnector | None = None,
    ) -> None:
        self._repository = repository
        self._settings = settings
        self._atlassian_oauth_service = atlassian_oauth_service
        self._confluence_page_write_connector = confluence_page_write_connector
        self._jira_ticketing_connector = jira_ticketing_connector

    @property
    def repository(self) -> SQLiteAuditRepository:
        return self._repository

    def create_run(self, *, payload: CreateAuditRunRequest) -> AuditRun:
        normalized_target = self._normalize_target(target=payload.target)
        analysis_mode: AnalysisMode = payload.analysis_mode
        run = AuditRun(
            analysis_mode=analysis_mode,
            target=normalized_target,
            progress=self._build_initial_progress(target=normalized_target, analysis_mode=analysis_mode),
            analysis_log=[
                AuditAnalysisLogEntry(
                    source_type="system",
                    title="Run angelegt",
                    message=(
                        "Audit-Run wurde angelegt. Externe Systeme bleiben read-only; Ergebnisse werden lokal gesammelt. "
                        + (
                            "Fast Audit ist als schneller Vergleichspfad aktiv."
                            if analysis_mode == "fast"
                            else "Deep Audit ist als forensischer Analysepfad aktiv."
                        )
                    ),
                    derived_changes=[
                        "Lokale Auditor-DB wurde als einzige schreibende SSOT fuer diesen Lauf vorbereitet.",
                        (
                            "Der schnelle Review-Karten-Pfad wurde fuer diesen Lauf aktiviert."
                            if analysis_mode == "fast"
                            else "Der tiefe Claim-/Graph-/Retrieval-Pfad wurde fuer diesen Lauf aktiviert."
                        ),
                    ],
                    impact_summary=["Der Worker kann den Lauf nun schrittweise analysieren und protokollieren."],
                )
            ],
        )
        return self._repository.upsert_run(run=run)

    def create_user_run(self, *, payload: CreateAuditRunRequest) -> AuditRun:
        if payload.analysis_mode == "deep" and not self._settings.enable_deep_audit_api_runs:
            raise ValueError(
                "Deep Audit ist serverseitig nicht fuer den normalen UI/API-Pfad freigeschaltet. "
                "Der produktive Standardpfad ist Fast Audit."
            )
        return self.create_run(payload=payload)

    def list_runs(self) -> list[AuditRun]:
        return self._repository.list_runs()

    def get_run(self, *, run_id: str) -> AuditRun | None:
        return self._repository.get_run(run_id=run_id)

    def reset_all_runs(self) -> None:
        """Delete all audit runs from the database. Keeps OAuth tokens."""
        self._repository.reset_all_runs()

    def get_latest_completed_run(self, *, exclude_run_id: str | None = None) -> AuditRun | None:
        completed_runs = [
            run
            for run in self._repository.list_runs()
            if run.status == "completed" and run.run_id != str(exclude_run_id or "").strip()
        ]
        return completed_runs[0] if completed_runs else None

    def replace_retrieval_index(
        self,
        *,
        run_id: str,
        segments: list[RetrievalSegment],
        claim_links: list[RetrievalSegmentClaimLink],
    ) -> None:
        self._repository.replace_retrieval_index(run_id=run_id, segments=segments, claim_links=claim_links)

    def list_retrieval_segments(self, *, run_id: str) -> list[RetrievalSegment]:
        return self._repository.list_retrieval_segments(run_id=run_id)

    def cache_documents(self, *, documents: list[CollectedDocument]) -> None:
        self._repository.cache_documents(documents=documents)

    def get_cached_document(
        self,
        *,
        source_type: str,
        source_id: str,
        content_hash: str,
    ) -> CachedCollectedDocument | None:
        return self._repository.get_cached_document(
            source_type=source_type,
            source_id=source_id,
            content_hash=content_hash,
        )

    def get_latest_cached_document(
        self,
        *,
        source_type: str,
        source_id: str,
    ) -> CachedCollectedDocument | None:
        return self._repository.get_latest_cached_document(
            source_type=source_type,
            source_id=source_id,
        )

    def search_retrieval_segments(
        self,
        *,
        run_id: str,
        query_text: str,
        limit: int = 12,
    ) -> list[tuple[str, float]]:
        return self._repository.search_retrieval_segments(
            run_id=run_id,
            query_text=query_text,
            limit=limit,
        )

    def claim_next_planned_run(self, *, worker_id: str | None = None) -> AuditRun | None:
        now_iso = utc_now_iso()
        next_run = self._repository.claim_next_planned_run(
            worker_id=str(worker_id or "local_worker").strip() or "local_worker",
            lease_expires_at=self._lease_expiry_iso(now_iso=now_iso),
            now_iso=now_iso,
        )
        if next_run is None:
            return None
        running = next_run.model_copy(
            update={
                "status": "running",
                "started_at": next_run.started_at or now_iso,
                "updated_at": now_iso,
                "progress": self._mark_pipeline_started(progress=next_run.progress),
                "analysis_log": self._append_log(
                    analysis_log=next_run.analysis_log,
                    entry=AuditAnalysisLogEntry(
                        source_type="pipeline",
                        title="Analyse-Pipeline gestartet",
                        message="Worker hat den Lauf uebernommen und bereitet die ersten Read-only-Pruefungen vor.",
                        derived_changes=[
                            "Der Run ist jetzt exklusiv in Bearbeitung und die naechsten Pipeline-Schritte werden protokolliert."
                        ],
                        impact_summary=[
                            "Ab diesem Zeitpunkt werden Fortschritt, Phasen und spaetere Ableitungen laufend im Statuslog sichtbar."
                        ],
                        metadata={"step_key": "starting", "worker_id": str(worker_id or "local_worker")},
                    ),
                ),
            }
        )
        return self._repository.upsert_run(run=running)

    def update_run_progress(
        self,
        *,
        run_id: str,
        step_key: str,
        progress_pct: int,
        current_activity: str,
        step_status: str = "running",
        detail: str | None = None,
        worker_id: str | None = None,
    ) -> AuditRun:
        run = self.get_run(run_id=run_id)
        if run is None:
            raise ValueError(f"Audit-Run nicht gefunden: {run_id}")
        now_iso = utc_now_iso()
        heartbeat_ok = self._repository.heartbeat_run_lease(
            run_id=run_id,
            lease_expires_at=self._lease_expiry_iso(now_iso=now_iso),
            now_iso=now_iso,
            worker_id=worker_id,
        )
        if worker_id is not None and not heartbeat_ok:
            raise RuntimeError(
                f"Lease-Heartbeat fuer Audit-Run {run_id} wurde abgelehnt; Worker {worker_id} ist nicht mehr Lease-Owner."
            )
        updated = run.model_copy(
            update={
                "status": "running",
                "updated_at": now_iso,
                "progress": self._progress_for_step(
                    progress=run.progress,
                    target=run.target,
                    analysis_mode=run.analysis_mode,
                    step_key=step_key,
                    progress_pct=progress_pct,
                    current_activity=current_activity,
                    step_status=step_status,
                    detail=detail,
                ),
                "analysis_log": self._append_log(
                    analysis_log=run.analysis_log,
                    entry=self._build_pipeline_log_entry(
                        step_key=step_key,
                        current_activity=current_activity,
                        detail=detail,
                    ),
                ),
            }
        )
        return self._repository.upsert_run(run=updated)

    def complete_run_with_demo_findings(self, *, run_id: str) -> AuditRun:
        run = self.get_run(run_id=run_id)
        if run is None:
            raise ValueError(f"Audit-Run nicht gefunden: {run_id}")
        demo_snapshot_ids = self._build_demo_snapshot_ids()
        findings = self._build_demo_findings(run=run, snapshot_ids=demo_snapshot_ids)
        links = self._build_demo_links(findings=findings)
        snapshots = self._build_demo_snapshots(run=run, snapshot_ids=demo_snapshot_ids)
        claims = self._build_demo_claims(run=run, snapshot_ids=demo_snapshot_ids)
        truths = self._build_demo_truths()
        packages = self._build_decision_packages(
            findings=findings,
            claims=claims,
            truths=truths,
            semantic_entities=[],
            semantic_relations=[],
        )
        completed = run.model_copy(
            update={
                "status": "completed",
                "updated_at": utc_now_iso(),
                "finished_at": utc_now_iso(),
                "summary": "Demo-Auswertung abgeschlossen. Claims, Truth-Ledger und Entscheidungspakete wurden lokal vorbereitet.",
                "progress": self._build_completed_progress(progress=run.progress),
                "analysis_log": self._append_log(
                    analysis_log=run.analysis_log,
                    entry=AuditAnalysisLogEntry(
                        source_type="impact_analysis",
                        title="Analyse abgeschlossen",
                        message="Findings, Claims, Wahrheiten und Entscheidungspakete wurden fuer den User zusammengestellt.",
                        related_finding_ids=[finding.finding_id for finding in findings],
                        derived_changes=[
                            f"{len(findings)} atomare Problemelemente wurden bereitgestellt.",
                            f"{len(claims)} Claims wurden in den lokalen Claim-Index geschrieben.",
                            f"{len(packages)} Entscheidungspakete wurden fuer die UI vorbereitet.",
                        ],
                        impact_summary=[
                            "Der naechste UI-Schritt kann atomare Entscheidungen und Truth-Supersession bearbeiten.",
                        ],
                    ),
                ),
                "source_snapshots": snapshots,
                "semantic_entities": [],
                "semantic_relations": [],
                "findings": findings,
                "finding_links": links,
                "claims": claims,
                "truths": truths,
                "decision_packages": packages,
                "decision_records": [],
                "approval_requests": [],
                "error": None,
            }
        )
        saved = self._repository.upsert_run(run=completed)
        self._repository.clear_run_lease(run_id=run_id)
        return self._require_run(run_id=run_id) if saved.run_id else saved

    def complete_run_with_analysis(
        self,
        *,
        run_id: str,
        source_snapshots: list[AuditSourceSnapshot],
        findings: list[AuditFinding],
        finding_links: list[AuditFindingLink],
        review_cards: list[ReviewCard] | None = None,
        claims: list[AuditClaimEntry],
        truths: list[TruthLedgerEntry],
        schema_truths: list[SchemaTruthEntry],
        semantic_entities: list[SemanticEntity],
        semantic_relations: list[SemanticRelation],
        summary: str,
        analysis_notes: list[str],
        budget_limited: bool = False,
        coverage_summary: ReviewCardCoverageSummary | None = None,
        llm_usage: dict | None = None,
        worker_id: str | None = None,
    ) -> AuditRun:
        run = self._require_run(run_id=run_id)
        now_iso = utc_now_iso()
        review_cards = list(review_cards or [])
        if run.analysis_mode == "fast":
            packages = []
            atomic_facts = []
            atomic_fact_notes = []
        else:
            packages = self._build_decision_packages(
                findings=findings,
                claims=claims,
                truths=truths,
                semantic_entities=semantic_entities,
                semantic_relations=semantic_relations,
            )
            atomic_facts = self._build_atomic_facts(packages=packages)
            atomic_facts, packages, atomic_fact_notes = self._apply_atomic_fact_history(
                run=run,
                atomic_facts=atomic_facts,
                packages=packages,
            )
        completed = run.model_copy(
            update={
                "status": "completed",
                "updated_at": now_iso,
                "finished_at": now_iso,
                "summary": summary,
                "progress": self._build_completed_progress(progress=run.progress),
                "analysis_log": self._append_pipeline_completion_notes(
                    analysis_log=run.analysis_log,
                    analysis_mode=run.analysis_mode,
                    findings=findings,
                    review_cards=review_cards,
                    claims=claims,
                    packages=packages,
                    notes=[*analysis_notes, *atomic_fact_notes],
                ),
                "source_snapshots": source_snapshots,
                "semantic_entities": semantic_entities,
                "semantic_relations": semantic_relations,
                "review_cards": review_cards,
                "budget_limited": budget_limited,
                "coverage_summary": coverage_summary,
                "findings": findings,
                "finding_links": finding_links,
                "claims": claims,
                "truths": truths,
                "schema_truths": schema_truths,
                "atomic_facts": atomic_facts,
                "decision_packages": packages,
                "decision_records": [],
                "approval_requests": [],
                "error": None,
                "llm_usage": llm_usage or {},
            }
        )
        saved = self._repository.upsert_run(run=completed)
        self._repository.clear_run_lease(run_id=run_id, worker_id=worker_id)
        return self._require_run(run_id=run_id) if saved.run_id else saved

    def fail_run(self, *, run_id: str, error: str, worker_id: str | None = None) -> AuditRun:
        run = self.get_run(run_id=run_id)
        if run is None:
            raise ValueError(f"Audit-Run nicht gefunden: {run_id}")
        now_iso = utc_now_iso()
        failed = run.model_copy(
            update={
                "status": "failed",
                "updated_at": now_iso,
                "finished_at": now_iso,
                "progress": self._build_failed_progress(progress=run.progress, error=error),
                "analysis_log": self._append_log(
                    analysis_log=run.analysis_log,
                    entry=AuditAnalysisLogEntry(
                        level="error",
                        source_type="pipeline",
                        title="Analyse fehlgeschlagen",
                        message=error,
                        derived_changes=["Der aktuelle Lauf wurde auf Fehlerstatus gesetzt."],
                        impact_summary=["Bis zur Korrektur werden keine weiteren Findings fuer diesen Lauf erzeugt."],
                    ),
                ),
                "error": error,
            }
        )
        saved = self._repository.upsert_run(run=failed)
        self._repository.clear_run_lease(run_id=run_id, worker_id=worker_id)
        return self._require_run(run_id=run_id) if saved.run_id else saved

    def record_decision_comment_effects(
        self,
        *,
        run_id: str,
        comment_text: str,
        normalized_truths: list[str],
        derived_changes: list[str],
        impact_summary: list[str],
        related_finding_ids: list[str] | None = None,
        related_scope_keys: list[str] | None = None,
    ) -> AuditRun:
        run = self.get_run(run_id=run_id)
        if run is None:
            raise ValueError(f"Audit-Run nicht gefunden: {run_id}")
        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "analysis_log": self._append_log(
                    analysis_log=run.analysis_log,
                    entry=AuditAnalysisLogEntry(
                        source_type="decision_comment",
                        title="User-Kommentar wurde in Wahrheiten ueberfuehrt",
                        message=comment_text,
                        related_finding_ids=list(related_finding_ids or []),
                        related_scope_keys=list(related_scope_keys or []),
                        derived_changes=list(normalized_truths) + list(derived_changes),
                        impact_summary=list(impact_summary),
                    ),
                ),
            }
        )
        return self._repository.upsert_run(run=updated)

    def process_decision_comment(
        self,
        *,
        run_id: str,
        comment_text: str,
        related_finding_ids: list[str] | None = None,
    ) -> AuditRun:
        run = self._require_run(run_id=run_id)
        analysis = self._derive_decision_comment_analysis(
            comment_text=comment_text,
            related_finding_ids=list(related_finding_ids or []),
        )
        next_log = self._append_comment_analysis_logs(
            analysis_log=run.analysis_log,
            comment_text=comment_text,
            related_finding_ids=list(related_finding_ids or []),
            analysis=analysis,
        )
        updated = run.model_copy(update={"updated_at": utc_now_iso(), "analysis_log": next_log})
        return self._repository.upsert_run(run=updated)

    def apply_package_decision(
        self,
        *,
        run_id: str,
        package_id: str,
        action: str,
        comment_text: str | None,
    ) -> AuditRun:
        run = self._require_run(run_id=run_id)
        package = self._require_package(run=run, package_id=package_id)
        decision_action = str(action)
        if decision_action == "specify" and not str(comment_text or "").strip():
            raise ValueError("Fuer 'specify' ist ein Kommentar erforderlich.")

        impacted_package_ids = self._find_impacted_package_ids(run=run, package=package)
        updated_truths = list(run.truths)
        created_truths: list[TruthLedgerEntry] = []
        next_log = list(run.analysis_log)

        if decision_action == "specify":
            analysis = self._derive_decision_comment_analysis(
                comment_text=str(comment_text or ""),
                related_finding_ids=list(package.related_finding_ids),
            )
            updated_truths, created_truths = self._merge_truths_from_specification(
                truths=updated_truths,
                package=package,
                analysis=analysis,
            )
            next_log = self._append_comment_analysis_logs(
                analysis_log=next_log,
                comment_text=str(comment_text or ""),
                related_finding_ids=list(package.related_finding_ids),
                analysis=analysis,
            )
        else:
            next_log = self._append_log(
                analysis_log=next_log,
                entry=AuditAnalysisLogEntry(
                    source_type="impact_analysis",
                    title=f"Entscheidung fuer Paket {package.title}",
                    message=f"Paket wurde mit Aktion '{decision_action}' bewertet.",
                    related_finding_ids=list(package.related_finding_ids),
                    derived_changes=[f"Entscheidungsstatus wurde auf {decision_action} gesetzt."],
                    impact_summary=[
                        "Die lokale Resolution-Ledger-Ansicht kann den Paketstatus jetzt gegen kommende Deltas halten."
                    ],
                    metadata={"package_id": package.package_id},
                ),
            )

        updated_packages = self._apply_package_state(
            packages=run.decision_packages,
            package_id=package.package_id,
            action=decision_action,
            impacted_package_ids=impacted_package_ids,
        )
        decision_record = DecisionRecord(
            package_id=package.package_id,
            action=decision_action,
            comment_text=comment_text,
            created_truth_ids=[truth.truth_id for truth in created_truths],
            impacted_package_ids=impacted_package_ids,
            metadata={"package_title": package.title},
        )
        updated_atomic_facts = self._sync_atomic_facts_from_package_decision(
            atomic_facts=run.atomic_facts,
            package=package,
            action=decision_action,
            comment_text=comment_text,
        )
        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "truths": updated_truths,
                "atomic_facts": updated_atomic_facts,
                "analysis_log": next_log,
                "decision_packages": updated_packages,
                "decision_records": [*run.decision_records, decision_record],
            }
        )
        return self._repository.upsert_run(run=updated)

    def apply_review_card_decision(
        self,
        *,
        run_id: str,
        card_id: str,
        action: str,
        comment_text: str | None,
    ) -> AuditRun:
        run = self._require_run(run_id=run_id)
        if run.analysis_mode != "fast":
            raise ValueError("Review-Karten-Entscheidungen sind nur fuer Fast-Audit-Laeufe verfuegbar.")
        next_state = {
            "accept": "accepted",
            "reject": "rejected",
            "clarify": "clarification_needed",
        }.get(str(action or "").strip())
        if next_state is None:
            raise ValueError(f"Unbekannte Review-Karten-Aktion: {action}")

        target_card: ReviewCard | None = None
        updated_cards: list[ReviewCard] = []
        related_finding_ids: list[str] = []
        for card in run.review_cards:
            if card.card_id != card_id:
                updated_cards.append(card)
                continue
            target_card = card.model_copy(
                update={
                    "decision_state": next_state,
                    "decided_at": utc_now_iso(),
                    "decision_comment": str(comment_text or "").strip() or None,
                }
            )
            related_finding_ids = list(target_card.related_finding_ids)
            updated_cards.append(target_card)
        if target_card is None:
            raise ValueError(f"Review-Karte nicht gefunden: {card_id}")

        finding_state = {
            "accepted": "accepted",
            "rejected": "dismissed",
            "clarification_needed": "open",
        }[next_state]
        related_finding_ids_set = set(related_finding_ids)
        updated_findings = [
            finding.model_copy(update={"resolution_state": finding_state})
            if finding.finding_id in related_finding_ids_set
            else finding
            for finding in run.findings
        ]
        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "review_cards": updated_cards,
                "findings": updated_findings,
                "analysis_log": self._append_log(
                    analysis_log=run.analysis_log,
                    entry=AuditAnalysisLogEntry(
                        source_type="impact_analysis",
                        title=f"Review-Karte {target_card.title} bewertet",
                        message=f"Review-Karte wurde mit Aktion '{action}' bewertet.",
                        related_finding_ids=related_finding_ids,
                        derived_changes=[f"Review-Karten-Status wurde auf {next_state} gesetzt."],
                        impact_summary=[
                            "Akzeptierte Review-Karten koennen jetzt fuer Folgeaktionen Richtung Jira oder Confluence verwendet werden."
                        ],
                        metadata={"review_card_id": target_card.card_id, "action": action, "comment_text": comment_text},
                    ),
                ),
            }
        )
        return self._repository.upsert_run(run=updated)

    def regenerate_package_from_clarification(
        self,
        *,
        run_id: str,
        package_id: str,
        thread_id: str,
    ) -> AuditRun:
        """Regenerate a decision package using clarification dialog outcomes."""
        run = self._require_run(run_id=run_id)
        package = self._require_package(run=run, package_id=package_id)
        thread = next((t for t in run.clarification_threads if t.thread_id == thread_id), None)
        if not thread:
            raise ValueError(f"Klärungsdialog {thread_id} nicht gefunden.")
        if thread.package_id and thread.package_id != package_id:
            raise ValueError("Klärungsdialog ist nicht an dieses Paket gebunden.")

        clarification_context = "\\n".join(
            f"{'User' if m.role == 'user' else 'Auditor'}: {m.content}"
            for m in thread.messages
        )

        rebuilt_packages = self._build_decision_packages(
            findings=run.findings,
            claims=run.claims,
            truths=run.truths,
            semantic_entities=run.semantic_entities,
            semantic_relations=run.semantic_relations,
        )
        rebuilt_candidate = self._select_regenerated_package_candidate(
            previous_package=package,
            rebuilt_packages=rebuilt_packages,
        )
        candidate_package = rebuilt_candidate or package

        revised_recommendation = self._generate_revised_recommendation(
            package=candidate_package,
            clarification_context=clarification_context,
            resolution_summary=thread.resolution_summary or "",
        )
        new_id = new_package_id()

        base_meta = dict(candidate_package.metadata or {})
        base_meta.update({
            "revision_of": package_id,
            "clarification_thread_id": thread_id,
            "revision_type": "clarification",
            "regenerated_scope_key": _canonical_package_scope_key(package=candidate_package),
        })

        title = f"✎ {candidate_package.title}" if not candidate_package.title.startswith("✎") else candidate_package.title

        revised_package = candidate_package.model_copy(update={
            "package_id": new_id,
            "title": title,
            "recommendation_summary": revised_recommendation,
            "decision_state": "open",
            "metadata": base_meta,
        })

        updated_packages = []
        for pkg in run.decision_packages:
            if pkg.package_id == package_id:
                sup_meta = dict(pkg.metadata or {})
                sup_meta["superseded_by"] = new_id
                updated_packages.append(pkg.model_copy(update={
                    "decision_state": "superseded",
                    "metadata": sup_meta,
                }))
            else:
                updated_packages.append(pkg)
        updated_packages.append(revised_package)

        updated_run = run.model_copy(update={
            "decision_packages": updated_packages,
            "updated_at": utc_now_iso(),
        })
        return self._repository.upsert_run(run=updated_run)

    def _generate_revised_recommendation(
        self,
        *,
        package: DecisionPackage,
        clarification_context: str,
        resolution_summary: str,
    ) -> str:
        """Helper to generate a revised recommendation via LLM."""
        fallback = f"{package.recommendation_summary}\\n\\n[!] Nach Klärung: {resolution_summary}"
        try:
            from fin_ai_auditor.llm import ChatMessage, GenerationConfig, LiteLLMClient
        except Exception:
            return fallback

        configured_slots_getter = getattr(self._settings, "get_configured_llm_slots", None)
        if not callable(configured_slots_getter):
            return fallback
        configured_slots = configured_slots_getter()
        if not isinstance(configured_slots, (list, tuple)):
            return fallback

        selected_slot = None
        for slot in configured_slots:
            model_hint = f"{getattr(slot, 'model', '')} {getattr(slot, 'deployment', '')}".casefold()
            if "embedding" in model_hint or "document-ai" in model_hint or "ocr" in model_hint:
                continue
            slot_value = getattr(slot, "slot", None)
            if slot_value is None:
                continue
            selected_slot = int(slot_value)
            break

        if selected_slot is None:
            return fallback

        try:
            client = LiteLLMClient(settings=self._settings, default_slot=selected_slot)
        except Exception:
            return fallback

        prompt = (
            f"Du bist der FIN-AI Auditor. Deine Aufgabe ist es, eine Lösungsempfehlung aufgrund eines Klärungsdialogs zu aktualisieren.\\n\\n"
            f"=== Ursprüngliche Empfehlung ===\\n{package.recommendation_summary}\\n\\n"
            f"=== Klärungsdialog mit dem User ===\\n{clarification_context}\\n\\n"
            f"=== Zusammenfassung der Klärung ===\\n{resolution_summary}\\n\\n"
            f"REGELN FÜR DIE NEUE EMPFEHLUNG:\\n"
            f"1. Integriere die im Dialog getroffenen Entscheidungen in die Empfehlung.\\n"
            f"2. Formuliere konkret, WAS exakt WO geändert werden muss, als klare Liste von Schritten.\\n"
            f"3. Keine technischen Präfixe wie 'Write-Decider', nur klare Anweisungen wie 'Datei X ändern: ...'.\\n"
            f"4. Behalte Hinweise auf betroffene Dateien (Vertragsketten, Schema) bei.\\n"
            f"5. Ignoriere irrelevante Teile des Dialogs.\\n\\n"
            f"Gib AUSSCHLIESSLICH den Text der überarbeiteten Empfehlung zurück, ohne Grußformel oder Erklärung."
        )

        import asyncio
        import concurrent.futures
        import logging
        logger = logging.getLogger(__name__)

        async def _call_llm() -> str:
            msg = ChatMessage(role="user", content=prompt)
            cfg = GenerationConfig(temperature=0.2, max_tokens=1000)
            return await client.generate(messages=[msg], config=cfg)

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    result = pool.submit(asyncio.run, _call_llm()).result(timeout=45)
                    return result.strip()
            else:
                return asyncio.run(_call_llm()).strip()
        except Exception as e:
            logger.error("Fehler bei LLM-Regeneration von Package %s: %s", package.package_id, e)
            return f"FEHLER BEI REVISION: {e}\\n\\nUrsprünglich:\\n{package.recommendation_summary}"

    @staticmethod
    def _select_regenerated_package_candidate(
        *,
        previous_package: DecisionPackage,
        rebuilt_packages: list[DecisionPackage],
    ) -> DecisionPackage | None:
        previous_scope_key = _canonical_package_scope_key(package=previous_package)
        previous_group_key = str(previous_package.metadata.get("group_key") or "").strip()
        previous_root_bucket = str(previous_package.metadata.get("root_cause_bucket") or "").strip()
        previous_finding_ids = set(previous_package.related_finding_ids)

        ranked_candidates: list[tuple[int, DecisionPackage]] = []
        for candidate in rebuilt_packages:
            score = 0
            candidate_scope_key = _canonical_package_scope_key(package=candidate)
            candidate_group_key = str(candidate.metadata.get("group_key") or "").strip()
            candidate_root_bucket = str(candidate.metadata.get("root_cause_bucket") or "").strip()
            candidate_finding_ids = set(candidate.related_finding_ids)
            if candidate_scope_key == previous_scope_key:
                score += 4
            if previous_group_key and candidate_group_key == previous_group_key:
                score += 3
            if previous_root_bucket and candidate_root_bucket == previous_root_bucket:
                score += 3
            if previous_finding_ids and previous_finding_ids.intersection(candidate_finding_ids):
                score += 5
            if score > 0:
                ranked_candidates.append((score, candidate))

        if not ranked_candidates:
            return None
        ranked_candidates.sort(
            key=lambda item: (
                -item[0],
                severity_rank(severity=item[1].severity_summary),
                item[1].title,
            )
        )
        return ranked_candidates[0][1]


    def update_atomic_fact_status(
        self,
        *,
        run_id: str,
        atomic_fact_id: str,
        status: str,
        comment_text: str | None,
    ) -> AuditRun:
        run = self._require_run(run_id=run_id)
        normalized_status = str(status or "").strip()
        if normalized_status not in {"open", "confirmed", "resolved", "superseded"}:
            raise ValueError(f"Unbekannter Atomic-Fact-Status: {status}")
        target_fact: AtomicFactEntry | None = None
        updated_facts: list[AtomicFactEntry] = []
        for fact in run.atomic_facts:
            if fact.atomic_fact_id != atomic_fact_id:
                updated_facts.append(fact)
                continue
            metadata = dict(fact.metadata or {})
            metadata["last_status_changed_at"] = utc_now_iso()
            metadata["last_status_comment"] = str(comment_text or "").strip() or None
            target_fact = fact.model_copy(update={"status": normalized_status, "metadata": metadata})
            updated_facts.append(target_fact)
        if target_fact is None:
            raise ValueError(f"Atomic Fact nicht gefunden: {atomic_fact_id}")
        updated_packages = self._sync_atomic_fact_status_into_packages(
            packages=run.decision_packages,
            target_fact=target_fact,
        )
        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "atomic_facts": updated_facts,
                "decision_packages": updated_packages,
                "analysis_log": self._append_log(
                    analysis_log=run.analysis_log,
                    entry=AuditAnalysisLogEntry(
                        source_type="impact_analysis",
                        title="Atomic Fact Status geaendert",
                        message=f"{target_fact.fact_key} wurde auf {normalized_status} gesetzt.",
                        related_finding_ids=list(target_fact.related_finding_ids),
                        derived_changes=[
                            f"Atomic Fact {target_fact.fact_key} hat jetzt den Status {normalized_status}.",
                        ],
                        impact_summary=[
                            "Die Faktenansicht und die zugehoerigen Entscheidungspakete spiegeln jetzt denselben Bewertungsstand."
                        ],
                        metadata={
                            "atomic_fact_id": target_fact.atomic_fact_id,
                            "atomic_fact_key": target_fact.fact_key,
                            "status": normalized_status,
                            "comment_text": comment_text,
                        },
                    ),
                ),
            }
        )
        return self._repository.upsert_run(run=updated)

    def create_writeback_approval_request(
        self,
        *,
        run_id: str,
        target_type: str,
        title: str,
        summary: str,
        target_url: str | None,
        related_review_card_ids: list[str] | None = None,
        related_package_ids: list[str] | None = None,
        related_finding_ids: list[str] | None = None,
        payload_preview: list[str] | None = None,
    ) -> AuditRun:
        run = self._require_run(run_id=run_id)
        related_review_card_ids = list(related_review_card_ids or [])
        related_package_ids = list(related_package_ids or [])
        related_finding_ids = list(related_finding_ids or [])
        payload_preview = list(payload_preview or [])
        related_findings = self._select_related_findings_for_approval(
            run=run,
            related_review_card_ids=related_review_card_ids,
            related_finding_ids=related_finding_ids,
            related_package_ids=related_package_ids,
        )
        approval_metadata: dict[str, object] = {}
        effective_payload_preview = list(payload_preview)
        effective_target_url = target_url
        if target_type == "confluence_page_update":
            patch_preview = build_confluence_patch_preview(
                run=run,
                findings=related_findings,
                fallback_page_url=target_url or self._settings.confluence_home_url,
                fallback_page_title=title.replace("Confluence-Writeback fuer ", "").strip() or "FIN-AI Spezifikation",
            )
            approval_metadata = {
                "confluence_patch_preview": patch_preview.model_dump(mode="json"),
            }
            effective_payload_preview = self._build_confluence_payload_preview(
                existing_preview=payload_preview,
                patch_preview=patch_preview,
            )
            effective_target_url = patch_preview.page_url
        elif target_type == "jira_ticket_create":
            brief = build_jira_ticket_brief(run=run, findings=related_findings)
            issue_payload = build_jira_issue_payload(
                brief=brief,
                project_key=self._settings.fixed_jira_project_key,
            )
            approval_metadata = {
                "jira_ticket_brief": brief.model_dump(mode="json"),
                "jira_issue_payload": issue_payload,
            }
            effective_payload_preview = self._build_jira_payload_preview(
                existing_preview=payload_preview,
                brief=brief,
            )
        target_guard = self._build_writeback_target_guard(
            target_type=target_type,
            target_url=effective_target_url,
            extra_metadata=approval_metadata,
        )
        if list(target_guard.get("blockers") or []):
            raise ValueError("; ".join(str(item).strip() for item in list(target_guard.get("blockers") or []) if str(item).strip()))
        approval_metadata["target_guard"] = target_guard
        approval_metadata["writeback_verification"] = self._build_writeback_verification_metadata(
            target_type=target_type,
            target_url=effective_target_url,
            extra_metadata=approval_metadata,
        )
        approval_metadata["writeback_preflight"] = self._build_writeback_preflight(
            target_type=target_type,
            verification_metadata=approval_metadata["writeback_verification"],
        )
        approval_metadata["execution_token"] = self._build_writeback_execution_token(
            run=run,
            target_type=target_type,
            target_url=effective_target_url,
            related_findings=related_findings,
        )
        approval_metadata["atomic_facts"] = [
            {
                "fact_key": _atomic_fact_key(finding=finding),
                "summary": _atomic_fact_summary(
                    finding=finding,
                    claims=_claims_for_finding_scope(finding=finding, claims=run.claims),
                ),
                "action_lane": _preferred_action_lane_for_finding(finding=finding),
            }
            for finding in related_findings[:6]
        ]
        effective_payload_preview = _dedupe_preserve_order(
            [
                *effective_payload_preview,
                *[
                    f"Fakt: {fact['summary']}"
                    for fact in approval_metadata["atomic_facts"]
                    if str(fact.get("summary") or "").strip()
                ][:3],
                *[
                    f"Aktionsspur: {fact['action_lane']}"
                    for fact in approval_metadata["atomic_facts"]
                    if str(fact.get("action_lane") or "").strip()
                ][:2],
                *[
                    f"Preflight: {risk}"
                    for risk in list((approval_metadata.get("writeback_preflight") or {}).get("warnings") or [])[:2]
                ],
                *[
                    f"Blocker: {risk}"
                    for risk in list((approval_metadata.get("writeback_preflight") or {}).get("blockers") or [])[:2]
                ],
            ]
        )
        approval = WritebackApprovalRequest(
            target_type=target_type,
            title=title,
            summary=summary,
            target_url=effective_target_url,
            related_package_ids=related_package_ids,
            related_finding_ids=[finding.finding_id for finding in related_findings],
            payload_preview=effective_payload_preview,
            metadata={**approval_metadata, "related_review_card_ids": related_review_card_ids},
        )
        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "approval_requests": [approval, *run.approval_requests],
                "analysis_log": self._append_log(
                    analysis_log=run.analysis_log,
                    entry=AuditAnalysisLogEntry(
                        source_type="impact_analysis",
                        title="Writeback-Freigabe angefordert",
                        message=summary,
                        related_finding_ids=[finding.finding_id for finding in related_findings],
                        derived_changes=[
                            f"Lokale Freigabeanfrage fuer {target_type} wurde angelegt.",
                        ],
                        impact_summary=[
                            "Bis zur Approval-Entscheidung bleibt der externe Writeback blockiert.",
                        ],
                        metadata={"approval_request_id": approval.approval_request_id},
                    ),
                ),
            }
        )
        return self._repository.upsert_run(run=updated)

    def resolve_writeback_approval_request(
        self,
        *,
        run_id: str,
        approval_request_id: str,
        decision: str,
        comment_text: str | None,
    ) -> AuditRun:
        run = self._require_run(run_id=run_id)
        next_status = {
            "approve": "approved",
            "reject": "rejected",
            "cancel": "cancelled",
        }.get(decision)
        if next_status is None:
            raise ValueError(f"Unbekannte Freigabe-Entscheidung: {decision}")
        now = utc_now_iso()
        updated_requests: list[WritebackApprovalRequest] = []
        changed_request: WritebackApprovalRequest | None = None
        for request in run.approval_requests:
            if request.approval_request_id != approval_request_id:
                updated_requests.append(request)
                continue
            changed_request = request.model_copy(
                update={
                    "status": next_status,
                    "decided_at": now,
                    "decision_comment": comment_text,
                }
            )
            updated_requests.append(changed_request)
        if changed_request is None:
            raise ValueError(f"Freigabeanfrage nicht gefunden: {approval_request_id}")

        updated = run.model_copy(
            update={
                "updated_at": now,
                "approval_requests": updated_requests,
                "analysis_log": self._append_log(
                    analysis_log=run.analysis_log,
                    entry=AuditAnalysisLogEntry(
                        source_type="impact_analysis",
                        title="Writeback-Freigabe entschieden",
                        message=changed_request.summary,
                        related_finding_ids=list(changed_request.related_finding_ids),
                        derived_changes=[f"Freigabestatus wurde auf {next_status} gesetzt."],
                        impact_summary=[
                            "Nur bei Status 'approved' darf der zugehoerige Writeback spaeter lokal als umgesetzt verbucht werden."
                        ],
                        metadata={"approval_request_id": changed_request.approval_request_id},
                    ),
                ),
            }
        )
        return self._repository.upsert_run(run=updated)

    def record_confluence_page_update(
        self,
        *,
        run_id: str,
        approval_request_id: str,
        page_title: str,
        page_url: str,
        changed_sections: list[str],
        change_summary: list[str],
        related_finding_ids: list[str] | None = None,
    ) -> AuditRun:
        run = self._require_run(run_id=run_id)
        approval = self._require_approved_request(
            run=run,
            approval_request_id=approval_request_id,
            target_type="confluence_page_update",
        )
        findings = self._select_related_findings(run=run, related_finding_ids=related_finding_ids)
        patch_preview = self._resolve_confluence_patch_preview_for_execution(
            run=run,
            approval=approval,
            findings=findings,
        )
        details = build_confluence_update_details(
            page_title=page_title,
            page_url=page_url,
            changed_sections=changed_sections,
            change_summary=change_summary,
            page_id=patch_preview.page_id,
            execution_mode="local_ledger_only",
            patch_preview=patch_preview,
        )
        change = AuditImplementedChange(
            change_type="confluence_page_updated",
            title=f"Seite {page_title} aktualisiert",
            summary="Die Confluence-Seite wurde nach expliziter Freigabe lokal als umgesetzt verbucht.",
            target_label=page_title,
            target_url=page_url,
            related_finding_ids=[finding.finding_id for finding in findings],
            implications=[
                "Die betroffenen Dokument-Claims muessen im naechsten Delta-Lauf gegen die neue Seitenaussage geprueft werden.",
                "Verknuepfte Problemelemente koennen dadurch aufgeloest oder neu gewichtet werden.",
            ],
            metadata={
                "changed_sections_count": len(details.changed_sections),
                "approval_request_id": approval.approval_request_id,
                "confluence_patch_preview": patch_preview.model_dump(mode="json"),
                "execution_mode": "local_ledger_only",
            },
            confluence_update=details,
        )
        updated_requests = self._mark_approval_request_executed(
            requests=run.approval_requests,
            approval_request_id=approval.approval_request_id,
            implemented_change_id=change.change_id,
        )
        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "implemented_changes": self._append_implemented_change(
                    implemented_changes=run.implemented_changes,
                    change=change,
                ),
                "approval_requests": updated_requests,
            }
        )
        return self._repository.upsert_run(run=updated)

    def record_jira_ticket_created(
        self,
        *,
        run_id: str,
        approval_request_id: str,
        ticket_key: str,
        ticket_url: str,
        related_finding_ids: list[str] | None = None,
    ) -> AuditRun:
        run = self._require_run(run_id=run_id)
        approval = self._require_approved_request(
            run=run,
            approval_request_id=approval_request_id,
            target_type="jira_ticket_create",
        )
        findings = self._select_related_findings(run=run, related_finding_ids=related_finding_ids)
        brief = self._resolve_jira_brief_for_execution(
            run=run,
            approval=approval,
            findings=findings,
            ticket_key=ticket_key,
            ticket_url=ticket_url,
        )
        jira_issue_payload = build_jira_issue_payload(
            brief=brief,
            project_key=self._settings.fixed_jira_project_key,
        )
        change = AuditImplementedChange(
            change_type="jira_ticket_created",
            title=f"Jira Ticket {ticket_key} zur FIN-AI Codeanpassung erstellt",
            summary=(
                "Das Ticket wurde lokal als erstellt verbucht und enthaelt einen vollstaendigen AI-Coding-Kontext."
            ),
            target_label=ticket_key,
            target_url=ticket_url,
            related_finding_ids=[finding.finding_id for finding in findings],
            implications=list(brief.implications),
            metadata={
                "affected_parts_count": len(brief.affected_parts),
                "approval_request_id": approval.approval_request_id,
                "jira_issue_payload": jira_issue_payload,
            },
            jira_ticket=brief,
        )
        updated_requests = self._mark_approval_request_executed(
            requests=run.approval_requests,
            approval_request_id=approval.approval_request_id,
            implemented_change_id=change.change_id,
        )
        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "implemented_changes": self._append_implemented_change(
                    implemented_changes=run.implemented_changes,
                    change=change,
                ),
                "approval_requests": updated_requests,
            }
        )
        return self._repository.upsert_run(run=updated)

    def execute_jira_ticket_writeback(
        self,
        *,
        run_id: str,
        approval_request_id: str,
    ) -> AuditRun:
        run = self._require_run(run_id=run_id)
        existing = self._return_existing_writeback_execution(
            run=run,
            approval_request_id=approval_request_id,
            target_type="jira_ticket_create",
            expected_change_type="jira_ticket_created",
        )
        if existing is not None:
            return existing
        approval = self._require_approved_request(
            run=run,
            approval_request_id=approval_request_id,
            target_type="jira_ticket_create",
        )
        if self._atlassian_oauth_service is None or self._jira_ticketing_connector is None:
            raise ValueError("Jira-Writeback ist im aktuellen Auditor-Kontext noch nicht verdrahtet.")
        verification_metadata = self._build_writeback_verification_metadata(
            target_type="jira_ticket_create",
            target_url=approval.target_url,
            extra_metadata=approval.metadata,
        )
        try:
            self._assert_writeback_target_allowed(
                target_type="jira_ticket_create",
                verification_metadata=verification_metadata,
            )
        except Exception as exc:
            self._persist_writeback_failure(
                run=run,
                approval_request_id=approval.approval_request_id,
                target_type="jira_ticket_create",
                exc=exc,
                verification_metadata=verification_metadata,
            )
            raise
        try:
            access_token = self._atlassian_oauth_service.get_valid_access_token_or_raise(
                required_scopes={"write:jira-work"}
            )
        except Exception as exc:
            self._persist_writeback_failure(
                run=run,
                approval_request_id=approval.approval_request_id,
                target_type="jira_ticket_create",
                exc=exc,
                verification_metadata=verification_metadata,
            )
            raise
        findings = self._select_related_findings(
            run=run,
            related_finding_ids=list(approval.related_finding_ids),
        )
        preview_brief = self._resolve_jira_brief_for_execution(
            run=run,
            approval=approval,
            findings=findings,
            ticket_key="PENDING",
            ticket_url=None,
        )
        jira_issue_payload = build_jira_issue_payload(
            brief=preview_brief,
            project_key=self._settings.fixed_jira_project_key,
        )
        try:
            created_issue = self._jira_ticketing_connector.create_ticket(
                target=JiraTicketTarget(
                    project_key=self._settings.fixed_jira_project_key,
                    board_url=self._settings.jira_board_url,
                ),
                issue_payload=jira_issue_payload,
                access_token=access_token,
            )
        except Exception as exc:
            self._persist_writeback_failure(
                run=run,
                approval_request_id=approval.approval_request_id,
                target_type="jira_ticket_create",
                exc=exc,
                verification_metadata=verification_metadata,
            )
            raise
        resolved_brief = self._resolve_jira_brief_for_execution(
            run=run,
            approval=approval,
            findings=findings,
            ticket_key=created_issue.issue_key,
            ticket_url=created_issue.issue_url,
        )
        change = AuditImplementedChange(
            change_type="jira_ticket_created",
            title=f"Jira Ticket {created_issue.issue_key} zur FIN-AI Codeanpassung erstellt",
            summary=(
                "Das Ticket wurde nach expliziter Freigabe extern ueber die Jira API erstellt und lokal nachvollziehbar verbucht."
            ),
            target_label=created_issue.issue_key,
            target_url=created_issue.issue_url,
            related_finding_ids=[finding.finding_id for finding in findings],
            implications=list(resolved_brief.implications),
            metadata={
                "affected_parts_count": len(resolved_brief.affected_parts),
                "approval_request_id": approval.approval_request_id,
                "jira_issue_payload": jira_issue_payload,
                "jira_issue_response": created_issue.response_payload,
                "execution_mode": "external_jira_api",
                "execution_token": approval.metadata.get("execution_token"),
                "writeback_verification": {
                    **verification_metadata,
                    **created_issue.verification_metadata,
                    "verified": True,
                },
            },
            jira_ticket=resolved_brief,
        )
        updated_requests = self._mark_approval_request_executed(
            requests=run.approval_requests,
            approval_request_id=approval.approval_request_id,
            implemented_change_id=change.change_id,
        )
        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "implemented_changes": self._append_implemented_change(
                    implemented_changes=run.implemented_changes,
                    change=change,
                ),
                "approval_requests": updated_requests,
                "analysis_log": self._append_log(
                    analysis_log=run.analysis_log,
                    entry=AuditAnalysisLogEntry(
                        source_type="impact_analysis",
                        title="Jira-Writeback ausgefuehrt",
                        message=(
                            f"Das Ticket {created_issue.issue_key} wurde nach expliziter Freigabe extern erstellt."
                        ),
                        related_finding_ids=[finding.finding_id for finding in findings],
                        derived_changes=[
                            "Der lokale Approval-Eintrag wurde als ausgefuehrt markiert.",
                            "Die externe Jira-Issue-Antwort wurde im lokalen Vollzugsledger abgelegt.",
                        ],
                        impact_summary=[
                            "Das Ticket kann jetzt als konkreter AI-Coding-Auftrag ausserhalb des Auditors weiterverarbeitet werden."
                        ],
                        metadata={
                            "approval_request_id": approval.approval_request_id,
                            "writeback_verification": {
                                **verification_metadata,
                                **created_issue.verification_metadata,
                                "verified": True,
                            },
                        },
                    ),
                ),
            }
        )
        return self._repository.upsert_run(run=updated)

    def execute_confluence_page_writeback(
        self,
        *,
        run_id: str,
        approval_request_id: str,
    ) -> AuditRun:
        run = self._require_run(run_id=run_id)
        existing = self._return_existing_writeback_execution(
            run=run,
            approval_request_id=approval_request_id,
            target_type="confluence_page_update",
            expected_change_type="confluence_page_updated",
        )
        if existing is not None:
            return existing
        approval = self._require_approved_request(
            run=run,
            approval_request_id=approval_request_id,
            target_type="confluence_page_update",
        )
        if self._atlassian_oauth_service is None or self._confluence_page_write_connector is None:
            raise ValueError("Confluence-Writeback ist im aktuellen Auditor-Kontext noch nicht verdrahtet.")
        verification_metadata = self._build_writeback_verification_metadata(
            target_type="confluence_page_update",
            target_url=approval.target_url,
            extra_metadata=approval.metadata,
        )
        try:
            self._assert_writeback_target_allowed(
                target_type="confluence_page_update",
                verification_metadata=verification_metadata,
            )
        except Exception as exc:
            self._persist_writeback_failure(
                run=run,
                approval_request_id=approval.approval_request_id,
                target_type="confluence_page_update",
                exc=exc,
                verification_metadata=verification_metadata,
            )
            raise
        try:
            access_token = self._atlassian_oauth_service.get_valid_access_token_or_raise(
                required_scopes={"write:page:confluence"}
            )
        except Exception as exc:
            self._persist_writeback_failure(
                run=run,
                approval_request_id=approval.approval_request_id,
                target_type="confluence_page_update",
                exc=exc,
                verification_metadata=verification_metadata,
            )
            raise
        findings = self._select_related_findings(
            run=run,
            related_finding_ids=list(approval.related_finding_ids),
        )
        patch_preview = self._resolve_confluence_patch_preview_for_execution(
            run=run,
            approval=approval,
            findings=findings,
        )
        if not patch_preview.page_id:
            exc = ValueError(
                "Der Confluence-Patch hat noch keinen konkreten Seitenanker und kann deshalb nicht extern ausgefuehrt werden."
            )
            self._persist_writeback_failure(
                run=run,
                approval_request_id=approval.approval_request_id,
                target_type="confluence_page_update",
                exc=exc,
                verification_metadata=verification_metadata,
            )
            raise exc
        try:
            updated_page = self._confluence_page_write_connector.update_page(
                target=ConfluencePageTarget(
                    page_id=patch_preview.page_id,
                    page_title=patch_preview.page_title,
                    page_url=patch_preview.page_url,
                    space_key=patch_preview.space_key,
                ),
                patch_preview=patch_preview,
                access_token=access_token,
            )
        except Exception as exc:
            self._persist_writeback_failure(
                run=run,
                approval_request_id=approval.approval_request_id,
                target_type="confluence_page_update",
                exc=exc,
                verification_metadata=verification_metadata,
            )
            raise
        details = build_confluence_update_details(
            page_title=updated_page.page_title,
            page_url=updated_page.page_url,
            changed_sections=list(patch_preview.changed_sections),
            change_summary=list(patch_preview.change_summary),
            page_id=updated_page.page_id,
            applied_revision_id=str(updated_page.version_number),
            execution_mode="external_confluence_api",
            patch_preview=patch_preview,
        )
        change = AuditImplementedChange(
            change_type="confluence_page_updated",
            title=f"Seite {updated_page.page_title} aktualisiert",
            summary=(
                "Die Confluence-Seite wurde nach expliziter Freigabe extern ueber die Confluence API aktualisiert "
                "und lokal nachvollziehbar verbucht."
            ),
            target_label=updated_page.page_title,
            target_url=updated_page.page_url,
            related_finding_ids=[finding.finding_id for finding in findings],
            implications=[
                "Der naechste Delta-Lauf muss die aktualisierte Seitenaussage gegen dieselben Claims neu bewerten.",
                "Verknuepfte Entscheidungspakete koennen dadurch aufgeloest oder neu priorisiert werden.",
            ],
            metadata={
                "changed_sections_count": len(details.changed_sections),
                "approval_request_id": approval.approval_request_id,
                "confluence_page_response": updated_page.response_payload,
                "confluence_patch_preview": patch_preview.model_dump(mode="json"),
                "execution_mode": "external_confluence_api",
                "execution_token": approval.metadata.get("execution_token"),
                "writeback_verification": {
                    **verification_metadata,
                    **updated_page.verification_metadata,
                    "verified": True,
                },
            },
            confluence_update=details,
        )
        updated_requests = self._mark_approval_request_executed(
            requests=run.approval_requests,
            approval_request_id=approval.approval_request_id,
            implemented_change_id=change.change_id,
        )
        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "implemented_changes": self._append_implemented_change(
                    implemented_changes=run.implemented_changes,
                    change=change,
                ),
                "approval_requests": updated_requests,
                "analysis_log": self._append_log(
                    analysis_log=run.analysis_log,
                    entry=AuditAnalysisLogEntry(
                        source_type="impact_analysis",
                        title="Confluence-Writeback ausgefuehrt",
                        message=(
                            f"Die Seite {updated_page.page_title} wurde nach expliziter Freigabe extern aktualisiert."
                        ),
                        related_finding_ids=[finding.finding_id for finding in findings],
                        derived_changes=[
                            "Der lokale Approval-Eintrag wurde als ausgefuehrt markiert.",
                            "Die externe Confluence-Seitenantwort wurde im lokalen Vollzugsledger abgelegt.",
                        ],
                        impact_summary=[
                            "Die dokumentierte Wahrheit ist jetzt extern aktualisiert und kann in Folge-Laeufen als neuer Soll-Stand gelesen werden."
                        ],
                        metadata={
                            "approval_request_id": approval.approval_request_id,
                            "writeback_verification": {
                                **verification_metadata,
                                **updated_page.verification_metadata,
                                "verified": True,
                            },
                        },
                    ),
                ),
            }
        )
        return self._repository.upsert_run(run=updated)

    def _build_writeback_verification_metadata(
        self,
        *,
        target_type: str,
        target_url: str | None,
        extra_metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        required_scopes = {
            "jira_ticket_create": {"write:jira-work"},
            "confluence_page_update": {"write:page:confluence"},
        }.get(target_type, set())
        verification: dict[str, object] = {
            "target_type": target_type,
            "target_url": str(target_url or "").strip() or None,
            "required_scopes": sorted(required_scopes),
        }
        if self._atlassian_oauth_service is not None:
            verification.update(
                self._atlassian_oauth_service.build_scope_verification(
                    required_scopes=required_scopes,
                    target_url=target_url,
                    target_type=target_type,
                )
            )
        if target_type == "confluence_page_update":
            preview = (extra_metadata or {}).get("confluence_patch_preview")
            if isinstance(preview, dict):
                verification["page_id"] = str(preview.get("page_id") or "").strip() or None
                verification["patch_execution_ready"] = bool(preview.get("execution_ready"))
                verification["patch_blockers"] = list(preview.get("blockers") or [])
        verification["target_guard"] = self._build_writeback_target_guard(
            target_type=target_type,
            target_url=target_url,
            extra_metadata=extra_metadata,
        )
        return verification

    @staticmethod
    def _build_writeback_preflight(
        *,
        target_type: str,
        verification_metadata: dict[str, object],
    ) -> dict[str, object]:
        blockers: list[str] = []
        warnings: list[str] = []
        if not bool(verification_metadata.get("oauth_ready")):
            blockers.append("OAuth-Kontext ist noch nicht betriebsbereit.")
        if not bool(verification_metadata.get("token_valid", True)):
            blockers.append("Kein gueltiger Atlassian-Token vorhanden.")
        missing_scopes = [str(item).strip() for item in list(verification_metadata.get("missing_scopes") or []) if str(item).strip()]
        if missing_scopes:
            blockers.append(f"Fehlende Scopes: {', '.join(missing_scopes)}")
        if target_type == "confluence_page_update":
            if not bool(verification_metadata.get("patch_execution_ready", True)):
                blockers.append("Confluence-Patch ist noch nicht ausfuehrbar.")
            for blocker in list(verification_metadata.get("patch_blockers") or []):
                text = str(blocker).strip()
                if text:
                    blockers.append(text)
        target_guard = verification_metadata.get("target_guard")
        if isinstance(target_guard, dict):
            blockers.extend(
                str(item).strip()
                for item in list(target_guard.get("blockers") or [])
                if str(item).strip()
            )
            warnings.extend(
                str(item).strip()
                for item in list(target_guard.get("warnings") or [])
                if str(item).strip()
            )
        if verification_metadata.get("target_url") in {None, ""}:
            warnings.append("Ziel-URL ist noch nicht explizit gesetzt.")
        if verification_metadata.get("redirect_uri_matches_local_api") is False:
            warnings.append("OAuth-Redirect stimmt nicht sauber mit der lokalen API ueberein.")
        return {
            "status": "blocked" if blockers else ("warning" if warnings else "ready"),
            "blockers": _dedupe_preserve_order(blockers),
            "warnings": _dedupe_preserve_order(warnings),
        }

    def _build_writeback_target_guard(
        self,
        *,
        target_type: str,
        target_url: str | None,
        extra_metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        normalized_target_url = str(target_url or "").strip() or None
        blockers: list[str] = []
        warnings: list[str] = []
        guard: dict[str, object] = {
            "mode": self._settings.writeback_target_mode,
            "target_type": target_type,
            "target_url": normalized_target_url,
            "allowed_confluence_space_keys": self._settings.get_allowed_writeback_confluence_space_keys(),
            "allowed_jira_project_keys": self._settings.get_allowed_writeback_jira_project_keys(),
        }
        if self._settings.writeback_target_mode == "disabled":
            blockers.append("Externer Writeback ist im aktuellen Betriebsmodus deaktiviert.")
        if target_type == "jira_ticket_create":
            issue_payload = (extra_metadata or {}).get("jira_issue_payload")
            project_key = ""
            if isinstance(issue_payload, dict):
                fields = issue_payload.get("fields")
                if isinstance(fields, dict):
                    project = fields.get("project")
                    if isinstance(project, dict):
                        project_key = _normalize_policy_key(project.get("key"))
            project_key = project_key or _normalize_policy_key(self._settings.fixed_jira_project_key)
            guard["project_key"] = project_key or None
            if not project_key:
                blockers.append("Jira-Writeback hat keinen gueltigen Projekt-Key.")
            elif project_key not in self._settings.get_allowed_writeback_jira_project_keys():
                blockers.append(
                    f"Jira-Writeback auf Projekt {project_key} ist nicht freigegeben."
                )
            expected_host = _normalized_host(self._settings.jira_board_url)
            actual_host = _normalized_host(normalized_target_url)
            guard["expected_host"] = expected_host
            guard["actual_host"] = actual_host
            if expected_host and actual_host and actual_host != expected_host:
                blockers.append(
                    f"Jira-Writeback zeigt auf Host {actual_host} statt auf den freigegebenen Host {expected_host}."
                )
        elif target_type == "confluence_page_update":
            preview = (extra_metadata or {}).get("confluence_patch_preview")
            space_key = ""
            if isinstance(preview, dict):
                space_key = _normalize_policy_key(preview.get("space_key"))
            space_key = space_key or _normalize_policy_key(self._settings.fixed_confluence_space_key)
            guard["space_key"] = space_key or None
            if not space_key:
                blockers.append("Confluence-Writeback hat keinen gueltigen Space-Key.")
            elif space_key not in self._settings.get_allowed_writeback_confluence_space_keys():
                blockers.append(
                    f"Confluence-Writeback auf Space {space_key} ist nicht freigegeben."
                )
            expected_host = _normalized_host(self._settings.confluence_home_url)
            actual_host = _normalized_host(normalized_target_url)
            guard["expected_host"] = expected_host
            guard["actual_host"] = actual_host
            if expected_host and actual_host and actual_host != expected_host:
                blockers.append(
                    f"Confluence-Writeback zeigt auf Host {actual_host} statt auf den freigegebenen Host {expected_host}."
                )
            if not normalized_target_url:
                warnings.append("Confluence-Zielseite ist noch nicht als konkrete URL aufgeloest.")
        guard["blockers"] = _dedupe_preserve_order(blockers)
        guard["warnings"] = _dedupe_preserve_order(warnings)
        guard["allowed"] = not bool(guard["blockers"])
        return guard

    @staticmethod
    def _assert_writeback_target_allowed(
        *,
        target_type: str,
        verification_metadata: dict[str, object],
    ) -> None:
        target_guard = verification_metadata.get("target_guard")
        if not isinstance(target_guard, dict):
            raise ValueError(f"{target_type} kann ohne Target-Guard nicht ausgefuehrt werden.")
        blockers = [str(item).strip() for item in list(target_guard.get("blockers") or []) if str(item).strip()]
        if blockers:
            raise ValueError("; ".join(blockers))

    @staticmethod
    def _build_writeback_execution_token(
        *,
        run: AuditRun,
        target_type: str,
        target_url: str | None,
        related_findings: list[AuditFinding],
    ) -> str:
        raw = "|".join(
            [
                run.run_id,
                target_type,
                str(target_url or "").strip(),
                *sorted(finding.finding_id for finding in related_findings),
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def _return_existing_writeback_execution(
        self,
        *,
        run: AuditRun,
        approval_request_id: str,
        target_type: str,
        expected_change_type: str,
    ) -> AuditRun | None:
        request = next(
            (item for item in run.approval_requests if item.approval_request_id == approval_request_id),
            None,
        )
        if request is None or request.target_type != target_type or request.status != "executed":
            return None
        implemented_change_id = str((request.metadata or {}).get("implemented_change_id") or "").strip()
        if not implemented_change_id:
            raise ValueError("Freigabeanfrage ist bereits als ausgefuehrt markiert, aber der Vollzugsledger ist unvollstaendig.")
        existing_change = next(
            (
                change
                for change in run.implemented_changes
                if change.change_id == implemented_change_id and change.change_type == expected_change_type
            ),
            None,
        )
        if existing_change is None:
            raise ValueError("Writeback wurde bereits markiert, aber der zugehoerige Vollzugseintrag fehlt.")
        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "analysis_log": self._append_log(
                    analysis_log=run.analysis_log,
                    entry=AuditAnalysisLogEntry(
                        source_type="impact_analysis",
                        title="Writeback bereits ausgefuehrt",
                        message=(
                            f"Die Freigabe {approval_request_id} wurde bereits extern ausgefuehrt; der bestehende Vollzugseintrag wird wiederverwendet."
                        ),
                        related_finding_ids=list(existing_change.related_finding_ids),
                        derived_changes=[
                            "Kein zweiter externer Writeback wurde ausgeloest.",
                            f"Vorhandener Vollzugseintrag: {implemented_change_id}.",
                        ],
                        impact_summary=[
                            "Die Ausfuehrung bleibt idempotent und erzeugt keine doppelten externen Artefakte."
                        ],
                        metadata={
                            "approval_request_id": approval_request_id,
                            "implemented_change_id": implemented_change_id,
                            "idempotent_reuse": True,
                        },
                    ),
                ),
            }
        )
        return self._repository.upsert_run(run=updated)

    def _persist_writeback_failure(
        self,
        *,
        run: AuditRun,
        approval_request_id: str,
        target_type: str,
        exc: Exception,
        verification_metadata: dict[str, object],
    ) -> None:
        failure = _classify_writeback_exception(exc=exc)
        updated_requests: list[WritebackApprovalRequest] = []
        target_request: WritebackApprovalRequest | None = None
        for request in run.approval_requests:
            if request.approval_request_id != approval_request_id:
                updated_requests.append(request)
                continue
            metadata = dict(request.metadata or {})
            metadata["writeback_verification"] = verification_metadata
            metadata["last_execution_error"] = failure
            target_request = request.model_copy(update={"metadata": metadata})
            updated_requests.append(target_request)
        updated_run = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "approval_requests": updated_requests,
                "analysis_log": self._append_log(
                    analysis_log=run.analysis_log,
                    entry=AuditAnalysisLogEntry(
                        source_type="impact_analysis",
                        title=f"{target_type} fehlgeschlagen",
                        message=str(failure.get('message') or str(exc)),
                        related_finding_ids=list(target_request.related_finding_ids if target_request else []),
                        derived_changes=[
                            "Der externe Writeback wurde nicht ausgefuehrt.",
                            f"Fehlerklasse: {failure.get('failure_class')}",
                        ],
                        impact_summary=[
                            "Die Freigabe bleibt bestehen, aber der Vollzug muss nach Fehlerbehebung erneut angestossen werden."
                        ],
                        metadata={
                            "approval_request_id": approval_request_id,
                            "writeback_verification": verification_metadata,
                            "last_execution_error": failure,
                        },
                    ),
                ),
            }
        )
        self._repository.upsert_run(run=updated_run)

    def _require_run(self, *, run_id: str) -> AuditRun:
        run = self.get_run(run_id=run_id)
        if run is None:
            raise ValueError(f"Audit-Run nicht gefunden: {run_id}")
        return run

    @staticmethod
    def _require_package(*, run: AuditRun, package_id: str) -> DecisionPackage:
        for package in run.decision_packages:
            if package.package_id == package_id:
                return package
        raise ValueError(f"Entscheidungspaket nicht gefunden: {package_id}")

    @staticmethod
    def _require_approved_request(
        *,
        run: AuditRun,
        approval_request_id: str,
        target_type: str,
    ) -> WritebackApprovalRequest:
        for request in run.approval_requests:
            if request.approval_request_id != approval_request_id:
                continue
            if request.target_type != target_type:
                raise ValueError("Freigabeanfrage passt nicht zum angeforderten Writeback-Typ.")
            if request.status != "approved":
                raise ValueError("Freigabeanfrage ist nicht genehmigt und blockiert den Writeback.")
            return request
        raise ValueError(f"Freigabeanfrage nicht gefunden: {approval_request_id}")

    def _normalize_target(self, *, target: AuditTarget) -> AuditTarget:
        return target.model_copy(
            update={
                "confluence_space_keys": [self._settings.fixed_confluence_space_key],
                "jira_project_keys": [self._settings.fixed_jira_project_key],
                "include_metamodel": True,
            }
        )

    @staticmethod
    def _build_demo_snapshot_ids() -> dict[str, str]:
        return {
            "repo": f"snapshot_repo_demo_{uuid4().hex}",
            "confluence": f"snapshot_confluence_demo_{uuid4().hex}",
            "local_doc": f"snapshot_local_docs_demo_{uuid4().hex}",
            "metamodel": f"snapshot_metamodel_demo_{uuid4().hex}",
        }

    def _build_demo_findings(self, *, run: AuditRun, snapshot_ids: dict[str, str]) -> list[AuditFinding]:
        repo_hint = run.target.local_repo_path or run.target.github_repo_url or "UNKNOWN"
        space_hint = run.target.confluence_space_keys[0] if run.target.confluence_space_keys else "UNKNOWN"
        return [
            AuditFinding(
                severity="high",
                category="implementation_drift",
                title="Dokumentierter Schreibpfad ist im Code nicht nachweisbar",
                summary=(
                    "Die Doku beschreibt einen kanonischen Write-Pfad fuer BSM-Artefakte, "
                    "der in der aktuellen Codebasis noch nicht eindeutig verankert ist."
                ),
                recommendation=(
                    "Claim-Modell fuer das betroffene Objekt haerten und den Write-Pfad "
                    "gegen reale Service- und Router-Stellen nachziehen."
                ),
                locations=[
                    AuditLocation(
                        snapshot_id=snapshot_ids["repo"],
                        source_type="github_file",
                        source_id="demo-code-1",
                        title="Repo-Snapshot",
                        path_hint=repo_hint,
                        position=AuditPosition(
                            anchor_kind="file_line_range",
                            anchor_value="src/demo/service.py#L42-L67",
                            section_path="DemoService.persist",
                            line_start=42,
                            line_end=67,
                            snippet_hash="sha256:demo-write-path",
                            content_hash="sha256:demo-repo-snapshot",
                        ),
                        metadata={"origin": "demo", "role": "implemented_path"},
                    ),
                    AuditLocation(
                        snapshot_id=snapshot_ids["confluence"],
                        source_type="confluence_page",
                        source_id="demo-page-1",
                        title=f"Confluence Space {space_hint}",
                        path_hint=f"Space: {space_hint}",
                        url=self._settings.confluence_home_url,
                        position=AuditPosition(
                            anchor_kind="confluence_heading_path",
                            anchor_value="BSM Prozess > Persistenz > Statement Writer",
                            section_path="BSM Prozess/Statement Writer",
                            char_start=820,
                            char_end=1120,
                            snippet_hash="sha256:demo-confluence-fragment",
                            content_hash="sha256:demo-confluence-page",
                        ),
                        metadata={"origin": "demo", "role": "documented_path"},
                    ),
                ],
                proposed_confluence_action="Abschnitt mit aktuellem Runtime-Vertrag und evidenzbasierten Referenzen nachschaerfen.",
                proposed_jira_action="Ticket fuer Vereinheitlichung von Doku und Write-Contract erzeugen.",
                metadata={"delta_tracking": "enabled", "object_key": "Statement.write_path"},
            ),
            AuditFinding(
                severity="medium",
                category="missing_definition",
                title="Objekt-Lifecycle ist fachlich nicht vollstaendig beschrieben",
                summary=(
                    "Fuer mindestens ein zentrales Objekt fehlt eine belastbare Beschreibung zu Status, "
                    "Promotion und Historisierung."
                ),
                recommendation="Objektkarte um Lifecycle, Scope und erlaubte Read-/Write-Operationen erweitern.",
                locations=[
                    AuditLocation(
                        snapshot_id=snapshot_ids["local_doc"],
                        source_type="local_doc",
                        source_id="demo-doc-1",
                        title="Lokale Planungsdoku",
                        path_hint="docs/roadmap.md",
                        position=AuditPosition(
                            anchor_kind="markdown_heading_path",
                            anchor_value="Stand 2 > Rueckfragen",
                            section_path="Stand 2/Rueckfragen",
                            line_start=20,
                            line_end=42,
                            snippet_hash="sha256:demo-local-doc",
                            content_hash="sha256:demo-local-doc-version",
                        ),
                        metadata={"origin": "demo", "role": "missing_definition"},
                    )
                ],
                proposed_confluence_action="Fehlende Lifecycle-Definition in die fachliche SSOT-Seite aufnehmen.",
                proposed_jira_action="Analyse-Ticket fuer fehlende Lifecycle-Definition anlegen.",
                metadata={"delta_tracking": "enabled", "object_key": "Statement.lifecycle"},
            ),
        ]

    def _build_demo_claims(self, *, run: AuditRun, snapshot_ids: dict[str, str]) -> list[AuditClaimEntry]:
        return [
            AuditClaimEntry(
                source_snapshot_id=snapshot_ids["repo"],
                source_type="github_file",
                source_id="demo-code-1",
                subject_kind="object_property",
                subject_key="Statement.write_path",
                predicate="implemented_as",
                normalized_value="DemoService.persist",
                scope_kind="project",
                scope_key="FINAI",
                confidence=0.89,
                fingerprint="Statement.write_path|implemented_as|DemoService.persist|FINAI",
                evidence_location_ids=[],
                metadata={"origin": "demo"},
            ),
            AuditClaimEntry(
                source_snapshot_id=snapshot_ids["confluence"],
                source_type="confluence_page",
                source_id="demo-page-1",
                subject_kind="object_property",
                subject_key="Statement.write_path",
                predicate="documented_as",
                normalized_value="BSM Prozess/Statement Writer",
                scope_kind="project",
                scope_key="FINAI",
                confidence=0.83,
                fingerprint="Statement.write_path|documented_as|BSM Prozess/Statement Writer|FINAI",
                metadata={"origin": "demo"},
            ),
            AuditClaimEntry(
                source_snapshot_id=snapshot_ids["local_doc"],
                source_type="local_doc",
                source_id="demo-doc-1",
                subject_kind="object_property",
                subject_key="Statement.lifecycle",
                predicate="documentation_status",
                normalized_value="missing_definition",
                scope_kind="project",
                scope_key="FINAI",
                confidence=0.78,
                fingerprint="Statement.lifecycle|documentation_status|missing_definition|FINAI",
                metadata={"origin": "demo"},
            ),
            AuditClaimEntry(
                source_snapshot_id=snapshot_ids["metamodel"],
                source_type="metamodel",
                source_id="finai-current-dump",
                subject_kind="process",
                subject_key="BSM.process",
                predicate="phase_source",
                normalized_value="metamodel_current_dump",
                scope_kind="global",
                scope_key="FINAI",
                confidence=0.92,
                fingerprint="BSM.process|phase_source|metamodel_current_dump|FINAI",
                metadata={"origin": "demo"},
            ),
        ]

    @staticmethod
    def _build_demo_truths() -> list[TruthLedgerEntry]:
        return [
            TruthLedgerEntry(
                canonical_key="BSM.process.phase_source",
                subject_kind="process",
                subject_key="BSM.process",
                predicate="phase_source",
                normalized_value="metamodel_current_dump",
                scope_kind="global",
                scope_key="FINAI",
                source_kind="system_inference",
                metadata={"origin": "demo"},
            )
        ]

    @staticmethod
    def _build_decision_packages(
        *,
        findings: list[AuditFinding],
        claims: list[AuditClaimEntry],
        truths: list[TruthLedgerEntry],
        semantic_entities: list[SemanticEntity],
        semantic_relations: list[SemanticRelation],
    ) -> list[DecisionPackage]:
        claim_map: dict[str, list[AuditClaimEntry]] = {}
        for claim in claims:
            claim_map.setdefault(claim.subject_key, []).append(claim)
        truth_map: dict[str, list[TruthLedgerEntry]] = {}
        for truth in truths:
            truth_map.setdefault(truth.subject_key, []).append(truth)
        entities_by_id = {entity.entity_id: entity for entity in semantic_entities}
        findings_by_cluster: dict[str, list[AuditFinding]] = defaultdict(list)
        actionable_findings = [
            finding
            for finding in findings
            if finding.category != "architecture_observation"
        ]
        prioritized_actionable_findings = prioritize_findings(findings=actionable_findings)
        finding_priority_ranks = {
            finding.finding_id: index
            for index, finding in enumerate(prioritized_actionable_findings)
        }
        for finding in prioritized_actionable_findings:
            findings_by_cluster[_package_cluster_key(finding=finding)].append(finding)
        packages: list[DecisionPackage] = []
        for cluster_key, cluster_findings in sorted(findings_by_cluster.items(), key=lambda item: item[0]):
            cluster_scope_keys = _package_scope_keys_for_findings(findings=cluster_findings)
            if not cluster_scope_keys:
                cluster_scope_keys = {package_scope_key(cluster_key)}
            display_label = _package_group_label(group_key=cluster_key, findings=cluster_findings)
            primary_cluster_scope_key = _primary_scope_key_for_findings(
                findings=cluster_findings,
                fallback_scope_keys=cluster_scope_keys,
            )
            related_claims = _dedupe_claims(
                [
                    claim
                    for claim_group in claim_map.values()
                    for claim in claim_group
                    if _claim_matches_any_package_scope(claim=claim, package_scope_keys=cluster_scope_keys)
                    or _claim_is_semantically_attached_to_cluster(claim=claim, cluster_key=primary_cluster_scope_key)
                ]
            )
            related_truths = [
                truth
                for truth_group in truth_map.values()
                for truth in truth_group
                if _claim_matches_any_truth_scope(truth=truth, package_scope_keys=cluster_scope_keys)
            ] or truth_map.get("BSM.process", [])
            available_core_buckets = {
                finding_root_cause_bucket(finding=finding)
                for finding in cluster_findings
                if is_core_root_cause_bucket(bucket=finding_root_cause_bucket(finding=finding))
            }
            package_findings_by_bucket: dict[str, list[AuditFinding]] = defaultdict(list)
            for finding in cluster_findings:
                assigned_bucket = assigned_root_cause_bucket(
                    finding=finding,
                    available_core_buckets=available_core_buckets,
                )
                package_findings_by_bucket[assigned_bucket].append(finding)

            for root_bucket, bucket_findings in sorted(
                package_findings_by_bucket.items(),
                key=lambda item: (
                    root_cause_priority(bucket=item[0]),
                    min(severity_rank(severity=finding.severity) for finding in item[1]),
                    item[0],
                ),
            ):
                ordered_bucket_findings = order_package_findings(
                    findings=bucket_findings,
                    package_bucket=root_bucket,
                )
                primary_finding = select_primary_finding(
                    findings=ordered_bucket_findings,
                    package_bucket=root_bucket,
                )
                dominant_category = primary_finding.category if primary_finding is not None else _dominant_category(findings=ordered_bucket_findings)
                severity_summary = _highest_severity(findings=ordered_bucket_findings)
                primary_finding_id = primary_finding.finding_id if primary_finding is not None else None
                primary_finding_rank = (
                    int(finding_priority_ranks.get(primary_finding_id, 999_999))
                    if primary_finding_id is not None
                    else 999_999
                )
                package_scope_keys = _package_scope_keys_for_findings(findings=ordered_bucket_findings) or set(cluster_scope_keys)
                primary_scope_key = _primary_scope_key_for_findings(
                    findings=ordered_bucket_findings,
                    fallback_scope_keys=package_scope_keys,
                )
                package_related_claims = _dedupe_claims(
                    [
                        claim
                        for claim in related_claims
                        if _claim_matches_any_package_scope(claim=claim, package_scope_keys=package_scope_keys)
                    ]
                ) or related_claims
                package_related_truths = [
                    truth
                    for truth in related_truths
                    if _claim_matches_any_truth_scope(truth=truth, package_scope_keys=package_scope_keys)
                ] or related_truths
                package_semantic_entity_ids = _dedupe_preserve_order(
                    [
                        entity_id
                        for claim in package_related_claims
                        for entity_id in _string_list_from_metadata(claim.metadata.get("semantic_entity_ids"))
                    ]
                )
                related_entities = [
                    entities_by_id[entity_id] for entity_id in package_semantic_entity_ids if entity_id in entities_by_id
                ]
                related_relations = _semantic_relations_for_entity_ids(
                    semantic_relations=semantic_relations,
                    entity_ids=set(package_semantic_entity_ids),
                )
                semantic_context = _dedupe_preserve_order(
                    [f"{entity.entity_type}:{entity.label}" for entity in related_entities]
                )
                semantic_relation_summaries = _dedupe_preserve_order(
                    [
                        _semantic_relation_summary(
                            relation=relation,
                            entities_by_id=entities_by_id,
                        )
                        for relation in related_relations
                    ]
                )
                semantic_contract_paths = _dedupe_preserve_order(
                    [
                        contract_path
                        for finding in ordered_bucket_findings
                        for contract_path in _string_list_from_metadata(finding.metadata.get("semantic_contract_paths"))
                    ]
                )
                semantic_section_paths = _dedupe_preserve_order(
                    [
                        section_path
                        for finding in ordered_bucket_findings
                        for section_path in _string_list_from_metadata(finding.metadata.get("semantic_section_paths"))
                    ]
                )
                causal_write_deciders = _dedupe_preserve_order(
                    [
                        label
                        for finding in ordered_bucket_findings
                        for label in _string_list_from_metadata(finding.metadata.get("causal_write_decider_labels"))
                    ]
                )
                causal_write_apis = _dedupe_preserve_order(
                    [
                        api
                        for finding in ordered_bucket_findings
                        for api in _string_list_from_metadata(finding.metadata.get("causal_write_apis"))
                    ]
                )
                causal_repository_adapters = _dedupe_preserve_order(
                    [
                        adapter
                        for finding in ordered_bucket_findings
                        for adapter in _string_list_from_metadata(finding.metadata.get("causal_repository_adapters"))
                    ]
                )
                causal_repository_adapter_symbols = _dedupe_preserve_order(
                    [
                        adapter
                        for finding in ordered_bucket_findings
                        for adapter in _string_list_from_metadata(finding.metadata.get("causal_repository_adapter_symbols"))
                    ]
                )
                causal_driver_adapters = _dedupe_preserve_order(
                    [
                        adapter
                        for finding in ordered_bucket_findings
                        for adapter in _string_list_from_metadata(finding.metadata.get("causal_driver_adapters"))
                    ]
                )
                causal_driver_adapter_symbols = _dedupe_preserve_order(
                    [
                        adapter
                        for finding in ordered_bucket_findings
                        for adapter in _string_list_from_metadata(finding.metadata.get("causal_driver_adapter_symbols"))
                    ]
                )
                causal_transaction_boundaries = _dedupe_preserve_order(
                    [
                        boundary
                        for finding in ordered_bucket_findings
                        for boundary in _string_list_from_metadata(finding.metadata.get("causal_transaction_boundaries"))
                    ]
                )
                causal_retry_paths = _dedupe_preserve_order(
                    [
                        retry_path
                        for finding in ordered_bucket_findings
                        for retry_path in _string_list_from_metadata(finding.metadata.get("causal_retry_paths"))
                    ]
                )
                causal_batch_paths = _dedupe_preserve_order(
                    [
                        batch_path
                        for finding in ordered_bucket_findings
                        for batch_path in _string_list_from_metadata(finding.metadata.get("causal_batch_paths"))
                    ]
                )
                causal_persistence_targets = _dedupe_preserve_order(
                    [
                        target
                        for finding in ordered_bucket_findings
                        for target in _string_list_from_metadata(finding.metadata.get("causal_persistence_targets"))
                    ]
                )
                causal_persistence_sink_kinds = _dedupe_preserve_order(
                    [
                        sink_kind
                        for finding in ordered_bucket_findings
                        for sink_kind in _string_list_from_metadata(finding.metadata.get("causal_persistence_sink_kinds"))
                    ]
                )
                causal_persistence_backends = _dedupe_preserve_order(
                    [
                        backend
                        for finding in ordered_bucket_findings
                        for backend in _string_list_from_metadata(finding.metadata.get("causal_persistence_backends"))
                    ]
                )
                causal_persistence_operation_types = _dedupe_preserve_order(
                    [
                        operation_type
                        for finding in ordered_bucket_findings
                        for operation_type in _string_list_from_metadata(
                            finding.metadata.get("causal_persistence_operation_types")
                        )
                    ]
                )
                causal_persistence_schema_targets = _dedupe_preserve_order(
                    [
                        schema_target
                        for finding in ordered_bucket_findings
                        for schema_target in _string_list_from_metadata(
                            finding.metadata.get("causal_persistence_schema_targets")
                        )
                    ]
                )
                causal_schema_validated_targets = _dedupe_preserve_order(
                    [
                        target
                        for finding in ordered_bucket_findings
                        for target in _string_list_from_metadata(finding.metadata.get("causal_schema_validated_targets"))
                    ]
                )
                causal_schema_observed_only_targets = _dedupe_preserve_order(
                    [
                        target
                        for finding in ordered_bucket_findings
                        for target in _string_list_from_metadata(finding.metadata.get("causal_schema_observed_only_targets"))
                    ]
                )
                causal_schema_unconfirmed_targets = _dedupe_preserve_order(
                    [
                        target
                        for finding in ordered_bucket_findings
                        for target in _string_list_from_metadata(finding.metadata.get("causal_schema_unconfirmed_targets"))
                    ]
                )
                causal_schema_validation_statuses = _dedupe_preserve_order(
                    [
                        status
                        for finding in ordered_bucket_findings
                        for status in _string_list_from_metadata(finding.metadata.get("causal_schema_validation_statuses"))
                    ]
                )

                problems: list[DecisionProblemElement] = []
                bucket_retrieval_context: list[str] = []
                bucket_anchor_values: list[str] = []
                bucket_delta_summary: list[str] = []
                bucket_delta_statuses: list[str] = []
                bucket_delta_reasons: list[str] = []
                related_finding_ids: list[str] = []
                for finding in ordered_bucket_findings:
                    scope_key = str(finding.metadata.get("object_key") or finding.canonical_key or finding.title)
                    atomic_fact_claims = _claims_for_finding_scope(
                        finding=finding,
                        claims=package_related_claims,
                    )
                    atomic_fact_summary = _atomic_fact_summary(
                        finding=finding,
                        claims=atomic_fact_claims,
                    )
                    action_lane = _preferred_action_lane_for_finding(finding=finding)
                    retrieval_context = _string_list_from_metadata(finding.metadata.get("retrieval_context"))
                    retrieval_anchor_values = _string_list_from_metadata(finding.metadata.get("retrieval_anchor_values"))
                    delta_summary = _string_list_from_metadata(finding.metadata.get("delta_summary"))
                    delta_statuses = _string_list_from_metadata(finding.metadata.get("delta_statuses"))
                    delta_reasons = _string_list_from_metadata(finding.metadata.get("delta_reasons"))
                    related_finding_ids.append(finding.finding_id)
                    bucket_retrieval_context.extend(retrieval_context)
                    bucket_anchor_values.extend(retrieval_anchor_values)
                    bucket_delta_summary.extend(delta_summary)
                    bucket_delta_statuses.extend(delta_statuses)
                    bucket_delta_reasons.extend(delta_reasons)
                    problems.append(
                        DecisionProblemElement(
                            finding_id=finding.finding_id,
                            category=finding.category,
                            severity=finding.severity,
                            scope_summary=scope_key,
                            short_explanation=finding.summary,
                            recommendation=finding.recommendation,
                            confidence=0.8 if finding.severity in {"critical", "high"} else 0.66,
                            affected_claim_ids=[claim.claim_id for claim in atomic_fact_claims],
                            affected_truth_ids=[truth.truth_id for truth in package_related_truths],
                            evidence_locations=list(finding.locations),
                            metadata={
                                "origin_finding_id": finding.finding_id,
                                "atomic_fact_key": _atomic_fact_key(finding=finding),
                                "atomic_fact_summary": atomic_fact_summary,
                                "atomic_fact_subject_keys": _dedupe_preserve_order(
                                    [claim.subject_key for claim in atomic_fact_claims]
                                ),
                                "atomic_fact_predicates": _dedupe_preserve_order(
                                    [claim.predicate for claim in atomic_fact_claims]
                                ),
                                "atomic_fact_source_types": _dedupe_preserve_order(
                                    [claim.source_type for claim in atomic_fact_claims]
                                ),
                                "atomic_fact_source_ids": _dedupe_preserve_order(
                                    [claim.source_id for claim in atomic_fact_claims]
                                ),
                                "action_lane": action_lane,
                                "atomic_fact_status": "open",
                                "retrieval_context": retrieval_context,
                                "delta_summary": delta_summary,
                                "delta_statuses": delta_statuses,
                                "semantic_context": _string_list_from_metadata(finding.metadata.get("semantic_context")),
                                "semantic_relation_summaries": _string_list_from_metadata(
                                    finding.metadata.get("semantic_relation_summaries")
                                ),
                                "semantic_contract_paths": _string_list_from_metadata(
                                    finding.metadata.get("semantic_contract_paths")
                                ),
                                "semantic_section_paths": _string_list_from_metadata(
                                    finding.metadata.get("semantic_section_paths")
                                ),
                                "causal_write_decider_labels": _string_list_from_metadata(
                                    finding.metadata.get("causal_write_decider_labels")
                                ),
                                "causal_write_apis": _string_list_from_metadata(
                                    finding.metadata.get("causal_write_apis")
                                ),
                                "causal_repository_adapters": _string_list_from_metadata(
                                    finding.metadata.get("causal_repository_adapters")
                                ),
                                "causal_repository_adapter_symbols": _string_list_from_metadata(
                                    finding.metadata.get("causal_repository_adapter_symbols")
                                ),
                                "causal_driver_adapters": _string_list_from_metadata(
                                    finding.metadata.get("causal_driver_adapters")
                                ),
                                "causal_driver_adapter_symbols": _string_list_from_metadata(
                                    finding.metadata.get("causal_driver_adapter_symbols")
                                ),
                                "causal_transaction_boundaries": _string_list_from_metadata(
                                    finding.metadata.get("causal_transaction_boundaries")
                                ),
                                "causal_retry_paths": _string_list_from_metadata(
                                    finding.metadata.get("causal_retry_paths")
                                ),
                                "causal_batch_paths": _string_list_from_metadata(
                                    finding.metadata.get("causal_batch_paths")
                                ),
                                "causal_persistence_targets": _string_list_from_metadata(
                                    finding.metadata.get("causal_persistence_targets")
                                ),
                                "causal_persistence_sink_kinds": _string_list_from_metadata(
                                    finding.metadata.get("causal_persistence_sink_kinds")
                                ),
                                "causal_persistence_backends": _string_list_from_metadata(
                                    finding.metadata.get("causal_persistence_backends")
                                ),
                                "causal_persistence_operation_types": _string_list_from_metadata(
                                    finding.metadata.get("causal_persistence_operation_types")
                                ),
                                "causal_persistence_schema_targets": _string_list_from_metadata(
                                    finding.metadata.get("causal_persistence_schema_targets")
                                ),
                                "causal_schema_validated_targets": _string_list_from_metadata(
                                    finding.metadata.get("causal_schema_validated_targets")
                                ),
                                "causal_schema_observed_only_targets": _string_list_from_metadata(
                                    finding.metadata.get("causal_schema_observed_only_targets")
                                ),
                                "causal_schema_unconfirmed_targets": _string_list_from_metadata(
                                    finding.metadata.get("causal_schema_unconfirmed_targets")
                                ),
                                "causal_schema_validation_statuses": _string_list_from_metadata(
                                    finding.metadata.get("causal_schema_validation_statuses")
                                ),
                                "root_cause_bucket": finding_root_cause_bucket(finding=finding),
                                "assigned_root_cause_bucket": root_bucket,
                                "root_cause_role": "primary" if finding.finding_id == primary_finding_id else "supporting",
                            },
                        )
                    )

                packages.append(
                    DecisionPackage(
                        title=_package_title(
                            cluster_key=display_label,
                            root_cause_bucket=root_bucket,
                            findings=ordered_bucket_findings,
                        ),
                        category=dominant_category,
                        severity_summary=severity_summary,
                        scope_summary=_package_scope_summary(
                            cluster_key=display_label,
                            root_cause_bucket=root_bucket,
                            findings=ordered_bucket_findings,
                            semantic_entities=related_entities,
                            semantic_relations=related_relations,
                        ),
                        rerender_required_after_decision=bool(bucket_delta_statuses),
                        recommendation_summary=_package_recommendation_summary(
                            findings=ordered_bucket_findings,
                            root_cause_bucket=root_bucket,
                            semantic_context=semantic_context,
                            semantic_relation_summaries=semantic_relation_summaries,
                            semantic_contract_paths=semantic_contract_paths,
                            causal_write_deciders=causal_write_deciders,
                            causal_write_apis=causal_write_apis,
                            causal_repository_adapters=causal_repository_adapters,
                            causal_repository_adapter_symbols=causal_repository_adapter_symbols,
                            causal_driver_adapters=causal_driver_adapters,
                            causal_driver_adapter_symbols=causal_driver_adapter_symbols,
                            causal_transaction_boundaries=causal_transaction_boundaries,
                            causal_retry_paths=causal_retry_paths,
                            causal_batch_paths=causal_batch_paths,
                            causal_persistence_targets=causal_persistence_targets,
                            causal_persistence_sink_kinds=causal_persistence_sink_kinds,
                            causal_persistence_backends=causal_persistence_backends,
                            causal_persistence_operation_types=causal_persistence_operation_types,
                            causal_persistence_schema_targets=causal_persistence_schema_targets,
                            causal_schema_validated_targets=causal_schema_validated_targets,
                            causal_schema_observed_only_targets=causal_schema_observed_only_targets,
                            causal_schema_unconfirmed_targets=causal_schema_unconfirmed_targets,
                            causal_schema_validation_statuses=causal_schema_validation_statuses,
                        ),
                        related_finding_ids=_dedupe_preserve_order(related_finding_ids),
                        problem_elements=problems,
                        metadata={
                            "origin": "analysis",
                            "cluster_key": primary_scope_key,
                            "group_key": cluster_key,
                            "group_label": display_label,
                            "primary_scope_key": primary_scope_key,
                            "scope_keys": sorted(package_scope_keys),
                            "cluster_categories": _dedupe_preserve_order([finding.category for finding in ordered_bucket_findings]),
                            "retrieval_context": _dedupe_preserve_order(bucket_retrieval_context),
                            "retrieval_anchor_values": _dedupe_preserve_order(bucket_anchor_values),
                            "delta_summary": _dedupe_preserve_order(bucket_delta_summary),
                            "delta_statuses": _dedupe_preserve_order(bucket_delta_statuses),
                            "delta_reasons": _dedupe_preserve_order(bucket_delta_reasons),
                            "truth_overlap_keys": [truth.canonical_key for truth in package_related_truths],
                            "semantic_entity_ids": package_semantic_entity_ids,
                            "semantic_context": semantic_context,
                            "semantic_relation_summaries": semantic_relation_summaries,
                            "semantic_contract_paths": semantic_contract_paths,
                            "semantic_section_paths": semantic_section_paths,
                            "causal_write_deciders": causal_write_deciders,
                            "causal_write_apis": causal_write_apis,
                            "causal_repository_adapters": causal_repository_adapters,
                            "causal_repository_adapter_symbols": causal_repository_adapter_symbols,
                            "causal_driver_adapters": causal_driver_adapters,
                            "causal_driver_adapter_symbols": causal_driver_adapter_symbols,
                            "causal_transaction_boundaries": causal_transaction_boundaries,
                            "causal_retry_paths": causal_retry_paths,
                            "causal_batch_paths": causal_batch_paths,
                            "causal_persistence_targets": causal_persistence_targets,
                            "causal_persistence_sink_kinds": causal_persistence_sink_kinds,
                            "causal_persistence_backends": causal_persistence_backends,
                            "causal_persistence_operation_types": causal_persistence_operation_types,
                            "causal_persistence_schema_targets": causal_persistence_schema_targets,
                            "causal_schema_validated_targets": causal_schema_validated_targets,
                            "causal_schema_observed_only_targets": causal_schema_observed_only_targets,
                            "causal_schema_unconfirmed_targets": causal_schema_unconfirmed_targets,
                            "causal_schema_validation_statuses": causal_schema_validation_statuses,
                            "root_cause_bucket": root_bucket,
                            "root_cause_label": root_cause_label(bucket=root_bucket),
                            "root_cause_priority": root_cause_priority(bucket=root_bucket),
                            "primary_finding_id": primary_finding_id,
                            "primary_finding_rank": primary_finding_rank,
                            "supporting_problem_count": max(len(problems) - 1, 0),
                            "action_lanes": _dedupe_preserve_order(
                                [
                                    str(problem.metadata.get("action_lane") or "").strip()
                                    for problem in problems
                                    if str(problem.metadata.get("action_lane") or "").strip()
                                ]
                            ),
                            "atomic_facts": [
                                {
                                    "fact_key": str(problem.metadata.get("atomic_fact_key") or "").strip(),
                                    "summary": str(problem.metadata.get("atomic_fact_summary") or "").strip(),
                                    "action_lane": str(problem.metadata.get("action_lane") or "").strip(),
                                    "status": str(problem.metadata.get("atomic_fact_status") or "open"),
                                    "source_types": list(problem.metadata.get("atomic_fact_source_types") or []),
                                    "source_ids": list(problem.metadata.get("atomic_fact_source_ids") or []),
                                }
                                for problem in problems
                            ],
                            "package_sort_rank": (
                                root_cause_priority(bucket=root_bucket) * 10
                                + severity_rank(severity=severity_summary)
                            ),
                            "causal_group_key": str(
                                (primary_finding.metadata.get("causal_group_key") if primary_finding is not None else "")
                                or cluster_key
                            ),
                            "grouped_boundary_paths": any(
                                bool(finding.metadata.get("grouped_boundary_paths")) for finding in ordered_bucket_findings
                            ),
                            "boundary_path_type": next(
                                (
                                    str(finding.metadata.get("boundary_path_type") or "").strip()
                                    for finding in ordered_bucket_findings
                                    if str(finding.metadata.get("boundary_path_type") or "").strip()
                                ),
                                "",
                            ),
                            "boundary_path_count": max(
                                (
                                    int(finding.metadata.get("path_count") or 0)
                                    for finding in ordered_bucket_findings
                                    if finding.metadata.get("path_count") is not None
                                ),
                                default=0,
                            ),
                            "boundary_function_names": _dedupe_preserve_order(
                                [
                                    str(function_name).strip()
                                    for finding in ordered_bucket_findings
                                    for function_name in (finding.metadata.get("boundary_function_names") or [])
                                    if str(function_name).strip()
                                ]
                            ),
                            "grouped_eventual_paths": any(
                                bool(finding.metadata.get("grouped_eventual_paths")) for finding in ordered_bucket_findings
                            ),
                            "eventual_path_type": next(
                                (
                                    str(finding.metadata.get("eventual_path_type") or "").strip()
                                    for finding in ordered_bucket_findings
                                    if str(finding.metadata.get("eventual_path_type") or "").strip()
                                ),
                                "",
                            ),
                            "eventual_path_count": max(
                                (
                                    int(finding.metadata.get("path_count") or 0)
                                    for finding in ordered_bucket_findings
                                    if bool(finding.metadata.get("grouped_eventual_paths"))
                                    and finding.metadata.get("path_count") is not None
                                ),
                                default=0,
                            ),
                            "eventual_function_names": _dedupe_preserve_order(
                                [
                                    str(function_name).strip()
                                    for finding in ordered_bucket_findings
                                    for function_name in (finding.metadata.get("sequence_functions") or [])
                                    if bool(finding.metadata.get("grouped_eventual_paths")) and str(function_name).strip()
                                ]
                            ),
                            "grouped_chain_paths": any(
                                bool(finding.metadata.get("grouped_chain_paths")) for finding in ordered_bucket_findings
                            ),
                            "chain_path_type": next(
                                (
                                    str(finding.metadata.get("chain_path_type") or "").strip()
                                    for finding in ordered_bucket_findings
                                    if str(finding.metadata.get("chain_path_type") or "").strip()
                                ),
                                "",
                            ),
                            "chain_path_count": max(
                                (
                                    int(finding.metadata.get("path_count") or 0)
                                    for finding in ordered_bucket_findings
                                    if bool(finding.metadata.get("grouped_chain_paths"))
                                    and finding.metadata.get("path_count") is not None
                                ),
                                default=0,
                            ),
                            "chain_function_names": _dedupe_preserve_order(
                                [
                                    str(function_name).strip()
                                    for finding in ordered_bucket_findings
                                    for function_name in (finding.metadata.get("sequence_functions") or [])
                                    if bool(finding.metadata.get("grouped_chain_paths")) and str(function_name).strip()
                                ]
                            ),
                            "chain_line_windows": _dedupe_preserve_order(
                                [
                                    str(line_window).strip()
                                    for finding in ordered_bucket_findings
                                    for line_window in (finding.metadata.get("sequence_line_windows") or [])
                                    if bool(finding.metadata.get("grouped_chain_paths")) and str(line_window).strip()
                                ]
                            ),
                        },
                    )
                )
        return sorted(
            packages,
            key=lambda package: (
                int(package.metadata.get("package_sort_rank")) if package.metadata.get("package_sort_rank") is not None else 999,
                int(package.metadata.get("primary_finding_rank")) if package.metadata.get("primary_finding_rank") is not None else 999_999,
                severity_rank(severity=package.severity_summary),
                package.title,
            ),
        )

    @staticmethod
    def _build_demo_decision_packages(
        *,
        findings: list[AuditFinding],
        claims: list[AuditClaimEntry],
        truths: list[TruthLedgerEntry],
    ) -> list[DecisionPackage]:
        return AuditService._build_decision_packages(
            findings=findings,
            claims=claims,
            truths=truths,
            semantic_entities=[],
            semantic_relations=[],
        )

    @staticmethod
    def _build_atomic_facts(*, packages: list[DecisionPackage]) -> list[AtomicFactEntry]:
        facts_by_key: dict[str, AtomicFactEntry] = {}
        for package in packages:
            for problem in package.problem_elements:
                metadata = dict(problem.metadata or {})
                fact_key = str(metadata.get("atomic_fact_key") or "").strip()
                summary = str(metadata.get("atomic_fact_summary") or "").strip()
                action_lane = str(metadata.get("action_lane") or "").strip()
                if not fact_key or not summary or not action_lane:
                    continue
                current = facts_by_key.get(fact_key)
                if current is None:
                    facts_by_key[fact_key] = AtomicFactEntry(
                        fact_key=fact_key,
                        summary=summary,
                        action_lane=action_lane,
                        primary_package_id=package.package_id,
                        primary_problem_id=problem.problem_id,
                        related_package_ids=[package.package_id],
                        related_problem_ids=[problem.problem_id],
                        related_finding_ids=[problem.finding_id] if problem.finding_id else [],
                        source_types=list(metadata.get("atomic_fact_source_types") or []),
                        source_ids=list(metadata.get("atomic_fact_source_ids") or []),
                        subject_keys=list(metadata.get("atomic_fact_subject_keys") or []),
                        predicates=list(metadata.get("atomic_fact_predicates") or []),
                        claim_ids=list(problem.affected_claim_ids),
                        truth_ids=list(problem.affected_truth_ids),
                        metadata={
                            "root_cause_bucket": metadata.get("assigned_root_cause_bucket") or metadata.get("root_cause_bucket"),
                            "root_cause_role": metadata.get("root_cause_role"),
                            "scope_summary": problem.scope_summary,
                        },
                    )
                    continue
                current.related_package_ids = _dedupe_preserve_order([*current.related_package_ids, package.package_id])
                current.related_problem_ids = _dedupe_preserve_order([*current.related_problem_ids, problem.problem_id])
                current.related_finding_ids = _dedupe_preserve_order(
                    [*current.related_finding_ids, *([problem.finding_id] if problem.finding_id else [])]
                )
                current.source_types = _dedupe_preserve_order([*current.source_types, *list(metadata.get("atomic_fact_source_types") or [])])
                current.source_ids = _dedupe_preserve_order([*current.source_ids, *list(metadata.get("atomic_fact_source_ids") or [])])
                current.subject_keys = _dedupe_preserve_order([*current.subject_keys, *list(metadata.get("atomic_fact_subject_keys") or [])])
                current.predicates = _dedupe_preserve_order([*current.predicates, *list(metadata.get("atomic_fact_predicates") or [])])
                current.claim_ids = _dedupe_preserve_order([*current.claim_ids, *problem.affected_claim_ids])
                current.truth_ids = _dedupe_preserve_order([*current.truth_ids, *problem.affected_truth_ids])
        return sorted(facts_by_key.values(), key=lambda fact: (fact.fact_key, fact.atomic_fact_id))

    def _apply_atomic_fact_history(
        self,
        *,
        run: AuditRun,
        atomic_facts: list[AtomicFactEntry],
        packages: list[DecisionPackage],
    ) -> tuple[list[AtomicFactEntry], list[DecisionPackage], list[str]]:
        previous_by_key = self._latest_previous_atomic_facts_by_key(run=run)
        carried_forward = 0
        reopened = 0
        updated_facts: list[AtomicFactEntry] = []
        next_packages = list(packages)
        for fact in atomic_facts:
            metadata = dict(fact.metadata or {})
            metadata["first_seen_run_id"] = run.run_id
            metadata["last_seen_run_id"] = run.run_id
            metadata["seen_run_ids"] = [run.run_id]
            metadata["occurrence_count"] = 1
            next_status = fact.status
            previous_entry = previous_by_key.get(fact.fact_key)
            if previous_entry is not None:
                previous_run_id, previous_fact = previous_entry
                previous_metadata = dict(previous_fact.metadata or {})
                previous_seen_run_ids = [
                    str(item).strip()
                    for item in list(previous_metadata.get("seen_run_ids") or [])
                    if str(item).strip()
                ]
                seen_run_ids = _dedupe_preserve_order([*previous_seen_run_ids, run.run_id])
                metadata["first_seen_run_id"] = (
                    str(previous_metadata.get("first_seen_run_id") or previous_run_id).strip()
                    or previous_run_id
                )
                metadata["previous_atomic_fact_id"] = previous_fact.atomic_fact_id
                metadata["previous_run_id"] = previous_run_id
                metadata["last_seen_run_id"] = run.run_id
                metadata["seen_run_ids"] = seen_run_ids
                metadata["occurrence_count"] = max(
                    len(seen_run_ids),
                    int(previous_metadata.get("occurrence_count") or 1) + 1,
                )
                if previous_fact.status in {"open", "confirmed"}:
                    next_status = previous_fact.status
                    metadata["carry_over_mode"] = "continued"
                    metadata["carry_over_status"] = previous_fact.status
                    metadata["last_status_changed_at"] = previous_metadata.get("last_status_changed_at")
                    metadata["last_status_comment"] = previous_metadata.get("last_status_comment")
                    metadata["last_status_source"] = previous_metadata.get("last_status_source")
                    carried_forward += 1
                else:
                    next_status = "open"
                    metadata["carry_over_mode"] = "reopened"
                    metadata["reopened_from_status"] = previous_fact.status
                    metadata["reopened_from_atomic_fact_id"] = previous_fact.atomic_fact_id
                    metadata["reopened_from_run_id"] = previous_run_id
                    reopened += 1
            updated_fact = fact.model_copy(update={"status": next_status, "metadata": metadata})
            updated_facts.append(updated_fact)
            next_packages = self._sync_atomic_fact_status_into_packages(
                packages=next_packages,
                target_fact=updated_fact,
            )
        notes: list[str] = []
        if carried_forward:
            notes.append(
                f"{carried_forward} atomare Fakten wurden aus dem letzten passenden Audit-Lauf mit ihrem offenen Bewertungsstand uebernommen."
            )
        if reopened:
            notes.append(
                f"{reopened} zuvor erledigte oder ersetzte atomare Fakten sind erneut aufgetreten und wurden wieder auf 'open' gesetzt."
            )
        return updated_facts, next_packages, notes

    def _latest_previous_atomic_facts_by_key(
        self,
        *,
        run: AuditRun,
    ) -> dict[str, tuple[str, AtomicFactEntry]]:
        target_signature = self._target_signature(run.target)
        previous_by_key: dict[str, tuple[str, AtomicFactEntry]] = {}
        for candidate in self._repository.list_runs():
            if candidate.run_id == run.run_id or candidate.status != "completed":
                continue
            if self._target_signature(candidate.target) != target_signature:
                continue
            for fact in candidate.atomic_facts:
                if fact.fact_key not in previous_by_key:
                    previous_by_key[fact.fact_key] = (candidate.run_id, fact)
        return previous_by_key

    @staticmethod
    def _target_signature(target: AuditTarget) -> str:
        return json.dumps(
            {
                "github_repo_url": str(target.github_repo_url or "").strip(),
                "local_repo_path": str(target.local_repo_path or "").strip(),
                "github_ref": str(target.github_ref or "").strip(),
                "confluence_space_keys": sorted(
                    str(item).strip()
                    for item in list(target.confluence_space_keys or [])
                    if str(item).strip()
                ),
                "confluence_page_ids": sorted(
                    str(item).strip()
                    for item in list(target.confluence_page_ids or [])
                    if str(item).strip()
                ),
                "jira_project_keys": sorted(
                    str(item).strip()
                    for item in list(target.jira_project_keys or [])
                    if str(item).strip()
                ),
                "include_metamodel": bool(target.include_metamodel),
                "include_local_docs": bool(target.include_local_docs),
            },
            sort_keys=True,
        )

    @staticmethod
    def _sync_atomic_facts_from_package_decision(
        *,
        atomic_facts: list[AtomicFactEntry],
        package: DecisionPackage,
        action: str,
        comment_text: str | None,
    ) -> list[AtomicFactEntry]:
        next_status = {
            "accept": "confirmed",
            "reject": "superseded",
            "specify": "confirmed",
        }.get(str(action or "").strip())
        if next_status is None:
            return list(atomic_facts)
        package_fact_keys = {
            str(item.get("fact_key") or "").strip()
            for item in package.metadata.get("atomic_facts", [])
            if isinstance(item, dict) and str(item.get("fact_key") or "").strip()
        }
        if not package_fact_keys:
            return list(atomic_facts)
        updated: list[AtomicFactEntry] = []
        for fact in atomic_facts:
            if fact.fact_key not in package_fact_keys:
                updated.append(fact)
                continue
            metadata = dict(fact.metadata or {})
            metadata["last_status_changed_at"] = utc_now_iso()
            metadata["last_status_source"] = "package_decision"
            metadata["last_status_comment"] = str(comment_text or "").strip() or None
            updated.append(fact.model_copy(update={"status": next_status, "metadata": metadata}))
        return updated

    @staticmethod
    def _sync_atomic_fact_status_into_packages(
        *,
        packages: list[DecisionPackage],
        target_fact: AtomicFactEntry,
    ) -> list[DecisionPackage]:
        updated_packages: list[DecisionPackage] = []
        for package in packages:
            package_changed = False
            next_problems: list[DecisionProblemElement] = []
            for problem in package.problem_elements:
                if str(problem.metadata.get("atomic_fact_key") or "").strip() != target_fact.fact_key:
                    next_problems.append(problem)
                    continue
                metadata = dict(problem.metadata or {})
                metadata["atomic_fact_status"] = target_fact.status
                metadata["atomic_fact_status_comment"] = target_fact.metadata.get("last_status_comment")
                next_problems.append(problem.model_copy(update={"metadata": metadata}))
                package_changed = True
            next_metadata = dict(package.metadata or {})
            atomic_facts_payload: list[dict[str, object]] = []
            for item in next_metadata.get("atomic_facts", []):
                if not isinstance(item, dict):
                    continue
                entry = dict(item)
                if str(entry.get("fact_key") or "").strip() == target_fact.fact_key:
                    entry["status"] = target_fact.status
                    entry["comment_text"] = target_fact.metadata.get("last_status_comment")
                    package_changed = True
                atomic_facts_payload.append(entry)
            next_metadata["atomic_facts"] = atomic_facts_payload
            updated_packages.append(
                package.model_copy(
                    update={
                        "problem_elements": next_problems if package_changed else package.problem_elements,
                        "metadata": next_metadata if package_changed else package.metadata,
                    }
                )
            )
        return updated_packages

    @staticmethod
    def _append_implemented_change(
        *,
        implemented_changes: list[AuditImplementedChange],
        change: AuditImplementedChange,
        max_entries: int = 100,
    ) -> list[AuditImplementedChange]:
        next_entries = [*implemented_changes, change]
        return next_entries if len(next_entries) <= max_entries else next_entries[-max_entries:]

    @staticmethod
    def _append_log(
        *,
        analysis_log: list[AuditAnalysisLogEntry],
        entry: AuditAnalysisLogEntry,
        max_entries: int = 200,
    ) -> list[AuditAnalysisLogEntry]:
        next_entries = [*analysis_log, entry]
        return next_entries if len(next_entries) <= max_entries else next_entries[-max_entries:]

    @staticmethod
    def _lease_expiry_iso(*, now_iso: str, lease_seconds: int = 180) -> str:
        now = datetime.fromisoformat(now_iso)
        return (now + timedelta(seconds=max(30, int(lease_seconds)))).astimezone(UTC).isoformat()

    def _append_comment_analysis_logs(
        self,
        *,
        analysis_log: list[AuditAnalysisLogEntry],
        comment_text: str,
        related_finding_ids: list[str],
        analysis: DecisionCommentAnalysis,
    ) -> list[AuditAnalysisLogEntry]:
        next_log = self._append_log(
            analysis_log=analysis_log,
            entry=AuditAnalysisLogEntry(
                source_type="decision_comment",
                title="User-Kommentar analysiert",
                message=comment_text.strip(),
                related_finding_ids=related_finding_ids,
                related_scope_keys=list(analysis.related_scope_keys),
                derived_changes=[
                    (
                        f"Kommentar wurde in {len(analysis.normalized_truths)} lokale Wahrheiten "
                        f"und {len(analysis.related_scope_keys)} Scope-Cluster ueberfuehrt."
                    )
                ],
                impact_summary=[
                    "Die nachfolgenden Log-Eintraege zeigen die gespeicherten Wahrheiten und die geplante Neugewichtung."
                ],
                metadata={"phase": "decision_comment_ingest"},
            ),
        )
        next_log = self._append_log(
            analysis_log=next_log,
            entry=AuditAnalysisLogEntry(
                source_type="truth_update",
                title="Lokale Wahrheiten aktualisiert",
                message="Die aus dem Kommentar abgeleiteten Wahrheiten wurden im lokalen Auditor-Kontext vorgemerkt.",
                related_finding_ids=related_finding_ids,
                related_scope_keys=list(analysis.related_scope_keys),
                derived_changes=list(analysis.normalized_truths),
                impact_summary=[
                    "Diese Wahrheiten werden bei der naechsten Generierung von Problemelementen und Empfehlungen einbezogen."
                ],
                metadata={"phase": "truth_update"},
            ),
        )
        return self._append_log(
            analysis_log=next_log,
            entry=AuditAnalysisLogEntry(
                source_type="recommendation_regeneration",
                title="Neubewertung vorbereitet",
                message="Betroffene Entscheidungs- und Empfehlungspakete wurden fuer eine neue Gewichtung markiert.",
                related_finding_ids=related_finding_ids,
                related_scope_keys=list(analysis.related_scope_keys),
                derived_changes=list(analysis.derived_changes),
                impact_summary=list(analysis.impact_summary),
                metadata={"phase": "recommendation_regeneration"},
            ),
        )

    def _append_pipeline_completion_notes(
        self,
        *,
        analysis_log: list[AuditAnalysisLogEntry],
        analysis_mode: AnalysisMode,
        findings: list[AuditFinding],
        review_cards: list[ReviewCard],
        claims: list[AuditClaimEntry],
        packages: list[DecisionPackage],
        notes: list[str],
    ) -> list[AuditAnalysisLogEntry]:
        next_log = self._append_log(
            analysis_log=analysis_log,
            entry=AuditAnalysisLogEntry(
                source_type="impact_analysis",
                title="Analyse abgeschlossen",
                message=(
                    "Der Fast-Audit-Pfad hat Review-Karten und Folgeaktionshinweise erzeugt."
                    if analysis_mode == "fast"
                    else "Die produktive Read-only-Pipeline hat Claims, Findings und Entscheidungspakete erzeugt."
                ),
                related_finding_ids=[finding.finding_id for finding in findings],
                derived_changes=[
                    (
                        f"{len(review_cards)} Review-Karten wurden aus priorisierten Vergleichskandidaten abgeleitet."
                        if analysis_mode == "fast"
                        else f"{len(claims)} Claims wurden aus den gelesenen Quellen extrahiert."
                    ),
                    (
                        f"{len(findings)} kompatible Findings wurden fuer Folgeaktionen vorbereitet."
                        if analysis_mode == "fast"
                        else f"{len(findings)} Findings wurden aus den Claims abgeleitet."
                    ),
                    (
                        "Entscheidungspakete bleiben im Fast-Audit-Modus bewusst leer."
                        if analysis_mode == "fast"
                        else f"{len(packages)} Entscheidungspakete wurden neu aufgebaut."
                    ),
                ],
                impact_summary=[
                    (
                        "Nachfolgende User-Entscheidungen arbeiten direkt auf Review-Karten mit Collector-Evidenz."
                        if analysis_mode == "fast"
                        else "Nachfolgende User-Entscheidungen arbeiten gegen echte Collector-Evidenz statt gegen Demo-Daten."
                    )
                ],
                metadata={"phase": "pipeline_complete"},
            ),
        )
        for note in _prioritize_pipeline_notes(notes=notes)[:12]:
            next_log = self._append_log(
                analysis_log=next_log,
                entry=AuditAnalysisLogEntry(
                    source_type="impact_analysis",
                    title="Pipeline-Notiz",
                    message=note,
                    impact_summary=["Die Notiz beeinflusst den naechsten Delta- und Review-Schritt."],
                    metadata={"phase": "pipeline_note"},
                ),
            )
        return next_log

    def _build_pipeline_log_entry(
        self,
        *,
        step_key: str,
        current_activity: str,
        detail: str | None,
    ) -> AuditAnalysisLogEntry:
        blueprint = STEP_LOG_BLUEPRINTS.get(step_key, {})
        return AuditAnalysisLogEntry(
            source_type="pipeline",
            title=str(blueprint.get("title") or current_activity),
            message=detail or current_activity,
            derived_changes=list(blueprint.get("derived_changes") or []),
            impact_summary=list(blueprint.get("impact_summary") or []),
            metadata={"step_key": step_key},
        )

    def _derive_decision_comment_analysis(
        self,
        *,
        comment_text: str,
        related_finding_ids: list[str],
    ) -> DecisionCommentAnalysis:
        normalized_comment = " ".join(comment_text.split())
        lowered_comment = normalized_comment.casefold()
        raw_fragments = [
            fragment.strip(" -")
            for fragment in re.split(r"[.!?;\n]+", normalized_comment)
            if fragment.strip(" -")
        ]
        truth_fragments = raw_fragments[:4] or [normalized_comment]
        normalized_truths = [f"User-Wahrheit: {fragment}" for fragment in truth_fragments]

        related_scope_keys = sorted(
            {
                scope_key
                for tokens, scope_key in DECISION_SCOPE_HINTS
                if any(token in lowered_comment for token in tokens)
            }
        )
        if not related_scope_keys:
            related_scope_keys = ["General.decision_context"]

        derived_changes = [
            f"Scope-Cluster {scope_key} wird im naechsten Delta-Lauf neu bewertet."
            for scope_key in related_scope_keys[:4]
        ]
        if any(token in lowered_comment for token in ("nur", "nicht", "kein")):
            derived_changes.append(
                "Empfehlungen mit entgegenstehender Annahme werden fuer eine harte Neugewichtung markiert."
            )
        if any(
            token in lowered_comment
            for token in ("write", "schreib", "geschrieb", "persist", "read", "les")
        ):
            derived_changes.append(
                "Read-/Write-Vertraege der betroffenen Objekte werden gegen Code, Doku und Metamodell erneut gespiegelt."
            )
        if related_finding_ids:
            derived_changes.append("Verknuepfte Problemelemente werden mit dem neuen Kommentar-Kontext neu gerankt.")

        impact_summary = [
            (
                "Neue Entscheidungspakete muessen die lokal gespeicherte Wahrheit gegen "
                "bestehende Evidenz und bisherige Empfehlungen abgleichen."
            )
        ]
        if any(token in lowered_comment for token in ("confluence", "wiki", "doku", "dokumentation", "ssot")):
            impact_summary.append(
                "Dokumentbasierte Claims werden bei der naechsten Regenerierung gegen die neue Aussage priorisiert geprueft."
            )
        if any(token in lowered_comment for token in ("metamodell", "metamodel", "bsm", "prozess", "process")):
            impact_summary.append(
                "Metamodell- und Prozessclaims werden auf moegliche Folgekonflikte mit der neuen Wahrheit abgeklopft."
            )
        if any(token in lowered_comment for token in ("jira", "ticket")):
            impact_summary.append(
                "Spaetere Jira-Tickets zur Codeaenderung muessen die neue Annahme im Acceptance-Scope und im Prompt-Kontext reflektieren."
            )

        return DecisionCommentAnalysis(
            normalized_truths=normalized_truths,
            derived_changes=derived_changes,
            impact_summary=impact_summary,
            related_scope_keys=related_scope_keys,
        )

    def _merge_truths_from_specification(
        self,
        *,
        truths: list[TruthLedgerEntry],
        package: DecisionPackage,
        analysis: DecisionCommentAnalysis,
    ) -> tuple[list[TruthLedgerEntry], list[TruthLedgerEntry]]:
        updated_truths = [truth.model_copy(deep=True) for truth in truths]
        created_truths: list[TruthLedgerEntry] = []
        problem_id = package.problem_elements[0].problem_id if package.problem_elements else None
        for index, normalized_truth in enumerate(analysis.normalized_truths):
            scope_key = analysis.related_scope_keys[min(index, len(analysis.related_scope_keys) - 1)]
            subject_key = scope_key if "." in scope_key else _canonical_package_scope_key(
                package=package,
                fallback_scope_key=scope_key,
            )
            subject_kind = "object_property" if "." in subject_key else "scope"
            predicate = "user_specification"
            canonical_key = f"{subject_key}|{predicate}"
            superseded_truth_id: str | None = None
            truth_delta_status = "added"
            next_truths: list[TruthLedgerEntry] = []
            for existing in updated_truths:
                if existing.canonical_key == canonical_key and existing.truth_status == "active":
                    superseded_truth_id = existing.truth_id
                    truth_delta_status = "changed"
                    next_truths.append(existing.model_copy(update={"truth_status": "superseded"}))
                else:
                    next_truths.append(existing)
            updated_truths = next_truths
            created = TruthLedgerEntry(
                canonical_key=canonical_key,
                subject_kind=subject_kind,
                subject_key=subject_key,
                predicate=predicate,
                normalized_value=normalized_truth,
                scope_kind="project",
                scope_key="FINAI",
                source_kind="user_specification",
                created_from_problem_id=problem_id,
                supersedes_truth_id=superseded_truth_id,
                metadata={
                    "package_id": package.package_id,
                    "scope_key": scope_key,
                    "truth_delta_status": truth_delta_status,
                    "pending_delta_recalculation": True,
                },
            )
            updated_truths.append(created)
            created_truths.append(created)
        return updated_truths, created_truths

    @staticmethod
    def _find_impacted_package_ids(*, run: AuditRun, package: DecisionPackage) -> list[str]:
        current_claim_ids = {claim_id for problem in package.problem_elements for claim_id in problem.affected_claim_ids}
        current_truth_ids = {truth_id for problem in package.problem_elements for truth_id in problem.affected_truth_ids}
        current_anchor_values = set(_string_list_from_metadata(package.metadata.get("retrieval_anchor_values")))
        current_cluster_key = _canonical_package_scope_key(package=package)
        current_group_key = str(package.metadata.get("group_key") or package.metadata.get("causal_group_key") or "").strip()
        current_scope_keys = {
            str(scope_key).strip()
            for scope_key in package.metadata.get("scope_keys", [])
            if str(scope_key).strip()
        }
        impacted: list[str] = []
        for candidate in run.decision_packages:
            candidate_claim_ids = {
                claim_id for problem in candidate.problem_elements for claim_id in problem.affected_claim_ids
            }
            candidate_truth_ids = {
                truth_id for problem in candidate.problem_elements for truth_id in problem.affected_truth_ids
            }
            candidate_anchor_values = set(_string_list_from_metadata(candidate.metadata.get("retrieval_anchor_values")))
            candidate_cluster_key = _canonical_package_scope_key(package=candidate)
            candidate_group_key = str(candidate.metadata.get("group_key") or candidate.metadata.get("causal_group_key") or "").strip()
            candidate_scope_keys = {
                str(scope_key).strip()
                for scope_key in candidate.metadata.get("scope_keys", [])
                if str(scope_key).strip()
            }
            if (
                candidate.package_id == package.package_id
                or current_claim_ids.intersection(candidate_claim_ids)
                or current_truth_ids.intersection(candidate_truth_ids)
                or (current_anchor_values and current_anchor_values.intersection(candidate_anchor_values))
                or (current_group_key and current_group_key == candidate_group_key)
                or (current_scope_keys and candidate_scope_keys and current_scope_keys.intersection(candidate_scope_keys))
                or candidate_cluster_key == current_cluster_key
            ):
                impacted.append(candidate.package_id)
        return impacted

    @staticmethod
    def _apply_package_state(
        *,
        packages: list[DecisionPackage],
        package_id: str,
        action: str,
        impacted_package_ids: list[str],
    ) -> list[DecisionPackage]:
        state_map = {
            "accept": "accepted",
            "reject": "rejected",
            "specify": "specified",
        }
        next_state = state_map.get(action)
        if next_state is None:
            raise ValueError(f"Unbekannte Paket-Aktion: {action}")
        updated_packages: list[DecisionPackage] = []
        impacted_set = set(impacted_package_ids)
        for package in packages:
            if package.package_id == package_id:
                updated_packages.append(
                    package.model_copy(
                        update={
                            "decision_state": next_state,
                            "rerender_required_after_decision": action == "specify",
                        }
                    )
                )
                continue
            if action == "specify" and package.package_id in impacted_set:
                updated_packages.append(package.model_copy(update={"rerender_required_after_decision": True}))
                continue
            updated_packages.append(package)
        return updated_packages

    @staticmethod
    def _mark_approval_request_executed(
        *,
        requests: list[WritebackApprovalRequest],
        approval_request_id: str,
        implemented_change_id: str,
    ) -> list[WritebackApprovalRequest]:
        now = utc_now_iso()
        updated: list[WritebackApprovalRequest] = []
        for request in requests:
            if request.approval_request_id != approval_request_id:
                updated.append(request)
                continue
            updated.append(
                request.model_copy(
                    update={
                        "status": "executed",
                        "decided_at": now,
                        "metadata": {
                            **request.metadata,
                            "implemented_change_id": implemented_change_id,
                        },
                    }
                )
            )
        return updated

    @staticmethod
    def _select_related_findings(
        *,
        run: AuditRun,
        related_finding_ids: list[str] | None,
    ) -> list[AuditFinding]:
        if related_finding_ids:
            selected_ids = set(related_finding_ids)
            selected = [finding for finding in run.findings if finding.finding_id in selected_ids]
            if selected:
                return selected
        return list(run.findings[:3])

    def _select_related_findings_for_approval(
        self,
        *,
        run: AuditRun,
        related_review_card_ids: list[str],
        related_finding_ids: list[str],
        related_package_ids: list[str],
    ) -> list[AuditFinding]:
        if related_review_card_ids:
            selected_review_cards = [
                card for card in run.review_cards if card.card_id in set(related_review_card_ids)
            ]
            if not selected_review_cards:
                raise ValueError("Keine der angeforderten Review-Karten wurde gefunden.")
            non_accepted = [card.title for card in selected_review_cards if card.decision_state != "accepted"]
            if non_accepted:
                raise ValueError(
                    "Folgeaktionen sind nur fuer akzeptierte Review-Karten erlaubt: " + ", ".join(non_accepted[:3])
                )
            selected_ids = {
                finding_id
                for card in selected_review_cards
                for finding_id in card.related_finding_ids
            }
            if selected_ids:
                return self._select_related_findings(run=run, related_finding_ids=list(selected_ids))
        if related_finding_ids:
            selected = self._select_related_findings(run=run, related_finding_ids=related_finding_ids)
            if selected:
                return selected
        if related_package_ids:
            selected_ids = {
                finding_id
                for package in run.decision_packages
                if package.package_id in set(related_package_ids)
                for finding_id in package.related_finding_ids
            }
            if selected_ids:
                return self._select_related_findings(run=run, related_finding_ids=list(selected_ids))
        return list(run.findings[:3])

    def _resolve_jira_brief_for_execution(
        self,
        *,
        run: AuditRun,
        approval: WritebackApprovalRequest,
        findings: list[AuditFinding],
        ticket_key: str,
        ticket_url: str,
    ) -> JiraTicketAICodingBrief:
        stored_brief = approval.metadata.get("jira_ticket_brief")
        if isinstance(stored_brief, dict):
            return build_jira_ticket_brief(
                run=run,
                findings=findings,
                ticket_key=ticket_key,
                ticket_url=ticket_url,
            ).model_copy(
                update={
                    **stored_brief,
                    "ticket_key": ticket_key,
                    "ticket_url": ticket_url,
                }
            )
        return build_jira_ticket_brief(
            run=run,
            ticket_key=ticket_key,
            ticket_url=ticket_url,
            findings=findings,
        )

    def _resolve_confluence_patch_preview_for_execution(
        self,
        *,
        run: AuditRun,
        approval: WritebackApprovalRequest,
        findings: list[AuditFinding],
    ) -> ConfluencePatchPreview:
        stored_preview = approval.metadata.get("confluence_patch_preview")
        if isinstance(stored_preview, dict):
            return ConfluencePatchPreview.model_validate(stored_preview)
        return build_confluence_patch_preview(
            run=run,
            findings=findings,
            fallback_page_url=approval.target_url or self._settings.confluence_home_url,
            fallback_page_title=approval.title.replace("Confluence-Writeback fuer ", "").strip()
            or "FIN-AI Spezifikation",
        )

    @staticmethod
    def _build_jira_payload_preview(
        *,
        existing_preview: list[str],
        brief: JiraTicketAICodingBrief,
    ) -> list[str]:
        preview = list(existing_preview)
        preview.extend(
            [
                f"Titel: {brief.title}",
                f"Grund: {brief.reason}",
                *[f"Abnahme: {item}" for item in brief.acceptance_criteria[:3]],
                *[f"Teil: {item}" for item in brief.affected_parts[:3]],
                *[f"Fakt: {item}" for item in brief.evidence[:2] if item.startswith("Atomarer Fakt:")],
                "Vollstaendiger AI-Coding-Prompt und Jira-ADF-Payload sind im lokalen Approval-Ledger gespeichert.",
            ]
        )
        return _dedupe_preserve_order(preview)

    @staticmethod
    def _build_confluence_payload_preview(
        *,
        existing_preview: list[str],
        patch_preview: ConfluencePatchPreview,
    ) -> list[str]:
        return build_confluence_payload_preview(
            existing_preview=existing_preview,
            patch_preview=patch_preview,
        )

    @staticmethod
    def _build_steps(*, include_local_docs: bool, analysis_mode: AnalysisMode) -> list[AuditProgressStep]:
        steps: list[AuditProgressStep] = []
        step_blueprint = FAST_AUDIT_PIPELINE_STEPS if analysis_mode == "fast" else AUDIT_PIPELINE_STEPS
        for step_key, label in step_blueprint:
            if step_key == "local_docs_check" and not include_local_docs:
                steps.append(
                    AuditProgressStep(
                        step_key=step_key,
                        label=label,
                        status="skipped",
                        detail="Lokale Doku wurde fuer diesen Lauf nicht angefordert.",
                    )
                )
                continue
            steps.append(AuditProgressStep(step_key=step_key, label=label))
        return steps

    def _build_initial_progress(self, *, target: AuditTarget, analysis_mode: AnalysisMode) -> AuditRunProgress:
        return AuditRunProgress(
            progress_pct=0,
            phase_key="queued",
            phase_label="Wartet auf Worker",
            current_activity=(
                "Run wurde angelegt und wartet auf den schnellen Vergleichspfad."
                if analysis_mode == "fast"
                else "Run wurde angelegt und wartet auf die Analyse-Pipeline."
            ),
            steps=self._build_steps(include_local_docs=bool(target.include_local_docs), analysis_mode=analysis_mode),
        )

    @staticmethod
    def _mark_pipeline_started(*, progress: AuditRunProgress) -> AuditRunProgress:
        return progress.model_copy(
            update={
                "progress_pct": max(int(progress.progress_pct), 2),
                "phase_key": "starting",
                "phase_label": "Initialisierung",
                "current_activity": "Worker uebernimmt den Run und startet die Analyse-Pipeline.",
                "steps": progress.steps or [],
            }
        )

    def _progress_for_step(
        self,
        *,
        progress: AuditRunProgress,
        target: AuditTarget,
        analysis_mode: AnalysisMode,
        step_key: str,
        progress_pct: int,
        current_activity: str,
        step_status: str,
        detail: str | None,
    ) -> AuditRunProgress:
        now = utc_now_iso()
        effective_steps = progress.steps or self._build_steps(
            include_local_docs=bool(target.include_local_docs),
            analysis_mode=analysis_mode,
        )
        current_index = next(
            (index for index, step in enumerate(effective_steps) if step.step_key == step_key),
            None,
        )
        if current_index is None:
            raise ValueError(f"Unbekannter Progress-Step: {step_key}")

        next_steps: list[AuditProgressStep] = []
        for index, step in enumerate(effective_steps):
            if step.status == "skipped":
                next_steps.append(step)
                continue
            if index < current_index and step.status == "pending":
                next_steps.append(
                    step.model_copy(
                        update={
                            "status": "completed",
                            "finished_at": now,
                            "detail": step.detail or "Analysephase abgeschlossen.",
                        }
                    )
                )
                continue
            if index == current_index:
                next_steps.append(
                    self._update_current_step(step=step, status=step_status, detail=detail, timestamp=now)
                )
                continue
            if index > current_index and step.status == "running":
                next_steps.append(step.model_copy(update={"status": "pending", "started_at": None}))
                continue
            next_steps.append(step)

        return AuditRunProgress(
            progress_pct=max(0, min(100, int(progress_pct))),
            phase_key=step_key,
            phase_label=next_steps[current_index].label,
            current_activity=current_activity,
            steps=next_steps,
        )

    @staticmethod
    def _update_current_step(
        *,
        step: AuditProgressStep,
        status: str,
        detail: str | None,
        timestamp: str,
    ) -> AuditProgressStep:
        update: dict[str, object] = {"status": status, "detail": detail or step.detail}
        if status == "running":
            update["started_at"] = step.started_at or timestamp
            update["finished_at"] = None
        elif status in {"completed", "failed"}:
            update["started_at"] = step.started_at or timestamp
            update["finished_at"] = timestamp
        return step.model_copy(update=update)

    @staticmethod
    def _build_completed_progress(*, progress: AuditRunProgress) -> AuditRunProgress:
        now = utc_now_iso()
        completed_steps = [
            step
            if step.status == "skipped"
            else step.model_copy(
                update={
                    "status": "completed",
                    "started_at": step.started_at or now,
                    "finished_at": step.finished_at or now,
                    "detail": step.detail or "Analysephase abgeschlossen.",
                }
            )
            for step in progress.steps
        ]
        return AuditRunProgress(
            progress_pct=100,
            phase_key="completed",
            phase_label="Abgeschlossen",
            current_activity="Analyse abgeschlossen. Findings, Evidenzen und Empfehlungen sind bereit.",
            steps=completed_steps,
        )

    @staticmethod
    def _build_failed_progress(*, progress: AuditRunProgress, error: str) -> AuditRunProgress:
        now = utc_now_iso()
        phase_key = progress.phase_key or "failed"
        failed_steps: list[AuditProgressStep] = []
        current_failed = False
        for step in progress.steps:
            if step.step_key == phase_key and step.status not in {"completed", "skipped"}:
                failed_steps.append(
                    step.model_copy(
                        update={
                            "status": "failed",
                            "started_at": step.started_at or now,
                            "finished_at": now,
                            "detail": error,
                        }
                    )
                )
                current_failed = True
            else:
                failed_steps.append(step)
        if not current_failed and failed_steps:
            last_step = failed_steps[-1]
            failed_steps[-1] = last_step.model_copy(
                update={
                    "status": "failed",
                    "started_at": last_step.started_at or now,
                    "finished_at": now,
                    "detail": error,
                }
            )
        return AuditRunProgress(
            progress_pct=max(0, min(100, int(progress.progress_pct))),
            phase_key="failed",
            phase_label="Fehlgeschlagen",
            current_activity=error,
            steps=failed_steps,
        )

    @staticmethod
    def _build_demo_links(*, findings: list[AuditFinding]) -> list[AuditFindingLink]:
        if len(findings) < 2:
            return []
        return [
            AuditFindingLink(
                from_finding_id=findings[0].finding_id,
                to_finding_id=findings[1].finding_id,
                relation_type="gap_hint",
                rationale=(
                    "Die fehlende Lifecycle-Definition erhoeht die Wahrscheinlichkeit, dass der "
                    "dokumentierte und implementierte Write-Pfad auseinanderlaufen."
                ),
                confidence=0.78,
                metadata={"origin": "demo", "resolution_strategy": "review_required"},
            )
        ]

    def _build_demo_snapshots(self, *, run: AuditRun, snapshot_ids: dict[str, str]) -> list[AuditSourceSnapshot]:
        repo_hint = run.target.local_repo_path or run.target.github_repo_url or "UNKNOWN"
        return [
            AuditSourceSnapshot(
                snapshot_id=snapshot_ids["repo"],
                source_type="github_file",
                source_id="demo-code-1",
                revision_id=run.target.github_ref,
                content_hash="sha256:demo-repo-snapshot",
                sync_token=run.target.github_ref,
                metadata={"repo_path": repo_hint, "kind": "local_repo_or_remote"},
            ),
            AuditSourceSnapshot(
                snapshot_id=snapshot_ids["confluence"],
                source_type="confluence_page",
                source_id="demo-page-1",
                revision_id="v1",
                content_hash="sha256:demo-confluence-page",
                sync_token="confluence:demo-page-1:v1",
                metadata={
                    "space_key": run.target.confluence_space_keys[0] if run.target.confluence_space_keys else None,
                    "confluence_url": self._settings.confluence_home_url,
                },
            ),
            AuditSourceSnapshot(
                snapshot_id=snapshot_ids["local_doc"],
                source_type="local_doc",
                source_id="demo-doc-1",
                revision_id="working-tree",
                content_hash="sha256:demo-local-doc-version",
                sync_token="local-doc:docs/roadmap.md:working-tree",
                metadata={"path_hint": "docs/roadmap.md"},
            ),
            AuditSourceSnapshot(
                snapshot_id=snapshot_ids["metamodel"],
                source_type="metamodel",
                source_id="finai-current-dump",
                revision_id="latest",
                content_hash="sha256:demo-metamodel-current-dump",
                sync_token="metamodel:current_dump",
                metadata={
                    "dump_path": str(self._settings.metamodel_dump_path),
                    "policy": "always_refresh_before_run",
                },
            ),
        ]


def _string_list_from_metadata(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        normalized = str(item or "").strip()
        if normalized:
            result.append(normalized)
    return result


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _prioritize_pipeline_notes(*, notes: list[str]) -> list[str]:
    priority_prefixes = (
        "Delta-Abgleich:",
        "Claim-Delta:",
        "Neubewertung fokussiert auf",
    )
    prioritized: list[str] = []
    deferred: list[str] = []
    for note in notes:
        normalized = str(note or "").strip()
        if not normalized:
            continue
        if any(normalized.startswith(prefix) for prefix in priority_prefixes):
            prioritized.append(normalized)
        else:
            deferred.append(normalized)
    return _dedupe_preserve_order([*prioritized, *deferred])


def _package_cluster_key(*, finding: AuditFinding) -> str:
    causal_group_key = str(finding.metadata.get("causal_group_key") or "").strip()
    if causal_group_key:
        return causal_group_key
    raw_key = str(finding.metadata.get("object_key") or finding.canonical_key or finding.title).strip()
    if not raw_key:
        return finding.title
    return package_scope_key(raw_key)


def _canonical_package_scope_key(
    *,
    package: DecisionPackage,
    fallback_scope_key: str | None = None,
) -> str:
    primary_scope_key = str(package.metadata.get("primary_scope_key") or "").strip()
    if primary_scope_key:
        return primary_scope_key
    scope_keys = [
        str(scope_key).strip()
        for scope_key in package.metadata.get("scope_keys", [])
        if str(scope_key).strip()
    ]
    if scope_keys:
        return scope_keys[0]
    cluster_key = str(package.metadata.get("cluster_key") or "").strip()
    if cluster_key and ":" not in cluster_key and " · " not in cluster_key:
        return cluster_key
    normalized_fallback = str(fallback_scope_key or "").strip()
    if normalized_fallback:
        return normalized_fallback
    return "General.decision_context"


def _package_group_label(*, group_key: str, findings: list[AuditFinding]) -> str:
    for finding in findings:
        label = str(finding.metadata.get("causal_group_label") or "").strip()
        if label:
            return label
    if ":" in group_key:
        return group_key.split(":", 1)[1]
    return group_key


def _package_scope_keys_for_findings(*, findings: list[AuditFinding]) -> set[str]:
    scope_keys: set[str] = set()
    for finding in findings:
        causal_scope_keys = _string_list_from_metadata(finding.metadata.get("causal_scope_keys"))
        for scope_key in causal_scope_keys:
            normalized = package_scope_key(scope_key)
            if normalized:
                scope_keys.add(normalized)
        object_key = str(finding.metadata.get("object_key") or finding.canonical_key or "").strip()
        if object_key and not causal_scope_keys:
            scope_keys.add(package_scope_key(object_key))
    return {scope_key for scope_key in scope_keys if str(scope_key or "").strip()}


def _primary_scope_key_for_findings(
    *,
    findings: list[AuditFinding],
    fallback_scope_keys: set[str],
) -> str:
    for finding in findings:
        primary_scope_key = str(finding.metadata.get("causal_primary_scope_key") or "").strip()
        if primary_scope_key:
            return package_scope_key(primary_scope_key)
        object_key = str(finding.metadata.get("object_key") or finding.canonical_key or "").strip()
        if object_key:
            return package_scope_key(object_key)
    ordered_fallbacks = sorted(
        {package_scope_key(scope_key) for scope_key in fallback_scope_keys if str(scope_key or "").strip()}
    )
    return ordered_fallbacks[0] if ordered_fallbacks else "General.decision_context"


def _claim_belongs_to_cluster(*, claim_key: str, cluster_key: str) -> bool:
    normalized_claim_key = str(claim_key or "").strip()
    normalized_cluster_key = str(cluster_key or "").strip()
    return normalized_claim_key == normalized_cluster_key or normalized_claim_key.startswith(f"{normalized_cluster_key}.")


def _claim_is_semantically_attached_to_cluster(*, claim: AuditClaimEntry, cluster_key: str) -> bool:
    if _claim_belongs_to_cluster(claim_key=claim.subject_key, cluster_key=cluster_key):
        return True
    semantic_cluster_keys = _string_list_from_metadata(claim.metadata.get("semantic_cluster_keys"))
    return cluster_key in semantic_cluster_keys


def _claim_matches_any_package_scope(*, claim: AuditClaimEntry, package_scope_keys: set[str]) -> bool:
    if not package_scope_keys:
        return False
    if any(_claim_belongs_to_cluster(claim_key=claim.subject_key, cluster_key=scope_key) for scope_key in package_scope_keys):
        return True
    semantic_cluster_keys = set(_string_list_from_metadata(claim.metadata.get("semantic_cluster_keys")))
    return bool(package_scope_keys & semantic_cluster_keys)


def _claim_matches_any_truth_scope(*, truth: TruthLedgerEntry, package_scope_keys: set[str]) -> bool:
    if not package_scope_keys:
        return False
    return any(_claim_belongs_to_cluster(claim_key=truth.subject_key, cluster_key=scope_key) for scope_key in package_scope_keys)


def _dominant_category(*, findings: list[AuditFinding]) -> str:
    category_counts: dict[str, tuple[int, int]] = {}
    for finding in findings:
        count, current_rank = category_counts.get(finding.category, (0, 99))
        category_counts[finding.category] = (count + 1, min(current_rank, severity_rank(severity=finding.severity)))
    return min(category_counts.items(), key=lambda item: (item[1][1], -item[1][0], item[0]))[0]


def _highest_severity(*, findings: list[AuditFinding]) -> str:
    return min((finding.severity for finding in findings), key=lambda severity: severity_rank(severity=severity))


def _severity_rank(severity: str) -> int:
    return severity_rank(severity=severity)


def _package_title(*, cluster_key: str, root_cause_bucket: str, findings: list[AuditFinding]) -> str:
    grouped_boundary_finding = next(
        (
            finding for finding in findings
            if bool(finding.metadata.get("grouped_boundary_paths"))
            and str(finding.metadata.get("boundary_path_type") or "").strip() == "manual_answer_entrypoint"
        ),
        None,
    )
    if grouped_boundary_finding is not None:
        path_count = int(grouped_boundary_finding.metadata.get("path_count") or 0)
        if path_count > 0:
            return f"Manuelle Antwortpfade angleichen ({path_count} Entry-Points)"
        return "Manuelle Antwortpfade angleichen"
    grouped_eventual_finding = next(
        (
            finding for finding in findings
            if bool(finding.metadata.get("grouped_eventual_paths"))
        ),
        None,
    )
    if grouped_eventual_finding is not None:
        title_prefix = str(grouped_eventual_finding.metadata.get("grouped_eventual_package_title") or "").strip()
        path_count = int(grouped_eventual_finding.metadata.get("path_count") or 0)
        if title_prefix:
            if path_count > 0:
                return f"{title_prefix} ({path_count} Pfade)"
            return title_prefix
    grouped_chain_finding = next(
        (
            finding for finding in findings
            if bool(finding.metadata.get("grouped_chain_paths"))
            and str(finding.metadata.get("chain_path_type") or "").strip() == "reaggregation_rebuild_path"
        ),
        None,
    )
    if grouped_chain_finding is not None:
        path_count = int(grouped_chain_finding.metadata.get("path_count") or 0)
        if path_count > 0:
            return f"Reaggregation atomisch schliessen ({path_count} Pfade)"
        return "Reaggregation atomisch schliessen"
    root_label = root_cause_label(bucket=root_cause_bucket)
    # Use the primary finding's actual human-readable title
    primary_title = ""
    if findings:
        primary_title = findings[0].title or ""
    if primary_title:
        if len(findings) > 1:
            return f"{primary_title} (+{len(findings) - 1} weitere)"
        return primary_title
    # Fallback to root_cause_label if no finding title available
    if len(findings) == 1:
        return f"{root_label} klaeren"
    return f"{root_label} konsolidieren"


def _package_scope_summary(
    *,
    cluster_key: str,
    root_cause_bucket: str,
    findings: list[AuditFinding],
    semantic_entities: list[SemanticEntity],
    semantic_relations: list[SemanticRelation],
) -> str:
    grouped_boundary_finding = next(
        (
            finding for finding in findings
            if bool(finding.metadata.get("grouped_boundary_paths"))
            and str(finding.metadata.get("boundary_path_type") or "").strip() == "manual_answer_entrypoint"
        ),
        None,
    )
    if grouped_boundary_finding is not None:
        function_names = [
            str(item).strip()
            for item in (grouped_boundary_finding.metadata.get("boundary_function_names") or [])
            if str(item).strip()
        ]
        path_count = int(grouped_boundary_finding.metadata.get("path_count") or len(function_names) or 0)
        function_suffix = f" · {', '.join(function_names)}" if function_names else ""
        return f"Manuelle bsmAnswer-Entry-Points · {path_count} Pfade betroffen{function_suffix}"
    grouped_eventual_finding = next(
        (
            finding for finding in findings
            if bool(finding.metadata.get("grouped_eventual_paths"))
        ),
        None,
    )
    if grouped_eventual_finding is not None:
        scope_label = str(grouped_eventual_finding.metadata.get("grouped_eventual_scope_label") or "").strip()
        function_names = [
            str(item).strip()
            for item in (grouped_eventual_finding.metadata.get("sequence_functions") or [])
            if str(item).strip()
        ]
        path_count = int(grouped_eventual_finding.metadata.get("path_count") or len(function_names) or 0)
        function_suffix = f" · {', '.join(function_names)}" if function_names else ""
        if scope_label:
            return f"{scope_label} · {path_count} Pfade betroffen{function_suffix}"
    grouped_chain_finding = next(
        (
            finding for finding in findings
            if bool(finding.metadata.get("grouped_chain_paths"))
            and str(finding.metadata.get("chain_path_type") or "").strip() == "reaggregation_rebuild_path"
        ),
        None,
    )
    if grouped_chain_finding is not None:
        function_names = [
            str(item).strip()
            for item in (grouped_chain_finding.metadata.get("sequence_functions") or [])
            if str(item).strip()
        ]
        path_count = int(grouped_chain_finding.metadata.get("path_count") or len(function_names) or 0)
        function_suffix = f" · {', '.join(function_names)}" if function_names else ""
        return f"Reaggregation-/Rebuild-Pfade · {path_count} Pfade betroffen{function_suffix}"
    categories = _dedupe_preserve_order([finding.category for finding in findings])
    root_label = root_cause_label(bucket=root_cause_bucket)
    semantic_suffix = ""
    if semantic_entities or semantic_relations:
        semantic_suffix = f" · {len(semantic_entities)} Knoten · {len(semantic_relations)} Relationen"
    if len(findings) == 1:
        return f"{cluster_key} · {root_label}{semantic_suffix}"
    return f"{cluster_key} · {root_label} · {len(findings)} Problemelemente · {', '.join(categories)}{semantic_suffix}"


def _package_recommendation_summary(
    *,
    findings: list[AuditFinding],
    root_cause_bucket: str,
    semantic_context: list[str],
    semantic_relation_summaries: list[str],
    semantic_contract_paths: list[str],
    causal_write_deciders: list[str],
    causal_write_apis: list[str],
    causal_repository_adapters: list[str],
    causal_repository_adapter_symbols: list[str],
    causal_driver_adapters: list[str],
    causal_driver_adapter_symbols: list[str],
    causal_transaction_boundaries: list[str],
    causal_retry_paths: list[str],
    causal_batch_paths: list[str],
    causal_persistence_targets: list[str],
    causal_persistence_sink_kinds: list[str],
    causal_persistence_backends: list[str],
    causal_persistence_operation_types: list[str],
    causal_persistence_schema_targets: list[str],
    causal_schema_validated_targets: list[str],
    causal_schema_observed_only_targets: list[str],
    causal_schema_unconfirmed_targets: list[str],
    causal_schema_validation_statuses: list[str],
) -> str:
    ordered_findings = order_package_findings(findings=findings, package_bucket=root_cause_bucket)
    grouped_boundary_finding = next(
        (
            finding for finding in ordered_findings
            if bool(finding.metadata.get("grouped_boundary_paths"))
            and str(finding.metadata.get("boundary_path_type") or "").strip() == "manual_answer_entrypoint"
        ),
        None,
    )
    if grouped_boundary_finding is not None:
        function_names = [
            str(item).strip()
            for item in (grouped_boundary_finding.metadata.get("boundary_function_names") or [])
            if str(item).strip()
        ]
        base = (
            "Die manuellen Antwort-Entry-Points auf einen kanonischen Pfad zusammenziehen und "
            "phase_run_id/run_id bis zur bsmAnswer-Persistenz konsistent durchreichen."
        )
        if function_names:
            return f"{base}\n\nBetroffene Funktionen: {', '.join(function_names)}"
        return base
    grouped_eventual_finding = next(
        (
            finding for finding in ordered_findings
            if bool(finding.metadata.get("grouped_eventual_paths"))
        ),
        None,
    )
    if grouped_eventual_finding is not None:
        function_names = [
            str(item).strip()
            for item in (grouped_eventual_finding.metadata.get("sequence_functions") or [])
            if str(item).strip()
        ]
        base = str(grouped_eventual_finding.recommendation or "").strip()
        if function_names:
            return f"{base}\n\nBetroffene Funktionen: {', '.join(function_names)}"
        return base
    grouped_chain_finding = next(
        (
            finding for finding in ordered_findings
            if bool(finding.metadata.get("grouped_chain_paths"))
            and str(finding.metadata.get("chain_path_type") or "").strip() == "reaggregation_rebuild_path"
        ),
        None,
    )
    if grouped_chain_finding is not None:
        function_names = [
            str(item).strip()
            for item in (grouped_chain_finding.metadata.get("sequence_functions") or [])
            if str(item).strip()
        ]
        base = (
            "Supersede-, Rebuild- und Materialisierungsschritte ueber die Reaggregationspfade in denselben "
            "transaktionalen Schutzraum ziehen oder eine Ersatzkette aufbauen, bevor die alte Kette deaktiviert wird."
        )
        if function_names:
            return f"{base}\n\nBetroffene Funktionen: {', '.join(function_names)}"
        return base
    recommendations = _dedupe_preserve_order([finding.recommendation for finding in ordered_findings if finding.recommendation])

    # Build a concise "affected sources" hint
    affected_sources: list[str] = []
    if semantic_contract_paths:
        affected_sources.append(f"Vertragskette: {semantic_contract_paths[0]}")
    if causal_persistence_schema_targets:
        affected_sources.append(f"Schema: {causal_persistence_schema_targets[0]}")

    if not recommendations:
        base = "Die betroffenen Dokumentquellen pruefen und eine konsistente Aussage festlegen."
        return f"{base} ({', '.join(affected_sources)})" if affected_sources else base

    # Use the primary finding's recommendation directly
    base = recommendations[0]
    supporting_count = max(len(ordered_findings) - 1, 0)
    if supporting_count and len(recommendations) > 1:
        base = f"{base}\n\nWeitere {supporting_count} zusammenhaengende Stelle(n) muessen danach ebenfalls angepasst werden."
    elif len(recommendations) > 1:
        base = "\n\n".join(recommendations[:3])

    if affected_sources:
        base = f"{base}\n\nBetroffene Stellen: {', '.join(affected_sources)}"
    return base


def _semantic_relations_for_entity_ids(
    *,
    semantic_relations: list[SemanticRelation],
    entity_ids: set[str],
) -> list[SemanticRelation]:
    return [
        relation
        for relation in semantic_relations
        if relation.source_entity_id in entity_ids or relation.target_entity_id in entity_ids
    ]


def _persistence_sink_kind_label(value: str) -> str:
    return {
        "node_sink": "Node-Sink",
        "relationship_sink": "Relationship-Sink",
        "history_sink": "History-Sink",
    }.get(str(value or "").strip(), str(value or "").strip())


def _semantic_relation_summary(
    *,
    relation: SemanticRelation,
    entities_by_id: dict[str, SemanticEntity],
) -> str:
    source_label = entities_by_id.get(relation.source_entity_id).label if relation.source_entity_id in entities_by_id else relation.source_entity_id
    target_label = entities_by_id.get(relation.target_entity_id).label if relation.target_entity_id in entities_by_id else relation.target_entity_id
    return f"{source_label} -> {relation.relation_type} -> {target_label}"


def _classify_writeback_exception(*, exc: Exception) -> dict[str, object]:
    if isinstance(exc, httpx.TimeoutException):
        return {
            "failure_class": "timeout",
            "status_code": None,
            "message": "Der externe Writeback ist in ein Timeout gelaufen.",
        }
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = int(exc.response.status_code)
        failure_class = {
            401: "unauthorized",
            403: "forbidden",
            429: "rate_limited",
        }.get(status_code, "http_error")
        message = {
            401: "Der externe Writeback wurde mit 401 abgewiesen. Token oder Consent-Kontext sind ungueltig.",
            403: "Der externe Writeback wurde mit 403 abgewiesen. Berechtigungen oder Zielzugriff reichen nicht aus.",
            429: "Der externe Writeback wurde rate-limitiert.",
        }.get(status_code, f"Der externe Writeback scheiterte mit HTTP {status_code}.")
        return {
            "failure_class": failure_class,
            "status_code": status_code,
            "message": message,
        }
    normalized_message = str(exc).strip()
    lowered = normalized_message.casefold()
    failure_class = "validation_error"
    if "redirect" in lowered or "callback" in lowered:
        failure_class = "oauth_redirect_mismatch"
    elif "scope" in lowered or "consent" in lowered:
        failure_class = "scope_mismatch"
    elif "freigabe" in lowered or "approval" in lowered:
        failure_class = "approval_blocked"
    elif "seite" in lowered or "page" in lowered or "ticket" in lowered or "target" in lowered:
        failure_class = "target_resolution_failed"
    return {
        "failure_class": failure_class,
        "status_code": None,
        "message": normalized_message,
    }


def _claims_for_finding_scope(*, finding: AuditFinding, claims: list[AuditClaimEntry]) -> list[AuditClaimEntry]:
    canonical_key = str(finding.canonical_key or "").strip()
    object_key = str(finding.metadata.get("object_key") or "").strip()
    target_keys = {
        key
        for key in (canonical_key, object_key)
        if key
    }
    if not target_keys:
        return list(claims)
    matching = [
        claim
        for claim in claims
        if claim.subject_key in target_keys
        or any(package_scope_key(claim.subject_key) == package_scope_key(target_key) for target_key in target_keys)
    ]
    return matching or list(claims)


def _normalized_host(url: str | None) -> str | None:
    text = str(url or "").strip()
    if not text:
        return None
    parsed = urlparse(text)
    host = str(parsed.netloc or parsed.path or "").strip().lower()
    return host or None


def _normalize_policy_key(value: object) -> str:
    if isinstance(value, (list, tuple, set)):
        first = next((item for item in value if str(item).strip()), "")
        return _normalize_policy_key(first)
    text = str(value or "").strip()
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1].strip()
        if not inner:
            return ""
        first = inner.split(",", 1)[0].strip().strip("'\"")
        return first.upper()
    return text.strip("'\"").upper()


def _atomic_fact_key(*, finding: AuditFinding) -> str:
    return (
        str(finding.canonical_key or "").strip()
        or str(finding.metadata.get("object_key") or "").strip()
        or str(finding.title or "").strip()
    )


def _atomic_fact_summary(*, finding: AuditFinding, claims: list[AuditClaimEntry]) -> str:
    source_labels = _dedupe_preserve_order(
        [
            _source_label_for_claim_type(claim.source_type)
            for claim in claims
        ]
    )
    for location in finding.locations:
        label = _source_label_for_claim_type(location.source_type)
        if label not in source_labels:
            source_labels.append(label)
    source_labels = sorted(
        source_labels,
        key=lambda label: _atomic_source_priority(label=label),
    )
    fact_key = _atomic_fact_key(finding=finding)
    comparison = " vs. ".join(source_labels[:4]) if source_labels else "mehrere Quellen"
    return f"{fact_key}: {comparison} widersprechen sich oder lassen denselben Sachverhalt unvollstaendig."


def _preferred_action_lane_for_finding(*, finding: AuditFinding) -> str:
    has_doc = any(location.source_type in {"confluence_page", "local_doc"} for location in finding.locations)
    has_code = any(location.source_type == "github_file" for location in finding.locations)
    has_model_artifact = any(
        location.source_type == "metamodel"
        or str(location.path_hint or location.source_id or "").strip().endswith((".puml", ".plantuml", ".json"))
        for location in finding.locations
    )
    if has_doc and has_code:
        return "confluence_and_jira"
    if has_model_artifact:
        return "jira_artifact"
    if has_doc:
        return "confluence_doc"
    return "jira_code"


def _source_label_for_claim_type(source_type: str) -> str:
    return {
        "confluence_page": "Confluence",
        "local_doc": "Lokale Doku",
        "github_file": "Code",
        "metamodel": "Metamodell",
        "jira_ticket": "Jira",
        "user_truth": "Bestaetigte Wahrheit",
    }.get(str(source_type or "").strip(), str(source_type or "").strip())


def _atomic_source_priority(*, label: str) -> tuple[int, str]:
    priority = {
        "Bestaetigte Wahrheit": 0,
        "Confluence": 1,
        "Lokale Doku": 2,
        "Code": 3,
        "Metamodell": 4,
        "Jira": 5,
    }
    normalized = str(label or "").strip()
    return (priority.get(normalized, 99), normalized)


def _dedupe_claims(claims: list[AuditClaimEntry]) -> list[AuditClaimEntry]:
    seen: set[str] = set()
    out: list[AuditClaimEntry] = []
    for claim in claims:
        if claim.claim_id in seen:
            continue
        seen.add(claim.claim_id)
        out.append(claim)
    return out
