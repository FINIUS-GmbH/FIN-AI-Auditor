from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
import re
from typing import Final

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import (
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
    RetrievalSegment,
    RetrievalSegmentClaimLink,
    SemanticEntity,
    SemanticRelation,
    TruthLedgerEntry,
    WritebackApprovalRequest,
    utc_now_iso,
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
        run = AuditRun(
            target=normalized_target,
            progress=self._build_initial_progress(target=normalized_target),
            analysis_log=[
                AuditAnalysisLogEntry(
                    source_type="system",
                    title="Run angelegt",
                    message="Audit-Run wurde angelegt. Externe Systeme bleiben read-only; Ergebnisse werden lokal gesammelt.",
                    derived_changes=["Lokale Auditor-DB wurde als einzige schreibende SSOT fuer diesen Lauf vorbereitet."],
                    impact_summary=["Der Worker kann den Lauf nun schrittweise analysieren und protokollieren."],
                )
            ],
        )
        return self._repository.upsert_run(run=run)

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
        findings = self._build_demo_findings(run=run)
        links = self._build_demo_links(findings=findings)
        snapshots = self._build_demo_snapshots(run=run)
        claims = self._build_demo_claims(run=run)
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
        claims: list[AuditClaimEntry],
        truths: list[TruthLedgerEntry],
        semantic_entities: list[SemanticEntity],
        semantic_relations: list[SemanticRelation],
        summary: str,
        analysis_notes: list[str],
        llm_usage: dict | None = None,
        worker_id: str | None = None,
    ) -> AuditRun:
        run = self._require_run(run_id=run_id)
        now_iso = utc_now_iso()
        packages = self._build_decision_packages(
            findings=findings,
            claims=claims,
            truths=truths,
            semantic_entities=semantic_entities,
            semantic_relations=semantic_relations,
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
                    findings=findings,
                    claims=claims,
                    packages=packages,
                    notes=analysis_notes,
                ),
                "source_snapshots": source_snapshots,
                "semantic_entities": semantic_entities,
                "semantic_relations": semantic_relations,
                "findings": findings,
                "finding_links": finding_links,
                "claims": claims,
                "truths": truths,
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
        updated = run.model_copy(
            update={
                "updated_at": utc_now_iso(),
                "truths": updated_truths,
                "analysis_log": next_log,
                "decision_packages": updated_packages,
                "decision_records": [*run.decision_records, decision_record],
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
        related_package_ids: list[str],
        related_finding_ids: list[str],
        payload_preview: list[str],
    ) -> AuditRun:
        run = self._require_run(run_id=run_id)
        related_findings = self._select_related_findings_for_approval(
            run=run,
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
        approval = WritebackApprovalRequest(
            target_type=target_type,
            title=title,
            summary=summary,
            target_url=effective_target_url,
            related_package_ids=list(related_package_ids),
            related_finding_ids=[finding.finding_id for finding in related_findings],
            payload_preview=effective_payload_preview,
            metadata=approval_metadata,
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
                        related_finding_ids=list(related_finding_ids),
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
        approval = self._require_approved_request(
            run=run,
            approval_request_id=approval_request_id,
            target_type="jira_ticket_create",
        )
        if self._atlassian_oauth_service is None or self._jira_ticketing_connector is None:
            raise ValueError("Jira-Writeback ist im aktuellen Auditor-Kontext noch nicht verdrahtet.")
        access_token = self._atlassian_oauth_service.get_valid_access_token_or_raise(
            required_scopes={"write:jira-work"}
        )
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
        created_issue = self._jira_ticketing_connector.create_ticket(
            target=JiraTicketTarget(
                project_key=self._settings.fixed_jira_project_key,
                board_url=self._settings.jira_board_url,
            ),
            issue_payload=jira_issue_payload,
            access_token=access_token,
        )
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
                        metadata={"approval_request_id": approval.approval_request_id},
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
        approval = self._require_approved_request(
            run=run,
            approval_request_id=approval_request_id,
            target_type="confluence_page_update",
        )
        if self._atlassian_oauth_service is None or self._confluence_page_write_connector is None:
            raise ValueError("Confluence-Writeback ist im aktuellen Auditor-Kontext noch nicht verdrahtet.")
        access_token = self._atlassian_oauth_service.get_valid_access_token_or_raise(
            required_scopes={"write:page:confluence"}
        )
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
            raise ValueError(
                "Der Confluence-Patch hat noch keinen konkreten Seitenanker und kann deshalb nicht extern ausgefuehrt werden."
            )
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
                        metadata={"approval_request_id": approval.approval_request_id},
                    ),
                ),
            }
        )
        return self._repository.upsert_run(run=updated)

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

    def _build_demo_findings(self, *, run: AuditRun) -> list[AuditFinding]:
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
                        snapshot_id="snapshot_repo_demo",
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
                        snapshot_id="snapshot_confluence_demo",
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
                        snapshot_id="snapshot_local_docs_demo",
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

    def _build_demo_claims(self, *, run: AuditRun) -> list[AuditClaimEntry]:
        return [
            AuditClaimEntry(
                source_snapshot_id="snapshot_repo_demo",
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
                source_snapshot_id="snapshot_confluence_demo",
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
                source_snapshot_id="snapshot_local_docs_demo",
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
                source_snapshot_id="snapshot_metamodel_demo",
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
        grouped_findings: dict[str, list[AuditFinding]] = defaultdict(list)
        for finding in findings:
            grouped_findings[_package_cluster_key(finding=finding)].append(finding)
        packages: list[DecisionPackage] = []
        for cluster_key, cluster_findings in sorted(grouped_findings.items(), key=lambda item: item[0]):
            dominant_category = _dominant_category(findings=cluster_findings)
            severity_summary = _highest_severity(findings=cluster_findings)
            related_claims = [
                claim
                for claim_key, claim_group in claim_map.items()
                if _claim_belongs_to_cluster(claim_key=claim_key, cluster_key=cluster_key)
                for claim in claim_group
            ]
            related_truths = [
                truth
                for truth_key, truth_group in truth_map.items()
                if _claim_belongs_to_cluster(claim_key=truth_key, cluster_key=cluster_key)
                for truth in truth_group
            ] or truth_map.get("BSM.process", [])
            cluster_claims = _dedupe_claims(
                [
                    claim
                    for claim_group in claim_map.values()
                    for claim in claim_group
                    if _claim_is_semantically_attached_to_cluster(claim=claim, cluster_key=cluster_key)
                ]
            )
            semantic_entity_ids = _dedupe_preserve_order(
                [
                    entity_id
                    for claim in cluster_claims
                    for entity_id in _string_list_from_metadata(claim.metadata.get("semantic_entity_ids"))
                ]
            )
            related_entities = [entities_by_id[entity_id] for entity_id in semantic_entity_ids if entity_id in entities_by_id]
            related_relations = _semantic_relations_for_entity_ids(
                semantic_relations=semantic_relations,
                entity_ids=set(semantic_entity_ids),
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
                    for finding in cluster_findings
                    for contract_path in _string_list_from_metadata(finding.metadata.get("semantic_contract_paths"))
                ]
            )
            semantic_section_paths = _dedupe_preserve_order(
                [
                    section_path
                    for finding in cluster_findings
                    for section_path in _string_list_from_metadata(finding.metadata.get("semantic_section_paths"))
                ]
            )

            problems: list[DecisionProblemElement] = []
            cluster_retrieval_context: list[str] = []
            cluster_anchor_values: list[str] = []
            cluster_delta_summary: list[str] = []
            cluster_delta_statuses: list[str] = []
            cluster_delta_reasons: list[str] = []
            related_finding_ids: list[str] = []
            for finding in cluster_findings:
                scope_key = str(finding.metadata.get("object_key") or finding.canonical_key or finding.title)
                retrieval_context = _string_list_from_metadata(finding.metadata.get("retrieval_context"))
                retrieval_anchor_values = _string_list_from_metadata(finding.metadata.get("retrieval_anchor_values"))
                delta_summary = _string_list_from_metadata(finding.metadata.get("delta_summary"))
                delta_statuses = _string_list_from_metadata(finding.metadata.get("delta_statuses"))
                delta_reasons = _string_list_from_metadata(finding.metadata.get("delta_reasons"))
                related_finding_ids.append(finding.finding_id)
                cluster_retrieval_context.extend(retrieval_context)
                cluster_anchor_values.extend(retrieval_anchor_values)
                cluster_delta_summary.extend(delta_summary)
                cluster_delta_statuses.extend(delta_statuses)
                cluster_delta_reasons.extend(delta_reasons)
                problems.append(
                    DecisionProblemElement(
                        finding_id=finding.finding_id,
                        category=finding.category,
                        severity=finding.severity,
                        scope_summary=scope_key,
                        short_explanation=finding.summary,
                        recommendation=finding.recommendation,
                        confidence=0.8 if finding.severity in {"critical", "high"} else 0.66,
                        affected_claim_ids=[claim.claim_id for claim in related_claims],
                        affected_truth_ids=[truth.truth_id for truth in related_truths],
                        evidence_locations=list(finding.locations),
                        metadata={
                            "origin_finding_id": finding.finding_id,
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
                        },
                    )
                )

            packages.append(
                DecisionPackage(
                    title=_package_title(cluster_key=cluster_key, findings=cluster_findings),
                    category=dominant_category,
                    severity_summary=severity_summary,
                    scope_summary=_package_scope_summary(
                        cluster_key=cluster_key,
                        findings=cluster_findings,
                        semantic_entities=related_entities,
                        semantic_relations=related_relations,
                    ),
                    rerender_required_after_decision=bool(cluster_delta_statuses),
                    recommendation_summary=_package_recommendation_summary(
                        findings=cluster_findings,
                        semantic_context=semantic_context,
                        semantic_relation_summaries=semantic_relation_summaries,
                        semantic_contract_paths=semantic_contract_paths,
                    ),
                    related_finding_ids=_dedupe_preserve_order(related_finding_ids),
                    problem_elements=problems,
                    metadata={
                        "origin": "analysis",
                        "cluster_key": cluster_key,
                        "cluster_categories": _dedupe_preserve_order([finding.category for finding in cluster_findings]),
                        "retrieval_context": _dedupe_preserve_order(cluster_retrieval_context),
                        "retrieval_anchor_values": _dedupe_preserve_order(cluster_anchor_values),
                        "delta_summary": _dedupe_preserve_order(cluster_delta_summary),
                        "delta_statuses": _dedupe_preserve_order(cluster_delta_statuses),
                        "delta_reasons": _dedupe_preserve_order(cluster_delta_reasons),
                        "truth_overlap_keys": [truth.canonical_key for truth in related_truths],
                        "semantic_entity_ids": semantic_entity_ids,
                        "semantic_context": semantic_context,
                        "semantic_relation_summaries": semantic_relation_summaries,
                        "semantic_contract_paths": semantic_contract_paths,
                        "semantic_section_paths": semantic_section_paths,
                    },
                )
            )
        return sorted(
            packages,
            key=lambda package: (
                _severity_rank(package.severity_summary),
                package.category,
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
        findings: list[AuditFinding],
        claims: list[AuditClaimEntry],
        packages: list[DecisionPackage],
        notes: list[str],
    ) -> list[AuditAnalysisLogEntry]:
        next_log = self._append_log(
            analysis_log=analysis_log,
            entry=AuditAnalysisLogEntry(
                source_type="impact_analysis",
                title="Analyse abgeschlossen",
                message="Die produktive Read-only-Pipeline hat Claims, Findings und Entscheidungspakete erzeugt.",
                related_finding_ids=[finding.finding_id for finding in findings],
                derived_changes=[
                    f"{len(claims)} Claims wurden aus den gelesenen Quellen extrahiert.",
                    f"{len(findings)} Findings wurden aus den Claims abgeleitet.",
                    f"{len(packages)} Entscheidungspakete wurden neu aufgebaut.",
                ],
                impact_summary=[
                    "Nachfolgende User-Entscheidungen arbeiten gegen echte Collector-Evidenz statt gegen Demo-Daten."
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
            subject_key = scope_key if "." in scope_key else package.scope_summary
            subject_kind = "object_property" if "." in subject_key else "scope"
            predicate = "user_specification"
            canonical_key = f"{subject_key}|{predicate}"
            superseded_truth_id: str | None = None
            next_truths: list[TruthLedgerEntry] = []
            for existing in updated_truths:
                if existing.canonical_key == canonical_key and existing.truth_status == "active":
                    superseded_truth_id = existing.truth_id
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
                metadata={"package_id": package.package_id, "scope_key": scope_key},
            )
            updated_truths.append(created)
            created_truths.append(created)
        return updated_truths, created_truths

    @staticmethod
    def _find_impacted_package_ids(*, run: AuditRun, package: DecisionPackage) -> list[str]:
        current_claim_ids = {claim_id for problem in package.problem_elements for claim_id in problem.affected_claim_ids}
        current_truth_ids = {truth_id for problem in package.problem_elements for truth_id in problem.affected_truth_ids}
        current_anchor_values = set(_string_list_from_metadata(package.metadata.get("retrieval_anchor_values")))
        current_cluster_key = str(package.metadata.get("cluster_key") or package.scope_summary).strip()
        impacted: list[str] = []
        for candidate in run.decision_packages:
            candidate_claim_ids = {
                claim_id for problem in candidate.problem_elements for claim_id in problem.affected_claim_ids
            }
            candidate_truth_ids = {
                truth_id for problem in candidate.problem_elements for truth_id in problem.affected_truth_ids
            }
            candidate_anchor_values = set(_string_list_from_metadata(candidate.metadata.get("retrieval_anchor_values")))
            candidate_cluster_key = str(candidate.metadata.get("cluster_key") or candidate.scope_summary).strip()
            if (
                candidate.package_id == package.package_id
                or current_claim_ids.intersection(candidate_claim_ids)
                or current_truth_ids.intersection(candidate_truth_ids)
                or (current_anchor_values and current_anchor_values.intersection(candidate_anchor_values))
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
        related_finding_ids: list[str],
        related_package_ids: list[str],
    ) -> list[AuditFinding]:
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
    def _build_steps(*, include_local_docs: bool) -> list[AuditProgressStep]:
        steps: list[AuditProgressStep] = []
        for step_key, label in AUDIT_PIPELINE_STEPS:
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

    def _build_initial_progress(self, *, target: AuditTarget) -> AuditRunProgress:
        return AuditRunProgress(
            progress_pct=0,
            phase_key="queued",
            phase_label="Wartet auf Worker",
            current_activity="Run wurde angelegt und wartet auf die Analyse-Pipeline.",
            steps=self._build_steps(include_local_docs=bool(target.include_local_docs)),
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
        step_key: str,
        progress_pct: int,
        current_activity: str,
        step_status: str,
        detail: str | None,
    ) -> AuditRunProgress:
        now = utc_now_iso()
        effective_steps = progress.steps or self._build_steps(include_local_docs=bool(target.include_local_docs))
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

    def _build_demo_snapshots(self, *, run: AuditRun) -> list[AuditSourceSnapshot]:
        repo_hint = run.target.local_repo_path or run.target.github_repo_url or "UNKNOWN"
        return [
            AuditSourceSnapshot(
                snapshot_id="snapshot_repo_demo",
                source_type="github_file",
                source_id="demo-code-1",
                revision_id=run.target.github_ref,
                content_hash="sha256:demo-repo-snapshot",
                sync_token=run.target.github_ref,
                metadata={"repo_path": repo_hint, "kind": "local_repo_or_remote"},
            ),
            AuditSourceSnapshot(
                snapshot_id="snapshot_confluence_demo",
                source_type="confluence_page",
                source_id="demo-page-1",
                revision_id="v1",
                content_hash="sha256:demo-confluence-page",
                sync_token="confluence:demo-page-1:v1",
                metadata={
                    "space_key": run.target.confluence_space_keys[:1],
                    "confluence_url": self._settings.confluence_home_url,
                },
            ),
            AuditSourceSnapshot(
                snapshot_id="snapshot_local_docs_demo",
                source_type="local_doc",
                source_id="demo-doc-1",
                revision_id="working-tree",
                content_hash="sha256:demo-local-doc-version",
                sync_token="local-doc:docs/roadmap.md:working-tree",
                metadata={"path_hint": "docs/roadmap.md"},
            ),
            AuditSourceSnapshot(
                snapshot_id="snapshot_metamodel_demo",
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
    raw_key = str(finding.metadata.get("object_key") or finding.canonical_key or finding.title).strip()
    if not raw_key:
        return finding.title
    return package_scope_key(raw_key)


def _claim_belongs_to_cluster(*, claim_key: str, cluster_key: str) -> bool:
    normalized_claim_key = str(claim_key or "").strip()
    normalized_cluster_key = str(cluster_key or "").strip()
    return normalized_claim_key == normalized_cluster_key or normalized_claim_key.startswith(f"{normalized_cluster_key}.")


def _claim_is_semantically_attached_to_cluster(*, claim: AuditClaimEntry, cluster_key: str) -> bool:
    if _claim_belongs_to_cluster(claim_key=claim.subject_key, cluster_key=cluster_key):
        return True
    semantic_cluster_keys = _string_list_from_metadata(claim.metadata.get("semantic_cluster_keys"))
    return cluster_key in semantic_cluster_keys


def _dominant_category(*, findings: list[AuditFinding]) -> str:
    category_counts: dict[str, tuple[int, int]] = {}
    for finding in findings:
        count, current_rank = category_counts.get(finding.category, (0, 99))
        category_counts[finding.category] = (count + 1, min(current_rank, _severity_rank(finding.severity)))
    return min(category_counts.items(), key=lambda item: (item[1][1], -item[1][0], item[0]))[0]


def _highest_severity(*, findings: list[AuditFinding]) -> str:
    return min((finding.severity for finding in findings), key=_severity_rank)


def _severity_rank(severity: str) -> int:
    order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    return order.get(str(severity), 9)


def _package_title(*, cluster_key: str, findings: list[AuditFinding]) -> str:
    if len(findings) == 1:
        return f"{cluster_key} klaeren"
    return f"{cluster_key} konsolidieren"


def _package_scope_summary(
    *,
    cluster_key: str,
    findings: list[AuditFinding],
    semantic_entities: list[SemanticEntity],
    semantic_relations: list[SemanticRelation],
) -> str:
    categories = _dedupe_preserve_order([finding.category for finding in findings])
    semantic_suffix = ""
    if semantic_entities or semantic_relations:
        semantic_suffix = f" · {len(semantic_entities)} Knoten · {len(semantic_relations)} Relationen"
    if len(findings) == 1:
        return f"{cluster_key}{semantic_suffix}"
    return f"{cluster_key} · {len(findings)} Problemelemente · {', '.join(categories)}{semantic_suffix}"


def _package_recommendation_summary(
    *,
    findings: list[AuditFinding],
    semantic_context: list[str],
    semantic_relation_summaries: list[str],
    semantic_contract_paths: list[str],
) -> str:
    recommendations = _dedupe_preserve_order([finding.recommendation for finding in findings])
    semantic_prefix_parts: list[str] = []
    if semantic_contract_paths:
        semantic_prefix_parts.append(f"Vertragskette: {semantic_contract_paths[0]}")
    if semantic_context:
        semantic_prefix_parts.append(f"Semantikfokus: {', '.join(semantic_context[:3])}")
    if semantic_relation_summaries:
        semantic_prefix_parts.append(f"Beziehungen pruefen: {semantic_relation_summaries[0]}")
    semantic_prefix = " | ".join(semantic_prefix_parts)
    if not recommendations:
        base = "Empfehlungen aus den betroffenen Problemelementen pruefen und konsolidieren."
        return f"{semantic_prefix} | {base}" if semantic_prefix else base
    if len(recommendations) == 1:
        return f"{semantic_prefix} | {recommendations[0]}" if semantic_prefix else recommendations[0]
    base = " / ".join(recommendations[:2])
    return f"{semantic_prefix} | {base}" if semantic_prefix else base


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


def _semantic_relation_summary(
    *,
    relation: SemanticRelation,
    entities_by_id: dict[str, SemanticEntity],
) -> str:
    source_label = entities_by_id.get(relation.source_entity_id).label if relation.source_entity_id in entities_by_id else relation.source_entity_id
    target_label = entities_by_id.get(relation.target_entity_id).label if relation.target_entity_id in entities_by_id else relation.target_entity_id
    return f"{source_label} -> {relation.relation_type} -> {target_label}"


def _dedupe_claims(claims: list[AuditClaimEntry]) -> list[AuditClaimEntry]:
    seen: set[str] = set()
    out: list[AuditClaimEntry] = []
    for claim in claims:
        if claim.claim_id in seen:
            continue
        seen.add(claim.claim_id)
        out.append(claim)
    return out
