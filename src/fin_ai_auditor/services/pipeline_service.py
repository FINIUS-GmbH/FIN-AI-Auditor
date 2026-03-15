from __future__ import annotations

import asyncio
from collections import defaultdict
import hashlib
import logging
import re
from typing import Iterable
from uuid import uuid4

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import (
    AuditClaimEntry,
    AuditFinding,
    AuditFindingLink,
    AuditLocation,
    AuditPosition,
    AuditRun,
    AuditSourceSnapshot,
    TruthLedgerEntry,
    new_claim_id,
    new_location_id,
    new_truth_id,
)
from fin_ai_auditor.services.audit_service import AuditService
from fin_ai_auditor.services.atlassian_oauth_service import AtlassianOAuthService
from fin_ai_auditor.services.claim_extractor import extract_claim_records
from fin_ai_auditor.services.claim_semantics import package_scope_key, semantic_values_aligned
from fin_ai_auditor.services.connectors.confluence_connector import (
    ConfluenceCollectionRequest,
    ConfluenceKnowledgeBaseConnector,
)
from fin_ai_auditor.services.connectors.github_connector import GitHubSnapshotConnector, GitHubSnapshotRequest
from fin_ai_auditor.services.connectors.metamodel_connector import MetaModelConnector
from fin_ai_auditor.services.finding_engine import build_finding_links, derive_truths, generate_findings
from fin_ai_auditor.services.pipeline_models import (
    CollectionBundle,
    CollectedDocument,
    ExtractedClaimEvidence,
    ExtractedClaimRecord,
    PipelineAnalysisResult,
)
from fin_ai_auditor.services.recommendation_engine import RecommendationEngine
from fin_ai_auditor.services.retrieval_index_service import (
    attach_retrieval_context_to_findings,
    attach_retrieval_insights_to_findings,
    build_recommendation_contexts,
    build_retrieval_index,
)
from fin_ai_auditor.services.runtime_observability_service import RuntimeObservabilityService
from fin_ai_auditor.services.semantic_graph_service import (
    attach_semantic_context_to_findings,
    build_semantic_graph,
)

logger = logging.getLogger(__name__)


class AuditPipelineService:
    def __init__(
        self,
        *,
        audit_service: AuditService,
        settings: Settings,
        allow_remote_llm: bool,
        atlassian_oauth_service: AtlassianOAuthService | None = None,
    ) -> None:
        self._audit_service = audit_service
        self._settings = settings
        self._repo_connector = GitHubSnapshotConnector()
        self._atlassian_oauth_service = atlassian_oauth_service
        self._confluence_connector = ConfluenceKnowledgeBaseConnector(settings=settings).with_access_token_provider(
            access_token_provider=(
                atlassian_oauth_service.get_valid_access_token if atlassian_oauth_service is not None else None
            )
        )
        self._metamodel_connector = MetaModelConnector(settings=settings)
        self._recommendation_engine = RecommendationEngine(
            settings=settings,
            allow_remote_calls=allow_remote_llm,
            db_path=str(audit_service.repository._db_path),
        )
        self._observability = RuntimeObservabilityService(repository=audit_service.repository)

    def execute_run(self, *, run_id: str, worker_id: str | None = None) -> AuditRun:
        run = self._audit_service.get_run(run_id=run_id)
        if run is None:
            raise ValueError(f"Audit-Run nicht gefunden: {run_id}")

        previous_run = self._audit_service.get_latest_completed_run(exclude_run_id=run_id)
        logger.info(
            "audit_pipeline_started",
            extra={
                "event_name": "audit_pipeline_started",
                "event_payload": {"run_id": run_id, "previous_run_id": previous_run.run_id if previous_run is not None else None},
            },
        )
        with self._observability.trace_span(
            trace_id=f"trace_{run_id}",
            run_id=run_id,
            worker_id=worker_id,
            span_name="pipeline.execute_run",
            metadata={"previous_run_id": previous_run.run_id if previous_run is not None else None},
        ):
            analysis = self._run_pipeline(run=run, previous_run=previous_run, worker_id=worker_id)
            completed = self._audit_service.complete_run_with_analysis(
                run_id=run.run_id,
                source_snapshots=analysis.source_snapshots,
                findings=analysis.findings,
                finding_links=analysis.finding_links,
                claims=analysis.claims,
                truths=analysis.truths,
                semantic_entities=analysis.semantic_entities,
                semantic_relations=analysis.semantic_relations,
                summary=analysis.summary,
                analysis_notes=analysis.analysis_log_messages,
                worker_id=worker_id,
            )
        self._audit_service.replace_retrieval_index(
            run_id=completed.run_id,
            segments=analysis.retrieval_segments,
            claim_links=analysis.retrieval_claim_links,
        )
        self._observability.increment_counter(
            metric_key="audit_pipeline_runs_total",
            run_id=completed.run_id,
            worker_id=worker_id,
            labels={"status": completed.status},
        )
        self._observability.increment_counter(
            metric_key="audit_pipeline_claims_total",
            run_id=completed.run_id,
            worker_id=worker_id,
            value=float(len(completed.claims)),
            labels={"status": completed.status},
        )
        self._observability.increment_counter(
            metric_key="audit_pipeline_findings_total",
            run_id=completed.run_id,
            worker_id=worker_id,
            value=float(len(completed.findings)),
            labels={"status": completed.status},
        )
        logger.info(
            "audit_pipeline_completed",
            extra={
                "event_name": "audit_pipeline_completed",
                "event_payload": {
                    "run_id": completed.run_id,
                    "claim_count": len(completed.claims),
                    "finding_count": len(completed.findings),
                    "snapshot_count": len(completed.source_snapshots),
                },
            },
        )
        refreshed = self._audit_service.get_run(run_id=completed.run_id)
        if refreshed is None:
            raise RuntimeError(f"Abgeschlossener Audit-Run konnte nach Retrieval-Indexierung nicht neu geladen werden: {completed.run_id}")
        return refreshed

    def _run_pipeline(self, *, run: AuditRun, previous_run: AuditRun | None, worker_id: str | None) -> PipelineAnalysisResult:
        notes: list[str] = []

        # Collect all sources in parallel — they are independent I/O-bound operations
        from concurrent.futures import ThreadPoolExecutor, Future

        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="collect") as pool:
            metamodel_future: Future[CollectionBundle] | None = None
            if run.target.include_metamodel:
                metamodel_future = pool.submit(self._collect_metamodel, run=run, notes=[], worker_id=worker_id)
            repo_future = pool.submit(self._collect_repo, run=run, previous_run=previous_run, notes=[], worker_id=worker_id)
            confluence_future = pool.submit(self._collect_confluence, run=run, previous_run=previous_run, notes=[], worker_id=worker_id)

        metamodel_bundle = metamodel_future.result() if metamodel_future is not None else CollectionBundle([], [])
        repo_bundle = repo_future.result()
        confluence_bundle = confluence_future.result()
        code_bundle = _select_bundle_documents(bundle=repo_bundle, source_types={"github_file"})
        local_docs_bundle = _select_bundle_documents(bundle=repo_bundle, source_types={"local_doc"})

        all_bundles = [metamodel_bundle, code_bundle, local_docs_bundle, confluence_bundle]
        notes.extend(n for bundle in all_bundles for n in bundle.analysis_notes)
        source_snapshots = [snapshot for bundle in all_bundles for snapshot in bundle.snapshots]
        documents = [document for bundle in all_bundles for document in bundle.documents]
        self._audit_service.cache_documents(documents=documents)
        source_snapshots = _annotate_snapshot_deltas(current=source_snapshots, previous=previous_run.source_snapshots if previous_run else [])

        self._audit_service.update_run_progress(
            run_id=run.run_id,
            step_key="delta_reconciliation",
            progress_pct=60,
            current_activity="Delta-Abgleich ordnet neue Aenderungen vorhandenen Anchors, Claims und Wahrheiten zu.",
            step_status="running",
            detail="Snapshot-Hashes und Claim-Fingerprints werden gegen den letzten abgeschlossenen Lauf gespiegelt.",
            worker_id=worker_id,
        )
        fresh_documents, reused_documents = _partition_documents_for_claim_reuse(documents=documents)
        regular_fresh_documents, section_regenerated_documents, section_reused_claim_records, section_regen_notes = (
            _prepare_section_level_confluence_regeneration(
                previous_run=previous_run,
                documents=fresh_documents,
            )
        )
        cached_claim_records, fully_reused_sources = _rebuild_cached_claim_records(
            previous_run=previous_run,
            reused_documents=reused_documents,
        )
        fallback_documents = [
            document
            for document in reused_documents
            if (document.source_type, document.source_id) not in fully_reused_sources
        ]
        claim_records = [
            *cached_claim_records,
            *section_reused_claim_records,
            *extract_claim_records(documents=[*regular_fresh_documents, *section_regenerated_documents, *fallback_documents]),
        ]
        notes.extend(section_regen_notes)
        notes.extend(
            _build_claim_reuse_notes(
                fresh_documents=[*regular_fresh_documents, *section_regenerated_documents],
                reused_documents=reused_documents,
                cached_claim_records=[*cached_claim_records, *section_reused_claim_records],
                fallback_documents=fallback_documents,
            )
        )
        semantic_graph = build_semantic_graph(
            run_id=run.run_id,
            claim_records=claim_records,
            truths=_inherit_truths(previous_run=previous_run),
        )
        claims = _annotate_claim_deltas(
            current=semantic_graph.claims,
            previous=previous_run.claims if previous_run else [],
        )
        claim_records = _replace_claims_in_records(records=claim_records, claims=claims)
        impacted_scope_keys = _derive_impacted_scope_keys(
            claims=claims,
            inherited_truths=_inherit_truths(previous_run=previous_run),
        )
        previous_segments = (
            self._audit_service.list_retrieval_segments(run_id=previous_run.run_id) if previous_run is not None else []
        )
        self._audit_service.update_run_progress(
            run_id=run.run_id,
            step_key="retrieval_indexing",
            progress_pct=70,
            current_activity="Lokaler Retrieval-Index segmentiert Quellen, verknuepft Claims und berechnet Suchanker.",
            step_status="running",
            detail="Geaenderte Code-, Doku- und Metamodellsegmente werden fuer spaetere Delta-Neubewertung vorbereitet.",
            worker_id=worker_id,
        )
        retrieval_index = build_retrieval_index(
            settings=self._settings,
            run_id=run.run_id,
            documents=documents,
            claim_records=claim_records,
            previous_segments=previous_segments,
            allow_remote_embeddings=self._recommendation_engine.allow_remote_calls,
        )
        inherited_truths = _inherit_truths(previous_run=previous_run)
        truths = derive_truths(inherited_truths=inherited_truths, claim_records=claim_records)
        self._audit_service.update_run_progress(
            run_id=run.run_id,
            step_key="finding_generation",
            progress_pct=78,
            current_activity="Atomare Problemelemente und Widerspruchscluster werden neu generiert.",
            step_status="running",
            detail=f"{len(claims)} Claims werden jetzt in Findings und Beziehungen ueberfuehrt.",
            worker_id=worker_id,
        )
        findings, finding_links = _generate_incremental_findings(
            claim_records=claim_records,
            previous_run=previous_run,
            truths=truths,
            impacted_scope_keys=impacted_scope_keys,
        )

        # Embedding-based cross-document contradiction detection
        from fin_ai_auditor.services.embedding_contradiction_detector import detect_cross_document_contradictions
        embedding_findings = detect_cross_document_contradictions(
            settings=self._settings,
            claim_records=claim_records,
            allow_remote_embeddings=self._recommendation_engine.allow_remote_calls,
        )
        if embedding_findings:
            findings.extend(embedding_findings)
            notes.append(f"Embedding-Widerspruchserkennung: {len(embedding_findings)} semantische Widersprueche gefunden.")

        # Deterministic BSM domain contradiction detection
        from fin_ai_auditor.services.bsm_domain_contradiction_detector import detect_bsm_domain_contradictions
        bsm_findings = detect_bsm_domain_contradictions(claim_records=claim_records)
        if bsm_findings:
            findings.extend(bsm_findings)
            notes.append(f"BSM-Domain-Widerspruchserkennung: {len(bsm_findings)} strukturelle Widersprueche gefunden.")

        findings = attach_semantic_context_to_findings(
            findings=findings,
            claims=claims,
            semantic_entities=semantic_graph.semantic_entities,
            semantic_relations=semantic_graph.semantic_relations,
        )
        recommendation_contexts = build_recommendation_contexts(
            settings=self._settings,
            findings=findings,
            segments=retrieval_index.segments,
            allow_remote_embeddings=self._recommendation_engine.allow_remote_calls,
        )
        findings = attach_retrieval_context_to_findings(findings=findings, contexts=recommendation_contexts)
        findings = attach_retrieval_insights_to_findings(findings=findings, segments=retrieval_index.segments)
        self._audit_service.update_run_progress(
            run_id=run.run_id,
            step_key="llm_recommendations",
            progress_pct=89,
            current_activity="LLM-Empfehlungen verdichten die Evidenz in kurze, pruefbare Handlungsoptionen.",
            step_status="running",
            detail="Chunked LLM-Anreicherung mit vollem Repo-, Metamodell- und Confluence-Kontext.",
            worker_id=worker_id,
        )

        # Build context layers for the LLM — with caching
        from fin_ai_auditor.services.context_builder import AuditContextBuilder
        from fin_ai_auditor.services.pipeline_cache_service import PipelineCacheService

        ctx_builder = AuditContextBuilder()
        cache = PipelineCacheService(db_path=self._audit_service.repository._db_path)

        # Repo summary — cached per content hash (immutable)
        repo_hash = PipelineCacheService.build_content_hash(documents, "github_file")
        repo_context = cache.get_repo_summary(content_hash=repo_hash)
        if repo_context is None:
            repo_context = ctx_builder.build_repo_summary(documents)
            cache.set_repo_summary(content_hash=repo_hash, summary=repo_context)

        # Metamodel summary — cached per content hash
        meta_hash = PipelineCacheService.build_content_hash(documents, "metamodel")
        metamodel_context = cache.get_context_summary(cache_type="metamodel_summary", content_hash=meta_hash)
        if metamodel_context is None:
            metamodel_context = ctx_builder.build_metamodel_summary(documents)
            cache.set_context_summary(cache_type="metamodel_summary", content_hash=meta_hash, summary=metamodel_context)

        # Confluence map — cached per content hash
        conf_hash = PipelineCacheService.build_content_hash(documents, "confluence_page")
        confluence_context = cache.get_context_summary(cache_type="confluence_map", content_hash=conf_hash)
        if confluence_context is None:
            confluence_context = ctx_builder.build_confluence_map(documents)
            cache.set_context_summary(cache_type="confluence_map", content_hash=conf_hash, summary=confluence_context)

        # FIN-AI architecture docs context — cached per repo hash
        arch_context = cache.get_context_summary(cache_type="arch_docs", content_hash=repo_hash)
        if arch_context is None:
            arch_context = ctx_builder.build_finai_architecture_context(documents) or ""
            cache.set_context_summary(cache_type="arch_docs", content_hash=repo_hash, summary=arch_context)
        if arch_context:
            confluence_context = confluence_context + "\n\n---\n\n" + arch_context

        def _on_llm_chunk(chunk_done: int, chunk_total: int) -> None:
            pct = 89 + int((chunk_done / max(chunk_total, 1)) * 7)  # 89% → 96%
            self._audit_service.update_run_progress(
                run_id=run.run_id,
                step_key="llm_recommendations",
                progress_pct=min(pct, 96),
                current_activity=f"LLM-Empfehlung: Chunk {chunk_done}/{chunk_total} verarbeitet.",
                step_status="running",
                detail=f"Chunk {chunk_done} von {chunk_total} abgeschlossen.",
                worker_id=worker_id,
            )

        findings, llm_notes, llm_usage = asyncio.run(
            self._recommendation_engine.enrich_findings(
                findings=findings,
                truths=truths,
                retrieved_contexts=recommendation_contexts,
                repo_context=repo_context,
                metamodel_context=metamodel_context,
                confluence_context=confluence_context,
                progress_callback=_on_llm_chunk,
            )
        )
        finding_links = build_finding_links(findings=findings)
        notes.extend(llm_notes)
        notes.extend(semantic_graph.notes)
        notes.extend(retrieval_index.notes)
        notes.extend(_build_delta_notes(snapshots=source_snapshots, claims=claims, truths=truths))
        if impacted_scope_keys:
            notes.append(
                f"Neubewertung fokussiert auf {len(impacted_scope_keys)} betroffene Scope-Cluster: "
                + ", ".join(sorted(impacted_scope_keys)[:8])
            )
        self._audit_service.update_run_progress(
            run_id=run.run_id,
            step_key="decision_packages",
            progress_pct=96,
            current_activity="Kleine Entscheidungspakete fuer den User werden vorbereitet.",
            step_status="running",
            detail=f"{len(findings)} Findings werden jetzt fuer die UI in atomare Pakete gruppiert.",
            worker_id=worker_id,
        )

        # Build cost note
        if llm_usage.get("total_prompt_tokens", 0) > 0:
            total_tokens = llm_usage["total_prompt_tokens"] + llm_usage.get("total_completion_tokens", 0)
            cost_eur = llm_usage.get("total_cost_eur", 0.0)
            model_details = []
            for model, data in llm_usage.get("by_model", {}).items():
                short_model = model.split("/")[-1] if "/" in model else model
                model_details.append(
                    f"{short_model}: {data['calls']}x, {data['prompt_tokens']+data['completion_tokens']} Tokens, {data['cost_eur']:.4f}€"
                )
            notes.append(
                f"LLM-Kosten: {total_tokens:,} Tokens gesamt, {cost_eur:.4f}€ geschaetzt. "
                + "; ".join(model_details)
            )

        summary = (
            f"Produktive Read-only-Analyse abgeschlossen: {len(claims)} Claims, "
            f"{len(findings)} Findings, {len(semantic_graph.semantic_entities)} semantische Knoten, "
            f"{len(semantic_graph.semantic_relations)} semantische Relationen, {len(source_snapshots)} Snapshots, "
            f"{len(retrieval_index.segments)} Retrieval-Segmente."
        )
        return PipelineAnalysisResult(
            source_snapshots=source_snapshots,
            findings=findings,
            finding_links=finding_links,
            claims=claims,
            truths=truths,
            semantic_entities=semantic_graph.semantic_entities,
            semantic_relations=semantic_graph.semantic_relations,
            retrieval_segments=retrieval_index.segments,
            retrieval_claim_links=retrieval_index.claim_links,
            analysis_log_messages=notes,
            summary=summary,
        )

    def _collect_metamodel(self, *, run: AuditRun, notes: list[str], worker_id: str | None) -> CollectionBundle:
        self._audit_service.update_run_progress(
            run_id=run.run_id,
            step_key="metamodel_check",
            progress_pct=8,
            current_activity="Metamodell-Dump wird gelesen, aktualisiert und gegen den letzten Stand verglichen.",
            step_status="running",
            detail="Der aktuelle BSM-Katalog wird read-only geladen und als lokaler Dump aktualisiert.",
            worker_id=worker_id,
        )
        with self._observability.trace_span(
            trace_id=f"trace_{run.run_id}",
            run_id=run.run_id,
            worker_id=worker_id,
            span_name="pipeline.collect_metamodel",
        ):
            bundle = self._metamodel_connector.collect_catalog()
        notes.extend(bundle.analysis_notes)
        return bundle

    def _collect_repo(
        self,
        *,
        run: AuditRun,
        previous_run: AuditRun | None,
        notes: list[str],
        worker_id: str | None,
    ) -> CollectionBundle:
        self._audit_service.update_run_progress(
            run_id=run.run_id,
            step_key="finai_code_check",
            progress_pct=22,
            current_activity="Lokales FIN-AI Repo wird auf fachliche Lese- und Schreibpfade geprueft.",
            step_status="running",
            detail="Code- und Doku-Dateien werden aus dem lokalen FIN-AI-Checkout read-only eingesammelt.",
            worker_id=worker_id,
        )
        with self._observability.trace_span(
            trace_id=f"trace_{run.run_id}",
            run_id=run.run_id,
            worker_id=worker_id,
            span_name="pipeline.collect_repo",
        ):
            bundle = self._repo_connector.collect_snapshot(
                request=GitHubSnapshotRequest(
                    repo_url=run.target.github_repo_url,
                    local_repo_path=run.target.local_repo_path,
                    git_ref=run.target.github_ref,
                    previous_snapshots=previous_run.source_snapshots if (previous_run is not None) else [],
                    document_cache_lookup=lambda source_type, source_id, content_hash: self._audit_service.get_cached_document(
                        source_type=source_type,
                        source_id=source_id,
                        content_hash=content_hash,
                    ),
                )
            )
        code_docs = sum(1 for document in bundle.documents if document.source_type == "github_file")
        reused_docs = sum(1 for document in bundle.documents if bool(document.metadata.get("incremental_reused")))
        self._observability.increment_counter(
            metric_key="audit_pipeline_reused_documents_total",
            run_id=run.run_id,
            worker_id=worker_id,
            value=float(reused_docs),
            labels={"source": "repo"},
        )
        self._audit_service.update_run_progress(
            run_id=run.run_id,
            step_key="local_docs_check",
            progress_pct=46,
            current_activity="Lokale `_docs` und Arbeitsdokumente werden mit den externen Quellen abgeglichen.",
            step_status="running",
            detail=(
                f"{code_docs} Code-Artefakte und "
                f"{sum(1 for document in bundle.documents if document.source_type == 'local_doc')} lokale Doku-Artefakte "
                "wurden aus dem Repo gelesen."
            ),
            worker_id=worker_id,
        )
        notes.extend(bundle.analysis_notes)
        return bundle

    def _collect_confluence(
        self,
        *,
        run: AuditRun,
        previous_run: AuditRun | None,
        notes: list[str],
        worker_id: str | None,
    ) -> CollectionBundle:
        self._audit_service.update_run_progress(
            run_id=run.run_id,
            step_key="confluence_check",
            progress_pct=34,
            current_activity="Confluence-Seiten werden lesend geladen und auf relevante Aussagen reduziert.",
            step_status="running",
            detail="Confluence-Collector versucht einen read-only Abruf der relevanten FINAI-Seiten.",
            worker_id=worker_id,
        )
        with self._observability.trace_span(
            trace_id=f"trace_{run.run_id}",
            run_id=run.run_id,
            worker_id=worker_id,
            span_name="pipeline.collect_confluence",
        ):
            bundle = self._confluence_connector.collect_pages(
                request=ConfluenceCollectionRequest(
                    space_keys=list(run.target.confluence_space_keys),
                    page_ids=list(run.target.confluence_page_ids),
                ),
                previous_snapshots=previous_run.source_snapshots if previous_run is not None else [],
                document_cache_lookup=lambda source_type, source_id, content_hash: self._audit_service.get_cached_document(
                    source_type=source_type,
                    source_id=source_id,
                    content_hash=content_hash,
                ),
                latest_document_cache_lookup=lambda source_type, source_id: self._audit_service.get_latest_cached_document(
                    source_type=source_type,
                    source_id=source_id,
                ),
            )
        reused_docs = sum(1 for document in bundle.documents if bool(document.metadata.get("incremental_reused")))
        changed_sections = sum(
            len(document.metadata.get("changed_section_paths") or [])
            for document in bundle.documents
            if document.source_type == "confluence_page"
        )
        self._observability.increment_counter(
            metric_key="audit_pipeline_reused_documents_total",
            run_id=run.run_id,
            worker_id=worker_id,
            value=float(reused_docs),
            labels={"source": "confluence"},
        )
        self._observability.increment_counter(
            metric_key="audit_pipeline_changed_sections_total",
            run_id=run.run_id,
            worker_id=worker_id,
            value=float(changed_sections),
            labels={"source": "confluence"},
        )
        notes.extend(bundle.analysis_notes)
        return bundle


def _select_bundle_documents(*, bundle: CollectionBundle, source_types: set[str]) -> CollectionBundle:
    documents = [document for document in bundle.documents if document.source_type in source_types]
    snapshot_ids = {document.snapshot.snapshot_id for document in documents}
    snapshots = [snapshot for snapshot in bundle.snapshots if snapshot.snapshot_id in snapshot_ids]
    return CollectionBundle(snapshots=snapshots, documents=documents, analysis_notes=list(bundle.analysis_notes))


def _partition_documents_for_claim_reuse(
    *,
    documents: list[CollectedDocument],
) -> tuple[list[CollectedDocument], list[CollectedDocument]]:
    fresh: list[CollectedDocument] = []
    reused: list[CollectedDocument] = []
    for document in documents:
        if bool(document.snapshot.metadata.get("incremental_reused")):
            reused.append(document)
        else:
            fresh.append(document)
    return fresh, reused


def _prepare_section_level_confluence_regeneration(
    *,
    previous_run: AuditRun | None,
    documents: list[CollectedDocument],
) -> tuple[list[CollectedDocument], list[CollectedDocument], list[ExtractedClaimRecord], list[str]]:
    if previous_run is None:
        return documents, [], [], []
    previous_claims_by_source: dict[tuple[str, str], list[AuditClaimEntry]] = defaultdict(list)
    for claim in previous_run.claims:
        previous_claims_by_source[(claim.source_type, claim.source_id)].append(claim)

    regular_documents: list[CollectedDocument] = []
    regenerated_documents: list[CollectedDocument] = []
    reused_claim_records: list[ExtractedClaimRecord] = []
    notes: list[str] = []

    for document in documents:
        if not _is_section_level_confluence_candidate(document=document):
            regular_documents.append(document)
            continue
        previous_claims = previous_claims_by_source.get((document.source_type, document.source_id), [])
        if not previous_claims:
            regular_documents.append(document)
            notes.append(
                f"Confluence-Section-Regen fuer {document.source_id} faellt auf Vollseite zurueck, weil keine Vorlaeufer-Claims vorliegen."
            )
            continue
        affected_section_paths = _minimal_section_paths(
            section_paths=[
                *_string_list(document.metadata.get("changed_section_paths")),
                *_string_list(document.metadata.get("added_section_paths")),
                *_string_list(document.metadata.get("removed_section_paths")),
            ]
        )
        if not affected_section_paths:
            regular_documents.append(document)
            continue
        selected_section_paths = _minimal_section_paths(
            section_paths=[
                *_string_list(document.metadata.get("changed_section_paths")),
                *_string_list(document.metadata.get("added_section_paths")),
            ]
        )
        section_documents = _build_section_regeneration_documents(
            document=document,
            selected_section_paths=selected_section_paths,
        )
        if selected_section_paths and not section_documents:
            regular_documents.append(document)
            notes.append(
                f"Confluence-Section-Regen fuer {document.source_id} konnte keine fokussierten Abschnittsdokumente bilden und liest die Seite deshalb vollstaendig neu."
            )
            continue
        reused_for_document = 0
        for claim in previous_claims:
            claim_section_path = str(claim.metadata.get("evidence_section_path") or "").strip()
            if _claim_is_affected_by_changed_sections(
                claim_section_path=claim_section_path,
                changed_section_paths=affected_section_paths,
            ):
                continue
            rebuilt_record = _rebuild_claim_record_from_cache(claim=claim, document=document)
            if rebuilt_record is None:
                continue
            reused_claim_records.append(rebuilt_record)
            reused_for_document += 1
        regenerated_documents.extend(section_documents)
        notes.append(
            f"Confluence-Section-Regen fuer {document.source_id}: {reused_for_document} Claims aus unveraenderten Sektionen wiederverwendet, "
            f"{len(section_documents)} Abschnittsdokumente fuer fokussierte Neu-Extraktion vorbereitet."
        )
    return regular_documents, regenerated_documents, reused_claim_records, notes


def _rebuild_cached_claim_records(
    *,
    previous_run: AuditRun | None,
    reused_documents: list[CollectedDocument],
) -> tuple[list[ExtractedClaimRecord], set[tuple[str, str]]]:
    if previous_run is None or not reused_documents:
        return [], set()
    document_map = {(document.source_type, document.source_id): document for document in reused_documents}
    claims_by_source: dict[tuple[str, str], list[AuditClaimEntry]] = defaultdict(list)
    for claim in previous_run.claims:
        key = (claim.source_type, claim.source_id)
        if key in document_map:
            claims_by_source[key].append(claim)

    rebuilt_records: list[ExtractedClaimRecord] = []
    fully_reused_sources: set[tuple[str, str]] = set()
    for source_key, claims in claims_by_source.items():
        document = document_map[source_key]
        source_records: list[ExtractedClaimRecord] = []
        rebuildable = True
        for claim in claims:
            record = _rebuild_claim_record_from_cache(claim=claim, document=document)
            if record is None:
                rebuildable = False
                break
            source_records.append(record)
        if not rebuildable:
            continue
        rebuilt_records.extend(source_records)
        fully_reused_sources.add(source_key)
    return rebuilt_records, fully_reused_sources


def _rebuild_claim_record_from_cache(
    *,
    claim: AuditClaimEntry,
    document: CollectedDocument,
) -> ExtractedClaimRecord | None:
    matched_text = str(claim.metadata.get("matched_text") or "").strip()
    anchor_kind = str(claim.metadata.get("evidence_anchor_kind") or "").strip()
    anchor_value = str(claim.metadata.get("evidence_anchor_value") or "").strip()
    if not matched_text or not anchor_kind or not anchor_value:
        return None
    line_start = _optional_int(claim.metadata.get("evidence_line_start"))
    line_end = _optional_int(claim.metadata.get("evidence_line_end"))
    location = AuditLocation(
        snapshot_id=document.snapshot.snapshot_id,
        source_type=document.source_type,
        source_id=document.source_id,
        title=str(claim.metadata.get("title") or document.title).strip() or document.title,
        path_hint=str(claim.metadata.get("path_hint") or document.path_hint or "").strip() or document.path_hint,
        url=str(claim.metadata.get("evidence_url") or document.url or "").strip() or document.url,
        position=AuditPosition(
            anchor_kind=anchor_kind,
            anchor_value=anchor_value,
            section_path=str(claim.metadata.get("evidence_section_path") or "").strip() or None,
            line_start=line_start,
            line_end=line_end,
            snippet_hash=_snippet_hash_or_none(matched_text=matched_text),
            content_hash=document.snapshot.content_hash,
        ),
        metadata={
            "matched_text": matched_text,
            "incremental_reused_claim": True,
            "reused_from_claim_id": claim.claim_id,
        },
    )
    rebuilt_claim = claim.model_copy(
        update={
            "claim_id": new_claim_id(),
            "source_snapshot_id": document.snapshot.snapshot_id,
            "evidence_location_ids": [location.location_id],
            "metadata": {
                **claim.metadata,
                "incremental_reused_claim": True,
                "reused_from_claim_id": claim.claim_id,
            },
        }
    )
    return ExtractedClaimRecord(
        claim=rebuilt_claim,
        evidence=ExtractedClaimEvidence(location=location, matched_text=matched_text),
    )


def _build_claim_reuse_notes(
    *,
    fresh_documents: list[CollectedDocument],
    reused_documents: list[CollectedDocument],
    cached_claim_records: list[ExtractedClaimRecord],
    fallback_documents: list[CollectedDocument],
) -> list[str]:
    return [
        (
            f"Claim-Reuse: {len(cached_claim_records)} Claims aus unveraenderten Quellen wiederverwendet, "
            f"{len(fresh_documents)} frische Dokumente neu extrahiert."
        ),
        (
            f"Fallback fuer {len(fallback_documents)} wiederverwendete Dokumente, "
            "deren alte Claim-Evidenz noch nicht vollstaendig rekonstruiert werden konnte."
            if fallback_documents
            else f"{len(reused_documents)} unveraenderte Dokumente konnten claim-seitig ohne Neuparse geprueft werden."
        ),
    ]


def _is_section_level_confluence_candidate(*, document: CollectedDocument) -> bool:
    if document.source_type != "confluence_page":
        return False
    if str(document.metadata.get("section_delta_status") or "").strip() != "changed":
        return False
    if not isinstance(document.metadata.get("structured_blocks"), list):
        return False
    return bool(
        _string_list(document.metadata.get("changed_section_paths"))
        or _string_list(document.metadata.get("added_section_paths"))
        or _string_list(document.metadata.get("removed_section_paths"))
    )


def _build_section_regeneration_documents(
    *,
    document: CollectedDocument,
    selected_section_paths: list[str],
) -> list[CollectedDocument]:
    blocks = list(document.metadata.get("structured_blocks") or [])
    documents: list[CollectedDocument] = []
    for index, section_path in enumerate(selected_section_paths, start=1):
        rendered_body = _render_section_document_body(blocks=blocks, target_section_path=section_path)
        if not rendered_body.strip():
            continue
        documents.append(
            CollectedDocument(
                snapshot=document.snapshot,
                source_type=document.source_type,
                source_id=document.source_id,
                title=document.title,
                body=rendered_body,
                path_hint=document.path_hint,
                url=document.url,
                metadata={
                    **document.metadata,
                    "section_regeneration_target": section_path,
                    "selected_section_paths": [section_path],
                    "incremental_section_regeneration": True,
                    "section_regeneration_ordinal": index,
                },
            )
        )
    return documents


def _render_section_document_body(*, blocks: list[object], target_section_path: str) -> str:
    normalized_target_tokens = _normalize_section_path_tokens(target_section_path)
    if not normalized_target_tokens:
        return ""
    lines: list[str] = []
    emitted_heading_paths: set[str] = set()
    for raw_block in blocks:
        if not isinstance(raw_block, dict):
            continue
        block_section_path = str(raw_block.get("section_path") or "").strip()
        block_tokens = _normalize_section_path_tokens(block_section_path)
        if not _section_path_tokens_overlap(target=normalized_target_tokens, candidate=block_tokens):
            continue
        kind = str(raw_block.get("kind") or "text").strip()
        text = str(raw_block.get("text") or "").strip()
        if not text:
            continue
        block_heading_path = _join_section_path_tokens(block_tokens)
        if block_heading_path and block_heading_path not in emitted_heading_paths:
            lines.extend(_heading_lines_for_section_path(block_tokens))
            emitted_heading_paths.add(block_heading_path)
        if kind == "heading":
            continue
        lines.append(text)
    return "\n".join(_dedupe_consecutive_lines(lines=lines))


def _heading_lines_for_section_path(tokens: list[str]) -> list[str]:
    lines: list[str] = []
    for depth, token in enumerate(tokens, start=1):
        lines.append(f"{'#' * max(1, depth)} {token}")
    return lines


def _dedupe_consecutive_lines(*, lines: list[str]) -> list[str]:
    deduped: list[str] = []
    previous = ""
    for line in lines:
        normalized = str(line or "").strip()
        if not normalized:
            continue
        if normalized == previous:
            continue
        deduped.append(normalized)
        previous = normalized
    return deduped


def _claim_is_affected_by_changed_sections(
    *,
    claim_section_path: str,
    changed_section_paths: list[str],
) -> bool:
    claim_tokens = _normalize_section_path_tokens(claim_section_path)
    if not claim_tokens:
        return True
    for changed_path in changed_section_paths:
        changed_tokens = _normalize_section_path_tokens(changed_path)
        if _section_path_tokens_overlap(target=changed_tokens, candidate=claim_tokens):
            return True
    return False


def _minimal_section_paths(*, section_paths: list[str]) -> list[str]:
    normalized_paths: list[list[str]] = []
    for section_path in section_paths:
        tokens = _normalize_section_path_tokens(section_path)
        if tokens:
            normalized_paths.append(tokens)
    minimal: list[list[str]] = []
    for tokens in sorted(normalized_paths, key=lambda item: (len(item), item)):
        if any(_section_path_tokens_overlap(target=existing, candidate=tokens) for existing in minimal):
            continue
        minimal.append(tokens)
    return [_join_section_path_tokens(tokens) for tokens in minimal]


def _section_path_tokens_overlap(*, target: list[str], candidate: list[str]) -> bool:
    if not target or not candidate:
        return False
    target_length = len(target)
    candidate_length = len(candidate)
    for start_index in range(0, max(1, candidate_length - target_length + 1)):
        if candidate[start_index : start_index + target_length] == target:
            return True
    return target[:candidate_length] == candidate if candidate_length <= target_length else False


def _normalize_section_path_tokens(section_path: str) -> list[str]:
    normalized = str(section_path or "").strip()
    if not normalized:
        return []
    separators_normalized = re.sub(r"\s*(?:/|>)\s*", ">", normalized)
    return [part.strip() for part in separators_normalized.split(">") if part.strip()]


def _join_section_path_tokens(tokens: list[str]) -> str:
    return " > ".join(str(token).strip() for token in tokens if str(token).strip())


def _generate_incremental_findings(
    *,
    claim_records: list[ExtractedClaimRecord],
    previous_run: AuditRun | None,
    truths: list[TruthLedgerEntry],
    impacted_scope_keys: set[str],
) -> tuple[list[AuditFinding], list[AuditFindingLink]]:
    if previous_run is None:
        return generate_findings(
            claim_records=claim_records,
            inherited_truths=truths,
            impacted_scope_keys=impacted_scope_keys,
        )
    if not impacted_scope_keys:
        reused = [_clone_reused_finding(finding=finding) for finding in previous_run.findings]
        return reused, build_finding_links(findings=reused)

    impacted_claim_records = _filter_claim_records_for_impacted_scopes(
        claim_records=claim_records,
        impacted_scope_keys=impacted_scope_keys,
    )
    regenerated_findings, _ = generate_findings(
        claim_records=impacted_claim_records,
        inherited_truths=truths,
        impacted_scope_keys=impacted_scope_keys,
    )
    reused_findings = [
        _clone_reused_finding(finding=finding)
        for finding in previous_run.findings
        if _finding_scope_key(finding=finding) not in impacted_scope_keys
    ]
    merged_findings = [*reused_findings, *regenerated_findings]
    return merged_findings, build_finding_links(findings=merged_findings)


def _clone_reused_finding(*, finding: AuditFinding) -> AuditFinding:
    return finding.model_copy(
        update={
            "finding_id": f"finding_{uuid4().hex}",
            "locations": [_clone_reused_location(location=location) for location in finding.locations],
            "metadata": {
                **finding.metadata,
                "incremental_reused_finding": True,
                "reused_from_finding_id": finding.finding_id,
            },
        }
    )


def _clone_reused_location(*, location: AuditLocation) -> AuditLocation:
    return location.model_copy(
        update={
            "location_id": new_location_id(),
            "metadata": {
                **location.metadata,
                "incremental_reused_location": True,
                "reused_from_location_id": location.location_id,
            },
        }
    )


def _filter_claim_records_for_impacted_scopes(
    *,
    claim_records: list[ExtractedClaimRecord],
    impacted_scope_keys: set[str],
) -> list[ExtractedClaimRecord]:
    if not impacted_scope_keys:
        return claim_records
    filtered: list[ExtractedClaimRecord] = []
    for record in claim_records:
        subject_scope = package_scope_key(record.claim.subject_key)
        if record.claim.source_type == "metamodel":
            filtered.append(record)
            continue
        if subject_scope in impacted_scope_keys or record.claim.subject_key in impacted_scope_keys:
            filtered.append(record)
    return filtered


def _finding_scope_key(*, finding: AuditFinding) -> str:
    return package_scope_key(str(finding.canonical_key or finding.metadata.get("object_key") or finding.title))


def _optional_int(value: object) -> int | None:
    try:
        parsed = int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
    return parsed if parsed is not None and parsed >= 0 else None


def _snippet_hash_or_none(*, matched_text: str) -> str | None:
    normalized = str(matched_text or "").strip()
    if not normalized:
        return None
    return f"sha256:{hashlib.sha256(normalized.encode('utf-8')).hexdigest()}"


def _annotate_snapshot_deltas(
    *,
    current: list[AuditSourceSnapshot],
    previous: list[AuditSourceSnapshot],
) -> list[AuditSourceSnapshot]:
    previous_map = {(snapshot.source_type, snapshot.source_id): snapshot for snapshot in previous}
    annotated: list[AuditSourceSnapshot] = []
    for snapshot in current:
        previous_snapshot = previous_map.get((snapshot.source_type, snapshot.source_id))
        delta_status = "added"
        if previous_snapshot is not None:
            delta_status = "unchanged" if previous_snapshot.content_hash == snapshot.content_hash else "changed"
        annotated.append(
            snapshot.model_copy(
                update={
                    "metadata": {
                        **snapshot.metadata,
                        "delta_status": delta_status,
                        "previous_snapshot_id": previous_snapshot.snapshot_id if previous_snapshot is not None else None,
                    }
                }
            )
        )
    return annotated


def _annotate_claim_deltas(*, current: list[AuditClaimEntry], previous: list[AuditClaimEntry]) -> list[AuditClaimEntry]:
    previous_fingerprints = {claim.fingerprint for claim in previous}
    previous_by_identity: dict[tuple[str, str, str, str], list[AuditClaimEntry]] = {}
    for claim in previous:
        previous_by_identity.setdefault(_claim_identity(claim=claim), []).append(claim)
    annotated: list[AuditClaimEntry] = []
    for claim in current:
        delta_status = "added"
        delta_change_type = "new_identity"
        previous_matches = previous_by_identity.get(_claim_identity(claim=claim), [])
        if claim.fingerprint in previous_fingerprints:
            delta_status = "unchanged"
            delta_change_type = "exact"
        elif previous_matches:
            if any(
                semantic_values_aligned(
                    subject_key=claim.subject_key,
                    predicate=claim.predicate,
                    left_value=claim.normalized_value,
                    right_value=previous_claim.normalized_value,
                )
                for previous_claim in previous_matches
            ):
                delta_status = "changed"
                delta_change_type = "textual_only"
            else:
                delta_status = "changed"
                delta_change_type = "semantic"
        annotated.append(
            claim.model_copy(
                update={
                    "metadata": {
                        **claim.metadata,
                        "delta_status": delta_status,
                        "delta_change_type": delta_change_type,
                        "delta_scope_key": package_scope_key(claim.subject_key),
                    }
                }
            )
        )
    return annotated


def _replace_claims_in_records(*, records: list[object], claims: list[AuditClaimEntry]) -> list[object]:
    claim_map = {claim.fingerprint: claim for claim in claims}
    updated_records = []
    for record in records:
        updated_records.append(
            record.__class__(
                claim=claim_map.get(record.claim.fingerprint, record.claim),
                evidence=record.evidence,
            )
        )
    return updated_records


def _inherit_truths(*, previous_run: AuditRun | None) -> list[TruthLedgerEntry]:
    if previous_run is None:
        return []
    inherited: list[TruthLedgerEntry] = []
    for truth in previous_run.truths:
        if truth.truth_status != "active":
            continue
        inherited.append(
            truth.model_copy(
                update={
                    "truth_id": new_truth_id(),
                    "metadata": {
                        **truth.metadata,
                        "inherited_from_run_id": previous_run.run_id,
                    },
                }
            )
        )
    return inherited


def _build_delta_notes(
    *,
    snapshots: list[AuditSourceSnapshot],
    claims: list[AuditClaimEntry],
    truths: list[TruthLedgerEntry],
) -> list[str]:
    changed_snapshots = sum(1 for snapshot in snapshots if snapshot.metadata.get("delta_status") == "changed")
    unchanged_snapshots = sum(1 for snapshot in snapshots if snapshot.metadata.get("delta_status") == "unchanged")
    changed_claims = sum(1 for claim in claims if claim.metadata.get("delta_status") in {"added", "changed"})
    semantic_claims = sum(1 for claim in claims if claim.metadata.get("delta_change_type") == "semantic")
    textual_claims = sum(1 for claim in claims if claim.metadata.get("delta_change_type") == "textual_only")
    inherited_truths = sum(1 for truth in truths if truth.source_kind in {"user_specification", "user_acceptance"})
    notes = [
        f"Delta-Abgleich: {changed_snapshots} Snapshots geaendert, {unchanged_snapshots} Snapshots unveraendert.",
        f"Claim-Delta: {changed_claims} Claims sind neu oder geaendert, davon {semantic_claims} semantisch und {textual_claims} nur textuell.",
    ]
    if inherited_truths:
        notes.append(f"{inherited_truths} aktive Wahrheiten aus dem letzten abgeschlossenen Lauf wurden erneut beruecksichtigt.")
    return notes


def _claim_identity(claim: AuditClaimEntry) -> tuple[str, str, str, str]:
    return (
        claim.source_type,
        claim.source_id,
        claim.subject_key,
        claim.predicate,
    )


def _derive_impacted_scope_keys(
    *,
    claims: list[AuditClaimEntry],
    inherited_truths: list[TruthLedgerEntry],
) -> set[str]:
    changed_scope_keys = {
        scope_key
        for claim in claims
        if claim.metadata.get("delta_status") in {"added", "changed"}
        for scope_key in _claim_scope_keys_for_delta(claim=claim)
    }
    impacted = set(changed_scope_keys)
    for truth in inherited_truths:
        if truth.truth_status != "active":
            continue
        truth_scope_key = package_scope_key(truth.subject_key)
        if any(_scope_keys_overlap(left=truth_scope_key, right=changed_scope_key) for changed_scope_key in changed_scope_keys):
            impacted.add(truth_scope_key)
    expanded = _expand_impacted_scope_keys_transitively(claims=claims, seed_scope_keys=impacted)
    return {scope_key for scope_key in expanded if str(scope_key or "").strip()}


def _scope_keys_overlap(*, left: str, right: str) -> bool:
    normalized_left = str(left or "").strip()
    normalized_right = str(right or "").strip()
    if not normalized_left or not normalized_right:
        return False
    return (
        normalized_left == normalized_right
        or normalized_left.startswith(f"{normalized_right}.")
        or normalized_right.startswith(f"{normalized_left}.")
    )


def _claim_scope_keys_for_delta(*, claim: AuditClaimEntry) -> set[str]:
    semantic_scope_keys = {
        str(scope_key or "").strip()
        for scope_key in claim.metadata.get("semantic_cluster_keys", [])
        if str(scope_key or "").strip()
    }
    semantic_scope_keys.add(str(claim.metadata.get("delta_scope_key") or package_scope_key(claim.subject_key)))
    return semantic_scope_keys


def _expand_impacted_scope_keys_transitively(
    *,
    claims: list[AuditClaimEntry],
    seed_scope_keys: set[str],
) -> set[str]:
    normalized_seed_scope_keys = {
        str(scope_key or "").strip()
        for scope_key in seed_scope_keys
        if str(scope_key or "").strip()
    }
    if not normalized_seed_scope_keys:
        return set()
    adjacency: dict[str, set[str]] = defaultdict(set)
    for claim in claims:
        claim_scope_keys = _claim_scope_keys_for_delta(claim=claim)
        if len(claim_scope_keys) < 2:
            continue
        for left_scope_key in claim_scope_keys:
            for right_scope_key in claim_scope_keys:
                if left_scope_key == right_scope_key:
                    continue
                adjacency[left_scope_key].add(right_scope_key)

    expanded = set(normalized_seed_scope_keys)
    queue = list(normalized_seed_scope_keys)
    while queue:
        current_scope_key = queue.pop(0)
        for related_scope_key in adjacency.get(current_scope_key, set()):
            if related_scope_key in expanded:
                continue
            expanded.add(related_scope_key)
            queue.append(related_scope_key)
    return expanded


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]
