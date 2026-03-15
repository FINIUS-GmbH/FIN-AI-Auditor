"""LLM-powered recommendation engine with chunked processing and full context.

Architecture:
 - Context Enrichment: Full repo, metamodel, and confluence context in every call
 - Chunked Processing: Findings split into chunks of 4 for reliable processing
 - Progress Callbacks: Each chunk reports progress back to the pipeline
 - Timeout Protection: 90s per chunk to prevent hanging
 - Quality First: Every finding gets full context for max detection quality
"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Callable

from pydantic import BaseModel, Field

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import AuditFinding, TruthLedgerEntry
from fin_ai_auditor.llm import ChatMessage, GenerationConfig, LiteLLMClient

logger = logging.getLogger(__name__)

CHUNK_SIZE = 4  # Findings pro LLM-Call — klein genug fuer zuverlaessige Antworten
LLM_CACHE_SCHEMA_VERSION = 2


class _RecommendedFindingItem(BaseModel):
    finding_key: str
    recommendation: str = Field(min_length=1)
    semantic_delta_note: str = Field(min_length=1)


class _RecommendationBatch(BaseModel):
    items: list[_RecommendedFindingItem] = Field(default_factory=list)
    global_delta_notes: list[str] = Field(default_factory=list)


class RecommendationEngine:
    def __init__(self, *, settings: Settings, allow_remote_calls: bool, db_path: str | None = None) -> None:
        self._settings = settings
        self._allow_remote_calls = allow_remote_calls
        self._db_path = db_path

    @property
    def allow_remote_calls(self) -> bool:
        return self._allow_remote_calls

    async def enrich_findings(
        self,
        *,
        findings: list[AuditFinding],
        truths: list[TruthLedgerEntry],
        retrieved_contexts: dict[str, list[str]] | None = None,
        repo_context: str = "",
        metamodel_context: str = "",
        confluence_context: str = "",
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> tuple[list[AuditFinding], list[str], dict]:
        """Returns (enriched_findings, notes, usage_report).

        usage_report has shape:
        {
            "by_model": {"<model>": {"calls": int, "prompt_tokens": int, "completion_tokens": int, "total_cost_eur": float}},
            "total_prompt_tokens": int, "total_completion_tokens": int,
            "total_cost_usd": float, "total_cost_eur": float,
        }
        """
        usage: dict = {"by_model": {}, "total_prompt_tokens": 0, "total_completion_tokens": 0,
                       "total_cost_usd": 0.0, "total_cost_eur": 0.0}
        if not findings:
            return findings, ["Keine Findings fuer die LLM-Empfehlung vorhanden."], usage
        if not self._allow_remote_calls:
            return findings, ["LLM-Empfehlungen wurden im aktuellen Modus bewusst nicht remote ausgefuehrt."], usage

        slot = _select_recommendation_slot(settings=self._settings)
        if slot is None:
            return findings, ["Kein geeigneter LLM-Slot fuer Empfehlungsgenerierung konfiguriert."], usage

        client = LiteLLMClient(settings=self._settings, default_slot=slot)

        # Split into chunks for reliable processing
        chunks = [findings[i:i + CHUNK_SIZE] for i in range(0, len(findings), CHUNK_SIZE)]
        total_chunks = len(chunks)
        all_enriched: list[AuditFinding] = []
        all_notes: list[str] = []

        logger.info(
            "llm_enrichment_started",
            extra={
                "event_name": "llm_enrichment_started",
                "event_payload": {"total_findings": len(findings), "total_chunks": total_chunks, "chunk_size": CHUNK_SIZE},
            },
        )

        for chunk_idx, chunk in enumerate(chunks):
            try:
                enriched_chunk, chunk_notes, chunk_usage = await self._enrich_chunk(
                    client=client,
                    slot=slot,
                    findings=chunk,
                    truths=truths,
                    retrieved_contexts=retrieved_contexts or {},
                    repo_context=repo_context,
                    metamodel_context=metamodel_context,
                    confluence_context=confluence_context,
                )
                all_enriched.extend(enriched_chunk)
                all_notes.extend(chunk_notes)
                _merge_usage(usage, chunk_usage)
            except Exception as exc:
                logger.warning(
                    "llm_chunk_failed",
                    extra={"event_name": "llm_chunk_failed", "event_payload": {"chunk": chunk_idx + 1, "error": str(exc)}},
                )
                # Keep deterministic findings on failure
                all_enriched.extend(chunk)
                all_notes.append(
                    f"LLM-Chunk {chunk_idx + 1}/{total_chunks} fehlgeschlagen: {exc}. "
                    f"Deterministische Empfehlungen wurden beibehalten."
                )

            if progress_callback:
                progress_callback(chunk_idx + 1, total_chunks)

        if not all_notes:
            all_notes = [
                f"LLM-Empfehlungen wurden erfolgreich in {total_chunks} Chunks "
                f"fuer {len(findings)} Findings generiert."
            ]

        return all_enriched, all_notes, usage

    async def _enrich_chunk(
        self,
        *,
        client: LiteLLMClient,
        slot: int,
        findings: list[AuditFinding],
        truths: list[TruthLedgerEntry],
        retrieved_contexts: dict[str, list[str]],
        repo_context: str,
        metamodel_context: str,
        confluence_context: str,
    ) -> tuple[list[AuditFinding], list[str], dict]:
        """Enriches a single chunk of findings via LLM, with response caching.

        Returns (enriched_findings, notes, usage_dict).
        """
        from pathlib import Path

        chunk_usage: dict = {}

        # Check LLM response cache
        cache_key = _build_chunk_cache_key(
            slot=slot,
            findings=findings,
            truths=truths,
            retrieved_contexts=retrieved_contexts,
            repo_context=repo_context,
            metamodel_context=metamodel_context,
            confluence_context=confluence_context,
        )

        batch: _RecommendationBatch | None = None

        if self._db_path:
            from fin_ai_auditor.services.pipeline_cache_service import PipelineCacheService
            cache_svc = PipelineCacheService(db_path=Path(self._db_path))
            cached = cache_svc.get_llm_response(cache_key=cache_key)
            if cached:
                try:
                    batch = _RecommendationBatch.model_validate_json(cached)
                except Exception:
                    batch = None

        if batch is None:
            batch, response = await client.structured_output_with_usage(
                messages=_build_enriched_messages(
                    findings=findings,
                    truths=truths,
                    retrieved_contexts=retrieved_contexts,
                    repo_context=repo_context,
                    metamodel_context=metamodel_context,
                    confluence_context=confluence_context,
                ),
                schema=_RecommendationBatch,
                config=GenerationConfig(slot=slot, max_tokens=2400, temperature=0.1, timeout_s=90.0),
            )
            # Track usage from this call
            chunk_usage = _extract_usage_from_response(response)

            # Cache the response
            if self._db_path:
                from fin_ai_auditor.services.pipeline_cache_service import PipelineCacheService
                cache_svc = PipelineCacheService(db_path=Path(self._db_path))
                cache_svc.set_llm_response(cache_key=cache_key, response_json=batch.model_dump_json())

        recommendations = {item.finding_key: item for item in batch.items}
        enriched: list[AuditFinding] = []
        for finding in findings:
            key = _finding_key(finding)
            recommendation = recommendations.get(key)
            if recommendation is None:
                enriched.append(finding)
                continue
            enriched.append(
                finding.model_copy(
                    update={
                        "recommendation": recommendation.recommendation,
                        "metadata": {
                            **finding.metadata,
                            "semantic_delta_note": recommendation.semantic_delta_note,
                            "recommended_by": f"litellm_slot_{slot}",
                        },
                    }
                )
            )
        return enriched, list(batch.global_delta_notes), chunk_usage


def _select_recommendation_slot(*, settings: Settings) -> int | None:
    for slot in settings.get_configured_llm_slots():
        model_hint = f"{slot.model} {slot.deployment or ''}".casefold()
        if "embedding" in model_hint or "document-ai" in model_hint or "ocr" in model_hint:
            continue
        return int(slot.slot)
    return None


USD_TO_EUR = 0.92  # Approximate conversion rate


def _extract_usage_from_response(response: object) -> dict:
    """Extract token usage and costs from an LLM response."""
    usage = getattr(response, "usage", {}) or {}
    if hasattr(usage, "model_dump"):
        usage = usage.model_dump()
    elif not isinstance(usage, dict):
        usage = {}
    model = getattr(response, "model", "unknown") or "unknown"
    pt = int(usage.get("prompt_tokens") or 0)
    ct = int(usage.get("completion_tokens") or 0)

    # Try litellm cost calculation
    cost_usd = 0.0
    try:
        from litellm import cost_per_token
        input_cpt, output_cpt = cost_per_token(model=model, prompt_tokens=1, completion_tokens=1)
        cost_usd = float(input_cpt) * pt + float(output_cpt) * ct
    except Exception:
        try:
            bare = model.split("/", 1)[-1] if "/" in model else model
            from litellm import cost_per_token
            input_cpt, output_cpt = cost_per_token(model=bare, prompt_tokens=1, completion_tokens=1)
            cost_usd = float(input_cpt) * pt + float(output_cpt) * ct
        except Exception:
            pass

    return {
        "model": model,
        "prompt_tokens": pt,
        "completion_tokens": ct,
        "cost_usd": cost_usd,
        "cost_eur": cost_usd * USD_TO_EUR,
    }


def _merge_usage(total: dict, chunk: dict) -> None:
    """Merge chunk usage into the total usage report."""
    if not chunk:
        return
    model = chunk.get("model", "unknown")
    pt = chunk.get("prompt_tokens", 0)
    ct = chunk.get("completion_tokens", 0)
    cost_usd = chunk.get("cost_usd", 0.0)
    cost_eur = chunk.get("cost_eur", 0.0)

    total["total_prompt_tokens"] += pt
    total["total_completion_tokens"] += ct
    total["total_cost_usd"] += cost_usd
    total["total_cost_eur"] += cost_eur

    by_model = total.setdefault("by_model", {})
    prev = by_model.get(model, {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0,
                                 "cost_usd": 0.0, "cost_eur": 0.0})
    by_model[model] = {
        "calls": prev["calls"] + 1,
        "prompt_tokens": prev["prompt_tokens"] + pt,
        "completion_tokens": prev["completion_tokens"] + ct,
        "cost_usd": prev["cost_usd"] + cost_usd,
        "cost_eur": prev["cost_eur"] + cost_eur,
    }


def _build_enriched_messages(
    *,
    findings: list[AuditFinding],
    truths: list[TruthLedgerEntry],
    retrieved_contexts: dict[str, list[str]],
    repo_context: str,
    metamodel_context: str,
    confluence_context: str,
) -> list[ChatMessage]:
    active_truths = [truth for truth in truths if truth.truth_status == "active"][:8]
    finding_lines = []
    for finding in findings:
        evidence = "; ".join(
            location.position.anchor_value if location.position else location.title
            for location in finding.locations[:3]
        )
        retrieval_context = " || ".join(retrieved_contexts.get(_finding_key(finding), [])[:2])
        semantic_context = " || ".join(_string_list(finding.metadata.get("semantic_context"))[:4])
        semantic_relations = " || ".join(_string_list(finding.metadata.get("semantic_relation_summaries"))[:4])
        semantic_contract_paths = " || ".join(_string_list(finding.metadata.get("semantic_contract_paths"))[:3])
        section_paths = " || ".join(_string_list(finding.metadata.get("semantic_section_paths"))[:3])
        finding_lines.append(
            f"- key={_finding_key(finding)} | title={finding.title} | category={finding.category} | "
            f"severity={finding.severity} | summary={finding.summary} | current_recommendation={finding.recommendation} | "
            f"evidence={evidence} | retrieval_context={retrieval_context or '-'} | "
            f"semantic_context={semantic_context or '-'} | semantic_relations={semantic_relations or '-'} | "
            f"semantic_contract_paths={semantic_contract_paths or '-'} | "
            f"section_paths={section_paths or '-'}"
        )
    truth_lines = [
        f"- key={truth.canonical_key} | subject={truth.subject_key} | value={truth.normalized_value}"
        for truth in active_truths
    ]

    # Build context sections — only include non-empty
    context_sections: list[str] = []
    if repo_context and repo_context != "Keine Code-Quellen verfuegbar.":
        context_sections.append(f"KONTEXT — FIN-AI Repository:\n{repo_context}")
    if metamodel_context and metamodel_context != "Kein Metamodell verfuegbar.":
        context_sections.append(f"KONTEXT — BSM Metamodell:\n{metamodel_context}")
    if confluence_context and confluence_context != "Keine Confluence-Seiten verfuegbar.":
        context_sections.append(f"KONTEXT — Confluence-Dokumentation:\n{confluence_context}")

    context_block = "\n\n---\n\n".join(context_sections) + "\n\n---\n\n" if context_sections else ""

    user_prompt = (
        f"{context_block}"
        "Leite fuer jedes Finding eine knappe, pruefbare Empfehlung und eine semantische Delta-Notiz ab. "
        "Die Delta-Notiz soll sagen, welche zuvor gespeicherten Wahrheiten, Dokumente oder Code-Cluster bei einer "
        "Neugenerierung besonders erneut bewertet werden muessen.\n\n"
        "Nutze den Kontext ueber die Repository-Struktur, das Metamodell und die Confluence-Seiten, "
        "um die Findings in ihrem fachlichen Zusammenhang zu bewerten. "
        "Identifiziere insbesondere Widersprueche zwischen Doku und Code, fehlende Definitionen, "
        "und Abweichungen vom Metamodell.\n\n"
        "Findings:\n"
        f"{chr(10).join(finding_lines)}\n\n"
        "Aktive Wahrheiten:\n"
        f"{chr(10).join(truth_lines) if truth_lines else '- keine'}"
    )
    return [
        ChatMessage(
            role="system",
            content=(
                "Du bist ein Governance-Auditor fuer das FIN-AI Softwaresystem.\n\n"
                "=== Was ist FIN-AI? ===\n"
                "FIN-AI ist eine Enterprise-Plattform fuer KI-gestuetzte Geschaeftsanalyse im regulierten "
                "Finanzumfeld. Der Kern ist das BSM-Cockpit (Business Service Management), das fachliche "
                "Fragestellungen (bsmQuestion) strukturiert bearbeitet und in pruefbare Evidenzartefakte "
                "(Statement, BSM_Element) ueberfuehrt.\n\n"
                "Zentrale Domaenenkonzepte:\n"
                "- Evidenzkette: bsmAnswer -> summarisedAnswerUnit -> Statement -> BSM_Element\n"
                "- Metamodell: Phasen, Fragen, Metaclasses, Relationen — definiert die fachliche Struktur\n"
                "- Graph-Datenbank: Labels, Properties, Kanten und Knoten in der Metamodell- und Kunden-DB "
                "sind die primaeren Quellen fuer Widersprueche, fehlende Elemente und Inkonsistenzen\n"
                "- HITL (Human-in-the-Loop): Review/Accept/Reject-Zyklen fuer Statements und BSM_Elements\n"
                "- Run-Modell: FINAI_AnalysisRun -> FINAI_PhaseRun -> FINAI_ChunkPhaseRun mit IN_RUN-Traceability\n"
                "- Status-Lifecycle: STAGED/PROPOSED -> VALIDATED/ACTIVE -> REJECTED/HISTORIC\n"
                "- Scope-Steuerung: project_id + phase_id + run_id als Artefaktkontext\n"
                "- Pipeline: PlantUML-definierte Verarbeitungsketten (AS-IS vs. v2 Zielbild)\n\n"
                "=== Specification Driven Development ===\n"
                "WICHTIG: FIN-AI folgt dem Prinzip Specification Driven Development.\n"
                "Die Dokumentation (Confluence, Architektur-Docs, Scope-Matrizen, PlantUML-Pipelines, "
                "Metamodell) ist die EINZIGE Single Source of Truth (SSOT).\n\n"
                "Die Dokumentation definiert das Zielbild und muss:\n"
                "  1. VOLLSTAENDIG sein — jede fachliche Anforderung muss dokumentiert sein\n"
                "  2. KONSISTENT sein — keine Widersprueche zwischen Dokumentquellen\n"
                "  3. KORREKT sein — Aussagen muessen fachlich stimmen\n\n"
                "Code, Implementierungen und technische Artefakte LEITEN SICH AUS der Dokumentation AB. "
                "Sie dienen in der Bewertung als INDIZ fuer den Umsetzungsstand, "
                "NICHT als Quelle fuer das Zielbild.\n\n"
                "Hierarchie der Quellen:\n"
                "  1. Explizit bestaetigte User-Wahrheiten (hoechste Prioritaet, uebersteuern alles)\n"
                "  2. Dokumentation (Confluence, Architektur-Docs, Metamodell) = SSOT / Zielbild\n"
                "  3. Code + technische Artefakte = Umsetzungsnachweise / Indizien\n\n"
                "Konsequenz fuer die Bewertung:\n"
                "- Wenn Doku und Code sich widersprechen: Die DOKU definiert das Soll. "
                "Der Code weicht ab und muss korrigiert werden (oder die Doku wird bewusst aktualisiert).\n"
                "- Wenn Doku-Quellen sich gegenseitig widersprechen: Benenne den Widerspruch, "
                "empfehle eine Klaerung und Konsolidierung der Dokumentation.\n"
                "- Wenn Code etwas implementiert, das nicht dokumentiert ist: "
                "Das ist eine Dokumentationsluecke, die geschlossen werden muss.\n"
                "- Wenn Doku etwas definiert, das nicht implementiert ist: "
                "Das ist ein Umsetzungsrueckstand, der als Finding erfasst wird.\n\n"
                "=== Deine Aufgabe ===\n"
                "Pruefe die Dokumentation auf Vollstaendigkeit, Konsistenz und Korrektheit. "
                "Nutze Code und technische Artefakte als Indizien fuer den Umsetzungsstand.\n\n"
                "Analysiere:\n"
                "1. Confluence-Dokumentation (Zielbilder, Scope-Matrizen, Run-SSOT-Dokumente)\n"
                "2. BSM-Metamodell (Neo4j, metamodel_export.json)\n"
                "3. Architektur-Docs (PlantUML-Pipelines, lokale Vertragsdoku)\n"
                "4. Code-Implementierung (Python, TypeScript) — NUR als Indiz\n\n"
                "Achte besonders auf:\n"
                "- Dokumentationsluecken: Fachliche Konzepte ohne vollstaendige Spezifikation\n"
                "- Dokumentationswidersprueche: Verschiedene Docs definieren verschiedene Zielzustaende\n"
                "- Status-Kanon-Inkonsistenzen zwischen Dokumentquellen\n"
                "- Entity-Rollen-Konflikte (z.B. summarisedAnswer: eigenstaendig vs. entfaellt)\n"
                "- HITL-Widersprueche (Statement-Review: zentral vs. ausgeschlossen im MVP)\n"
                "- Run-Modell-Inkonsistenzen in der Spezifikation\n"
                "- Fehlende Definitionen im Metamodell die in der Doku referenziert werden\n"
                "- Umsetzungsdrift: Stellen wo Code vom dokumentierten Zielbild abweicht (als Indiz)\n\n"
                "=== Haeufigkeit = Konfidenz ===\n"
                "Die Mehrheit der Dokumentquellen beschreibt, wie der BSM-Prozess gedacht ist. "
                "Wenn 5 Dokumentquellen sagen 'Status startet als PROPOSED' und 1 Quelle sagt 'STAGED', "
                "dann ist PROPOSED die wahrscheinlichere Zielaussage und STAGED die vermutlich veraltete Angabe. "
                "Benenne die Mehrheitsposition und markiere den Ausreisser als klaerungsbeduerftigen Dokumentationsfehler. "
                "Code dient anschliessend als Indiz, ob der dokumentierte Zielzustand bereits umgesetzt ist.\n\n"
                "Qualitaet geht vor Geschwindigkeit — jeder Fehler, jede Luecke und jeder Widerspruch "
                "muss gefunden werden. "
                "Erzeuge nur konkrete, pruefbare Handlungsempfehlungen. "
                "Kein Marketing, keine Allgemeinplaetze, keine unpruefbaren Aussagen. "
                "Priorisiere Empfehlungen zur Dokumentationskorrektur vor Code-Anpassungen. "
                "Nutze Heading-Hierarchien und semantische Relationen "
                "als relevanten Kontext, nicht nur flachen Text. Wenn eine Vertragskette wie "
                "phase -> question -> policy -> write_contract erkennbar ist, muss sie explizit "
                "konsistent in der Empfehlung behandelt werden. "
                "Antworte ausschliesslich als gueltiges JSON passend zum angeforderten Schema."
            ),
        ),
        ChatMessage(role="user", content=user_prompt),
    ]


def _finding_key(finding: AuditFinding) -> str:
    return str(finding.canonical_key or finding.finding_id)


def _build_chunk_cache_key(
    *,
    slot: int,
    findings: list[AuditFinding],
    truths: list[TruthLedgerEntry],
    retrieved_contexts: dict[str, list[str]],
    repo_context: str,
    metamodel_context: str,
    confluence_context: str,
) -> str:
    active_truths = [
        {
            "canonical_key": truth.canonical_key,
            "subject_key": truth.subject_key,
            "normalized_value": truth.normalized_value,
        }
        for truth in truths
        if truth.truth_status == "active"
    ][:8]
    payload = {
        "schema_version": LLM_CACHE_SCHEMA_VERSION,
        "slot": slot,
        "findings": [
            _finding_cache_payload(
                finding=finding,
                retrieval_contexts=retrieved_contexts.get(_finding_key(finding), []),
            )
            for finding in findings
        ],
        "truths": active_truths,
        "repo_context": repo_context,
        "metamodel_context": metamodel_context,
        "confluence_context": confluence_context,
    }
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:32]


def _finding_cache_payload(*, finding: AuditFinding, retrieval_contexts: list[str]) -> dict[str, object]:
    return {
        "key": _finding_key(finding),
        "title": finding.title,
        "category": finding.category,
        "severity": finding.severity,
        "summary": finding.summary,
        "recommendation": finding.recommendation,
        "locations": [
            {
                "source_type": location.source_type,
                "source_id": location.source_id,
                "title": location.title,
                "path_hint": location.path_hint,
                "url": location.url,
                "anchor_kind": location.position.anchor_kind if location.position else None,
                "anchor_value": location.position.anchor_value if location.position else None,
            }
            for location in finding.locations[:3]
        ],
        "retrieval_contexts": [str(item).strip() for item in retrieval_contexts[:2] if str(item).strip()],
        "semantic_context": _string_list(finding.metadata.get("semantic_context"))[:4],
        "semantic_relations": _string_list(finding.metadata.get("semantic_relation_summaries"))[:4],
        "semantic_contract_paths": _string_list(finding.metadata.get("semantic_contract_paths"))[:3],
        "section_paths": _string_list(finding.metadata.get("semantic_section_paths"))[:3],
    }


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]
