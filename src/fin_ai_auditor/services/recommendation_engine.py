from __future__ import annotations

from pydantic import BaseModel, Field

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import AuditFinding, TruthLedgerEntry
from fin_ai_auditor.llm import ChatMessage, GenerationConfig, LiteLLMClient


class _RecommendedFindingItem(BaseModel):
    finding_key: str
    recommendation: str = Field(min_length=1)
    semantic_delta_note: str = Field(min_length=1)


class _RecommendationBatch(BaseModel):
    items: list[_RecommendedFindingItem] = Field(default_factory=list)
    global_delta_notes: list[str] = Field(default_factory=list)


class RecommendationEngine:
    def __init__(self, *, settings: Settings, allow_remote_calls: bool) -> None:
        self._settings = settings
        self._allow_remote_calls = allow_remote_calls

    @property
    def allow_remote_calls(self) -> bool:
        return self._allow_remote_calls

    async def enrich_findings(
        self,
        *,
        findings: list[AuditFinding],
        truths: list[TruthLedgerEntry],
        retrieved_contexts: dict[str, list[str]] | None = None,
    ) -> tuple[list[AuditFinding], list[str]]:
        if not findings:
            return findings, ["Keine Findings fuer die LLM-Empfehlung vorhanden."]
        if not self._allow_remote_calls:
            return findings, ["LLM-Empfehlungen wurden im aktuellen Modus bewusst nicht remote ausgefuehrt."]

        slot = _select_recommendation_slot(settings=self._settings)
        if slot is None:
            return findings, ["Kein geeigneter LLM-Slot fuer Empfehlungsgenerierung konfiguriert."]

        client = LiteLLMClient(settings=self._settings, default_slot=slot)
        try:
            batch = await client.structured_output(
                messages=_build_recommendation_messages(
                    findings=findings,
                    truths=truths,
                    retrieved_contexts=retrieved_contexts or {},
                ),
                schema=_RecommendationBatch,
                config=GenerationConfig(slot=slot, max_tokens=2400, temperature=0.1),
            )
        except Exception as exc:
            return findings, [f"LLM-Empfehlungen sind fehlgeschlagen; es bleibt bei den deterministischen Empfehlungen. Grund: {exc}"]

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
        notes = [*batch.global_delta_notes]
        if not notes:
            notes = ["LLM-Empfehlungen wurden erfolgreich aus den aktuellen Findings und Wahrheiten abgeleitet."]
        return enriched, notes


def _select_recommendation_slot(*, settings: Settings) -> int | None:
    for slot in settings.get_configured_llm_slots():
        model_hint = f"{slot.model} {slot.deployment or ''}".casefold()
        if "embedding" in model_hint or "document-ai" in model_hint or "ocr" in model_hint:
            continue
        return int(slot.slot)
    return None


def _build_recommendation_messages(
    *,
    findings: list[AuditFinding],
    truths: list[TruthLedgerEntry],
    retrieved_contexts: dict[str, list[str]],
) -> list[ChatMessage]:
    active_truths = [truth for truth in truths if truth.truth_status == "active"][:8]
    finding_lines = []
    for finding in findings[:8]:
        evidence = "; ".join(location.position.anchor_value if location.position else location.title for location in finding.locations[:2])
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
    user_prompt = (
        "Leite fuer jedes Finding eine knappe, pruefbare Empfehlung und eine semantische Delta-Notiz ab. "
        "Die Delta-Notiz soll sagen, welche zuvor gespeicherten Wahrheiten, Dokumente oder Code-Cluster bei einer "
        "Neugenerierung besonders erneut bewertet werden muessen.\n\n"
        "Findings:\n"
        f"{chr(10).join(finding_lines)}\n\n"
        "Aktive Wahrheiten:\n"
        f"{chr(10).join(truth_lines) if truth_lines else '- keine'}"
    )
    return [
        ChatMessage(
            role="system",
            content=(
                "Du erzeugst fuer einen Governance-Auditor nur konkrete, pruefbare Handlungsempfehlungen. "
                "Kein Marketing, keine Allgemeinplaetze, keine unpruefbaren Aussagen. "
                "Bevorzuge Aussagen ueber Dokumentaenderungen, Codeanpassungen, Delta-Neubewertung, Traceability "
                "und explizite Objekt-/Prozessbeziehungen. Nutze Heading-Hierarchien und semantische Relationen "
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


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]
