from __future__ import annotations

from typing import Any

from fin_ai_auditor.config import Settings
from fin_ai_auditor.llm.slot_resolver import resolve_litellm_config_from_slot


def _looks_like_embedding_model(*, name: str) -> bool:
    return "embedding" in str(name or "").strip().lower()


def resolve_default_embedding_model_for_slot(*, settings: Settings, llm_slot: int) -> str:
    slot_config = settings.get_llm_slot_config(int(llm_slot))
    if slot_config is None:
        raise ValueError(f"LLM-Slot {int(llm_slot)} ist fuer Embeddings nicht konfiguriert.")
    deployment = str(slot_config.deployment or "").strip()
    model = str(slot_config.model or "").strip()
    if _looks_like_embedding_model(name=deployment):
        return deployment
    if _looks_like_embedding_model(name=model):
        return model
    return "text-embedding-3-small"


class LiteLLMEmbeddingClient:
    def __init__(
        self,
        *,
        model: str,
        api_base: str | None = None,
        api_key: str | None = None,
        api_version: str | None = None,
    ) -> None:
        self.model = str(model)
        self.api_base = api_base
        self.api_key = api_key
        self.api_version = api_version

    def _build_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if self.api_base:
            kwargs["api_base"] = self.api_base
        if self.api_key:
            kwargs["api_key"] = self.api_key
        if self.api_version:
            kwargs["api_version"] = self.api_version
        return kwargs

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        from litellm import embedding

        clean = [text if isinstance(text, str) and text.strip() else " " for text in texts]
        response = embedding(model=self.model, input=clean, **self._build_kwargs())
        data = response.get("data") if isinstance(response, dict) else getattr(response, "data", [])
        return [list(item.get("embedding") or []) for item in (data or []) if isinstance(item, dict)]

    def embed_query(self, text: str) -> list[float]:
        vectors = self.embed_documents([text])
        return vectors[0] if vectors else []


def get_embeddings_from_llm_slot(
    *,
    settings: Settings,
    llm_slot: int,
    embedding_model: str | None = None,
) -> LiteLLMEmbeddingClient:
    resolved = resolve_litellm_config_from_slot(settings=settings, slot=int(llm_slot))
    model = embedding_model or resolve_default_embedding_model_for_slot(settings=settings, llm_slot=int(llm_slot))
    provider = str(resolved.provider).lower()
    if provider in {"azure", "azure_openai", "azure_ai_foundry"} and not str(model).startswith("azure/"):
        model = f"azure/{model}"
    return LiteLLMEmbeddingClient(
        model=str(model),
        api_base=str(resolved.extra_kwargs.get("api_base") or "") or None,
        api_key=str(resolved.extra_kwargs.get("api_key") or "") or None,
        api_version=str(resolved.extra_kwargs.get("api_version") or "") or None,
    )
