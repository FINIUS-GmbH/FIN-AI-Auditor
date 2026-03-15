from pathlib import Path

from fin_ai_auditor.config import Settings
from fin_ai_auditor.llm.embeddings import is_embedding_slot_configured, select_embedding_slot
from fin_ai_auditor.llm.slot_resolver import resolve_litellm_config_from_slot


def test_settings_reads_finai_llm_slot_configuration(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FINAI_LLM_1_PROVIDER", "openai")
    monkeypatch.setenv("FINAI_LLM_1_MODEL", "gpt-4o-mini")
    monkeypatch.setenv("FINAI_LLM_1_API_KEY", "secret-key")
    monkeypatch.setenv("FINAI_LLM_1_MAX_OUTPUT_TOKENS", "2048")

    settings = Settings(database_path=tmp_path / "auditor.db")
    slot = settings.get_llm_slot_config(1)

    assert slot is not None
    assert slot.provider == "openai"
    assert slot.model == "gpt-4o-mini"
    assert slot.api_key == "secret-key"
    assert slot.max_output_tokens == 2048


def test_slot_resolver_builds_azure_litellm_model(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FINAI_LLM_2_PROVIDER", "azure")
    monkeypatch.setenv("FINAI_LLM_2_MODEL", "gpt-4.1")
    monkeypatch.setenv("FINAI_LLM_2_DEPLOYMENT", "auditor-gpt4")
    monkeypatch.setenv("FINAI_LLM_2_ENDPOINT", "https://example.openai.azure.com/")
    monkeypatch.setenv("FINAI_LLM_2_API_KEY", "azure-secret")
    monkeypatch.setenv("FINAI_LLM_2_API_VERSION", "2024-10-21")

    settings = Settings(database_path=tmp_path / "auditor.db")
    resolved = resolve_litellm_config_from_slot(settings=settings, slot=2)

    assert resolved.provider == "azure"
    assert resolved.model == "azure/auditor-gpt4"
    assert resolved.extra_kwargs["api_base"] == "https://example.openai.azure.com"
    assert resolved.extra_kwargs["api_key"] == "azure-secret"
    assert resolved.extra_kwargs["api_version"] == "2024-10-21"


def test_embedding_slot_selection_prefers_explicit_embedding_slot(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FINAI_LLM_1_PROVIDER", "openai")
    monkeypatch.setenv("FINAI_LLM_1_MODEL", "gpt-4o-mini")
    monkeypatch.setenv("FINAI_LLM_1_API_KEY", "secret-key")
    monkeypatch.setenv("FINAI_LLM_2_PROVIDER", "openai")
    monkeypatch.setenv("FINAI_LLM_2_MODEL", "text-embedding-3-small")
    monkeypatch.setenv("FINAI_LLM_2_API_KEY", "secret-key")

    settings = Settings(database_path=tmp_path / "auditor.db")

    assert is_embedding_slot_configured(settings=settings, llm_slot=1) is False
    assert is_embedding_slot_configured(settings=settings, llm_slot=2) is True
    assert select_embedding_slot(settings=settings) == 2
