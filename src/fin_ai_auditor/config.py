from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AuditorLLMSlotConfig(BaseModel):
    slot: int
    provider: str
    model: str
    display_name: str | None = None
    endpoint: str | None = None
    api_key: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    api_version: str | None = None
    deployment: str | None = None
    temperature: float | None = None
    max_output_tokens: int | None = None
    context_window_tokens: int | None = None


class DirectMetaModelConfig(BaseModel):
    source: str
    uri: str
    username: str
    password: str
    database: str = "neo4j"


def _coerce_optional_str(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _coerce_optional_float(value: object) -> float | None:
    text = _coerce_optional_str(value)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _coerce_optional_int(value: object) -> int | None:
    text = _coerce_optional_str(value)
    if text is None:
        return None
    try:
        parsed = int(text)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _strip_env_value(raw: str) -> str:
    value = raw.strip()
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    return value


def _read_env_file_pairs(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    pairs: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key_text = key.strip()
        if not key_text:
            continue
        pairs[key_text] = _strip_env_value(raw_value)
    return pairs


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="FIN_AI_AUDITOR_",
        extra="ignore",
    )

    app_name: str = "FIN-AI Auditor"
    env: str = "dev"
    host: str = "127.0.0.1"
    port: int = 8088
    storage_dir: Path = Field(default=Path("./data"))
    database_path: Path = Field(default=Path("./data/fin_ai_auditor.db"))
    metamodel_dump_path: Path = Field(default=Path("./data/metamodel/current_dump.json"))
    cors_origins: list[str] = Field(default_factory=lambda: ["http://127.0.0.1:5174", "http://localhost:5174", "http://127.0.0.1:8080", "http://localhost:8080", "http://0.0.0.0:8080"])
    default_finai_local_repo_path: Path = Field(default=Path("/Users/martinwaelter/GitHub/FIN-AI"))
    default_finai_github_repo_url: str = "https://github.com/FINIUS-GmbH/FIN-AI.git"
    default_finai_github_ref: str = "main"
    fixed_confluence_space_key: str = "FP"
    fixed_jira_project_key: str = "FINAI"
    confluence_home_url: str = "https://fin-ai.atlassian.net/wiki/spaces/FP/overview"
    jira_board_url: str = "https://finius.atlassian.net/jira/software/projects/FINAI/boards/67"
    external_resource_access_mode: str = "read_only"
    external_write_requires_user_decision: bool = True
    local_database_is_only_writable_store: bool = True
    github_token: str | None = None
    atlassian_enabled: bool = True
    atlassian_oauth_client_id: str | None = None
    atlassian_oauth_client_secret: str | None = None
    atlassian_oauth_redirect_uri: str | None = None
    atlassian_oauth_scope: str | None = None
    confluence_base_url: str | None = None
    confluence_client_id: str | None = None
    confluence_client_secret: str | None = None
    jira_base_url: str | None = None
    jira_client_id: str | None = None
    jira_client_secret: str | None = None
    mothership_url: str | None = None
    license_key: str | None = None
    license_tenant: str | None = None
    metamodel_base_url: str | None = None
    metamodel_token: str | None = None

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, value: object) -> object:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    def get_llm_slot_config(self, slot: int) -> AuditorLLMSlotConfig | None:
        env_map = self._collect_external_env_map()
        prefix = f"FINAI_LLM_{int(slot)}_"
        legacy_prefix = f"LLM_{{name}}_{int(slot)}"

        def get_value(name: str) -> str | None:
            return _coerce_optional_str(
                env_map.get(f"{prefix}{name}") or env_map.get(legacy_prefix.format(name=name))
            )

        provider = get_value("PROVIDER")
        model = get_value("MODEL")
        deployment = get_value("DEPLOYMENT")
        api_key = get_value("API_KEY")
        endpoint = get_value("ENDPOINT")
        if provider is None and model is None and deployment is None:
            return None
        return AuditorLLMSlotConfig(
            slot=int(slot),
            provider=provider or "openai",
            model=model or deployment or "gpt-4o-mini",
            display_name=get_value("DISPLAYNAME"),
            endpoint=endpoint,
            api_key=api_key,
            client_id=get_value("CLIENT_ID"),
            client_secret=get_value("CLIENT_SECRET"),
            api_version=get_value("API_VERSION"),
            deployment=deployment,
            temperature=_coerce_optional_float(get_value("TEMPERATURE")),
            max_output_tokens=_coerce_optional_int(
                get_value("MAX_OUTPUT_TOKENS") or get_value("MAX_TOKENS")
            ),
            context_window_tokens=_coerce_optional_int(
                get_value("CONTEXT_WINDOW_TOKENS") or get_value("CONTEXT_WINDOW")
            ),
        )

    def get_configured_llm_slots(self) -> list[AuditorLLMSlotConfig]:
        slots: list[AuditorLLMSlotConfig] = []
        for slot in range(1, 10):
            config = self.get_llm_slot_config(slot)
            if config is not None:
                slots.append(config)
        return slots

    def get_direct_metamodel_config(self) -> DirectMetaModelConfig | None:
        env_map = self._collect_external_env_map()

        def get_value(*keys: str) -> str | None:
            for key in keys:
                value = _coerce_optional_str(env_map.get(key))
                if value:
                    return value
            return None

        source = get_value("FINAI_META_SOURCE", "FIN_AI_AUDITOR_META_SOURCE")
        if str(source or "").strip().upper() != "DIRECT":
            return None
        uri = get_value("FINAI_META_MODEL_URI", "FIN_AI_AUDITOR_META_MODEL_URI")
        username = get_value("FINAI_META_MODEL_USERNAME", "FIN_AI_AUDITOR_META_MODEL_USERNAME")
        password = get_value("FINAI_META_MODEL_PASSWORD", "FIN_AI_AUDITOR_META_MODEL_PASSWORD")
        database = get_value("FINAI_META_MODEL_DATABASE", "FIN_AI_AUDITOR_META_MODEL_DATABASE") or "neo4j"
        if not uri or not username or not password:
            return None
        return DirectMetaModelConfig(
            source="DIRECT",
            uri=uri,
            username=username,
            password=password,
            database=database,
        )

    def _collect_external_env_map(self) -> dict[str, str]:
        env_file_value = self.model_config.get("env_file")
        env_pairs: dict[str, str] = {}
        if isinstance(env_file_value, str):
            env_pairs.update(_read_env_file_pairs(Path(env_file_value)))
        elif isinstance(env_file_value, (list, tuple)):
            for entry in env_file_value:
                env_pairs.update(_read_env_file_pairs(Path(str(entry))))
        env_pairs.update({key: value for key, value in os.environ.items() if value is not None})
        return env_pairs


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.storage_dir.mkdir(parents=True, exist_ok=True)
    settings.metamodel_dump_path.parent.mkdir(parents=True, exist_ok=True)
    return settings
