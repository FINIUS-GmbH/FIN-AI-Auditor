from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from fin_ai_auditor.config import AuditorLLMSlotConfig, Settings


@dataclass(frozen=True)
class ResolvedLiteLLMConfig:
    slot: int
    provider: str
    model: str
    temperature: float | None
    max_tokens: int | None
    extra_kwargs: dict[str, Any]


_TEMP_UNSUPPORTED_MODEL_KEYS: set[str] = set()
_KNOWN_NO_TEMPERATURE_PREFIXES: tuple[str, ...] = ("gpt-5",)
_MAX_TOKENS_MODEL_CAPS: dict[str, int] = {}
_MAX_TOKENS_RETRY_STEPS: tuple[int, ...] = (
    30000,
    24000,
    20000,
    16384,
    12000,
    8192,
    4096,
    2048,
    1024,
    512,
    256,
    128,
    64,
    32,
    16,
    8,
    4,
    2,
    1,
)
_MAX_TOKENS_TOKENS: tuple[str, ...] = (
    "max_tokens",
    "max tokens",
    "max_completion_tokens",
    "max completion tokens",
    "max_output_tokens",
    "max output tokens",
)
_MAX_TOKENS_LIMIT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        r"(?:max(?:_| )(?:completion(?:_| ))?tokens?|max(?:_| )output(?:_| )tokens?)"
        r".{0,120}?(?:<=|less than or equal to|at most|maximum(?: allowed)?(?: is|:)?|must be between\s+\d+\s+and)"
        r"\s*(\d+)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:max(?:_| )(?:completion(?:_| ))?tokens?|max(?:_| )output(?:_| )tokens?)"
        r".{0,120}?between\s+1\s+and\s+(\d+)",
        re.IGNORECASE,
    ),
)


def _normalize_model_key(model: str) -> str:
    raw = str(model or "").strip().lower()
    if "/" in raw:
        raw = raw.split("/", 1)[1]
    return raw


def _coerce_positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return int(value) if int(value) > 0 else None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, float):
        parsed = int(value)
        return parsed if parsed > 0 else None
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def model_supports_temperature(*, model: str) -> bool:
    key = _normalize_model_key(model)
    if not key:
        return True
    if key in _TEMP_UNSUPPORTED_MODEL_KEYS:
        return False
    return not any(key.startswith(prefix) for prefix in _KNOWN_NO_TEMPERATURE_PREFIXES)


def mark_model_temperature_unsupported(*, model: str) -> None:
    key = _normalize_model_key(model)
    if key:
        _TEMP_UNSUPPORTED_MODEL_KEYS.add(key)


def mark_model_max_tokens_cap(*, model: str, max_tokens: int) -> None:
    key = _normalize_model_key(model)
    cap = _coerce_positive_int(max_tokens)
    if not key or cap is None:
        return
    current = _MAX_TOKENS_MODEL_CAPS.get(key)
    if current is None or cap < current:
        _MAX_TOKENS_MODEL_CAPS[key] = int(cap)


def resolve_provider_safe_max_tokens(*, model: str, max_tokens: int | None) -> int | None:
    resolved = _coerce_positive_int(max_tokens)
    if resolved is None:
        return None
    resolved = min(int(resolved), 30000)
    discovered_cap = _MAX_TOKENS_MODEL_CAPS.get(_normalize_model_key(model))
    if discovered_cap is not None:
        resolved = min(int(resolved), int(discovered_cap))
    return int(resolved)


def iter_lower_max_tokens_retry_values(*, model: str, max_tokens: int | None) -> tuple[int, ...]:
    start = resolve_provider_safe_max_tokens(model=model, max_tokens=max_tokens)
    if start is None:
        return ()
    out: list[int] = [int(start)]
    seen = {int(start)}
    discovered_cap = resolve_provider_safe_max_tokens(
        model=model,
        max_tokens=_MAX_TOKENS_MODEL_CAPS.get(_normalize_model_key(model)),
    )
    if discovered_cap is not None and discovered_cap < start and discovered_cap not in seen:
        out.append(int(discovered_cap))
        seen.add(int(discovered_cap))
    for candidate in _MAX_TOKENS_RETRY_STEPS:
        if candidate >= start or candidate in seen:
            continue
        out.append(int(candidate))
        seen.add(int(candidate))
    return tuple(out)


def temperature_error_requires_retry_without_parameter(*, exc: Exception) -> bool:
    msg = str(exc or "").strip().lower()
    if not msg:
        return False
    return any(
        token in msg
        for token in (
            "temperature",
            "unsupportedparamserror",
            "unsupported parameter",
            "unsupported params",
            "does not support",
            "not supported",
            "invalid param",
            "invalid parameter",
            "extra_forbidden",
        )
    )


def max_tokens_error_requires_retry_with_lower_value(*, exc: Exception) -> bool:
    msg = str(exc or "").strip().lower()
    if not msg:
        return False
    if any(token in msg for token in ("context length", "maximum context", "context window")):
        return False
    if not any(token in msg for token in _MAX_TOKENS_TOKENS):
        return False
    if any(token in msg for token in ("unsupported parameter", "unsupported params", "extra_forbidden")):
        return False
    return any(
        token in msg
        for token in (
            "less than or equal",
            "must be between",
            "between 1 and",
            "at most",
            "out of range",
            "invalid value",
            "too large",
            "too high",
            "greater than maximum",
            "exceeds the maximum",
            "over the limit",
            "must be <=",
        )
    )


def _extract_max_tokens_limit_from_error(*, exc: Exception) -> int | None:
    message = str(exc or "").strip()
    if not message:
        return None
    lowered = message.lower()
    if not any(token in lowered for token in _MAX_TOKENS_TOKENS):
        return None
    for pattern in _MAX_TOKENS_LIMIT_PATTERNS:
        match = pattern.search(message)
        if match is None:
            continue
        limit = _coerce_positive_int(match.group(1))
        if limit is not None:
            return int(limit)
    return None


def next_lower_max_tokens_for_error(*, model: str, current_max_tokens: int | None, exc: Exception) -> int | None:
    current = resolve_provider_safe_max_tokens(model=model, max_tokens=current_max_tokens)
    if current is None or not max_tokens_error_requires_retry_with_lower_value(exc=exc):
        return None
    hinted_limit = resolve_provider_safe_max_tokens(
        model=model,
        max_tokens=_extract_max_tokens_limit_from_error(exc=exc),
    )
    if hinted_limit is not None and hinted_limit < current:
        return int(hinted_limit)
    for candidate in iter_lower_max_tokens_retry_values(model=model, max_tokens=current):
        if candidate < current:
            return int(candidate)
    return None


def _resolve_base_model(slot_config: AuditorLLMSlotConfig, model_override: str | None) -> str:
    configured = str(model_override or slot_config.model or "").strip()
    deployment = str(slot_config.deployment or "").strip()
    return configured or deployment or "gpt-4o-mini"


def resolve_litellm_config_from_slot(
    *,
    settings: Settings,
    slot: int,
    model_override: str | None = None,
    temperature_override: float | None = None,
    max_tokens_override: int | None = None,
) -> ResolvedLiteLLMConfig:
    slot_config = settings.get_llm_slot_config(int(slot))
    if slot_config is None:
        raise ValueError(f"LLM-Slot {int(slot)} ist im Auditor nicht konfiguriert.")

    provider = str(slot_config.provider or "openai").strip().lower()
    model_raw = _resolve_base_model(slot_config, model_override)
    temperature = temperature_override if temperature_override is not None else slot_config.temperature
    max_tokens = max_tokens_override if max_tokens_override is not None else slot_config.max_output_tokens
    if max_tokens is None:
        max_tokens = 4000

    extra: dict[str, Any] = {}
    endpoint = str(slot_config.endpoint or "").strip().rstrip("/")
    api_key = str(slot_config.api_key or "").strip()
    client_id = str(slot_config.client_id or "").strip()
    client_secret = str(slot_config.client_secret or "").strip()
    api_version = str(slot_config.api_version or "").strip()
    deployment = str(slot_config.deployment or "").strip()

    if provider in {"azure", "azure_openai", "azure_ai_foundry"}:
        endpoint_lower = endpoint.lower()
        is_azure_ai_services = (
            "services.ai.azure.com" in endpoint_lower
            or "inference.ai.azure.com" in endpoint_lower
            or "models.ai.azure.com" in endpoint_lower
        )
        model = f"azure_ai/{deployment or model_raw}" if is_azure_ai_services else f"azure/{deployment or model_raw}"
        if endpoint:
            extra["api_base"] = endpoint
        if api_key:
            extra["api_key"] = api_key
        if api_version and not is_azure_ai_services:
            extra["api_version"] = api_version
        if client_id:
            extra["client_id"] = client_id
        if client_secret:
            extra["client_secret"] = client_secret
    elif provider in {"ollama"}:
        bare_model = model_raw[len("ollama/") :] if model_raw.startswith("ollama/") else model_raw
        model = f"ollama/{bare_model}"
        extra["api_base"] = endpoint or "http://127.0.0.1:11434"
    else:
        model = model_raw
        if endpoint:
            extra["api_base"] = endpoint
        if api_key:
            extra["api_key"] = api_key
        if api_version:
            extra["api_version"] = api_version
        if client_id:
            extra["client_id"] = client_id
        if client_secret:
            extra["client_secret"] = client_secret

    return ResolvedLiteLLMConfig(
        slot=int(slot),
        provider=provider,
        model=model,
        temperature=temperature if model_supports_temperature(model=model) else None,
        max_tokens=resolve_provider_safe_max_tokens(model=model, max_tokens=max_tokens),
        extra_kwargs=extra,
    )
