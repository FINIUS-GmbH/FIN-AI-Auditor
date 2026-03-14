from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from typing import Any

from pydantic import BaseModel

from fin_ai_auditor.config import Settings
from fin_ai_auditor.llm.interfaces import LLMProviderInterface
from fin_ai_auditor.llm.slot_resolver import (
    mark_model_max_tokens_cap,
    mark_model_temperature_unsupported,
    next_lower_max_tokens_for_error,
    resolve_litellm_config_from_slot,
    temperature_error_requires_retry_without_parameter,
)
from fin_ai_auditor.llm.types import ChatMessage, GenerationConfig, LLMResponse, ToolCall, ToolSpec


def _safe_get(raw: object, key: str, default: object = None) -> object:
    if isinstance(raw, dict):
        return raw.get(key, default)
    return getattr(raw, key, default)


def _strip_markdown_code_fences(text: str) -> str:
    trimmed = str(text or "").strip()
    if not trimmed.startswith("```"):
        return trimmed
    parts = trimmed.split("```")
    inner = (parts[1] if len(parts) >= 2 else trimmed).strip()
    if "\n" in inner:
        first_line, rest = inner.split("\n", 1)
        if first_line.strip().lower() in {"json", "jsonc", "application/json"}:
            inner = rest.strip()
    return inner.strip()


def _extract_first_json_block(text: str) -> str | None:
    source = str(text or "").strip()
    if not source:
        return None
    start_candidates = [(source.find("{"), "{", "}"), (source.find("["), "[", "]")]
    start_candidates = [(index, open_ch, close_ch) for index, open_ch, close_ch in start_candidates if index != -1]
    if not start_candidates:
        return None
    start, open_ch, close_ch = min(start_candidates, key=lambda item: item[0])

    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(source)):
        char = source[index]
        if in_string:
            if escaped:
                escaped = False
                continue
            if char == "\\":
                escaped = True
                continue
            if char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char == open_ch:
            depth += 1
            continue
        if char == close_ch:
            depth -= 1
            if depth == 0:
                return source[start : index + 1].strip()
    return None


def _json_loads_or_empty(raw: object) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_tool_calls(raw_tool_calls: object) -> list[ToolCall]:
    if not isinstance(raw_tool_calls, list):
        return []
    parsed: list[ToolCall] = []
    for raw_call in raw_tool_calls:
        function = _safe_get(raw_call, "function", {})
        raw_name = _safe_get(function, "name", "") or _safe_get(raw_call, "name", "")
        if not raw_name:
            continue
        parsed.append(
            ToolCall(
                id=str(_safe_get(raw_call, "id", "tool_call")),
                name=str(raw_name),
                arguments=_json_loads_or_empty(_safe_get(function, "arguments", {})),
            )
        )
    return parsed


def _parse_usage(raw_response: object) -> dict[str, Any]:
    usage = _safe_get(raw_response, "usage", {})
    if isinstance(usage, dict):
        return {str(key): value for key, value in usage.items()}
    if usage is None:
        return {}
    if hasattr(usage, "model_dump"):
        return usage.model_dump()
    return {
        "prompt_tokens": _safe_get(usage, "prompt_tokens", None),
        "completion_tokens": _safe_get(usage, "completion_tokens", None),
        "total_tokens": _safe_get(usage, "total_tokens", None),
    }


def _parse_completion_response(*, raw_response: object, model: str, provider: str) -> LLMResponse:
    choices = _safe_get(raw_response, "choices", [])
    first_choice = choices[0] if isinstance(choices, list) and choices else {}
    message = _safe_get(first_choice, "message", {}) or {}
    content = _safe_get(message, "content", "") or ""
    return LLMResponse(
        content=str(content),
        tool_calls=_parse_tool_calls(_safe_get(message, "tool_calls", [])),
        model=model,
        provider=provider,
        usage=_parse_usage(raw_response),
        raw_response=raw_response,
    )


def _messages_to_payload(messages: list[ChatMessage]) -> list[dict[str, object]]:
    return [message.to_litellm_dict() for message in messages]


def _tools_to_payload(tools: list[ToolSpec] | None) -> list[dict[str, object]] | None:
    if not tools:
        return None
    return [tool.to_litellm_dict() for tool in tools]


class LiteLLMClient(LLMProviderInterface):
    def __init__(self, *, settings: Settings, default_slot: int | None = None) -> None:
        self._settings = settings
        self._default_slot = default_slot

    def _resolve_slot(self, config: GenerationConfig | None) -> int:
        explicit_slot = config.slot if config is not None else None
        if explicit_slot is not None:
            return int(explicit_slot)
        if self._default_slot is not None:
            return int(self._default_slot)
        configured = self._settings.get_configured_llm_slots()
        if configured:
            return int(configured[0].slot)
        raise ValueError("Im Auditor ist kein LLM-Slot konfiguriert.")

    async def chat(
        self,
        *,
        messages: list[ChatMessage],
        tools: list[ToolSpec] | None = None,
        config: GenerationConfig | None = None,
    ) -> LLMResponse:
        from litellm import acompletion

        if not messages:
            raise ValueError("LiteLLMClient.chat benoetigt mindestens eine Nachricht.")

        slot = self._resolve_slot(config)
        resolved = resolve_litellm_config_from_slot(
            settings=self._settings,
            slot=slot,
            model_override=config.model if config is not None else None,
            temperature_override=config.temperature if config is not None else None,
            max_tokens_override=config.max_tokens if config is not None else None,
        )
        request_kwargs: dict[str, Any] = {
            "model": resolved.model,
            "messages": _messages_to_payload(messages),
            **resolved.extra_kwargs,
        }
        tools_payload = _tools_to_payload(tools)
        if tools_payload is not None:
            request_kwargs["tools"] = tools_payload
        if config is not None and config.seed is not None:
            request_kwargs["seed"] = int(config.seed)
        if config is not None and config.timeout_s is not None:
            request_kwargs["timeout"] = float(config.timeout_s)
        if config is not None and config.json_mode:
            request_kwargs["response_format"] = {"type": "json_object"}

        current_temperature = resolved.temperature
        current_max_tokens = resolved.max_tokens

        for _attempt in range(4):
            try:
                call_kwargs = dict(request_kwargs)
                if current_temperature is not None:
                    call_kwargs["temperature"] = float(current_temperature)
                if current_max_tokens is not None:
                    call_kwargs["max_tokens"] = int(current_max_tokens)
                raw_response = await acompletion(**call_kwargs)
                return _parse_completion_response(
                    raw_response=raw_response,
                    model=resolved.model,
                    provider=resolved.provider,
                )
            except Exception as exc:
                if current_temperature is not None and temperature_error_requires_retry_without_parameter(exc=exc):
                    mark_model_temperature_unsupported(model=resolved.model)
                    current_temperature = None
                    continue
                next_max_tokens = next_lower_max_tokens_for_error(
                    model=resolved.model,
                    current_max_tokens=current_max_tokens,
                    exc=exc,
                )
                if next_max_tokens is not None and next_max_tokens != current_max_tokens:
                    mark_model_max_tokens_cap(model=resolved.model, max_tokens=int(next_max_tokens))
                    current_max_tokens = int(next_max_tokens)
                    continue
                raise RuntimeError(
                    f"LiteLLM-Chat fehlgeschlagen fuer Slot {slot} ({resolved.provider}/{resolved.model}): {exc}"
                ) from exc
        raise RuntimeError(f"LiteLLM-Chat konnte fuer Slot {slot} nicht erfolgreich abgeschlossen werden.")

    async def stream(
        self,
        *,
        messages: list[ChatMessage],
        tools: list[ToolSpec] | None = None,
        config: GenerationConfig | None = None,
    ) -> AsyncGenerator[str | LLMResponse, None]:
        from litellm import acompletion

        slot = self._resolve_slot(config)
        resolved = resolve_litellm_config_from_slot(
            settings=self._settings,
            slot=slot,
            model_override=config.model if config is not None else None,
            temperature_override=config.temperature if config is not None else None,
            max_tokens_override=config.max_tokens if config is not None else None,
        )
        request_kwargs: dict[str, Any] = {
            "model": resolved.model,
            "messages": _messages_to_payload(messages),
            "stream": True,
            **resolved.extra_kwargs,
        }
        tools_payload = _tools_to_payload(tools)
        if tools_payload is not None:
            request_kwargs["tools"] = tools_payload
        if config is not None and config.seed is not None:
            request_kwargs["seed"] = int(config.seed)
        if config is not None and config.timeout_s is not None:
            request_kwargs["timeout"] = float(config.timeout_s)
        if resolved.temperature is not None:
            request_kwargs["temperature"] = float(resolved.temperature)
        if resolved.max_tokens is not None:
            request_kwargs["max_tokens"] = int(resolved.max_tokens)

        stream = await acompletion(**request_kwargs)
        collected_chunks: list[str] = []
        async for chunk in stream:
            choices = _safe_get(chunk, "choices", [])
            first_choice = choices[0] if isinstance(choices, list) and choices else {}
            delta = _safe_get(first_choice, "delta", {}) or {}
            content = _safe_get(delta, "content", "") or ""
            if content:
                collected_chunks.append(str(content))
                yield str(content)
        yield LLMResponse(
            content="".join(collected_chunks),
            model=resolved.model,
            provider=resolved.provider,
        )

    async def structured_output(
        self,
        *,
        messages: list[ChatMessage],
        schema: type[BaseModel],
        config: GenerationConfig | None = None,
    ) -> BaseModel:
        effective_config = (
            config.model_copy(update={"json_mode": True}) if config is not None else GenerationConfig(json_mode=True)
        )
        response = await self.chat(messages=messages, tools=None, config=effective_config)
        raw_text = _strip_markdown_code_fences(response.content)
        json_text = _extract_first_json_block(raw_text) or raw_text
        try:
            parsed = json.loads(json_text)
        except json.JSONDecodeError as exc:
            raise RuntimeError("LLM lieferte kein gueltiges JSON fuer structured_output.") from exc
        return schema.model_validate(parsed)
