from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator

from pydantic import BaseModel

from fin_ai_auditor.llm.types import ChatMessage, GenerationConfig, LLMResponse, ToolSpec


class LLMProviderInterface(ABC):
    @abstractmethod
    async def chat(
        self,
        *,
        messages: list[ChatMessage],
        tools: list[ToolSpec] | None = None,
        config: GenerationConfig | None = None,
    ) -> LLMResponse:
        raise NotImplementedError

    @abstractmethod
    async def stream(
        self,
        *,
        messages: list[ChatMessage],
        tools: list[ToolSpec] | None = None,
        config: GenerationConfig | None = None,
    ) -> AsyncGenerator[str | LLMResponse, None]:
        raise NotImplementedError

    @abstractmethod
    async def structured_output(
        self,
        *,
        messages: list[ChatMessage],
        schema: type[BaseModel],
        config: GenerationConfig | None = None,
    ) -> BaseModel:
        raise NotImplementedError
