from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, Field


ChatRole = Literal["system", "user", "assistant", "tool"]


class ChatMessage(BaseModel):
    role: ChatRole
    content: str
    name: str | None = None
    tool_call_id: str | None = None

    def to_litellm_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "role": self.role,
            "content": self.content,
        }
        if self.name:
            payload["name"] = self.name
        if self.tool_call_id:
            payload["tool_call_id"] = self.tool_call_id
        return payload


class ToolSpec(BaseModel):
    name: str
    description: str = ""
    parameters: dict[str, Any] = Field(default_factory=dict)

    def to_litellm_dict(self) -> dict[str, object]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


class ToolCall(BaseModel):
    id: str
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)

    def to_openai_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "type": "function",
            "function": {
                "name": self.name,
                "arguments": json.dumps(self.arguments, ensure_ascii=False),
            },
        }


class LLMResponse(BaseModel):
    content: str = ""
    tool_calls: list[ToolCall] = Field(default_factory=list)
    model: str | None = None
    provider: str | None = None
    usage: dict[str, Any] = Field(default_factory=dict)
    raw_response: Any = None


class GenerationConfig(BaseModel):
    model: str | None = None
    slot: int | None = None
    temperature: float | None = None
    max_tokens: int | None = None
    json_mode: bool = False
    seed: int | None = None
    timeout_s: float | None = None
