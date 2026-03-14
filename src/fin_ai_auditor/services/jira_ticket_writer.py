from __future__ import annotations

from typing import Any

from fin_ai_auditor.domain.models import JiraTicketAICodingBrief


def build_jira_issue_payload(
    *,
    brief: JiraTicketAICodingBrief,
    project_key: str,
    issue_type: str = "Story",
) -> dict[str, Any]:
    return {
        "fields": {
            "project": {"key": str(project_key)},
            "issuetype": {"name": str(issue_type)},
            "summary": brief.title,
            "description": _build_description_adf(brief=brief),
            "labels": ["fin-ai-auditor", "ai-coding", "spec-drift"],
        }
    }


def _build_description_adf(*, brief: JiraTicketAICodingBrief) -> dict[str, Any]:
    content: list[dict[str, Any]] = []
    content.extend(_heading(brief.title, level=1))
    content.extend(_paragraph(brief.problem_description))
    content.extend(_section("Grund", [brief.reason]))
    content.extend(_section("Korrekturmassnahmen", brief.correction_measures))
    content.extend(_section("Erwartetes Zielbild", brief.target_state))
    content.extend(_section("Pruefbare Abnahmekriterien", brief.acceptance_criteria))
    content.extend(_section("Implikationen", brief.implications))
    content.extend(_section("Betroffene Teile", brief.affected_parts))
    content.extend(_section("Evidenz", brief.evidence))
    content.extend(_section("Implementierungshinweise", brief.implementation_notes))
    content.extend(_section("Validierung", brief.validation_steps))
    content.extend(_section("AI Coding Prompt", [brief.ai_coding_prompt], code_block=True))
    return {"version": 1, "type": "doc", "content": content}


def _section(title: str, items: list[str], *, code_block: bool = False) -> list[dict[str, Any]]:
    blocks = _heading(title, level=2)
    cleaned = [str(item or "").strip() for item in items if str(item or "").strip()]
    if not cleaned:
        return blocks
    if code_block:
        blocks.append({"type": "codeBlock", "attrs": {"language": "text"}, "content": [{"type": "text", "text": cleaned[0]}]})
        return blocks
    if len(cleaned) == 1:
        blocks.extend(_paragraph(cleaned[0]))
        return blocks
    blocks.append(
        {
            "type": "bulletList",
            "content": [
                {
                    "type": "listItem",
                    "content": [{"type": "paragraph", "content": [{"type": "text", "text": item}]}],
                }
                for item in cleaned
            ],
        }
    )
    return blocks


def _heading(text: str, *, level: int) -> list[dict[str, Any]]:
    cleaned = str(text or "").strip()
    if not cleaned:
        return []
    return [
        {
            "type": "heading",
            "attrs": {"level": int(level)},
            "content": [{"type": "text", "text": cleaned}],
        }
    ]


def _paragraph(text: str) -> list[dict[str, Any]]:
    cleaned = str(text or "").strip()
    if not cleaned:
        return []
    return [
        {
            "type": "paragraph",
            "content": [{"type": "text", "text": cleaned}],
        }
    ]
