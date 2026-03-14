from __future__ import annotations

from collections import Counter, defaultdict
import html
import re

from fin_ai_auditor.domain.models import (
    AuditFinding,
    AuditLocation,
    AuditRun,
    ConfluencePatchMarkerKind,
    ConfluencePatchOperation,
    ConfluencePatchPreview,
)


def build_confluence_patch_preview(
    *,
    run: AuditRun,
    findings: list[AuditFinding],
    fallback_page_url: str,
    fallback_page_title: str,
) -> ConfluencePatchPreview:
    page_context = _resolve_primary_page_context(run=run, findings=findings)
    page_id = page_context.get("page_id")
    page_title = _resolve_page_title(
        primary_title=page_context.get("page_title"),
        fallback_page_title=fallback_page_title,
    )
    page_url = str(page_context.get("page_url") or fallback_page_url).strip() or fallback_page_url
    space_key = _optional_text(page_context.get("space_key"))
    base_revision_id = _optional_text(page_context.get("base_revision_id"))

    operations: list[ConfluencePatchOperation] = []
    changed_sections: list[str] = []
    change_summary: list[str] = []
    blockers: list[str] = []

    if page_id is None:
        blockers.append(
            "Kein konkreter Confluence-Seitenanker wurde in den verknuepften Findings erkannt. "
            "Der Patch ist sichtbar vorbereitbar, aber noch nicht extern ausfuehrbar."
        )

    selected_findings = findings[:6]
    for finding in selected_findings:
        location = _preferred_confluence_location(finding=finding, selected_page_id=page_id)
        section_path = _section_path_for_location(location=location) or "Auditor Review"
        anchor_heading = _anchor_heading(section_path=section_path, fallback=finding.title)
        marker_kind = _marker_for_finding(finding=finding)
        current_statement = _current_statement(finding=finding)
        proposed_statement = _proposed_statement(finding=finding)
        rationale = finding.summary.strip() or finding.title
        storage_snippet = _build_storage_snippet(
            marker_kind=marker_kind,
            current_statement=current_statement,
            proposed_statement=proposed_statement,
            rationale=rationale,
        )
        operations.append(
            ConfluencePatchOperation(
                related_finding_id=finding.finding_id,
                action_type="append_after_heading" if location is not None else "append_to_page",
                marker_kind=marker_kind,
                section_path=section_path,
                anchor_heading=anchor_heading,
                current_statement=current_statement,
                proposed_statement=proposed_statement,
                rationale=rationale,
                storage_snippet=storage_snippet,
            )
        )
        changed_sections.append(section_path)
        change_summary.extend(
            [
                f"{section_path}: {finding.title}",
                f"Empfohlene Korrektur: {proposed_statement}",
            ]
        )

    if not operations:
        fallback_statement = "Fuer die ausgewaehlten Findings liegt noch kein konkretes Abschnitts-Delta vor."
        operations.append(
            ConfluencePatchOperation(
                action_type="append_to_page",
                marker_kind="insert",
                section_path="Auditor Review",
                anchor_heading="Auditor Review",
                current_statement=None,
                proposed_statement=fallback_statement,
                rationale="Der Auditor braucht fuer diese Freigabe noch einen konkreten Dokumentenanker oder eine User-Spezifizierung.",
                storage_snippet=_build_storage_snippet(
                    marker_kind="insert",
                    current_statement=None,
                    proposed_statement=fallback_statement,
                    rationale="Noch kein konkreter Dokumentenanker vorhanden.",
                ),
            )
        )
        changed_sections.append("Auditor Review")
        change_summary.append(fallback_statement)

    return ConfluencePatchPreview(
        page_id=page_id,
        page_title=page_title,
        page_url=page_url,
        space_key=space_key,
        base_revision_id=base_revision_id,
        execution_ready=page_id is not None and not blockers,
        blockers=_dedupe_preserve_order(blockers),
        changed_sections=_dedupe_preserve_order(changed_sections),
        change_summary=_dedupe_preserve_order(change_summary),
        review_storage_snippets=[operation.storage_snippet for operation in operations],
        operations=operations,
    )


def apply_confluence_patch_preview(
    *,
    storage_html: str,
    patch_preview: ConfluencePatchPreview,
) -> str:
    current_html = storage_html.strip() or "<p></p>"
    grouped_operations: dict[str, list[ConfluencePatchOperation]] = defaultdict(list)
    fallback_operations: list[ConfluencePatchOperation] = []

    for operation in patch_preview.operations:
        if operation.action_type == "append_after_heading":
            grouped_operations[operation.anchor_heading].append(operation)
        else:
            fallback_operations.append(operation)

    next_html = current_html
    appended_anchor_keys: set[str] = set()

    for anchor_heading, operations in grouped_operations.items():
        inserted_html = _build_section_review_block(
            section_path=operations[0].section_path,
            operations=operations,
        )
        updated_html, inserted = _insert_after_heading(
            current_html=next_html,
            anchor_heading=anchor_heading,
            insertion_html=inserted_html,
        )
        if inserted:
            next_html = updated_html
            appended_anchor_keys.add(anchor_heading.casefold())
        else:
            fallback_operations.extend(operations)

    if fallback_operations:
        fallback_groups: dict[str, list[ConfluencePatchOperation]] = defaultdict(list)
        for operation in fallback_operations:
            fallback_groups[operation.section_path].append(operation)
        appendix = "".join(
            _build_section_review_block(section_path=section_path, operations=operations)
            for section_path, operations in fallback_groups.items()
        )
        next_html = f"{next_html}\n{appendix}".strip()

    return next_html


def build_confluence_payload_preview(
    *,
    existing_preview: list[str],
    patch_preview: ConfluencePatchPreview,
) -> list[str]:
    preview = list(existing_preview)
    preview.extend(
        [
            f"Seite: {patch_preview.page_title}",
            f"Ziel-URL: {patch_preview.page_url}",
            *[f"Abschnitt: {section}" for section in patch_preview.changed_sections[:4]],
            *[f"Patch: {summary}" for summary in patch_preview.change_summary[:4]],
        ]
    )
    if patch_preview.blockers:
        preview.extend(f"Blocker: {entry}" for entry in patch_preview.blockers)
    else:
        preview.append("Der Patch ist section-anchored vorbereitet und nach Approval extern ausfuehrbar.")
    return _dedupe_preserve_order(preview)


def _resolve_primary_page_context(*, run: AuditRun, findings: list[AuditFinding]) -> dict[str, str | None]:
    page_votes: Counter[str] = Counter()
    page_locations: dict[str, AuditLocation] = {}

    for finding in findings:
        for location in finding.locations:
            if location.source_type != "confluence_page":
                continue
            page_id = str(location.source_id or "").strip()
            if not page_id:
                continue
            page_votes[page_id] += 1
            page_locations.setdefault(page_id, location)

    selected_page_id = page_votes.most_common(1)[0][0] if page_votes else None
    location = page_locations.get(selected_page_id or "")
    snapshot = next(
        (
            item
            for item in run.source_snapshots
            if item.source_type == "confluence_page" and item.source_id == selected_page_id
        ),
        None,
    )
    return {
        "page_id": selected_page_id,
        "page_title": str(location.title or "").strip() if location is not None else None,
        "page_url": str(location.url or "").strip() if location is not None else None,
        "space_key": str((snapshot.metadata or {}).get("space_key") or "").strip() if snapshot is not None else None,
        "base_revision_id": str(snapshot.revision_id or "").strip() if snapshot is not None else None,
    }


def _preferred_confluence_location(
    *,
    finding: AuditFinding,
    selected_page_id: str | None,
) -> AuditLocation | None:
    confluence_locations = [location for location in finding.locations if location.source_type == "confluence_page"]
    if not confluence_locations:
        return None
    if selected_page_id:
        for location in confluence_locations:
            if location.source_id == selected_page_id:
                return location
    return confluence_locations[0]


def _section_path_for_location(*, location: AuditLocation | None) -> str | None:
    if location is None:
        return None
    position = location.position
    if position is not None and position.section_path:
        return position.section_path
    if location.path_hint:
        return location.path_hint
    if position is not None and position.anchor_value:
        return position.anchor_value
    return None


def _anchor_heading(*, section_path: str, fallback: str) -> str:
    parts = [part.strip() for part in section_path.split(" / ") if part.strip()]
    if parts:
        return parts[-1]
    return fallback.strip() or "Auditor Review"


def _marker_for_finding(*, finding: AuditFinding) -> ConfluencePatchMarkerKind:
    if finding.category in {"missing_definition", "clarification_needed", "traceability_gap"}:
        return "insert"
    if finding.category in {"open_decision", "ownership_gap"}:
        return "confirm"
    if finding.category in {"obsolete_documentation", "stale_source"}:
        return "remove"
    return "correct"


def _current_statement(*, finding: AuditFinding) -> str | None:
    retrieval_entries = _string_entries(finding.metadata.get("retrieval_context"))
    if retrieval_entries:
        return retrieval_entries[0]
    summary = finding.summary.strip()
    return summary or None


def _proposed_statement(*, finding: AuditFinding) -> str:
    recommendation = finding.recommendation.strip()
    if recommendation:
        return recommendation
    return f"{finding.title} konsistent und pruefbar in der SSOT-Doku nachziehen."


def _build_storage_snippet(
    *,
    marker_kind: ConfluencePatchMarkerKind,
    current_statement: str | None,
    proposed_statement: str,
    rationale: str,
) -> str:
    lines: list[str] = []
    if current_statement and marker_kind in {"remove", "correct"}:
        lines.append(
            _styled_paragraph(
                text=f"Bisherige Aussage: {current_statement}",
                background="rgb(255, 205, 210)",
                strike=True,
            )
        )
    if marker_kind in {"correct", "insert"}:
        lines.append(
            _styled_paragraph(
                text=f"Korrigierte Aussage: {proposed_statement}",
                background="rgb(255, 249, 196)",
            )
        )
    if marker_kind == "confirm":
        lines.append(
            _styled_paragraph(
                text=f"Bestaetigte Entscheidung: {proposed_statement}",
                background="rgb(200, 230, 201)",
            )
        )
    elif marker_kind == "remove":
        lines.append(
            _styled_paragraph(
                text="Die gestrichene Aussage ist fuer den aktuellen Vertragsstand nicht mehr gueltig.",
                background="rgb(255, 249, 196)",
            )
        )
    else:
        lines.append(
            _styled_paragraph(
                text=f"Review-Hinweis: {rationale}",
                background="rgb(200, 230, 201)",
            )
        )
    return "".join(lines)


def _build_section_review_block(
    *,
    section_path: str,
    operations: list[ConfluencePatchOperation],
) -> str:
    block_lines = [
        '<div data-finai-auditor-review="true" style="border-left: 4px solid #546e7a; padding-left: 12px; margin: 12px 0;">',
        f"<p><strong>{html.escape(f'FIN-AI Auditor Review: {section_path}')}</strong></p>",
    ]
    for operation in operations:
        block_lines.append(operation.storage_snippet)
    block_lines.append("</div>")
    return "".join(block_lines)


def _insert_after_heading(
    *,
    current_html: str,
    anchor_heading: str,
    insertion_html: str,
) -> tuple[str, bool]:
    escaped_heading = re.escape(anchor_heading.strip())
    if not escaped_heading:
        return current_html, False
    pattern = re.compile(
        rf"(<h[1-6][^>]*>\s*{escaped_heading}\s*</h[1-6]>)",
        flags=re.IGNORECASE,
    )
    updated_html, replacements = pattern.subn(rf"\1{insertion_html}", current_html, count=1)
    return updated_html, replacements > 0


def _styled_paragraph(*, text: str, background: str, strike: bool = False) -> str:
    decoration = " text-decoration: line-through;" if strike else ""
    return (
        '<p><span style="display: inline-block; padding: 2px 4px; '
        f"background-color: {background};{decoration}\">{html.escape(text)}</span></p>"
    )


def _string_entries(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _resolve_page_title(*, primary_title: object, fallback_page_title: str) -> str:
    primary_text = str(primary_title or "").strip()
    if not primary_text:
        return fallback_page_title
    if primary_text.casefold().startswith("confluence space "):
        return fallback_page_title
    return primary_text


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        marker = text.casefold()
        if marker in seen:
            continue
        seen.add(marker)
        ordered.append(text)
    return ordered
