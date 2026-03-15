"""Documentation gap detection and Markdown proposal generation.

Detects concepts, definitions, and processes that exist in code but have NO
corresponding Confluence or local documentation page.  For each gap a ready-
to-use Markdown page proposal is generated that matches the style and structure
of the existing Confluence space.

Finding category: ``missing_documentation``
Metadata key:     ``proposed_page_md``   — the full Markdown content
                  ``proposed_page_title``
                  ``gap_type``           — entity | process | policy | lifecycle
"""
from __future__ import annotations

import logging
import re
import textwrap
from collections import defaultdict
from typing import Sequence

from fin_ai_auditor.domain.models import AuditFinding, AuditLocation
from fin_ai_auditor.services.pipeline_models import CollectedDocument, ExtractedClaimRecord

logger = logging.getLogger(__name__)

# ── Documented-subject index ────────────────────────────────────────

_DOC_SOURCE_TYPES = frozenset({"confluence_page", "local_doc"})
_CODE_SOURCE_TYPES = frozenset({"github_file"})


def detect_documentation_gaps(
    *,
    claim_records: list[ExtractedClaimRecord],
    documents: Sequence[CollectedDocument],
) -> list[AuditFinding]:
    """Find code-implemented concepts that lack Confluence documentation.

    Returns findings with ``category='missing_documentation'`` and a full
    Markdown page proposal inside ``metadata['proposed_page_md']``.
    """
    logger.info(
        "doc_gap_detection_start",
        extra={"event_name": "doc_gap_detection_start", "event_payload": {"claims": len(claim_records), "documents": len(documents)}},
    )

    # 1. Build sets: subjects documented vs subjects in code
    documented_subjects: set[str] = set()
    documented_roots: set[str] = set()
    code_subjects: dict[str, list[ExtractedClaimRecord]] = defaultdict(list)

    for record in claim_records:
        subject = record.claim.subject_key
        if record.claim.source_type in _DOC_SOURCE_TYPES:
            authority = str(getattr(record.claim, "source_authority", "") or "").strip()
            assertion_status = str(getattr(record.claim, "assertion_status", "asserted") or "asserted").strip()
            if authority == "historical" or assertion_status == "secondary_only":
                continue
            documented_subjects.add(subject)
            documented_roots.add(subject.split(".", 1)[0] if "." in subject else subject)
        elif record.claim.source_type in _CODE_SOURCE_TYPES:
            code_subjects[subject].append(record)

    # 2. Find undocumented scopes. If the root object is documented, keep missing
    # subscope findings instead of suppressing them behind the root.
    undocumented_scopes: dict[str, list[ExtractedClaimRecord]] = defaultdict(list)
    for subject, records in code_subjects.items():
        if subject in documented_subjects:
            continue
        root = subject.split(".", 1)[0] if "." in subject else subject
        if root in documented_roots and subject != root:
            undocumented_scopes[subject].extend(records)
            continue
        undocumented_scopes[root].extend(records)

    if not undocumented_scopes:
        logger.info("doc_gap_detection_done", extra={"event_name": "doc_gap_detection_done", "event_payload": {"gaps_found": 0}})
        return []

    # 3. Learn the style from existing documentation
    doc_style = _learn_document_style(documents=documents)

    # 4. Generate findings with MD proposals
    findings: list[AuditFinding] = []
    for scope_root, records in sorted(undocumented_scopes.items()):
        root = scope_root.split(".", 1)[0] if "." in scope_root else scope_root
        min_records = 1 if scope_root != root and root in documented_roots else 2
        if len(records) < min_records:
            continue

        gap_type = _classify_gap(records=records)
        title_de = _german_title(scope_root=scope_root, gap_type=gap_type)
        md_proposal = _generate_md_proposal(
            scope_root=scope_root,
            records=records,
            gap_type=gap_type,
            doc_style=doc_style,
        )
        code_locations = _build_locations(records=records)
        summary = _build_summary(scope_root=scope_root, records=records, gap_type=gap_type)

        findings.append(
            AuditFinding(
                severity="medium",
                category="missing_documentation",
                title=title_de,
                summary=summary,
                recommendation=(
                    f"Erstellen Sie eine Confluence-Seite '{_page_title(scope_root)}' "
                    f"mit der vorgeschlagenen Struktur. Der Markdown-Entwurf steht als Vorlage bereit."
                ),
                canonical_key=f"doc_gap:{scope_root}",
                locations=code_locations,
                proposed_confluence_action=f"Neue Seite: {_page_title(scope_root)}",
                metadata={
                    "gap_type": gap_type,
                    "proposed_page_title": _page_title(scope_root),
                    "proposed_page_md": md_proposal,
                    "undocumented_claims_count": len(records),
                    "root_documented": root in documented_roots,
                    "code_files": list({r.evidence.location.source_id for r in records})[:10],
                },
            )
        )

    logger.info(
        "doc_gap_detection_done",
        extra={"event_name": "doc_gap_detection_done", "event_payload": {"gaps_found": len(findings), "scopes_checked": len(undocumented_scopes)}},
    )
    return findings


# ── Style learning ──────────────────────────────────────────────────

class _DocStyle:
    """Learned document style from existing Confluence pages."""
    heading_prefix: str = "#"
    uses_tables: bool = False
    uses_admonitions: bool = False
    avg_section_count: int = 4
    common_sections: list[str] = []
    language: str = "de"

    def __init__(self) -> None:
        self.common_sections = []


def _learn_document_style(*, documents: Sequence[CollectedDocument]) -> _DocStyle:
    style = _DocStyle()
    doc_docs = [d for d in documents if d.source_type in _DOC_SOURCE_TYPES and len(d.body) > 100]
    if not doc_docs:
        style.common_sections = ["Übersicht", "Definition", "Verantwortlichkeiten", "Referenzen"]
        return style

    section_counter: dict[str, int] = defaultdict(int)
    total_sections = 0

    for doc in doc_docs[:20]:  # Sample up to 20 docs
        body = doc.body
        headings = re.findall(r"^#{1,3}\s+(.+)$", body, re.MULTILINE)
        if not headings:
            headings = re.findall(r"^<h[123][^>]*>(.+?)</h[123]>", body, re.MULTILINE | re.IGNORECASE)
        for h in headings:
            clean = re.sub(r"[0-9.]+\s*", "", h).strip()
            if clean:
                section_counter[clean] += 1
                total_sections += 1

        if "|" in body and "---" in body:
            style.uses_tables = True
        if any(kw in body.lower() for kw in ["note:", "warning:", "info:", "hinweis:", "achtung:"]):
            style.uses_admonitions = True

    if doc_docs:
        style.avg_section_count = max(3, total_sections // len(doc_docs))

    # Top sections by frequency
    style.common_sections = [
        s for s, _ in sorted(section_counter.items(), key=lambda x: -x[1])[:8]
    ]
    if not style.common_sections:
        style.common_sections = ["Übersicht", "Definition", "Verantwortlichkeiten", "Referenzen"]

    return style


# ── Gap classification ──────────────────────────────────────────────

def _classify_gap(*, records: list[ExtractedClaimRecord]) -> str:
    predicates = {r.claim.predicate for r in records}
    subjects = {r.claim.subject_key for r in records}

    if any("policy" in p or "approval" in p for p in predicates):
        return "policy"
    if any("lifecycle" in p or "review_status" in p for p in predicates):
        return "lifecycle"
    if any(s.startswith("BSM.phase") or s.startswith("BSM.process") for s in subjects):
        return "process"
    return "entity"


# ── Markdown generation ─────────────────────────────────────────────

def _page_title(scope_root: str) -> str:
    """Human-readable page title."""
    parts = scope_root.replace("_", " ").replace(".", " – ")
    return parts.title()


def _german_title(*, scope_root: str, gap_type: str) -> str:
    type_labels = {
        "entity": "Objekt-Dokumentation",
        "process": "Prozess-Dokumentation",
        "policy": "Richtlinien-Dokumentation",
        "lifecycle": "Lebenszyklus-Dokumentation",
    }
    return f"Fehlende {type_labels.get(gap_type, 'Dokumentation')}: {_page_title(scope_root)}"


def _generate_md_proposal(
    *,
    scope_root: str,
    records: list[ExtractedClaimRecord],
    gap_type: str,
    doc_style: _DocStyle,
) -> str:
    """Generate a ready-to-use Markdown page proposal."""
    title = _page_title(scope_root)
    sections: list[str] = []

    # Header
    sections.append(f"# {title}\n")

    # Overview section
    overview_lines = [f"## Übersicht\n"]
    overview_lines.append(
        f"Dieses Dokument beschreibt **{title}** und definiert die relevanten "
        f"Eigenschaften, Verantwortlichkeiten und Schnittstellen.\n"
    )
    overview_lines.append(f"> **Status:** Entwurf — generiert aus Code-Analyse\n")
    sections.append("\n".join(overview_lines))

    # Group claims by predicate
    by_predicate: dict[str, list[ExtractedClaimRecord]] = defaultdict(list)
    for r in records:
        by_predicate[r.claim.predicate].append(r)

    # Definition / Properties section
    prop_lines = ["## Definition und Eigenschaften\n"]
    if doc_style.uses_tables and len(by_predicate) > 2:
        prop_lines.append("| Eigenschaft | Wert | Quelle |")
        prop_lines.append("|-------------|------|--------|")
        for pred, recs in sorted(by_predicate.items()):
            for rec in recs[:3]:  # Max 3 per predicate
                val = rec.claim.normalized_value[:80]
                src = (rec.evidence.location.path_hint or rec.evidence.location.source_id or "")
                src_short = src.split("/")[-1] if "/" in src else src
                prop_lines.append(f"| `{pred}` | {val} | `{src_short}` |")
    else:
        for pred, recs in sorted(by_predicate.items()):
            prop_lines.append(f"### {pred.replace('_', ' ').title()}\n")
            for rec in recs[:3]:
                val = rec.claim.normalized_value
                src = (rec.evidence.location.path_hint or rec.evidence.location.source_id or "")
                prop_lines.append(f"- **Wert:** {val}")
                prop_lines.append(f"  - *Quelle:* `{src}`\n")
    sections.append("\n".join(prop_lines))

    # Gap-type-specific sections
    if gap_type == "process":
        sections.append(_process_section(records=records))
    elif gap_type == "policy":
        sections.append(_policy_section(records=records))
    elif gap_type == "lifecycle":
        sections.append(_lifecycle_section(records=records))

    # Code references section
    code_files = sorted({
        rec.evidence.location.path_hint or rec.evidence.location.source_id or ""
        for rec in records
    })[:15]
    ref_lines = ["## Code-Referenzen\n"]
    ref_lines.append("Die folgenden Dateien implementieren dieses Konzept:\n")
    for f in code_files:
        ref_lines.append(f"- `{f}`")
    sections.append("\n".join(ref_lines))

    # Open questions section
    sections.append(textwrap.dedent("""\
        ## Offene Fragen

        - [ ] Ist die hier beschriebene Logik korrekt und vollständig?
        - [ ] Welche zusätzlichen Randbedingungen gelten?
        - [ ] Wer ist verantwortlich für Änderungen an diesem Konzept?
    """))

    return "\n\n".join(sections)


def _process_section(*, records: list[ExtractedClaimRecord]) -> str:
    lines = ["## Prozessablauf\n"]
    lines.append("Der folgende Ablauf wurde aus der Code-Implementierung abgeleitet:\n")

    phase_records = [r for r in records if "phase" in r.claim.subject_key.lower()]
    if phase_records:
        lines.append("### Phasen\n")
        seen = set()
        for r in phase_records:
            key = r.claim.subject_key
            if key in seen:
                continue
            seen.add(key)
            lines.append(f"1. **{key}**: {r.claim.normalized_value}")
    else:
        lines.append("*Prozessschritte konnten nicht automatisch abgeleitet werden. Bitte ergänzen.*\n")

    return "\n".join(lines)


def _policy_section(*, records: list[ExtractedClaimRecord]) -> str:
    lines = ["## Richtlinien und Genehmigungen\n"]
    policy_recs = [r for r in records if "policy" in r.claim.predicate or "approval" in r.claim.predicate]
    if policy_recs:
        lines.append("| Richtlinie | Wert | Beschreibung |")
        lines.append("|------------|------|--------------|")
        for r in policy_recs[:10]:
            lines.append(f"| `{r.claim.predicate}` | {r.claim.normalized_value} | *Aus Code abgeleitet* |")
    else:
        lines.append("*Richtlinien konnten nicht automatisch abgeleitet werden.*\n")
    return "\n".join(lines)


def _lifecycle_section(*, records: list[ExtractedClaimRecord]) -> str:
    lines = ["## Lebenszyklus\n"]
    lifecycle_recs = [r for r in records if "lifecycle" in r.claim.predicate or "review" in r.claim.predicate or "status" in r.claim.predicate]
    if lifecycle_recs:
        lines.append("### Status-Übergänge\n")
        for r in lifecycle_recs[:8]:
            lines.append(f"- **{r.claim.predicate}**: {r.claim.normalized_value}")
    else:
        lines.append("*Lebenszyklus-Details konnten nicht automatisch abgeleitet werden.*\n")
    return "\n".join(lines)


# ── Helpers ─────────────────────────────────────────────────────────

def _build_locations(*, records: list[ExtractedClaimRecord]) -> list[AuditLocation]:
    seen: set[str] = set()
    locations: list[AuditLocation] = []
    for rec in records[:8]:
        loc = rec.evidence.location
        key = f"{loc.source_type}:{loc.source_id}"
        if key in seen:
            continue
        seen.add(key)
        locations.append(loc)
    return locations


def _build_summary(*, scope_root: str, records: list[ExtractedClaimRecord], gap_type: str) -> str:
    predicates = sorted({r.claim.predicate for r in records})[:5]
    files = sorted({(r.evidence.location.path_hint or r.evidence.location.source_id or "").split("/")[-1] for r in records})[:4]
    return (
        f"Das Konzept '{scope_root}' wird im Code implementiert "
        f"({', '.join(files)}), hat aber keine zugehoerige Confluence-Dokumentation. "
        f"Betroffene Aspekte: {', '.join(predicates)}. "
        f"Ein Markdown-Vorschlag fuer eine neue Confluence-Seite wurde generiert."
    )
