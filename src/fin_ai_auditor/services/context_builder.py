"""Builds compressed context strings for LLM calls.

Provides a 3-layer context system:
 1. Repo-Tiefenanalyse — module structure, services, APIs, patterns
 2. Metamodell-Kontext — phases, questions, relations, metaclasses
 3. Confluence-Map — page tree, thematic tags
"""
from __future__ import annotations

import ast
import json
import logging
import os
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Sequence

from fin_ai_auditor.services.pipeline_models import CollectedDocument

logger = logging.getLogger(__name__)


class AuditContextBuilder:
    """Komprimiert gesammelte Dokumente zu kontextoptimalen LLM-Summaries."""

    # ── Layer 1: Repository ──────────────────────────────────────────

    def build_repo_summary(self, documents: Sequence[CollectedDocument]) -> str:
        """Erzeugt eine kompakte Repo-Übersicht für den LLM-Kontext.

        Enthält:
        - Modulbaum (Verzeichnisse + Dateianzahl)
        - Service-Klassen mit public methods
        - API-Routen mit HTTP-Verben
        - Konfigurationsschlüssel
        """
        code_docs = [d for d in documents if d.source_type == "github_file"]
        if not code_docs:
            return "Keine Code-Quellen verfuegbar."

        # Directory tree
        dirs: dict[str, int] = Counter()
        for d in code_docs:
            parts = (d.path_hint or d.source_id or "").split("/")
            if len(parts) > 1:
                dirs["/".join(parts[:-1])] += 1

        tree_lines = [f"  {path}/ ({count} Dateien)" for path, count in sorted(dirs.items())[:30]]

        # Service classes & API routes
        services: list[str] = []
        routes: list[str] = []
        config_keys: list[str] = []

        for d in code_docs:
            body = d.body or ""
            path = d.path_hint or d.source_id or ""

            # Python classes
            for cls_name, public in _extract_python_services(body=body):
                if public:
                    services.append(f"  {cls_name} ({path}): {', '.join(public[:5])}")

            # FastAPI routes
            for m in re.finditer(
                r'@\w*\.(?:router\.)?(?P<verb>get|post|put|patch|delete)\(\s*["\'](?P<path>[^"\']+)',
                body,
                re.IGNORECASE,
            ):
                routes.append(f"  {m.group('verb').upper()} {m.group('path')}")

            # Config keys (settings, env vars)
            for m in re.finditer(
                r'(?:FIN_AI_AUDITOR_|FINAI_)\w+',
                body,
            ):
                config_keys.append(f"  {m.group()}")

        sections = ["=== FIN-AI Repository Struktur ==="]
        if tree_lines:
            sections.append("Verzeichnisse:\n" + "\n".join(tree_lines[:20]))
        if services:
            sections.append("Services:\n" + "\n".join(sorted(set(services))[:15]))
        if routes:
            sections.append("API-Routen:\n" + "\n".join(sorted(set(routes))[:20]))
        if config_keys:
            sections.append("Konfiguration:\n" + "\n".join(sorted(set(config_keys))[:10]))

        return "\n\n".join(sections)

    # ── Layer 2: Metamodell ──────────────────────────────────────────

    def build_metamodel_summary(self, documents: Sequence[CollectedDocument]) -> str:
        """Komprimiert das Metamodell zu einer LLM-lesbaren Übersicht."""
        meta_docs = [d for d in documents if d.source_type == "metamodel"]
        if not meta_docs:
            return "Kein Metamodell verfuegbar."

        phases: list[str] = []
        metaclasses: list[str] = []
        functions: list[str] = []

        for d in meta_docs:
            try:
                rows = json.loads(d.body)
            except (ValueError, TypeError):
                continue
            if not isinstance(rows, list):
                continue

            for row in rows:
                if not isinstance(row, dict):
                    continue
                kind = str(row.get("entity_kind", "phase")).strip().casefold()

                if kind == "metaclass":
                    name = row.get("metaclass_name") or row.get("name", "?")
                    rels = row.get("outbound_relation_types", [])
                    rel_str = f" → {', '.join(rels[:4])}" if rels else ""
                    metaclasses.append(f"  {name}{rel_str}")
                elif kind == "bsm_function":
                    name = row.get("function_name") or row.get("name", "?")
                    labels = row.get("labels", [])
                    label_str = f" [{', '.join(labels[:3])}]" if labels else ""
                    functions.append(f"  {name}{label_str}")
                else:
                    phase_name = row.get("phase_name") or row.get("name", "?")
                    questions = row.get("questions", [])
                    q_count = len(questions) if isinstance(questions, list) else 0
                    order = row.get("phase_order") or row.get("order", "")
                    phases.append(f"  {order}. {phase_name} ({q_count} Fragen)")

        sections = ["=== BSM Metamodell ==="]
        if phases:
            sections.append(f"Phasen ({len(phases)}):\n" + "\n".join(phases[:20]))
        if metaclasses:
            sections.append(f"Metaclasses ({len(metaclasses)}):\n" + "\n".join(metaclasses[:15]))
        if functions:
            sections.append(f"BSM-Funktionen ({len(functions)}):\n" + "\n".join(functions[:10]))

        return "\n\n".join(sections)

    # ── Layer 3: Confluence ──────────────────────────────────────────

    def build_confluence_map(self, documents: Sequence[CollectedDocument]) -> str:
        """Baut eine Confluence-Seiten-Übersicht für den LLM-Kontext."""
        conf_docs = [d for d in documents if d.source_type == "confluence_page"]
        if not conf_docs:
            return "Keine Confluence-Seiten verfuegbar."

        # Group by space
        spaces: dict[str, list[str]] = defaultdict(list)
        for d in conf_docs:
            space = str(d.metadata.get("space_key") or "default")
            title = d.title or d.source_id
            # Extract first significant heading for thematic tag
            body_preview = (d.body or "")[:300]
            theme = ""
            heading_match = re.search(r"^#{1,3}\s+(.+)", body_preview, re.MULTILINE)
            if heading_match:
                theme = f" — {heading_match.group(1).strip()[:60]}"
            spaces[space].append(f"  📄 {title}{theme}")

        sections = ["=== Confluence-Dokumentation ==="]
        for space_key, pages in sorted(spaces.items()):
            sections.append(f"Space {space_key} ({len(pages)} Seiten):\n" + "\n".join(pages[:25]))

        return "\n\n".join(sections)

    # ── Combined ─────────────────────────────────────────────────────

    def build_full_context(self, documents: Sequence[CollectedDocument]) -> str:
        """Baut alle 4 Kontext-Layer zusammen."""
        logger.info("context_build_start", extra={"event_name": "context_build_start", "event_payload": {"total_docs": len(documents)}})
        repo = self.build_repo_summary(documents)
        logger.info("context_layer_done", extra={"event_name": "context_layer_done", "event_payload": {"layer": "repo", "chars": len(repo)}})
        metamodel = self.build_metamodel_summary(documents)
        logger.info("context_layer_done", extra={"event_name": "context_layer_done", "event_payload": {"layer": "metamodel", "chars": len(metamodel)}})
        confluence = self.build_confluence_map(documents)
        logger.info("context_layer_done", extra={"event_name": "context_layer_done", "event_payload": {"layer": "confluence", "chars": len(confluence)}})
        arch = self.build_finai_architecture_context(documents)
        logger.info("context_layer_done", extra={"event_name": "context_layer_done", "event_payload": {"layer": "architecture", "chars": len(arch)}})
        parts = [repo, metamodel, confluence, arch]
        result = "\n\n---\n\n".join(p for p in parts if p)
        logger.info("context_build_done", extra={"event_name": "context_build_done", "event_payload": {"total_chars": len(result)}})
        return result

    # ── Layer 4: FIN-AI Architecture Docs ────────────────────────────

    def build_finai_architecture_context(
        self, documents: Sequence[CollectedDocument]
    ) -> str:
        """Laedt kritische Architektur-Docs aus dem FIN-AI Repo als LLM-Kontext.

        Sucht das lokale FIN-AI-Repo anhand der gesammelten github_file-Pfade
        und liest dann die relevantesten Architektur-Dokumente direkt ein.
        """
        # Try to find the local FIN-AI repo path from collected documents
        repo_root = self._find_local_repo_root(documents)
        if not repo_root:
            return ""

        # Critical docs hierarchy — ordered by importance
        critical_paths = [
            # BSM Pipeline definitions (most important for contradictions)
            "models/finai_meta_ssot_pipeline_v2.puml",
            "models/finai_meta_ssot_pipeline_AS_IS.puml",
            # BSM process & target architecture
            "_docs/bsm/process-ssot.md",
            "_docs/bsm/target-architecture-unit-run-scope.md",
            "_docs/bsm/process-and-terminology.md",
            "_docs/bsm/implementation-reference.md",
            "_docs/bsm/e2e-reference.md",
            # Architecture contracts
            "_docs/contracts/BSM_FACHLOGIK_INVARIANTEN.md",
            "_docs/contracts/CONTRACTS.md",
            # Core architecture
            "_docs/architecture/01-core-principles.md",
            "_docs/architecture/02-context-contract.md",
            "_docs/architecture/11-bsm-logic-guardrails.md",
        ]

        sections: list[str] = ["=== FIN-AI Architektur-Dokumentation ==="]
        total_chars = 0
        max_total = 25_000  # Budget: ~25k chars for arch docs

        for rel_path in critical_paths:
            full = repo_root / rel_path
            if not full.is_file():
                continue
            try:
                content = full.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue

            # Truncate individual files
            max_file = min(4000, max_total - total_chars)
            if max_file <= 200:
                break

            truncated = content[:max_file]
            if len(content) > max_file:
                truncated += f"\n\n[... gekuerzt, Gesamtlaenge: {len(content)} Zeichen]"

            sections.append(
                f"--- {rel_path} ---\n{truncated}"
            )
            total_chars += len(truncated)

        if len(sections) <= 1:
            return ""
        return "\n\n".join(sections)

    @staticmethod
    def _find_local_repo_root(
        documents: Sequence[CollectedDocument],
    ) -> Path | None:
        """Find the local FIN-AI repo root from collected document paths."""
        env_path = os.environ.get("FIN_AI_AUDITOR_DEFAULT_FINAI_LOCAL_REPO_PATH", "")
        if env_path:
            p = Path(env_path)
            if p.is_dir():
                return p

        # Try to infer from collected github_file documents
        for d in documents:
            if d.source_type != "github_file":
                continue
            path_hint = d.path_hint or ""
            # Look for known FIN-AI markers
            if "/FIN-AI/" in path_hint:
                idx = path_hint.index("/FIN-AI/")
                candidate = Path(path_hint[:idx + len("/FIN-AI")])
                if candidate.is_dir():
                    return candidate
        return None


def _extract_python_services(*, body: str) -> list[tuple[str, list[str]]]:
    try:
        module = ast.parse(body)
    except SyntaxError:
        return _extract_python_services_fallback(body=body)

    services: list[tuple[str, list[str]]] = []
    for node in module.body:
        if not isinstance(node, ast.ClassDef):
            continue
        public_methods = [
            child.name
            for child in node.body
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
            and child.name
            and not child.name.startswith("_")
        ][:8]
        if public_methods:
            services.append((node.name, public_methods))
    return services


def _extract_python_services_fallback(*, body: str) -> list[tuple[str, list[str]]]:
    class_matches = list(re.finditer(r"^class\s+(\w+).*:", body, re.MULTILINE))
    services: list[tuple[str, list[str]]] = []
    for index, match in enumerate(class_matches):
        next_start = class_matches[index + 1].start() if index + 1 < len(class_matches) else len(body)
        class_block = body[match.end():next_start]
        public_methods = [
            method_name
            for method_name in re.findall(r"^    (?:async\s+)?def\s+([a-z]\w*)\s*\(", class_block, re.MULTILINE)
            if not method_name.startswith("_")
        ][:8]
        if public_methods:
            services.append((match.group(1), public_methods))
    return services
