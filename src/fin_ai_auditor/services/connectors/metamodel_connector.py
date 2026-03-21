from __future__ import annotations

import hashlib
import json
from typing import Any

from fin_ai_auditor.config import DirectMetaModelConfig, Settings
from fin_ai_auditor.domain.models import AuditSourceSnapshot, utc_now_iso
from fin_ai_auditor.services.pipeline_models import CollectionBundle, CollectedDocument

_BSM_CATALOG_QUERY = """
MATCH (phase:bsmPhase)
OPTIONAL MATCH (phase)-[:BSM_PHASE_BSMPHASE_ASKS_BSMQUESTION_BSM_QUESTION]->(question:bsmQuestion)
WITH
  phase,
  question,
  properties(phase) AS phase_props,
  CASE
    WHEN question IS NULL THEN {}
    ELSE properties(question)
  END AS question_props
ORDER BY coalesce(toString(phase_props['order']), ''), coalesce(toString(question_props['order']), '')
RETURN
  coalesce(phase_props['public_id'], phase_props['xmi_id'], phase_props['id'], elementId(phase)) AS phase_id,
  coalesce(phase_props['name'], phase_props['label'], phase_props['title'], phase_props['public_id'], elementId(phase)) AS phase_name,
  coalesce(toString(phase_props['order']), '') AS phase_order,
  phase_props AS phase_properties,
  collect(
    CASE
      WHEN question IS NULL THEN NULL
      ELSE {
        question_id: coalesce(question_props['public_id'], question_props['xmi_id'], question_props['id'], elementId(question)),
        question_text: coalesce(question_props['question'], question_props['text'], question_props['name'], question_props['label'], ''),
        order: coalesce(toString(question_props['order']), ''),
        intent: coalesce(question_props['intent'], ''),
        properties: question_props
      }
    END
  ) AS raw_questions
"""

_LABEL_SUMMARY_QUERY = """
MATCH (node)
UNWIND labels(node) AS label
RETURN label, count(*) AS node_count
ORDER BY node_count DESC, label ASC
LIMIT 40
"""

_RELATIONSHIP_SUMMARY_QUERY = """
MATCH ()-[rel]->()
RETURN type(rel) AS relation_type, count(*) AS relation_count
ORDER BY relation_count DESC, relation_type ASC
LIMIT 120
"""

_METACLASS_QUERY = """
MATCH (meta:metaclass)
OPTIONAL MATCH (pkg:package)-[:CONTAINS]->(meta)
OPTIONAL MATCH (meta)-[rel]->()
WITH
  meta,
  properties(meta) AS meta_props,
  collect(DISTINCT coalesce(pkg.name, pkg.label, pkg.title, pkg.public_id)) AS package_names,
  collect(DISTINCT type(rel)) AS relation_types
ORDER BY coalesce(meta_props['name'], meta_props['label'], meta_props['title'], meta_props['public_id'], elementId(meta))
RETURN
  coalesce(meta_props['public_id'], meta_props['xmi_id'], meta_props['id'], elementId(meta)) AS metaclass_id,
  coalesce(meta_props['name'], meta_props['label'], meta_props['title'], meta_props['public_id'], elementId(meta)) AS metaclass_name,
  meta_props AS metaclass_properties,
  package_names,
  relation_types
LIMIT 250
"""

_BSM_FUNCTION_QUERY = """
MATCH (fn)
WHERE any(label IN labels(fn) WHERE label = 'bsmFunction' OR label ENDS WITH 'Function')
OPTIONAL MATCH (source)-[rel]->(fn)
WHERE source:bsmQuestion OR source:bsmPhase
WITH
  fn,
  labels(fn) AS fn_labels,
  properties(fn) AS fn_props,
  collect(DISTINCT type(rel)) AS relation_types,
  collect(
    DISTINCT CASE
      WHEN source IS NULL THEN NULL
      ELSE coalesce(source.public_id, source.name, source.label, source.title, elementId(source))
    END
  ) AS question_or_phase_refs
ORDER BY coalesce(fn_props['name'], fn_props['label'], fn_props['title'], fn_props['public_id'], elementId(fn))
RETURN
  coalesce(fn_props['public_id'], fn_props['xmi_id'], fn_props['id'], elementId(fn)) AS function_id,
  coalesce(fn_props['name'], fn_props['label'], fn_props['title'], fn_props['public_id'], elementId(fn)) AS function_name,
  fn_labels AS labels,
  fn_props AS function_properties,
  relation_types,
  question_or_phase_refs
LIMIT 250
"""


class MetaModelConnector:
    """Read-only Connector fuer FIN-AI Metamodell-Operationen und den lokalen Current Dump.

    Bevorzugt direkten read-only Zugriff auf das Metamodell. Kein FIN-AI API-Read in diesem Pfad.
    """

    def __init__(self, *, settings: Settings) -> None:
        self._settings = settings

    def collect_catalog(self) -> CollectionBundle:
        notes: list[str] = []
        rows: list[dict[str, Any]] = []
        source = "local_dump"
        previous_payload = _read_existing_dump(settings=self._settings)
        direct_config = self._settings.get_direct_metamodel_config()

        try:
            if direct_config is None:
                raise RuntimeError("Kein DIRECT-Metamodellzugang konfiguriert.")
            rows = _fetch_direct_catalog(config=direct_config)
            source = "direct_neo4j"
            notes.append("Metamodell-Katalog wurde read-only direkt aus Neo4j geladen.")
        except Exception as exc:
            if previous_payload is not None:
                rows = [row for row in previous_payload.get("rows", []) if isinstance(row, dict)]
                notes.append(
                    "Direkter Metamodell-Read ist fehlgeschlagen; der letzte lokale Current Dump wurde als Fallback verwendet."
                )
                notes.append(f"Fallback-Grund: {type(exc).__name__}: {exc}")
            else:
                rows = []
                notes.append(
                    "Direkter Metamodell-Read ist fehlgeschlagen und es lag noch kein lokaler Dump vor; "
                    "der Lauf setzt mit leerem Metamodell-Fallback fort."
                )
                notes.append(f"Fallback-Grund: {type(exc).__name__}: {exc}")

        dump_payload = {
            "collected_at": utc_now_iso(),
            "source": source,
            "rows": rows,
        }
        self._settings.metamodel_dump_path.write_text(
            json.dumps(dump_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        dump_text = json.dumps(rows, ensure_ascii=False, sort_keys=True)
        content_hash = _sha256_text(dump_text)
        previous_hash = None
        if previous_payload is not None:
            previous_hash = _sha256_text(json.dumps(previous_payload.get("rows", []), ensure_ascii=False, sort_keys=True))

        snapshot = AuditSourceSnapshot(
            source_type="metamodel",
            source_id="finai-current-dump",
            revision_id=str(dump_payload.get("collected_at") or "").strip() or None,
            content_hash=content_hash,
            sync_token="metamodel:current_dump",
            metadata={
                "dump_path": str(self._settings.metamodel_dump_path),
                "source": source,
                "row_count": len(rows),
                "changed_since_last_dump": previous_hash != content_hash if previous_hash is not None else True,
            },
        )
        document = CollectedDocument(
            snapshot=snapshot,
            source_type="metamodel",
            source_id="finai-current-dump",
            title="FIN-AI Metamodell Current Dump",
            body=json.dumps(rows, ensure_ascii=False, indent=2),
            path_hint=str(self._settings.metamodel_dump_path),
            metadata={"row_count": len(rows), "source": source},
        )
        notes.append(f"Metamodell-Dump wurde lokal unter {self._settings.metamodel_dump_path} aktualisiert.")
        return CollectionBundle(snapshots=[snapshot], documents=[document], analysis_notes=notes)


def _fetch_direct_catalog(*, config: DirectMetaModelConfig) -> list[dict[str, Any]]:
    from neo4j import GraphDatabase, READ_ACCESS

    driver = GraphDatabase.driver(
        str(config.uri),
        auth=(str(config.username), str(config.password)),
    )
    try:
        driver.verify_connectivity()
        with driver.session(database=str(config.database), default_access_mode=READ_ACCESS) as session:
            phase_rows = [record.data() for record in session.run(_BSM_CATALOG_QUERY)]
            label_rows = [record.data() for record in session.run(_LABEL_SUMMARY_QUERY)]
            relationship_rows = [record.data() for record in session.run(_RELATIONSHIP_SUMMARY_QUERY)]
            metaclass_rows = [record.data() for record in session.run(_METACLASS_QUERY)]
            function_rows = [record.data() for record in session.run(_BSM_FUNCTION_QUERY)]
    finally:
        driver.close()
    rows: list[dict[str, Any]] = []
    rows.extend(_shape_phase_row(row) for row in phase_rows)
    rows.extend(_shape_label_summary_row(row) for row in label_rows)
    rows.extend(_shape_relationship_summary_row(row) for row in relationship_rows)
    rows.extend(_shape_metaclass_row(row) for row in metaclass_rows)
    rows.extend(_shape_function_row(row) for row in function_rows)
    return rows


def _read_existing_dump(*, settings: Settings) -> dict[str, Any] | None:
    if not settings.metamodel_dump_path.exists():
        return None
    try:
        payload = json.loads(settings.metamodel_dump_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _sha256_text(value: str) -> str:
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _shape_phase_row(row: dict[str, Any]) -> dict[str, Any]:
    questions = []
    for raw_question in row.get("raw_questions") or []:
        if not isinstance(raw_question, dict):
            continue
        questions.append(
            {
                "question_id": str(raw_question.get("question_id") or "").strip(),
                "question_text": str(raw_question.get("question_text") or "").strip(),
                "order": str(raw_question.get("order") or "").strip(),
                "intent": str(raw_question.get("intent") or "").strip(),
                "properties": _to_jsonable(raw_question.get("properties") or {}),
            }
        )
    return {
        "entity_kind": "phase",
        "phase_id": str(row.get("phase_id") or "").strip(),
        "phase_name": str(row.get("phase_name") or "").strip(),
        "phase_order": str(row.get("phase_order") or "").strip(),
        "phase_properties": _to_jsonable(row.get("phase_properties") or {}),
        "questions": questions,
    }


def _shape_label_summary_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "entity_kind": "label_summary",
        "label": str(row.get("label") or "").strip(),
        "node_count": int(row.get("node_count") or 0),
    }


def _shape_relationship_summary_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "entity_kind": "relationship_summary",
        "relation_type": str(row.get("relation_type") or "").strip(),
        "relation_count": int(row.get("relation_count") or 0),
    }


def _shape_metaclass_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "entity_kind": "metaclass",
        "metaclass_id": str(row.get("metaclass_id") or "").strip(),
        "metaclass_name": str(row.get("metaclass_name") or "").strip(),
        "metaclass_properties": _to_jsonable(row.get("metaclass_properties") or {}),
        "package_names": [
            str(item).strip()
            for item in row.get("package_names") or []
            if str(item).strip()
        ],
        "outbound_relation_types": [
            str(item).strip()
            for item in row.get("relation_types") or []
            if str(item).strip()
        ],
    }


def _shape_function_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "entity_kind": "bsm_function",
        "function_id": str(row.get("function_id") or "").strip(),
        "function_name": str(row.get("function_name") or "").strip(),
        "labels": [
            str(item).strip()
            for item in row.get("labels") or []
            if str(item).strip()
        ],
        "function_properties": _to_jsonable(row.get("function_properties") or {}),
        "relation_types": [
            str(item).strip()
            for item in row.get("relation_types") or []
            if str(item).strip()
        ],
        "question_keys": [
            _normalize_question_or_phase_ref(str(item).strip())
            for item in row.get("question_or_phase_refs") or []
            if str(item).strip()
        ],
    }


def _normalize_question_or_phase_ref(value: str) -> str:
    lowered = str(value or "").strip().casefold()
    collapsed = "".join(char if char.isalnum() else "_" for char in lowered)
    return collapsed.strip("_")


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _to_jsonable(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
