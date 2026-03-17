from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import logging
import re
from typing import Callable, Sequence

from fin_ai_auditor.domain.models import (
    AuditClaimEntry,
    SemanticEntity,
    SemanticRelation,
    TruthLedgerEntry,
)
from fin_ai_auditor.services.claim_semantics import package_scope_key
from fin_ai_auditor.services.pipeline_models import ExtractedClaimRecord

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SemanticGraphBuildResult:
    claims: list[AuditClaimEntry]
    semantic_entities: list[SemanticEntity]
    semantic_relations: list[SemanticRelation]
    notes: list[str]


def build_semantic_graph(
    *,
    run_id: str,
    claim_records: list[ExtractedClaimRecord],
    truths: list[TruthLedgerEntry],
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> SemanticGraphBuildResult:
    logger.info("semantic_graph_build_start", extra={"event_name": "semantic_graph_build_start", "event_payload": {"claims": len(claim_records), "truths": len(truths)}})
    entity_map: dict[str, SemanticEntity] = {}
    owner_entity_index: dict[tuple[str, str], set[str]] = defaultdict(set)
    relation_map: dict[tuple[str, str, str], SemanticRelation] = {}
    claim_context_by_id: dict[str, dict[str, object]] = {}
    total_claims = len(claim_records)

    for index, record in enumerate(claim_records, start=1):
        claim = record.claim
        semantic_entity_ids: list[str] = []
        semantic_cluster_keys: list[str] = [package_scope_key(claim.subject_key)]

        subject_entity = _ensure_subject_entity(
            entity_map=entity_map,
            owner_entity_index=owner_entity_index,
            run_id=run_id,
            claim=claim,
        )
        semantic_entity_ids.append(subject_entity.entity_id)
        semantic_cluster_keys.extend(_cluster_keys_for_subject(subject_key=claim.subject_key))

        for related_entity in _ensure_parent_entities(
            entity_map=entity_map,
            owner_entity_index=owner_entity_index,
            relation_map=relation_map,
            run_id=run_id,
            subject_entity=subject_entity,
            claim=claim,
        ):
            semantic_entity_ids.append(related_entity.entity_id)
            semantic_cluster_keys.append(related_entity.scope_key)

        evidence_entity = _ensure_evidence_entity(
            entity_map=entity_map,
            owner_entity_index=owner_entity_index,
            relation_map=relation_map,
            run_id=run_id,
            record=record,
            subject_entity=subject_entity,
        )
        if evidence_entity is not None:
            semantic_entity_ids.append(evidence_entity.entity_id)

        semantic_entity_ids.extend(
            _ensure_contract_context_relations(
                entity_map=entity_map,
                owner_entity_index=owner_entity_index,
                relation_map=relation_map,
                run_id=run_id,
                claim=claim,
                record=record,
                subject_entity=subject_entity,
            )
        )
        semantic_entity_ids.extend(
            _ensure_evidence_chain_step_relations(
                entity_map=entity_map,
                owner_entity_index=owner_entity_index,
                relation_map=relation_map,
                run_id=run_id,
                claim=claim,
                subject_entity=subject_entity,
            )
        )

        claim_context_by_id[claim.claim_id] = {
            "semantic_entity_ids": _dedupe_preserve_order(semantic_entity_ids),
            "semantic_cluster_keys": _dedupe_preserve_order(semantic_cluster_keys),
            "semantic_subject_type": subject_entity.entity_type,
            "semantic_section_paths": _dedupe_preserve_order(
                [
                    str(record.evidence.location.position.section_path).strip()
                    for _ in [record]
                    if record.evidence.location.position is not None
                    and str(record.evidence.location.position.section_path or "").strip()
                ]
            ),
        }
        if progress_callback is not None and (index % 5000 == 0 or index == total_claims):
            progress_callback(index, total_claims, "Semantik-Graph verdichtet Claim-Kontexte und Relationen.")

    for truth in truths:
        if truth.truth_status != "active":
            continue
        truth_entity = _ensure_entity(
            entity_map=entity_map,
            owner_entity_index=owner_entity_index,
            run_id=run_id,
            entity_type="truth",
            canonical_key=f"truth:{truth.canonical_key}",
            label=truth.canonical_key,
            scope_key=package_scope_key(truth.subject_key),
            source_ids=[truth.truth_id],
            metadata={"truth_id": truth.truth_id, "predicate": truth.predicate},
        )
        subject_entity = _ensure_subject_entity_for_truth(
            entity_map=entity_map,
            owner_entity_index=owner_entity_index,
            run_id=run_id,
            truth=truth,
        )
        _ensure_relation(
            relation_map=relation_map,
            run_id=run_id,
            source_entity_id=truth_entity.entity_id,
            target_entity_id=subject_entity.entity_id,
            relation_type="derived_from_truth",
            confidence=0.86,
            metadata={"truth_id": truth.truth_id},
        )

    entities_by_id = {entity.entity_id: entity for entity in entity_map.values()}
    cluster_claims: dict[str, list[AuditClaimEntry]] = defaultdict(list)
    for record in claim_records:
        cluster_keys = _string_list(
            claim_context_by_id.get(record.claim.claim_id, {}).get("semantic_cluster_keys")
        ) or [package_scope_key(record.claim.subject_key)]
        for cluster_key in cluster_keys:
            cluster_claims[cluster_key].append(record.claim)

    cluster_full_path_summaries = {
        cluster_key: _evidence_chain_full_path_summaries(claims=cluster_claims_for_key)
        for cluster_key, cluster_claims_for_key in cluster_claims.items()
    }
    relation_types_cache: dict[tuple[str, ...], list[str]] = {}
    process_context_cache: dict[tuple[str, ...], list[str]] = {}
    contract_paths_cache: dict[tuple[str, ...], list[str]] = {}
    evidence_chain_paths_cache: dict[tuple[str, ...], list[str]] = {}
    enriched_claims: list[AuditClaimEntry] = []
    for index, record in enumerate(claim_records, start=1):
        claim_context = claim_context_by_id.get(record.claim.claim_id, {})
        enriched_claims.append(
            record.claim.model_copy(
                update={
                    "metadata": {
                        **record.claim.metadata,
                        **claim_context,
                        "semantic_relation_types": _cached_claim_relation_types(
                            relation_map=relation_map,
                            claim_context=claim_context,
                            cache=relation_types_cache,
                        ),
                        "semantic_process_context": _cached_process_context_summaries(
                            relation_map=relation_map,
                            entities_by_id=entities_by_id,
                            claim_context=claim_context,
                            cache=process_context_cache,
                        ),
                        "semantic_contract_paths": _cached_contract_path_summaries(
                            relation_map=relation_map,
                            entities_by_id=entities_by_id,
                            claim_context=claim_context,
                            cache=contract_paths_cache,
                        ),
                        "semantic_evidence_chain_paths": _cached_evidence_chain_path_summaries(
                            relation_map=relation_map,
                            entities_by_id=entities_by_id,
                            claim_context=claim_context,
                            cache=evidence_chain_paths_cache,
                        ),
                        "semantic_evidence_chain_full_paths": list(
                            cluster_full_path_summaries.get(package_scope_key(record.claim.subject_key), [])
                        ),
                    }
                }
            )
        )
        if progress_callback is not None and (index % 5000 == 0 or index == total_claims):
            progress_callback(index, total_claims, "Semantik-Graph schreibt angereicherte Claim-Metadaten zurueck.")

    notes = [
        f"Semantik-Graph aufgebaut: {len(entity_map)} Knoten, {len(relation_map)} Relationen.",
    ]
    return SemanticGraphBuildResult(
        claims=enriched_claims,
        semantic_entities=sorted(entity_map.values(), key=lambda item: (item.scope_key, item.entity_type, item.label)),
        semantic_relations=sorted(
            relation_map.values(),
            key=lambda item: (item.relation_type, item.source_entity_id, item.target_entity_id),
        ),
        notes=notes,
    )


def attach_semantic_context_to_findings(
    *,
    findings: list[object],
    claims: list[AuditClaimEntry],
    semantic_entities: list[SemanticEntity],
    semantic_relations: list[SemanticRelation],
) -> list[object]:
    claim_map = {claim.claim_id: claim for claim in claims}
    entities_by_id = {entity.entity_id: entity for entity in semantic_entities}
    relation_map = {
        (relation.source_entity_id, relation.target_entity_id, relation.relation_type): relation
        for relation in semantic_relations
    }
    relations_by_entity: dict[str, list[SemanticRelation]] = {}
    for relation in semantic_relations:
        relations_by_entity.setdefault(relation.source_entity_id, []).append(relation)
        relations_by_entity.setdefault(relation.target_entity_id, []).append(relation)

    enriched = []
    for finding in findings:
        cluster_anchor = str(
            finding.metadata.get("subject_key")
            or finding.metadata.get("object_key")
            or finding.canonical_key
            or finding.title
        )
        cluster_key = package_scope_key(cluster_anchor)
        related_claims = [
            claim
            for claim in claims
            if _claim_matches_cluster(claim=claim, cluster_key=cluster_key)
        ]
        entity_ids = _dedupe_preserve_order(
            [
                entity_id
                for claim in related_claims
                for entity_id in _string_list(claim.metadata.get("semantic_entity_ids"))
            ]
        )
        related_entities = [entities_by_id[entity_id] for entity_id in entity_ids if entity_id in entities_by_id]
        related_relations = _dedupe_relations(
            [
                relation
                for entity_id in entity_ids
                for relation in relations_by_entity.get(entity_id, [])
                if relation.source_entity_id in entity_ids or relation.target_entity_id in entity_ids
            ]
        )
        section_paths = _dedupe_preserve_order(
            [
                str(location.position.section_path).strip()
                for location in finding.locations
                if location.position is not None and str(location.position.section_path or "").strip()
            ]
        )
        semantic_context = _dedupe_preserve_order(
            [
                f"{entity.entity_type}:{entity.label}"
                for entity in related_entities[:8]
            ]
        )
        relation_context = _dedupe_preserve_order(
            [
                _relation_summary(
                    relation=relation,
                    entities_by_id=entities_by_id,
                )
                for relation in related_relations[:8]
            ]
        )
        contract_paths = _contract_path_summaries(
            relation_map=relation_map,
            entities_by_id=entities_by_id,
            entity_ids=set(entity_ids),
        )
        evidence_chain_paths = _evidence_chain_path_summaries(
            relation_map=relation_map,
            entities_by_id=entities_by_id,
            entity_ids=set(entity_ids),
        )
        evidence_chain_full_paths = _evidence_chain_full_path_summaries(claims=related_claims)
        enriched.append(
            finding.model_copy(
                update={
                    "metadata": {
                        **finding.metadata,
                        "semantic_entity_ids": entity_ids,
                        "semantic_context": semantic_context,
                        "semantic_relation_summaries": relation_context,
                        "semantic_section_paths": section_paths,
                        "semantic_contract_paths": contract_paths,
                        "semantic_evidence_chain_paths": evidence_chain_paths,
                        "semantic_evidence_chain_full_paths": evidence_chain_full_paths,
                        "semantic_process_context": _process_context_summaries(
                            relation_map=relation_map,
                            entities_by_id=entities_by_id,
                            entity_ids=set(entity_ids),
                        ),
                    }
                }
            )
        )
    return enriched


def _ensure_subject_entity(
    *,
    entity_map: dict[str, SemanticEntity],
    owner_entity_index: dict[tuple[str, str], set[str]],
    run_id: str,
    claim: AuditClaimEntry,
) -> SemanticEntity:
    entity_type, label = _entity_type_and_label_for_subject(subject_key=claim.subject_key, predicate=claim.predicate)
    return _ensure_entity(
        entity_map=entity_map,
        owner_entity_index=owner_entity_index,
        run_id=run_id,
        entity_type=entity_type,
        canonical_key=claim.subject_key,
        label=label,
        scope_key=package_scope_key(claim.subject_key),
        source_ids=[claim.source_id],
        metadata={"predicate": claim.predicate, "source_type": claim.source_type},
    )


def _ensure_subject_entity_for_truth(
    *,
    entity_map: dict[str, SemanticEntity],
    owner_entity_index: dict[tuple[str, str], set[str]],
    run_id: str,
    truth: TruthLedgerEntry,
) -> SemanticEntity:
    entity_type, label = _entity_type_and_label_for_subject(subject_key=truth.subject_key, predicate=truth.predicate)
    return _ensure_entity(
        entity_map=entity_map,
        owner_entity_index=owner_entity_index,
        run_id=run_id,
        entity_type=entity_type,
        canonical_key=truth.subject_key,
        label=label,
        scope_key=package_scope_key(truth.subject_key),
        source_ids=[truth.truth_id],
        metadata={"predicate": truth.predicate, "source_type": truth.source_kind},
    )


def _ensure_parent_entities(
    *,
    entity_map: dict[str, SemanticEntity],
    owner_entity_index: dict[tuple[str, str], set[str]],
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    run_id: str,
    subject_entity: SemanticEntity,
    claim: AuditClaimEntry,
) -> list[SemanticEntity]:
    created: list[SemanticEntity] = []
    subject_key = claim.subject_key

    if subject_key.startswith("BSM.phase.") and ".question." in subject_key:
        phase_key = ".".join(subject_key.split(".")[:3])
        phase_entity = _ensure_entity(
            entity_map=entity_map,
            owner_entity_index=owner_entity_index,
            run_id=run_id,
            entity_type="phase",
            canonical_key=phase_key,
            label=phase_key.rsplit(".", 1)[-1],
            scope_key=package_scope_key(phase_key),
            source_ids=[claim.source_id],
            metadata={},
        )
        process_entity = _ensure_entity(
            entity_map=entity_map,
            owner_entity_index=owner_entity_index,
            run_id=run_id,
            entity_type="process",
            canonical_key="BSM.process",
            label="BSM.process",
            scope_key="BSM.process",
            source_ids=[claim.source_id],
            metadata={},
        )
        _ensure_relation(
            relation_map=relation_map,
            run_id=run_id,
            source_entity_id=subject_entity.entity_id,
            target_entity_id=phase_entity.entity_id,
            relation_type="belongs_to",
            confidence=0.95,
            metadata={"subject_key": subject_key},
        )
        _ensure_relation(
            relation_map=relation_map,
            run_id=run_id,
            source_entity_id=phase_entity.entity_id,
            target_entity_id=subject_entity.entity_id,
            relation_type="contains",
            confidence=0.95,
            metadata={"subject_key": subject_key},
        )
        _ensure_relation(
            relation_map=relation_map,
            run_id=run_id,
            source_entity_id=phase_entity.entity_id,
            target_entity_id=process_entity.entity_id,
            relation_type="belongs_to",
            confidence=0.95,
            metadata={"subject_key": phase_key},
        )
        _ensure_relation(
            relation_map=relation_map,
            run_id=run_id,
            source_entity_id=process_entity.entity_id,
            target_entity_id=phase_entity.entity_id,
            relation_type="contains",
            confidence=0.95,
            metadata={"subject_key": phase_key},
        )
        created.extend([phase_entity, process_entity])
    elif subject_key.startswith("BSM.phase."):
        process_entity = _ensure_entity(
            entity_map=entity_map,
            owner_entity_index=owner_entity_index,
            run_id=run_id,
            entity_type="process",
            canonical_key="BSM.process",
            label="BSM.process",
            scope_key="BSM.process",
            source_ids=[claim.source_id],
            metadata={},
        )
        _ensure_relation(
            relation_map=relation_map,
            run_id=run_id,
            source_entity_id=subject_entity.entity_id,
            target_entity_id=process_entity.entity_id,
            relation_type="belongs_to",
            confidence=0.92,
            metadata={"subject_key": subject_key},
        )
        _ensure_relation(
            relation_map=relation_map,
            run_id=run_id,
            source_entity_id=process_entity.entity_id,
            target_entity_id=subject_entity.entity_id,
            relation_type="contains",
            confidence=0.92,
            metadata={"subject_key": subject_key},
        )
        created.append(process_entity)
    elif "." in subject_key and not subject_key.startswith("BSM.process"):
        owner_key = subject_key.split(".", 1)[0]
        owner_entity = _ensure_entity(
            entity_map=entity_map,
            owner_entity_index=owner_entity_index,
            run_id=run_id,
            entity_type="object",
            canonical_key=owner_key,
            label=owner_key,
            scope_key=owner_key,
            source_ids=[claim.source_id],
            metadata={},
        )
        relation_type = _relation_type_for_predicate(predicate=claim.predicate)
        _ensure_relation(
            relation_map=relation_map,
            run_id=run_id,
            source_entity_id=owner_entity.entity_id,
            target_entity_id=subject_entity.entity_id,
            relation_type=relation_type,
            confidence=0.88,
            metadata={"predicate": claim.predicate},
        )
        created.append(owner_entity)
    return created


def _ensure_evidence_entity(
    *,
    entity_map: dict[str, SemanticEntity],
    owner_entity_index: dict[tuple[str, str], set[str]],
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    run_id: str,
    record: ExtractedClaimRecord,
    subject_entity: SemanticEntity,
) -> SemanticEntity | None:
    location = record.evidence.location
    section_path = str(location.position.section_path if location.position is not None else "" or "").strip()
    if location.source_type in {"confluence_page", "local_doc"}:
        label = section_path or location.title
        entity = _ensure_entity(
            entity_map=entity_map,
            owner_entity_index=owner_entity_index,
            run_id=run_id,
            entity_type="documentation_section",
            canonical_key=f"doc:{location.source_type}:{location.source_id}:{label}",
            label=label,
            scope_key=package_scope_key(record.claim.subject_key),
            source_ids=[location.source_id],
            metadata={
                "path_hint": location.path_hint,
                "url": location.url,
                "section_path": section_path,
            },
        )
        _ensure_relation(
            relation_map=relation_map,
            run_id=run_id,
            source_entity_id=entity.entity_id,
            target_entity_id=subject_entity.entity_id,
            relation_type="documents",
            confidence=0.9,
            metadata={"source_type": location.source_type},
        )
        return entity
    if location.source_type == "github_file":
        label = section_path or str(location.path_hint or location.title)
        entity = _ensure_entity(
            entity_map=entity_map,
            owner_entity_index=owner_entity_index,
            run_id=run_id,
            entity_type="code_component",
            canonical_key=f"code:{location.source_id}:{label}",
            label=label,
            scope_key=package_scope_key(record.claim.subject_key),
            source_ids=[location.source_id],
            metadata={
                "path_hint": location.path_hint,
                "anchor_value": location.position.anchor_value if location.position is not None else None,
            },
        )
        _ensure_relation(
            relation_map=relation_map,
            run_id=run_id,
            source_entity_id=entity.entity_id,
            target_entity_id=subject_entity.entity_id,
            relation_type="implements",
            confidence=0.9,
            metadata={"source_type": location.source_type},
        )
        return entity
    return None


def _ensure_entity(
    *,
    entity_map: dict[str, SemanticEntity],
    owner_entity_index: dict[tuple[str, str], set[str]],
    run_id: str,
    entity_type: str,
    canonical_key: str,
    label: str,
    scope_key: str,
    source_ids: list[str],
    metadata: dict[str, object],
) -> SemanticEntity:
    existing = entity_map.get(canonical_key)
    if existing is not None:
        merged_sources = _dedupe_preserve_order([*existing.source_ids, *source_ids])
        merged_metadata = {**existing.metadata, **metadata}
        updated = existing.model_copy(update={"source_ids": merged_sources, "metadata": merged_metadata})
        entity_map[canonical_key] = updated
        return updated
    entity = SemanticEntity(
        run_id=run_id,
        entity_type=entity_type,  # type: ignore[arg-type]
        canonical_key=canonical_key,
        label=label,
        scope_key=scope_key,
        source_ids=_dedupe_preserve_order(source_ids),
        metadata=dict(metadata),
    )
    entity_map[canonical_key] = entity
    owner_key = _owner_key_for_canonical_key(canonical_key)
    if owner_key is not None:
        owner_entity_index[(owner_key, entity_type)].add(canonical_key)
    return entity


def _ensure_relation(
    *,
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    run_id: str,
    source_entity_id: str,
    target_entity_id: str,
    relation_type: str,
    confidence: float,
    metadata: dict[str, object],
) -> None:
    key = (source_entity_id, target_entity_id, relation_type)
    existing = relation_map.get(key)
    if existing is not None:
        relation_map[key] = existing.model_copy(
            update={
                "confidence": max(existing.confidence, confidence),
                "metadata": {**existing.metadata, **metadata},
            }
        )
        return
    relation_map[key] = SemanticRelation(
        run_id=run_id,
        source_entity_id=source_entity_id,
        target_entity_id=target_entity_id,
        relation_type=relation_type,  # type: ignore[arg-type]
        confidence=confidence,
        metadata=dict(metadata),
    )


def _entity_type_and_label_for_subject(*, subject_key: str, predicate: str) -> tuple[str, str]:
    if subject_key == "BSM.process":
        return "process", "BSM.process"
    if subject_key.startswith("BSM.phase.") and ".question." in subject_key:
        return "question", subject_key.rsplit(".question.", 1)[1]
    if subject_key.startswith("BSM.phase."):
        return "phase", subject_key.rsplit(".", 1)[-1]
    if subject_key.endswith((".approval_policy", ".scope_policy", ".policy")):
        return "policy", subject_key
    if subject_key.endswith((".review_status", ".lifecycle")):
        return "lifecycle", subject_key
    if subject_key.endswith(".write_path"):
        return "write_contract", subject_key
    if subject_key.endswith(".read_path"):
        return "read_contract", subject_key
    if "." not in subject_key:
        return "object", subject_key
    if predicate.endswith("_process") or subject_key.startswith("BSM.process"):
        return "process", subject_key
    return "object", subject_key


def _relation_type_for_predicate(*, predicate: str) -> str:
    if "policy" in predicate:
        return "contains"
    if "review" in predicate or "lifecycle" in predicate:
        return "contains"
    if "reference" in predicate or "process" in predicate:
        return "references"
    if "write" in predicate or "read" in predicate:
        return "contains"
    return "contains"


def _cluster_keys_for_subject(*, subject_key: str) -> list[str]:
    keys = [package_scope_key(subject_key)]
    if subject_key.startswith("BSM.phase."):
        keys.append("BSM.process")
        if ".question." in subject_key:
            keys.append(".".join(subject_key.split(".")[:3]))
    elif "." in subject_key and not subject_key.startswith("BSM.process"):
        keys.append(subject_key.split(".", 1)[0])
    return _dedupe_preserve_order(keys)


def _claim_relation_types(
    *,
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    entity_ids: set[str],
) -> list[str]:
    relation_types = [
        relation.relation_type
        for relation in relation_map.values()
        if relation.source_entity_id in entity_ids or relation.target_entity_id in entity_ids
    ]
    return _dedupe_preserve_order(relation_types)


def _claim_context_entity_ids_key(claim_context: dict[str, object]) -> tuple[str, ...]:
    return tuple(_string_list(claim_context.get("semantic_entity_ids")))


def _cached_claim_relation_types(
    *,
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    claim_context: dict[str, object],
    cache: dict[tuple[str, ...], list[str]],
) -> list[str]:
    entity_ids_key = _claim_context_entity_ids_key(claim_context)
    cached = cache.get(entity_ids_key)
    if cached is not None:
        return cached
    computed = _claim_relation_types(
        relation_map=relation_map,
        entity_ids=set(entity_ids_key),
    )
    cache[entity_ids_key] = computed
    return computed


def _cached_process_context_summaries(
    *,
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    entities_by_id: dict[str, SemanticEntity],
    claim_context: dict[str, object],
    cache: dict[tuple[str, ...], list[str]],
) -> list[str]:
    entity_ids_key = _claim_context_entity_ids_key(claim_context)
    cached = cache.get(entity_ids_key)
    if cached is not None:
        return cached
    computed = _process_context_summaries(
        relation_map=relation_map,
        entities_by_id=entities_by_id,
        entity_ids=set(entity_ids_key),
    )
    cache[entity_ids_key] = computed
    return computed


def _cached_contract_path_summaries(
    *,
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    entities_by_id: dict[str, SemanticEntity],
    claim_context: dict[str, object],
    cache: dict[tuple[str, ...], list[str]],
) -> list[str]:
    entity_ids_key = _claim_context_entity_ids_key(claim_context)
    cached = cache.get(entity_ids_key)
    if cached is not None:
        return cached
    computed = _contract_path_summaries(
        relation_map=relation_map,
        entities_by_id=entities_by_id,
        entity_ids=set(entity_ids_key),
    )
    cache[entity_ids_key] = computed
    return computed


def _cached_evidence_chain_path_summaries(
    *,
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    entities_by_id: dict[str, SemanticEntity],
    claim_context: dict[str, object],
    cache: dict[tuple[str, ...], list[str]],
) -> list[str]:
    entity_ids_key = _claim_context_entity_ids_key(claim_context)
    cached = cache.get(entity_ids_key)
    if cached is not None:
        return cached
    computed = _evidence_chain_path_summaries(
        relation_map=relation_map,
        entities_by_id=entities_by_id,
        entity_ids=set(entity_ids_key),
    )
    cache[entity_ids_key] = computed
    return computed


def _claim_matches_cluster(*, claim: AuditClaimEntry, cluster_key: str) -> bool:
    semantic_cluster_keys = _string_list(claim.metadata.get("semantic_cluster_keys"))
    if cluster_key in semantic_cluster_keys:
        return True
    normalized_claim_key = str(claim.subject_key or "").strip()
    return normalized_claim_key == cluster_key or normalized_claim_key.startswith(f"{cluster_key}.")


def _relation_summary(*, relation: SemanticRelation, entities_by_id: dict[str, SemanticEntity]) -> str:
    source_label = entities_by_id.get(relation.source_entity_id).label if relation.source_entity_id in entities_by_id else relation.source_entity_id
    target_label = entities_by_id.get(relation.target_entity_id).label if relation.target_entity_id in entities_by_id else relation.target_entity_id
    return f"{source_label} -> {relation.relation_type} -> {target_label}"


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _dedupe_relations(relations: list[SemanticRelation]) -> list[SemanticRelation]:
    seen: set[str] = set()
    out: list[SemanticRelation] = []
    for relation in relations:
        if relation.relation_id in seen:
            continue
        seen.add(relation.relation_id)
        out.append(relation)
    return out


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _ensure_contract_context_relations(
    *,
    entity_map: dict[str, SemanticEntity],
    owner_entity_index: dict[tuple[str, str], set[str]],
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    run_id: str,
    claim: AuditClaimEntry,
    record: ExtractedClaimRecord,
    subject_entity: SemanticEntity,
) -> list[str]:
    entity_ids: list[str] = []
    if "." not in claim.subject_key or claim.subject_key.startswith("BSM.process"):
        return entity_ids

    owner_key = claim.subject_key.split(".", 1)[0]
    question_entity, phase_entity, process_entity = _ensure_context_entities_from_section_path(
        entity_map=entity_map,
        owner_entity_index=owner_entity_index,
        relation_map=relation_map,
        run_id=run_id,
        record=record,
    )
    for entity in (question_entity, phase_entity, process_entity):
        if entity is not None:
            entity_ids.append(entity.entity_id)

    policy_entities = _owner_semantic_entities(
        entity_map=entity_map,
        owner_entity_index=owner_entity_index,
        owner_key=owner_key,
        entity_type="policy",
    )
    contract_entities = _owner_semantic_entities(
        entity_map=entity_map,
        owner_entity_index=owner_entity_index,
        owner_key=owner_key,
        entity_type="write_contract",
    ) + _owner_semantic_entities(
        entity_map=entity_map,
        owner_entity_index=owner_entity_index,
        owner_key=owner_key,
        entity_type="read_contract",
    )

    if subject_entity.entity_type == "policy":
        for contract_entity in contract_entities:
            _ensure_relation(
                relation_map=relation_map,
                run_id=run_id,
                source_entity_id=subject_entity.entity_id,
                target_entity_id=contract_entity.entity_id,
                relation_type="governs",
                confidence=0.9,
                metadata={"owner_key": owner_key},
            )
            entity_ids.append(contract_entity.entity_id)
        for context_entity in [question_entity, phase_entity]:
            if context_entity is None:
                continue
            _ensure_relation(
                relation_map=relation_map,
                run_id=run_id,
                source_entity_id=context_entity.entity_id,
                target_entity_id=subject_entity.entity_id,
                relation_type="governs",
                confidence=0.86,
                metadata={"owner_key": owner_key},
            )

    if subject_entity.entity_type in {"write_contract", "read_contract"}:
        for policy_entity in policy_entities:
            _ensure_relation(
                relation_map=relation_map,
                run_id=run_id,
                source_entity_id=policy_entity.entity_id,
                target_entity_id=subject_entity.entity_id,
                relation_type="governs",
                confidence=0.9,
                metadata={"owner_key": owner_key},
            )
            entity_ids.append(policy_entity.entity_id)
        if not policy_entities:
            for context_entity in [question_entity, phase_entity]:
                if context_entity is None:
                    continue
                _ensure_relation(
                    relation_map=relation_map,
                    run_id=run_id,
                    source_entity_id=context_entity.entity_id,
                    target_entity_id=subject_entity.entity_id,
                    relation_type="references",
                    confidence=0.74,
                    metadata={"owner_key": owner_key},
                )

    if subject_entity.entity_type == "lifecycle":
        for context_entity in [question_entity, phase_entity]:
            if context_entity is None:
                continue
            _ensure_relation(
                relation_map=relation_map,
                run_id=run_id,
                source_entity_id=context_entity.entity_id,
                target_entity_id=subject_entity.entity_id,
                relation_type="governs",
                confidence=0.82,
                metadata={"owner_key": owner_key},
            )

    return _dedupe_preserve_order(entity_ids)


def _ensure_evidence_chain_step_relations(
    *,
    entity_map: dict[str, SemanticEntity],
    owner_entity_index: dict[tuple[str, str], set[str]],
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    run_id: str,
    claim: AuditClaimEntry,
    subject_entity: SemanticEntity,
) -> list[str]:
    if claim.predicate not in {"code_evidence_chain_step", "yaml_evidence_chain_step"}:
        return []
    metadata = claim.metadata or {}
    start_label = str(metadata.get("start_label") or "").strip()
    end_label = str(metadata.get("end_label") or "").strip()
    relationship_type = str(metadata.get("relationship_type") or "").strip()
    if not start_label or not end_label or not relationship_type:
        return []

    start_entity = _ensure_entity(
        entity_map=entity_map,
        owner_entity_index=owner_entity_index,
        run_id=run_id,
        entity_type="object",
        canonical_key=start_label,
        label=start_label,
        scope_key="EvidenceChain",
        source_ids=[claim.source_id],
        metadata={"evidence_chain_node": True},
    )
    end_entity = _ensure_entity(
        entity_map=entity_map,
        owner_entity_index=owner_entity_index,
        run_id=run_id,
        entity_type="object",
        canonical_key=end_label,
        label=end_label,
        scope_key="EvidenceChain",
        source_ids=[claim.source_id],
        metadata={"evidence_chain_node": True},
    )
    _ensure_relation(
        relation_map=relation_map,
        run_id=run_id,
        source_entity_id=start_entity.entity_id,
        target_entity_id=end_entity.entity_id,
        relation_type="references",
        confidence=0.9,
        metadata={
            "evidence_chain_step": True,
            "relationship_type": relationship_type,
            "hop_kind": metadata.get("hop_kind"),
            "subject_key": claim.subject_key,
        },
    )
    _ensure_relation(
        relation_map=relation_map,
        run_id=run_id,
        source_entity_id=subject_entity.entity_id,
        target_entity_id=start_entity.entity_id,
        relation_type="references",
        confidence=0.82,
        metadata={
            "evidence_chain_subject": True,
            "evidence_chain_role": "start",
            "subject_key": claim.subject_key,
        },
    )
    _ensure_relation(
        relation_map=relation_map,
        run_id=run_id,
        source_entity_id=subject_entity.entity_id,
        target_entity_id=end_entity.entity_id,
        relation_type="references",
        confidence=0.82,
        metadata={
            "evidence_chain_subject": True,
            "evidence_chain_role": "end",
            "subject_key": claim.subject_key,
        },
    )
    return [start_entity.entity_id, end_entity.entity_id]


def _ensure_context_entities_from_section_path(
    *,
    entity_map: dict[str, SemanticEntity],
    owner_entity_index: dict[tuple[str, str], set[str]],
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    run_id: str,
    record: ExtractedClaimRecord,
) -> tuple[SemanticEntity | None, SemanticEntity | None, SemanticEntity | None]:
    location = record.evidence.location
    section_path = str(location.position.section_path if location.position is not None else "" or "").strip()
    if not section_path:
        return None, None, None

    phase_key = _extract_phase_key_from_section_path(section_path)
    question_key = _extract_question_key_from_section_path(section_path)
    if not phase_key and not question_key:
        return None, None, None

    process_entity = _ensure_entity(
        entity_map=entity_map,
        owner_entity_index=owner_entity_index,
        run_id=run_id,
        entity_type="process",
        canonical_key="BSM.process",
        label="BSM.process",
        scope_key="BSM.process",
        source_ids=[location.source_id],
        metadata={},
    )
    phase_entity: SemanticEntity | None = None
    question_entity: SemanticEntity | None = None
    if phase_key:
        phase_entity = _ensure_entity(
            entity_map=entity_map,
            owner_entity_index=owner_entity_index,
            run_id=run_id,
            entity_type="phase",
            canonical_key=f"BSM.phase.{phase_key}",
            label=phase_key,
            scope_key=f"BSM.phase.{phase_key}",
            source_ids=[location.source_id],
            metadata={"derived_from_section_path": section_path},
        )
        _ensure_relation(
            relation_map=relation_map,
            run_id=run_id,
            source_entity_id=process_entity.entity_id,
            target_entity_id=phase_entity.entity_id,
            relation_type="contains",
            confidence=0.84,
            metadata={"section_path": section_path},
        )
        _ensure_relation(
            relation_map=relation_map,
            run_id=run_id,
            source_entity_id=phase_entity.entity_id,
            target_entity_id=process_entity.entity_id,
            relation_type="belongs_to",
            confidence=0.84,
            metadata={"section_path": section_path},
        )
    if question_key:
        canonical_key = f"BSM.phase.{phase_key}.question.{question_key}" if phase_key else f"BSM.process.question.{question_key}"
        question_scope = f"BSM.phase.{phase_key}" if phase_key else "BSM.process"
        question_entity = _ensure_entity(
            entity_map=entity_map,
            owner_entity_index=owner_entity_index,
            run_id=run_id,
            entity_type="question",
            canonical_key=canonical_key,
            label=question_key,
            scope_key=question_scope,
            source_ids=[location.source_id],
            metadata={"derived_from_section_path": section_path},
        )
        if phase_entity is not None:
            _ensure_relation(
                relation_map=relation_map,
                run_id=run_id,
                source_entity_id=phase_entity.entity_id,
                target_entity_id=question_entity.entity_id,
                relation_type="contains",
                confidence=0.88,
                metadata={"section_path": section_path},
            )
            _ensure_relation(
                relation_map=relation_map,
                run_id=run_id,
                source_entity_id=question_entity.entity_id,
                target_entity_id=phase_entity.entity_id,
                relation_type="belongs_to",
                confidence=0.88,
                metadata={"section_path": section_path},
            )
        else:
            _ensure_relation(
                relation_map=relation_map,
                run_id=run_id,
                source_entity_id=process_entity.entity_id,
                target_entity_id=question_entity.entity_id,
                relation_type="contains",
                confidence=0.8,
                metadata={"section_path": section_path},
            )
            _ensure_relation(
                relation_map=relation_map,
                run_id=run_id,
                source_entity_id=question_entity.entity_id,
                target_entity_id=process_entity.entity_id,
                relation_type="belongs_to",
                confidence=0.8,
                metadata={"section_path": section_path},
            )
    return question_entity, phase_entity, process_entity


def _owner_semantic_entities(
    *,
    entity_map: dict[str, SemanticEntity],
    owner_entity_index: dict[tuple[str, str], set[str]],
    owner_key: str,
    entity_type: str,
) -> list[SemanticEntity]:
    return [
        entity_map[canonical_key]
        for canonical_key in sorted(owner_entity_index.get((owner_key, entity_type), set()))
        if canonical_key in entity_map
    ]


def _owner_key_for_canonical_key(canonical_key: str) -> str | None:
    normalized = str(canonical_key or "").strip()
    if "." not in normalized or normalized.startswith("BSM.process") or normalized.startswith("truth:"):
        return None
    owner_key = normalized.split(".", 1)[0]
    return owner_key or None


def _extract_phase_key_from_section_path(section_path: str) -> str | None:
    match = re.search(r"(?:^|>\s*)phase\s*:\s*([^>]+)", section_path, flags=re.IGNORECASE)
    if match is None:
        return None
    return _slugify(match.group(1))


def _extract_question_key_from_section_path(section_path: str) -> str | None:
    match = re.search(r"(?:^|>\s*)(?:question|frage)\s*:\s*([^>]+)", section_path, flags=re.IGNORECASE)
    if match is None:
        return None
    return _slugify(match.group(1))


def _slugify(value: str) -> str:
    lowered = str(value or "").strip().casefold()
    collapsed = re.sub(r"[^a-z0-9]+", "_", lowered)
    return collapsed.strip("_")


def _process_context_summaries(
    *,
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    entities_by_id: dict[str, SemanticEntity],
    entity_ids: set[str],
) -> list[str]:
    questions = [
        entity
        for entity in entities_by_id.values()
        if entity.entity_id in entity_ids and entity.entity_type == "question"
    ]
    phases = [
        entity
        for entity in entities_by_id.values()
        if entity.entity_id in entity_ids and entity.entity_type == "phase"
    ]
    summaries: list[str] = []
    for question in questions:
        phase = _neighbor_entity(
            relation_map=relation_map,
            entities_by_id=entities_by_id,
            entity=question,
            relation_type="belongs_to",
            target_type="phase",
        )
        process = _neighbor_entity(
            relation_map=relation_map,
            entities_by_id=entities_by_id,
            entity=phase or question,
            relation_type="belongs_to",
            target_type="process",
        )
        parts = [_entity_path_label(process), _entity_path_label(phase), _entity_path_label(question)]
        summaries.append(" -> ".join(part for part in parts if part))
    if summaries:
        return _dedupe_preserve_order(summaries)
    for phase in phases:
        process = _neighbor_entity(
            relation_map=relation_map,
            entities_by_id=entities_by_id,
            entity=phase,
            relation_type="belongs_to",
            target_type="process",
        )
        parts = [_entity_path_label(process), _entity_path_label(phase)]
        summaries.append(" -> ".join(part for part in parts if part))
    return _dedupe_preserve_order(summaries)


def _contract_path_summaries(
    *,
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    entities_by_id: dict[str, SemanticEntity],
    entity_ids: set[str],
) -> list[str]:
    policy_entities = _entities_by_type(
        entities_by_id=entities_by_id,
        entity_ids=entity_ids,
        entity_type="policy",
    )
    contract_entities = [
        *list(
            _entities_by_type(
                entities_by_id=entities_by_id,
                entity_ids=entity_ids,
                entity_type="write_contract",
            )
        ),
        *list(
            _entities_by_type(
                entities_by_id=entities_by_id,
                entity_ids=entity_ids,
                entity_type="read_contract",
            )
        ),
    ]
    owner_entities = _entities_by_type(
        entities_by_id=entities_by_id,
        entity_ids=entity_ids,
        entity_type="object",
    )
    process_contexts = _process_context_summaries(
        relation_map=relation_map,
        entities_by_id=entities_by_id,
        entity_ids=entity_ids,
    )
    summaries: list[str] = []

    for policy_entity in policy_entities:
        targets = _outgoing_entities(
            relation_map=relation_map,
            entities_by_id=entities_by_id,
            source_entity=policy_entity,
            relation_type="governs",
            target_types={"write_contract", "read_contract"},
        )
        if not targets:
            targets = contract_entities
        owners = _incoming_entities(
            relation_map=relation_map,
            entities_by_id=entities_by_id,
            target_entity=policy_entity,
            relation_types={"contains"},
            source_types={"object"},
        )
        context_sources = _incoming_entities(
            relation_map=relation_map,
            entities_by_id=entities_by_id,
            target_entity=policy_entity,
            relation_types={"governs"},
            source_types={"question", "phase", "process"},
        )
        context_chain = _context_chain_for_entities(context_sources)
        owner_chain = _context_chain_for_entities(owners or owner_entities)
        prefixes = process_contexts or ([context_chain] if context_chain else []) or ([owner_chain] if owner_chain else [])
        for prefix in prefixes:
            prefix_parts = [prefix] if prefix else []
            if owners:
                prefix_parts.append(_entity_path_label(owners[0]))
            for target in targets or [None]:
                path_parts = [*prefix_parts, _entity_path_label(policy_entity)]
                if target is not None:
                    path_parts.append(_entity_path_label(target))
                summaries.append(" -> ".join(part for part in path_parts if part))

    if summaries:
        return _dedupe_preserve_order(summaries)

    owner_chain = _context_chain_for_entities(owner_entities)
    fallback_prefixes = process_contexts or ([owner_chain] if owner_chain else [])
    for contract_entity in contract_entities:
        for prefix in fallback_prefixes or [""]:
            path_parts = [prefix] if prefix else []
            path_parts.append(_entity_path_label(contract_entity))
            summaries.append(" -> ".join(part for part in path_parts if part))
    return _dedupe_preserve_order(summaries)


def _evidence_chain_path_summaries(
    *,
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    entities_by_id: dict[str, SemanticEntity],
    entity_ids: set[str],
) -> list[str]:
    step_relations = [
        relation
        for relation in relation_map.values()
        if relation.metadata.get("evidence_chain_step") is True
        and (relation.source_entity_id in entity_ids or relation.target_entity_id in entity_ids)
    ]
    if not step_relations:
        return []

    direct_steps = []
    for relation in step_relations:
        source = entities_by_id.get(relation.source_entity_id)
        target = entities_by_id.get(relation.target_entity_id)
        if source is None or target is None:
            continue
        rel_type = str(relation.metadata.get("relationship_type") or relation.relation_type).strip()
        direct_steps.append(f"{_entity_path_label(source)} -[{rel_type}]-> {_entity_path_label(target)}")

    adjacency: dict[str, list[tuple[str, str]]] = {}
    for relation in step_relations:
        source = entities_by_id.get(relation.source_entity_id)
        target = entities_by_id.get(relation.target_entity_id)
        if source is None or target is None:
            continue
        rel_type = str(relation.metadata.get("relationship_type") or relation.relation_type).strip()
        adjacency.setdefault(source.entity_id, []).append((target.entity_id, rel_type))

    chained_paths: list[str] = []
    for start_id, edges in adjacency.items():
        start_entity = entities_by_id.get(start_id)
        if start_entity is None:
            continue
        for mid_id, first_rel in edges:
            mid_entity = entities_by_id.get(mid_id)
            if mid_entity is None:
                continue
            for end_id, second_rel in adjacency.get(mid_id, []):
                end_entity = entities_by_id.get(end_id)
                if end_entity is None:
                    continue
                chained_paths.append(
                    f"{_entity_path_label(start_entity)} -[{first_rel}]-> {_entity_path_label(mid_entity)} -[{second_rel}]-> {_entity_path_label(end_entity)}"
                )

    return _dedupe_preserve_order([*chained_paths, *direct_steps])


def _evidence_chain_full_path_summaries(*, claims: Sequence[AuditClaimEntry]) -> list[str]:
    full_path_claims = [
        claim
        for claim in claims
        if claim.subject_key == "EvidenceChain.full_path"
        and claim.predicate in {
            "documented_evidence_chain_full_path",
            "puml_evidence_chain_full_path",
            "code_evidence_chain_full_path",
            "yaml_evidence_chain_full_path",
        }
    ]
    if not full_path_claims:
        return []
    return _dedupe_preserve_order(
        [
            str(claim.normalized_value or "").strip()
            for claim in full_path_claims
            if str(claim.normalized_value or "").strip()
        ]
    )


def _entities_by_type(
    *,
    entities_by_id: dict[str, SemanticEntity],
    entity_ids: set[str],
    entity_type: str,
) -> list[SemanticEntity]:
    return [
        entity
        for entity in entities_by_id.values()
        if entity.entity_id in entity_ids and entity.entity_type == entity_type
    ]


def _neighbor_entity(
    *,
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    entities_by_id: dict[str, SemanticEntity],
    entity: SemanticEntity | None,
    relation_type: str,
    target_type: str,
) -> SemanticEntity | None:
    if entity is None:
        return None
    for relation in relation_map.values():
        if relation.source_entity_id != entity.entity_id or relation.relation_type != relation_type:
            continue
        candidate = entities_by_id.get(relation.target_entity_id)
        if candidate is not None and candidate.entity_type == target_type:
            return candidate
    return None


def _outgoing_entities(
    *,
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    entities_by_id: dict[str, SemanticEntity],
    source_entity: SemanticEntity,
    relation_type: str,
    target_types: set[str],
) -> list[SemanticEntity]:
    out: list[SemanticEntity] = []
    for relation in relation_map.values():
        if relation.source_entity_id != source_entity.entity_id or relation.relation_type != relation_type:
            continue
        candidate = entities_by_id.get(relation.target_entity_id)
        if candidate is not None and candidate.entity_type in target_types:
            out.append(candidate)
    return out


def _incoming_entities(
    *,
    relation_map: dict[tuple[str, str, str], SemanticRelation],
    entities_by_id: dict[str, SemanticEntity],
    target_entity: SemanticEntity,
    relation_types: set[str],
    source_types: set[str],
) -> list[SemanticEntity]:
    out: list[SemanticEntity] = []
    for relation in relation_map.values():
        if relation.target_entity_id != target_entity.entity_id or relation.relation_type not in relation_types:
            continue
        candidate = entities_by_id.get(relation.source_entity_id)
        if candidate is not None and candidate.entity_type in source_types:
            out.append(candidate)
    return out


def _context_chain_for_entities(entities: list[SemanticEntity]) -> str:
    ordered = sorted(entities, key=lambda entity: _context_rank(entity.entity_type))
    return " -> ".join(_entity_path_label(entity) for entity in ordered if _entity_path_label(entity))


def _context_rank(entity_type: str) -> int:
    order = {"process": 0, "phase": 1, "question": 2, "object": 3}
    return order.get(entity_type, 9)


def _entity_path_label(entity: SemanticEntity | None) -> str:
    if entity is None:
        return ""
    return f"{entity.entity_type}:{entity.label}"
