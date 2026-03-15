from __future__ import annotations

from collections import deque

from fin_ai_auditor.domain.models import AuditClaimEntry, AuditFinding, SemanticEntity, SemanticRelation
from fin_ai_auditor.services.causal_graph_models import CausalGraph, CausalGraphEdge, CausalGraphNode


_RELATION_STRENGTH: dict[str, float] = {
    "derived_from_truth": 1.0,
    "governs": 0.96,
    "contains": 0.88,
    "belongs_to": 0.86,
    "implements": 0.82,
    "documents": 0.8,
    "references": 0.66,
}
_ENTITY_BUCKETS: dict[str, str] = {
    "truth": "truth",
    "policy": "policy",
    "write_contract": "write_contract",
    "read_contract": "write_contract",
    "lifecycle": "lifecycle",
    "phase": "process",
    "question": "process",
    "process": "process",
}
_BUCKET_SELECTION_PRIORITY: dict[str, int] = {
    "truth": 0,
    "policy": 1,
    "write_contract": 2,
    "lifecycle": 3,
    "process": 4,
}


def attach_causal_attribution_to_findings(
    *,
    findings: list[AuditFinding],
    claims: list[AuditClaimEntry],
    semantic_entities: list[SemanticEntity],
    semantic_relations: list[SemanticRelation],
    causal_graph: CausalGraph | None = None,
) -> list[AuditFinding]:
    if causal_graph is not None:
        return _attach_graph_based_causal_attribution(
            findings=findings,
            claims=claims,
            causal_graph=causal_graph,
        )

    entities_by_id = {entity.entity_id: entity for entity in semantic_entities}
    adjacency = _build_undirected_adjacency(relations=semantic_relations)
    claims_by_subject: dict[str, list[AuditClaimEntry]] = {}
    for claim in claims:
        claims_by_subject.setdefault(str(claim.subject_key), []).append(claim)

    enriched: list[AuditFinding] = []
    for finding in findings:
        seed_entity_ids = _seed_entity_ids_for_finding(
            finding=finding,
            claims_by_subject=claims_by_subject,
        )
        attribution = _derive_causal_attribution(
            seed_entity_ids=seed_entity_ids,
            entities_by_id=entities_by_id,
            adjacency=adjacency,
        )
        enriched.append(
            finding.model_copy(
                update={
                    "metadata": {
                        **finding.metadata,
                        **attribution,
                    }
                }
            )
        )
    return enriched


def _attach_graph_based_causal_attribution(
    *,
    findings: list[AuditFinding],
    claims: list[AuditClaimEntry],
    causal_graph: CausalGraph,
) -> list[AuditFinding]:
    nodes_by_id = {node.node_id: node for node in causal_graph.nodes}
    outgoing = _graph_adjacency(edges=causal_graph.edges, reverse=False)
    incoming = _graph_adjacency(edges=causal_graph.edges, reverse=True)
    node_id_by_semantic_id = {
        str(semantic_id): str(node_id)
        for semantic_id, node_id in (causal_graph.metadata.get("semantic_to_causal_node_ids") or {}).items()
        if str(semantic_id).strip() and str(node_id).strip()
    }
    claims_by_subject: dict[str, list[AuditClaimEntry]] = {}
    for claim in claims:
        claims_by_subject.setdefault(str(claim.subject_key), []).append(claim)
    truth_ids_by_node_id: dict[str, list[str]] = {}
    for binding in causal_graph.truth_bindings:
        truth_ids_by_node_id.setdefault(binding.bound_node_id, []).append(binding.truth_id)

    enriched: list[AuditFinding] = []
    for finding in findings:
        seed_node_ids = _seed_graph_node_ids_for_finding(
            finding=finding,
            claims_by_subject=claims_by_subject,
            node_id_by_semantic_id=node_id_by_semantic_id,
        )
        attribution = _derive_graph_causal_attribution(
            seed_node_ids=seed_node_ids,
            nodes_by_id=nodes_by_id,
            outgoing=outgoing,
            incoming=incoming,
            truth_ids_by_node_id=truth_ids_by_node_id,
        )
        enriched.append(
            finding.model_copy(
                update={
                    "metadata": {
                        **finding.metadata,
                        **attribution,
                    }
                }
            )
        )
    return enriched


def _seed_entity_ids_for_finding(
    *,
    finding: AuditFinding,
    claims_by_subject: dict[str, list[AuditClaimEntry]],
) -> list[str]:
    object_key = str(finding.metadata.get("object_key") or finding.canonical_key or "").strip()
    direct_claims: list[AuditClaimEntry] = []
    if object_key:
        for subject_key, claim_group in claims_by_subject.items():
            if subject_key == object_key or subject_key.startswith(f"{object_key}."):
                direct_claims.extend(claim_group)
    direct_entity_ids = _dedupe_preserve_order(
        [
            entity_id
            for claim in direct_claims
            for entity_id in _string_list(claim.metadata.get("semantic_entity_ids"))
        ]
    )
    if direct_entity_ids:
        return direct_entity_ids
    return _dedupe_preserve_order(_string_list(finding.metadata.get("semantic_entity_ids")))


def _derive_causal_attribution(
    *,
    seed_entity_ids: list[str],
    entities_by_id: dict[str, SemanticEntity],
    adjacency: dict[str, list[tuple[str, SemanticRelation]]],
) -> dict[str, object]:
    if not seed_entity_ids:
        return {}

    queue = deque[tuple[str, list[str], list[str], float, int]]()
    visited_best_score: dict[str, float] = {}
    candidates: dict[str, dict[str, object]] = {}
    seed_types = _dedupe_preserve_order(
        [
            entity.entity_type
            for entity_id in seed_entity_ids
            for entity in [entities_by_id.get(entity_id)]
            if entity is not None
        ]
    )
    for entity_id in seed_entity_ids:
        if entity_id not in entities_by_id:
            continue
        queue.append((entity_id, [entity_id], [], 1.0, 0))
        visited_best_score[entity_id] = 1.0

    while queue:
        entity_id, path_entity_ids, relation_types, score, depth = queue.popleft()
        entity = entities_by_id.get(entity_id)
        if entity is None:
            continue
        bucket = _ENTITY_BUCKETS.get(entity.entity_type)
        if bucket is not None:
            candidate = {
                "bucket": bucket,
                "entity_id": entity.entity_id,
                "entity_label": entity.label,
                "entity_type": entity.entity_type,
                "distance": depth,
                "score": round(score, 4),
                "path": _format_path(path_entity_ids=path_entity_ids, entities_by_id=entities_by_id, relation_types=relation_types),
                "path_relation_types": list(relation_types),
            }
            existing = candidates.get(bucket)
            if existing is None or _candidate_sort_key(candidate) < _candidate_sort_key(existing):
                candidates[bucket] = candidate
        if depth >= 4:
            continue
        for neighbor_id, relation in adjacency.get(entity_id, []):
            relation_strength = _RELATION_STRENGTH.get(relation.relation_type, 0.55)
            next_score = score * float(relation.confidence or 0.5) * relation_strength
            if next_score <= visited_best_score.get(neighbor_id, 0.0):
                continue
            visited_best_score[neighbor_id] = next_score
            queue.append(
                (
                    neighbor_id,
                    [*path_entity_ids, neighbor_id],
                    [*relation_types, relation.relation_type],
                    next_score,
                    depth + 1,
                )
            )

    if not candidates:
        return {
            "causal_seed_entity_ids": seed_entity_ids,
            "causal_seed_entity_types": seed_types,
        }

    selected = min(candidates.values(), key=_candidate_sort_key)
    return {
        "causal_seed_entity_ids": seed_entity_ids,
        "causal_seed_entity_types": seed_types,
        "causal_root_cause_bucket": selected["bucket"],
        "causal_root_cause_confidence": selected["score"],
        "causal_root_cause_entity_id": selected["entity_id"],
        "causal_root_cause_entity_label": selected["entity_label"],
        "causal_root_cause_entity_type": selected["entity_type"],
        "causal_root_cause_distance": selected["distance"],
        "causal_root_cause_path": selected["path"],
        "causal_root_cause_candidates": [
            candidates[bucket]
            for bucket in sorted(candidates, key=lambda item: (_BUCKET_SELECTION_PRIORITY.get(item, 99), item))
        ],
    }


def _seed_graph_node_ids_for_finding(
    *,
    finding: AuditFinding,
    claims_by_subject: dict[str, list[AuditClaimEntry]],
    node_id_by_semantic_id: dict[str, str],
) -> list[str]:
    semantic_ids = _dedupe_preserve_order(
        [
            node_id_by_semantic_id[semantic_id]
            for semantic_id in _string_list(finding.metadata.get("semantic_entity_ids"))
            if semantic_id in node_id_by_semantic_id
        ]
    )
    if semantic_ids:
        return semantic_ids

    object_key = str(finding.metadata.get("object_key") or finding.canonical_key or "").strip()
    if not object_key:
        return []
    claim_node_ids = _dedupe_preserve_order(
        [
            node_id_by_semantic_id[semantic_id]
            for subject_key, claim_group in claims_by_subject.items()
            if subject_key == object_key or subject_key.startswith(f"{object_key}.")
            for claim in claim_group
            for semantic_id in _string_list(claim.metadata.get("semantic_entity_ids"))
            if semantic_id in node_id_by_semantic_id
        ]
    )
    return claim_node_ids


def _derive_graph_causal_attribution(
    *,
    seed_node_ids: list[str],
    nodes_by_id: dict[str, CausalGraphNode],
    outgoing: dict[str, list[CausalGraphEdge]],
    incoming: dict[str, list[CausalGraphEdge]],
    truth_ids_by_node_id: dict[str, list[str]],
) -> dict[str, object]:
    if not seed_node_ids:
        return {}

    queue = deque[tuple[str, list[str], list[str], float, int]]()
    visited_best_score: dict[str, float] = {}
    visited_paths: dict[str, dict[str, object]] = {}
    candidates: dict[str, dict[str, object]] = {}
    seed_types = _dedupe_preserve_order(
        [
            node.node_type
            for node_id in seed_node_ids
            for node in [nodes_by_id.get(node_id)]
            if node is not None
        ]
    )

    for node_id in seed_node_ids:
        if node_id not in nodes_by_id:
            continue
        queue.append((node_id, [node_id], [], 1.0, 0))
        visited_best_score[node_id] = 1.0
        visited_paths[node_id] = {"path": [node_id], "relations": [], "score": 1.0, "distance": 0}

    while queue:
        node_id, path_node_ids, relation_types, score, depth = queue.popleft()
        node = nodes_by_id.get(node_id)
        if node is None:
            continue
        bucket = _bucket_for_causal_node(node=node)
        if bucket is not None:
            candidate = {
                "bucket": bucket,
                "node_id": node.node_id,
                "node_label": node.label,
                "node_type": node.node_type,
                "node_scope_key": node.scope_key,
                "node_canonical_key": node.canonical_key,
                "distance": depth,
                "score": round(score, 4),
                "path": _format_graph_path(path_node_ids=path_node_ids, relation_types=relation_types, nodes_by_id=nodes_by_id),
                "path_node_ids": list(path_node_ids),
                "path_relation_types": list(relation_types),
            }
            existing = candidates.get(bucket)
            if existing is None or _graph_candidate_sort_key(candidate) < _graph_candidate_sort_key(existing):
                candidates[bucket] = candidate
        if depth >= 5:
            continue
        for edge, neighbor_id, direction_factor in _graph_neighbors(node_id=node_id, outgoing=outgoing, incoming=incoming):
            next_score = score * max(float(edge.strength or 0.5), 0.35) * direction_factor
            if next_score <= visited_best_score.get(neighbor_id, 0.0):
                continue
            visited_best_score[neighbor_id] = next_score
            next_path = [*path_node_ids, neighbor_id]
            next_relations = [*relation_types, edge.edge_type]
            visited_paths[neighbor_id] = {
                "path": next_path,
                "relations": next_relations,
                "score": next_score,
                "distance": depth + 1,
            }
            queue.append((neighbor_id, next_path, next_relations, next_score, depth + 1))

    selected = min(candidates.values(), key=_graph_candidate_sort_key) if candidates else None
    reachable_nodes = [nodes_by_id[node_id] for node_id in visited_paths if node_id in nodes_by_id]
    causal_scope_keys = _dedupe_preserve_order(
        [
            scope_key
            for node in reachable_nodes
            for scope_key in {node.scope_key, node.canonical_key}
            if str(scope_key or "").strip()
            and (node.write_relevant or node.decision_relevant or node.node_type not in {"document_anchor"})
        ]
    )
    related_truth_ids = _dedupe_preserve_order(
        [
            truth_id
            for node_id in visited_paths
            for truth_id in truth_ids_by_node_id.get(node_id, [])
        ]
    )
    write_decider_nodes = [node for node in reachable_nodes if node.node_type == "write_decider"]
    repository_nodes = [node for node in reachable_nodes if node.node_type == "repository_adapter"]
    driver_nodes = [node for node in reachable_nodes if node.node_type == "driver_adapter"]
    transaction_nodes = [node for node in reachable_nodes if node.node_type == "transaction_boundary"]
    retry_nodes = [node for node in reachable_nodes if node.node_type == "retry_boundary"]
    batch_nodes = [node for node in reachable_nodes if node.node_type == "batch_boundary"]
    persistence_target_nodes = [node for node in reachable_nodes if node.node_type == "persistence_target"]
    write_api_nodes = [
        node
        for node in reachable_nodes
        if node.metadata.get("api_kind") == "db_write_api"
    ]
    persistence_backends = _dedupe_preserve_order(
        [
            backend
            for node in [*write_api_nodes, *persistence_target_nodes]
            for backend in _string_list(node.metadata.get("persistence_backends"))
        ]
    )
    persistence_operation_types = _dedupe_preserve_order(
        [
            operation_type
            for node in [*write_api_nodes, *persistence_target_nodes]
            for operation_type in _string_list(node.metadata.get("persistence_operation_types"))
        ]
    )
    repository_adapter_symbols = _dedupe_preserve_order(
        [
            str(node.metadata.get("repository_adapter_symbol") or "").strip()
            for node in repository_nodes
            if str(node.metadata.get("repository_adapter_symbol") or "").strip()
        ]
    )
    driver_adapter_symbols = _dedupe_preserve_order(
        [
            str(node.metadata.get("driver_adapter_symbol") or "").strip()
            for node in driver_nodes
            if str(node.metadata.get("driver_adapter_symbol") or "").strip()
        ]
    )
    persistence_schema_targets = _dedupe_preserve_order(
        [
            schema_target
            for node in persistence_target_nodes
            for schema_target in _string_list(node.metadata.get("persistence_schema_targets"))
        ]
    )
    schema_validated_targets = _dedupe_preserve_order(
        [
            target
            for node in persistence_target_nodes
            for target in _string_list(node.metadata.get("schema_validated_targets"))
        ]
    )
    schema_observed_only_targets = _dedupe_preserve_order(
        [
            target
            for node in persistence_target_nodes
            for target in _string_list(node.metadata.get("schema_observed_only_targets"))
        ]
    )
    schema_unconfirmed_targets = _dedupe_preserve_order(
        [
            target
            for node in persistence_target_nodes
            for target in _string_list(node.metadata.get("schema_unconfirmed_targets"))
        ]
    )
    schema_validation_statuses = _dedupe_preserve_order(
        [
            str(node.metadata.get("schema_validation_status") or "").strip()
            for node in persistence_target_nodes
            if str(node.metadata.get("schema_validation_status") or "").strip()
        ]
    )

    if selected is None:
        return {
            "causal_seed_node_ids": seed_node_ids,
            "causal_seed_entity_types": seed_types,
            "causal_scope_keys": causal_scope_keys,
            "causal_related_truth_ids": related_truth_ids,
            "causal_write_decider_ids": [node.node_id for node in write_decider_nodes],
            "causal_write_decider_labels": [node.label for node in write_decider_nodes],
            "causal_repository_adapter_ids": [node.node_id for node in repository_nodes],
            "causal_repository_adapters": [node.label for node in repository_nodes],
            "causal_repository_adapter_symbols": repository_adapter_symbols,
            "causal_driver_adapter_ids": [node.node_id for node in driver_nodes],
            "causal_driver_adapters": [node.label for node in driver_nodes],
            "causal_driver_adapter_symbols": driver_adapter_symbols,
            "causal_transaction_boundary_ids": [node.node_id for node in transaction_nodes],
            "causal_transaction_boundaries": [node.label for node in transaction_nodes],
            "causal_retry_boundary_ids": [node.node_id for node in retry_nodes],
            "causal_retry_paths": [node.label for node in retry_nodes],
            "causal_batch_boundary_ids": [node.node_id for node in batch_nodes],
            "causal_batch_paths": [node.label for node in batch_nodes],
            "causal_write_api_ids": [node.node_id for node in write_api_nodes],
            "causal_write_apis": [node.label for node in write_api_nodes],
            "causal_persistence_target_ids": [node.node_id for node in persistence_target_nodes],
            "causal_persistence_targets": [node.label for node in persistence_target_nodes],
            "causal_persistence_sink_kinds": _dedupe_preserve_order(
                [str(node.metadata.get("sink_kind") or "").strip() for node in persistence_target_nodes]
            ),
            "causal_persistence_backends": persistence_backends,
            "causal_persistence_operation_types": persistence_operation_types,
            "causal_persistence_schema_targets": persistence_schema_targets,
            "causal_schema_validated_targets": schema_validated_targets,
            "causal_schema_observed_only_targets": schema_observed_only_targets,
            "causal_schema_unconfirmed_targets": schema_unconfirmed_targets,
            "causal_schema_validation_statuses": schema_validation_statuses,
            "causal_attribution_source": "causal_graph",
        }

    group_anchor = _group_anchor_for_candidate(candidate=selected, nodes_by_id=nodes_by_id)
    group_key = f"{selected['bucket']}:{group_anchor.canonical_key}"
    return {
        "causal_seed_node_ids": seed_node_ids,
        "causal_seed_entity_types": seed_types,
        "causal_scope_keys": causal_scope_keys,
        "causal_related_truth_ids": related_truth_ids,
        "causal_write_decider_ids": [node.node_id for node in write_decider_nodes],
        "causal_write_decider_labels": [node.label for node in write_decider_nodes],
        "causal_repository_adapter_ids": [node.node_id for node in repository_nodes],
        "causal_repository_adapters": [node.label for node in repository_nodes],
        "causal_repository_adapter_symbols": repository_adapter_symbols,
        "causal_driver_adapter_ids": [node.node_id for node in driver_nodes],
        "causal_driver_adapters": [node.label for node in driver_nodes],
        "causal_driver_adapter_symbols": driver_adapter_symbols,
        "causal_transaction_boundary_ids": [node.node_id for node in transaction_nodes],
        "causal_transaction_boundaries": [node.label for node in transaction_nodes],
        "causal_retry_boundary_ids": [node.node_id for node in retry_nodes],
        "causal_retry_paths": [node.label for node in retry_nodes],
        "causal_batch_boundary_ids": [node.node_id for node in batch_nodes],
        "causal_batch_paths": [node.label for node in batch_nodes],
        "causal_write_api_ids": [node.node_id for node in write_api_nodes],
        "causal_write_apis": [node.label for node in write_api_nodes],
        "causal_persistence_target_ids": [node.node_id for node in persistence_target_nodes],
        "causal_persistence_targets": [node.label for node in persistence_target_nodes],
        "causal_persistence_sink_kinds": _dedupe_preserve_order(
            [str(node.metadata.get("sink_kind") or "").strip() for node in persistence_target_nodes]
        ),
        "causal_persistence_backends": persistence_backends,
        "causal_persistence_operation_types": persistence_operation_types,
        "causal_persistence_schema_targets": persistence_schema_targets,
        "causal_schema_validated_targets": schema_validated_targets,
        "causal_schema_observed_only_targets": schema_observed_only_targets,
        "causal_schema_unconfirmed_targets": schema_unconfirmed_targets,
        "causal_schema_validation_statuses": schema_validation_statuses,
        "causal_attribution_source": "causal_graph",
        "causal_root_cause_bucket": selected["bucket"],
        "causal_root_cause_confidence": selected["score"],
        "causal_root_cause_entity_id": selected["node_id"],
        "causal_root_cause_entity_label": selected["node_label"],
        "causal_root_cause_entity_type": selected["node_type"],
        "causal_root_cause_distance": selected["distance"],
        "causal_root_cause_path": selected["path"],
        "causal_root_cause_scope_key": selected["node_scope_key"],
        "causal_group_key": group_key,
        "causal_group_label": group_anchor.label,
        "causal_primary_scope_key": group_anchor.scope_key,
        "causal_root_cause_candidates": [
            candidates[bucket]
            for bucket in sorted(candidates, key=lambda item: (_BUCKET_SELECTION_PRIORITY.get(item, 99), item))
        ],
    }


def _bucket_for_causal_node(*, node: CausalGraphNode) -> str | None:
    if node.node_type == "truth":
        return "truth"
    if node.node_type == "policy":
        return "policy"
    if node.node_type == "lifecycle":
        return "lifecycle"
    if node.node_type == "write_decider":
        return "implementation"
    if node.node_type in {"repository_adapter", "driver_adapter", "transaction_boundary", "retry_boundary", "batch_boundary"}:
        return "implementation"
    if node.node_type == "persistence_target":
        return None
    if node.node_type in {"write_contract", "read_contract"} or node.write_relevant:
        return "write_contract"
    if node.node_type in {"scope", "phase_scope"} or node.canonical_key == "BSM.process" or node.canonical_key.startswith("BSM.phase."):
        return "process"
    if node.node_type in {"agent", "worker", "service", "api_route", "code_anchor"}:
        return "implementation"
    return None


def _graph_neighbors(
    *,
    node_id: str,
    outgoing: dict[str, list[CausalGraphEdge]],
    incoming: dict[str, list[CausalGraphEdge]],
) -> list[tuple[CausalGraphEdge, str, float]]:
    neighbors: list[tuple[CausalGraphEdge, str, float]] = []
    for edge in outgoing.get(node_id, []):
        neighbors.append((edge, edge.target_node_id, 1.0))
    for edge in incoming.get(node_id, []):
        neighbors.append((edge, edge.source_node_id, 0.88))
    return neighbors


def _graph_adjacency(
    *,
    edges: list[CausalGraphEdge],
    reverse: bool,
) -> dict[str, list[CausalGraphEdge]]:
    adjacency: dict[str, list[CausalGraphEdge]] = {}
    for edge in edges:
        key = edge.target_node_id if reverse else edge.source_node_id
        adjacency.setdefault(key, []).append(edge)
    return adjacency


def _format_graph_path(
    *,
    path_node_ids: list[str],
    relation_types: list[str],
    nodes_by_id: dict[str, CausalGraphNode],
) -> str:
    if not path_node_ids:
        return ""
    parts: list[str] = []
    for index, node_id in enumerate(path_node_ids):
        node = nodes_by_id.get(node_id)
        parts.append(node.label if node is not None else node_id)
        if index < len(relation_types):
            parts.append(relation_types[index])
    return " -> ".join(parts)


def _group_anchor_for_candidate(
    *,
    candidate: dict[str, object],
    nodes_by_id: dict[str, CausalGraphNode],
) -> CausalGraphNode:
    path_node_ids = [
        str(node_id)
        for node_id in candidate.get("path_node_ids", [])
        if str(node_id).strip() and str(node_id) in nodes_by_id
    ]
    non_truth_nodes = [
        nodes_by_id[node_id]
        for node_id in path_node_ids
        if nodes_by_id[node_id].node_type != "truth"
    ]
    if non_truth_nodes:
        return non_truth_nodes[-1] if candidate.get("bucket") == "truth" else non_truth_nodes[0]
    node_id = str(candidate.get("node_id") or "")
    return nodes_by_id[node_id]


def _graph_candidate_sort_key(candidate: dict[str, object]) -> tuple[int, int, float, str]:
    bucket = str(candidate.get("bucket") or "")
    distance = int(candidate.get("distance") or 99)
    score = float(candidate.get("score") or 0.0)
    node_label = str(candidate.get("node_label") or "")
    return (
        _BUCKET_SELECTION_PRIORITY.get(bucket, 99),
        distance,
        -score,
        node_label,
    )


def _candidate_sort_key(candidate: dict[str, object]) -> tuple[int, int, float, str]:
    bucket = str(candidate.get("bucket") or "")
    distance = int(candidate.get("distance") or 99)
    score = float(candidate.get("score") or 0.0)
    entity_label = str(candidate.get("entity_label") or "")
    return (
        _BUCKET_SELECTION_PRIORITY.get(bucket, 99),
        distance,
        -score,
        entity_label,
    )


def _format_path(
    *,
    path_entity_ids: list[str],
    entities_by_id: dict[str, SemanticEntity],
    relation_types: list[str],
) -> str:
    if not path_entity_ids:
        return ""
    parts: list[str] = []
    for index, entity_id in enumerate(path_entity_ids):
        entity = entities_by_id.get(entity_id)
        parts.append(entity.label if entity is not None else entity_id)
        if index < len(relation_types):
            parts.append(relation_types[index])
    return " -> ".join(parts)


def _build_undirected_adjacency(
    *,
    relations: list[SemanticRelation],
) -> dict[str, list[tuple[str, SemanticRelation]]]:
    adjacency: dict[str, list[tuple[str, SemanticRelation]]] = {}
    for relation in relations:
        adjacency.setdefault(relation.source_entity_id, []).append((relation.target_entity_id, relation))
        adjacency.setdefault(relation.target_entity_id, []).append((relation.source_entity_id, relation))
    return adjacency


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out
