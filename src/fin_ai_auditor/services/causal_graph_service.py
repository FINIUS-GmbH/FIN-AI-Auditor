from __future__ import annotations

from collections import defaultdict, deque
import re

from fin_ai_auditor.domain.models import AuditClaimEntry, SemanticEntity, SemanticRelation, TruthLedgerEntry
from fin_ai_auditor.services.causal_graph_models import (
    CausalGraph,
    CausalGraphEdge,
    CausalGraphNode,
    CausalNodeLayer,
    CausalNodeType,
    CausalGraphTruthBinding,
    CausalPropagationFrame,
)
from fin_ai_auditor.services.claim_semantics import package_scope_key


_NODE_LAYER_BY_TYPE: dict[str, CausalNodeLayer] = {
    "truth": "truth",
    "policy": "governance",
    "lifecycle": "governance",
    "write_contract": "governance",
    "read_contract": "governance",
    "persistence_target": "storage",
    "process": "process",
    "phase": "process",
    "question": "process",
    "object": "process",
    "documentation_section": "evidence",
    "code_component": "runtime",
    "write_decider": "runtime",
    "repository_adapter": "runtime",
    "driver_adapter": "runtime",
    "transaction_boundary": "runtime",
    "retry_boundary": "runtime",
    "batch_boundary": "runtime",
}


def build_causal_graph(
    *,
    run_id: str,
    claims: list[AuditClaimEntry],
    truths: list[TruthLedgerEntry],
    semantic_entities: list[SemanticEntity],
    semantic_relations: list[SemanticRelation],
) -> CausalGraph:
    node_map: dict[str, CausalGraphNode] = {}
    semantic_to_causal_node_ids: dict[str, str] = {}
    edge_map: dict[tuple[str, str, str], CausalGraphEdge] = {}

    for entity in semantic_entities:
        node = _ensure_node_from_semantic_entity(
            node_map=node_map,
            run_id=run_id,
            entity=entity,
        )
        semantic_to_causal_node_ids[entity.entity_id] = node.node_id

    nodes_by_id = {node.node_id: node for node in node_map.values()}
    for relation in semantic_relations:
        source_node_id = semantic_to_causal_node_ids.get(relation.source_entity_id)
        target_node_id = semantic_to_causal_node_ids.get(relation.target_entity_id)
        if not source_node_id or not target_node_id:
            continue
        source_node = nodes_by_id.get(source_node_id)
        target_node = nodes_by_id.get(target_node_id)
        if source_node is None or target_node is None:
            continue
        for edge in _edges_for_semantic_relation(
            run_id=run_id,
            relation=relation,
            source_node=source_node,
            target_node=target_node,
        ):
            _merge_edge(edge_map=edge_map, edge=edge)
    _inject_write_runtime_nodes_and_edges(
        run_id=run_id,
        claims=claims,
        node_map=node_map,
        edge_map=edge_map,
        semantic_to_causal_node_ids=semantic_to_causal_node_ids,
    )

    truth_bindings: list[CausalGraphTruthBinding] = []
    for truth in truths:
        if truth.truth_status != "active":
            continue
        truth_node = _ensure_truth_node(node_map=node_map, run_id=run_id, truth=truth)
        subject_node = _ensure_subject_node(node_map=node_map, run_id=run_id, subject_key=truth.subject_key)
        _merge_edge(
            edge_map=edge_map,
            edge=CausalGraphEdge(
                run_id=run_id,
                source_node_id=truth_node.node_id,
                target_node_id=subject_node.node_id,
                edge_type="propagates_truth_to",
                propagation_mode="truth_and_delta",
                strength=1.0,
                truth_relevant=True,
                write_relevant=subject_node.write_relevant,
                metadata={"truth_id": truth.truth_id, "subject_key": truth.subject_key},
            ),
        )
        truth_bindings.append(
            CausalGraphTruthBinding(
                truth_id=truth.truth_id,
                truth_canonical_key=truth.canonical_key,
                bound_node_id=subject_node.node_id,
                predicate=truth.predicate,
                propagation_mode="truth_and_delta",
                confidence=1.0 if _is_explicit_truth(truth=truth) else 0.84,
                metadata={
                    "subject_key": truth.subject_key,
                    "scope_key": package_scope_key(truth.subject_key),
                    "truth_delta_status": truth.metadata.get("truth_delta_status"),
                    "pending_delta_recalculation": truth.metadata.get("pending_delta_recalculation"),
                    "truth_delta_retrigger": truth.metadata.get("truth_delta_retrigger"),
                },
            )
        )

    propagation_frames = _build_truth_propagation_frames(
        graph_nodes=list(node_map.values()),
        graph_edges=list(edge_map.values()),
        truth_bindings=truth_bindings,
    )

    return CausalGraph(
        run_id=run_id,
        nodes=sorted(node_map.values(), key=lambda item: (item.scope_key, item.layer, item.label)),
        edges=sorted(edge_map.values(), key=lambda item: (item.source_node_id, item.target_node_id, item.edge_type)),
        truth_bindings=truth_bindings,
        propagation_frames=propagation_frames,
        metadata={
            "semantic_to_causal_node_ids": semantic_to_causal_node_ids,
            "claim_subjects": sorted({claim.subject_key for claim in claims}),
            "truth_subjects": sorted({truth.subject_key for truth in truths if truth.truth_status == "active"}),
            "focus": "write_dependency_graph",
        },
    )


def expand_impacted_scope_keys(
    *,
    graph: CausalGraph,
    seed_scope_keys: set[str],
    truths: list[TruthLedgerEntry],
    max_depth: int = 5,
) -> set[str]:
    nodes_by_id = {node.node_id: node for node in graph.nodes}
    outgoing = _outgoing_edges(edges=graph.edges)
    normalized_seed_scope_keys = {
        str(scope_key or "").strip()
        for scope_key in seed_scope_keys
        if str(scope_key or "").strip()
    }
    queue: deque[tuple[str, int]] = deque()
    visited: set[str] = set()

    for node in graph.nodes:
        if any(_scope_keys_overlap(left=node.scope_key, right=scope_key) for scope_key in normalized_seed_scope_keys):
            queue.append((node.node_id, 0))
            visited.add(node.node_id)

    truth_bindings_by_id = {binding.truth_id: binding for binding in graph.truth_bindings}
    for truth in truths:
        if not _truth_requires_delta_recalculation(truth=truth):
            continue
        binding = truth_bindings_by_id.get(truth.truth_id)
        if binding is None:
            continue
        if binding.bound_node_id not in visited:
            queue.append((binding.bound_node_id, 0))
            visited.add(binding.bound_node_id)

    impacted_scope_keys = set(normalized_seed_scope_keys)
    while queue:
        node_id, depth = queue.popleft()
        current_node = nodes_by_id.get(node_id)
        if current_node is None:
            continue
        impacted_scope_keys.add(current_node.scope_key)
        impacted_scope_keys.add(package_scope_key(current_node.canonical_key))
        if depth >= max_depth:
            continue
        for edge in outgoing.get(node_id, []):
            if edge.propagation_mode == "none":
                continue
            if edge.propagation_mode == "truth_only" and not edge.truth_relevant:
                continue
            if edge.target_node_id in visited:
                continue
            visited.add(edge.target_node_id)
            queue.append((edge.target_node_id, depth + 1))
    return {scope_key for scope_key in impacted_scope_keys if str(scope_key or "").strip()}


def _ensure_node_from_semantic_entity(
    *,
    node_map: dict[str, CausalGraphNode],
    run_id: str,
    entity: SemanticEntity,
) -> CausalGraphNode:
    existing = node_map.get(entity.canonical_key)
    if existing is not None:
        merged_metadata = {**existing.metadata, **entity.metadata, "semantic_entity_id": entity.entity_id}
        updated = existing.model_copy(update={"metadata": merged_metadata})
        node_map[entity.canonical_key] = updated
        return updated

    node_type = _causal_node_type_for_semantic_entity(entity=entity)
    node = CausalGraphNode(
        run_id=run_id,
        node_type=node_type,
        layer=_NODE_LAYER_BY_TYPE.get(entity.entity_type, "runtime"),  # type: ignore[arg-type]
        canonical_key=entity.canonical_key,
        label=entity.label,
        scope_key=entity.scope_key,
        write_relevant=_is_write_relevant(entity_type=entity.entity_type, canonical_key=entity.canonical_key),
        decision_relevant=_is_decision_relevant(entity_type=entity.entity_type, canonical_key=entity.canonical_key),
        metadata={**entity.metadata, "semantic_entity_id": entity.entity_id, "semantic_entity_type": entity.entity_type},
    )
    node_map[entity.canonical_key] = node
    return node


def _ensure_truth_node(
    *,
    node_map: dict[str, CausalGraphNode],
    run_id: str,
    truth: TruthLedgerEntry,
) -> CausalGraphNode:
    canonical_key = f"truth:{truth.canonical_key}"
    existing = node_map.get(canonical_key)
    if existing is not None:
        return existing
    node = CausalGraphNode(
        run_id=run_id,
        node_type="truth",
        layer="truth",
        canonical_key=canonical_key,
        label=truth.canonical_key,
        scope_key=package_scope_key(truth.subject_key),
        decision_relevant=True,
        metadata={"truth_id": truth.truth_id, "predicate": truth.predicate, "subject_key": truth.subject_key},
    )
    node_map[canonical_key] = node
    return node


def _ensure_subject_node(
    *,
    node_map: dict[str, CausalGraphNode],
    run_id: str,
    subject_key: str,
) -> CausalGraphNode:
    existing = node_map.get(subject_key)
    if existing is not None:
        return existing
    node_type, layer = _fallback_subject_node_type(subject_key=subject_key)
    node = CausalGraphNode(
        run_id=run_id,
        node_type=node_type,
        layer=layer,
        canonical_key=subject_key,
        label=subject_key,
        scope_key=package_scope_key(subject_key),
        write_relevant=_is_write_relevant(entity_type=node_type, canonical_key=subject_key),
        decision_relevant=_is_decision_relevant(entity_type=node_type, canonical_key=subject_key),
    )
    node_map[subject_key] = node
    return node


def _edges_for_semantic_relation(
    *,
    run_id: str,
    relation: SemanticRelation,
    source_node: CausalGraphNode,
    target_node: CausalGraphNode,
) -> list[CausalGraphEdge]:
    confidence = max(float(relation.confidence or 0.5), 0.4)
    metadata = {**relation.metadata, "semantic_relation_id": relation.relation_id}
    if relation.relation_type == "derived_from_truth":
        return [
            CausalGraphEdge(
                run_id=run_id,
                source_node_id=source_node.node_id,
                target_node_id=target_node.node_id,
                edge_type="propagates_truth_to",
                propagation_mode="truth_and_delta",
                strength=confidence,
                truth_relevant=True,
                write_relevant=target_node.write_relevant,
                metadata=metadata,
            )
        ]
    if relation.relation_type == "governs":
        return [
            CausalGraphEdge(
                run_id=run_id,
                source_node_id=source_node.node_id,
                target_node_id=target_node.node_id,
                edge_type="propagates_truth_to",
                propagation_mode="truth_and_delta",
                strength=confidence,
                truth_relevant=True,
                write_relevant=True,
                metadata=metadata,
            ),
            CausalGraphEdge(
                run_id=run_id,
                source_node_id=target_node.node_id,
                target_node_id=source_node.node_id,
                edge_type="gated_by",
                propagation_mode="delta_only",
                strength=max(confidence - 0.02, 0.4),
                blocking=True,
                write_relevant=True,
                truth_relevant=True,
                metadata=metadata,
            ),
        ]
    if relation.relation_type == "contains":
        return [
            CausalGraphEdge(
                run_id=run_id,
                source_node_id=source_node.node_id,
                target_node_id=target_node.node_id,
                edge_type="propagates_truth_to",
                propagation_mode="truth_and_delta",
                strength=confidence,
                truth_relevant=source_node.decision_relevant,
                write_relevant=source_node.write_relevant or target_node.write_relevant,
                metadata=metadata,
            ),
            CausalGraphEdge(
                run_id=run_id,
                source_node_id=target_node.node_id,
                target_node_id=source_node.node_id,
                edge_type="depends_on",
                propagation_mode="delta_only",
                strength=max(confidence - 0.04, 0.4),
                truth_relevant=source_node.decision_relevant,
                write_relevant=source_node.write_relevant or target_node.write_relevant,
                metadata=metadata,
            ),
        ]
    if relation.relation_type == "belongs_to":
        return [
            CausalGraphEdge(
                run_id=run_id,
                source_node_id=source_node.node_id,
                target_node_id=target_node.node_id,
                edge_type="depends_on",
                propagation_mode="truth_and_delta",
                strength=confidence,
                truth_relevant=target_node.decision_relevant,
                write_relevant=source_node.write_relevant or target_node.write_relevant,
                metadata=metadata,
            )
        ]
    if relation.relation_type == "references":
        return [
            CausalGraphEdge(
                run_id=run_id,
                source_node_id=source_node.node_id,
                target_node_id=target_node.node_id,
                edge_type="depends_on",
                propagation_mode="delta_only",
                strength=confidence,
                truth_relevant=target_node.decision_relevant,
                write_relevant=source_node.write_relevant or target_node.write_relevant,
                metadata=metadata,
            )
        ]
    if relation.relation_type == "implements":
        edges = [
            CausalGraphEdge(
                run_id=run_id,
                source_node_id=target_node.node_id,
                target_node_id=source_node.node_id,
                edge_type="implemented_by",
                propagation_mode="truth_and_delta",
                strength=confidence,
                truth_relevant=target_node.decision_relevant,
                write_relevant=target_node.write_relevant,
                metadata=metadata,
            )
        ]
        if target_node.write_relevant:
            edges.append(
                CausalGraphEdge(
                    run_id=run_id,
                    source_node_id=source_node.node_id,
                    target_node_id=target_node.node_id,
                    edge_type="decides_write",
                    propagation_mode="truth_and_delta",
                    strength=max(confidence - 0.02, 0.4),
                    truth_relevant=True,
                    write_relevant=True,
                    metadata=metadata,
                )
            )
        return edges
    if relation.relation_type == "documents":
        return [
            CausalGraphEdge(
                run_id=run_id,
                source_node_id=target_node.node_id,
                target_node_id=source_node.node_id,
                edge_type="evidenced_by",
                propagation_mode="none",
                strength=confidence,
                metadata=metadata,
            )
        ]
    return []


def _inject_write_runtime_nodes_and_edges(
    *,
    run_id: str,
    claims: list[AuditClaimEntry],
    node_map: dict[str, CausalGraphNode],
    edge_map: dict[tuple[str, str, str], CausalGraphEdge],
    semantic_to_causal_node_ids: dict[str, str],
) -> None:
    for claim in claims:
        if not _claim_drives_persistence(claim=claim):
            continue
        target_node = _resolve_write_target_node(
            claim=claim,
            node_map=node_map,
            semantic_to_causal_node_ids=semantic_to_causal_node_ids,
        )
        if target_node is None:
            continue

        decider_node = _ensure_write_decider_node(
            node_map=node_map,
            run_id=run_id,
            claim=claim,
            target_node=target_node,
        )
        write_api_node = _ensure_write_api_node(
            node_map=node_map,
            run_id=run_id,
            claim=claim,
            decider_node=decider_node,
        )
        repository_nodes = _ensure_runtime_chain_nodes(
            node_map=node_map,
            run_id=run_id,
            claim=claim,
            labels=_string_list(claim.metadata.get("repository_adapters")),
            symbols=_string_list(claim.metadata.get("repository_adapter_symbols")),
            node_type="repository_adapter",
            metadata_key="repository_adapter",
        )
        driver_nodes = _ensure_runtime_chain_nodes(
            node_map=node_map,
            run_id=run_id,
            claim=claim,
            labels=_string_list(claim.metadata.get("driver_adapters")),
            symbols=_string_list(claim.metadata.get("driver_adapter_symbols")),
            node_type="driver_adapter",
            metadata_key="driver_adapter",
        )
        transaction_nodes = _ensure_runtime_chain_nodes(
            node_map=node_map,
            run_id=run_id,
            claim=claim,
            labels=_string_list(claim.metadata.get("transaction_boundaries")),
            node_type="transaction_boundary",
            metadata_key="transaction_boundary",
        )
        retry_nodes = _ensure_runtime_chain_nodes(
            node_map=node_map,
            run_id=run_id,
            claim=claim,
            labels=_string_list(claim.metadata.get("retry_paths")),
            node_type="retry_boundary",
            metadata_key="retry_boundary",
        )
        batch_nodes = _ensure_runtime_chain_nodes(
            node_map=node_map,
            run_id=run_id,
            claim=claim,
            labels=_string_list(claim.metadata.get("batch_paths")),
            node_type="batch_boundary",
            metadata_key="batch_boundary",
        )
        persistence_target_node = _ensure_persistence_target_node(
            node_map=node_map,
            run_id=run_id,
            claim=claim,
            target_node=target_node,
        )
        code_node = _resolve_runtime_code_node(
            claim=claim,
            node_map=node_map,
            semantic_to_causal_node_ids=semantic_to_causal_node_ids,
        )

        _merge_edge(
            edge_map=edge_map,
            edge=CausalGraphEdge(
                run_id=run_id,
                source_node_id=decider_node.node_id,
                target_node_id=target_node.node_id,
                edge_type="decides_write",
                propagation_mode="truth_and_delta",
                strength=0.93,
                write_relevant=True,
                truth_relevant=True,
                metadata={"claim_id": claim.claim_id, "subject_key": claim.subject_key},
            ),
        )
        _merge_edge(
            edge_map=edge_map,
            edge=CausalGraphEdge(
                run_id=run_id,
                source_node_id=target_node.node_id,
                target_node_id=decider_node.node_id,
                edge_type="implemented_by",
                propagation_mode="truth_and_delta",
                strength=0.9,
                write_relevant=True,
                truth_relevant=True,
                metadata={"claim_id": claim.claim_id, "subject_key": claim.subject_key},
            ),
        )
        _merge_edge(
            edge_map=edge_map,
            edge=CausalGraphEdge(
                run_id=run_id,
                source_node_id=target_node.node_id,
                target_node_id=persistence_target_node.node_id,
                edge_type="writes_to",
                propagation_mode="truth_and_delta",
                strength=0.95,
                write_relevant=True,
                truth_relevant=True,
                metadata={"claim_id": claim.claim_id, "subject_key": claim.subject_key},
            ),
        )
        previous_nodes: list[CausalGraphNode] = [decider_node]
        for runtime_chain_nodes in (batch_nodes, retry_nodes, transaction_nodes, repository_nodes, driver_nodes):
            if not runtime_chain_nodes:
                continue
            next_head = runtime_chain_nodes[0]
            for previous_node in previous_nodes:
                _merge_edge(
                    edge_map=edge_map,
                    edge=CausalGraphEdge(
                        run_id=run_id,
                        source_node_id=previous_node.node_id,
                        target_node_id=next_head.node_id,
                        edge_type="depends_on",
                        propagation_mode="truth_and_delta",
                        strength=0.9,
                        write_relevant=True,
                        truth_relevant=True,
                        metadata={"claim_id": claim.claim_id, "subject_key": claim.subject_key},
                    ),
                )
            for left_node, right_node in zip(runtime_chain_nodes, runtime_chain_nodes[1:], strict=False):
                _merge_edge(
                    edge_map=edge_map,
                    edge=CausalGraphEdge(
                        run_id=run_id,
                        source_node_id=left_node.node_id,
                        target_node_id=right_node.node_id,
                        edge_type="depends_on",
                        propagation_mode="truth_and_delta",
                        strength=0.88,
                        write_relevant=True,
                        truth_relevant=True,
                        metadata={"claim_id": claim.claim_id, "subject_key": claim.subject_key},
                    ),
                )
            previous_nodes = runtime_chain_nodes
        _merge_edge(
            edge_map=edge_map,
            edge=CausalGraphEdge(
                run_id=run_id,
                source_node_id=(previous_nodes[-1].node_id if previous_nodes else decider_node.node_id),
                target_node_id=write_api_node.node_id,
                edge_type="materializes",
                propagation_mode="truth_and_delta",
                strength=0.89,
                write_relevant=True,
                truth_relevant=True,
                metadata={"claim_id": claim.claim_id, "subject_key": claim.subject_key},
            ),
        )
        _merge_edge(
            edge_map=edge_map,
            edge=CausalGraphEdge(
                run_id=run_id,
                source_node_id=write_api_node.node_id,
                target_node_id=persistence_target_node.node_id,
                edge_type="writes_to",
                propagation_mode="truth_and_delta",
                strength=0.97,
                write_relevant=True,
                truth_relevant=True,
                metadata={"claim_id": claim.claim_id, "subject_key": claim.subject_key},
            ),
        )
        _merge_edge(
            edge_map=edge_map,
            edge=CausalGraphEdge(
                run_id=run_id,
                source_node_id=(previous_nodes[-1].node_id if previous_nodes else decider_node.node_id),
                target_node_id=persistence_target_node.node_id,
                edge_type="writes_to",
                propagation_mode="delta_only",
                strength=0.92,
                write_relevant=True,
                metadata={"claim_id": claim.claim_id, "subject_key": claim.subject_key},
            ),
        )
        if code_node is not None:
            _merge_edge(
                edge_map=edge_map,
                edge=CausalGraphEdge(
                    run_id=run_id,
                    source_node_id=decider_node.node_id,
                    target_node_id=code_node.node_id,
                    edge_type="materializes",
                    propagation_mode="delta_only",
                    strength=0.84,
                    write_relevant=True,
                    metadata={"claim_id": claim.claim_id, "subject_key": claim.subject_key},
                ),
            )
            _merge_edge(
                edge_map=edge_map,
                edge=CausalGraphEdge(
                    run_id=run_id,
                    source_node_id=code_node.node_id,
                    target_node_id=decider_node.node_id,
                    edge_type="triggered_by",
                    propagation_mode="delta_only",
                    strength=0.8,
                    write_relevant=True,
                    metadata={"claim_id": claim.claim_id, "subject_key": claim.subject_key},
                ),
            )


def _claim_drives_persistence(*, claim: AuditClaimEntry) -> bool:
    descriptor = " ".join(
        [
            str(claim.subject_key or ""),
            str(claim.predicate or ""),
            str(claim.normalized_value or ""),
            str(claim.metadata.get("matched_text") or ""),
            " ".join(_string_list(claim.metadata.get("semantic_contract_paths"))),
            " ".join(_string_list(claim.metadata.get("db_write_api_calls"))),
            " ".join(_string_list(claim.metadata.get("persistence_operation_types"))),
        ]
    ).casefold()
    return (
        claim.source_type == "github_file"
        and (
            claim.subject_key.endswith(".write_path")
            or "implemented_write" in claim.predicate
            or any(token in descriptor for token in ("persist", "write", "save", "upsert", "merge", "patch"))
        )
    )


def _resolve_write_target_node(
    *,
    claim: AuditClaimEntry,
    node_map: dict[str, CausalGraphNode],
    semantic_to_causal_node_ids: dict[str, str],
) -> CausalGraphNode | None:
    direct = node_map.get(claim.subject_key)
    if direct is not None:
        return direct
    for semantic_entity_id in _string_list(claim.metadata.get("semantic_entity_ids")):
        node_id = semantic_to_causal_node_ids.get(semantic_entity_id)
        if not node_id:
            continue
        for node in node_map.values():
            if node.node_id == node_id and node.write_relevant:
                return node
    return None


def _resolve_runtime_code_node(
    *,
    claim: AuditClaimEntry,
    node_map: dict[str, CausalGraphNode],
    semantic_to_causal_node_ids: dict[str, str],
) -> CausalGraphNode | None:
    for semantic_entity_id in _string_list(claim.metadata.get("semantic_entity_ids")):
        node_id = semantic_to_causal_node_ids.get(semantic_entity_id)
        if not node_id:
            continue
        for node in node_map.values():
            if node.node_id == node_id and node.node_type in {"agent", "worker", "service", "api_route", "code_anchor"}:
                return node
    return None


def _ensure_write_decider_node(
    *,
    node_map: dict[str, CausalGraphNode],
    run_id: str,
    claim: AuditClaimEntry,
    target_node: CausalGraphNode,
) -> CausalGraphNode:
    section_path = str(claim.metadata.get("evidence_section_path") or claim.metadata.get("claim_section_path") or "").strip()
    runtime_hint = _runtime_kind_hint(claim=claim, section_path=section_path)
    descriptor = section_path or str(claim.metadata.get("path_hint") or claim.source_id or claim.subject_key)
    canonical_key = f"write_decider:{claim.source_id}:{descriptor}"
    existing = node_map.get(canonical_key)
    if existing is not None:
        return existing
    node = CausalGraphNode(
        run_id=run_id,
        node_type="write_decider",
        layer="runtime",
        canonical_key=canonical_key,
        label=descriptor,
        scope_key=package_scope_key(claim.subject_key),
        write_relevant=True,
        decision_relevant=True,
        metadata={
            "subject_key": claim.subject_key,
            "runtime_type": runtime_hint,
            "source_id": claim.source_id,
            "path_hint": claim.metadata.get("path_hint"),
            "target_canonical_key": target_node.canonical_key,
            "claim_id": claim.claim_id,
        },
    )
    node_map[canonical_key] = node
    return node


def _ensure_runtime_chain_nodes(
    *,
    node_map: dict[str, CausalGraphNode],
    run_id: str,
    claim: AuditClaimEntry,
    labels: list[str],
    symbols: list[str] | None = None,
    node_type: str,
    metadata_key: str,
) -> list[CausalGraphNode]:
    nodes: list[CausalGraphNode] = []
    resolved_symbols = symbols or []
    for index, label in enumerate(labels):
        normalized_label = str(label or "").strip()
        if not normalized_label:
            continue
        normalized_symbol = str(resolved_symbols[index] or "").strip() if index < len(resolved_symbols) else ""
        canonical_reference = normalized_symbol or normalized_label
        canonical_key = f"{node_type}:{claim.source_id}:{canonical_reference}"
        existing = node_map.get(canonical_key)
        if existing is not None:
            if normalized_symbol:
                existing.metadata[f"{metadata_key}_symbol"] = normalized_symbol
            nodes.append(existing)
            continue
        node = CausalGraphNode(
            run_id=run_id,
            node_type=node_type,  # type: ignore[arg-type]
            layer="runtime",
            canonical_key=canonical_key,
            label=normalized_label,
            scope_key=package_scope_key(claim.subject_key),
            write_relevant=True,
            decision_relevant=node_type in {"repository_adapter", "driver_adapter", "transaction_boundary"},
            metadata={
                "subject_key": claim.subject_key,
                "source_id": claim.source_id,
                "claim_id": claim.claim_id,
                metadata_key: normalized_label,
                f"{metadata_key}_symbol": normalized_symbol or None,
            },
        )
        node_map[canonical_key] = node
        nodes.append(node)
    return nodes


def _ensure_persistence_target_node(
    *,
    node_map: dict[str, CausalGraphNode],
    run_id: str,
    claim: AuditClaimEntry,
    target_node: CausalGraphNode,
) -> CausalGraphNode:
    target_key, sink_kind = _persistence_target_key(claim=claim, target_node=target_node)
    persistence_backends = _string_list(claim.metadata.get("persistence_backends"))
    persistence_operation_types = _string_list(claim.metadata.get("persistence_operation_types"))
    persistence_schema_targets = _string_list(claim.metadata.get("persistence_schema_targets"))
    schema_validated_targets = _string_list(claim.metadata.get("schema_validated_targets"))
    schema_observed_only_targets = _string_list(claim.metadata.get("schema_observed_only_targets"))
    schema_unconfirmed_targets = _string_list(claim.metadata.get("schema_unconfirmed_targets"))
    schema_validation_status = str(claim.metadata.get("schema_validation_status") or "").strip()
    canonical_key = f"persistence_target:{target_key}"
    existing = node_map.get(canonical_key)
    if existing is not None:
        merged_metadata = {
            **existing.metadata,
            "persistence_backends": _dedupe_preserve_order(
                [*_string_list(existing.metadata.get("persistence_backends")), *persistence_backends]
            ),
            "persistence_operation_types": _dedupe_preserve_order(
                [*_string_list(existing.metadata.get("persistence_operation_types")), *persistence_operation_types]
            ),
            "persistence_schema_targets": _dedupe_preserve_order(
                [*_string_list(existing.metadata.get("persistence_schema_targets")), *persistence_schema_targets]
            ),
            "schema_validated_targets": _dedupe_preserve_order(
                [*_string_list(existing.metadata.get("schema_validated_targets")), *schema_validated_targets]
            ),
            "schema_observed_only_targets": _dedupe_preserve_order(
                [*_string_list(existing.metadata.get("schema_observed_only_targets")), *schema_observed_only_targets]
            ),
            "schema_unconfirmed_targets": _dedupe_preserve_order(
                [*_string_list(existing.metadata.get("schema_unconfirmed_targets")), *schema_unconfirmed_targets]
            ),
            "schema_validation_status": schema_validation_status or str(existing.metadata.get("schema_validation_status") or "").strip(),
        }
        updated = existing.model_copy(update={"metadata": merged_metadata})
        node_map[canonical_key] = updated
        return updated
    node = CausalGraphNode(
        run_id=run_id,
        node_type="persistence_target",
        layer="storage",
        canonical_key=canonical_key,
        label=target_key,
        scope_key=package_scope_key(claim.subject_key),
        write_relevant=True,
        decision_relevant=False,
        metadata={
            "subject_key": claim.subject_key,
            "target_canonical_key": target_node.canonical_key,
            "claim_id": claim.claim_id,
            "sink_kind": sink_kind,
            "persistence_backends": persistence_backends,
            "persistence_operation_types": persistence_operation_types,
            "persistence_schema_targets": persistence_schema_targets,
            "schema_validated_targets": schema_validated_targets,
            "schema_observed_only_targets": schema_observed_only_targets,
            "schema_unconfirmed_targets": schema_unconfirmed_targets,
            "schema_validation_status": schema_validation_status,
        },
    )
    node_map[canonical_key] = node
    return node


def _ensure_write_api_node(
    *,
    node_map: dict[str, CausalGraphNode],
    run_id: str,
    claim: AuditClaimEntry,
    decider_node: CausalGraphNode,
) -> CausalGraphNode:
    api_label = _write_api_label(claim=claim, fallback_label=decider_node.label)
    persistence_backends = _string_list(claim.metadata.get("persistence_backends"))
    persistence_operation_types = _string_list(claim.metadata.get("persistence_operation_types"))
    canonical_key = f"write_api:{claim.source_id}:{api_label}"
    existing = node_map.get(canonical_key)
    if existing is not None:
        return existing
    node = CausalGraphNode(
        run_id=run_id,
        node_type="code_anchor",
        layer="runtime",
        canonical_key=canonical_key,
        label=api_label,
        scope_key=package_scope_key(claim.subject_key),
        write_relevant=True,
        decision_relevant=True,
        metadata={
            "subject_key": claim.subject_key,
            "source_id": claim.source_id,
            "claim_id": claim.claim_id,
            "api_kind": "db_write_api",
            "api_call_pattern": api_label,
            "persistence_backends": persistence_backends,
            "persistence_operation_types": persistence_operation_types,
        },
    )
    node_map[canonical_key] = node
    return node


def _runtime_kind_hint(*, claim: AuditClaimEntry, section_path: str) -> str:
    descriptor = " ".join(
        [
            str(claim.source_id or ""),
            str(claim.metadata.get("path_hint") or ""),
            section_path,
        ]
    ).casefold()
    if any(token in descriptor for token in ("a3b", "a4", "agent", "/agents/")):
        return "agent"
    if "worker" in descriptor or "/workers/" in descriptor:
        return "worker"
    if "router" in descriptor or "/routes/" in descriptor or "/api/" in descriptor:
        return "api_route"
    if "service" in descriptor or "/services/" in descriptor:
        return "service"
    return "code_anchor"


def _persistence_target_key(*, claim: AuditClaimEntry, target_node: CausalGraphNode) -> tuple[str, str]:
    schema_targets = _string_list(claim.metadata.get("persistence_schema_targets"))
    if schema_targets:
        mapped = _map_schema_target_to_sink(schema_target=schema_targets[0])
        if mapped is not None:
            return mapped
    descriptor = " ".join(
        [
            str(claim.subject_key or ""),
            str(claim.normalized_value or ""),
            str(claim.metadata.get("matched_text") or ""),
            str(claim.metadata.get("evidence_section_path") or ""),
        ]
    ).casefold()
    if "relationship" in descriptor:
        return "CustomerGraph.Relationship", "relationship_sink"
    if any(token in descriptor for token in ("historic", "history", "version", "historisierung", "audit_trail", "historic:modified")):
        owner_key = claim.subject_key.split(".", 1)[0]
        return f"CustomerGraph.History.{owner_key}", "history_sink"
    if "bsm_element" in descriptor or "bsm element" in descriptor:
        return "CustomerGraph.Node.BSM_Element", "node_sink"
    if target_node.canonical_key.startswith("BSM.phase."):
        return f"CustomerGraph.Node.{target_node.canonical_key}", "node_sink"
    if target_node.canonical_key.endswith(".write_path"):
        owner_key = target_node.canonical_key.rsplit(".write_path", 1)[0]
        return f"CustomerGraph.Node.{owner_key}", "node_sink"
    owner_key = claim.subject_key.split(".", 1)[0]
    return f"CustomerGraph.Node.{owner_key}", "node_sink"


def _write_api_label(*, claim: AuditClaimEntry, fallback_label: str) -> str:
    explicit_api_calls = _string_list(claim.metadata.get("db_write_api_calls"))
    if explicit_api_calls:
        return explicit_api_calls[0]
    descriptor = " ".join(
        [
            str(claim.normalized_value or ""),
            str(claim.metadata.get("matched_text") or ""),
            str(claim.metadata.get("evidence_section_path") or ""),
        ]
    )
    explicit_match = _extract_db_write_api_call(descriptor=descriptor)
    if explicit_match is not None:
        return explicit_match
    match = next(
        (
            token
            for token in (
                "upsert",
                "merge",
                "save",
                "persist",
                "write",
                "patch",
                "create_relationship",
                "merge_relationship",
            )
            if token in descriptor.casefold()
        ),
        None,
    )
    if match is not None:
        return match
    return fallback_label.rsplit(".", 1)[-1] if "." in fallback_label else fallback_label


def _extract_db_write_api_call(*, descriptor: str) -> str | None:
    for pattern in (
        r"\b([A-Za-z_][A-Za-z0-9_]*\.(?:save|persist|upsert|merge|patch|write|create|create_relationship|merge_relationship))\b",
        r"\b([A-Za-z_][A-Za-z0-9_]*\.(?:run|execute_query|execute_write|query))\b",
        r"\b((?:save|persist|upsert|merge|patch|write|create_relationship|merge_relationship))\s*\(",
    ):
        match = re.search(pattern, descriptor)
        if match is None:
            continue
        return match.group(1)
    return None


def _map_schema_target_to_sink(*, schema_target: str) -> tuple[str, str] | None:
    normalized = str(schema_target or "").strip()
    if not normalized or ":" not in normalized:
        return None
    kind, _, target = normalized.partition(":")
    target = target.strip()
    if not target:
        return None
    normalized_kind = kind.strip().casefold()
    if normalized_kind == "node":
        return f"CustomerGraph.Node.{target}", "node_sink"
    if normalized_kind == "relationship":
        return f"CustomerGraph.Relationship.{target}", "relationship_sink"
    if normalized_kind == "history":
        return f"CustomerGraph.History.{target}", "history_sink"
    return None


def _build_truth_propagation_frames(
    *,
    graph_nodes: list[CausalGraphNode],
    graph_edges: list[CausalGraphEdge],
    truth_bindings: list[CausalGraphTruthBinding],
) -> list[CausalPropagationFrame]:
    nodes_by_id = {node.node_id: node for node in graph_nodes}
    outgoing = _outgoing_edges(edges=graph_edges)
    frames: list[CausalPropagationFrame] = []
    for binding in truth_bindings:
        queue: deque[tuple[str, int]] = deque([(binding.bound_node_id, 0)])
        visited: set[str] = {binding.bound_node_id}
        affected_node_ids: list[str] = []
        affected_edge_ids: list[str] = []
        while queue:
            node_id, depth = queue.popleft()
            if depth >= 5:
                continue
            for edge in outgoing.get(node_id, []):
                if edge.propagation_mode == "none":
                    continue
                if not (edge.truth_relevant or edge.write_relevant or edge.propagation_mode in {"truth_only", "truth_and_delta", "delta_only"}):
                    continue
                affected_edge_ids.append(edge.edge_id)
                if edge.target_node_id not in visited:
                    visited.add(edge.target_node_id)
                    affected_node_ids.append(edge.target_node_id)
                    queue.append((edge.target_node_id, depth + 1))
        frames.append(
            CausalPropagationFrame(
                origin_node_id=binding.bound_node_id,
                truth_id=binding.truth_id,
                affected_node_ids=_dedupe_preserve_order(affected_node_ids),
                affected_edge_ids=_dedupe_preserve_order(affected_edge_ids),
                metadata={
                    "origin_scope_key": (
                        nodes_by_id[binding.bound_node_id].scope_key
                        if binding.bound_node_id in nodes_by_id
                        else ""
                    ),
                    "predicate": binding.predicate,
                },
            )
        )
    return frames


def _merge_edge(
    *,
    edge_map: dict[tuple[str, str, str], CausalGraphEdge],
    edge: CausalGraphEdge,
) -> None:
    key = (edge.source_node_id, edge.target_node_id, edge.edge_type)
    existing = edge_map.get(key)
    if existing is None:
        edge_map[key] = edge
        return
    merged_metadata = {**existing.metadata, **edge.metadata}
    edge_map[key] = existing.model_copy(
        update={
            "strength": max(existing.strength, edge.strength),
            "blocking": existing.blocking or edge.blocking,
            "write_relevant": existing.write_relevant or edge.write_relevant,
            "truth_relevant": existing.truth_relevant or edge.truth_relevant,
            "propagation_mode": _merge_propagation_mode(existing.propagation_mode, edge.propagation_mode),
            "metadata": merged_metadata,
        }
    )


def _merge_propagation_mode(left: str, right: str) -> str:
    modes = {left, right}
    if "truth_and_delta" in modes:
        return "truth_and_delta"
    if {"truth_only", "delta_only"} <= modes:
        return "truth_and_delta"
    if "truth_only" in modes:
        return "truth_only"
    if "delta_only" in modes:
        return "delta_only"
    return "none"


def _causal_node_type_for_semantic_entity(*, entity: SemanticEntity) -> CausalNodeType:
    if entity.entity_type == "truth":
        return "truth"
    if entity.entity_type == "process":
        return "scope"
    if entity.entity_type == "phase":
        return "phase_scope"
    if entity.entity_type == "question":
        return "artifact"
    if entity.entity_type == "policy":
        return "policy"
    if entity.entity_type == "lifecycle":
        return "lifecycle"
    if entity.entity_type in {"write_contract", "read_contract"}:
        return "write_contract" if entity.entity_type == "write_contract" else "read_contract"
    if entity.entity_type == "documentation_section":
        return "document_anchor"
    if entity.entity_type == "code_component":
        return _classify_code_node_type(entity=entity)
    return "artifact"


def _classify_code_node_type(*, entity: SemanticEntity) -> CausalNodeType:
    descriptor = " ".join(
        [
            str(entity.label or ""),
            str(entity.canonical_key or ""),
            str(entity.metadata.get("path_hint") or ""),
        ]
    ).casefold()
    if any(token in descriptor for token in ("a3b", "a4", "agent", "/agents/")):
        return "agent"
    if "worker" in descriptor or "/workers/" in descriptor:
        return "worker"
    if "router" in descriptor or "/routes/" in descriptor or "/api/" in descriptor:
        return "api_route"
    if "service" in descriptor or "/services/" in descriptor:
        return "service"
    return "code_anchor"


def _fallback_subject_node_type(*, subject_key: str) -> tuple[CausalNodeType, CausalNodeLayer]:
    if subject_key == "BSM.process":
        return "scope", "process"
    if subject_key.startswith("BSM.phase."):
        return "phase_scope", "process"
    if subject_key.endswith((".policy", ".approval_policy", ".scope_policy")):
        return "policy", "governance"
    if subject_key.endswith((".lifecycle", ".review_status")):
        return "lifecycle", "governance"
    if subject_key.endswith(".write_path"):
        return "write_contract", "governance"
    if subject_key.endswith(".read_path"):
        return "read_contract", "governance"
    return "artifact", "process"


def _is_write_relevant(*, entity_type: str, canonical_key: str) -> bool:
    normalized_type = str(entity_type or "")
    normalized_key = str(canonical_key or "").casefold()
    return (
        normalized_type in {
            "write_contract",
            "read_contract",
            "agent",
            "worker",
            "service",
            "api_route",
            "write_decider",
            "repository_adapter",
            "driver_adapter",
            "transaction_boundary",
            "retry_boundary",
            "batch_boundary",
            "persistence_target",
        }
        or normalized_key.endswith((".write_path", ".read_path"))
        or any(token in normalized_key for token in ("persist", "write", "save", "upsert", "merge", "patch"))
    )


def _is_decision_relevant(*, entity_type: str, canonical_key: str) -> bool:
    normalized_type = str(entity_type or "")
    normalized_key = str(canonical_key or "")
    return (
        normalized_type in {
            "truth",
            "policy",
            "lifecycle",
            "write_contract",
            "read_contract",
            "scope",
            "phase_scope",
            "write_decider",
            "repository_adapter",
            "driver_adapter",
            "transaction_boundary",
        }
        or normalized_key == "BSM.process"
        or normalized_key.startswith("BSM.phase.")
    )


def _outgoing_edges(*, edges: list[CausalGraphEdge]) -> dict[str, list[CausalGraphEdge]]:
    outgoing: dict[str, list[CausalGraphEdge]] = defaultdict(list)
    for edge in edges:
        outgoing[edge.source_node_id].append(edge)
    return outgoing


def _truth_requires_delta_recalculation(*, truth: TruthLedgerEntry) -> bool:
    metadata = truth.metadata or {}
    return bool(metadata.get("truth_delta_retrigger") or metadata.get("pending_delta_recalculation"))


def _scope_keys_overlap(*, left: str, right: str) -> bool:
    normalized_left = str(left or "").strip()
    normalized_right = str(right or "").strip()
    if not normalized_left or not normalized_right:
        return False
    return (
        normalized_left == normalized_right
        or normalized_left.startswith(f"{normalized_right}.")
        or normalized_right.startswith(f"{normalized_left}.")
    )


def _is_explicit_truth(*, truth: TruthLedgerEntry) -> bool:
    return truth.source_kind in {"user_specification", "user_acceptance"}


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]
