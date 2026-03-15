from fin_ai_auditor.domain.models import AuditLocation
from fin_ai_auditor.services.causal_graph_models import (
    CausalGraph,
    CausalGraphEdge,
    CausalGraphNode,
    CausalGraphTruthBinding,
    CausalPropagationFrame,
)


def test_causal_graph_models_capture_write_and_truth_propagation() -> None:
    a3b_node = CausalGraphNode(
        run_id="run_1",
        node_type="agent",
        layer="runtime",
        canonical_key="A3B",
        label="A3B Consolidation Agent",
        scope_key="BSM.phase.scoping",
        decision_relevant=True,
    )
    statement_policy_node = CausalGraphNode(
        run_id="run_1",
        node_type="policy",
        layer="governance",
        canonical_key="Statement.policy",
        label="Statement Policy",
        scope_key="Statement",
        write_relevant=True,
    )
    statement_node = CausalGraphNode(
        run_id="run_1",
        node_type="artifact",
        layer="process",
        canonical_key="Statement",
        label="Statement",
        scope_key="Statement",
        write_relevant=True,
    )
    graph_target_node = CausalGraphNode(
        run_id="run_1",
        node_type="persistence_target",
        layer="storage",
        canonical_key="CustomerGraph.Statement",
        label="Customer Graph Statement Target",
        scope_key="Statement",
        write_relevant=True,
        evidence_refs=[
            {
                "role": "primary",
                "location": AuditLocation(
                    source_type="github_file",
                    source_id="src/finai/workers/job_worker.py",
                    title="A3B",
                    path_hint="src/finai/workers/job_worker.py",
                ),
                "confidence": 0.92,
            }
        ],
    )

    edges = [
        CausalGraphEdge(
            run_id="run_1",
            source_node_id=a3b_node.node_id,
            target_node_id=statement_node.node_id,
            edge_type="decides_write",
            propagation_mode="truth_and_delta",
            strength=0.95,
            write_relevant=True,
            truth_relevant=True,
        ),
        CausalGraphEdge(
            run_id="run_1",
            source_node_id=statement_policy_node.node_id,
            target_node_id=statement_node.node_id,
            edge_type="gated_by",
            propagation_mode="truth_and_delta",
            strength=1.0,
            blocking=True,
            write_relevant=True,
            truth_relevant=True,
        ),
        CausalGraphEdge(
            run_id="run_1",
            source_node_id=statement_node.node_id,
            target_node_id=graph_target_node.node_id,
            edge_type="writes_to",
            propagation_mode="delta_only",
            strength=0.98,
            write_relevant=True,
        ),
    ]
    truth_binding = CausalGraphTruthBinding(
        truth_id="truth_1",
        truth_canonical_key="Statement.policy|user_specification",
        bound_node_id=statement_policy_node.node_id,
        predicate="user_specification",
        propagation_mode="truth_only",
    )
    propagation_frame = CausalPropagationFrame(
        origin_node_id=statement_policy_node.node_id,
        truth_id="truth_1",
        affected_node_ids=[statement_node.node_id, graph_target_node.node_id],
        affected_edge_ids=[edge.edge_id for edge in edges],
        metadata={"reason": "Explizite Wahrheit muss bis zum Persistenzziel gespiegelt werden."},
    )

    graph = CausalGraph(
        run_id="run_1",
        nodes=[a3b_node, statement_policy_node, statement_node, graph_target_node],
        edges=edges,
        truth_bindings=[truth_binding],
        propagation_frames=[propagation_frame],
        metadata={"focus": "write_paths_only"},
    )

    assert len(graph.nodes) == 4
    assert len(graph.edges) == 3
    assert graph.truth_bindings[0].bound_node_id == statement_policy_node.node_id
    assert graph.propagation_frames[0].affected_node_ids[-1] == graph_target_node.node_id
    assert graph.edges[1].blocking is True
