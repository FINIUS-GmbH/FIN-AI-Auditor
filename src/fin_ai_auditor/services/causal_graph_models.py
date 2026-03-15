from __future__ import annotations

from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field

from fin_ai_auditor.domain.models import AuditLocation


CausalNodeLayer = Literal["truth", "governance", "process", "runtime", "storage", "evidence"]
CausalNodeType = Literal[
    "truth",
    "scope",
    "phase_scope",
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
    "write_contract",
    "read_contract",
    "policy",
    "lifecycle",
    "artifact",
    "relationship",
    "persistence_target",
    "document_anchor",
    "code_anchor",
]
CausalEdgeType = Literal[
    "scopes_to",
    "feeds",
    "materializes",
    "reads_from",
    "writes_to",
    "decides_write",
    "gated_by",
    "derived_from",
    "documents",
    "implemented_by",
    "triggered_by",
    "depends_on",
    "invalidates",
    "propagates_truth_to",
    "evidenced_by",
]
CausalPropagationMode = Literal["none", "truth_only", "delta_only", "truth_and_delta"]
CausalEvidenceRole = Literal["primary", "supporting", "derived"]


def new_causal_node_id() -> str:
    return f"causal_node_{uuid4().hex}"


def new_causal_edge_id() -> str:
    return f"causal_edge_{uuid4().hex}"


def new_causal_evidence_id() -> str:
    return f"causal_evidence_{uuid4().hex}"


def new_causal_binding_id() -> str:
    return f"causal_binding_{uuid4().hex}"


def new_causal_frame_id() -> str:
    return f"causal_frame_{uuid4().hex}"


def new_causal_graph_id() -> str:
    return f"causal_graph_{uuid4().hex}"


class CausalGraphEvidenceRef(BaseModel):
    evidence_id: str = Field(default_factory=new_causal_evidence_id)
    role: CausalEvidenceRole = "supporting"
    location: AuditLocation
    confidence: float = Field(default=0.7, ge=0.0, le=1.0)
    metadata: dict[str, object] = Field(default_factory=dict)


class CausalGraphNode(BaseModel):
    node_id: str = Field(default_factory=new_causal_node_id)
    run_id: str | None = None
    node_type: CausalNodeType
    layer: CausalNodeLayer
    canonical_key: str = Field(min_length=1)
    label: str = Field(min_length=1)
    scope_key: str = Field(min_length=1)
    write_relevant: bool = False
    decision_relevant: bool = False
    evidence_refs: list[CausalGraphEvidenceRef] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


class CausalGraphEdge(BaseModel):
    edge_id: str = Field(default_factory=new_causal_edge_id)
    run_id: str | None = None
    source_node_id: str = Field(min_length=1)
    target_node_id: str = Field(min_length=1)
    edge_type: CausalEdgeType
    propagation_mode: CausalPropagationMode = "none"
    strength: float = Field(default=0.7, ge=0.0, le=1.0)
    blocking: bool = False
    write_relevant: bool = False
    truth_relevant: bool = False
    evidence_refs: list[CausalGraphEvidenceRef] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


class CausalGraphTruthBinding(BaseModel):
    binding_id: str = Field(default_factory=new_causal_binding_id)
    truth_id: str = Field(min_length=1)
    truth_canonical_key: str = Field(min_length=1)
    bound_node_id: str = Field(min_length=1)
    predicate: str = Field(min_length=1)
    propagation_mode: CausalPropagationMode = "truth_only"
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    metadata: dict[str, object] = Field(default_factory=dict)


class CausalPropagationFrame(BaseModel):
    frame_id: str = Field(default_factory=new_causal_frame_id)
    origin_node_id: str = Field(min_length=1)
    truth_id: str | None = None
    affected_node_ids: list[str] = Field(default_factory=list)
    affected_edge_ids: list[str] = Field(default_factory=list)
    max_depth: int = Field(default=4, ge=1, le=12)
    stop_node_types: list[CausalNodeType] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


class CausalGraph(BaseModel):
    graph_id: str = Field(default_factory=new_causal_graph_id)
    run_id: str | None = None
    nodes: list[CausalGraphNode] = Field(default_factory=list)
    edges: list[CausalGraphEdge] = Field(default_factory=list)
    truth_bindings: list[CausalGraphTruthBinding] = Field(default_factory=list)
    propagation_frames: list[CausalPropagationFrame] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)
