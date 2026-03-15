from fin_ai_auditor.domain.models import AuditFinding
from fin_ai_auditor.services.recommendation_engine import _build_chunk_cache_key


def _finding(*, summary: str, recommendation: str = "Pruefen") -> AuditFinding:
    return AuditFinding(
        severity="high",
        category="contradiction",
        title="Widerspruch",
        summary=summary,
        recommendation=recommendation,
        canonical_key="BSM.process.statement",
    )


def test_chunk_cache_key_changes_when_finding_content_changes() -> None:
    base = _build_chunk_cache_key(
        slot=1,
        findings=[_finding(summary="Version A")],
        truths=[],
        retrieved_contexts={},
        repo_context="Repo",
        metamodel_context="Metamodell",
        confluence_context="Confluence",
    )
    changed = _build_chunk_cache_key(
        slot=1,
        findings=[_finding(summary="Version B")],
        truths=[],
        retrieved_contexts={},
        repo_context="Repo",
        metamodel_context="Metamodell",
        confluence_context="Confluence",
    )

    assert base != changed


def test_chunk_cache_key_changes_when_retrieval_context_changes() -> None:
    finding = _finding(summary="Unveraenderte Evidenz")
    key = str(finding.canonical_key)

    base = _build_chunk_cache_key(
        slot=1,
        findings=[finding],
        truths=[],
        retrieved_contexts={key: ["Kontext A"]},
        repo_context="Repo",
        metamodel_context="Metamodell",
        confluence_context="Confluence",
    )
    changed = _build_chunk_cache_key(
        slot=1,
        findings=[finding],
        truths=[],
        retrieved_contexts={key: ["Kontext B"]},
        repo_context="Repo",
        metamodel_context="Metamodell",
        confluence_context="Confluence",
    )

    assert base != changed
