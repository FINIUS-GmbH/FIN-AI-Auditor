from fin_ai_auditor.domain.models import AuditSourceSnapshot
from fin_ai_auditor.services.context_builder import AuditContextBuilder
from fin_ai_auditor.services.pipeline_models import CollectedDocument


def test_build_repo_summary_keeps_methods_with_their_own_class() -> None:
    document = CollectedDocument(
        snapshot=AuditSourceSnapshot(
            source_type="github_file",
            source_id="src/services/demo.py",
            content_hash="sha256:demo",
        ),
        source_type="github_file",
        source_id="src/services/demo.py",
        title="demo.py",
        path_hint="src/services/demo.py",
        body=(
            "class FirstService:\n"
            "    def load(self):\n"
            "        return True\n"
            "\n"
            "class SecondService:\n"
            "    def persist(self):\n"
            "        return True\n"
        ),
    )

    summary = AuditContextBuilder().build_repo_summary([document])

    assert "FirstService (src/services/demo.py): load" in summary
    assert "SecondService (src/services/demo.py): persist" in summary
    assert "FirstService (src/services/demo.py): load, persist" not in summary
