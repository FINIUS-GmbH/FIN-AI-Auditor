from __future__ import annotations

import argparse
import json

from fin_ai_auditor.api.dependencies import get_audit_service
from fin_ai_auditor.config import get_settings
from fin_ai_auditor.domain.models import AuditSourceSnapshot, AuditTarget, CreateAuditRunRequest


DEFAULT_CONFLUENCE_PAGE_ID = "2654426"
DEFAULT_CONFLUENCE_PAGE_TITLE = "FIN-AI Testing"
DEFAULT_CONFLUENCE_PAGE_URL = "https://fin-ai.atlassian.net/spaces/FP/pages/2654426/FIN-AI+Testing"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fuehrt einen kontrollierten Live-Smoke-Test fuer Jira- und Confluence-Writeback "
            "ueber den Auditor-Approval-Pfad aus."
        )
    )
    parser.add_argument("--reset-runs", action="store_true", help="Loescht vorhandene Audit-Runs vor dem Smoke-Test.")
    parser.add_argument("--skip-jira", action="store_true", help="Ueberspringt den Jira-Writeback.")
    parser.add_argument("--skip-confluence", action="store_true", help="Ueberspringt den Confluence-Writeback.")
    parser.add_argument("--confluence-page-id", default=DEFAULT_CONFLUENCE_PAGE_ID)
    parser.add_argument("--confluence-page-title", default=DEFAULT_CONFLUENCE_PAGE_TITLE)
    parser.add_argument("--confluence-page-url", default=DEFAULT_CONFLUENCE_PAGE_URL)
    return parser


def _prepare_run(*, page_id: str, page_title: str, page_url: str):
    settings = get_settings()
    service = get_audit_service()
    repository = service.repository

    run = service.create_run(
        payload=CreateAuditRunRequest(
            target=AuditTarget(
                local_repo_path=str(settings.default_finai_local_repo_path),
                confluence_space_keys=[settings.fixed_confluence_space_key],
                jira_project_keys=[settings.fixed_jira_project_key],
                include_metamodel=True,
                include_local_docs=True,
            )
        )
    )
    run = service.complete_run_with_demo_findings(run_id=run.run_id)
    base_finding = next(finding for finding in run.findings if any(loc.source_type == "confluence_page" for loc in finding.locations))
    patched_finding = base_finding.model_copy(
        update={
            "locations": [
                loc.model_copy(
                    update={
                        "source_id": page_id,
                        "title": page_title,
                        "url": page_url,
                        "path_hint": f"Space: {settings.fixed_confluence_space_key}",
                    }
                )
                if loc.source_type == "confluence_page"
                else loc
                for loc in base_finding.locations
            ]
        }
    )
    findings = [patched_finding if finding.finding_id == base_finding.finding_id else finding for finding in run.findings]
    snapshots: list[AuditSourceSnapshot] = []
    replaced_snapshot = False
    for snapshot in run.source_snapshots:
        if snapshot.source_type == "confluence_page":
            snapshots.append(
                snapshot.model_copy(
                    update={
                        "source_id": page_id,
                        "metadata": {
                            **snapshot.metadata,
                            "space_key": settings.fixed_confluence_space_key,
                            "title": page_title,
                            "url": page_url,
                        },
                    }
                )
            )
            replaced_snapshot = True
        else:
            snapshots.append(snapshot)
    if not replaced_snapshot:
        snapshots.append(
            AuditSourceSnapshot(
                source_type="confluence_page",
                source_id=page_id,
                metadata={
                    "space_key": settings.fixed_confluence_space_key,
                    "title": page_title,
                    "url": page_url,
                },
            )
        )
    run = repository.upsert_run(run=run.model_copy(update={"findings": findings, "source_snapshots": snapshots}))
    return service, run, patched_finding


def main() -> int:
    args = _build_parser().parse_args()
    settings = get_settings()
    service = get_audit_service()
    if args.reset_runs:
        service.reset_all_runs()

    service, run, finding = _prepare_run(
        page_id=str(args.confluence_page_id).strip(),
        page_title=str(args.confluence_page_title).strip(),
        page_url=str(args.confluence_page_url).strip(),
    )

    result: dict[str, object] = {
        "run_id": run.run_id,
        "finding_id": finding.finding_id,
        "jira": None,
        "confluence": None,
    }

    if not args.skip_jira:
        run = service.create_writeback_approval_request(
            run_id=run.run_id,
            target_type="jira_ticket_create",
            title="Jira-Writeback fuer FIN-AI Auditor E2E Smoke",
            summary="Kontrollierter E2E-Smoke-Test fuer Jira-Writeback ueber den Auditor.",
            target_url=settings.jira_board_url,
            related_package_ids=[],
            related_finding_ids=[finding.finding_id],
            payload_preview=["[AUDITOR E2E TEST] Jira-Smoke ueber Approval und externen Writeback verifizieren."],
        )
        jira_approval = next(item for item in run.approval_requests if item.target_type == "jira_ticket_create")
        run = service.resolve_writeback_approval_request(
            run_id=run.run_id,
            approval_request_id=jira_approval.approval_request_id,
            decision="approve",
            comment_text="Automatischer E2E-Smoke-Test",
        )
        run = service.execute_jira_ticket_writeback(
            run_id=run.run_id,
            approval_request_id=jira_approval.approval_request_id,
        )
        jira_change = next(change for change in run.implemented_changes if change.change_type == "jira_ticket_created")
        result["jira"] = {
            "issue_key": jira_change.target_label,
            "issue_url": jira_change.target_url,
            "issue_type": (jira_change.metadata.get("writeback_verification") or {}).get("resolved_issue_type"),
        }

    if not args.skip_confluence:
        run = service.create_writeback_approval_request(
            run_id=run.run_id,
            target_type="confluence_page_update",
            title="Confluence-Writeback fuer FIN-AI Auditor E2E Smoke",
            summary="Kontrollierter E2E-Smoke-Test fuer Confluence-Writeback ueber den Auditor.",
            target_url=str(args.confluence_page_url).strip(),
            related_package_ids=[],
            related_finding_ids=[finding.finding_id],
            payload_preview=["[AUDITOR E2E TEST] Confluence-Smoke ueber Approval und externen Patch verifizieren."],
        )
        confluence_approval = next(item for item in run.approval_requests if item.target_type == "confluence_page_update")
        run = service.resolve_writeback_approval_request(
            run_id=run.run_id,
            approval_request_id=confluence_approval.approval_request_id,
            decision="approve",
            comment_text="Automatischer E2E-Smoke-Test",
        )
        run = service.execute_confluence_page_writeback(
            run_id=run.run_id,
            approval_request_id=confluence_approval.approval_request_id,
        )
        confluence_change = next(change for change in run.implemented_changes if change.change_type == "confluence_page_updated")
        result["confluence"] = {
            "page_title": confluence_change.target_label,
            "page_url": confluence_change.target_url,
            "applied_revision_id": (
                confluence_change.confluence_update.applied_revision_id
                if confluence_change.confluence_update is not None
                else None
            ),
        }

    print(json.dumps(result, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
