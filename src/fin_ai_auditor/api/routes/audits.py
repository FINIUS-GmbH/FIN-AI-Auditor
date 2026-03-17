from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from fin_ai_auditor.api.dependencies import get_audit_service
from fin_ai_auditor.domain.models import (
    AuditRun,
    AuditRunListResponse,
    ConfirmTruthFromClarificationRequest,
    CreateClarificationThreadRequest,
    CreateWritebackApprovalRequest,
    CreateAuditRunRequest,
    CreateDecisionCommentRequest,
    DecisionPackageActionRequest,
    RecordConfluencePageUpdateRequest,
    RecordJiraTicketCreatedRequest,
    ReviewCardActionRequest,
    ResolveWritebackApprovalRequest,
    SendClarificationMessageRequest,
    SupersedeTruthFromClarificationRequest,
    UpdateAtomicFactStatusRequest,
)
from fin_ai_auditor.services.audit_service import AuditService
from fin_ai_auditor.services.clarification_service import ClarificationService

router = APIRouter(prefix="/api/audits", tags=["audits"])


@router.post("/runs", response_model=AuditRun)
def create_audit_run(
    payload: CreateAuditRunRequest,
    service: AuditService = Depends(get_audit_service),
) -> AuditRun:
    try:
        return service.create_user_run(payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs", response_model=AuditRunListResponse)
def list_audit_runs(service: AuditService = Depends(get_audit_service)) -> AuditRunListResponse:
    return AuditRunListResponse(items=service.list_runs())


@router.get("/runs/{run_id}", response_model=AuditRun)
def get_audit_run(run_id: str, service: AuditService = Depends(get_audit_service)) -> AuditRun:
    run = service.get_run(run_id=run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Audit-Run nicht gefunden.")
    return run


@router.post("/runs/{run_id}/decision-comments", response_model=AuditRun)
def process_decision_comment(
    run_id: str,
    payload: CreateDecisionCommentRequest,
    service: AuditService = Depends(get_audit_service),
) -> AuditRun:
    try:
        return service.process_decision_comment(
            run_id=run_id,
            comment_text=payload.comment_text,
            related_finding_ids=payload.related_finding_ids,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/runs/{run_id}/review-cards/{card_id}/decision", response_model=AuditRun)
def apply_review_card_decision(
    run_id: str,
    card_id: str,
    payload: ReviewCardActionRequest,
    service: AuditService = Depends(get_audit_service),
) -> AuditRun:
    try:
        return service.apply_review_card_decision(
            run_id=run_id,
            card_id=card_id,
            action=payload.action,
            comment_text=payload.comment_text,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{run_id}/packages/{package_id}/regenerate", response_model=AuditRun)
def regenerate_package_from_clarification(
    run_id: str,
    package_id: str,
    thread_id: str = Query(...),
    service: AuditService = Depends(get_audit_service),
) -> AuditRun:
    try:
        return service.regenerate_package_from_clarification(
            run_id=run_id,
            package_id=package_id,
            thread_id=thread_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

@router.post("/runs/{run_id}/packages/{package_id}/decisions", response_model=AuditRun)
def apply_package_decision(
    run_id: str,
    package_id: str,
    payload: DecisionPackageActionRequest,
    service: AuditService = Depends(get_audit_service),
) -> AuditRun:
    try:
        return service.apply_package_decision(
            run_id=run_id,
            package_id=package_id,
            action=payload.action,
            comment_text=payload.comment_text,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{run_id}/atomic-facts/{atomic_fact_id}/status", response_model=AuditRun)
def update_atomic_fact_status(
    run_id: str,
    atomic_fact_id: str,
    payload: UpdateAtomicFactStatusRequest,
    service: AuditService = Depends(get_audit_service),
) -> AuditRun:
    try:
        return service.update_atomic_fact_status(
            run_id=run_id,
            atomic_fact_id=atomic_fact_id,
            status=payload.status,
            comment_text=payload.comment_text,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{run_id}/approval-requests", response_model=AuditRun)
def create_writeback_approval_request(
    run_id: str,
    payload: CreateWritebackApprovalRequest,
    service: AuditService = Depends(get_audit_service),
) -> AuditRun:
    try:
        return service.create_writeback_approval_request(
            run_id=run_id,
            target_type=payload.target_type,
            title=payload.title,
            summary=payload.summary,
            target_url=payload.target_url,
            related_review_card_ids=payload.related_review_card_ids,
            related_package_ids=payload.related_package_ids,
            related_finding_ids=payload.related_finding_ids,
            payload_preview=payload.payload_preview,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{run_id}/approval-requests/{approval_request_id}/decision", response_model=AuditRun)
def resolve_writeback_approval_request(
    run_id: str,
    approval_request_id: str,
    payload: ResolveWritebackApprovalRequest,
    service: AuditService = Depends(get_audit_service),
) -> AuditRun:
    try:
        return service.resolve_writeback_approval_request(
            run_id=run_id,
            approval_request_id=approval_request_id,
            decision=payload.decision,
            comment_text=payload.comment_text,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{run_id}/implemented-changes/confluence-page-updated", response_model=AuditRun)
def record_confluence_page_update(
    run_id: str,
    payload: RecordConfluencePageUpdateRequest,
    service: AuditService = Depends(get_audit_service),
) -> AuditRun:
    try:
        return service.record_confluence_page_update(
            run_id=run_id,
            approval_request_id=payload.approval_request_id,
            page_title=payload.page_title,
            page_url=payload.page_url,
            changed_sections=payload.changed_sections,
            change_summary=payload.change_summary,
            related_finding_ids=payload.related_finding_ids,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/runs/{run_id}/implemented-changes/jira-ticket-created", response_model=AuditRun)
def record_jira_ticket_created(
    run_id: str,
    payload: RecordJiraTicketCreatedRequest,
    service: AuditService = Depends(get_audit_service),
) -> AuditRun:
    try:
        return service.record_jira_ticket_created(
            run_id=run_id,
            approval_request_id=payload.approval_request_id,
            ticket_key=payload.ticket_key,
            ticket_url=payload.ticket_url,
            related_finding_ids=payload.related_finding_ids,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/runs/{run_id}/approval-requests/{approval_request_id}/execute/jira-ticket", response_model=AuditRun)
def execute_jira_ticket_writeback(
    run_id: str,
    approval_request_id: str,
    service: AuditService = Depends(get_audit_service),
) -> AuditRun:
    try:
        return service.execute_jira_ticket_writeback(
            run_id=run_id,
            approval_request_id=approval_request_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{run_id}/approval-requests/{approval_request_id}/execute/confluence-page", response_model=AuditRun)
def execute_confluence_page_writeback(
    run_id: str,
    approval_request_id: str,
    service: AuditService = Depends(get_audit_service),
) -> AuditRun:
    try:
        return service.execute_confluence_page_writeback(
            run_id=run_id,
            approval_request_id=approval_request_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/reset")
def reset_audit_database(
    service: AuditService = Depends(get_audit_service),
) -> dict:
    """Drop all audit runs, findings, packages, and truths. Keeps OAuth tokens."""
    service.reset_all_runs()
    return {"ok": True, "message": "Alle Audit-Runs wurden geloescht."}


# ── Clarification Dialog ──

def _get_clarification_service(service: AuditService = Depends(get_audit_service)) -> ClarificationService:
    from fin_ai_auditor.config import get_settings
    return ClarificationService(audit_service=service, settings=get_settings())


@router.post("/runs/{run_id}/clarification-threads", response_model=AuditRun)
def create_clarification_thread(
    run_id: str,
    payload: CreateClarificationThreadRequest,
    cs: ClarificationService = Depends(_get_clarification_service),
) -> AuditRun:
    try:
        return cs.open_thread(
            run_id=run_id,
            package_id=payload.package_id,
            atomic_fact_id=payload.atomic_fact_id,
            review_card_id=payload.review_card_id,
            purpose=payload.purpose,
            initial_content=payload.initial_content,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{run_id}/clarification-threads/{thread_id}/messages", response_model=AuditRun)
def send_clarification_message(
    run_id: str,
    thread_id: str,
    payload: SendClarificationMessageRequest,
    cs: ClarificationService = Depends(_get_clarification_service),
) -> AuditRun:
    try:
        return cs.process_answer(
            run_id=run_id,
            thread_id=thread_id,
            content=payload.content,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{run_id}/clarification-threads/{thread_id}/confirm-truth", response_model=AuditRun)
def confirm_truth_from_clarification(
    run_id: str,
    thread_id: str,
    payload: ConfirmTruthFromClarificationRequest,
    cs: ClarificationService = Depends(_get_clarification_service),
) -> AuditRun:
    try:
        return cs.confirm_truth(
            run_id=run_id,
            thread_id=thread_id,
            truth_canonical_key=payload.truth_canonical_key,
            truth_normalized_value=payload.truth_normalized_value,
            subject_kind=payload.subject_kind,
            subject_key=payload.subject_key,
            predicate=payload.predicate,
            scope_kind=payload.scope_kind,
            scope_key=payload.scope_key,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{run_id}/clarification-threads/{thread_id}/supersede-truth", response_model=AuditRun)
def supersede_truth_from_clarification(
    run_id: str,
    thread_id: str,
    payload: SupersedeTruthFromClarificationRequest,
    cs: ClarificationService = Depends(_get_clarification_service),
) -> AuditRun:
    try:
        return cs.supersede_truth(
            run_id=run_id,
            thread_id=thread_id,
            existing_truth_id=payload.existing_truth_id,
            new_canonical_key=payload.new_canonical_key,
            new_normalized_value=payload.new_normalized_value,
            new_subject_kind=payload.new_subject_kind,
            new_subject_key=payload.new_subject_key,
            new_predicate=payload.new_predicate,
            new_scope_kind=payload.new_scope_kind,
            new_scope_key=payload.new_scope_key,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{run_id}/clarification-threads/{thread_id}/capture-indication", response_model=AuditRun)
def capture_indication_from_clarification(
    run_id: str,
    thread_id: str,
    payload: SendClarificationMessageRequest,
    cs: ClarificationService = Depends(_get_clarification_service),
) -> AuditRun:
    try:
        return cs.capture_indication(
            run_id=run_id,
            thread_id=thread_id,
            content=payload.content,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{run_id}/clarification-threads/{thread_id}/dismiss", response_model=AuditRun)
def dismiss_clarification_thread(
    run_id: str,
    thread_id: str,
    cs: ClarificationService = Depends(_get_clarification_service),
) -> AuditRun:
    try:
        return cs.dismiss_thread(
            run_id=run_id,
            thread_id=thread_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
