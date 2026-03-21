from __future__ import annotations

from typing import cast

from fin_ai_auditor.config import Settings
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository


def build_persistence_profile(*, settings: Settings) -> dict[str, object]:
    backend = "sqlite"
    mode_ready = settings.operational_mode != "prod_like" or settings.allow_sqlite_in_prod_like
    notes: list[str]
    if settings.operational_mode == "prod_like" and not settings.allow_sqlite_in_prod_like:
        notes = [
            "SQLite ist fuer local_dev und Pilot tragbar, aber fuer prod_like ohne explizite Ausnahme blockiert.",
            "Fuer belastbaren Dauerbetrieb ist eine produktionsnahe Mehrprozess-Persistenz wie Postgres erforderlich.",
        ]
    elif settings.operational_mode == "prod_like":
        notes = [
            "SQLite ist per Override auch in prod_like zugelassen. Das ist nur als bewusstes Risiko fuer begrenzte Uebergangsphasen gedacht.",
        ]
    else:
        notes = [
            "SQLite ist fuer lokalen Betrieb und Pilotierung aktiv.",
            "Fuer prod_like bleibt eine produktionsnahe Persistenz weiterhin empfohlen.",
        ]
    return {
        "backend": backend,
        "database_path": str(settings.database_path),
        "mode_ready": mode_ready,
        "production_ready": False,
        "notes": notes,
    }


def build_runtime_guard(
    *,
    settings: Settings,
    repository: SQLiteAuditRepository,
    context: str,
) -> dict[str, object]:
    persistence_profile = build_persistence_profile(settings=settings)
    secret_storage = repository.get_secret_storage_summary()
    blockers: list[str] = []
    warnings: list[str] = []

    if not bool(persistence_profile.get("mode_ready")):
        blockers.append(
            "prod_like ist mit SQLite ohne explizite Ausnahme nicht zulaessig."
        )

    if settings.operational_mode == "prod_like":
        if not bool(secret_storage.get("available")):
            blockers.append(
                "Der konfigurierte Secret-Store ist in prod_like nicht verfuegbar."
            )
        if not bool(secret_storage.get("secure")) and not settings.allow_insecure_secret_store_in_prod_like:
            blockers.append(
                "prod_like verlangt einen sicheren Secret-Store oder eine explizite Ausnahme."
            )
    elif settings.operational_mode == "pilot":
        if not bool(secret_storage.get("available")):
            warnings.append(
                "Der konfigurierte Secret-Store ist im Pilot nicht verfuegbar."
            )
        if not bool(secret_storage.get("secure")):
            warnings.append(
                "Der Pilot laeuft ohne sicheren Secret-Store. Das ist nur fuer eng kontrollierte Nutzung tragbar."
            )

    return {
        "context": context,
        "ready": not bool(blockers),
        "enforcement_active": settings.startup_enforce_runtime_guard,
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "persistence_profile": persistence_profile,
        "secret_storage": secret_storage,
    }


def ensure_runtime_ready(
    *,
    settings: Settings,
    repository: SQLiteAuditRepository,
    context: str,
) -> dict[str, object]:
    guard = build_runtime_guard(settings=settings, repository=repository, context=context)
    blockers = _string_list(guard.get("blockers"))
    if settings.startup_enforce_runtime_guard and blockers:
        raise RuntimeError(
            "Operative Startblockade: " + "; ".join(blockers)
        )
    return guard


def build_operational_alert_summary(
    *,
    observability: dict[str, object],
    worker_recovery: dict[str, object],
) -> dict[str, object]:
    blockers: list[str] = []
    warnings: list[str] = []
    notes: list[str] = []

    trace_count = _coerce_int(observability.get("trace_count"))
    metric_sample_count = _coerce_int(observability.get("metric_sample_count"))
    recent_error_span_count = _coerce_int(observability.get("recent_error_span_count"))
    reclaimable_run_count = _coerce_int(worker_recovery.get("reclaimable_run_count"))

    if trace_count <= 0:
        warnings.append(
            "Es liegen noch keine Runtime-Trace-Spans vor. Operative Fehlerpfade sind damit noch nicht belastbar beobachtbar."
        )
    if metric_sample_count <= 0:
        warnings.append(
            "Es liegen noch keine Runtime-Metriken vor. Last- und Stabilitaetsbeobachtung ist damit noch nicht aussagekraeftig."
        )
    if recent_error_span_count > 0:
        warnings.append(
            f"In den letzten 24h wurden {recent_error_span_count} Runtime-Fehlerspans erfasst."
        )
    if reclaimable_run_count > 0:
        warnings.append(
            f"Es existieren {reclaimable_run_count} reclaimbare oder stale Runs. Recovery ist damit aktuell nicht sauber abgeschlossen."
        )
    if trace_count > 0 and metric_sample_count > 0 and recent_error_span_count <= 0 and reclaimable_run_count <= 0:
        notes.append(
            "Tracing, Metriken und Lease-Recovery sind aktuell ohne akute Auffaelligkeiten."
        )

    severity = "ok"
    if blockers:
        severity = "critical"
    elif warnings:
        severity = "warning"
    return {
        "status": severity,
        "ready": not bool(blockers),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "notes": notes,
        "observability_signals": {
            "trace_count": trace_count,
            "metric_sample_count": metric_sample_count,
            "recent_error_span_count": recent_error_span_count,
        },
        "recovery_signals": {
            "reclaimable_run_count": reclaimable_run_count,
        },
    }


def build_go_live_gate_summary(
    *,
    runtime_guard: dict[str, object],
    gold_set_gate: dict[str, object],
    delta_gate: dict[str, object],
    alert_summary: dict[str, object],
    confluence_live_read_ready: bool | None = None,
    jira_writeback_ready: bool | None = None,
) -> dict[str, object]:
    checks: list[dict[str, object]] = [
        {
            "gate": "runtime_guard",
            "label": "Runtime Guard",
            "passed": bool(runtime_guard.get("ready")),
            "notes": _string_list(runtime_guard.get("blockers")) or _string_list(runtime_guard.get("warnings")),
        },
        {
            "gate": "gold_set",
            "label": "Referenz-Gold-Set",
            "passed": bool(gold_set_gate.get("passed")),
            "notes": _string_list(gold_set_gate.get("failure_reasons")),
        },
        {
            "gate": "delta_recompute",
            "label": "Delta-Neuberechnung",
            "passed": bool(delta_gate.get("passed")),
            "notes": _string_list(delta_gate.get("failure_reasons")),
        },
        {
            "gate": "operational_alerts",
            "label": "Operative Signale",
            "passed": not bool(alert_summary.get("blockers")) and not bool(alert_summary.get("warnings")),
            "notes": _string_list(alert_summary.get("blockers")) + _string_list(alert_summary.get("warnings")),
        },
    ]
    if confluence_live_read_ready is not None:
        checks.append(
            {
                "gate": "confluence_live_read",
                "label": "Confluence Live-Read",
                "passed": bool(confluence_live_read_ready),
                "notes": [] if confluence_live_read_ready else ["Confluence Live-Read ist aktuell nicht verifiziert oder nicht scope-bereit."],
            }
        )
    if jira_writeback_ready is not None:
        checks.append(
            {
                "gate": "jira_writeback",
                "label": "Jira-Writeback",
                "passed": bool(jira_writeback_ready),
                "notes": [] if jira_writeback_ready else ["Jira-Writeback ist aktuell nicht verifiziert oder nicht scope-bereit."],
            }
        )

    blockers = [
        f"{str(check.get('label') or '')}: {notes[0]}"
        for check in checks
        if not bool(check.get("passed")) and (notes := _string_list(check.get("notes")))
    ]
    if not blockers:
        blockers = [
            str(check.get("label") or "")
            for check in checks
            if not bool(check.get("passed"))
        ]
    return {
        "ready": all(bool(check["passed"]) for check in checks),
        "checks": checks,
        "blocking_gates": blockers,
    }


def _coerce_int(value: object) -> int:
    try:
        return int(cast(int | float | str, value))
    except (TypeError, ValueError):
        return 0


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in cast(list[object], value) if str(item).strip()]
