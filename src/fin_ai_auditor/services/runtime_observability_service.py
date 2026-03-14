from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from time import perf_counter
from typing import Iterator

from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository


class RuntimeObservabilityService:
    def __init__(self, *, repository: SQLiteAuditRepository) -> None:
        self._repository = repository

    @contextmanager
    def trace_span(
        self,
        *,
        trace_id: str,
        span_name: str,
        run_id: str | None = None,
        worker_id: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> Iterator[None]:
        started_at = datetime.now(UTC).isoformat()
        started_clock = perf_counter()
        status = "ok"
        span_metadata = dict(metadata or {})
        try:
            yield
        except Exception as exc:
            status = "error"
            span_metadata["error_type"] = exc.__class__.__name__
            span_metadata["error_message"] = str(exc)
            raise
        finally:
            finished_at = datetime.now(UTC).isoformat()
            duration_ms = (perf_counter() - started_clock) * 1000.0
            self._repository.record_runtime_trace_span(
                trace_id=trace_id,
                run_id=run_id,
                worker_id=worker_id,
                span_name=span_name,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=duration_ms,
                metadata=span_metadata,
            )
            self._repository.record_runtime_metric_sample(
                metric_key=f"span_duration_ms.{span_name}",
                metric_kind="histogram",
                value=duration_ms,
                run_id=run_id,
                worker_id=worker_id,
                observed_at=finished_at,
                labels={"status": status},
            )

    def increment_counter(
        self,
        *,
        metric_key: str,
        run_id: str | None = None,
        worker_id: str | None = None,
        value: float = 1.0,
        labels: dict[str, object] | None = None,
    ) -> None:
        self._repository.record_runtime_metric_sample(
            metric_key=metric_key,
            metric_kind="counter",
            value=value,
            run_id=run_id,
            worker_id=worker_id,
            observed_at=datetime.now(UTC).isoformat(),
            labels=labels or {},
        )

    def get_summary(self) -> dict[str, object]:
        return self._repository.get_runtime_observability_summary()
