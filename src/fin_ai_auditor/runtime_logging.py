from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any


class _JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        event_name = getattr(record, "event_name", None)
        if isinstance(event_name, str) and event_name.strip():
            payload["event"] = event_name.strip()
        event_payload = getattr(record, "event_payload", None)
        if isinstance(event_payload, dict) and event_payload:
            payload["context"] = event_payload
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def setup_logging(*, level: int = logging.INFO) -> None:
    root_logger = logging.getLogger()
    if any(getattr(handler, "_fin_ai_auditor_logging", False) for handler in root_logger.handlers):
        root_logger.setLevel(level)
        return
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonLogFormatter())
    handler._fin_ai_auditor_logging = True  # type: ignore[attr-defined]
    root_logger.handlers = [handler]
    root_logger.setLevel(level)
