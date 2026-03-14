from __future__ import annotations

import logging

import uvicorn

from fin_ai_auditor.api.app import app
from fin_ai_auditor.config import get_settings
from fin_ai_auditor.runtime_logging import setup_logging


def main() -> None:
    settings = get_settings()
    setup_logging(level=logging.INFO)
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        reload=False,
    )


if __name__ == "__main__":
    main()
