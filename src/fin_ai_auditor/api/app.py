from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from fin_ai_auditor.api.routes.atlassian import router as atlassian_router
from fin_ai_auditor.api.routes.audits import router as audits_router
from fin_ai_auditor.api.routes.bootstrap import router as bootstrap_router
from fin_ai_auditor.api.routes.health import router as health_router
from fin_ai_auditor.config import get_settings


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(health_router)
    app.include_router(bootstrap_router)
    app.include_router(atlassian_router)
    app.include_router(audits_router)
    return app


app = create_app()
