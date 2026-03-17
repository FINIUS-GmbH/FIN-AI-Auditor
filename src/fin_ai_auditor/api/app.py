from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from fin_ai_auditor.api.routes.atlassian import router as atlassian_router
from fin_ai_auditor.api.routes.audits import router as audits_router
from fin_ai_auditor.api.routes.bootstrap import router as bootstrap_router
from fin_ai_auditor.api.routes.health import router as health_router
from fin_ai_auditor.config import get_settings
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository
from fin_ai_auditor.services.operational_readiness import ensure_runtime_ready
from fin_ai_auditor.services.secret_store import build_secret_store


@asynccontextmanager
async def _lifespan(app: FastAPI):
    settings = get_settings()
    repository = SQLiteAuditRepository(
        db_path=settings.database_path,
        secret_store=build_secret_store(
            mode=settings.secret_storage_mode,
            service_name=settings.secret_storage_service_name,
        ),
    )
    app.state.runtime_guard = ensure_runtime_ready(
        settings=settings,
        repository=repository,
        context="api",
    )
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, lifespan=_lifespan)
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
