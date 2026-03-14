from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse

from fin_ai_auditor.api.dependencies import get_atlassian_oauth_service
from fin_ai_auditor.domain.models import (
    AtlassianAuthStatus,
    AtlassianAuthorizationStart,
    ConfluenceVerificationResponse,
)
from fin_ai_auditor.services.atlassian_oauth_service import AtlassianOAuthService

router = APIRouter(prefix="/api/ingestion/atlassian", tags=["atlassian"])


@router.get("/auth/status", response_model=AtlassianAuthStatus)
def get_auth_status(
    service: AtlassianOAuthService = Depends(get_atlassian_oauth_service),
) -> AtlassianAuthStatus:
    return service.get_auth_status()


@router.get("/auth/start", response_model=AtlassianAuthorizationStart)
def start_auth_flow(
    service: AtlassianOAuthService = Depends(get_atlassian_oauth_service),
) -> AtlassianAuthorizationStart:
    try:
        return service.build_authorization_start()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/auth/callback", response_class=HTMLResponse)
def oauth_callback(
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
    error_description: str | None = Query(default=None),
    service: AtlassianOAuthService = Depends(get_atlassian_oauth_service),
) -> HTMLResponse:
    try:
        status = service.handle_callback(
            code=code,
            state=state,
            error=error,
            error_description=error_description,
        )
    except ValueError as exc:
        return HTMLResponse(
            status_code=400,
            content=_render_callback_page(
                title="Atlassian OAuth fehlgeschlagen",
                message=str(exc),
                notes=[],
                success=False,
            ),
        )
    return HTMLResponse(
        content=_render_callback_page(
            title="Atlassian OAuth abgeschlossen",
            message="Der lokale Auditor hat den Access Token gespeichert. Confluence-Live-Reads koennen jetzt verifiziert werden.",
            notes=status.notes,
            success=True,
        )
    )


@router.get("/confluence/verify", response_model=ConfluenceVerificationResponse)
def verify_confluence_access(
    space_key: str = Query(default="FINAI", min_length=1),
    max_pages: int = Query(default=3, ge=1, le=10),
    service: AtlassianOAuthService = Depends(get_atlassian_oauth_service),
) -> ConfluenceVerificationResponse:
    return service.verify_confluence_access(space_key=space_key, max_pages=max_pages)


@router.get("/confluence/pages")
def list_confluence_pages(
    space_key: str = Query(default="FINAI", min_length=1),
    max_pages: int = Query(default=50, ge=1, le=200),
    service: AtlassianOAuthService = Depends(get_atlassian_oauth_service),
) -> dict:
    """Return flat page list for the given space so the frontend can build a tree."""
    from fin_ai_auditor.config import get_settings
    from fin_ai_auditor.services.connectors.confluence_connector import (
        _fetch_pages_for_space,
        _fetch_space,
        _resolve_access_context,
    )
    import httpx

    settings = get_settings()
    access_token = service.get_valid_access_token()
    if not access_token:
        raise HTTPException(status_code=401, detail="Kein Atlassian Access Token vorhanden. Bitte zuerst OAuth-Anmeldung durchfuehren.")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    with httpx.Client(timeout=20.0, headers=headers) as client:
        access_ctx = _resolve_access_context(client=client, settings=settings, access_token=access_token)
        space = _fetch_space(client=client, api_base_url=access_ctx.api_base_url, space_key=space_key.upper())
        if space is None:
            raise HTTPException(status_code=404, detail=f"Confluence Space '{space_key}' nicht gefunden.")
        space_id = str(space.get("id") or "")
        space_name = str(space.get("name") or space_key)
        pages = _fetch_pages_for_space(client=client, api_base_url=access_ctx.api_base_url, space_id=space_id, limit=max_pages)
    items = []
    for p in pages:
        page_id = str(p.get("id") or "")
        title = str(p.get("title") or "")
        parent_id = str(p.get("parentId") or "")
        items.append({"id": page_id, "title": title, "parentId": parent_id})
    return {"space_key": space_key.upper(), "space_name": space_name, "pages": items}


def _render_callback_page(
    *,
    title: str,
    message: str,
    notes: list[str],
    success: bool,
) -> str:
    note_items = "".join(f"<li>{_html_escape(note)}</li>" for note in notes if note)
    return f"""
<!doctype html>
<html lang="de">
  <head>
    <meta charset="utf-8" />
    <title>{_html_escape(title)}</title>
    <style>
      body {{
        font-family: ui-sans-serif, system-ui, sans-serif;
        background: #f3efe5;
        color: #1f1f1b;
        margin: 0;
        padding: 32px;
      }}
      main {{
        max-width: 720px;
        margin: 0 auto;
        background: #fffdf7;
        border: 1px solid #d8cfba;
        border-radius: 18px;
        padding: 28px;
        box-shadow: 0 18px 50px rgba(59, 43, 9, 0.08);
      }}
      .status {{
        display: inline-block;
        padding: 6px 10px;
        border-radius: 999px;
        background: {"#d7f5de" if success else "#f7d9d9"};
        color: {"#175c25" if success else "#8c2323"};
        font-weight: 600;
      }}
      ul {{
        padding-left: 20px;
      }}
    </style>
  </head>
  <body>
    <main>
      <span class="status">{'Erfolgreich' if success else 'Fehler'}</span>
      <h1>{_html_escape(title)}</h1>
      <p>{_html_escape(message)}</p>
      {"<ul>" + note_items + "</ul>" if note_items else ""}
      <p>Dieses Fenster kann geschlossen werden. Der Auditor aktualisiert den Status nach dem naechsten Reload.</p>
    </main>
  </body>
</html>
""".strip()


def _html_escape(value: str) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )
