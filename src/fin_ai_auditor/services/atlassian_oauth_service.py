from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode, urlparse

import httpx

from fin_ai_auditor.config import Settings
from fin_ai_auditor.domain.models import (
    AtlassianAuthStatus,
    AtlassianAuthorizationStart,
    AtlassianOAuthStateRecord,
    AtlassianOAuthTokenRecord,
    ConfluenceVerificationResponse,
    utc_now_iso,
)
from fin_ai_auditor.services.audit_repository import SQLiteAuditRepository
from fin_ai_auditor.services.connectors.confluence_connector import (
    ConfluenceCollectionRequest,
    ConfluenceKnowledgeBaseConnector,
)


_ATLASSIAN_AUTHORIZE_URL = "https://auth.atlassian.com/authorize"
_ATLASSIAN_TOKEN_URL = "https://auth.atlassian.com/oauth/token"
_CALLBACK_PATH = "/api/ingestion/atlassian/auth/callback"


class AtlassianOAuthService:
    def __init__(self, *, repository: SQLiteAuditRepository, settings: Settings) -> None:
        self._repository = repository
        self._settings = settings

    def get_auth_status(self) -> AtlassianAuthStatus:
        configured_redirect_uri = str(self._settings.atlassian_oauth_redirect_uri or "").strip() or None
        redirect_uri = _recommended_redirect_uri(settings=self._settings)
        token = self._repository.get_atlassian_token()
        notes: list[str] = []
        token_valid = False
        token_expires_at = token.expires_at if token is not None else None
        if token is None:
            notes.append("Noch kein lokaler Atlassian Access Token im Auditor gespeichert.")
        else:
            token_valid = _token_is_valid(token=token)
            if token_valid:
                notes.append("Ein lokaler Atlassian Access Token ist vorhanden und fuer Live-Reads nutzbar.")
            elif token.refresh_token:
                notes.append("Der gespeicherte Atlassian Access Token ist abgelaufen und wird beim naechsten Read refreshiert.")
            else:
                notes.append("Der gespeicherte Atlassian Access Token ist abgelaufen und braucht neuen User-Consent.")
        if configured_redirect_uri and not _redirect_matches_local_api(
            configured_redirect_uri=configured_redirect_uri,
            expected_redirect_uri=redirect_uri,
        ):
            notes.append(
                "Die konfigurierte Redirect-URI passt nicht zum lokalen Auditor. Fuer den separaten OAuth-Flow "
                f"sollte {redirect_uri} in Atlassian registriert werden."
            )
        elif configured_redirect_uri:
            notes.append("Die konfigurierte Redirect-URI passt zum lokalen Auditor-Callback.")

        return AtlassianAuthStatus(
            enabled=bool(self._settings.atlassian_enabled),
            client_configured=bool(
                self._settings.atlassian_oauth_client_id and self._settings.atlassian_oauth_client_secret
            ),
            token_present=token is not None,
            token_valid=token_valid,
            needs_user_consent=token is None or (not token_valid and not bool(token.refresh_token if token else None)),
            redirect_uri=redirect_uri,
            configured_redirect_uri=configured_redirect_uri,
            recommended_redirect_uri=redirect_uri,
            redirect_uri_matches_local_api=(
                bool(configured_redirect_uri)
                and _redirect_matches_local_api(
                    configured_redirect_uri=configured_redirect_uri,
                    expected_redirect_uri=redirect_uri,
                )
            ),
            scope=str(self._settings.atlassian_oauth_scope or "").strip() or None,
            token_expires_at=token_expires_at,
            notes=notes,
        )

    def build_authorization_start(self) -> AtlassianAuthorizationStart:
        self._ensure_client_configured()
        redirect_uri = _recommended_redirect_uri(settings=self._settings)
        state = AtlassianOAuthStateRecord(
            expires_at=(datetime.now(UTC) + timedelta(minutes=15)).isoformat(),
            redirect_uri=redirect_uri,
            scope=_resolve_scope(settings=self._settings),
            metadata={
                "configured_redirect_uri": str(self._settings.atlassian_oauth_redirect_uri or "").strip() or None,
                "recommended_redirect_uri": redirect_uri,
            },
        )
        stored_state = self._repository.save_atlassian_oauth_state(state=state)
        query = urlencode(
            {
                "audience": "api.atlassian.com",
                "client_id": self._settings.atlassian_oauth_client_id,
                "scope": _resolve_scope(settings=self._settings),
                "redirect_uri": redirect_uri,
                "state": stored_state.state_id,
                "response_type": "code",
                "prompt": "consent",
            }
        )
        notes = [
            "Der Consent-Flow bleibt read-only; der Token wird nur fuer Confluence-Reads im lokalen Auditor gespeichert.",
        ]
        configured_redirect_uri = str(self._settings.atlassian_oauth_redirect_uri or "").strip() or None
        if configured_redirect_uri and configured_redirect_uri != redirect_uri:
            notes.append(
                "Der Auditor verwendet bewusst die lokale Redirect-URI. Falls Atlassian noch die FIN-AI-Callback-URI "
                "registriert hat, muss die App-Konfiguration auf den Auditor angepasst werden."
            )
        return AtlassianAuthorizationStart(
            authorization_url=f"{_ATLASSIAN_AUTHORIZE_URL}?{query}",
            state_id=stored_state.state_id,
            redirect_uri=redirect_uri,
            notes=notes,
        )

    def handle_callback(
        self,
        *,
        code: str | None,
        state: str | None,
        error: str | None,
        error_description: str | None,
    ) -> AtlassianAuthStatus:
        if error:
            raise ValueError(f"Atlassian OAuth Fehler: {error}: {str(error_description or '').strip()}")
        if not code or not state:
            raise ValueError("Atlassian OAuth Callback enthaelt keinen gueltigen Code oder State.")
        state_record = self._repository.get_atlassian_oauth_state(state_id=state)
        if state_record is None:
            raise ValueError("Unbekannter Atlassian OAuth State.")
        if state_record.status != "pending":
            raise ValueError("Atlassian OAuth State ist bereits verbraucht oder ungueltig.")
        if _parse_iso(state_record.expires_at) <= datetime.now(UTC):
            expired_state = state_record.model_copy(update={"status": "expired"})
            self._repository.save_atlassian_oauth_state(state=expired_state)
            raise ValueError("Atlassian OAuth State ist abgelaufen.")

        token = self._exchange_code_for_token(code=code, redirect_uri=state_record.redirect_uri)
        self._repository.upsert_atlassian_token(token=token)
        self._repository.save_atlassian_oauth_state(state=state_record.model_copy(update={"status": "consumed"}))
        return self.get_auth_status()

    def get_valid_access_token(self) -> str | None:
        token = self._repository.get_atlassian_token()
        if token is not None and _token_is_valid(token=token):
            return token.access_token
        if token is not None and token.refresh_token:
            refreshed = self._refresh_access_token(token=token)
            self._repository.upsert_atlassian_token(token=refreshed)
            return refreshed.access_token
        env_map = self._settings._collect_external_env_map()
        for key in (
            "FIN_AI_AUDITOR_ATLASSIAN_ACCESS_TOKEN",
            "FINAI_ATLASSIAN_ACCESS_TOKEN",
            "ATLASSIAN_ACCESS_TOKEN",
            "CONFLUENCE_ACCESS_TOKEN",
        ):
            value = str(env_map.get(key) or "").strip()
            if value:
                return value
        return None

    def invalidate_token(self) -> None:
        """Delete the stored OAuth token explicitly."""
        self._repository.delete_atlassian_token()

    def get_valid_access_token_or_raise(self, *, required_scopes: set[str] | None = None) -> str:
        access_token = self.get_valid_access_token()
        if access_token is None:
            raise ValueError("Kein gueltiger Atlassian Access Token fuer den angeforderten Live-Zugriff vorhanden.")
        if required_scopes:
            available_scopes = self.get_granted_scope_set()
            if not available_scopes:
                raise ValueError(
                    "Dem aktuellen Atlassian-Token sind keine explizit gewaehrten Scopes zuordenbar. "
                    "Bitte den lokalen OAuth-Consent fuer den Auditor erneut durchlaufen, bevor ein Writeback ausgefuehrt wird."
                )
            missing_scopes = sorted(scope for scope in required_scopes if scope.casefold() not in available_scopes)
            if missing_scopes:
                raise ValueError(
                    "Dem aktuellen Atlassian-Kontext fehlen die noetigen Scopes fuer diesen Writeback: "
                    + ", ".join(missing_scopes)
                )
        return access_token

    def get_granted_scope_set(self) -> set[str]:
        token = self._repository.get_atlassian_token()
        if token is not None and token.scope:
            return _parse_scope_set(token.scope)
        return set()

    def get_configured_scope_set(self) -> set[str]:
        scope_candidates: list[str] = []
        configured_scope = str(self._settings.atlassian_oauth_scope or "").strip()
        if configured_scope:
            scope_candidates.append(configured_scope)
        env_map = self._settings._collect_external_env_map()
        for key in ("FINAI_ATLASSIAN_OAUTH_SCOPE", "ATLASSIAN_OAUTH_SCOPE"):
            value = str(env_map.get(key) or "").strip()
            if value:
                scope_candidates.append(value)
        normalized: set[str] = set()
        for scope_text in scope_candidates:
            normalized.update(_parse_scope_set(scope_text))
        return normalized

    def get_effective_scope_set(self) -> set[str]:
        granted = self.get_granted_scope_set()
        return granted or self.get_configured_scope_set()

    def get_runtime_access_status(self) -> dict[str, object]:
        auth_status = self.get_auth_status()
        token_available = False
        refreshed = False
        try:
            token_available = bool(self.get_valid_access_token())
        except Exception:
            token_available = False
        if token_available:
            refreshed = not bool(auth_status.token_valid)
            auth_status = self.get_auth_status()
        return {
            "token_available": token_available,
            "token_valid": bool(auth_status.token_valid),
            "refreshed_during_check": refreshed,
            "granted_scopes": sorted(self.get_granted_scope_set()),
            "configured_scopes": sorted(self.get_configured_scope_set()),
            "effective_scopes": sorted(self.get_effective_scope_set()),
            "oauth_ready": bool(
                auth_status.enabled
                and auth_status.client_configured
                and auth_status.redirect_uri_matches_local_api
            ),
            "auth_status": auth_status,
        }

    def build_scope_verification(
        self,
        *,
        required_scopes: set[str] | None = None,
        target_url: str | None = None,
        target_type: str | None = None,
    ) -> dict[str, Any]:
        required = {str(scope or "").strip().casefold() for scope in (required_scopes or set()) if str(scope or "").strip()}
        granted = self.get_granted_scope_set()
        configured = self.get_configured_scope_set()
        effective = self.get_effective_scope_set()
        auth_status = self.get_auth_status()
        missing = sorted(scope for scope in required if scope not in granted) if required else []
        return {
            "target_type": str(target_type or "").strip() or None,
            "target_url": str(target_url or "").strip() or None,
            "required_scopes": sorted(required),
            "granted_scopes": sorted(granted),
            "configured_scopes": sorted(configured),
            "effective_scopes": sorted(effective),
            "missing_scopes": missing,
            "token_valid": bool(auth_status.token_valid),
            "needs_user_consent": bool(auth_status.needs_user_consent),
            "redirect_uri_matches_local_api": bool(auth_status.redirect_uri_matches_local_api),
            "oauth_ready": bool(
                auth_status.enabled
                and auth_status.client_configured
                and auth_status.redirect_uri_matches_local_api
            ),
        }

    def verify_confluence_access(self, *, space_key: str, max_pages: int = 3) -> ConfluenceVerificationResponse:
        connector = ConfluenceKnowledgeBaseConnector(settings=self._settings).with_access_token_provider(
            access_token_provider=self.get_valid_access_token
        )
        bundle = connector.collect_pages(
            request=ConfluenceCollectionRequest(space_keys=[space_key], max_pages_per_space=max(1, max_pages))
        )
        return ConfluenceVerificationResponse(
            ok=bool(bundle.documents),
            space_key=space_key,
            page_count=len(bundle.documents),
            page_titles=[document.title for document in bundle.documents[: max(1, max_pages)]],
            analysis_notes=list(bundle.analysis_notes),
        )

    def _exchange_code_for_token(self, *, code: str, redirect_uri: str) -> AtlassianOAuthTokenRecord:
        self._ensure_client_configured()
        payload = {
            "grant_type": "authorization_code",
            "client_id": self._settings.atlassian_oauth_client_id,
            "client_secret": self._settings.atlassian_oauth_client_secret,
            "code": code,
            "redirect_uri": redirect_uri,
        }
        with httpx.Client(timeout=20.0) as client:
            response = client.post(_ATLASSIAN_TOKEN_URL, json=payload)
            response.raise_for_status()
            body = response.json()
        return _token_from_response(body=body)

    def _refresh_access_token(self, *, token: AtlassianOAuthTokenRecord) -> AtlassianOAuthTokenRecord:
        self._ensure_client_configured()
        payload = {
            "grant_type": "refresh_token",
            "client_id": self._settings.atlassian_oauth_client_id,
            "client_secret": self._settings.atlassian_oauth_client_secret,
            "refresh_token": token.refresh_token,
        }
        with httpx.Client(timeout=20.0) as client:
            response = client.post(_ATLASSIAN_TOKEN_URL, json=payload)
            response.raise_for_status()
            body = response.json()
        return _token_from_response(body=body, fallback_refresh_token=token.refresh_token)

    def _ensure_client_configured(self) -> None:
        if not self._settings.atlassian_enabled:
            raise ValueError("Atlassian ist fuer den Auditor nicht aktiviert.")
        if not self._settings.atlassian_oauth_client_id or not self._settings.atlassian_oauth_client_secret:
            raise ValueError("Atlassian OAuth Client-Credentials fehlen im Auditor.")


def _resolve_scope(*, settings: Settings) -> str:
    scope = str(settings.atlassian_oauth_scope or "").strip()
    return scope or (
        "read:jira-work read:jira-user read:confluence-content.summary "
        "read:confluence-content.all readonly:content.attachment:confluence"
    )


def _recommended_redirect_uri(*, settings: Settings) -> str:
    host = "localhost" if settings.host in {"127.0.0.1", "0.0.0.0"} else settings.host
    return f"http://{host}:{int(settings.port)}{_CALLBACK_PATH}"


def _redirect_matches_local_api(*, configured_redirect_uri: str, expected_redirect_uri: str) -> bool:
    configured = urlparse(configured_redirect_uri)
    expected = urlparse(expected_redirect_uri)
    configured_port = configured.port or (443 if configured.scheme == "https" else 80)
    expected_port = expected.port or (443 if expected.scheme == "https" else 80)
    return (
        configured.scheme == expected.scheme
        and configured.hostname == expected.hostname
        and configured_port == expected_port
        and configured.path.rstrip("/") == expected.path.rstrip("/")
    )


def _token_is_valid(*, token: AtlassianOAuthTokenRecord, safety_window_seconds: int = 120) -> bool:
    if not token.access_token:
        return False
    if token.expires_at is None:
        return True
    return _parse_iso(token.expires_at) > datetime.now(UTC) + timedelta(seconds=max(0, safety_window_seconds))


def _token_from_response(
    *,
    body: dict[str, Any],
    fallback_refresh_token: str | None = None,
) -> AtlassianOAuthTokenRecord:
    access_token = str(body.get("access_token") or "").strip()
    if not access_token:
        raise ValueError("Atlassian OAuth Antwort enthaelt keinen Access Token.")
    expires_in = body.get("expires_in")
    expires_at: str | None = None
    if isinstance(expires_in, (int, float)) and float(expires_in) > 0:
        expires_at = (datetime.now(UTC) + timedelta(seconds=int(expires_in))).isoformat()
    refresh_token = str(body.get("refresh_token") or "").strip() or fallback_refresh_token
    return AtlassianOAuthTokenRecord(
        access_token=access_token,
        refresh_token=refresh_token,
        scope=str(body.get("scope") or "").strip() or None,
        token_type=str(body.get("token_type") or "bearer").strip() or "bearer",
        obtained_at=utc_now_iso(),
        expires_at=expires_at,
        metadata={
            "scope": str(body.get("scope") or "").strip() or None,
        },
    )


def _parse_iso(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _parse_scope_set(scope_text: str) -> set[str]:
    return {
        token.casefold()
        for token in str(scope_text or "").split()
        if token.strip()
    }
