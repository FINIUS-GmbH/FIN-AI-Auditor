from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class SecretStoreStatus:
    mode: str
    available: bool
    secure: bool
    backend: str
    notes: list[str] = field(default_factory=list)


class SecretStore:
    mode: str = "database_legacy"
    backend_label: str = "legacy_database"
    secure: bool = False

    def is_available(self) -> bool:
        return True

    def status(self) -> SecretStoreStatus:
        return SecretStoreStatus(
            mode=self.mode,
            available=self.is_available(),
            secure=self.secure,
            backend=self.backend_label,
        )

    def set_secret(self, *, key: str, value: str) -> None:
        raise NotImplementedError

    def get_secret(self, *, key: str) -> str | None:
        raise NotImplementedError

    def delete_secret(self, *, key: str) -> None:
        raise NotImplementedError


class MemorySecretStore(SecretStore):
    mode = "memory"
    backend_label = "memory"
    secure = False

    def __init__(self) -> None:
        self._secrets: dict[str, str] = {}

    def set_secret(self, *, key: str, value: str) -> None:
        self._secrets[key] = value

    def get_secret(self, *, key: str) -> str | None:
        return self._secrets.get(key)

    def delete_secret(self, *, key: str) -> None:
        self._secrets.pop(key, None)


class UnavailableSecretStore(SecretStore):
    mode = "keyring"
    backend_label = "keyring_unavailable"
    secure = True

    def __init__(self, *, reason: str) -> None:
        self._reason = str(reason or "").strip() or "Unbekannter Fehler"

    def is_available(self) -> bool:
        return False

    def status(self) -> SecretStoreStatus:
        status = super().status()
        status.notes.append(self._reason)
        return status

    def set_secret(self, *, key: str, value: str) -> None:
        raise RuntimeError(self._reason)

    def get_secret(self, *, key: str) -> str | None:
        return None

    def delete_secret(self, *, key: str) -> None:
        return None


class KeyringSecretStore(SecretStore):
    mode = "keyring"
    backend_label = "os_keyring"
    secure = True

    def __init__(self, *, service_name: str) -> None:
        self._service_name = str(service_name or "").strip() or "fin-ai-auditor"
        try:
            import keyring  # type: ignore
            from keyring.errors import KeyringError  # type: ignore
        except Exception as exc:  # pragma: no cover - runtime-only path
            raise RuntimeError(f"Keyring nicht verfuegbar: {exc}") from exc
        self._keyring = keyring
        self._keyring_error = KeyringError
        backend = keyring.get_keyring()
        self._backend_name = backend.__class__.__name__
        self.backend_label = f"os_keyring:{self._backend_name}"

    def set_secret(self, *, key: str, value: str) -> None:
        try:
            self._keyring.set_password(self._service_name, key, value)
        except self._keyring_error as exc:  # pragma: no cover - runtime-only path
            raise RuntimeError(f"Secret konnte nicht im Keyring gespeichert werden: {exc}") from exc

    def get_secret(self, *, key: str) -> str | None:
        try:
            return self._keyring.get_password(self._service_name, key)
        except self._keyring_error as exc:  # pragma: no cover - runtime-only path
            raise RuntimeError(f"Secret konnte nicht aus dem Keyring gelesen werden: {exc}") from exc

    def delete_secret(self, *, key: str) -> None:
        try:
            self._keyring.delete_password(self._service_name, key)
        except self._keyring_error:  # pragma: no cover - runtime-only path
            return None


def build_secret_store(*, mode: str, service_name: str) -> SecretStore:
    normalized_mode = str(mode or "").strip().lower() or "keyring"
    if normalized_mode == "memory":
        return MemorySecretStore()
    if normalized_mode == "database_legacy":
        return SecretStore()
    if normalized_mode != "keyring":
        return UnavailableSecretStore(reason=f"Unbekannter Secret-Store-Modus: {normalized_mode}")
    try:
        return KeyringSecretStore(service_name=service_name)
    except Exception as exc:
        return UnavailableSecretStore(reason=str(exc))
