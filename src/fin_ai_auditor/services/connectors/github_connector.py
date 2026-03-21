from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from fin_ai_auditor.domain.models import AuditSourceSnapshot
from fin_ai_auditor.services.pipeline_models import (
    CachedCollectedDocument,
    CollectedSourceType,
    CollectionBundle,
    CollectedDocument,
)


_TEXT_FILE_SUFFIXES: tuple[str, ...] = (
    ".py",
    ".pyi",
    ".ts",
    ".tsx",
    ".js",
    ".jsx",
    ".json",
    ".yaml",
    ".yml",
    ".md",
    ".txt",
    ".toml",
    ".ini",
    ".cfg",
    ".sh",
    ".puml",
    ".plantuml",
)
_EXCLUDED_DIR_NAMES: frozenset[str] = frozenset(
    {
        ".git",
        ".venv",
        ".mypy_cache",
        ".pytest_cache",
        "__pycache__",
        "node_modules",
        "dist",
        "build",
        "coverage",
        "data",
        "tmp",
    }
)


@dataclass(frozen=True)
class GitHubSnapshotRequest:
    git_ref: str
    repo_url: str | None = None
    local_repo_path: str | None = None
    max_files: int | None = None
    max_chars_per_file: int | None = None
    previous_snapshots: list[AuditSourceSnapshot] | None = None
    document_cache_lookup: Callable[[str, str, str], CachedCollectedDocument | None] | None = None


@dataclass(frozen=True)
class _RepoDocumentEntry:
    relative_path: str
    title: str
    source_type: CollectedSourceType
    body: str | None = None
    file_path: Path | None = None
    metadata: dict[str, object] = field(default_factory=dict)


class GitHubSnapshotConnector:
    """Read-only Collector fuer das lokale FIN-AI-Repo oder einen GitHub-Checkout."""

    def collect_snapshot(self, *, request: GitHubSnapshotRequest) -> CollectionBundle:
        repo_path = _resolve_repo_path(request=request)
        git_repo_available = _is_git_repo(repo_path=repo_path)
        repo_revision = _resolve_git_revision(
            repo_path=repo_path,
            git_ref=request.git_ref,
            require_exact=git_repo_available,
        )
        read_mode = "git_ref" if git_repo_available else "working_tree"

        finai_backend_focus_active = _is_finai_repo_with_backend_focus(repo_path=repo_path)
        previous_snapshot_map = _build_previous_snapshot_map(previous_snapshots=request.previous_snapshots or [])
        changed_paths = (
            _resolve_changed_paths(
                repo_path=repo_path,
                repo_revision=repo_revision,
                previous_snapshots=request.previous_snapshots or [],
            )
            if git_repo_available
            else None
        )
        snapshots: list[AuditSourceSnapshot] = []
        documents: list[CollectedDocument] = []
        inspected_files = 0
        reused_documents = 0
        reread_documents = 0

        for entry in _iter_repo_documents(
            repo_path=repo_path,
            repo_revision=repo_revision if git_repo_available else None,
        ):
            relative_path = entry.relative_path
            if finai_backend_focus_active and not _is_relevant_finai_analysis_path(relative_path=relative_path):
                continue
            if request.max_files is not None and inspected_files >= int(request.max_files):
                break
            inspected_files += 1
            source_type = entry.source_type
            previous_snapshot = previous_snapshot_map.get((source_type, relative_path))
            can_reuse = _can_reuse_snapshot(
                previous_snapshot=previous_snapshot,
                relative_path=relative_path,
                repo_revision=repo_revision,
                current_metadata=entry.metadata,
                changed_paths=changed_paths,
            )
            if can_reuse and previous_snapshot is not None and request.document_cache_lookup is not None:
                cached_document = request.document_cache_lookup(
                    source_type,
                    relative_path,
                    str(previous_snapshot.content_hash or ""),
                )
                if cached_document is not None:
                    snapshots.append(
                        AuditSourceSnapshot(
                            source_type=source_type,
                            source_id=relative_path,
                            revision_id=repo_revision,
                            content_hash=previous_snapshot.content_hash,
                            sync_token=f"git:{repo_revision}:{relative_path}",
                            metadata={
                                "repo_path": str(repo_path),
                                "git_ref": request.git_ref,
                                "repo_url": str(request.repo_url or "").strip() or None,
                                "truncated": bool(cached_document.metadata.get("truncated", False)),
                                "char_count": _coerce_int_metadata(
                                    cached_document.metadata.get("char_count"),
                                    default=len(cached_document.body),
                                ),
                                "incremental_reused": True,
                                "reused_from_snapshot_id": previous_snapshot.snapshot_id,
                                **entry.metadata,
                            },
                        )
                    )
                    documents.append(
                        CollectedDocument(
                            snapshot=snapshots[-1],
                            source_type=source_type,
                            source_id=relative_path,
                            title=cached_document.title,
                            body=cached_document.body,
                            path_hint=cached_document.path_hint,
                            url=cached_document.url,
                            metadata={
                                **cached_document.metadata,
                                "repo_revision": repo_revision,
                                "incremental_reused": True,
                                "source_read_mode": read_mode,
                            },
                        )
                    )
                    reused_documents += 1
                    continue

            raw_text = entry.body
            if raw_text is None and entry.file_path is not None:
                raw_text = _read_text_file(file_path=entry.file_path)
            if raw_text is None:
                continue
            truncated_text = (
                raw_text[: int(request.max_chars_per_file)]
                if request.max_chars_per_file is not None
                else raw_text
            )
            content_hash = _sha256_text(raw_text)
            snapshot = AuditSourceSnapshot(
                source_type=source_type,
                source_id=relative_path,
                revision_id=repo_revision,
                content_hash=content_hash,
                sync_token=f"git:{repo_revision}:{relative_path}",
                metadata={
                    "repo_path": str(repo_path),
                    "git_ref": request.git_ref,
                    "repo_url": str(request.repo_url or "").strip() or None,
                    "truncated": len(truncated_text) < len(raw_text),
                    "char_count": len(raw_text),
                    "incremental_reused": False,
                    **entry.metadata,
                },
            )
            snapshots.append(snapshot)
            documents.append(
                CollectedDocument(
                    snapshot=snapshot,
                    source_type=source_type,
                    source_id=relative_path,
                    title=entry.title,
                    body=truncated_text,
                    path_hint=relative_path,
                    metadata={"repo_revision": repo_revision, "source_read_mode": read_mode, **entry.metadata},
                )
            )
            reread_documents += 1

        repo_label = str(request.repo_url or "").strip() or str(repo_path)
        return CollectionBundle(
            snapshots=snapshots,
            documents=documents,
            analysis_notes=[
                f"{len(documents)} lesbare Dateien aus {repo_label} wurden fuer den Lauf eingesammelt.",
                f"Analysierte Revision: {repo_revision}.",
                (
                    f"Die Quellen wurden exakt aus dem Git-Ref {request.git_ref} gelesen."
                    if git_repo_available
                    else "Das Zielverzeichnis ist kein nutzbares Git-Repo; es wurde konservativ der aktuelle Arbeitsbaum gelesen."
                ),
                (
                    "Fachliche Repo-Abdeckung ist auf relevante FIN-AI-Backend-, Chunking-, Mining- und Write-Contract-Pfade fokussiert."
                    if finai_backend_focus_active
                    else "Repo-Abdeckung nutzt fuer dieses Zielrepo keine FIN-AI-spezifische Pfadfokussierung."
                ),
                (
                    f"Inkrementelle Repo-Wiederverwendung: {reused_documents} Dokumente aus dem lokalen Cache uebernommen, "
                    f"{reread_documents} Dateien real neu gelesen."
                ),
            ],
        )


def _resolve_repo_path(*, request: GitHubSnapshotRequest) -> Path:
    local_repo_path = str(request.local_repo_path or "").strip()
    if not local_repo_path:
        repo_url = str(request.repo_url or "").strip()
        if repo_url:
            raise ValueError(
                "GitHub-Repo-URLs allein werden vom Auditor aktuell nicht direkt ausgecheckt. "
                "Bitte einen lokalen Checkout ueber local_repo_path angeben."
            )
        raise ValueError("Mindestens local_repo_path muss auf ein lesbares lokales Repository zeigen.")
    repo_path = Path(local_repo_path).expanduser().resolve()
    if not repo_path.exists() or not repo_path.is_dir():
        raise ValueError(f"Lokaler Repo-Pfad nicht gefunden: {repo_path}")
    return repo_path


def _iter_repo_files(*, repo_path: Path) -> list[Path]:
    candidates = [
        path
        for path in repo_path.rglob("*")
        if path.is_file()
        and path.suffix.lower() in _TEXT_FILE_SUFFIXES
        and not any(part in _EXCLUDED_DIR_NAMES for part in path.parts)
    ]
    return sorted(candidates, key=lambda item: item.as_posix())


def _iter_repo_documents(*, repo_path: Path, repo_revision: str | None) -> list[_RepoDocumentEntry]:
    if repo_revision is not None:
        return _iter_repo_documents_from_git(repo_path=repo_path, repo_revision=repo_revision)
    return _iter_repo_documents_from_worktree(repo_path=repo_path)


def _iter_repo_documents_from_worktree(*, repo_path: Path) -> list[_RepoDocumentEntry]:
    entries: list[_RepoDocumentEntry] = []
    for file_path in _iter_repo_files(repo_path=repo_path):
        relative_path = file_path.relative_to(repo_path).as_posix()
        stat_result = file_path.stat()
        entries.append(
            _RepoDocumentEntry(
                relative_path=relative_path,
                title=file_path.stem.replace("_", " ").replace("-", " ").strip() or relative_path,
                source_type="local_doc" if relative_path.startswith("_docs/") or file_path.suffix == ".md" else "github_file",
                file_path=file_path,
                metadata={
                    "file_size": int(stat_result.st_size),
                    "file_mtime_ns": int(stat_result.st_mtime_ns),
                    "source_read_mode": "working_tree",
                },
            )
        )
    return entries


def _iter_repo_documents_from_git(*, repo_path: Path, repo_revision: str) -> list[_RepoDocumentEntry]:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_path), "ls-tree", "-r", "--name-only", repo_revision],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        raise ValueError(f"Git-Dateiliste fuer Ref {repo_revision} konnte nicht gelesen werden: {exc}") from exc
    entries: list[_RepoDocumentEntry] = []
    for raw_path in sorted(line.strip() for line in str(result.stdout or "").splitlines() if line.strip()):
        path_obj = Path(raw_path)
        if path_obj.suffix.lower() not in _TEXT_FILE_SUFFIXES:
            continue
        if any(part in _EXCLUDED_DIR_NAMES for part in path_obj.parts):
            continue
        raw_text = _read_git_text_file(repo_path=repo_path, repo_revision=repo_revision, relative_path=raw_path)
        if raw_text is None:
            continue
        entries.append(
            _RepoDocumentEntry(
                relative_path=raw_path,
                title=path_obj.stem.replace("_", " ").replace("-", " ").strip() or raw_path,
                source_type="local_doc" if raw_path.startswith("_docs/") or path_obj.suffix == ".md" else "github_file",
                body=raw_text,
                metadata={"source_read_mode": "git_ref"},
            )
        )
    return entries


def _is_finai_repo_with_backend_focus(*, repo_path: Path) -> bool:
    return (repo_path / "src" / "finai").is_dir()


def _is_relevant_finai_analysis_path(*, relative_path: str) -> bool:
    normalized = str(relative_path or "").strip().replace("\\", "/")
    if not normalized:
        return False
    if normalized in {"README.md", "AGENTS.md", "CLAUDE.md"}:
        return True
    return normalized.startswith(
        (
            "src/finai/",
            "_docs/",
            "models/",
            "config/",
        )
    )


def _read_text_file(*, file_path: Path) -> str | None:
    try:
        raw = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            raw = file_path.read_text(encoding="latin-1")
        except UnicodeDecodeError:
            return None
    except OSError:
        return None
    if "\x00" in raw:
        return None
    return raw


def _read_git_text_file(*, repo_path: Path, repo_revision: str, relative_path: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_path), "show", f"{repo_revision}:{relative_path}"],
            capture_output=True,
            text=False,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    for encoding in ("utf-8", "latin-1"):
        try:
            raw = bytes(result.stdout or b"").decode(encoding)
        except UnicodeDecodeError:
            continue
        if "\x00" in raw:
            return None
        return raw
    return None


def _is_git_repo(*, repo_path: Path) -> bool:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_path), "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False
    return str(result.stdout or "").strip() == "true"


def _resolve_git_revision(*, repo_path: Path, git_ref: str, require_exact: bool) -> str:
    normalized_ref = str(git_ref or "").strip() or "HEAD"
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_path), "rev-parse", f"{normalized_ref}^{{commit}}"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        if require_exact:
            raise ValueError(f"Git-Ref konnte nicht aufgeloest werden: {normalized_ref}") from exc
        return normalized_ref
    return str(result.stdout or "").strip() or normalized_ref


def _sha256_text(value: str) -> str:
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _build_previous_snapshot_map(
    *,
    previous_snapshots: list[AuditSourceSnapshot],
) -> dict[tuple[str, str], AuditSourceSnapshot]:
    return {
        (snapshot.source_type, snapshot.source_id): snapshot
        for snapshot in previous_snapshots
        if snapshot.source_type in {"github_file", "local_doc"}
    }


def _resolve_changed_paths(
    *,
    repo_path: Path,
    repo_revision: str,
    previous_snapshots: list[AuditSourceSnapshot],
) -> set[str] | None:
    previous_revisions = {
        str(snapshot.revision_id or "").strip()
        for snapshot in previous_snapshots
        if snapshot.source_type in {"github_file", "local_doc"} and str(snapshot.revision_id or "").strip()
    }
    if len(previous_revisions) != 1:
        return None
    previous_revision = next(iter(previous_revisions))
    if not previous_revision or previous_revision == repo_revision:
        return None
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_path), "diff", "--name-only", previous_revision, repo_revision],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    changed = {
        line.strip()
        for line in str(result.stdout or "").splitlines()
        if line.strip()
    }
    return changed


def _can_reuse_snapshot(
    *,
    previous_snapshot: AuditSourceSnapshot | None,
    relative_path: str,
    repo_revision: str,
    current_metadata: dict[str, object],
    changed_paths: set[str] | None,
) -> bool:
    if previous_snapshot is None:
        return False
    previous_revision = str(previous_snapshot.revision_id or "").strip()
    if (
        previous_revision
        and previous_revision == repo_revision
        and str(current_metadata.get("source_read_mode") or "").strip() == "git_ref"
    ):
        return True
    if changed_paths is not None:
        return relative_path not in changed_paths
    current_size = _coerce_int_metadata(current_metadata.get("file_size"))
    current_mtime_ns = _coerce_int_metadata(current_metadata.get("file_mtime_ns"))
    if current_size < 0 or current_mtime_ns < 0:
        return False
    previous_size = _coerce_int_metadata(previous_snapshot.metadata.get("file_size"))
    previous_mtime_ns = _coerce_int_metadata(previous_snapshot.metadata.get("file_mtime_ns"))
    return previous_size == current_size and previous_mtime_ns == current_mtime_ns


def _coerce_int_metadata(value: object, *, default: int = -1) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return default
        try:
            return int(stripped)
        except ValueError:
            return default
    return default
