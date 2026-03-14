from __future__ import annotations

import hashlib
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from fin_ai_auditor.domain.models import AuditSourceSnapshot
from fin_ai_auditor.services.pipeline_models import CachedCollectedDocument, CollectionBundle, CollectedDocument


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
    max_files: int = 400
    max_chars_per_file: int = 16000
    previous_snapshots: list[AuditSourceSnapshot] | None = None
    document_cache_lookup: Callable[[str, str, str], CachedCollectedDocument | None] | None = None


class GitHubSnapshotConnector:
    """Read-only Collector fuer das lokale FIN-AI-Repo oder einen GitHub-Checkout."""

    def collect_snapshot(self, *, request: GitHubSnapshotRequest) -> CollectionBundle:
        repo_path = Path(str(request.local_repo_path or "")).expanduser().resolve()
        if not repo_path.exists() or not repo_path.is_dir():
            raise ValueError(f"Lokaler Repo-Pfad nicht gefunden: {repo_path}")

        repo_revision = _resolve_git_revision(repo_path=repo_path, git_ref=request.git_ref)
        previous_snapshot_map = _build_previous_snapshot_map(previous_snapshots=request.previous_snapshots or [])
        changed_paths = _resolve_changed_paths(
            repo_path=repo_path,
            repo_revision=repo_revision,
            previous_snapshots=request.previous_snapshots or [],
        )
        snapshots: list[AuditSourceSnapshot] = []
        documents: list[CollectedDocument] = []
        inspected_files = 0
        reused_documents = 0
        reread_documents = 0

        for file_path in _iter_repo_files(repo_path=repo_path):
            if inspected_files >= int(request.max_files):
                break
            inspected_files += 1
            relative_path = file_path.relative_to(repo_path).as_posix()
            source_type = "local_doc" if relative_path.startswith("_docs/") or file_path.suffix == ".md" else "github_file"
            stat_result = file_path.stat()
            previous_snapshot = previous_snapshot_map.get((source_type, relative_path))
            can_reuse = _can_reuse_snapshot(
                previous_snapshot=previous_snapshot,
                relative_path=relative_path,
                stat_result=stat_result,
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
                                "char_count": int(cached_document.metadata.get("char_count") or len(cached_document.body)),
                                "file_size": int(stat_result.st_size),
                                "file_mtime_ns": int(stat_result.st_mtime_ns),
                                "incremental_reused": True,
                                "reused_from_snapshot_id": previous_snapshot.snapshot_id,
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
                            },
                        )
                    )
                    reused_documents += 1
                    continue

            raw_text = _read_text_file(file_path=file_path)
            if raw_text is None:
                continue
            truncated_text = raw_text[: int(request.max_chars_per_file)]
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
                    "file_size": int(stat_result.st_size),
                    "file_mtime_ns": int(stat_result.st_mtime_ns),
                    "incremental_reused": False,
                },
            )
            title = file_path.stem.replace("_", " ").replace("-", " ").strip() or relative_path
            snapshots.append(snapshot)
            documents.append(
                CollectedDocument(
                    snapshot=snapshot,
                    source_type=source_type,
                    source_id=relative_path,
                    title=title,
                    body=truncated_text,
                    path_hint=relative_path,
                    metadata={"repo_revision": repo_revision},
                )
            )
            reread_documents += 1

        return CollectionBundle(
            snapshots=snapshots,
            documents=documents,
            analysis_notes=[
                f"{len(documents)} lesbare Dateien aus dem lokalen FIN-AI-Repo wurden fuer den Lauf eingesammelt.",
                f"Git-Revision fuer den Snapshot: {repo_revision}.",
                (
                    f"Inkrementelle Repo-Wiederverwendung: {reused_documents} Dokumente aus dem lokalen Cache uebernommen, "
                    f"{reread_documents} Dateien real neu gelesen."
                ),
            ],
        )


def _iter_repo_files(*, repo_path: Path) -> list[Path]:
    candidates = [
        path
        for path in repo_path.rglob("*")
        if path.is_file()
        and path.suffix.lower() in _TEXT_FILE_SUFFIXES
        and not any(part in _EXCLUDED_DIR_NAMES for part in path.parts)
    ]
    return sorted(candidates, key=lambda item: item.as_posix())


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


def _resolve_git_revision(*, repo_path: Path, git_ref: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_path), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return str(git_ref or "unknown").strip() or "unknown"
    return str(result.stdout or "").strip() or str(git_ref or "unknown").strip() or "unknown"


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
    stat_result: os.stat_result,
    changed_paths: set[str] | None,
) -> bool:
    if previous_snapshot is None:
        return False
    if changed_paths is not None:
        return relative_path not in changed_paths
    previous_size = int(previous_snapshot.metadata.get("file_size") or -1)
    previous_mtime_ns = int(previous_snapshot.metadata.get("file_mtime_ns") or -1)
    return previous_size == int(stat_result.st_size) and previous_mtime_ns == int(stat_result.st_mtime_ns)
