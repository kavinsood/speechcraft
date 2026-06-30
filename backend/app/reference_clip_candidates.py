from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .dataset_runs import get_candidate_review_media_bytes

REFERENCE_CLIP_CANDIDATES_DIR = "reference-clip-candidates"
FORBIDDEN_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
MAX_FILENAME_STEM_LENGTH = 180


def reference_clip_candidates_root(media_root: Path, project_id: str) -> Path:
    return (media_root / REFERENCE_CLIP_CANDIDATES_DIR / project_id).resolve()


def reference_clip_candidates_manifest_path(media_root: Path, project_id: str) -> Path:
    return reference_clip_candidates_root(media_root, project_id) / "manifest.jsonl"


def transcript_filename_stem(transcript_text: str, clip_id: str) -> str:
    normalized = " ".join(transcript_text.strip().split())
    normalized = FORBIDDEN_FILENAME_CHARS.sub("", normalized).strip(" .")
    if not normalized:
        normalized = clip_id.strip() or "reference-clip-candidate"
    if len(normalized) > MAX_FILENAME_STEM_LENGTH:
        normalized = normalized[:MAX_FILENAME_STEM_LENGTH].rstrip(" .")
    return normalized or clip_id


def append_manifest_entry(manifest_path: Path, entry: dict[str, Any]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True))
        handle.write("\n")


def resolve_unique_wav_path(directory: Path, stem: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    candidate = directory / f"{stem}.wav"
    if not candidate.exists():
        return candidate
    for index in range(2, 1000):
        candidate = directory / f"{stem} ({index}).wav"
        if not candidate.exists():
            return candidate
    raise ValueError(f"Could not allocate a unique filename for {stem!r}")


def _get_dataset_run_for_project(repository: Any, project_id: str, dataset_run_id: str) -> None:
    from sqlmodel import Session

    from .models import ProcessingRun

    with Session(repository.engine, expire_on_commit=False) as session:
        run = session.get(ProcessingRun, dataset_run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        if run.project_id != project_id:
            raise ValueError("Dataset run does not belong to this project")


def mark_dataset_clip_as_reference_candidate(
    repository: Any,
    *,
    project_id: str,
    dataset_run_id: str,
    clip_id: str,
    transcript_text: str,
) -> dict[str, Any]:
    repository.get_project(project_id)
    _get_dataset_run_for_project(repository, project_id, dataset_run_id)

    audio_bytes = get_candidate_review_media_bytes(repository, dataset_run_id, clip_id)
    destination_root = reference_clip_candidates_root(repository.media_root, project_id)
    stem = transcript_filename_stem(transcript_text, clip_id)
    destination_path = resolve_unique_wav_path(destination_root, stem)
    destination_path.write_bytes(audio_bytes)

    media_root = repository.media_root.resolve()
    relative_path = destination_path.relative_to(media_root).as_posix()
    source_relative = f"dataset-runs/{dataset_run_id}/candidate-review/{clip_id}.wav"
    entry = {
        "project_id": project_id,
        "dataset_run_id": dataset_run_id,
        "clip_id": clip_id,
        "transcript_text": transcript_text.strip(),
        "filename": destination_path.name,
        "relative_path": relative_path,
        "source_audio_path": source_relative,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    append_manifest_entry(reference_clip_candidates_manifest_path(repository.media_root, project_id), entry)
    return entry
