from __future__ import annotations

import json
import os
import shutil
import wave
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlmodel import Session

from .clip_lab_audio import sha256_file
from .clip_lab_audio_ops import render_cache_path
from .clip_lab_state import (
    ClipLabValidationError,
    build_clip_lab_view,
    clip_lab_run_lock,
    index_manifest_by_clip_id,
    load_candidate_manifest,
    load_clip_lab_state,
)
from .dataset_runs import _run_root
from .models import (
    CanonicalExportBlockedReasonView,
    CanonicalExportPreviewView,
    CanonicalExportSummaryView,
    ProcessingRun,
)

SCHEMA_VERSION = 1
EXPORTS_RELATIVE_DIR = Path("artifacts/canonical_exports")
MANIFEST_FILENAME = "speechcraft_dataset.jsonl"
METADATA_FILENAME = "speechcraft_export.json"
REPORT_FILENAME = "export_report.json"


class CanonicalExportConflictError(ValueError):
    """Current Clip Lab state cannot produce a canonical export."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc_now_iso() -> str:
    return _utc_now().isoformat().replace("+00:00", "Z")


def _canonical_exports_root(run_root: Path) -> Path:
    return run_root / EXPORTS_RELATIVE_DIR


def _format_export_id(now: datetime) -> str:
    return f"canonical_export_{now.strftime('%Y-%m-%d_%H%M%S')}_{uuid4().hex[:8]}"


def _next_export_id(exports_root: Path) -> str:
    while True:
        candidate = _format_export_id(_utc_now())
        if not (exports_root / candidate).exists():
            return candidate


def _read_wave_metadata(path: Path) -> dict[str, int | float]:
    with wave.open(str(path), "rb") as handle:
        frame_count = int(handle.getnframes())
        sample_rate_hz = int(handle.getframerate())
        channels = int(handle.getnchannels())
    if sample_rate_hz <= 0:
        raise ClipLabValidationError(f"audio file has invalid sample rate: {path}")
    return {
        "sample_rate_hz": sample_rate_hz,
        "channels": channels,
        "duration_sec": round(frame_count / sample_rate_hz, 6),
    }


def _resolve_manifest_audio_path(run_root: Path, manifest_row: dict[str, Any], *, clip_id: str) -> Path:
    audio_path = manifest_row.get("audio_path")
    if not isinstance(audio_path, str) or not audio_path.strip():
        raise ClipLabValidationError(f"{clip_id} candidate manifest row is missing audio_path")
    return run_root / audio_path


def _relative_path(from_dir: Path, to_path: Path) -> str:
    return os.path.relpath(to_path, start=from_dir)


def _serialize_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _quality_payload(clip_view: dict[str, Any]) -> dict[str, float] | None:
    quality: dict[str, float] = {}
    transcript_match = clip_view.get("transcript_match")
    speaker_check = clip_view.get("speaker_check")
    if isinstance(transcript_match, (int, float)):
        quality["transcript_match"] = round(float(transcript_match), 2)
    if isinstance(speaker_check, (int, float)):
        quality["speaker_check"] = round(float(speaker_check), 2)
    return quality or None


def _build_clip_export_row(
    *,
    export_dir: Path,
    run_root: Path,
    manifest_row: dict[str, Any],
    clip_view: dict[str, Any],
    clip_entry: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, list[str], str | None, float]:
    clip_id = str(clip_view["clip_id"])
    transcript = clip_view["transcript"]
    if not isinstance(transcript, str):
        return None, ["transcript_missing"], None, 0.0

    source_audio_sha256 = clip_view.get("source_audio_sha256")
    if not isinstance(source_audio_sha256, str) or not source_audio_sha256:
        return None, ["source_audio_sha256_missing"], None, 0.0

    audio_path: Path | None = None
    audio_sha256: str | None = None
    audio_revision_hash: str | None = None
    reasons: list[str] = []
    audio_kind: str
    audio_edit = clip_entry.get("audio_edit") if isinstance(clip_entry, dict) else None
    has_active_audio_edit = isinstance(audio_edit, dict) and bool(audio_edit.get("ops") or [])

    if has_active_audio_edit:
        audio_kind = "rendered_revision"
        render_status = audio_edit.get("render_status")
        audio_revision_hash = audio_edit.get("audio_revision_hash")
        rendered_audio_sha256 = audio_edit.get("rendered_audio_sha256")
        if render_status != "ready":
            reasons.append("rendered_audio_not_ready")
        if not isinstance(audio_revision_hash, str) or not audio_revision_hash:
            reasons.append("audio_revision_hash_missing")
        if not isinstance(rendered_audio_sha256, str) or not rendered_audio_sha256:
            reasons.append("rendered_audio_sha256_missing")
        if not reasons:
            audio_path = render_cache_path(run_root, clip_id, audio_revision_hash)
            if not audio_path.is_file():
                reasons.append("rendered_audio_missing")
            elif sha256_file(audio_path) != rendered_audio_sha256:
                reasons.append("rendered_audio_hash_mismatch")
            else:
                audio_sha256 = rendered_audio_sha256
    else:
        audio_kind = "candidate_original"
        audio_path = _resolve_manifest_audio_path(run_root, manifest_row, clip_id=clip_id)
        if not audio_path.is_file():
            reasons.append("candidate_audio_missing")
        elif sha256_file(audio_path) != source_audio_sha256:
            reasons.append("candidate_audio_hash_mismatch")
        else:
            audio_sha256 = source_audio_sha256

    if reasons or audio_path is None or audio_sha256 is None:
        return None, reasons, audio_kind, 0.0

    audio_meta = _read_wave_metadata(audio_path)
    audio_payload: dict[str, Any] = {
        "path": _relative_path(export_dir, audio_path),
        "kind": audio_kind,
        "sha256": audio_sha256,
        "source_audio_sha256": source_audio_sha256,
        "sample_rate_hz": audio_meta["sample_rate_hz"],
        "channels": audio_meta["channels"],
        "duration_sec": audio_meta["duration_sec"],
    }
    if audio_kind == "rendered_revision" and audio_revision_hash:
        audio_payload["audio_revision_hash"] = audio_revision_hash

    row: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "clip_id": clip_id,
        "transcript": transcript,
        "lineage": {
            "source_clip_id": clip_id,
            "parent_clip_ids": [],
        },
        "audio": audio_payload,
        "review": {
            "status": "accepted",
            "reviewer_tags": list(clip_view.get("reviewer_tags") or []),
        },
    }
    quality = _quality_payload(clip_view)
    if quality:
        row["quality"] = quality
    return row, [], audio_kind, float(audio_meta["duration_sec"])


def _collect_export_material(
    repository: Any,
    run_id: str,
) -> tuple[ProcessingRun, Path, list[tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]], CanonicalExportPreviewView]:
    with Session(repository.engine) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        run_root = _run_root(repository, run)

    with clip_lab_run_lock(run_root):
        view = build_clip_lab_view(run_root, run_id=run_id)
        if view.get("stale_state"):
            raise CanonicalExportConflictError(
                f"Clip Lab state is stale: {view.get('stale_reason') or 'candidate manifest changed'}"
            )
        if view.get("invalid_state"):
            raise CanonicalExportConflictError(
                f"Clip Lab state is invalid: {view.get('invalid_state_reason') or 'validation failed'}"
            )

        manifest = load_candidate_manifest(run_root)
        manifest_by_id = index_manifest_by_clip_id(manifest)
        saved_state = load_clip_lab_state(run_root)
        stored_clips = saved_state.get("clips") if isinstance(saved_state, dict) else None
        accepted_clip_views = [dict(clip) for clip in view.get("clips") or [] if clip.get("review_status") == "accepted"]
        accepted_manifest_rows: dict[str, dict[str, Any]] = {}
        accepted_clip_entries: dict[str, dict[str, Any] | None] = {}
        for clip_view in accepted_clip_views:
            clip_id = str(clip_view["clip_id"])
            manifest_row = manifest_by_id.get(clip_id)
            if manifest_row is not None:
                accepted_manifest_rows[clip_id] = dict(manifest_row)
            if isinstance(stored_clips, dict) and isinstance(stored_clips.get(clip_id), dict):
                accepted_clip_entries[clip_id] = dict(stored_clips[clip_id])
            else:
                accepted_clip_entries[clip_id] = None

    export_inputs: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]] = []
    blocked_reasons: list[CanonicalExportBlockedReasonView] = []
    total_duration_sec = 0.0
    original_audio_count = 0
    edited_audio_count = 0
    preview_export_dir = _canonical_exports_root(run_root) / "preview"
    for clip_view in accepted_clip_views:
        clip_id = str(clip_view["clip_id"])
        manifest_row = accepted_manifest_rows.get(clip_id)
        if manifest_row is None:
            blocked_reasons.append(
                CanonicalExportBlockedReasonView(clip_id=clip_id, reasons=["manifest_row_missing"])
            )
            continue
        row, reasons, audio_kind, duration_sec = _build_clip_export_row(
            export_dir=preview_export_dir,
            run_root=run_root,
            manifest_row=manifest_row,
            clip_view=clip_view,
            clip_entry=accepted_clip_entries.get(clip_id),
        )
        if reasons:
            blocked_reasons.append(CanonicalExportBlockedReasonView(clip_id=clip_id, reasons=reasons))
            continue
        assert row is not None
        export_inputs.append((manifest_row, clip_view, accepted_clip_entries.get(clip_id)))
        total_duration_sec += duration_sec
        if audio_kind == "rendered_revision":
            edited_audio_count += 1
        else:
            original_audio_count += 1

    preview = CanonicalExportPreviewView(
        run_id=run_id,
        accepted_clip_count=len(accepted_clip_views),
        total_duration_sec=round(total_duration_sec, 6),
        original_audio_count=original_audio_count,
        edited_audio_count=edited_audio_count,
        blocked_clip_count=len(blocked_reasons),
        blocked_reasons=blocked_reasons,
    )
    return run, run_root, export_inputs, preview


def preview_canonical_export(repository: Any, run_id: str) -> CanonicalExportPreviewView:
    _run, _run_root_path, _rows, preview = _collect_export_material(repository, run_id)
    return preview


def create_canonical_export(repository: Any, run_id: str) -> CanonicalExportSummaryView:
    run, run_root, export_inputs, preview = _collect_export_material(repository, run_id)
    if preview.accepted_clip_count == 0:
        raise CanonicalExportConflictError("no accepted clips available for canonical export")
    if preview.blocked_clip_count > 0:
        raise CanonicalExportConflictError("accepted clips are blocked; resolve Clip Lab export blockers first")

    exports_root = _canonical_exports_root(run_root)
    exports_root.mkdir(parents=True, exist_ok=True)
    export_id = _next_export_id(exports_root)
    tmp_dir = exports_root / f".tmp_{export_id}_{uuid4().hex}"
    final_dir = exports_root / export_id
    if final_dir.exists():
        raise CanonicalExportConflictError("canonical export snapshot already exists; retry")
    tmp_dir.mkdir(parents=True, exist_ok=False)

    try:
        export_rows: list[dict[str, Any]] = []
        for manifest_row, clip_view, clip_entry in export_inputs:
            row, reasons, _audio_kind, _duration_sec = _build_clip_export_row(
                export_dir=final_dir,
                run_root=run_root,
                manifest_row=manifest_row,
                clip_view=clip_view,
                clip_entry=clip_entry,
            )
            if reasons or row is None:
                raise CanonicalExportConflictError("canonical export inputs changed while exporting; retry")
            export_rows.append(row)

        manifest_path = tmp_dir / MANIFEST_FILENAME
        with manifest_path.open("w", encoding="utf-8") as handle:
            for row in export_rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
                handle.write("\n")

        metadata = {
            "schema_version": SCHEMA_VERSION,
            "export_id": export_id,
            "run_id": run.id,
            "project_id": run.project_id,
            "created_at": _utc_now_iso(),
            "path_mode": "snapshot_relative",
            "audio_storage_mode": "project_artifact_reference",
            "portable": False,
            "dataset_manifest": MANIFEST_FILENAME,
            "accepted_clip_count": preview.accepted_clip_count,
            "total_duration_sec": preview.total_duration_sec,
            "original_audio_count": preview.original_audio_count,
            "edited_audio_count": preview.edited_audio_count,
            "blocked_clip_count": preview.blocked_clip_count,
        }
        _serialize_json(tmp_dir / METADATA_FILENAME, metadata)
        _serialize_json(
            tmp_dir / REPORT_FILENAME,
            {
                "run_id": run.id,
                "export_id": export_id,
                "accepted_clip_count": preview.accepted_clip_count,
                "blocked_clip_count": preview.blocked_clip_count,
                "blocked_reasons": [item.model_dump() for item in preview.blocked_reasons],
            },
        )

        tmp_dir.rename(final_dir)
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise
    return CanonicalExportSummaryView(
        export_id=export_id,
        run_id=run.id,
        project_id=run.project_id,
        created_at=str(metadata["created_at"]),
        accepted_clip_count=preview.accepted_clip_count,
        total_duration_sec=preview.total_duration_sec,
        snapshot_dir=str(final_dir),
        manifest_path=str(final_dir / MANIFEST_FILENAME),
    )


def _summary_from_snapshot(run_id: str, snapshot_dir: Path) -> CanonicalExportSummaryView | None:
    metadata_path = snapshot_dir / METADATA_FILENAME
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    export_id = payload.get("export_id")
    project_id = payload.get("project_id")
    created_at = payload.get("created_at")
    accepted_clip_count = payload.get("accepted_clip_count")
    total_duration_sec = payload.get("total_duration_sec")
    if not isinstance(export_id, str) or not isinstance(project_id, str) or not isinstance(created_at, str):
        return None
    if not isinstance(accepted_clip_count, int) or not isinstance(total_duration_sec, (int, float)):
        return None
    return CanonicalExportSummaryView(
        export_id=export_id,
        run_id=run_id,
        project_id=project_id,
        created_at=created_at,
        accepted_clip_count=accepted_clip_count,
        total_duration_sec=round(float(total_duration_sec), 6),
        snapshot_dir=str(snapshot_dir),
        manifest_path=str(snapshot_dir / MANIFEST_FILENAME),
    )


def list_canonical_exports(repository: Any, run_id: str) -> list[CanonicalExportSummaryView]:
    with Session(repository.engine) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")

    run_root = _run_root(repository, run)
    exports_root = _canonical_exports_root(run_root)
    if not exports_root.exists():
        return []
    summaries: list[CanonicalExportSummaryView] = []
    for child in exports_root.iterdir():
        if not child.is_dir() or child.name.startswith(".tmp_"):
            continue
        summary = _summary_from_snapshot(run_id, child)
        if summary is not None:
            summaries.append(summary)
    return sorted(summaries, key=lambda item: item.created_at, reverse=True)
