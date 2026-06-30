"""Dataset Clip Lab audio operation routes (P2)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .clip_lab_audio import (
    ClipLabAudioValidationError,
    build_peaks_payload,
    compute_audio_revision_hash as compute_source_audio_revision_hash,
    load_pcm16_mono_wav,
    load_pcm16_mono_wav_bytes,
    peaks_cache_path,
    render_cache_path,
    render_or_reuse_audio_revision_from_bytes,
    sha256_file,
    validate_audio_op,
    verify_source_wav_bytes,
)
from .clip_lab_state import (
    ClipLabValidationError,
    ClipNotFoundError,
    StaleClipError,
    StaleManifestError,
    _build_clip_view,
    _clear_acceptance_fields,
    _default_clip_entry,
    _ensure_state_document,
    _load_optional_qc_indexes,
    _normalize_stale_acceptance,
    _stored_clip_entry,
    _utc_now_iso,
    candidate_manifest_path,
    clip_lab_run_lock,
    compute_manifest_sha256,
    index_manifest_by_clip_id,
    load_candidate_manifest,
    load_clip_lab_state,
    resolve_manifest_source_audio_hash,
    save_clip_lab_state,
)


class ClipLabUnrenderedAudioError(ClipLabValidationError):
    """Clip has audio edits that are not ready for acceptance."""


class ClipLabRenderError(Exception):
    """Synchronous audio render failed after state was marked pending."""


class ClipLabRevisionNotFoundError(ClipLabValidationError):
    """Requested audio revision key does not match the clip's current state."""


class ClipLabPeaksCacheMissingError(ClipLabValidationError):
    """Rendered peaks cache is missing for a ready audio revision."""


@dataclass(frozen=True)
class _RenderJob:
    run_root: Path
    clip_id: str
    source_wav_bytes: bytes
    source_audio_sha256: str
    expected_manifest_sha256: str
    sample_rate: int
    ops: list[dict[str, Any]]
    revision_hash: str


def _manifest_wav_path(run_root: Path, manifest_row: dict[str, Any]) -> Path:
    return run_root / str(manifest_row["audio_path"])


def _source_identity(manifest_row: dict[str, Any], *, clip_id: str) -> str:
    source_sha = resolve_manifest_source_audio_hash(manifest_row, clip_id=clip_id)
    if source_sha is None:
        raise ClipLabValidationError(f"{clip_id} is missing source audio identity")
    return source_sha


def _sample_rate_hz(manifest_row: dict[str, Any]) -> int:
    sample_rate = manifest_row.get("sample_rate")
    if isinstance(sample_rate, bool) or not isinstance(sample_rate, int) or sample_rate <= 0:
        raise ClipLabValidationError("candidate manifest row is missing sample_rate")
    return sample_rate


def _original_duration_sec(manifest_row: dict[str, Any], *, sample_rate_hz: int | None) -> float:
    duration_samples = manifest_row.get("duration_samples")
    if isinstance(duration_samples, int) and duration_samples >= 0 and sample_rate_hz:
        return round(duration_samples / sample_rate_hz, 6)
    return float(manifest_row.get("duration_sec") or 0.0)


def _audio_edit_block(clip_entry: dict[str, Any]) -> dict[str, Any] | None:
    audio_edit = clip_entry.get("audio_edit")
    if audio_edit is None:
        return None
    if not isinstance(audio_edit, dict):
        raise ClipLabValidationError("audio_edit must be an object")
    return audio_edit


def _audio_edit_is_active(audio_edit: dict[str, Any]) -> bool:
    ops = audio_edit.get("ops")
    redo_ops = audio_edit.get("redo_ops")
    return bool(ops) or bool(redo_ops)


def _ensure_audio_edit(
    clip_entry: dict[str, Any],
    *,
    manifest_row: dict[str, Any],
    clip_id: str,
) -> dict[str, Any]:
    audio_edit = _audio_edit_block(clip_entry)
    source_sha = _source_identity(manifest_row, clip_id=clip_id)
    sample_rate = _sample_rate_hz(manifest_row)
    if audio_edit is None:
        audio_edit = {
            "schema_version": 1,
            "source_audio_sha256": source_sha,
            "source_sample_rate_hz": sample_rate,
            "ops": [],
            "redo_ops": [],
            "audio_revision_hash": None,
            "rendered_audio_sha256": None,
            "render_status": "ready",
        }
        clip_entry["audio_edit"] = audio_edit
    return audio_edit


def _maybe_remove_audio_edit(clip_entry: dict[str, Any]) -> None:
    audio_edit = _audio_edit_block(clip_entry)
    if audio_edit is None:
        return
    if not _audio_edit_is_active(audio_edit):
        clip_entry.pop("audio_edit", None)


def _recompute_audio_edit_hash(
    audio_edit: dict[str, Any],
    *,
    source_sample_count: int,
    sample_rate: int,
) -> None:
    ops = list(audio_edit.get("ops") or [])
    source_sha = str(audio_edit["source_audio_sha256"])
    if not ops:
        audio_edit["audio_revision_hash"] = None
        return
    audio_edit["audio_revision_hash"] = compute_source_audio_revision_hash(
        source_sha,
        ops,
        source_sample_count=source_sample_count,
        sample_rate=sample_rate,
    )


def _capture_source_wav_bytes(run_root: Path, manifest_row: dict[str, Any], *, source_sha: str) -> bytes:
    source_wav_path = _manifest_wav_path(run_root, manifest_row)
    source_wav_bytes = source_wav_path.read_bytes()
    verify_source_wav_bytes(source_wav_bytes, source_sha)
    return source_wav_bytes


def _audio_render_guard_matches(
    audio_edit: dict[str, Any],
    *,
    expected_revision_hash: str,
    expected_source_hash: str,
    expected_manifest_sha256: str,
    current_manifest_sha256: str,
) -> bool:
    return (
        audio_edit.get("audio_revision_hash") == expected_revision_hash
        and audio_edit.get("source_audio_sha256") == expected_source_hash
        and current_manifest_sha256 == expected_manifest_sha256
    )


def effective_revision_key(manifest_row: dict[str, Any], clip_entry: dict[str, Any], *, clip_id: str) -> str:
    audio_edit = _audio_edit_block(clip_entry)
    source_sha = _source_identity(manifest_row, clip_id=clip_id)
    if audio_edit is None:
        return source_sha
    ops = audio_edit.get("ops") or []
    if not ops:
        return source_sha
    revision_hash = audio_edit.get("audio_revision_hash")
    render_status = audio_edit.get("render_status")
    if render_status == "ready" and isinstance(revision_hash, str):
        return revision_hash
    return source_sha


def _edited_duration_sec(
    manifest_row: dict[str, Any],
    ops: list[dict[str, Any]],
    *,
    sample_rate_hz: int | None,
) -> float | None:
    if not sample_rate_hz or not ops:
        return None
    try:
        duration_samples = int(manifest_row.get("duration_samples") or 0)
        from .clip_lab_audio import _timeline_length_after_op, validate_audio_ops_recipe

        validate_audio_ops_recipe(
            ops,
            source_sample_count=duration_samples,
            sample_rate=sample_rate_hz,
        )
        current = duration_samples
        for op in ops:
            current = _timeline_length_after_op(current, op)
        return round(current / sample_rate_hz, 6)
    except ClipLabAudioValidationError:
        return None


def audio_view_fields(
    *,
    run_id: str,
    manifest_row: dict[str, Any],
    clip_entry: dict[str, Any],
) -> dict[str, Any]:
    clip_id = str(manifest_row.get("id") or manifest_row.get("clip_id"))
    source_sha = resolve_manifest_source_audio_hash(manifest_row, clip_id=clip_id)
    sample_rate = manifest_row.get("sample_rate")
    sample_rate_hz = int(sample_rate) if isinstance(sample_rate, int) and sample_rate > 0 else None
    audio_edit = _audio_edit_block(clip_entry)
    ops: list[dict[str, Any]] = []
    redo_ops: list[dict[str, Any]] = []
    audio_revision_hash: str | None = None
    rendered_audio_sha256: str | None = None
    render_status = "ready"
    original_duration_sec = _original_duration_sec(manifest_row, sample_rate_hz=sample_rate_hz)

    if audio_edit is not None:
        ops = list(audio_edit.get("ops") or [])
        redo_ops = list(audio_edit.get("redo_ops") or [])
        revision = audio_edit.get("audio_revision_hash")
        audio_revision_hash = revision if isinstance(revision, str) else None
        rendered = audio_edit.get("rendered_audio_sha256")
        rendered_audio_sha256 = rendered if isinstance(rendered, str) else None
        status = audio_edit.get("render_status")
        render_status = status if isinstance(status, str) else "ready"

    if source_sha is None:
        return {
            "sample_rate_hz": sample_rate_hz,
            "source_audio_sha256": None,
            "effective_audio_kind": "candidate_original",
            "effective_audio_revision_key": None,
            "audio_revision_hash": None,
            "rendered_audio_sha256": None,
            "audio_url": None,
            "waveform_peaks_url": None,
            "current_duration_sec": original_duration_sec,
            "audio_edit_op_count": 0,
            "audio_edit_ops": [],
            "can_undo_audio": False,
            "can_redo_audio": False,
            "render_status": render_status,
        }

    revision_key = effective_revision_key(manifest_row, clip_entry, clip_id=clip_id)
    kind = "rendered_revision" if revision_key != source_sha else "candidate_original"
    edited_duration_sec = _edited_duration_sec(manifest_row, ops, sample_rate_hz=sample_rate_hz)
    playback_ready = kind == "rendered_revision"
    current_duration_sec = edited_duration_sec if playback_ready and edited_duration_sec is not None else original_duration_sec
    return {
        "sample_rate_hz": sample_rate_hz,
        "source_audio_sha256": source_sha,
        "effective_audio_kind": kind,
        "effective_audio_revision_key": revision_key,
        "audio_revision_hash": audio_revision_hash,
        "rendered_audio_sha256": rendered_audio_sha256,
        "audio_url": f"/media/dataset-runs/{run_id}/clip-lab/{clip_id}/audio/{revision_key}.wav",
        "waveform_peaks_url": f"/api/dataset-runs/{run_id}/clips/{clip_id}/waveform-peaks/{revision_key}",
        "current_duration_sec": current_duration_sec,
        "audio_edit_op_count": len(ops),
        "audio_edit_ops": ops,
        "can_undo_audio": bool(ops),
        "can_redo_audio": bool(redo_ops),
        "render_status": render_status,
    }


def _perform_sync_render(job: _RenderJob) -> str:
    cache_path = render_cache_path(job.run_root, job.clip_id, job.revision_hash)
    peaks_path = peaks_cache_path(job.run_root, job.revision_hash)
    _, _, rendered_sha = render_or_reuse_audio_revision_from_bytes(
        source_wav_bytes=job.source_wav_bytes,
        ops=job.ops,
        cache_path=cache_path,
        peaks_path=peaks_path,
        revision_key=job.revision_hash,
        source_audio_sha256=job.source_audio_sha256,
    )
    return rendered_sha


def _finalize_render(run_root: Path, clip_id: str, *, job: _RenderJob, rendered_audio_sha256: str) -> bool:
    with clip_lab_run_lock(run_root):
        current_manifest_sha = compute_manifest_sha256(candidate_manifest_path(run_root))
        manifest = load_candidate_manifest(run_root)
        manifest_by_id = index_manifest_by_clip_id(manifest)
        if clip_id not in manifest_by_id:
            return False
        saved_state = load_clip_lab_state(run_root)
        if saved_state is None:
            return False
        clip_entry = _stored_clip_entry(saved_state, clip_id)
        audio_edit = _audio_edit_block(clip_entry)
        if audio_edit is None:
            return False
        if not _audio_render_guard_matches(
            audio_edit,
            expected_revision_hash=job.revision_hash,
            expected_source_hash=job.source_audio_sha256,
            expected_manifest_sha256=job.expected_manifest_sha256,
            current_manifest_sha256=current_manifest_sha,
        ):
            return False
        audio_edit["rendered_audio_sha256"] = rendered_audio_sha256
        audio_edit["render_status"] = "ready"
        clip_entry["updated_at"] = _utc_now_iso()
        saved_state["updated_at"] = _utc_now_iso()
        clips = saved_state.get("clips")
        if isinstance(clips, dict):
            clips[clip_id] = clip_entry
        save_clip_lab_state(run_root, saved_state)
        return True


def _require_finalize_render(
    run_root: Path,
    clip_id: str,
    *,
    job: _RenderJob,
    rendered_audio_sha256: str,
) -> None:
    if _finalize_render(run_root, clip_id, job=job, rendered_audio_sha256=rendered_audio_sha256):
        return
    raise StaleManifestError(
        "candidate generation or audio revision changed while rendering; reload Clip Lab"
    )


def _mark_render_failed_if_current(run_root: Path, clip_id: str, *, job: _RenderJob) -> None:
    with clip_lab_run_lock(run_root):
        current_manifest_sha = compute_manifest_sha256(candidate_manifest_path(run_root))
        saved_state = load_clip_lab_state(run_root)
        if saved_state is None:
            return
        clip_entry = _stored_clip_entry(saved_state, clip_id)
        audio_edit = _audio_edit_block(clip_entry)
        if audio_edit is None:
            return
        if not _audio_render_guard_matches(
            audio_edit,
            expected_revision_hash=job.revision_hash,
            expected_source_hash=job.source_audio_sha256,
            expected_manifest_sha256=job.expected_manifest_sha256,
            current_manifest_sha256=current_manifest_sha,
        ):
            return
        audio_edit["render_status"] = "failed"
        save_clip_lab_state(run_root, saved_state)


def _clip_view_after_mutation(
    run_root: Path,
    *,
    run_id: str,
    clip_id: str,
    manifest_row: dict[str, Any],
    clip_entry: dict[str, Any],
) -> dict[str, Any]:
    qc_state = _load_optional_qc_indexes(run_root)
    view = _build_clip_view(
        manifest_row=manifest_row,
        clip_entry=clip_entry,
        transcript_by_id=qc_state.transcript_by_id,
        speaker_by_id=qc_state.speaker_by_id,
    )
    view.update(audio_view_fields(run_id=run_id, manifest_row=manifest_row, clip_entry=clip_entry))
    view["clip_id"] = clip_id
    return view


def _build_render_job(
    *,
    run_root: Path,
    clip_id: str,
    manifest_row: dict[str, Any],
    audio_edit: dict[str, Any],
    current_manifest_sha: str,
    source_wav_bytes: bytes,
    sample_rate: int,
    ops: list[dict[str, Any]],
    revision_hash: str,
) -> _RenderJob:
    return _RenderJob(
        run_root=run_root,
        clip_id=clip_id,
        source_wav_bytes=source_wav_bytes,
        source_audio_sha256=str(audio_edit["source_audio_sha256"]),
        expected_manifest_sha256=current_manifest_sha,
        sample_rate=sample_rate,
        ops=list(ops),
        revision_hash=revision_hash,
    )


def append_clip_audio_operation(
    run_root: Path,
    *,
    run_id: str,
    clip_id: str,
    expected_manifest_sha256: str,
    expected_clip_version: int,
    operation: dict[str, Any],
) -> dict[str, Any]:
    with clip_lab_run_lock(run_root):
        manifest = load_candidate_manifest(run_root)
        manifest_by_id = index_manifest_by_clip_id(manifest)
        if clip_id not in manifest_by_id:
            raise ClipNotFoundError(f"unknown clip_id: {clip_id}")
        current_manifest_sha = compute_manifest_sha256(candidate_manifest_path(run_root))
        if expected_manifest_sha256 != current_manifest_sha:
            raise StaleManifestError("expected_manifest_sha256 does not match current manifest")
        manifest_row = manifest_by_id[clip_id]
        saved_state = load_clip_lab_state(run_root)
        state = _ensure_state_document(saved_state=saved_state, current_manifest_sha=current_manifest_sha)
        clips = state["clips"]
        assert isinstance(clips, dict)
        clip_entry = clips.get(clip_id)
        if not isinstance(clip_entry, dict):
            clip_entry = _default_clip_entry()
            clips[clip_id] = clip_entry
        stored_version = int(clip_entry.get("clip_version") or 0)
        if expected_clip_version != stored_version:
            raise StaleClipError(
                f"expected_clip_version {expected_clip_version} does not match stored version {stored_version}"
            )

        _normalize_stale_acceptance(clip_entry, manifest_row=manifest_row)
        if clip_entry.get("review_status") == "accepted":
            clip_entry["review_status"] = "unresolved"
            _clear_acceptance_fields(clip_entry)

        audio_edit = _ensure_audio_edit(clip_entry, manifest_row=manifest_row, clip_id=clip_id)
        source_sha = str(audio_edit["source_audio_sha256"])
        source_wav_bytes = _capture_source_wav_bytes(run_root, manifest_row, source_sha=source_sha)
        samples, _ = load_pcm16_mono_wav_bytes(source_wav_bytes)
        sample_rate = _sample_rate_hz(manifest_row)
        try:
            validate_audio_op(operation, len(samples), index=len(audio_edit.get("ops") or []), sample_rate=sample_rate)
        except ClipLabAudioValidationError as exc:
            raise ClipLabValidationError(str(exc)) from exc

        ops = list(audio_edit.get("ops") or [])
        ops.append(operation)
        audio_edit["ops"] = ops
        audio_edit["redo_ops"] = []
        _recompute_audio_edit_hash(audio_edit, source_sample_count=len(samples), sample_rate=sample_rate)
        revision_hash = audio_edit.get("audio_revision_hash")
        if not isinstance(revision_hash, str):
            raise ClipLabValidationError("audio operation did not produce a revision hash")

        audio_edit["rendered_audio_sha256"] = None
        audio_edit["render_status"] = "pending"
        clip_entry["clip_version"] = stored_version + 1
        clip_entry["updated_at"] = _utc_now_iso()
        state["updated_at"] = _utc_now_iso()
        save_clip_lab_state(run_root, state)

        render_job = _build_render_job(
            run_root=run_root,
            clip_id=clip_id,
            manifest_row=manifest_row,
            audio_edit=audio_edit,
            current_manifest_sha=current_manifest_sha,
            source_wav_bytes=source_wav_bytes,
            sample_rate=sample_rate,
            ops=ops,
            revision_hash=revision_hash,
        )

    try:
        rendered_sha = _perform_sync_render(render_job)
    except Exception as exc:
        _mark_render_failed_if_current(run_root, clip_id, job=render_job)
        raise ClipLabRenderError("audio render failed") from exc

    _require_finalize_render(run_root, clip_id, job=render_job, rendered_audio_sha256=rendered_sha)

    with clip_lab_run_lock(run_root):
        manifest = load_candidate_manifest(run_root)
        manifest_by_id = index_manifest_by_clip_id(manifest)
        if clip_id not in manifest_by_id:
            raise ClipNotFoundError(f"unknown clip_id: {clip_id}")
        manifest_row = manifest_by_id[clip_id]
        saved_state = load_clip_lab_state(run_root)
        clip_entry = _stored_clip_entry(saved_state or {}, clip_id)
        return _clip_view_after_mutation(run_root, run_id=run_id, clip_id=clip_id, manifest_row=manifest_row, clip_entry=clip_entry)


def _mutate_audio_edit_stack(
    run_root: Path,
    *,
    run_id: str,
    clip_id: str,
    expected_manifest_sha256: str,
    expected_clip_version: int,
    direction: str,
) -> dict[str, Any]:
    render_job: _RenderJob | None = None
    with clip_lab_run_lock(run_root):
        manifest = load_candidate_manifest(run_root)
        manifest_by_id = index_manifest_by_clip_id(manifest)
        if clip_id not in manifest_by_id:
            raise ClipNotFoundError(f"unknown clip_id: {clip_id}")
        current_manifest_sha = compute_manifest_sha256(candidate_manifest_path(run_root))
        if expected_manifest_sha256 != current_manifest_sha:
            raise StaleManifestError("expected_manifest_sha256 does not match current manifest")
        manifest_row = manifest_by_id[clip_id]
        saved_state = load_clip_lab_state(run_root)
        state = _ensure_state_document(saved_state=saved_state, current_manifest_sha=current_manifest_sha)
        clips = state["clips"]
        assert isinstance(clips, dict)
        clip_entry = clips.get(clip_id)
        if not isinstance(clip_entry, dict):
            raise ClipLabValidationError("clip has no audio edits to mutate")
        stored_version = int(clip_entry.get("clip_version") or 0)
        if expected_clip_version != stored_version:
            raise StaleClipError(
                f"expected_clip_version {expected_clip_version} does not match stored version {stored_version}"
            )
        audio_edit = _audio_edit_block(clip_entry)
        if audio_edit is None:
            raise ClipLabValidationError("clip has no audio edits to mutate")

        _normalize_stale_acceptance(clip_entry, manifest_row=manifest_row)
        if clip_entry.get("review_status") == "accepted":
            clip_entry["review_status"] = "unresolved"
            _clear_acceptance_fields(clip_entry)

        ops = list(audio_edit.get("ops") or [])
        redo_ops = list(audio_edit.get("redo_ops") or [])
        source_sha = str(audio_edit["source_audio_sha256"])
        source_wav_bytes = _capture_source_wav_bytes(run_root, manifest_row, source_sha=source_sha)
        samples, _ = load_pcm16_mono_wav_bytes(source_wav_bytes)
        sample_rate = _sample_rate_hz(manifest_row)

        if direction == "undo":
            if not ops:
                raise ClipLabValidationError("no audio operations to undo")
            redo_ops.append(ops.pop())
        elif direction == "redo":
            if not redo_ops:
                raise ClipLabValidationError("no audio operations to redo")
            ops.append(redo_ops.pop())
        else:
            raise ClipLabValidationError(f"unsupported audio edit direction: {direction}")

        audio_edit["ops"] = ops
        audio_edit["redo_ops"] = redo_ops
        _recompute_audio_edit_hash(audio_edit, source_sample_count=len(samples), sample_rate=sample_rate)
        _maybe_remove_audio_edit(clip_entry)

        clip_entry["clip_version"] = stored_version + 1
        clip_entry["updated_at"] = _utc_now_iso()
        state["updated_at"] = _utc_now_iso()

        audio_edit = _audio_edit_block(clip_entry)
        if audio_edit is not None and (audio_edit.get("ops") or []):
            revision_hash = audio_edit.get("audio_revision_hash")
            if not isinstance(revision_hash, str):
                raise ClipLabValidationError("audio edit is missing revision hash")
            audio_edit["rendered_audio_sha256"] = None
            audio_edit["render_status"] = "pending"
            render_job = _build_render_job(
                run_root=run_root,
                clip_id=clip_id,
                manifest_row=manifest_row,
                audio_edit=audio_edit,
                current_manifest_sha=current_manifest_sha,
                source_wav_bytes=source_wav_bytes,
                sample_rate=sample_rate,
                ops=list(audio_edit.get("ops") or []),
                revision_hash=revision_hash,
            )
        elif audio_edit is not None:
            audio_edit["rendered_audio_sha256"] = None
            audio_edit["render_status"] = "ready"

        save_clip_lab_state(run_root, state)

    if render_job is not None:
        try:
            rendered_sha = _perform_sync_render(render_job)
        except Exception as exc:
            _mark_render_failed_if_current(run_root, clip_id, job=render_job)
            raise ClipLabRenderError("audio render failed") from exc
        _require_finalize_render(run_root, clip_id, job=render_job, rendered_audio_sha256=rendered_sha)

    with clip_lab_run_lock(run_root):
        manifest = load_candidate_manifest(run_root)
        manifest_by_id = index_manifest_by_clip_id(manifest)
        if clip_id not in manifest_by_id:
            raise ClipNotFoundError(f"unknown clip_id: {clip_id}")
        manifest_row = manifest_by_id[clip_id]
        saved_state = load_clip_lab_state(run_root)
        clip_entry = _stored_clip_entry(saved_state or {}, clip_id)
        return _clip_view_after_mutation(run_root, run_id=run_id, clip_id=clip_id, manifest_row=manifest_row, clip_entry=clip_entry)


def undo_clip_audio_operation(
    run_root: Path,
    *,
    run_id: str,
    clip_id: str,
    expected_manifest_sha256: str,
    expected_clip_version: int,
) -> dict[str, Any]:
    return _mutate_audio_edit_stack(
        run_root,
        run_id=run_id,
        clip_id=clip_id,
        expected_manifest_sha256=expected_manifest_sha256,
        expected_clip_version=expected_clip_version,
        direction="undo",
    )


def redo_clip_audio_operation(
    run_root: Path,
    *,
    run_id: str,
    clip_id: str,
    expected_manifest_sha256: str,
    expected_clip_version: int,
) -> dict[str, Any]:
    return _mutate_audio_edit_stack(
        run_root,
        run_id=run_id,
        clip_id=clip_id,
        expected_manifest_sha256=expected_manifest_sha256,
        expected_clip_version=expected_clip_version,
        direction="redo",
    )


def assert_clip_audio_acceptable(
    clip_entry: dict[str, Any],
    *,
    manifest_row: dict[str, Any],
    run_root: Path,
    clip_id: str,
) -> None:
    audio_edit = _audio_edit_block(clip_entry)
    if audio_edit is None:
        return
    ops = audio_edit.get("ops") or []
    if not ops:
        return
    render_status = audio_edit.get("render_status")
    if render_status == "failed":
        raise ClipLabUnrenderedAudioError(
            "cannot accept clip: audio render failed; undo the edit or retry rendering"
        )
    if render_status != "ready":
        raise ClipLabUnrenderedAudioError("cannot accept clip with unrendered audio edit")
    revision_hash = audio_edit.get("audio_revision_hash")
    rendered_sha = audio_edit.get("rendered_audio_sha256")
    if not isinstance(revision_hash, str) or not isinstance(rendered_sha, str):
        raise ClipLabUnrenderedAudioError("cannot accept clip without a rendered audio revision")
    cache_path = render_cache_path(run_root, clip_id, revision_hash)
    if not cache_path.is_file():
        raise ClipLabUnrenderedAudioError("cannot accept clip: rendered audio cache is missing")
    if sha256_file(cache_path) != rendered_sha:
        raise ClipLabUnrenderedAudioError("cannot accept clip: rendered audio cache does not match stored hash")


def resolve_revision_media_bytes(
    run_root: Path,
    *,
    clip_id: str,
    revision_key: str,
    manifest_row: dict[str, Any],
    clip_entry: dict[str, Any],
) -> bytes:
    expected_key = effective_revision_key(manifest_row, clip_entry, clip_id=clip_id)
    if revision_key != expected_key:
        raise ClipLabRevisionNotFoundError(f"revision key {revision_key!r} is not current for clip {clip_id}")
    source_sha = _source_identity(manifest_row, clip_id=clip_id)
    if revision_key == source_sha:
        return _manifest_wav_path(run_root, manifest_row).read_bytes()
    cache_path = render_cache_path(run_root, clip_id, revision_key)
    if not cache_path.is_file():
        raise ClipLabRevisionNotFoundError(f"rendered audio is not available for revision {revision_key}")
    return cache_path.read_bytes()


def load_source_peaks_payload_from_bytes(
    *,
    run_root: Path,
    source_wav_bytes: bytes,
    revision_key: str,
    expected_source_sha256: str,
) -> dict[str, Any]:
    verify_source_wav_bytes(source_wav_bytes, expected_source_sha256)
    peaks_path = peaks_cache_path(run_root, revision_key)
    if peaks_path.is_file():
        payload = json.loads(peaks_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    samples, sample_rate = load_pcm16_mono_wav_bytes(source_wav_bytes)
    from .clip_lab_audio import atomic_publish_peaks_payload

    payload = build_peaks_payload(
        revision_key=revision_key,
        samples=samples,
        sample_rate=sample_rate,
    )
    atomic_publish_peaks_payload(peaks_path, payload)
    return payload


def load_revision_peaks_payload(
    run_root: Path,
    *,
    clip_id: str,
    revision_key: str,
    manifest_row: dict[str, Any],
    clip_entry: dict[str, Any],
    source_wav_bytes: bytes | None = None,
) -> dict[str, Any]:
    expected_key = effective_revision_key(manifest_row, clip_entry, clip_id=clip_id)
    if revision_key != expected_key:
        raise ClipLabRevisionNotFoundError(f"revision key {revision_key!r} is not current for clip {clip_id}")
    source_sha = _source_identity(manifest_row, clip_id=clip_id)
    if revision_key == source_sha:
        if source_wav_bytes is None:
            raise ClipLabValidationError("source waveform bytes are required for original-audio peaks")
        return load_source_peaks_payload_from_bytes(
            run_root=run_root,
            source_wav_bytes=source_wav_bytes,
            revision_key=revision_key,
            expected_source_sha256=source_sha,
        )
    peaks_path = peaks_cache_path(run_root, revision_key)
    if peaks_path.is_file():
        payload = json.loads(peaks_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    audio_edit = _audio_edit_block(clip_entry)
    render_status = audio_edit.get("render_status") if audio_edit is not None else "ready"
    if render_status == "ready":
        raise ClipLabPeaksCacheMissingError(
            f"peaks cache is missing for ready rendered revision {revision_key}"
        )
    raise ClipLabRevisionNotFoundError(f"peaks are not available for revision {revision_key}")
