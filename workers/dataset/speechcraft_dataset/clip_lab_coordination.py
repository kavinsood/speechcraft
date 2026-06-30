"""Clip Lab coordination shared with the backend via lock path and artifact layout."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

from filelock import FileLock, Timeout

from .io import sha256_file

LOCK_FILENAME = ".clip_lab_state.lock"
LOCK_TIMEOUT_SEC = 5
CLIP_LAB_STATE_REL = "artifacts/clip_lab_state.json"
CLIP_LAB_STATE_ARCHIVE_REL = "artifacts/clip_lab_state_archive"
CLIP_LAB_RENDERS_REL = "artifacts/clip_lab_renders"
CLIP_LAB_PEAKS_REL = "artifacts/clip_lab_peaks"
CANDIDATE_MANIFEST_REL = "artifacts/candidate_review_manifest.json"
CANDIDATE_REJECTED_REL = "artifacts/candidate_review_rejected.json"
CANDIDATE_SUMMARY_REL = "artifacts/candidate_review_summary.json"
CANDIDATE_CLIPS_REL = "artifacts/candidate_review_clips"
CANDIDATE_STAGE_PARENT_REL = "artifacts/_candidate_review_stage"
CANDIDATE_PROMOTE_BACKUP_REL = "artifacts/_candidate_review_promote_backup"
_SHA256_HEX = re.compile(r"^[a-f0-9]{64}$")
_SHA256_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")


class CandidatePromotionError(RuntimeError):
    """Candidate artifact promotion failed."""


class CandidatePromotionRecoveryRequiredError(CandidatePromotionError):
    """Promotion failed and the backup could not be restored; manual recovery is required."""


def clip_lab_lock_path(run_root: Path) -> Path:
    return run_root / LOCK_FILENAME


def clip_lab_state_path(run_root: Path) -> Path:
    return run_root / CLIP_LAB_STATE_REL


def manifest_sha256(manifest_path: Path) -> str:
    return hashlib.sha256(manifest_path.read_bytes()).hexdigest()


def normalize_digest(value: str) -> str:
    if _SHA256_HEX.fullmatch(value):
        return value
    if _SHA256_DIGEST.fullmatch(value):
        return value[7:]
    raise ValueError(f"invalid sha256 digest: {value!r}")


def file_digest_hex(path: Path) -> str:
    return normalize_digest(sha256_file(path))


def allocate_candidate_stage_root(run_root: Path) -> Path:
    stage_root = run_root / CANDIDATE_STAGE_PARENT_REL / uuid4().hex
    stage_root.mkdir(parents=True, exist_ok=False)
    return stage_root


def cleanup_candidate_stage_root(stage_root: Path) -> None:
    if not stage_root.exists():
        return
    shutil.rmtree(stage_root, ignore_errors=True)
    parent = stage_root.parent
    if parent.name == Path(CANDIDATE_STAGE_PARENT_REL).name and parent.exists():
        try:
            next(parent.iterdir())
        except StopIteration:
            parent.rmdir()


@contextmanager
def clip_lab_run_lock(run_root: Path, *, timeout: float = LOCK_TIMEOUT_SEC) -> Iterator[None]:
    lock = FileLock(str(clip_lab_lock_path(run_root)), timeout=timeout)
    try:
        with lock:
            yield
    except Timeout as exc:
        raise TimeoutError("Clip Lab state is busy; retry shortly.") from exc


def clear_clip_lab_render_caches(run_root: Path) -> None:
    for relative in (CLIP_LAB_RENDERS_REL, CLIP_LAB_PEAKS_REL):
        path = run_root / relative
        if path.exists():
            shutil.rmtree(path)


def archive_clip_lab_state(run_root: Path, *, previous_manifest_sha256: str) -> Path | None:
    state_path = clip_lab_state_path(run_root)
    if not state_path.exists():
        return None
    archive_dir = run_root / CLIP_LAB_STATE_ARCHIVE_REL / previous_manifest_sha256
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_path = archive_dir / f"{stamp}-{uuid4().hex}.json"
    if archive_path.exists():
        raise RuntimeError(f"refusing to overwrite existing clip lab archive: {archive_path}")
    shutil.move(str(state_path), str(archive_path))
    return archive_path


def finalize_candidate_regeneration(
    run_root: Path,
    *,
    previous_manifest_sha256: str | None,
    new_manifest_sha256: str,
) -> None:
    if previous_manifest_sha256 and previous_manifest_sha256 != new_manifest_sha256:
        archive_clip_lab_state(run_root, previous_manifest_sha256=previous_manifest_sha256)
        clear_clip_lab_render_caches(run_root)


def validate_staged_candidate_artifacts(stage_root: Path) -> None:
    manifest_path = stage_root / CANDIDATE_MANIFEST_REL
    if not manifest_path.is_file():
        raise ValueError(f"staged candidate manifest is missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, list):
        raise ValueError("staged candidate manifest must be a JSON list")

    seen_ids: set[str] = set()
    for index, row in enumerate(manifest):
        if not isinstance(row, dict):
            raise ValueError(f"staged candidate manifest row {index} must be an object")
        clip_id = row.get("id")
        audio_path = row.get("audio_path")
        raw_audio_sha256 = row.get("audio_sha256")
        if not isinstance(clip_id, str) or not clip_id.strip():
            raise ValueError(f"staged candidate manifest row {index} is missing clip id")
        if clip_id in seen_ids:
            raise ValueError(f"duplicate staged clip id: {clip_id}")
        seen_ids.add(clip_id)
        if not isinstance(audio_path, str) or not audio_path.strip():
            raise ValueError(f"{clip_id} staged manifest row is missing audio_path")
        if not isinstance(raw_audio_sha256, str):
            raise ValueError(f"{clip_id} staged manifest row has invalid audio_sha256")
        try:
            audio_sha256 = normalize_digest(raw_audio_sha256)
        except ValueError as exc:
            raise ValueError(f"{clip_id} staged manifest row has invalid audio_sha256") from exc
        legacy_hash = row.get("audio_hash")
        if legacy_hash is not None:
            try:
                normalized_legacy = normalize_digest(str(legacy_hash))
            except ValueError as exc:
                raise ValueError(f"{clip_id} staged manifest row has invalid audio_hash") from exc
            if normalized_legacy != audio_sha256:
                raise ValueError(f"{clip_id} staged audio_sha256 and audio_hash must match")
        wav_path = stage_root / audio_path
        if not wav_path.is_file():
            raise ValueError(f"{clip_id} staged WAV is missing: {wav_path}")
        if file_digest_hex(wav_path) != audio_sha256:
            raise ValueError(f"{clip_id} staged audio_sha256 does not match WAV bytes")


def _atomic_copy_file(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_suffix(target.suffix + ".tmp")
    tmp_path.write_bytes(source.read_bytes())
    tmp_path.replace(target)


def _restore_live_candidate_artifacts(artifacts: Path, backup: Path) -> None:
    live_clips = artifacts / "candidate_review_clips"
    backup_clips = backup / "candidate_review_clips"
    if backup_clips.exists():
        if live_clips.exists():
            shutil.rmtree(live_clips)
        shutil.copytree(backup_clips, live_clips)
    elif live_clips.exists():
        shutil.rmtree(live_clips)

    for relative in (
        "candidate_review_manifest.json",
        "candidate_review_rejected.json",
        "candidate_review_summary.json",
    ):
        live_file = artifacts / relative
        backup_file = backup / relative
        if backup_file.exists():
            _atomic_copy_file(backup_file, live_file)
        elif live_file.exists():
            live_file.unlink()


def _verify_live_matches_backup(artifacts: Path, backup: Path) -> None:
    live_clips = artifacts / "candidate_review_clips"
    backup_clips = backup / "candidate_review_clips"
    if backup_clips.exists() != live_clips.exists():
        raise CandidatePromotionRecoveryRequiredError("candidate clips directory was not restored from backup")
    if backup_clips.exists():
        live_files = sorted(path.name for path in live_clips.glob("*.wav"))
        backup_files = sorted(path.name for path in backup_clips.glob("*.wav"))
        if live_files != backup_files:
            raise CandidatePromotionRecoveryRequiredError("candidate clips filenames do not match backup")
        for name in backup_files:
            if (live_clips / name).read_bytes() != (backup_clips / name).read_bytes():
                raise CandidatePromotionRecoveryRequiredError(
                    f"candidate clip {name} bytes do not match backup"
                )
    for relative in (
        "candidate_review_manifest.json",
        "candidate_review_rejected.json",
        "candidate_review_summary.json",
    ):
        backup_file = backup / relative
        live_file = artifacts / relative
        if backup_file.exists() != live_file.exists():
            raise CandidatePromotionRecoveryRequiredError(f"{relative} was not restored from backup")
        if backup_file.exists() and backup_file.read_bytes() != live_file.read_bytes():
            raise CandidatePromotionRecoveryRequiredError(f"{relative} bytes do not match backup")


def _assert_promotion_backup_clear(run_root: Path) -> None:
    backup = run_root / CANDIDATE_PROMOTE_BACKUP_REL
    if backup.exists():
        raise CandidatePromotionRecoveryRequiredError(
            f"candidate promotion backup exists at {backup}; "
            "recover or remove it explicitly before retrying"
        )


def promote_staged_candidate_artifacts(stage_root: Path, run_root: Path) -> None:
    artifacts = run_root / "artifacts"
    staged_artifacts = stage_root / "artifacts"
    backup = run_root / CANDIDATE_PROMOTE_BACKUP_REL
    promotion_succeeded = False
    rollback_succeeded = False

    _assert_promotion_backup_clear(run_root)
    backup.mkdir(parents=True)

    def backup_live(name: str) -> None:
        live = artifacts / name
        if live.exists():
            shutil.move(str(live), str(backup / name))

    backup_live("candidate_review_clips")
    for relative in (
        "candidate_review_manifest.json",
        "candidate_review_rejected.json",
        "candidate_review_summary.json",
    ):
        backup_live(relative)

    try:
        shutil.move(str(staged_artifacts / "candidate_review_clips"), str(artifacts / "candidate_review_clips"))
        for relative in (
            "candidate_review_manifest.json",
            "candidate_review_rejected.json",
            "candidate_review_summary.json",
        ):
            _atomic_copy_file(staged_artifacts / relative, artifacts / relative)
        promotion_succeeded = True
    except Exception as promote_exc:
        try:
            _restore_live_candidate_artifacts(artifacts, backup)
            _verify_live_matches_backup(artifacts, backup)
            rollback_succeeded = True
        except Exception as rollback_exc:
            raise CandidatePromotionRecoveryRequiredError(
                f"candidate promotion failed and backup at {backup} could not be restored"
            ) from rollback_exc
        raise CandidatePromotionError("candidate promotion failed; live artifacts were restored from backup") from promote_exc
    finally:
        if promotion_succeeded or rollback_succeeded:
            if backup.exists():
                shutil.rmtree(backup, ignore_errors=True)


def assemble_candidate_review_clips_locked(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    from .assembly import assemble_candidate_review_clips

    _assert_promotion_backup_clear(run_root)
    stage_root = allocate_candidate_stage_root(run_root)
    recovery_required = False

    try:
        summary = assemble_candidate_review_clips(run_root, config, artifact_root=stage_root)
        validate_staged_candidate_artifacts(stage_root)
    except Exception:
        cleanup_candidate_stage_root(stage_root)
        raise

    staged_manifest_path = stage_root / CANDIDATE_MANIFEST_REL
    new_manifest_sha256 = manifest_sha256(staged_manifest_path)

    try:
        with clip_lab_run_lock(run_root):
            live_manifest_path = run_root / CANDIDATE_MANIFEST_REL
            previous_manifest_sha256 = manifest_sha256(live_manifest_path) if live_manifest_path.exists() else None
            promote_staged_candidate_artifacts(stage_root, run_root)
            finalize_candidate_regeneration(
                run_root,
                previous_manifest_sha256=previous_manifest_sha256,
                new_manifest_sha256=new_manifest_sha256,
            )
    except CandidatePromotionRecoveryRequiredError:
        recovery_required = True
        raise
    finally:
        if not recovery_required:
            cleanup_candidate_stage_root(stage_root)

    return summary
