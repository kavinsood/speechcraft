"""Dataset Clip Lab durable review state.

Acceptance fingerprint (v1): protects transcript edits and stored internal EDL
recipe hashes when present. When the candidate manifest exposes ``audio_sha256``,
that worker-written hash participates in acceptance identity for unedited clips.
Manifests without ``audio_sha256`` do not yet independently fingerprint an
unchanged candidate WAV if the file is replaced without regenerating the manifest.

TODO(slicer-coordination): slicer rerun must acquire ``clip_lab_run_lock`` before
replacing ``candidate_review_manifest.json`` / ``candidate_review_clips/`` so Clip
Lab edits and candidate regeneration cannot race.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Literal

from filelock import FileLock, Timeout

logger = logging.getLogger(__name__)

CLIP_LAB_STATE_REL = "artifacts/clip_lab_state.json"
CANDIDATE_MANIFEST_REL = "artifacts/candidate_review_manifest.json"
TRANSCRIPT_QC_REL = "artifacts/transcript_qc.json"
SPEAKER_PURITY_REL = "artifacts/speaker_purity.json"
LOCK_FILENAME = ".clip_lab_state.lock"

SCHEMA_VERSION = 1
STAGE = "clip_lab_state"
LOCK_TIMEOUT_SEC = 5
_SHA256_HEX = re.compile(r"^[a-f0-9]{64}$")
_ISO_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

ReviewStatus = Literal["unresolved", "accepted", "rejected", "quarantined"]

REVIEW_STATUSES: frozenset[str] = frozenset({"unresolved", "accepted", "rejected", "quarantined"})
RESERVED_REVIEWER_TAG_NAMES: frozenset[str] = frozenset({"accepted", "rejected", "quarantined", "unresolved"})

MAX_REVIEWER_TAGS = 20
MAX_REVIEWER_TAG_LENGTH = 48

PIPELINE_FINDING_LABELS: dict[str, str] = {
    "clip_contains_oov": "clip contains OOV",
    "clip_contains_symbol_hazard": "clip contains symbol hazard",
    "clip_contains_numeric_token": "clip contains numeric token",
    "transcript_requires_review": "transcript requires review",
    "word_near_trusted_edge": "word near trusted edge",
    "contains_currency_symbol": "contains currency symbol",
    "contains_numeric_token": "contains numeric token",
    "contains_oov": "contains OOV",
}


class ClipLabStateError(Exception):
    """Base error for clip lab state operations."""


class ClipLabValidationError(ClipLabStateError):
    """Invalid clip lab input or artifact shape."""


class StaleManifestError(ClipLabStateError):
    """Saved state or mutation targets a regenerated candidate manifest."""


class StaleClipError(ClipLabStateError):
    """Clip version mismatch from concurrent edits."""


class ClipNotFoundError(ClipLabStateError):
    """Clip id is not present in the current candidate manifest."""


class _UnsetType:
    __slots__ = ()


UNSET = _UnsetType()


@dataclass(frozen=True)
class QcLoadState:
    transcript_by_id: dict[str, dict[str, Any]]
    speaker_by_id: dict[str, dict[str, Any]]
    qc_available: bool
    qc_error: str | None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clip_lab_state_path(run_root: Path) -> Path:
    return run_root / CLIP_LAB_STATE_REL


def clip_lab_lock_path(run_root: Path) -> Path:
    return run_root / LOCK_FILENAME


def candidate_manifest_path(run_root: Path) -> Path:
    return run_root / CANDIDATE_MANIFEST_REL


def normalize_reviewer_tag(name: str) -> str:
    normalized = re.sub(r"\s+", " ", name.strip())
    if not normalized:
        raise ClipLabValidationError("tag must not be empty")
    if len(normalized) > MAX_REVIEWER_TAG_LENGTH:
        raise ClipLabValidationError(f"tag must be at most {MAX_REVIEWER_TAG_LENGTH} characters")
    return normalized


def validate_reviewer_tags(tags: list[str]) -> list[str]:
    if not isinstance(tags, list):
        raise ClipLabValidationError("reviewer_tags must be a list")
    if len(tags) > MAX_REVIEWER_TAGS:
        raise ClipLabValidationError(f"at most {MAX_REVIEWER_TAGS} reviewer tags allowed")
    seen: set[str] = set()
    validated: list[str] = []
    for raw in tags:
        if not isinstance(raw, str):
            raise ClipLabValidationError("reviewer_tags must contain strings")
        tag = normalize_reviewer_tag(raw)
        folded = tag.casefold()
        if folded in RESERVED_REVIEWER_TAG_NAMES:
            raise ClipLabValidationError(f"reserved tag name: {tag}")
        if folded in seen:
            continue
        seen.add(folded)
        validated.append(tag)
    return validated


def compute_audio_revision_hash(audio_edl_recipe: dict | list | None) -> str | None:
    if audio_edl_recipe is None:
        return None
    canonical = json.dumps(audio_edl_recipe, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_content_hash(
    *,
    manifest_transcript: str,
    transcript_override: str | None,
    audio_revision_hash: str | None,
    base_audio_hash: str | None = None,
) -> str:
    effective_transcript = transcript_override if transcript_override is not None else manifest_transcript
    effective_audio_hash = audio_revision_hash if audio_revision_hash is not None else (base_audio_hash or "")
    payload = f"transcript:{effective_transcript}\naudio:{effective_audio_hash}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_manifest_sha256(manifest_path: Path) -> str:
    if not manifest_path.exists():
        raise ClipLabValidationError(f"{CANDIDATE_MANIFEST_REL} is missing")
    return hashlib.sha256(manifest_path.read_bytes()).hexdigest()


def pipeline_finding_label(code: str) -> str:
    if code in PIPELINE_FINDING_LABELS:
        return PIPELINE_FINDING_LABELS[code]
    logger.warning("unknown pipeline finding code: %s", code)
    return code.replace("_", " ")


def pipeline_findings_from_manifest_row(row: dict[str, Any]) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for raw_code in row.get("review_reason_codes") or []:
        if not isinstance(raw_code, str) or not raw_code.strip():
            continue
        code = raw_code.strip()
        findings.append({"code": code, "label": pipeline_finding_label(code)})
    return findings


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ClipLabValidationError(f"malformed JSON artifact: {path.name}") from exc


def _validate_sha256_field(value: Any, *, field_name: str, clip_id: str | None = None) -> None:
    if value is None:
        return
    prefix = f"{clip_id} " if clip_id else ""
    if not isinstance(value, str) or not _SHA256_HEX.fullmatch(value):
        raise ClipLabValidationError(f"{prefix}{field_name} must be a lowercase sha256 hex string or null")


def _validate_iso_timestamp(value: Any, *, field_name: str, clip_id: str | None = None) -> None:
    if value is None:
        return
    prefix = f"{clip_id} " if clip_id else ""
    if not isinstance(value, str) or not _ISO_TIMESTAMP.fullmatch(value):
        raise ClipLabValidationError(f"{prefix}{field_name} must be an ISO-8601 UTC timestamp or null")


def _validate_transcript_override(value: Any, *, clip_id: str) -> None:
    if value is not None and not isinstance(value, str):
        raise ClipLabValidationError(f"{clip_id} transcript_override must be a string or null")


def _validate_audio_edl_recipe(value: Any, *, clip_id: str) -> None:
    if value is not None and not isinstance(value, (dict, list)):
        raise ClipLabValidationError(f"{clip_id} audio_edl_recipe must be an object, array, or null")


def load_candidate_manifest(run_root: Path) -> list[dict[str, Any]]:
    path = candidate_manifest_path(run_root)
    payload = _read_json(path)
    if not isinstance(payload, list):
        raise ClipLabValidationError(f"{CANDIDATE_MANIFEST_REL} must be a JSON list")
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, row in enumerate(payload):
        if not isinstance(row, dict):
            raise ClipLabValidationError(f"{CANDIDATE_MANIFEST_REL} row {index} must be an object")
        clip_id = _manifest_clip_id(row)
        if clip_id in seen:
            raise ClipLabValidationError(f"duplicate clip_id in candidate manifest: {clip_id}")
        seen.add(clip_id)
        rows.append(row)
    return rows


def index_manifest_by_clip_id(manifest: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {_manifest_clip_id(row): row for row in manifest}


def _manifest_clip_id(row: dict[str, Any]) -> str:
    clip_id = row.get("id") or row.get("clip_id")
    if not isinstance(clip_id, str) or not clip_id.strip():
        raise ClipLabValidationError("candidate manifest row missing clip id")
    return clip_id


def _manifest_transcript(row: dict[str, Any]) -> str:
    transcript = row.get("training_text")
    if not isinstance(transcript, str):
        raise ClipLabValidationError("candidate manifest row missing training_text")
    return transcript


def _manifest_base_audio_hash(row: dict[str, Any]) -> str | None:
    value = row.get("audio_sha256")
    if isinstance(value, str) and _SHA256_HEX.fullmatch(value):
        return value
    return None


def _validate_clip_version(raw: Any, *, clip_id: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ClipLabValidationError(f"{clip_id} clip_version must be an integer")
    if raw < 0:
        raise ClipLabValidationError(f"{clip_id} clip_version must be >= 0")
    return raw


def _validate_clip_entry(clip_id: str, entry: Any) -> None:
    if not isinstance(entry, dict):
        raise ClipLabValidationError(f"{clip_id} clip entry must be an object")
    if "clip_version" in entry:
        _validate_clip_version(entry.get("clip_version"), clip_id=clip_id)
    review_status = entry.get("review_status")
    if review_status is not None and review_status not in REVIEW_STATUSES:
        raise ClipLabValidationError(f"{clip_id} has invalid review_status: {review_status}")
    _validate_transcript_override(entry.get("transcript_override"), clip_id=clip_id)
    _validate_audio_edl_recipe(entry.get("audio_edl_recipe"), clip_id=clip_id)
    _validate_sha256_field(entry.get("audio_revision_hash"), field_name="audio_revision_hash", clip_id=clip_id)
    _validate_sha256_field(entry.get("accepted_content_hash"), field_name="accepted_content_hash", clip_id=clip_id)
    _validate_iso_timestamp(entry.get("accepted_at"), field_name="accepted_at", clip_id=clip_id)
    _validate_iso_timestamp(entry.get("updated_at"), field_name="updated_at", clip_id=clip_id)
    reviewer_tags = entry.get("reviewer_tags")
    if reviewer_tags is not None:
        validate_reviewer_tags(reviewer_tags)


def validate_clip_lab_state_document(payload: dict[str, Any]) -> None:
    schema_version = payload.get("schema_version")
    if schema_version != SCHEMA_VERSION:
        raise ClipLabValidationError(
            f"{CLIP_LAB_STATE_REL} has unsupported schema_version {schema_version!r}; expected {SCHEMA_VERSION}"
        )
    if payload.get("stage") != STAGE:
        raise ClipLabValidationError(f"{CLIP_LAB_STATE_REL} has invalid stage {payload.get('stage')!r}")
    manifest_sha = payload.get("candidate_manifest_sha256")
    if not isinstance(manifest_sha, str) or not manifest_sha.strip():
        raise ClipLabValidationError(f"{CLIP_LAB_STATE_REL} candidate_manifest_sha256 must be a non-empty string")
    _validate_iso_timestamp(payload.get("updated_at"), field_name="updated_at")
    clips = payload.get("clips")
    if clips is None:
        raise ClipLabValidationError(f"{CLIP_LAB_STATE_REL} is missing clips object")
    if not isinstance(clips, dict):
        raise ClipLabValidationError(f"{CLIP_LAB_STATE_REL} clips must be an object")
    for clip_id, entry in clips.items():
        if not isinstance(clip_id, str) or not clip_id.strip():
            raise ClipLabValidationError(f"{CLIP_LAB_STATE_REL} clips keys must be non-empty strings")
        _validate_clip_entry(clip_id, entry)


def load_clip_lab_state(run_root: Path) -> dict[str, Any] | None:
    path = clip_lab_state_path(run_root)
    if not path.exists():
        return None
    payload = _read_json(path)
    if not isinstance(payload, dict):
        raise ClipLabValidationError(f"{CLIP_LAB_STATE_REL} must be a JSON object")
    return payload


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    data = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    with tmp_path.open("w", encoding="utf-8") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    tmp_path.replace(path)
    dir_fd = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def save_clip_lab_state(run_root: Path, payload: dict[str, Any]) -> None:
    path = clip_lab_state_path(run_root)
    _atomic_write_json(path, payload)


@contextmanager
def clip_lab_run_lock(run_root: Path) -> Iterator[None]:
    lock = FileLock(str(clip_lab_lock_path(run_root)), timeout=LOCK_TIMEOUT_SEC)
    try:
        with lock:
            yield
    except Timeout as exc:
        raise ClipLabStateError("Clip Lab state is busy; retry shortly.") from exc


def _default_clip_entry() -> dict[str, Any]:
    return {
        "clip_version": 0,
        "review_status": "unresolved",
        "accepted_content_hash": None,
        "accepted_at": None,
        "transcript_override": None,
        "audio_edl_recipe": None,
        "audio_revision_hash": None,
        "reviewer_tags": [],
        "updated_at": None,
    }


def _stored_clip_entry(state: dict[str, Any], clip_id: str) -> dict[str, Any]:
    clips = state.get("clips")
    if not isinstance(clips, dict):
        return _default_clip_entry()
    entry = clips.get(clip_id)
    if not isinstance(entry, dict):
        return _default_clip_entry()
    merged = _default_clip_entry()
    merged.update(entry)
    return merged


def _effective_transcript(manifest_row: dict[str, Any], clip_entry: dict[str, Any]) -> str:
    override = clip_entry.get("transcript_override")
    if isinstance(override, str):
        return override
    return _manifest_transcript(manifest_row)


def _current_content_hash(manifest_row: dict[str, Any], clip_entry: dict[str, Any]) -> str:
    override = clip_entry.get("transcript_override")
    transcript_override = override if isinstance(override, str) else None
    audio_hash = clip_entry.get("audio_revision_hash")
    audio_revision_hash = audio_hash if isinstance(audio_hash, str) else None
    return compute_content_hash(
        manifest_transcript=_manifest_transcript(manifest_row),
        transcript_override=transcript_override,
        audio_revision_hash=audio_revision_hash,
        base_audio_hash=_manifest_base_audio_hash(manifest_row),
    )


def _clip_rows_from_qc_artifact(payload: Any, *, artifact_name: str) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        clips = payload.get("clips")
        if clips is None:
            raise ClipLabValidationError(f"{artifact_name} is missing clips[]")
        if not isinstance(clips, list):
            raise ClipLabValidationError(f"{artifact_name} clips must be a list")
        rows = clips
    else:
        raise ClipLabValidationError(f"{artifact_name} must be a JSON object or clip list")
    validated: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ClipLabValidationError(f"{artifact_name} row {index} must be an object")
        validated.append(row)
    return validated


def _index_qc_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        clip_id = row.get("clip_id") or row.get("id")
        if isinstance(clip_id, str) and clip_id.strip():
            indexed[clip_id] = row
    return indexed


def _score_from_fraction(raw: Any) -> float | None:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    value = float(raw)
    if 0.0 <= value <= 1.0:
        return round(value * 100, 2)
    if 0.0 <= value <= 100.0:
        return round(value, 2)
    return None


def _transcript_match_score(row: dict[str, Any]) -> float | None:
    if row.get("transcript_match_score") is not None:
        return _score_from_fraction(row.get("transcript_match_score"))
    for field_name in (
        "ctc_min_span_score",
        "ctc_min_aligned_token_score",
        "ctc_min_window_score",
        "ctc_mean_score",
    ):
        if field_name in row and row.get(field_name) is not None:
            return _score_from_fraction(row.get(field_name))
    return None


def _speaker_check_score(row: dict[str, Any]) -> float | None:
    if row.get("speaker_check_score") is not None:
        return _score_from_fraction(row.get("speaker_check_score"))
    if row.get("min_window_similarity") is not None:
        return _score_from_fraction(row.get("min_window_similarity"))
    return None


def _load_optional_qc_indexes(run_root: Path) -> QcLoadState:
    transcript_by_id: dict[str, dict[str, Any]] = {}
    speaker_by_id: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    transcript_path = run_root / TRANSCRIPT_QC_REL
    speaker_path = run_root / SPEAKER_PURITY_REL
    if transcript_path.exists():
        try:
            transcript_by_id = _index_qc_rows(
                _clip_rows_from_qc_artifact(_read_json(transcript_path), artifact_name=TRANSCRIPT_QC_REL)
            )
        except ClipLabValidationError as exc:
            errors.append(str(exc))
    if speaker_path.exists():
        try:
            speaker_by_id = _index_qc_rows(
                _clip_rows_from_qc_artifact(_read_json(speaker_path), artifact_name=SPEAKER_PURITY_REL)
            )
        except ClipLabValidationError as exc:
            errors.append(str(exc))
    qc_error = "; ".join(errors) if errors else None
    qc_available = not errors and (transcript_path.exists() or speaker_path.exists())
    return QcLoadState(
        transcript_by_id=transcript_by_id,
        speaker_by_id=speaker_by_id,
        qc_available=qc_available,
        qc_error=qc_error,
    )


def _build_clip_view(
    *,
    manifest_row: dict[str, Any],
    clip_entry: dict[str, Any],
    transcript_by_id: dict[str, dict[str, Any]],
    speaker_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    clip_id = _manifest_clip_id(manifest_row)
    original_transcript = _manifest_transcript(manifest_row)
    transcript_override = clip_entry.get("transcript_override")
    override_value = transcript_override if isinstance(transcript_override, str) else None
    transcript = _effective_transcript(manifest_row, clip_entry)
    content_hash = _current_content_hash(manifest_row, clip_entry)

    review_status = clip_entry.get("review_status")
    if review_status not in REVIEW_STATUSES:
        review_status = "unresolved"

    accepted_content_hash = clip_entry.get("accepted_content_hash")
    accepted_hash = accepted_content_hash if isinstance(accepted_content_hash, str) else None
    acceptance_stale = False
    if review_status == "accepted" and accepted_hash != content_hash:
        review_status = "unresolved"
        acceptance_stale = True

    reviewer_tags = clip_entry.get("reviewer_tags")
    tags = list(reviewer_tags) if isinstance(reviewer_tags, list) else []

    transcript_row = transcript_by_id.get(clip_id, {})
    speaker_row = speaker_by_id.get(clip_id, {})

    return {
        "clip_id": clip_id,
        "clip_version": int(clip_entry.get("clip_version") or 0),
        "review_status": review_status,
        "transcript": transcript,
        "original_transcript": original_transcript,
        "transcript_override": override_value,
        "reviewer_tags": tags,
        "pipeline_findings": pipeline_findings_from_manifest_row(manifest_row),
        "content_hash": content_hash,
        "accepted_content_hash": accepted_hash,
        "accepted_at": clip_entry.get("accepted_at") if isinstance(clip_entry.get("accepted_at"), str) else None,
        "acceptance_stale": acceptance_stale,
        "transcript_match": _transcript_match_score(transcript_row),
        "speaker_check": _speaker_check_score(speaker_row),
    }


def build_clip_lab_view(run_root: Path, *, run_id: str) -> dict[str, Any]:
    manifest = load_candidate_manifest(run_root)
    current_manifest_sha = compute_manifest_sha256(candidate_manifest_path(run_root))
    saved_state = load_clip_lab_state(run_root)
    qc_state = _load_optional_qc_indexes(run_root)

    stale_state = False
    stale_reason: str | None = None
    invalid_state = False
    invalid_state_reason: str | None = None
    saved_state_clip_count = 0
    overlay_state: dict[str, Any] | None = saved_state

    if saved_state is not None:
        clips = saved_state.get("clips")
        if isinstance(clips, dict):
            saved_state_clip_count = len(clips)
        try:
            validate_clip_lab_state_document(saved_state)
        except ClipLabValidationError as exc:
            invalid_state = True
            invalid_state_reason = str(exc)
            overlay_state = None
        else:
            saved_manifest_sha = saved_state.get("candidate_manifest_sha256")
            if saved_manifest_sha != current_manifest_sha:
                stale_state = True
                stale_reason = "candidate_manifest_changed"
                overlay_state = None

    clips_out: list[dict[str, Any]] = []
    for manifest_row in manifest:
        clip_id = _manifest_clip_id(manifest_row)
        if overlay_state is None:
            clip_entry = _default_clip_entry()
        else:
            clip_entry = _stored_clip_entry(overlay_state, clip_id)
        clips_out.append(
            _build_clip_view(
                manifest_row=manifest_row,
                clip_entry=clip_entry,
                transcript_by_id=qc_state.transcript_by_id,
                speaker_by_id=qc_state.speaker_by_id,
            )
        )

    return {
        "run_id": run_id,
        "candidate_manifest_sha256": current_manifest_sha,
        "stale_state": stale_state,
        "stale_reason": stale_reason,
        "invalid_state": invalid_state,
        "invalid_state_reason": invalid_state_reason,
        "saved_state_clip_count": saved_state_clip_count,
        "qc_available": qc_state.qc_available,
        "qc_error": qc_state.qc_error,
        "clips": clips_out,
    }


def _validate_review_status(value: str) -> ReviewStatus:
    if value not in REVIEW_STATUSES:
        raise ClipLabValidationError(f"invalid review_status: {value}")
    return value  # type: ignore[return-value]


def _clear_acceptance_fields(clip_entry: dict[str, Any]) -> None:
    clip_entry["accepted_content_hash"] = None
    clip_entry["accepted_at"] = None


def _set_acceptance_fields(
    clip_entry: dict[str, Any],
    *,
    manifest_row: dict[str, Any],
) -> None:
    clip_entry["accepted_content_hash"] = _current_content_hash(manifest_row, clip_entry)
    clip_entry["accepted_at"] = _utc_now_iso()


def _normalize_stale_acceptance(
    clip_entry: dict[str, Any],
    *,
    manifest_row: dict[str, Any],
) -> None:
    if clip_entry.get("review_status") != "accepted":
        return
    current_hash = _current_content_hash(manifest_row, clip_entry)
    accepted_hash = clip_entry.get("accepted_content_hash")
    if not isinstance(accepted_hash, str) or accepted_hash != current_hash:
        clip_entry["review_status"] = "unresolved"
        _clear_acceptance_fields(clip_entry)


def _apply_patch_fields(
    clip_entry: dict[str, Any],
    *,
    manifest_row: dict[str, Any],
    review_status: str | None,
    transcript_override: str | None | _UnsetType,
    reviewer_tags: list[str] | None,
    audio_edl_recipe: Any | None | _UnsetType,
) -> None:
    _normalize_stale_acceptance(clip_entry, manifest_row=manifest_row)

    was_accepted = clip_entry.get("review_status") == "accepted"
    previous_content_hash = _current_content_hash(manifest_row, clip_entry)

    if transcript_override is not UNSET:
        if transcript_override is None:
            clip_entry["transcript_override"] = None
        elif isinstance(transcript_override, str):
            clip_entry["transcript_override"] = transcript_override
        else:
            raise ClipLabValidationError("transcript_override must be a string or null")

    if audio_edl_recipe is not UNSET:
        if audio_edl_recipe is not None and not isinstance(audio_edl_recipe, (dict, list)):
            raise ClipLabValidationError("audio_edl_recipe must be an object, array, or null")
        clip_entry["audio_edl_recipe"] = audio_edl_recipe
        clip_entry["audio_revision_hash"] = compute_audio_revision_hash(audio_edl_recipe)

    if was_accepted and _current_content_hash(manifest_row, clip_entry) != previous_content_hash:
        clip_entry["review_status"] = "unresolved"
        _clear_acceptance_fields(clip_entry)

    if reviewer_tags is not None:
        clip_entry["reviewer_tags"] = validate_reviewer_tags(reviewer_tags)

    if review_status is not None:
        validated_status = _validate_review_status(review_status)
        clip_entry["review_status"] = validated_status
        if validated_status == "accepted":
            _set_acceptance_fields(clip_entry, manifest_row=manifest_row)
        else:
            _clear_acceptance_fields(clip_entry)


def _ensure_state_document(
    *,
    saved_state: dict[str, Any] | None,
    current_manifest_sha: str,
) -> dict[str, Any]:
    if saved_state is None:
        return {
            "schema_version": SCHEMA_VERSION,
            "stage": STAGE,
            "candidate_manifest_sha256": current_manifest_sha,
            "updated_at": _utc_now_iso(),
            "clips": {},
        }
    validate_clip_lab_state_document(saved_state)
    if saved_state.get("candidate_manifest_sha256") != current_manifest_sha:
        raise StaleManifestError("candidate manifest changed since saved clip lab state")
    clips = saved_state.get("clips")
    if clips is None:
        saved_state["clips"] = {}
    elif not isinstance(clips, dict):
        raise ClipLabValidationError(f"{CLIP_LAB_STATE_REL} clips must be an object")
    saved_state["schema_version"] = SCHEMA_VERSION
    saved_state["stage"] = STAGE
    return saved_state


def patch_clip_lab_clip(
    run_root: Path,
    clip_id: str,
    *,
    expected_manifest_sha256: str,
    expected_clip_version: int,
    review_status: str | None = None,
    transcript_override: str | None | _UnsetType = UNSET,
    reviewer_tags: list[str] | None = None,
    audio_edl_recipe: Any | None | _UnsetType = UNSET,
) -> dict[str, Any]:
    if (
        review_status is None
        and transcript_override is UNSET
        and reviewer_tags is None
        and audio_edl_recipe is UNSET
    ):
        raise ClipLabValidationError("patch must include at least one mutable field")

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

        _apply_patch_fields(
            clip_entry,
            manifest_row=manifest_row,
            review_status=review_status,
            transcript_override=transcript_override,
            reviewer_tags=reviewer_tags,
            audio_edl_recipe=audio_edl_recipe,
        )

        clip_entry["clip_version"] = stored_version + 1
        clip_entry["updated_at"] = _utc_now_iso()
        state["updated_at"] = _utc_now_iso()
        state["candidate_manifest_sha256"] = current_manifest_sha
        save_clip_lab_state(run_root, state)

        qc_state = _load_optional_qc_indexes(run_root)
        return _build_clip_view(
            manifest_row=manifest_row,
            clip_entry=clip_entry,
            transcript_by_id=qc_state.transcript_by_id,
            speaker_by_id=qc_state.speaker_by_id,
        )


def find_clip_lab_view(run_root: Path, *, run_id: str, clip_id: str) -> dict[str, Any]:
    view = build_clip_lab_view(run_root, run_id=run_id)
    for clip in view["clips"]:
        if clip["clip_id"] == clip_id:
            return clip
    raise ClipNotFoundError(f"unknown clip_id: {clip_id}")


def _serialize_clip_lab_clip(raw: dict[str, Any]) -> Any:
    from .models import DatasetClipLabClipView, DatasetClipLabPipelineFindingView

    findings = [
        DatasetClipLabPipelineFindingView(**finding)
        for finding in raw.get("pipeline_findings") or []
        if isinstance(finding, dict)
    ]
    return DatasetClipLabClipView(
        clip_id=raw["clip_id"],
        clip_version=int(raw.get("clip_version") or 0),
        review_status=raw["review_status"],
        transcript=raw["transcript"],
        original_transcript=raw["original_transcript"],
        transcript_override=raw.get("transcript_override"),
        reviewer_tags=list(raw.get("reviewer_tags") or []),
        pipeline_findings=findings,
        content_hash=raw["content_hash"],
        accepted_content_hash=raw.get("accepted_content_hash"),
        accepted_at=raw.get("accepted_at"),
        acceptance_stale=bool(raw.get("acceptance_stale")),
        transcript_match=raw.get("transcript_match"),
        speaker_check=raw.get("speaker_check"),
    )


def _serialize_clip_lab_view(raw: dict[str, Any]) -> Any:
    from .models import DatasetClipLabView

    return DatasetClipLabView(
        run_id=raw["run_id"],
        candidate_manifest_sha256=raw["candidate_manifest_sha256"],
        stale_state=bool(raw.get("stale_state")),
        stale_reason=raw.get("stale_reason"),
        invalid_state=bool(raw.get("invalid_state")),
        invalid_state_reason=raw.get("invalid_state_reason"),
        saved_state_clip_count=int(raw.get("saved_state_clip_count") or 0),
        qc_available=bool(raw.get("qc_available")),
        qc_error=raw.get("qc_error"),
        clips=[_serialize_clip_lab_clip(clip) for clip in raw.get("clips") or []],
    )


def get_dataset_clip_lab(repository: Any, run_id: str) -> Any:
    from sqlmodel import Session

    from .dataset_runs import _run_root
    from .models import ProcessingRun

    with Session(repository.engine) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        root = _run_root(repository, run)
    return _serialize_clip_lab_view(build_clip_lab_view(root, run_id=run_id))


def patch_dataset_clip_lab_clip(
    repository: Any,
    run_id: str,
    clip_id: str,
    payload: Any,
) -> Any:
    from sqlmodel import Session

    from .dataset_runs import _run_root
    from .models import ProcessingRun

    with Session(repository.engine) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        root = _run_root(repository, run)

    fields_set = payload.model_fields_set
    patch_kwargs: dict[str, Any] = {
        "expected_manifest_sha256": payload.expected_manifest_sha256,
        "expected_clip_version": payload.expected_clip_version,
    }
    if "review_status" in fields_set:
        patch_kwargs["review_status"] = payload.review_status
    if "transcript_override" in fields_set:
        patch_kwargs["transcript_override"] = payload.transcript_override
    if "reviewer_tags" in fields_set:
        patch_kwargs["reviewer_tags"] = payload.reviewer_tags

    updated = patch_clip_lab_clip(run_root=root, clip_id=clip_id, **patch_kwargs)
    return _serialize_clip_lab_clip(updated)
