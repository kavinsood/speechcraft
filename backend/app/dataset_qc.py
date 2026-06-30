from __future__ import annotations

import json
import math
import shutil
from pathlib import Path
from typing import Any, Literal

from sqlmodel import Session

from .dataset_runs import _index_artifacts, _read_json_object, _run_root, dataset_storage_root
from .models import (
    DatasetQcClipView,
    DatasetQcDefaultsView,
    DatasetQcFinalizeRequest,
    DatasetQcFinalizeResponse,
    DatasetQcFinalizeSummaryView,
    DatasetQcFinalizedThresholdsView,
    DatasetQcPayloadView,
    DatasetQcWeakSpanView,
    ProcessingRun,
    resolve_run_artifact_path,
    utc_now,
)

QC_SCHEMA_VERSION = 1
DEFAULT_TRANSCRIPT_THRESHOLD = 85
DEFAULT_SPEAKER_THRESHOLD = 70

TRANSCRIPT_QC_REL = "artifacts/transcript_qc.json"
TRANSCRIPT_QC_SUMMARY_REL = "artifacts/transcript_qc_summary.json"
SPEAKER_PURITY_REL = "artifacts/speaker_purity.json"
SPEAKER_PURITY_SUMMARY_REL = "artifacts/speaker_purity_summary.json"
DATASET_QC_REL = "artifacts/dataset_qc.json"
DATASET_QC_SUMMARY_REL = "artifacts/dataset_qc_summary.json"
CANDIDATE_MANIFEST_REL = "artifacts/candidate_review_manifest.json"
EXPORT_MANIFEST_REL = "artifacts/export_manifest.json"
EXPORT_AUDIT_REL = "artifacts/export_audit.json"
EXPORT_SUMMARY_REL = "artifacts/export_summary.json"
NATIVE_EXPORT_DIR_REL = "artifacts/native_export_clips"

SCORE_METHODS = {
    "transcript_match": "min_meaningful_ctc_span",
    "speaker_check": "min_valid_window_similarity",
}


class DatasetQcValidationError(ValueError):
    pass


def _read_artifact_payload(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DatasetQcValidationError(f"Malformed JSON artifact: {path.name}") from exc


def _clip_rows_from_artifact(payload: Any, *, artifact_name: str) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        raise DatasetQcValidationError(f"{artifact_name} must be a JSON object or clip list")
    schema_version = payload.get("schema_version")
    if schema_version is not None and schema_version != QC_SCHEMA_VERSION:
        raise DatasetQcValidationError(
            f"{artifact_name} has unsupported schema_version {schema_version!r}; expected {QC_SCHEMA_VERSION}"
        )
    clips = payload.get("clips")
    if clips is None:
        raise DatasetQcValidationError(f"{artifact_name} is missing clips[]")
    if not isinstance(clips, list):
        raise DatasetQcValidationError(f"{artifact_name} clips must be a list")
    return [row for row in clips if isinstance(row, dict)]


def _validate_qc_score(raw: Any, field_name: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise DatasetQcValidationError(f"{field_name} must be a finite number 0-100")
    score = float(raw)
    if not math.isfinite(score) or score < 0 or score > 100:
        raise DatasetQcValidationError(f"{field_name} must be a finite number 0-100")
    return round(score, 2)


def _score_from_fraction(raw: Any, field_name: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise DatasetQcValidationError(f"{field_name} must be a finite number 0-1")
    value = float(raw)
    if not math.isfinite(value) or value < 0 or value > 1:
        raise DatasetQcValidationError(f"{field_name} must be a finite number 0-1")
    return round(value * 100, 2)


def _validate_duration_sec(raw: Any, field_name: str) -> float:
    if not isinstance(raw, (int, float)) or isinstance(raw, bool):
        raise DatasetQcValidationError(f"{field_name} must be a finite number >= 0")
    value = float(raw)
    if not math.isfinite(value) or value < 0:
        raise DatasetQcValidationError(f"{field_name} must be a finite number >= 0")
    return value


def _index_rows_by_clip_id(rows: list[dict[str, Any]], *, artifact_name: str) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        clip_id = row.get("clip_id") or row.get("id")
        if not isinstance(clip_id, str) or not clip_id.strip():
            continue
        if clip_id in indexed:
            raise DatasetQcValidationError(f"duplicate clip_id in {artifact_name}: {clip_id}")
        indexed[clip_id] = row
    return indexed


def _normalize_span_score(raw: Any) -> float | None:
    if not isinstance(raw, (int, float)) or isinstance(raw, bool):
        return None
    value = float(raw)
    if 0.0 <= value <= 1.0:
        return round(value * 100, 3)
    if 0.0 <= value <= 100.0:
        return value
    return None


def _weak_transcript_spans(row: dict[str, Any]) -> list[DatasetQcWeakSpanView]:
    spans: list[DatasetQcWeakSpanView] = []
    for span in row.get("weak_spans") or []:
        if not isinstance(span, dict):
            continue
        start = span.get("start_sec")
        end = span.get("end_sec")
        if not isinstance(start, (int, float)) or not isinstance(end, (int, float)):
            continue
        spans.append(
            DatasetQcWeakSpanView(
                start_sec=float(start),
                end_sec=float(end),
                text=span.get("text") if isinstance(span.get("text"), str) else None,
                score=_normalize_span_score(span.get("score")),
            )
        )
    return spans


def _weak_speaker_spans(row: dict[str, Any]) -> list[DatasetQcWeakSpanView]:
    spans: list[DatasetQcWeakSpanView] = []
    for span in row.get("suspicious_spans") or []:
        if not isinstance(span, dict):
            continue
        start = span.get("start_sec")
        end = span.get("end_sec")
        if not isinstance(start, (int, float)) or not isinstance(end, (int, float)):
            continue
        score = span.get("similarity", span.get("score"))
        spans.append(
            DatasetQcWeakSpanView(
                start_sec=float(start),
                end_sec=float(end),
                score=_normalize_span_score(score),
            )
        )
    return spans


def _transcript_score_from_row(row: dict[str, Any]) -> float:
    for field_name in (
        "ctc_min_span_score",
        "ctc_min_aligned_token_score",
        "ctc_min_window_score",
        "ctc_mean_score",
    ):
        if field_name in row and row.get(field_name) is not None:
            return _score_from_fraction(row.get(field_name), field_name)
    return _validate_qc_score(row.get("transcript_match_score"), "transcript_match_score")


def _speaker_score_from_row(row: dict[str, Any]) -> float:
    if row.get("min_window_similarity") is not None:
        return _score_from_fraction(row.get("min_window_similarity"), "min_window_similarity")
    if row.get("speaker_check_score") is not None:
        return _validate_qc_score(row.get("speaker_check_score"), "speaker_check_score")
    raise DatasetQcValidationError("speaker_check_score missing")


def _audio_url(run_id: str, clip_id: str) -> str:
    return f"/media/dataset-runs/{run_id}/candidate-review/{clip_id}.wav"


def _resolve_audio_path(repository: Any, run: ProcessingRun, audio_path: str) -> bool:
    if not audio_path:
        return False
    try:
        path = resolve_run_artifact_path(dataset_storage_root(repository), str(run.artifact_root), audio_path)
    except ValueError:
        return False
    return path.exists() and path.is_file()


def _threshold_status(
    *,
    transcript_match: float | None,
    speaker_check: float | None,
    transcript_threshold: int,
    speaker_threshold: int,
    audio_missing: bool = False,
) -> Literal["accepted", "rejected"]:
    if audio_missing or transcript_match is None or speaker_check is None:
        return "rejected"
    if transcript_match >= transcript_threshold and speaker_check >= speaker_threshold:
        return "accepted"
    return "rejected"


def _final_status(
    *,
    threshold_status: Literal["accepted", "rejected"],
    manual_override: Literal["force_keep", "force_reject"] | None,
    hard_failed: bool = False,
) -> Literal["accepted", "rejected"]:
    if hard_failed:
        return "rejected"
    if manual_override == "force_keep":
        return "accepted"
    if manual_override == "force_reject":
        return "rejected"
    return threshold_status


def _failed_checks(
    *,
    transcript_match: float | None,
    speaker_check: float | None,
    transcript_threshold: int,
    speaker_threshold: int,
    audio_missing: bool = False,
) -> list[str]:
    failed: list[str] = []
    if audio_missing:
        failed.append("missing_audio_file")
    if transcript_match is None:
        failed.append("missing_transcript_qc")
    elif transcript_match < transcript_threshold:
        failed.append("low_transcript_match")
    if speaker_check is None:
        failed.append("missing_speaker_qc")
    elif speaker_check < speaker_threshold:
        failed.append("low_speaker_similarity_window")
    return failed


def _combined_reason_codes(clip: DatasetQcClipView, failed_checks: list[str]) -> list[str]:
    return sorted(
        set(
            clip.candidate_reason_codes
            + clip.transcript_reason_codes
            + clip.speaker_reason_codes
            + clip.qc_reason_codes
            + failed_checks
        )
    )


def _not_ready_payload(
    *,
    run_id: str,
    missing: list[str],
    invalid: list[str],
    finalized: bool,
    finalized_thresholds: DatasetQcFinalizedThresholdsView | None,
) -> DatasetQcPayloadView:
    return DatasetQcPayloadView(
        run_id=run_id,
        ready=False,
        missing_artifacts=missing,
        invalid_artifacts=invalid,
        defaults=DatasetQcDefaultsView(
            transcript_match_threshold=DEFAULT_TRANSCRIPT_THRESHOLD,
            speaker_check_threshold=DEFAULT_SPEAKER_THRESHOLD,
        ),
        finalized=finalized,
        finalized_thresholds=finalized_thresholds,
        clips=[],
    )


def _read_manifest_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = _read_artifact_payload(path)
    if not isinstance(payload, list):
        raise DatasetQcValidationError(f"{CANDIDATE_MANIFEST_REL} must be a JSON list")
    return [row for row in payload if isinstance(row, dict)]


def _validated_manifest_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    validated: list[dict[str, Any]] = []
    for row in rows:
        clip_id = row.get("id") or row.get("clip_id")
        if not isinstance(clip_id, str) or not clip_id.strip():
            raise DatasetQcValidationError("candidate manifest row missing clip id")
        if clip_id in seen:
            raise DatasetQcValidationError(f"duplicate clip_id in candidate manifest: {clip_id}")
        seen.add(clip_id)
        _validate_duration_sec(row.get("duration_sec"), "duration_sec")
        validated.append(row)
    return validated


def _read_finalized_state(
    root: Path,
) -> tuple[bool, DatasetQcFinalizedThresholdsView | None, dict[str, str | None], bool]:
    path = root / DATASET_QC_REL
    if not path.exists():
        return False, None, {}, False
    try:
        payload = _read_artifact_payload(path)
    except DatasetQcValidationError:
        return False, None, {}, True
    if not isinstance(payload, dict) or not payload:
        return False, None, {}, True
    thresholds = payload.get("thresholds") if isinstance(payload.get("thresholds"), dict) else {}
    transcript_min = thresholds.get("transcript_match_min")
    speaker_min = thresholds.get("speaker_check_min")
    finalized_thresholds = None
    if isinstance(transcript_min, int) and isinstance(speaker_min, int):
        finalized_thresholds = DatasetQcFinalizedThresholdsView(
            transcript_match_min=transcript_min,
            speaker_check_min=speaker_min,
        )
    overrides_by_clip: dict[str, str | None] = {}
    for override in payload.get("manual_overrides") or []:
        if not isinstance(override, dict):
            continue
        clip_id = override.get("clip_id")
        value = override.get("override")
        if isinstance(clip_id, str) and value in {"force_keep", "force_reject"}:
            overrides_by_clip[clip_id] = value
    for clip in payload.get("clips") or []:
        if not isinstance(clip, dict):
            continue
        clip_id = clip.get("clip_id")
        value = clip.get("manual_override")
        if isinstance(clip_id, str) and value in {"force_keep", "force_reject"}:
            overrides_by_clip[clip_id] = value
    return True, finalized_thresholds, overrides_by_clip, False


def _score_method_from_rows(
    rows_by_id: dict[str, dict[str, Any]],
    *,
    row_field: str,
) -> str | None:
    for row in rows_by_id.values():
        value = row.get(row_field)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _dataset_qc_score_methods(
    transcript_by_id: dict[str, dict[str, Any]],
    speaker_by_id: dict[str, dict[str, Any]],
) -> dict[str, str]:
    transcript_method = _score_method_from_rows(
        transcript_by_id,
        row_field="transcript_score_method",
    ) or SCORE_METHODS["transcript_match"]
    speaker_method = _score_method_from_rows(
        speaker_by_id,
        row_field="speaker_score_method",
    ) or SCORE_METHODS["speaker_check"]
    return {
        "transcript_match": transcript_method,
        "speaker_check": speaker_method,
    }


def _clear_export_artifacts(root: Path) -> None:
    for relative in (EXPORT_MANIFEST_REL, EXPORT_AUDIT_REL, EXPORT_SUMMARY_REL):
        path = root / relative
        if path.exists():
            path.unlink()
    export_dir = root / NATIVE_EXPORT_DIR_REL
    if export_dir.exists():
        shutil.rmtree(export_dir)


def get_dataset_qc(repository: Any, run_id: str) -> DatasetQcPayloadView:
    with Session(repository.engine) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        root = _run_root(repository, run)
        missing: list[str] = []
        invalid: list[str] = []
        manifest_path = root / CANDIDATE_MANIFEST_REL
        transcript_path = root / TRANSCRIPT_QC_REL
        speaker_path = root / SPEAKER_PURITY_REL
        if not manifest_path.exists():
            missing.append(CANDIDATE_MANIFEST_REL)
        if not transcript_path.exists():
            missing.append(TRANSCRIPT_QC_REL)
        if not speaker_path.exists():
            missing.append(SPEAKER_PURITY_REL)

        finalized, finalized_thresholds, finalized_overrides, dataset_qc_invalid = _read_finalized_state(root)

        if missing:
            return _not_ready_payload(
                run_id=run_id,
                missing=missing,
                invalid=invalid,
                finalized=finalized and not dataset_qc_invalid,
                finalized_thresholds=finalized_thresholds if not dataset_qc_invalid else None,
            )

        try:
            manifest = _validated_manifest_rows(_read_manifest_rows(manifest_path))
        except DatasetQcValidationError:
            return _not_ready_payload(
                run_id=run_id,
                missing=[],
                invalid=[CANDIDATE_MANIFEST_REL],
                finalized=False,
                finalized_thresholds=None,
            )

        transcript_rows: list[dict[str, Any]] = []
        speaker_rows: list[dict[str, Any]] = []
        try:
            transcript_rows = _clip_rows_from_artifact(
                _read_artifact_payload(transcript_path),
                artifact_name=TRANSCRIPT_QC_REL,
            )
        except DatasetQcValidationError:
            invalid.append(TRANSCRIPT_QC_REL)
        try:
            speaker_rows = _clip_rows_from_artifact(
                _read_artifact_payload(speaker_path),
                artifact_name=SPEAKER_PURITY_REL,
            )
        except DatasetQcValidationError:
            invalid.append(SPEAKER_PURITY_REL)

        if invalid:
            return _not_ready_payload(
                run_id=run_id,
                missing=[],
                invalid=invalid,
                finalized=finalized,
                finalized_thresholds=finalized_thresholds,
            )

        transcript_by_id: dict[str, dict[str, Any]] = {}
        speaker_by_id: dict[str, dict[str, Any]] = {}
        try:
            transcript_by_id = _index_rows_by_clip_id(transcript_rows, artifact_name=TRANSCRIPT_QC_REL)
        except DatasetQcValidationError:
            invalid.append(TRANSCRIPT_QC_REL)
        try:
            speaker_by_id = _index_rows_by_clip_id(speaker_rows, artifact_name=SPEAKER_PURITY_REL)
        except DatasetQcValidationError:
            invalid.append(SPEAKER_PURITY_REL)
        if invalid:
            return _not_ready_payload(
                run_id=run_id,
                missing=[],
                invalid=invalid,
                finalized=finalized,
                finalized_thresholds=finalized_thresholds,
            )

        clips: list[DatasetQcClipView] = []

        for candidate in manifest:
            clip_id = str(candidate.get("id") or candidate.get("clip_id") or "")
            audio_path = str(candidate.get("audio_path") or "")
            duration_sec = float(candidate["duration_sec"])
            transcript_row = transcript_by_id.get(clip_id)
            speaker_row = speaker_by_id.get(clip_id)

            transcript_match: float | None = None
            speaker_check: float | None = None
            transcript_reason_codes: list[str] = []
            speaker_reason_codes: list[str] = []
            qc_reason_codes: list[str] = []
            weak_transcript_spans: list[DatasetQcWeakSpanView] = []
            weak_speaker_spans: list[DatasetQcWeakSpanView] = []

            if not _resolve_audio_path(repository, run, audio_path):
                qc_reason_codes.append("missing_audio_file")

            if transcript_row is None:
                qc_reason_codes.append("missing_transcript_qc")
            else:
                try:
                    transcript_match = _transcript_score_from_row(transcript_row)
                    transcript_reason_codes = [
                        str(code) for code in (transcript_row.get("reason_codes") or []) if isinstance(code, str)
                    ]
                    weak_transcript_spans = _weak_transcript_spans(transcript_row)
                except DatasetQcValidationError:
                    qc_reason_codes.append("missing_transcript_qc")

            if speaker_row is None:
                qc_reason_codes.append("missing_speaker_qc")
            else:
                try:
                    speaker_check = _speaker_score_from_row(speaker_row)
                    speaker_reason_codes = [
                        str(code) for code in (speaker_row.get("reason_codes") or []) if isinstance(code, str)
                    ]
                    weak_speaker_spans = _weak_speaker_spans(speaker_row)
                except DatasetQcValidationError:
                    qc_reason_codes.append("missing_speaker_qc")

            manual_override = finalized_overrides.get(clip_id)
            if manual_override not in {"force_keep", "force_reject"}:
                manual_override = None

            clips.append(
                DatasetQcClipView(
                    clip_id=clip_id,
                    audio_path=audio_path,
                    audio_url=_audio_url(run_id, clip_id),
                    duration_sec=duration_sec,
                    training_text=str(candidate.get("training_text") or ""),
                    alignment_text=(
                        str(candidate.get("alignment_text"))
                        if candidate.get("alignment_text") is not None
                        else None
                    ),
                    transcript_match=transcript_match,
                    speaker_check=speaker_check,
                    transcript_reason_codes=transcript_reason_codes,
                    speaker_reason_codes=speaker_reason_codes,
                    candidate_reason_codes=[
                        str(code) for code in (candidate.get("review_reason_codes") or []) if isinstance(code, str)
                    ],
                    qc_reason_codes=qc_reason_codes,
                    weak_transcript_spans=weak_transcript_spans,
                    weak_speaker_spans=weak_speaker_spans,
                    manual_override=manual_override,
                )
            )

        response_invalid = list(invalid)
        if dataset_qc_invalid:
            response_invalid.append(DATASET_QC_REL)

        return DatasetQcPayloadView(
            run_id=run_id,
            ready=True,
            missing_artifacts=[],
            invalid_artifacts=response_invalid,
            defaults=DatasetQcDefaultsView(
                transcript_match_threshold=DEFAULT_TRANSCRIPT_THRESHOLD,
                speaker_check_threshold=DEFAULT_SPEAKER_THRESHOLD,
            ),
            finalized=finalized and not dataset_qc_invalid,
            finalized_thresholds=finalized_thresholds if not dataset_qc_invalid else None,
            clips=clips,
        )


def _validate_manual_overrides(
    request: DatasetQcFinalizeRequest,
    known_clip_ids: set[str],
    clips_by_id: dict[str, DatasetQcClipView],
) -> dict[str, Literal["force_keep", "force_reject"]]:
    overrides: dict[str, Literal["force_keep", "force_reject"]] = {}
    for entry in request.manual_overrides:
        clip_id = entry.clip_id.strip()
        if not clip_id:
            raise ValueError("manual_overrides clip_id must not be empty")
        if clip_id not in known_clip_ids:
            raise ValueError(f"Unknown clip_id in manual_overrides: {clip_id}")
        if entry.override not in {"force_keep", "force_reject"}:
            raise ValueError(f"Invalid manual override for {clip_id}")
        if entry.override == "force_keep":
            clip = clips_by_id.get(clip_id)
            if clip is not None and "missing_audio_file" in clip.qc_reason_codes:
                raise ValueError(f"Cannot force_keep clip with missing audio: {clip_id}")
        overrides[clip_id] = entry.override
    return overrides


def finalize_dataset_qc(repository: Any, run_id: str, request: DatasetQcFinalizeRequest) -> DatasetQcFinalizeResponse:
    payload = get_dataset_qc(repository, run_id)
    if not payload.ready:
        parts: list[str] = []
        if payload.missing_artifacts:
            parts.append(f"missing: {', '.join(payload.missing_artifacts)}")
        if payload.invalid_artifacts:
            parts.append(f"invalid: {', '.join(payload.invalid_artifacts)}")
        detail = "; ".join(parts) or "required QC artifacts"
        raise ValueError(f"QC is not ready; {detail}")

    known_clip_ids = {clip.clip_id for clip in payload.clips}
    clips_by_id = {clip.clip_id: clip for clip in payload.clips}
    overrides = _validate_manual_overrides(request, known_clip_ids, clips_by_id)
    transcript_threshold = request.thresholds.transcript_match_min
    speaker_threshold = request.thresholds.speaker_check_min
    with Session(repository.engine) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        root = _run_root(repository, run)
        transcript_rows = _clip_rows_from_artifact(
            _read_artifact_payload(root / TRANSCRIPT_QC_REL),
            artifact_name=TRANSCRIPT_QC_REL,
        )
        speaker_rows = _clip_rows_from_artifact(
            _read_artifact_payload(root / SPEAKER_PURITY_REL),
            artifact_name=SPEAKER_PURITY_REL,
        )
        score_methods = _dataset_qc_score_methods(
            _index_rows_by_clip_id(transcript_rows, artifact_name=TRANSCRIPT_QC_REL),
            _index_rows_by_clip_id(speaker_rows, artifact_name=SPEAKER_PURITY_REL),
        )

    now = utc_now().isoformat()
    clip_rows: list[dict[str, Any]] = []
    manual_override_rows: list[dict[str, Any]] = []
    accepted_count = 0
    rejected_count = 0
    accepted_duration_sec = 0.0
    rejected_duration_sec = 0.0
    failure_counts = {"transcript": 0, "speaker": 0, "both": 0}
    reason_counts: dict[str, int] = {}
    manual_force_keep_count = 0
    manual_force_reject_count = 0

    for clip in payload.clips:
        manual_override = overrides.get(clip.clip_id)
        audio_missing = "missing_audio_file" in clip.qc_reason_codes
        threshold_status = _threshold_status(
            transcript_match=clip.transcript_match,
            speaker_check=clip.speaker_check,
            transcript_threshold=transcript_threshold,
            speaker_threshold=speaker_threshold,
            audio_missing=audio_missing,
        )
        hard_failed = audio_missing
        status = _final_status(
            threshold_status=threshold_status,
            manual_override=manual_override,
            hard_failed=hard_failed,
        )
        failed_checks = _failed_checks(
            transcript_match=clip.transcript_match,
            speaker_check=clip.speaker_check,
            transcript_threshold=transcript_threshold,
            speaker_threshold=speaker_threshold,
            audio_missing=audio_missing,
        )
        if status == "accepted":
            accepted_count += 1
            accepted_duration_sec += clip.duration_sec
        else:
            rejected_count += 1
            rejected_duration_sec += clip.duration_sec

        if manual_override == "force_keep" and status == "accepted":
            manual_force_keep_count += 1
            manual_override_rows.append(
                {
                    "clip_id": clip.clip_id,
                    "override": "force_keep",
                    "reason": next(
                        (entry.reason for entry in request.manual_overrides if entry.clip_id == clip.clip_id),
                        "user_listened_and_accepted",
                    ),
                    "updated_at": now,
                    "source": "qc_page",
                }
            )
        elif manual_override == "force_reject":
            manual_force_reject_count += 1
            manual_override_rows.append(
                {
                    "clip_id": clip.clip_id,
                    "override": "force_reject",
                    "reason": next(
                        (entry.reason for entry in request.manual_overrides if entry.clip_id == clip.clip_id),
                        "user_listened_and_rejected",
                    ),
                    "updated_at": now,
                    "source": "qc_page",
                }
            )

        transcript_failed = clip.transcript_match is None or (
            clip.transcript_match is not None and clip.transcript_match < transcript_threshold
        )
        speaker_failed = clip.speaker_check is None or (
            clip.speaker_check is not None and clip.speaker_check < speaker_threshold
        )
        if transcript_failed and speaker_failed:
            failure_counts["both"] += 1
        elif transcript_failed:
            failure_counts["transcript"] += 1
        elif speaker_failed:
            failure_counts["speaker"] += 1
        combined_reason_codes = _combined_reason_codes(clip, failed_checks)
        for code in combined_reason_codes:
            reason_counts[code] = reason_counts.get(code, 0) + 1

        clip_rows.append(
            {
                "clip_id": clip.clip_id,
                "audio_path": clip.audio_path,
                "duration_sec": clip.duration_sec,
                "training_text": clip.training_text,
                "transcript_match": clip.transcript_match,
                "speaker_check": clip.speaker_check,
                "status": status,
                "threshold_status": threshold_status,
                "manual_override": manual_override,
                "failed_checks": failed_checks,
                "reason_codes": combined_reason_codes,
            }
        )

    dataset_qc = {
        "schema_version": QC_SCHEMA_VERSION,
        "stage": "dataset_qc",
        "created_at": now,
        "updated_at": now,
        "thresholds": {
            "transcript_match_min": transcript_threshold,
            "speaker_check_min": speaker_threshold,
        },
        "score_methods": dict(score_methods),
        "manual_overrides": manual_override_rows,
        "clips": clip_rows,
    }
    summary = {
        "schema_version": QC_SCHEMA_VERSION,
        "stage": "dataset_qc",
        "created_at": now,
        "updated_at": now,
        "candidate_count": len(clip_rows),
        "accepted_count": accepted_count,
        "rejected_count": rejected_count,
        "manual_force_keep_count": manual_force_keep_count,
        "manual_force_reject_count": manual_force_reject_count,
        "total_duration_sec": round(accepted_duration_sec + rejected_duration_sec, 6),
        "accepted_duration_sec": round(accepted_duration_sec, 6),
        "rejected_duration_sec": round(rejected_duration_sec, 6),
        "thresholds": {
            "transcript_match_min": transcript_threshold,
            "speaker_check_min": speaker_threshold,
        },
        "failure_counts": failure_counts,
        "reason_counts": reason_counts,
    }

    with Session(repository.engine, expire_on_commit=False) as session:
        run = session.get(ProcessingRun, run_id)
        if run is None:
            raise KeyError("Dataset run not found")
        root = _run_root(repository, run)
        existing_path = root / DATASET_QC_REL
        if existing_path.exists():
            existing = _read_json_object(existing_path)
            if existing.get("created_at"):
                dataset_qc["created_at"] = existing["created_at"]
        artifacts_dir = root / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        (root / DATASET_QC_REL).write_text(json.dumps(dataset_qc, indent=2, sort_keys=True), encoding="utf-8")
        (root / DATASET_QC_SUMMARY_REL).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        _clear_export_artifacts(root)
        _index_artifacts(session, repository, run)
        session.commit()

    return DatasetQcFinalizeResponse(
        run_id=run_id,
        dataset_qc_path=DATASET_QC_REL,
        summary=DatasetQcFinalizeSummaryView(
            accepted_count=accepted_count,
            rejected_count=rejected_count,
            accepted_duration_sec=round(accepted_duration_sec, 6),
            rejected_duration_sec=round(rejected_duration_sec, 6),
        ),
    )
