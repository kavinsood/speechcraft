from __future__ import annotations

import argparse
import json
import wave
from pathlib import Path
from typing import Any

import numpy as np
from sqlmodel import Session, delete, select

from app.models import (
    AudioVariant,
    EditCommit,
    ExportRun,
    ImportBatch,
    ProcessingJob,
    Slice,
    SliceTagLink,
    SourceRecording,
    Transcript,
)
from app.repository import SQLiteRepository

DEFAULT_REVIEW_CONTEXT_SECONDS = 0.06
DEFAULT_REVIEW_TAIL_SECONDS = 0.08
BREATH_REVIEW_CONTEXT_SECONDS = 0.18
WORD_EDGE_MARGIN_SECONDS = 0.015
REVIEW_EDGE_SCAN_WINDOW_SECONDS = 0.02
REVIEW_EDGE_MAX_SAMPLE_SECONDS = 0.003
REVIEW_EDGE_ENERGY_SPIKE_RATIO = 4.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import provisional slicer outputs as normal slices into the live Speechcraft DB."
    )
    parser.add_argument("--batch-id", required=True, help="ImportBatch id to create or replace.")
    parser.add_argument("--batch-name", required=True, help="Human-readable project name shown in the UI.")
    parser.add_argument("--audio-dir", required=True, help="Directory containing the source WAV files.")
    parser.add_argument("--slicer-dir", required=True, help="Directory containing one slicer result JSON per WAV stem.")
    parser.add_argument(
        "--bounds-mode",
        choices=("raw", "snapped", "training", "context"),
        default="training",
        help="Which slicer timestamps to use for imported clip audio bounds.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Delete any existing batch with the same id before importing.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repository = SQLiteRepository()
    audio_dir = Path(args.audio_dir).expanduser().resolve()
    slicer_dir = Path(args.slicer_dir).expanduser().resolve()

    if not audio_dir.is_dir():
        raise SystemExit(f"Audio directory not found: {audio_dir}")
    if not slicer_dir.is_dir():
        raise SystemExit(f"Slicer directory not found: {slicer_dir}")

    with repository._session() as session:
        existing_batch = session.get(ImportBatch, args.batch_id)
        if existing_batch is not None:
            if not args.replace_existing:
                raise SystemExit(
                    f"Batch {args.batch_id!r} already exists. Rerun with --replace-existing to overwrite it."
                )
            delete_batch(session, repository, args.batch_id)

        batch = ImportBatch(id=args.batch_id, name=args.batch_name)
        session.add(batch)
        session.flush()

        next_order_index = 0
        imported_recordings = 0
        imported_slices = 0
        audio_cache: dict[str, tuple[np.ndarray, int]] = {}

        for slicer_path in sorted(slicer_dir.glob("*.json")):
            stem = slicer_path.stem
            audio_path = audio_dir / f"{stem}.wav"
            if not audio_path.exists():
                raise SystemExit(f"Missing WAV for slicer result {slicer_path.name}: {audio_path}")

            sample_rate, channels, frames = wav_metadata(audio_path)
            recording_duration = frames / sample_rate if sample_rate > 0 else 0.0
            recording = SourceRecording(
                id=f"{args.batch_id}-src-{stem}",
                batch_id=batch.id,
                file_path=str(audio_path),
                sample_rate=sample_rate,
                num_channels=channels,
                num_samples=frames,
                processing_recipe="temp_fullwav_slicer_eval",
            )
            session.add(recording)
            session.flush()
            imported_recordings += 1

            payload = json.loads(slicer_path.read_text(encoding="utf-8"))
            slices = payload.get("slices") or []
            if not isinstance(slices, list):
                raise SystemExit(f"Invalid slicer payload in {slicer_path}")

            for index, slice_payload in enumerate(slices):
                previous_slice = slices[index - 1] if index > 0 else None
                next_slice = slices[index + 1] if index + 1 < len(slices) else None
                start_seconds, end_seconds = resolve_slice_bounds(
                    slice_payload,
                    args.bounds_mode,
                    recording_duration=recording_duration,
                    audio_path=audio_path,
                    audio_cache=audio_cache,
                    previous_slice=previous_slice,
                    next_slice=next_slice,
                )
                transcript_text = str(slice_payload.get("transcript") or "").strip()
                if end_seconds <= start_seconds or not transcript_text:
                    continue

                alignment_data = build_alignment_payload(slice_payload, stem)
                slice_row = repository._create_slice_from_source_span(
                    session,
                    recording,
                    slice_id=repository._new_id("slice"),
                    start_seconds=start_seconds,
                    end_seconds=end_seconds,
                    transcript_text=transcript_text,
                    order_index=next_order_index,
                    alignment_data=alignment_data,
                )
                slice_row.model_metadata = {
                    **dict(slice_row.model_metadata or {}),
                    "temp_import_batch_id": batch.id,
                    "temp_import_source_stem": stem,
                    "temp_import_kind": "fullwav_slicer_eval",
                    "slicer_boundary_type": slice_payload.get("boundary_type"),
                    "slicer_forced_cut": bool(slice_payload.get("forced_cut")),
                    "slicer_flag_reason": slice_payload.get("flag_reason"),
                    "slicer_manifest_path": str(slicer_path),
                }
                session.add(slice_row)
                next_order_index += 1
                imported_slices += 1

        session.commit()

    print(
        json.dumps(
            {
                "batch_id": args.batch_id,
                "batch_name": args.batch_name,
                "recordings": imported_recordings,
                "slices": imported_slices,
                "db_path": str(repository.db_path),
                "bounds_mode": args.bounds_mode,
            },
            indent=2,
        )
    )


def build_alignment_payload(slice_payload: dict[str, Any], source_stem: str) -> dict[str, Any]:
    return {
        "source": "temp_fullwav_slicer_eval",
        "source_recording_stem": source_stem,
        "confidence": slice_payload.get("avg_alignment_confidence"),
        "boundary_type": slice_payload.get("boundary_type"),
        "boundary_gap_s": slice_payload.get("boundary_gap_s"),
        "raw_start": slice_payload.get("raw_start"),
        "raw_end": slice_payload.get("raw_end"),
        "snapped_start": slice_payload.get("snapped_start"),
        "snapped_end": slice_payload.get("snapped_end"),
        "training_start": slice_payload.get("training_start"),
        "training_end": slice_payload.get("training_end"),
        "training_duration": slice_payload.get("training_duration"),
        "forced_cut": bool(slice_payload.get("forced_cut")),
        "is_flagged": bool(slice_payload.get("is_flagged")),
        "flag_reason": slice_payload.get("flag_reason"),
        "flag_reasons": list(slice_payload.get("flag_reasons") or []),
        "transcript_original": slice_payload.get("transcript_original"),
        "words": list(slice_payload.get("words") or []),
    }


def resolve_slice_bounds(
    slice_payload: dict[str, Any],
    bounds_mode: str,
    *,
    recording_duration: float,
    audio_path: Path,
    audio_cache: dict[str, tuple[np.ndarray, int]],
    previous_slice: dict[str, Any] | None = None,
    next_slice: dict[str, Any] | None = None,
) -> tuple[float, float]:
    if bounds_mode == "raw":
        return clamp_bounds(
            float(slice_payload["raw_start"]),
            float(slice_payload["raw_end"]),
            recording_duration,
        )
    if bounds_mode == "training":
        return clamp_bounds(
            float(slice_payload["training_start"]),
            float(slice_payload["training_end"]),
            recording_duration,
        )
    if bounds_mode == "context":
        return clamp_bounds(
            *resolve_context_bounds(
            slice_payload,
            audio_path=audio_path,
            audio_cache=audio_cache,
            previous_slice=previous_slice,
            next_slice=next_slice,
        ),
            recording_duration,
        )
    return clamp_bounds(
        float(slice_payload.get("snapped_start", slice_payload["training_start"])),
        float(slice_payload.get("snapped_end", slice_payload["training_end"])),
        recording_duration,
    )


def clamp_bounds(start_seconds: float, end_seconds: float, recording_duration: float) -> tuple[float, float]:
    start_seconds = max(0.0, min(start_seconds, recording_duration))
    end_seconds = max(0.0, min(end_seconds, recording_duration))
    if end_seconds < start_seconds:
        end_seconds = start_seconds
    return start_seconds, end_seconds


def resolve_context_bounds(
    slice_payload: dict[str, Any],
    *,
    audio_path: Path,
    audio_cache: dict[str, tuple[np.ndarray, int]],
    previous_slice: dict[str, Any] | None,
    next_slice: dict[str, Any] | None,
) -> tuple[float, float]:
    raw_start = float(slice_payload["raw_start"])
    raw_end = float(slice_payload["raw_end"])
    training_start = float(slice_payload["training_start"])
    training_end = float(slice_payload["training_end"])

    breath_at_start = bool(slice_payload.get("breath_at_start"))
    breath_at_end = bool(slice_payload.get("breath_at_end"))
    start_context = BREATH_REVIEW_CONTEXT_SECONDS if breath_at_start else DEFAULT_REVIEW_CONTEXT_SECONDS
    end_context = BREATH_REVIEW_CONTEXT_SECONDS if breath_at_end else DEFAULT_REVIEW_TAIL_SECONDS
    start_anchor = training_start
    end_anchor = training_end

    start_seconds = start_anchor - start_context
    end_seconds = end_anchor + end_context

    if previous_slice is not None:
        previous_gap = max(float(previous_slice.get("boundary_gap_s") or 0.0), 0.0)
        previous_word_end = raw_start - previous_gap
        start_seconds = max(start_seconds, previous_word_end + WORD_EDGE_MARGIN_SECONDS)

    if next_slice is not None and str(slice_payload.get("boundary_type") or "") != "end_of_recording":
        next_gap = max(float(slice_payload.get("boundary_gap_s") or 0.0), 0.0)
        next_word_start = raw_end + next_gap
        end_seconds = min(end_seconds, next_word_start - WORD_EDGE_MARGIN_SECONDS)

    if start_seconds < start_anchor:
        start_seconds = _adjust_review_edge(
            anchor_seconds=start_anchor,
            proposed_seconds=start_seconds,
            audio_path=audio_path,
            audio_cache=audio_cache,
            prefer="start",
        )
    if end_seconds > end_anchor:
        end_seconds = _adjust_review_edge(
            anchor_seconds=end_anchor,
            proposed_seconds=end_seconds,
            audio_path=audio_path,
            audio_cache=audio_cache,
            prefer="end",
        )

    start_seconds = min(start_seconds, start_anchor)
    end_seconds = max(end_seconds, end_anchor)
    return start_seconds, end_seconds


def wav_metadata(audio_path: Path) -> tuple[int, int, int]:
    with wave.open(str(audio_path), "rb") as wav_file:
        return wav_file.getframerate(), wav_file.getnchannels(), wav_file.getnframes()


def load_pcm_wav(audio_path: Path) -> tuple[np.ndarray, int]:
    with wave.open(str(audio_path), "rb") as wav_file:
        channels = wav_file.getnchannels()
        sample_rate = wav_file.getframerate()
        frame_count = wav_file.getnframes()
        raw_frames = wav_file.readframes(frame_count)

    audio = np.frombuffer(raw_frames, dtype="<i2").astype(np.float64)
    if frame_count == 0:
        return np.zeros(0, dtype=np.float64), sample_rate
    if channels > 1:
        audio = audio.reshape(frame_count, channels).mean(axis=1)
    return audio / 32768.0, sample_rate


def _cached_audio(
    audio_path: Path,
    audio_cache: dict[str, tuple[np.ndarray, int]],
) -> tuple[np.ndarray, int]:
    key = str(audio_path)
    cached = audio_cache.get(key)
    if cached is not None:
        return cached
    loaded = load_pcm_wav(audio_path)
    audio_cache[key] = loaded
    return loaded


def _adjust_review_edge(
    *,
    anchor_seconds: float,
    proposed_seconds: float,
    audio_path: Path,
    audio_cache: dict[str, tuple[np.ndarray, int]],
    prefer: str,
) -> float:
    audio, sample_rate = _cached_audio(audio_path, audio_cache)
    if len(audio) == 0 or sample_rate <= 0:
        return proposed_seconds
    if proposed_seconds <= anchor_seconds and prefer == "end":
        return anchor_seconds
    if proposed_seconds >= anchor_seconds and prefer == "start":
        return anchor_seconds

    scan_span = abs(proposed_seconds - anchor_seconds)
    if scan_span <= 0:
        return anchor_seconds

    frame_samples = max(1, int(REVIEW_EDGE_MAX_SAMPLE_SECONDS * sample_rate))
    scan_samples = max(1, int(REVIEW_EDGE_SCAN_WINDOW_SECONDS * sample_rate))

    if prefer == "end":
        start_idx = max(0, int(anchor_seconds * sample_rate))
        end_idx = min(len(audio), int(proposed_seconds * sample_rate))
        if end_idx <= start_idx + frame_samples:
            return proposed_seconds
        baseline = _region_rms(audio, start_idx, min(len(audio), start_idx + scan_samples))
        best_idx = end_idx
        best_rms = _region_rms(audio, max(start_idx, end_idx - frame_samples), end_idx)
        limit_rms = max(baseline * REVIEW_EDGE_ENERGY_SPIKE_RATIO, baseline + 1e-5)
        for idx in range(start_idx + frame_samples, end_idx + 1):
            rms = _region_rms(audio, idx - frame_samples, idx)
            if rms < best_rms:
                best_rms = rms
                best_idx = idx
            if rms > limit_rms and idx > start_idx + frame_samples:
                break
        return _nearest_zero_crossing_time(audio, sample_rate, best_idx, start_idx, end_idx)

    start_idx = max(0, int(proposed_seconds * sample_rate))
    end_idx = min(len(audio), int(anchor_seconds * sample_rate))
    if end_idx <= start_idx + frame_samples:
        return proposed_seconds
    baseline = _region_rms(audio, max(0, end_idx - scan_samples), end_idx)
    best_idx = start_idx
    best_rms = _region_rms(audio, start_idx, min(len(audio), start_idx + frame_samples))
    limit_rms = max(baseline * REVIEW_EDGE_ENERGY_SPIKE_RATIO, baseline + 1e-5)
    for idx in range(end_idx - frame_samples, start_idx - 1, -1):
        rms = _region_rms(audio, idx, idx + frame_samples)
        if rms < best_rms:
            best_rms = rms
            best_idx = idx
        if rms > limit_rms and idx < end_idx - frame_samples:
            break
    return _nearest_zero_crossing_time(audio, sample_rate, best_idx, start_idx, end_idx)


def _region_rms(audio: np.ndarray, start_idx: int, end_idx: int) -> float:
    start_idx = max(0, min(start_idx, len(audio)))
    end_idx = max(start_idx + 1, min(end_idx, len(audio)))
    region = audio[start_idx:end_idx]
    if len(region) == 0:
        return 0.0
    return float(np.sqrt(np.mean(region**2)))


def _nearest_zero_crossing_time(
    audio: np.ndarray,
    sample_rate: int,
    center_idx: int,
    lo_idx: int,
    hi_idx: int,
) -> float:
    center_idx = max(lo_idx, min(center_idx, hi_idx - 1))
    for offset in range(0, max(center_idx - lo_idx, hi_idx - center_idx) + 1):
        for candidate in (center_idx - offset, center_idx + offset):
            if candidate <= lo_idx or candidate >= hi_idx:
                continue
            prev = audio[candidate - 1]
            cur = audio[candidate]
            if prev == 0 or cur == 0 or (prev < 0 <= cur) or (prev > 0 >= cur):
                return candidate / sample_rate
    return center_idx / sample_rate


def delete_batch(session: Session, repository: SQLiteRepository, batch_id: str) -> None:
    batch = session.get(ImportBatch, batch_id)
    if batch is None:
        return

    recording_ids = session.exec(select(SourceRecording.id).where(SourceRecording.batch_id == batch_id)).all()
    slices = session.exec(select(Slice).where(Slice.source_recording_id.in_(recording_ids))).all() if recording_ids else []
    deleted_paths = [variant.file_path for slice_row in slices for variant in slice_row.variants]
    slice_ids = [slice_row.id for slice_row in slices]

    if slice_ids:
        session.exec(delete(SliceTagLink).where(SliceTagLink.slice_id.in_(slice_ids)))
        session.exec(delete(EditCommit).where(EditCommit.slice_id.in_(slice_ids)))
        session.exec(delete(Transcript).where(Transcript.slice_id.in_(slice_ids)))
        session.exec(delete(AudioVariant).where(AudioVariant.slice_id.in_(slice_ids)))
        session.exec(delete(Slice).where(Slice.id.in_(slice_ids)))
    if recording_ids:
        session.exec(delete(ProcessingJob).where(ProcessingJob.source_recording_id.in_(recording_ids)))
        session.exec(delete(SourceRecording).where(SourceRecording.id.in_(recording_ids)))
    session.exec(delete(ExportRun).where(ExportRun.batch_id == batch_id))
    session.exec(delete(ImportBatch).where(ImportBatch.id == batch_id))
    session.commit()
    repository._delete_unreferenced_media_files(deleted_paths)
    repository._prune_derived_media_cache()


if __name__ == "__main__":
    main()
