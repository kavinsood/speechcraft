from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from .io import read_json, read_jsonl, resolve_under_root, sha256_file, write_json


def sec_to_sample(seconds: float, sample_rate: int) -> int:
    return int(round(seconds * sample_rate))


def read_analysis_audio(path: Path) -> tuple[Any, int]:
    import numpy as np
    import soundfile as sf

    samples, sample_rate = sf.read(str(path), dtype="float32", always_2d=False)
    if not isinstance(samples, np.ndarray):
        samples = np.asarray(samples, dtype=np.float32)
    if samples.ndim != 1:
        raise ValueError(f"Processing buffer source must be mono: {path}")
    return samples.astype(np.float32, copy=False), int(sample_rate)


def write_pcm16_mono(path: Path, samples: Any, sample_rate: int) -> None:
    import soundfile as sf

    path.parent.mkdir(parents=True, exist_ok=True)
    sf.write(str(path), samples, sample_rate, subtype="PCM_16")


def frame_rms_db(
    samples: Any,
    sample_rate: int,
    *,
    frame_sec: float = 0.020,
    hop_sec: float = 0.010,
) -> tuple[Any, Any]:
    # Intended for bounded split-search windows, not whole-file RMS over long podcasts.
    import numpy as np

    frame = sec_to_sample(frame_sec, sample_rate)
    hop = sec_to_sample(hop_sec, sample_rate)
    if len(samples) < frame:
        return np.asarray([], dtype=np.int64), np.asarray([], dtype=np.float32)
    starts = np.arange(0, len(samples) - frame + 1, hop, dtype=np.int64)
    windows = np.lib.stride_tricks.sliding_window_view(samples, frame)[::hop]
    rms = np.sqrt(np.mean(np.square(windows, dtype=np.float64), axis=1))
    levels = (20.0 * np.log10(np.maximum(rms, 1e-10))).astype(np.float32)
    if len(levels) >= 3:
        levels = np.convolve(levels, np.ones(3, dtype=np.float32) / 3.0, mode="same")
    return starts + frame // 2, levels


def vad_speech_intervals(vad_segments: list[dict[str, Any]], source_audio_id: str) -> list[tuple[int, int]]:
    intervals: list[tuple[int, int]] = []
    for segment in vad_segments:
        if str(segment.get("source_audio_id")) != source_audio_id:
            continue
        start = int(segment.get("analysis_start_sample", segment.get("start_sample", 0)))
        end = int(segment.get("analysis_end_sample", segment.get("end_sample", 0)))
        if end > start:
            intervals.append((start, end))
    return sorted(intervals)


def overlaps(start_a: int, end_a: int, start_b: int, end_b: int) -> bool:
    return start_a < end_b and start_b < end_a


def has_non_target_intrusion(gap_start: int, gap_end: int, non_target_regions: list[dict[str, Any]]) -> bool:
    if gap_end <= gap_start:
        return False
    return any(overlaps(gap_start, gap_end, int(row["start_sample"]), int(row["end_sample"])) for row in non_target_regions)


def merge_target_regions(
    speaker_regions: list[dict[str, Any]],
    *,
    source_audio_id: str,
    target_speaker_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source_rows = [
        row
        for row in speaker_regions
        if str(row.get("source_audio_id")) == source_audio_id and int(row.get("end_sample", 0)) > int(row.get("start_sample", 0))
    ]
    target_rows = sorted(
        [row for row in source_rows if str(row.get("speaker_id")) == target_speaker_id],
        key=lambda row: int(row["start_sample"]),
    )
    non_target_rows = [row for row in source_rows if str(row.get("speaker_id")) != target_speaker_id]

    merged: list[dict[str, Any]] = []
    clean_gap_merges = 0
    non_target_blocked_merges = 0
    for row in target_rows:
        start_sample = int(row["start_sample"])
        end_sample = int(row["end_sample"])
        if not merged:
            merged.append(
                {
                    "trusted_region_id": f"{source_audio_id}_trusted_region_{len(merged):06d}",
                    "start_sample": start_sample,
                    "end_sample": end_sample,
                    "included_region_ids": [str(row["id"])],
                }
            )
            continue
        previous = merged[-1]
        if not has_non_target_intrusion(int(previous["end_sample"]), start_sample, non_target_rows):
            previous["end_sample"] = max(int(previous["end_sample"]), end_sample)
            previous["included_region_ids"].append(str(row["id"]))
            clean_gap_merges += 1
        else:
            non_target_blocked_merges += 1
            merged.append(
                {
                    "trusted_region_id": f"{source_audio_id}_trusted_region_{len(merged):06d}",
                    "start_sample": start_sample,
                    "end_sample": end_sample,
                    "included_region_ids": [str(row["id"])],
                }
            )

    return merged, {
        "target_regions": len(target_rows),
        "non_target_regions": len(non_target_rows),
        "clean_gap_merges": clean_gap_merges,
        "non_target_blocked_merges": non_target_blocked_merges,
    }


def vad_gaps_in_range(start_sample: int, end_sample: int, speech_intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    cursor = start_sample
    gaps: list[tuple[int, int]] = []
    for speech_start, speech_end in speech_intervals:
        if speech_end <= start_sample:
            continue
        if speech_start >= end_sample:
            break
        clipped_start = max(speech_start, start_sample)
        clipped_end = min(speech_end, end_sample)
        if clipped_start > cursor:
            gaps.append((cursor, clipped_start))
        cursor = max(cursor, clipped_end)
    if cursor < end_sample:
        gaps.append((cursor, end_sample))
    return gaps


def padded_duration_samples(chunk_start: int, chunk_end: int, audio_len: int, pad_samples: int) -> int:
    processing_start = max(0, chunk_start - pad_samples)
    processing_end = min(audio_len, chunk_end + pad_samples)
    return processing_end - processing_start


def latest_feasible_split_sample(
    chunk_start: int,
    region_end: int,
    audio_len: int,
    *,
    pad_samples: int,
    max_buffer_samples: int,
) -> int:
    low = chunk_start + 1
    high = min(region_end, audio_len)
    while low < high:
        candidate = (low + high + 1) // 2
        if padded_duration_samples(chunk_start, candidate, audio_len, pad_samples) < max_buffer_samples:
            low = candidate
        else:
            high = candidate - 1
    return low


def quietest_sample(samples: Any, start_sample: int, end_sample: int, sample_rate: int) -> int | None:
    import numpy as np

    centers, levels = frame_rms_db(samples[start_sample:end_sample], sample_rate)
    if len(centers) == 0:
        return None
    return start_sample + int(centers[int(np.argmin(levels))])


def choose_split_sample(
    *,
    chunk_start: int,
    region_end: int,
    speech_intervals: list[tuple[int, int]],
    samples: Any,
    sample_rate: int,
    target_chunk_samples: int,
    min_split_gap_samples: int,
    pad_samples: int,
    max_buffer_samples: int,
) -> tuple[int, str, list[str]]:
    desired = min(chunk_start + target_chunk_samples, region_end)
    for suffix, before_sec, after_sec in (
        ("near_target", 2.0, 2.0),
        ("widened_4s", 4.0, 4.0),
        ("widened_8s", 8.0, 4.0),
    ):
        search_start = max(chunk_start, desired - sec_to_sample(before_sec, sample_rate))
        search_end = min(region_end, desired + sec_to_sample(after_sec, sample_rate))
        gaps = [
            gap
            for gap in vad_gaps_in_range(search_start, search_end, speech_intervals)
            if gap[1] - gap[0] >= min_split_gap_samples
        ]
        if not gaps:
            continue
        containing = [gap for gap in gaps if gap[0] <= desired <= gap[1]]
        candidates = containing or gaps
        best_start, best_end = sorted(
            candidates,
            key=lambda gap: (
                0 if gap[0] <= desired <= gap[1] else min(abs(desired - gap[0]), abs(desired - gap[1])),
                -(gap[1] - gap[0]),
            ),
        )[0]
        valley = quietest_sample(samples, best_start, best_end, sample_rate)
        if valley is not None:
            return valley, f"vad_gap_rms_valley_{suffix}", []
        return (best_start + best_end) // 2, f"vad_gap_midpoint_fallback_{suffix}", ["missing_rms_frame_in_vad_gap"]

    forced_start = chunk_start + sec_to_sample(15.0, sample_rate)
    forced_end = latest_feasible_split_sample(
        chunk_start,
        region_end,
        len(samples),
        pad_samples=pad_samples,
        max_buffer_samples=max_buffer_samples,
    )
    valley = quietest_sample(samples, forced_start, forced_end, sample_rate)
    if valley is not None:
        return valley, "forced_rms_valley_full_feasible_range", ["provisional_pre_mfa_split", "forced_chunk_split"]
    return desired, "forced_target_fallback", [
        "provisional_pre_mfa_split",
        "forced_chunk_split",
        "missing_rms_frame_for_forced_split",
    ]


def trusted_regions_for_single_speaker(
    speech_intervals: list[tuple[int, int]],
    audio_len: int,
    *,
    allow_no_vad_full_span_fallback: bool,
) -> list[dict[str, Any]]:
    if not speech_intervals:
        if not allow_no_vad_full_span_fallback:
            return []
        return [{"trusted_region_id": "trusted_region_000000", "start_sample": 0, "end_sample": audio_len}]
    return [
        {
            "trusted_region_id": "trusted_region_000000",
            "start_sample": max(0, speech_intervals[0][0]),
            "end_sample": min(audio_len, speech_intervals[-1][1]),
        }
    ]


def split_trusted_regions(
    trusted_regions: list[dict[str, Any]],
    speech_intervals: list[tuple[int, int]],
    samples: Any,
    sample_rate: int,
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    target_chunk_samples = sec_to_sample(float(config.get("target_processing_chunk_sec", 24.0)), sample_rate)
    min_split_gap_samples = sec_to_sample(float(config.get("min_split_gap_sec", 0.12)), sample_rate)
    pad_samples = sec_to_sample(float(config.get("processing_buffer_pad_sec", 0.5)), sample_rate)
    max_buffer_samples = sec_to_sample(float(config.get("max_processing_buffer_sec", 29.5)), sample_rate)
    for region in trusted_regions:
        cursor = int(region["start_sample"])
        left_provisional = False
        while int(region["end_sample"]) - cursor > target_chunk_samples:
            split_sample, split_strategy, reason_codes = choose_split_sample(
                chunk_start=cursor,
                region_end=int(region["end_sample"]),
                speech_intervals=speech_intervals,
                samples=samples,
                sample_rate=sample_rate,
                target_chunk_samples=target_chunk_samples,
                min_split_gap_samples=min_split_gap_samples,
                pad_samples=pad_samples,
                max_buffer_samples=max_buffer_samples,
            )
            if split_sample <= cursor:
                split_sample = min(int(region["end_sample"]), cursor + target_chunk_samples)
                split_strategy = "forced_target_fallback"
                reason_codes = ["provisional_pre_mfa_split", "forced_chunk_split", "non_advancing_split_guard"]
            chunks.append(
                {
                    "trusted_region_id": region["trusted_region_id"],
                    "trusted_start_sample": cursor,
                    "trusted_end_sample": split_sample,
                    "split_strategy": split_strategy,
                    "reason_codes": reason_codes,
                    "left_provisional_boundary": left_provisional,
                    "right_provisional_boundary": "provisional_pre_mfa_split" in reason_codes,
                }
            )
            cursor = split_sample
            left_provisional = "provisional_pre_mfa_split" in reason_codes
        if int(region["end_sample"]) > cursor:
            chunks.append(
                {
                    "trusted_region_id": region["trusted_region_id"],
                    "trusted_start_sample": cursor,
                    "trusted_end_sample": int(region["end_sample"]),
                    "split_strategy": "tail" if cursor != int(region["start_sample"]) else "whole_region",
                    "reason_codes": [],
                    "left_provisional_boundary": left_provisional,
                    "right_provisional_boundary": False,
                }
            )
    return chunks


def run_processing_buffers(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    audio_variants_manifest_path = resolve_under_root(run_root, "artifacts/audio_variants_manifest.json")
    vad_segments_path = resolve_under_root(run_root, "artifacts/vad_segments.jsonl")
    speaker_regions_path = resolve_under_root(run_root, "artifacts/speaker_regions.jsonl")
    speaker_selection_path = resolve_under_root(run_root, "artifacts/speaker_selection.json")
    variants = list(read_json(audio_variants_manifest_path).get("variants") or [])
    vad_segments = read_jsonl(vad_segments_path)
    speaker_regions = read_jsonl(speaker_regions_path)
    selection = read_json(speaker_selection_path)
    pad_sec = float(config.get("processing_buffer_pad_sec", 0.5))
    max_buffer_sec = float(config.get("max_processing_buffer_sec", 29.5))
    allow_no_vad_full_span_fallback = bool(config.get("allow_no_vad_full_span_fallback", False))
    mode = str(config.get("mode") or "single_speaker")
    target_speaker_id = str(selection.get("target_speaker_id") or "").strip()
    if mode == "diarization" and not bool(selection.get("selected")):
        raise ValueError("speaker_selection.json does not select a target speaker")
    if not target_speaker_id:
        raise ValueError("speaker_selection.json is missing target_speaker_id")
    rows: list[dict[str, Any]] = []
    skipped_sources: list[dict[str, Any]] = []
    trusted_region_totals = Counter()

    for variant in variants:
        source_audio_id = str(variant["source_audio_id"])
        analysis_path = resolve_under_root(run_root, str(variant["path"]))
        samples, sample_rate = read_analysis_audio(analysis_path)
        expected_sample_rate = int(config.get("analysis_sample_rate") or 16000)
        if sample_rate != expected_sample_rate:
            raise ValueError(
                f"Analysis sample-rate mismatch for {source_audio_id}: {sample_rate} != {expected_sample_rate}"
            )
        speech_intervals = vad_speech_intervals(vad_segments, source_audio_id)
        trusted_regions, trusted_summary = merge_target_regions(
            speaker_regions,
            source_audio_id=source_audio_id,
            target_speaker_id=target_speaker_id,
        )
        trusted_region_totals.update(trusted_summary)
        if not trusted_regions and mode == "single_speaker":
            trusted_regions = trusted_regions_for_single_speaker(
                speech_intervals,
                len(samples),
                allow_no_vad_full_span_fallback=allow_no_vad_full_span_fallback,
            )
        if not trusted_regions:
            skipped_sources.append(
                {
                    "source_audio_id": source_audio_id,
                    "reason_codes": ["no_target_speaker_regions_detected" if mode == "diarization" else "no_speech_detected"],
                }
            )
            continue
        chunks = split_trusted_regions(trusted_regions, speech_intervals, samples, sample_rate, config)
        pad_samples = sec_to_sample(pad_sec, sample_rate)
        max_buffer_samples = sec_to_sample(max_buffer_sec, sample_rate)
        previous_end: int | None = None
        for chunk in chunks:
            if previous_end is not None and int(chunk["trusted_start_sample"]) < previous_end:
                raise RuntimeError("trusted chunks overlap")
            previous_end = int(chunk["trusted_end_sample"])

            trusted_start = int(chunk["trusted_start_sample"])
            trusted_end = int(chunk["trusted_end_sample"])
            source_start = max(0, trusted_start - pad_samples)
            source_end = min(len(samples), trusted_end + pad_samples)
            duration_samples = source_end - source_start
            if duration_samples >= max_buffer_samples:
                raise RuntimeError(f"processing buffer too long: {duration_samples / sample_rate:.3f}s")
            buffer_id = f"buffer_{len(rows):06d}"
            rel_audio_path = f"artifacts/buffers/{buffer_id}.wav"
            audio_path = resolve_under_root(run_root, rel_audio_path)
            write_pcm16_mono(audio_path, samples[source_start:source_end], sample_rate)
            rows.append(
                {
                    "buffer_id": buffer_id,
                    "source_audio_id": source_audio_id,
                    "analysis_audio_path": variant["path"],
                    "audio_path": rel_audio_path,
                    "content_hash": sha256_file(audio_path),
                    "source_start_sample": source_start,
                    "source_end_sample": source_end,
                    "source_start_sec": round(source_start / sample_rate, 6),
                    "source_end_sec": round(source_end / sample_rate, 6),
                    "trusted_start_sample": trusted_start,
                    "trusted_end_sample": trusted_end,
                    "trusted_start_sec": round(trusted_start / sample_rate, 6),
                    "trusted_end_sec": round(trusted_end / sample_rate, 6),
                    "trusted_local_start_sample": trusted_start - source_start,
                    "trusted_local_end_sample": trusted_end - source_start,
                    "duration_samples": duration_samples,
                    "duration_sec": round(duration_samples / sample_rate, 6),
                    "sample_rate": sample_rate,
                    "split_strategy": chunk["split_strategy"],
                    "reason_codes": chunk["reason_codes"],
                    "left_provisional_boundary": chunk["left_provisional_boundary"],
                    "right_provisional_boundary": chunk["right_provisional_boundary"],
                    "target_speaker_id": target_speaker_id,
                }
            )

    durations = sorted(float(row["duration_sec"]) for row in rows)
    reason_counts = Counter(reason for row in rows for reason in row["reason_codes"])
    strategy_counts = Counter(str(row["split_strategy"]) for row in rows)
    buffers_path = resolve_under_root(run_root, "artifacts/processing_buffers.json")
    write_json(buffers_path, rows)
    summary = {
        "stage": "processing_buffers",
        "config_hash": str(config.get("config_hash") or ""),
        "input_artifact_hashes": {
            "audio_variants_manifest": sha256_file(audio_variants_manifest_path),
            "vad_segments_jsonl": sha256_file(vad_segments_path),
            "speaker_regions_jsonl": sha256_file(speaker_regions_path),
            "speaker_selection_json": sha256_file(speaker_selection_path),
        },
        "output_hashes": {
            "processing_buffers_json": sha256_file(buffers_path),
            "buffer_wavs": {row["buffer_id"]: row["content_hash"] for row in rows},
        },
        "buffer_count": len(rows),
        "skipped_source_count": len(skipped_sources),
        "skipped_sources": skipped_sources,
        "total_duration_sec": round(sum(durations), 6),
        "max_duration_sec": max(durations) if durations else 0.0,
        "min_duration_sec": min(durations) if durations else 0.0,
        "buffers_under_max": all(duration < max_buffer_sec for duration in durations),
        "split_strategy_counts": dict(strategy_counts),
        "reason_code_counts": dict(reason_counts),
        "allow_no_vad_full_span_fallback": allow_no_vad_full_span_fallback,
        "mode": mode,
        "target_speaker_id": target_speaker_id,
        "trusted_region_summary": dict(trusted_region_totals),
    }
    write_json(resolve_under_root(run_root, "artifacts/processing_buffer_summary.json"), summary)
    return summary
