from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any

from .buffers import frame_rms_db, read_analysis_audio, write_pcm16_mono
from .io import read_json, read_json_value, read_jsonl, resolve_under_root


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def _ms_from_samples(samples: int, sample_rate: int) -> float:
    return (samples / sample_rate) * 1000.0


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    try:
        import numpy as np

        return round(float(np.percentile(values, quantile)), 6)
    except Exception:
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, int(round((quantile / 100.0) * (len(ordered) - 1)))))
        return round(float(ordered[index]), 6)


def _overlaps(start_a: int, end_a: int, start_b: int, end_b: int) -> bool:
    return start_a < end_b and start_b < end_a


def _interval_distance_to_center(sample: int, start: int, end: int) -> int:
    center = start + ((end - start) // 2)
    return abs(sample - center)


def _clip_interval(start: int, end: int, clip_start: int, clip_end: int) -> tuple[int, int] | None:
    clipped_start = max(start, clip_start)
    clipped_end = min(end, clip_end)
    if clipped_end <= clipped_start:
        return None
    return clipped_start, clipped_end


@dataclass(frozen=True)
class GapRow:
    buffer_id: str
    source_audio_id: str
    local_start_sample: int
    local_end_sample: int
    source_start_sample: int
    source_end_sample: int
    duration_samples: int
    duration_ms: float
    min_rms_dbfs: float | None
    mean_rms_dbfs: float | None
    noise_margin_db: float | None
    kind: str
    left_ref: str | None = None
    right_ref: str | None = None


def _acoustic_stats(samples: Any, sample_rate: int, start: int, end: int) -> tuple[float | None, float | None, float | None]:
    import numpy as np

    _, levels = frame_rms_db(samples[start:end], sample_rate)
    if len(levels) == 0:
        return None, None, None
    min_db = float(np.min(levels))
    mean_db = float(np.mean(levels))
    noise_floor = float(np.percentile(levels, 10))
    return min_db, mean_db, min_db - noise_floor


def _summarize_gap_rows(rows: list[GapRow]) -> dict[str, Any]:
    durations = [row.duration_ms for row in rows]
    min_rms = [row.min_rms_dbfs for row in rows if row.min_rms_dbfs is not None]
    mean_rms = [row.mean_rms_dbfs for row in rows if row.mean_rms_dbfs is not None]
    noise_margin = [row.noise_margin_db for row in rows if row.noise_margin_db is not None]
    return {
        "count": len(rows),
        "duration_ms_p50": _percentile(durations, 50),
        "duration_ms_p90": _percentile(durations, 90),
        "min_rms_dbfs_p50": _percentile(min_rms, 50),
        "mean_rms_dbfs_p50": _percentile(mean_rms, 50),
        "noise_margin_db_p50": _percentile(noise_margin, 50),
    }


def _trusted_words_for_buffer(words: list[dict[str, Any]], buffer_row: dict[str, Any]) -> list[dict[str, Any]]:
    trusted_start = int(buffer_row["trusted_local_start_sample"])
    trusted_end = int(buffer_row["trusted_local_end_sample"])
    return [
        word
        for word in words
        if int(word["local_start_sample"]) >= trusted_start
        and int(word["local_end_sample"]) <= trusted_end
        and int(word["local_end_sample"]) > int(word["local_start_sample"])
    ]


def _vad_gaps_for_buffer(
    vad_segments: list[dict[str, Any]],
    buffer_row: dict[str, Any],
    sample_rate: int,
    analysis_samples: Any,
) -> list[GapRow]:
    source_audio_id = str(buffer_row["source_audio_id"])
    source_start = int(buffer_row["source_start_sample"])
    trusted_start = int(buffer_row["trusted_start_sample"])
    trusted_end = int(buffer_row["trusted_end_sample"])
    intervals: list[tuple[int, int]] = []
    for segment in vad_segments:
        if str(segment.get("source_audio_id")) != source_audio_id:
            continue
        clipped = _clip_interval(
            int(segment.get("analysis_start_sample", segment.get("start_sample", 0))),
            int(segment.get("analysis_end_sample", segment.get("end_sample", 0))),
            trusted_start,
            trusted_end,
        )
        if clipped is not None:
            intervals.append(clipped)
    intervals.sort()

    cursor = trusted_start
    rows: list[GapRow] = []
    for speech_start, speech_end in intervals:
        if speech_start > cursor:
            local_start = cursor - source_start
            local_end = speech_start - source_start
            min_db, mean_db, noise_margin = _acoustic_stats(analysis_samples, sample_rate, local_start, local_end)
            rows.append(
                GapRow(
                    buffer_id=str(buffer_row["buffer_id"]),
                    source_audio_id=source_audio_id,
                    local_start_sample=local_start,
                    local_end_sample=local_end,
                    source_start_sample=cursor,
                    source_end_sample=speech_start,
                    duration_samples=speech_start - cursor,
                    duration_ms=_ms_from_samples(speech_start - cursor, sample_rate),
                    min_rms_dbfs=min_db,
                    mean_rms_dbfs=mean_db,
                    noise_margin_db=noise_margin,
                    kind="vad_gap",
                )
            )
        cursor = max(cursor, speech_end)
    if cursor < trusted_end:
        local_start = cursor - source_start
        local_end = trusted_end - source_start
        min_db, mean_db, noise_margin = _acoustic_stats(analysis_samples, sample_rate, local_start, local_end)
        rows.append(
            GapRow(
                buffer_id=str(buffer_row["buffer_id"]),
                source_audio_id=source_audio_id,
                local_start_sample=local_start,
                local_end_sample=local_end,
                source_start_sample=cursor,
                source_end_sample=trusted_end,
                duration_samples=trusted_end - cursor,
                duration_ms=_ms_from_samples(trusted_end - cursor, sample_rate),
                min_rms_dbfs=min_db,
                mean_rms_dbfs=mean_db,
                noise_margin_db=noise_margin,
                kind="vad_gap",
            )
        )
    return rows


def _mfa_gaps_for_buffer(
    words: list[dict[str, Any]],
    buffer_row: dict[str, Any],
    sample_rate: int,
    analysis_samples: Any,
) -> list[GapRow]:
    trusted_words = sorted(_trusted_words_for_buffer(words, buffer_row), key=lambda row: int(row["local_start_sample"]))
    rows: list[GapRow] = []
    for left, right in zip(trusted_words, trusted_words[1:]):
        local_start = int(left["local_end_sample"])
        local_end = int(right["local_start_sample"])
        if local_end <= local_start:
            continue
        source_start = int(left["source_end_sample"])
        source_end = int(right["source_start_sample"])
        min_db, mean_db, noise_margin = _acoustic_stats(analysis_samples, sample_rate, local_start, local_end)
        rows.append(
            GapRow(
                buffer_id=str(buffer_row["buffer_id"]),
                source_audio_id=str(buffer_row["source_audio_id"]),
                local_start_sample=local_start,
                local_end_sample=local_end,
                source_start_sample=source_start,
                source_end_sample=source_end,
                duration_samples=local_end - local_start,
                duration_ms=_ms_from_samples(local_end - local_start, sample_rate),
                min_rms_dbfs=min_db,
                mean_rms_dbfs=mean_db,
                noise_margin_db=noise_margin,
                kind="mfa_gap",
                left_ref=str(left["id"]),
                right_ref=str(right["id"]),
            )
        )
    return rows


def _choose_examples(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: (-float(row.get("duration_ms") or 0.0), str(row.get("buffer_id") or "")))[:limit]


def _write_gap_context_wavs(
    *,
    run_root: Path,
    out_dir: Path,
    examples: dict[str, list[dict[str, Any]]],
    queue_by_buffer: dict[str, dict[str, Any]],
    context_sec: float,
) -> dict[str, list[str]]:
    context_paths: dict[str, list[str]] = defaultdict(list)
    context_samples_by_buffer: dict[str, tuple[Any, int]] = {}
    context_samples = {}
    for bucket, rows in examples.items():
        bucket_dir = out_dir / "gap_context_wavs" / bucket
        bucket_dir.mkdir(parents=True, exist_ok=True)
        for index, row in enumerate(rows):
            buffer_id = str(row["buffer_id"])
            if buffer_id not in context_samples_by_buffer:
                queue_row = queue_by_buffer[buffer_id]
                audio_path = resolve_under_root(run_root, str(queue_row.get("queue_audio_path") or queue_row["audio_path"]))
                context_samples_by_buffer[buffer_id] = read_analysis_audio(audio_path)
            samples, sample_rate = context_samples_by_buffer[buffer_id]
            if sample_rate not in context_samples:
                context_samples[sample_rate] = int(round(context_sec * sample_rate))
            half = context_samples[sample_rate]
            local_start = int(row["local_start_sample"])
            local_end = int(row["local_end_sample"])
            center = local_start + ((local_end - local_start) // 2)
            snippet_start = max(0, center - half)
            snippet_end = min(len(samples), center + half)
            rel_path = Path("gap_context_wavs") / bucket / f"{index:02d}_{buffer_id}_{local_start}_{local_end}.wav"
            write_pcm16_mono(out_dir / rel_path, samples[snippet_start:snippet_end], sample_rate)
            context_paths[bucket].append(str(rel_path))
    return dict(context_paths)


def analyze_vad_mfa_gaps(
    run_root: Path,
    out_dir: Path,
    *,
    cuttable_gap_ms: float = 80.0,
    context_sec: float = 2.0,
    max_examples_per_bucket: int = 20,
) -> dict[str, Any]:
    queue_path = resolve_under_root(run_root, "artifacts/asr_mfa_queue.json")
    if queue_path.exists():
        queue_rows = list(read_json_value(queue_path))
    else:
        queue_rows = list(read_json_value(resolve_under_root(run_root, "artifacts/processing_buffers.json")))
    queue_by_buffer = {str(row["buffer_id"]): row for row in queue_rows}
    vad_segments = read_jsonl(resolve_under_root(run_root, "artifacts/vad_segments.jsonl"))
    words = read_jsonl(resolve_under_root(run_root, "artifacts/aligned_words.jsonl"))
    safe_cutpoints = read_jsonl(resolve_under_root(run_root, "artifacts/safe_cutpoints.jsonl"))
    rejected_cutpoints = read_jsonl(resolve_under_root(run_root, "artifacts/rejected_cutpoint_candidates.jsonl"))
    candidate_manifest = list(read_json_value(resolve_under_root(run_root, "artifacts/candidate_review_manifest.json")))
    alignment_qc = list(read_json_value(resolve_under_root(run_root, "artifacts/alignment_qc_by_buffer.json")))
    safe_summary = read_json(resolve_under_root(run_root, "artifacts/safe_cutpoint_summary.json"))

    sample_rate = int(queue_rows[0]["sample_rate"]) if queue_rows else 16000
    cuttable_gap_samples = int(round((cuttable_gap_ms / 1000.0) * sample_rate))
    if "thresholds" in safe_summary:
        cuttable_gap_samples = max(
            cuttable_gap_samples,
            int(round(float(safe_summary["thresholds"].get("min_gap_sec", 0.08)) * sample_rate)),
        )

    words_by_buffer: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in words:
        words_by_buffer[str(row["buffer_id"])].append(row)
    safe_cutpoints_by_buffer: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in safe_cutpoints:
        safe_cutpoints_by_buffer[str(row["buffer_id"])].append(row)
    rejected_by_buffer: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rejected_cutpoints:
        if "buffer_id" in row:
            rejected_by_buffer[str(row["buffer_id"])].append(row)
    qc_by_buffer = {str(row["buffer_id"]): row for row in alignment_qc}
    candidate_by_buffer: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidate_manifest:
        candidate_by_buffer[str(row["buffer_id"])].append(row)

    mfa_gap_count = 0
    mfa_gap_with_vad_gap_count = 0
    vad_gap_count = 0
    vad_gap_with_mfa_gap_count = 0
    safe_cutpoint_inside_vad_gap = 0
    safe_cutpoint_center_distances_ms: list[float] = []
    safe_cutpoint_recall_center_distances_ms: list[float] = []
    cuttable_vad_gap_count = 0
    cuttable_vad_gap_without_mfa_gap_count = 0

    global_examples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    bucket_rows: dict[str, list[GapRow]] = defaultdict(list)
    by_buffer_rows: list[dict[str, Any]] = []

    for buffer_id, buffer_row in sorted(queue_by_buffer.items()):
        audio_path = resolve_under_root(run_root, str(buffer_row.get("queue_audio_path") or buffer_row["audio_path"]))
        analysis_samples, actual_sample_rate = read_analysis_audio(audio_path)
        if actual_sample_rate != sample_rate:
            raise ValueError(f"Expected sample rate {sample_rate} for {buffer_id}, got {actual_sample_rate}")

        buffer_vad_gaps = _vad_gaps_for_buffer(vad_segments, buffer_row, sample_rate, analysis_samples)
        buffer_mfa_gaps = _mfa_gaps_for_buffer(words_by_buffer.get(buffer_id, []), buffer_row, sample_rate, analysis_samples)
        buffer_safe_cutpoints = sorted(safe_cutpoints_by_buffer.get(buffer_id, []), key=lambda row: int(row["cut_local_sample"]))

        mfa_gap_count += len(buffer_mfa_gaps)
        vad_gap_count += len(buffer_vad_gaps)

        buffer_mfa_overlap = 0
        buffer_vad_overlap = 0
        buffer_safe_inside = 0
        buffer_safe_distances_ms: list[float] = []

        for gap in buffer_mfa_gaps:
            overlap = any(
                _overlaps(gap.local_start_sample, gap.local_end_sample, vad_gap.local_start_sample, vad_gap.local_end_sample)
                for vad_gap in buffer_vad_gaps
            )
            if overlap:
                mfa_gap_with_vad_gap_count += 1
                buffer_mfa_overlap += 1
                bucket = "mfa_vad_agree"
            else:
                bucket = "mfa_gap_only"
            bucket_rows[bucket].append(gap)
            global_examples[bucket].append(
                {
                    "buffer_id": buffer_id,
                    "source_audio_id": gap.source_audio_id,
                    "local_start_sample": gap.local_start_sample,
                    "local_end_sample": gap.local_end_sample,
                    "source_start_sample": gap.source_start_sample,
                    "source_end_sample": gap.source_end_sample,
                    "duration_ms": gap.duration_ms,
                    "left_ref": gap.left_ref,
                    "right_ref": gap.right_ref,
                }
            )

        for gap in buffer_vad_gaps:
            overlap = any(
                _overlaps(gap.local_start_sample, gap.local_end_sample, mfa_gap.local_start_sample, mfa_gap.local_end_sample)
                for mfa_gap in buffer_mfa_gaps
            )
            if overlap:
                vad_gap_with_mfa_gap_count += 1
                buffer_vad_overlap += 1
                bucket = "mfa_vad_agree"
            else:
                bucket = "vad_gap_only"
            bucket_rows[bucket].append(gap)
            global_examples[bucket].append(
                {
                    "buffer_id": buffer_id,
                    "source_audio_id": gap.source_audio_id,
                    "local_start_sample": gap.local_start_sample,
                    "local_end_sample": gap.local_end_sample,
                    "source_start_sample": gap.source_start_sample,
                    "source_end_sample": gap.source_end_sample,
                    "duration_ms": gap.duration_ms,
                }
            )
            if gap.duration_samples >= cuttable_gap_samples:
                cuttable_vad_gap_count += 1
                if not overlap:
                    cuttable_vad_gap_without_mfa_gap_count += 1
                    global_examples["false_safe_vad_gap"].append(
                        {
                            "buffer_id": buffer_id,
                            "source_audio_id": gap.source_audio_id,
                            "local_start_sample": gap.local_start_sample,
                            "local_end_sample": gap.local_end_sample,
                            "source_start_sample": gap.source_start_sample,
                            "source_end_sample": gap.source_end_sample,
                            "duration_ms": gap.duration_ms,
                        }
                    )

        cuttable_vad_gaps = [gap for gap in buffer_vad_gaps if gap.duration_samples >= cuttable_gap_samples]
        for cutpoint in buffer_safe_cutpoints:
            cut_local_sample = int(cutpoint["cut_local_sample"])
            inside = any(gap.local_start_sample <= cut_local_sample <= gap.local_end_sample for gap in buffer_vad_gaps)
            if inside:
                safe_cutpoint_inside_vad_gap += 1
                buffer_safe_inside += 1
            nearest_center_distance = None
            if buffer_vad_gaps:
                nearest_center_distance = min(
                    _interval_distance_to_center(cut_local_sample, gap.local_start_sample, gap.local_end_sample)
                    for gap in buffer_vad_gaps
                )
                distance_ms = _ms_from_samples(nearest_center_distance, sample_rate)
                safe_cutpoint_center_distances_ms.append(distance_ms)
                safe_cutpoint_recall_center_distances_ms.append(distance_ms)
                buffer_safe_distances_ms.append(distance_ms)
            if not inside:
                global_examples["safe_cutpoint_not_in_vad_gap"].append(
                    {
                        "buffer_id": buffer_id,
                        "source_audio_id": str(buffer_row["source_audio_id"]),
                        "local_start_sample": cut_local_sample,
                        "local_end_sample": cut_local_sample + 1,
                        "source_start_sample": int(cutpoint["source_sample"]),
                        "source_end_sample": int(cutpoint["source_sample"]) + 1,
                        "duration_ms": 0.0,
                        "safe_cutpoint_id": str(cutpoint["id"]),
                    }
                )

        by_buffer_rows.append(
            {
                "buffer_id": buffer_id,
                "source_audio_id": str(buffer_row["source_audio_id"]),
                "candidate_clip_count": len(candidate_by_buffer.get(buffer_id, [])),
                "alignment_qc_fatal_count": len(list(qc_by_buffer.get(buffer_id, {}).get("fatal_reason_codes") or [])),
                "alignment_qc_warning_count": len(list(qc_by_buffer.get(buffer_id, {}).get("warning_reason_codes") or [])),
                "mfa_gap_count": len(buffer_mfa_gaps),
                "mfa_gap_with_vad_gap_count": buffer_mfa_overlap,
                "mfa_gap_vad_coverage_ratio": _round(_ratio(buffer_mfa_overlap, len(buffer_mfa_gaps))),
                "vad_gap_count": len(buffer_vad_gaps),
                "vad_gap_with_mfa_gap_count": buffer_vad_overlap,
                "vad_gap_mfa_coverage_ratio": _round(_ratio(buffer_vad_overlap, len(buffer_vad_gaps))),
                "safe_cutpoint_count": len(buffer_safe_cutpoints),
                "safe_cutpoints_inside_vad_gap_count": buffer_safe_inside,
                "safe_cutpoints_inside_vad_gap_ratio": _round(_ratio(buffer_safe_inside, len(buffer_safe_cutpoints))),
                "median_safe_cutpoint_distance_to_vad_gap_center_ms": _round(median(buffer_safe_distances_ms), 3)
                if buffer_safe_distances_ms
                else None,
                "rejected_cutpoint_candidate_count": len(rejected_by_buffer.get(buffer_id, [])),
            }
        )

    recall_at_50 = sum(distance <= 50.0 for distance in safe_cutpoint_recall_center_distances_ms)
    recall_at_100 = sum(distance <= 100.0 for distance in safe_cutpoint_recall_center_distances_ms)
    recall_at_200 = sum(distance <= 200.0 for distance in safe_cutpoint_recall_center_distances_ms)

    chosen_examples = {bucket: _choose_examples(rows, limit=max_examples_per_bucket) for bucket, rows in global_examples.items() if rows}
    context_paths = _write_gap_context_wavs(
        run_root=run_root,
        out_dir=out_dir,
        examples=chosen_examples,
        queue_by_buffer=queue_by_buffer,
        context_sec=context_sec,
    )

    rejected_reason_counts = Counter(reason for row in rejected_cutpoints for reason in row.get("reason_codes", []))
    summary = {
        "run_root": str(run_root),
        "dataset_id": run_root.name,
        "analysis_sample_rate": sample_rate,
        "buffer_count": len(queue_by_buffer),
        "candidate_clip_count": len(candidate_manifest),
        "mfa_gap_count": mfa_gap_count,
        "mfa_gap_with_vad_gap_count": mfa_gap_with_vad_gap_count,
        "mfa_gap_vad_coverage_ratio": _round(_ratio(mfa_gap_with_vad_gap_count, mfa_gap_count)),
        "vad_gap_count": vad_gap_count,
        "vad_gap_with_mfa_gap_count": vad_gap_with_mfa_gap_count,
        "vad_gap_mfa_coverage_ratio": _round(_ratio(vad_gap_with_mfa_gap_count, vad_gap_count)),
        "safe_cutpoint_count": len(safe_cutpoints),
        "safe_cutpoints_inside_vad_gap_count": safe_cutpoint_inside_vad_gap,
        "safe_cutpoints_inside_vad_gap_ratio": _round(_ratio(safe_cutpoint_inside_vad_gap, len(safe_cutpoints))),
        "median_distance_to_vad_gap_center_ms": _percentile(safe_cutpoint_center_distances_ms, 50),
        "p95_distance_to_vad_gap_center_ms": _percentile(safe_cutpoint_center_distances_ms, 95),
        "safe_cutpoint_vad_recall_50ms": _round(_ratio(recall_at_50, len(safe_cutpoint_recall_center_distances_ms))),
        "safe_cutpoint_vad_recall_100ms": _round(_ratio(recall_at_100, len(safe_cutpoint_recall_center_distances_ms))),
        "safe_cutpoint_vad_recall_200ms": _round(_ratio(recall_at_200, len(safe_cutpoint_recall_center_distances_ms))),
        "vad_cuttable_gap_count": cuttable_vad_gap_count,
        "vad_cuttable_gap_without_mfa_gap_count": cuttable_vad_gap_without_mfa_gap_count,
        "false_safe_vad_gap_ratio": _round(_ratio(cuttable_vad_gap_without_mfa_gap_count, cuttable_vad_gap_count)),
        "cuttable_gap_ms": cuttable_gap_ms,
        "bucket_acoustics": {
            bucket: _summarize_gap_rows(rows) for bucket, rows in sorted(bucket_rows.items())
        },
        "accepted_safe_cutpoint_stats": {
            "count": len(safe_cutpoints),
            "median_distance_to_vad_gap_center_ms": _percentile(safe_cutpoint_center_distances_ms, 50),
            "p95_distance_to_vad_gap_center_ms": _percentile(safe_cutpoint_center_distances_ms, 95),
        },
        "rejected_cutpoint_reason_counts": dict(sorted(rejected_reason_counts.items())),
        "example_context_wavs": context_paths,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "gap_agreement_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    (out_dir / "gap_agreement_examples.json").write_text(
        json.dumps(
            {
                "examples": chosen_examples,
                "example_context_wavs": context_paths,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    with (out_dir / "gap_agreement_by_buffer.csv").open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "buffer_id",
            "source_audio_id",
            "candidate_clip_count",
            "alignment_qc_fatal_count",
            "alignment_qc_warning_count",
            "mfa_gap_count",
            "mfa_gap_with_vad_gap_count",
            "mfa_gap_vad_coverage_ratio",
            "vad_gap_count",
            "vad_gap_with_mfa_gap_count",
            "vad_gap_mfa_coverage_ratio",
            "safe_cutpoint_count",
            "safe_cutpoints_inside_vad_gap_count",
            "safe_cutpoints_inside_vad_gap_ratio",
            "median_safe_cutpoint_distance_to_vad_gap_center_ms",
            "rejected_cutpoint_candidate_count",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(by_buffer_rows)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze agreement between VAD gaps, MFA word gaps, and SafeCutPoints.")
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--cuttable-gap-ms", type=float, default=80.0)
    parser.add_argument("--context-sec", type=float, default=2.0)
    parser.add_argument("--max-examples-per-bucket", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = analyze_vad_mfa_gaps(
        Path(args.run_root).expanduser().resolve(),
        Path(args.out).expanduser().resolve(),
        cuttable_gap_ms=args.cuttable_gap_ms,
        context_sec=args.context_sec,
        max_examples_per_bucket=args.max_examples_per_bucket,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
