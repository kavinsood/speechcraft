from __future__ import annotations

import shutil
from collections import Counter
from pathlib import Path
from typing import Any

from .buffers import read_analysis_audio, sec_to_sample, write_pcm16_mono
from .io import read_json_value, read_jsonl, resolve_under_root, sha256_file, write_json


def reconstruct_training_text(words: list[dict[str, Any]]) -> str:
    raw_tokens: list[str] = []
    previous_raw_token_id = None
    for word in words:
        raw_token_id = word.get("raw_token_id")
        if raw_token_id and raw_token_id == previous_raw_token_id:
            continue
        raw_tokens.append(str(word.get("raw_token") or word["word"]))
        previous_raw_token_id = raw_token_id
    return " ".join(raw_tokens).strip()


def assemble_candidate_review_clips(
    run_root: Path,
    config: dict[str, Any],
    *,
    artifact_root: Path | None = None,
) -> dict[str, Any]:
    destination_root = artifact_root or run_root
    sample_rate = int(config.get("analysis_sample_rate") or 16000)
    min_clip_sec = float(config.get("candidate_min_clip_sec", 3.0))
    target_clip_sec = float(config.get("candidate_target_clip_sec", 8.0))
    max_clip_sec = float(config.get("candidate_max_clip_sec", 15.0))
    min_samples = sec_to_sample(min_clip_sec, sample_rate)
    target_samples = sec_to_sample(target_clip_sec, sample_rate)
    max_samples = sec_to_sample(max_clip_sec, sample_rate)
    if not 0 < min_samples <= target_samples <= max_samples:
        raise ValueError("Candidate clip durations must satisfy 0 < min <= target <= max")

    queue_path = resolve_under_root(run_root, "artifacts/asr_mfa_queue.json")
    words_path = resolve_under_root(run_root, "artifacts/aligned_words.jsonl")
    cutpoints_path = resolve_under_root(run_root, "artifacts/safe_cutpoints.jsonl")
    qc_path = resolve_under_root(run_root, "artifacts/alignment_qc_by_buffer.json")
    buffers = {row["buffer_id"]: row for row in read_json_value(queue_path)}
    qc_by_buffer = {row["buffer_id"]: row for row in read_json_value(qc_path)}
    words_by_buffer: dict[str, list[dict[str, Any]]] = {buffer_id: [] for buffer_id in buffers}
    cutpoints_by_buffer: dict[str, list[dict[str, Any]]] = {buffer_id: [] for buffer_id in buffers}
    for word in read_jsonl(words_path):
        words_by_buffer.setdefault(str(word["buffer_id"]), []).append(word)
    for cutpoint in read_jsonl(cutpoints_path):
        cutpoints_by_buffer.setdefault(str(cutpoint["buffer_id"]), []).append(cutpoint)

    review_dir = resolve_under_root(destination_root, "artifacts/candidate_review_clips")
    if review_dir.exists():
        shutil.rmtree(review_dir)
    review_dir.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for buffer_id, buffer in sorted(buffers.items()):
        words = sorted(words_by_buffer.get(buffer_id, []), key=lambda row: int(row["source_start_sample"]))
        cutpoints = sorted(cutpoints_by_buffer.get(buffer_id, []), key=lambda row: int(row["cut_local_sample"]))
        qc = qc_by_buffer.get(buffer_id)
        if qc is None:
            rejected.append(
                {
                    "buffer_id": buffer_id,
                    "reason_codes": ["missing_alignment_qc_for_buffer", "buffer_excluded_from_clip_assembly"],
                }
            )
            continue
        if qc.get("automatic_cutpoints_disabled") or qc.get("fatal_reason_codes"):
            rejected.append(
                {
                    "buffer_id": buffer_id,
                    "reason_codes": ["alignment_qc_fatal", "buffer_excluded_from_clip_assembly"],
                    "alignment_qc_fatal_reason_codes": list(qc.get("fatal_reason_codes") or []),
                }
            )
            continue
        if not words:
            rejected.append(
                {
                    "buffer_id": buffer_id,
                    "reason_codes": ["no_aligned_words", "buffer_excluded_from_clip_assembly"],
                }
            )
            continue
        if len(cutpoints) < 2:
            rejected.append(
                {
                    "buffer_id": buffer_id,
                    "reason_codes": ["insufficient_safe_cutpoints", "buffer_excluded_from_clip_assembly"],
                }
            )
            continue

        audio_path = resolve_under_root(run_root, str(buffer.get("queue_audio_path") or buffer["audio_path"]))
        audio, actual_sample_rate = read_analysis_audio(audio_path)
        if actual_sample_rate != sample_rate:
            raise ValueError(f"Candidate review audio sample-rate mismatch for {buffer_id}: {actual_sample_rate} != {sample_rate}")
        start_index = 0
        while start_index < len(cutpoints) - 1:
            start = cutpoints[start_index]
            start_local = int(start["cut_local_sample"])
            candidates = [
                end
                for end in cutpoints[start_index + 1 :]
                if min_samples <= int(end["cut_local_sample"]) - start_local <= max_samples
            ]
            if not candidates:
                rejected.append(
                    {
                        "buffer_id": buffer_id,
                        "start_cutpoint_ref": start["id"],
                        "reason_codes": ["no_valid_end_cutpoint"],
                    }
                )
                start_index += 1
                continue
            end = min(
                candidates,
                key=lambda row: (
                    abs((int(row["cut_local_sample"]) - start_local) - target_samples),
                    int(row["cut_local_sample"]),
                ),
            )
            end_local = int(end["cut_local_sample"])
            if start_local < 0 or end_local > len(audio) or end_local <= start_local:
                raise RuntimeError(f"Invalid candidate review clip bounds for {buffer_id}: {start_local}:{end_local}")
            source_start = int(buffer["source_start_sample"]) + start_local
            source_end = int(buffer["source_start_sample"]) + end_local
            if int(start["source_sample"]) != source_start or int(end["source_sample"]) != source_end:
                raise RuntimeError(f"SafeCutPoint source/local coordinate mismatch for {buffer_id}")
            included_words = [
                word
                for word in words
                if int(word["source_start_sample"]) >= source_start and int(word["source_end_sample"]) <= source_end
            ]
            if not included_words:
                rejected.append(
                    {
                        "buffer_id": buffer_id,
                        "start_cutpoint_ref": start["id"],
                        "end_cutpoint_ref": end["id"],
                        "reason_codes": ["no_words_inside_candidate_span"],
                    }
                )
                start_index = cutpoints.index(end)
                continue
            review_reasons = {
                reason
                for word in included_words
                for reason in word.get("review_reason_codes", [])
            }
            buffer_warning_reasons = sorted(
                set(qc.get("warning_reason_codes") or [])
                | set(start.get("buffer_warning_reason_codes") or [])
                | set(end.get("buffer_warning_reason_codes") or [])
            )
            if any(word.get("contains_danger_symbol") for word in included_words):
                review_reasons.update(["clip_contains_symbol_hazard", "transcript_requires_review"])
            if any(word.get("contains_numeric") for word in included_words):
                review_reasons.update(["clip_contains_numeric_token", "transcript_requires_review"])
            if any(word.get("is_oov") for word in included_words):
                review_reasons.update(["clip_contains_oov", "transcript_requires_review"])

            clip_id = f"candidate_review_clip_{len(manifest):06d}"
            rel_audio_path = f"artifacts/candidate_review_clips/{clip_id}.wav"
            clip_path = resolve_under_root(destination_root, rel_audio_path)
            write_pcm16_mono(clip_path, audio[start_local:end_local], sample_rate)
            duration_samples = end_local - start_local
            audio_sha256 = sha256_file(clip_path)
            # Compatibility release: emit both fields with the same value. New code reads
            # audio_sha256; audio_hash remains a legacy alias until consumers migrate.
            manifest.append(
                {
                    "id": clip_id,
                    "buffer_id": buffer_id,
                    "source_audio_id": buffer.get("source_audio_id"),
                    "audio_path": rel_audio_path,
                    "audio_sha256": audio_sha256,
                    "audio_hash": audio_sha256,
                    "sample_rate": sample_rate,
                    "start_cutpoint_ref": start["id"],
                    "end_cutpoint_ref": end["id"],
                    "buffer_local_start_sample": start_local,
                    "buffer_local_end_sample": end_local,
                    "source_start_sample": source_start,
                    "source_end_sample": source_end,
                    "duration_samples": duration_samples,
                    "duration_sec": round(duration_samples / sample_rate, 6),
                    "word_ids": [word["id"] for word in included_words],
                    "training_text": reconstruct_training_text(included_words),
                    "alignment_text": " ".join(str(word["word"]) for word in included_words),
                    "needs_review": bool(review_reasons),
                    "review_reason_codes": sorted(review_reasons),
                    "buffer_warning_reason_codes": buffer_warning_reasons,
                    "status": "candidate_review",
                }
            )
            start_index = cutpoints.index(end)

    manifest_path = resolve_under_root(destination_root, "artifacts/candidate_review_manifest.json")
    rejected_path = resolve_under_root(destination_root, "artifacts/candidate_review_rejected.json")
    write_json(manifest_path, manifest)
    write_json(rejected_path, rejected)
    durations = [float(row["duration_sec"]) for row in manifest]
    review_reason_counts = Counter(reason for row in manifest for reason in row["review_reason_codes"])
    rejection_reason_counts = Counter(reason for row in rejected for reason in row["reason_codes"])
    summary = {
        "stage": "candidate_review_clips",
        "config_hash": str(config.get("config_hash") or ""),
        "input_artifact_hashes": {
            "asr_mfa_queue_json": sha256_file(queue_path),
            "aligned_words_jsonl": sha256_file(words_path),
            "safe_cutpoints_jsonl": sha256_file(cutpoints_path),
            "alignment_qc_by_buffer_json": sha256_file(qc_path),
        },
        "output_hashes": {
            "candidate_review_manifest_json": sha256_file(manifest_path),
            "candidate_review_rejected_json": sha256_file(rejected_path),
            "candidate_review_wavs": {row["id"]: row["audio_hash"] for row in manifest},
        },
        "candidate_review_clips": len(manifest),
        "rejected_spans": len(rejected),
        "total_duration_sec": round(sum(durations), 6),
        "min_clip_duration_sec": min(durations, default=None),
        "max_clip_duration_sec": max(durations, default=None),
        "clips_needing_review": sum(row["needs_review"] for row in manifest),
        "clips_needing_review_for_symbols": sum(
            "clip_contains_symbol_hazard" in row["review_reason_codes"] for row in manifest
        ),
        "clips_needing_review_for_numbers": sum(
            "clip_contains_numeric_token" in row["review_reason_codes"] for row in manifest
        ),
        "clips_needing_review_for_oovs": sum(
            "clip_contains_oov" in row["review_reason_codes"] for row in manifest
        ),
        "review_reason_counts": dict(sorted(review_reason_counts.items())),
        "rejection_reason_counts": dict(sorted(rejection_reason_counts.items())),
        "thresholds": {
            "min_clip_sec": min_clip_sec,
            "target_clip_sec": target_clip_sec,
            "max_clip_sec": max_clip_sec,
        },
        "output_dir": "artifacts/candidate_review_clips",
    }
    write_json(resolve_under_root(destination_root, "artifacts/candidate_review_summary.json"), summary)
    return summary
