from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from .buffers import frame_rms_db, read_analysis_audio, sec_to_sample
from .io import read_json_value, read_jsonl, resolve_under_root, sha256_file, write_json, write_jsonl


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    import numpy as np

    return round(float(np.percentile(values, quantile)), 6)


def generate_safe_cutpoint_diagnostics(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    import numpy as np

    sample_rate = int(config.get("analysis_sample_rate") or 16000)
    left_guard_sec = float(config.get("cutpoint_left_word_edge_guard_ms", 30)) / 1000.0
    min_gap_sec = float(config.get("cutpoint_min_gap_ms", 80)) / 1000.0
    right_guard_sec = float(config.get("cutpoint_right_word_edge_guard_ms", 30)) / 1000.0
    frame_sec = float(config.get("cutpoint_frame_ms", 20)) / 1000.0
    hop_sec = float(config.get("cutpoint_hop_ms", 10)) / 1000.0
    noise_margin_db = float(config.get("cutpoint_noise_margin_db", 6.0))
    oov_guard_sec = float(config.get("oov_cut_guard_sec", 0.5))
    symbol_guard_sec = float(config.get("symbol_cut_guard_sec", 0.5))
    numeric_guard_sec = float(config.get("numeric_cut_guard_sec", 0.5))
    provisional_guard_sec = float(config.get("provisional_split_guard_sec", 0.5))

    queue_path = resolve_under_root(run_root, "artifacts/asr_mfa_queue.json")
    words_path = resolve_under_root(run_root, "artifacts/aligned_words.jsonl")
    qc_path = resolve_under_root(run_root, "artifacts/alignment_qc_by_buffer.json")
    buffers = {row["buffer_id"]: row for row in read_json_value(queue_path)}
    qc_by_buffer = {row["buffer_id"]: row for row in read_json_value(qc_path)}
    missing_alignment_qc_buffer_ids = sorted(set(buffers) - set(qc_by_buffer))
    unexpected_alignment_qc_buffer_ids = sorted(set(qc_by_buffer) - set(buffers))
    words_by_buffer: dict[str, list[dict[str, Any]]] = {buffer_id: [] for buffer_id in buffers}
    for word in read_jsonl(words_path):
        words_by_buffer.setdefault(str(word["buffer_id"]), []).append(word)

    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    valley_dbs: list[float] = []
    gap_durations: list[float] = []
    left_guard = sec_to_sample(left_guard_sec, sample_rate)
    right_guard = sec_to_sample(right_guard_sec, sample_rate)
    min_gap = sec_to_sample(min_gap_sec, sample_rate)
    oov_guard = sec_to_sample(oov_guard_sec, sample_rate)
    symbol_guard = sec_to_sample(symbol_guard_sec, sample_rate)
    numeric_guard = sec_to_sample(numeric_guard_sec, sample_rate)
    provisional_guard = sec_to_sample(provisional_guard_sec, sample_rate)

    for buffer_id, buffer in sorted(buffers.items()):
        audio_path = resolve_under_root(run_root, str(buffer.get("queue_audio_path") or buffer["audio_path"]))
        samples, actual_sample_rate = read_analysis_audio(audio_path)
        if actual_sample_rate != sample_rate:
            raise ValueError(f"SafeCutPoint audio sample-rate mismatch for {buffer_id}: {actual_sample_rate} != {sample_rate}")
        frame_centers, buffer_db = frame_rms_db(samples, sample_rate, frame_sec=frame_sec, hop_sec=hop_sec)
        noise_floor_db = percentile(buffer_db.tolist(), 10) if len(buffer_db) else None
        words = sorted(words_by_buffer.get(buffer_id, []), key=lambda row: int(row["source_start_sample"]))
        local_words = [
            {
                **word,
                "local_start_sample": int(word.get("local_start_sample", int(word["source_start_sample"]) - int(buffer["source_start_sample"]))),
                "local_end_sample": int(word.get("local_end_sample", int(word["source_end_sample"]) - int(buffer["source_start_sample"]))),
            }
            for word in words
        ]
        trusted_words = [
            word
            for word in local_words
            if word["local_start_sample"] >= int(buffer["trusted_local_start_sample"])
            and word["local_end_sample"] <= int(buffer["trusted_local_end_sample"])
            and word["local_end_sample"] > word["local_start_sample"]
        ]
        qc = qc_by_buffer.get(buffer_id)
        if qc is None:
            qc_fatal_reasons = ["missing_alignment_qc_for_buffer"]
            qc_warning_reasons: list[str] = []
            automatic_cutpoints_disabled = True
        else:
            qc_fatal_reasons = list(qc.get("fatal_reason_codes") or [])
            qc_warning_reasons = list(qc.get("warning_reason_codes") or [])
            automatic_cutpoints_disabled = bool(qc.get("automatic_cutpoints_disabled")) or bool(qc_fatal_reasons)

        for index, (left, right) in enumerate(zip(trusted_words, trusted_words[1:])):
            gap_start = int(left["local_end_sample"])
            gap_end = int(right["local_start_sample"])
            candidate_start = gap_start + left_guard
            candidate_end = gap_end - right_guard
            usable_gap = candidate_end - candidate_start
            gap_duration_sec = max(0, gap_end - gap_start) / sample_rate
            reason_codes: list[str] = []
            if automatic_cutpoints_disabled:
                reason_codes.append("buffer_automatic_cutpoints_disabled")
                reason_codes.extend(f"alignment_qc_fatal:{reason}" for reason in qc_fatal_reasons)
            if buffer.get("left_provisional_boundary") and candidate_start < int(buffer["trusted_local_start_sample"]) + provisional_guard:
                reason_codes.append("near_left_provisional_pre_mfa_split")
            if buffer.get("right_provisional_boundary") and candidate_end > int(buffer["trusted_local_end_sample"]) - provisional_guard:
                reason_codes.append("near_right_provisional_pre_mfa_split")
            if left.get("alignment_token_word_mismatch") or right.get("alignment_token_word_mismatch"):
                reason_codes.append("alignment_token_word_mismatch")
            if left.get("is_oov") or right.get("is_oov"):
                reason_codes.append("adjacent_to_oov")
            if left.get("contains_danger_symbol") or right.get("contains_danger_symbol"):
                reason_codes.append("adjacent_to_symbol_hazard")
            if left.get("contains_numeric") or right.get("contains_numeric"):
                reason_codes.append("adjacent_to_numeric_hazard")
            if gap_end <= gap_start:
                reason_codes.append("non_positive_word_gap")
            if usable_gap < min_gap:
                reason_codes.append("usable_gap_too_short")
            if usable_gap >= min_gap:
                if any(
                    word.get("contains_danger_symbol")
                    and candidate_end >= word["local_start_sample"] - symbol_guard
                    and candidate_start <= word["local_end_sample"] + symbol_guard
                    for word in trusted_words
                ):
                    reason_codes.append("near_symbol_hazard")
                if any(
                    word.get("contains_numeric")
                    and candidate_end >= word["local_start_sample"] - numeric_guard
                    and candidate_start <= word["local_end_sample"] + numeric_guard
                    for word in trusted_words
                ):
                    reason_codes.append("near_numeric_hazard")
            if noise_floor_db is None:
                reason_codes.append("missing_buffer_energy")

            valley_sample = None
            valley_db = None
            if not reason_codes:
                mask = (frame_centers >= candidate_start) & (frame_centers <= candidate_end)
                if not np.any(mask):
                    reason_codes.append("no_rms_frame_inside_gap")
                else:
                    scoped_centers = frame_centers[mask]
                    scoped_db = buffer_db[mask]
                    valley_index = int(np.argmin(scoped_db))
                    valley_sample = int(scoped_centers[valley_index])
                    valley_db = float(scoped_db[valley_index])
                    if any(
                        word.get("is_oov")
                        and valley_sample >= word["local_start_sample"] - oov_guard
                        and valley_sample <= word["local_end_sample"] + oov_guard
                        for word in trusted_words
                    ):
                        reason_codes.append("near_oov_word")
                    if valley_db > float(noise_floor_db) + noise_margin_db:
                        reason_codes.append("valley_above_noise_floor_margin")
                    if not reason_codes:
                        valley_dbs.append(valley_db)
                        gap_durations.append(gap_duration_sec)

            row = {
                "id": f"{buffer_id}-cut-{index:04d}",
                "buffer_id": buffer_id,
                "left_word_id": left["id"],
                "right_word_id": right["id"],
                "left_word": left["word"],
                "right_word": right["word"],
                "gap_start_local_sample": gap_start,
                "gap_end_local_sample": gap_end,
                "candidate_start_local_sample": candidate_start,
                "candidate_end_local_sample": candidate_end,
                "cut_local_sample": valley_sample,
                "source_sample": None if valley_sample is None else int(buffer["source_start_sample"]) + valley_sample,
                "gap_duration_sec": round(gap_duration_sec, 6),
                "usable_gap_sec": round(max(0, usable_gap) / sample_rate, 6),
                "valley_dbfs": valley_db,
                "noise_floor_dbfs": noise_floor_db,
                "noise_margin_db": None if valley_db is None or noise_floor_db is None else valley_db - float(noise_floor_db),
                "frame_sec": frame_sec,
                "hop_sec": hop_sec,
                "left_word_edge_guard_sec": left_guard_sec,
                "right_word_edge_guard_sec": right_guard_sec,
                "buffer_warning_reason_codes": sorted(set(qc_warning_reasons)),
                "reason_codes": sorted(set(reason_codes)),
            }
            if reason_codes:
                rejected.append(row)
            else:
                accepted.append(row)

    accepted_path = resolve_under_root(run_root, "artifacts/safe_cutpoints.jsonl")
    rejected_path = resolve_under_root(run_root, "artifacts/rejected_cutpoint_candidates.jsonl")
    write_jsonl(accepted_path, accepted)
    write_jsonl(rejected_path, rejected)
    rejection_counts = Counter(reason for row in rejected for reason in row["reason_codes"])
    summary = {
        "stage": "safe_cutpoints",
        "config_hash": str(config.get("config_hash") or ""),
        "input_artifact_hashes": {
            "asr_mfa_queue_json": sha256_file(queue_path),
            "aligned_words_jsonl": sha256_file(words_path),
            "alignment_qc_by_buffer_json": sha256_file(qc_path),
        },
        "output_hashes": {
            "safe_cutpoints_jsonl": sha256_file(accepted_path),
            "rejected_cutpoint_candidates_jsonl": sha256_file(rejected_path),
        },
        "buffers_evaluated": len(buffers),
        "missing_alignment_qc_buffer_count": len(missing_alignment_qc_buffer_ids),
        "missing_alignment_qc_buffer_ids": missing_alignment_qc_buffer_ids,
        "unexpected_alignment_qc_buffer_count": len(unexpected_alignment_qc_buffer_ids),
        "unexpected_alignment_qc_buffer_ids": unexpected_alignment_qc_buffer_ids,
        "buffers_with_automatic_cutpoints_disabled": sum(
            buffer_id in missing_alignment_qc_buffer_ids
            or bool(qc_by_buffer.get(buffer_id, {}).get("automatic_cutpoints_disabled"))
            or bool(qc_by_buffer.get(buffer_id, {}).get("fatal_reason_codes"))
            for buffer_id in buffers
        ),
        "accepted_cutpoints": len(accepted),
        "rejected_cutpoint_candidates": len(rejected),
        "acceptance_rate": 0.0 if not accepted and not rejected else len(accepted) / (len(accepted) + len(rejected)),
        "p50_gap_duration_sec": percentile(gap_durations, 50),
        "p90_gap_duration_sec": percentile(gap_durations, 90),
        "p50_valley_dbfs": percentile(valley_dbs, 50),
        "p90_valley_dbfs": percentile(valley_dbs, 90),
        "rejection_reason_counts": dict(sorted(rejection_counts.items())),
        "thresholds": {
            "left_word_edge_guard_sec": left_guard_sec,
            "right_word_edge_guard_sec": right_guard_sec,
            "min_gap_sec": min_gap_sec,
            "frame_sec": frame_sec,
            "hop_sec": hop_sec,
            "noise_margin_db": noise_margin_db,
            "oov_cut_guard_sec": oov_guard_sec,
            "symbol_cut_guard_sec": symbol_guard_sec,
            "numeric_cut_guard_sec": numeric_guard_sec,
            "provisional_split_guard_sec": provisional_guard_sec,
        },
    }
    write_json(resolve_under_root(run_root, "artifacts/safe_cutpoint_summary.json"), summary)
    return summary
