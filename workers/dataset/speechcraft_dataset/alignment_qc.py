from __future__ import annotations

from pathlib import Path
from typing import Any

from .io import read_json_value, read_jsonl, resolve_under_root, sha256_file, write_json


WARNING_REASON_CODES = {
    "word_near_trusted_edge",
    "word_outside_trusted_chunk",
    "word_contains_symbol_hazard",
    "word_contains_numeric_hazard",
    "word_is_oov",
}

FATAL_REASON_CODES = {
    "no_aligned_words",
    "alignment_token_count_mismatch",
    "alignment_token_word_mismatch",
    "word_non_positive_duration",
    "word_absurdly_short",
    "word_absurdly_long",
    "word_order_backwards",
    "word_outside_buffer",
}


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    try:
        import numpy as np

        return round(float(np.percentile(values, quantile)), 6)
    except Exception:
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, int(round((quantile / 100.0) * (len(ordered) - 1)))))
        return round(float(ordered[index]), 6)


def run_alignment_qc(run_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    sample_rate = int(config.get("analysis_sample_rate") or 16000)
    tiny_word_sec = float(config.get("alignment_tiny_word_sec", 0.020))
    long_word_sec = float(config.get("alignment_long_word_sec", 2.0))
    trusted_edge_warn_sec = float(config.get("alignment_trusted_edge_warn_sec", 0.080))
    edge_warn_samples = int(round(trusted_edge_warn_sec * sample_rate))

    queue_path = resolve_under_root(run_root, "artifacts/asr_mfa_queue.json")
    normalized_path = resolve_under_root(run_root, "artifacts/normalized_transcripts.json")
    transcripts_path = resolve_under_root(run_root, "artifacts/transcripts.json")
    aligned_words_path = resolve_under_root(run_root, "artifacts/aligned_words.jsonl")
    buffers = {row["buffer_id"]: row for row in read_json_value(queue_path)}
    normalized = {row["buffer_id"]: row for row in read_json_value(normalized_path)}
    transcripts = {row["buffer_id"]: row for row in read_json_value(transcripts_path)}

    words_by_buffer: dict[str, list[dict[str, Any]]] = {buffer_id: [] for buffer_id in buffers}
    for word in read_jsonl(aligned_words_path):
        words_by_buffer.setdefault(str(word["buffer_id"]), []).append(word)
    unexpected_aligned_word_buffer_ids = sorted(set(words_by_buffer) - set(buffers))

    by_buffer: list[dict[str, Any]] = []
    all_word_durations: list[float] = []
    all_word_gaps: list[float] = []
    for buffer_id, buffer in sorted(buffers.items()):
        words = sorted(words_by_buffer.get(buffer_id, []), key=lambda row: int(row["source_start_sample"]))
        token_count = len(normalized.get(buffer_id, {}).get("tokens", []))
        reason_codes: list[str] = []
        word_durations: list[float] = []
        word_gaps: list[float] = []
        negative_or_zero = 0
        tiny_words = 0
        long_words = 0
        backwards = 0
        outside_buffer = 0
        outside_trusted = 0
        near_left_trusted_edge = 0
        near_right_trusted_edge = 0
        symbol_hazard_words = 0
        numeric_hazard_words = 0
        oov_words = 0
        token_word_mismatches = 0

        previous_end: int | None = None
        for word in words:
            start = int(word["source_start_sample"])
            end = int(word["source_end_sample"])
            duration_sec = (end - start) / sample_rate
            if end <= start:
                negative_or_zero += 1
                reason_codes.append("word_non_positive_duration")
            else:
                word_durations.append(duration_sec)
                all_word_durations.append(duration_sec)
            if 0 < duration_sec < tiny_word_sec:
                tiny_words += 1
                reason_codes.append("word_absurdly_short")
            if duration_sec > long_word_sec:
                long_words += 1
                reason_codes.append("word_absurdly_long")
            if previous_end is not None:
                gap_sec = (start - previous_end) / sample_rate
                word_gaps.append(gap_sec)
                all_word_gaps.append(gap_sec)
                if start < previous_end:
                    backwards += 1
                    reason_codes.append("word_order_backwards")
            previous_end = end

            if start < int(buffer["source_start_sample"]) or end > int(buffer["source_end_sample"]):
                outside_buffer += 1
                reason_codes.append("word_outside_buffer")
            if start < int(buffer["trusted_start_sample"]) or end > int(buffer["trusted_end_sample"]):
                outside_trusted += 1
                reason_codes.append("word_outside_trusted_chunk")
            if abs(start - int(buffer["trusted_start_sample"])) < edge_warn_samples:
                near_left_trusted_edge += 1
                reason_codes.append("word_near_trusted_edge")
            if abs(end - int(buffer["trusted_end_sample"])) < edge_warn_samples:
                near_right_trusted_edge += 1
                reason_codes.append("word_near_trusted_edge")
            if word.get("contains_danger_symbol"):
                symbol_hazard_words += 1
                reason_codes.append("word_contains_symbol_hazard")
            if word.get("contains_numeric"):
                numeric_hazard_words += 1
                reason_codes.append("word_contains_numeric_hazard")
            if word.get("is_oov"):
                oov_words += 1
                reason_codes.append("word_is_oov")
            if word.get("alignment_token_word_mismatch"):
                token_word_mismatches += 1
                reason_codes.append("alignment_token_word_mismatch")

        alignment_token_word_delta = token_count - len(words)
        if not words:
            reason_codes.append("no_aligned_words")
        if alignment_token_word_delta != 0:
            reason_codes.append("alignment_token_count_mismatch")
        unique_reasons = sorted(set(reason_codes))
        warning_reason_codes = sorted(reason for reason in unique_reasons if reason in WARNING_REASON_CODES)
        fatal_reason_codes = sorted(reason for reason in unique_reasons if reason in FATAL_REASON_CODES)

        segments = transcripts.get(buffer_id, {}).get("segments", [])
        by_buffer.append(
            {
                "buffer_id": buffer_id,
                "duration_sec": buffer.get("duration_sec"),
                "split_strategy": buffer.get("split_strategy"),
                "normalized_token_count": token_count,
                "aligned_word_count": len(words),
                "alignment_token_word_delta": alignment_token_word_delta,
                "negative_or_zero_word_durations": negative_or_zero,
                "absurdly_short_words": tiny_words,
                "absurdly_long_words": long_words,
                "backwards_word_order": backwards,
                "words_outside_buffer": outside_buffer,
                "words_outside_trusted_chunk": outside_trusted,
                "words_near_left_trusted_edge": near_left_trusted_edge,
                "words_near_right_trusted_edge": near_right_trusted_edge,
                "words_near_trusted_edges": near_left_trusted_edge + near_right_trusted_edge,
                "words_with_symbol_hazards": symbol_hazard_words,
                "words_with_numeric_hazards": numeric_hazard_words,
                "oov_words": oov_words,
                "alignment_token_word_mismatches": token_word_mismatches,
                "first_word_local_start_sample": None
                if not words
                else int(words[0]["source_start_sample"]) - int(buffer["source_start_sample"]),
                "last_word_local_end_sample": None
                if not words
                else int(words[-1]["source_end_sample"]) - int(buffer["source_start_sample"]),
                "p50_word_duration_sec": percentile(word_durations, 50),
                "p90_word_duration_sec": percentile(word_durations, 90),
                "p50_word_gap_sec": percentile(word_gaps, 50),
                "p90_word_gap_sec": percentile(word_gaps, 90),
                "asr_min_avg_logprob": None
                if not segments
                else min(float(segment["avg_logprob"]) for segment in segments),
                "asr_max_no_speech_prob": None
                if not segments
                else max(float(segment["no_speech_prob"]) for segment in segments),
                "asr_max_compression_ratio": None
                if not segments
                else max(float(segment["compression_ratio"]) for segment in segments),
                "warning_reason_codes": warning_reason_codes,
                "fatal_reason_codes": fatal_reason_codes,
                "automatic_cutpoints_disabled": bool(fatal_reason_codes),
                "reason_codes": unique_reasons,
            }
        )

    by_buffer_path = resolve_under_root(run_root, "artifacts/alignment_qc_by_buffer.json")
    write_json(by_buffer_path, by_buffer)
    summary = {
        "stage": "alignment_qc",
        "config_hash": str(config.get("config_hash") or ""),
        "input_artifact_hashes": {
            "asr_mfa_queue_json": sha256_file(queue_path),
            "normalized_transcripts_json": sha256_file(normalized_path),
            "transcripts_json": sha256_file(transcripts_path),
            "aligned_words_jsonl": sha256_file(aligned_words_path),
        },
        "buffer_count": len(buffers),
        "aligned_word_count": sum(len(words_by_buffer.get(buffer_id, [])) for buffer_id in buffers),
        "unexpected_aligned_word_count": sum(
            len(words_by_buffer[buffer_id]) for buffer_id in unexpected_aligned_word_buffer_ids
        ),
        "unexpected_aligned_word_buffer_count": len(unexpected_aligned_word_buffer_ids),
        "unexpected_aligned_word_buffer_ids": unexpected_aligned_word_buffer_ids,
        "normalized_token_count": sum(len(row.get("tokens", [])) for row in normalized.values()),
        "buffers_with_no_words": sum(row["aligned_word_count"] == 0 for row in by_buffer),
        "buffers_with_alignment_mismatch": sum(row["alignment_token_word_delta"] != 0 for row in by_buffer),
        "buffers_with_words_outside_buffer": sum(row["words_outside_buffer"] > 0 for row in by_buffer),
        "buffers_with_words_outside_trusted_chunk": sum(
            row["words_outside_trusted_chunk"] > 0 for row in by_buffer
        ),
        "buffers_with_non_positive_word_durations": sum(
            row["negative_or_zero_word_durations"] > 0 for row in by_buffer
        ),
        "buffers_with_absurdly_short_words": sum(row["absurdly_short_words"] > 0 for row in by_buffer),
        "buffers_with_absurdly_long_words": sum(row["absurdly_long_words"] > 0 for row in by_buffer),
        "buffers_with_backwards_word_order": sum(row["backwards_word_order"] > 0 for row in by_buffer),
        "buffers_with_symbol_hazards": sum(row["words_with_symbol_hazards"] > 0 for row in by_buffer),
        "buffers_with_numeric_hazards": sum(row["words_with_numeric_hazards"] > 0 for row in by_buffer),
        "buffers_with_oovs": sum(row["oov_words"] > 0 for row in by_buffer),
        "buffers_with_alignment_token_word_mismatch": sum(
            row["alignment_token_word_mismatches"] > 0 for row in by_buffer
        ),
        "buffers_with_warnings": sum(bool(row["warning_reason_codes"]) for row in by_buffer),
        "buffers_with_fatal_reasons": sum(bool(row["fatal_reason_codes"]) for row in by_buffer),
        "buffers_with_automatic_cutpoints_disabled": sum(row["automatic_cutpoints_disabled"] for row in by_buffer),
        "p50_word_duration_sec": percentile(all_word_durations, 50),
        "p90_word_duration_sec": percentile(all_word_durations, 90),
        "p50_word_gap_sec": percentile(all_word_gaps, 50),
        "p90_word_gap_sec": percentile(all_word_gaps, 90),
        "thresholds": {
            "absurdly_short_word_sec": tiny_word_sec,
            "absurdly_long_word_sec": long_word_sec,
            "trusted_edge_warn_sec": trusted_edge_warn_sec,
        },
    }
    summary_path = resolve_under_root(run_root, "artifacts/alignment_qc_summary.json")
    summary["output_hashes"] = {
        "alignment_qc_by_buffer_json": sha256_file(by_buffer_path),
    }
    write_json(summary_path, summary)
    return summary
