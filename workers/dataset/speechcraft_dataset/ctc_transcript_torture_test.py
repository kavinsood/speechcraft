from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .analyze_ctc_transcript_qc import (
    DEFAULT_MODEL,
    load_ctc_model,
    score_clip,
)
from .buffers import read_analysis_audio
from .io import read_json_value, resolve_under_root, write_json

FILLER_WORDS = [
    "BANANA",
    "ZEBRA",
    "QUANTUM",
    "HYPOTHESIS",
    "WIDGET",
    "FOOBAR",
    "XYLOPHONE",
    "NEBULA",
    "PARADOX",
    "CACTUS",
]


@dataclass(frozen=True)
class TortureConfig:
    min_baseline_score: float = 90.0
    clips_per_dataset: int = 20
    omit_tries: int = 3
    add_tries: int = 2
    replace_tries: int = 3
    seed: int = 42


def log(message: str) -> None:
    print(f"[ctc_torture_test] {message}", flush=True)


def split_words(text: str) -> list[str]:
    if "|" in text:
        return [word for word in text.split("|") if word]
    return [word for word in text.split() if word]


def join_words(words: list[str]) -> str:
    return "|".join(words)


def omit_word(text: str, index: int) -> tuple[str, str] | None:
    words = split_words(text)
    if len(words) <= 1 or index < 0 or index >= len(words):
        return None
    removed = words.pop(index)
    return join_words(words), removed


def add_word(text: str, word: str, index: int) -> str:
    words = split_words(text)
    index = max(0, min(index, len(words)))
    words.insert(index, word)
    return join_words(words)


def replace_word(text: str, index: int, new_word: str) -> tuple[str, str] | None:
    words = split_words(text)
    if index < 0 or index >= len(words):
        return None
    old_word = words[index]
    words[index] = new_word
    return join_words(words), old_word


def load_high_scoring_clips(
    qc_json_path: Path,
    *,
    min_score: float,
    limit: int,
) -> list[dict[str, Any]]:
    rows = read_json_value(qc_json_path)
    if not isinstance(rows, list):
        raise ValueError(f"{qc_json_path} must contain a list")
    eligible = [
        row
        for row in rows
        if isinstance(row, dict)
        and row.get("transcript_match_score") is not None
        and float(row["transcript_match_score"]) >= min_score
        and row.get("verifier_text")
        and row.get("bucket") != "failed"
    ]
    eligible.sort(
        key=lambda row: (
            -float(row["transcript_match_score"]),
            str(row.get("clip_id") or ""),
        )
    )
    return eligible[: max(0, limit)]


def localized_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    """Expose per-token/window CTC confidences alongside the clip average."""

    def to_pct(value: float | None) -> float | None:
        if value is None:
            return None
        return round(max(0.0, min(1.0, float(value))) * 100.0, 3)

    min_token_pct = to_pct(metrics.get("ctc_min_token_score"))
    min_window_pct = to_pct(metrics.get("ctc_min_window_score"))
    min_aligned_token_pct = to_pct(metrics.get("ctc_min_aligned_token_score"))
    mean_pct = to_pct(metrics.get("ctc_mean_score"))
    return {
        "transcript_match_score": metrics.get("transcript_match_score"),
        "ctc_mean_pct": mean_pct,
        "ctc_min_token_pct": min_token_pct,
        "ctc_min_aligned_token_pct": min_aligned_token_pct,
        "ctc_min_window_pct": min_window_pct,
        "unaligned_token_count": metrics.get("unaligned_token_count"),
        "weak_span_count": metrics.get("weak_span_count"),
        "segment_confidence_pct": to_pct(metrics.get("segment_confidence")),
        "audio_duration_sec": metrics.get("audio_duration_sec"),
        "aligned_speech_sec": metrics.get("aligned_speech_sec"),
        "unexplained_speech_sec": metrics.get("unexplained_speech_sec"),
        "aligned_speech_ratio": metrics.get("aligned_speech_ratio"),
        "unaligned_speech_ratio": metrics.get("unaligned_speech_ratio"),
        "char_timings_span_sec": metrics.get("char_timings_span_sec"),
        "bucket": metrics.get("bucket"),
    }


def build_trial_record(
    *,
    perturbation: str,
    try_index: int,
    word_index: int,
    detail: dict[str, Any],
    perturbed_text: str,
    baseline: dict[str, Any],
    perturbed: dict[str, Any],
) -> dict[str, Any]:
    baseline_score = float(baseline["transcript_match_score"])
    perturbed_score = float(perturbed["transcript_match_score"])
    baseline_min_token = baseline["ctc_min_token_pct"]
    perturbed_min_token = perturbed["ctc_min_token_pct"]
    baseline_min_aligned = baseline["ctc_min_aligned_token_pct"]
    perturbed_min_aligned = perturbed["ctc_min_aligned_token_pct"]
    baseline_min_window = baseline["ctc_min_window_pct"]
    perturbed_min_window = perturbed["ctc_min_window_pct"]

    min_token_delta = None
    if baseline_min_token is not None and perturbed_min_token is not None:
        min_token_delta = round(float(baseline_min_token) - float(perturbed_min_token), 3)
    min_aligned_delta = None
    if baseline_min_aligned is not None and perturbed_min_aligned is not None:
        min_aligned_delta = round(float(baseline_min_aligned) - float(perturbed_min_aligned), 3)
    min_window_delta = None
    if baseline_min_window is not None and perturbed_min_window is not None:
        min_window_delta = round(float(baseline_min_window) - float(perturbed_min_window), 3)

    unaligned_delta = None
    if baseline.get("unaligned_token_count") is not None and perturbed.get("unaligned_token_count") is not None:
        unaligned_delta = int(perturbed["unaligned_token_count"]) - int(baseline["unaligned_token_count"])
    weak_span_delta = None
    if baseline.get("weak_span_count") is not None and perturbed.get("weak_span_count") is not None:
        weak_span_delta = int(perturbed["weak_span_count"]) - int(baseline["weak_span_count"])

    unaligned_speech_ratio_delta = None
    if baseline.get("unaligned_speech_ratio") is not None and perturbed.get("unaligned_speech_ratio") is not None:
        unaligned_speech_ratio_delta = round(
            float(perturbed["unaligned_speech_ratio"]) - float(baseline["unaligned_speech_ratio"]),
            6,
        )
    unexplained_speech_sec_delta = None
    if baseline.get("unexplained_speech_sec") is not None and perturbed.get("unexplained_speech_sec") is not None:
        unexplained_speech_sec_delta = round(
            float(perturbed["unexplained_speech_sec"]) - float(baseline["unexplained_speech_sec"]),
            6,
        )
    aligned_speech_ratio_delta = None
    if baseline.get("aligned_speech_ratio") is not None and perturbed.get("aligned_speech_ratio") is not None:
        aligned_speech_ratio_delta = round(
            float(perturbed["aligned_speech_ratio"]) - float(baseline["aligned_speech_ratio"]),
            6,
        )

    poison_threshold_pct = 40.0
    unaligned_speech_threshold = 0.15
    poisoned_by_min_window = perturbed_min_window is not None and float(perturbed_min_window) < poison_threshold_pct
    poisoned_by_min_aligned = perturbed_min_aligned is not None and float(perturbed_min_aligned) < poison_threshold_pct
    poisoned_by_weak_spans = (weak_span_delta or 0) > 0
    poisoned_by_unaligned_speech = (
        perturbed.get("unaligned_speech_ratio") is not None
        and float(perturbed["unaligned_speech_ratio"]) >= unaligned_speech_threshold
    )
    omission_signal = (
        perturbation == "omit"
        and unaligned_speech_ratio_delta is not None
        and unaligned_speech_ratio_delta > 0.01
    )

    return {
        "perturbation": perturbation,
        "try_index": try_index,
        "word_index": word_index,
        "detail": detail,
        "perturbed_text": perturbed_text,
        "baseline_score": baseline_score,
        "perturbed_score": perturbed_score,
        "score_delta": round(baseline_score - perturbed_score, 3),
        "baseline_min_token_pct": baseline_min_token,
        "perturbed_min_token_pct": perturbed_min_token,
        "min_token_delta": min_token_delta,
        "baseline_min_aligned_token_pct": baseline_min_aligned,
        "perturbed_min_aligned_token_pct": perturbed_min_aligned,
        "min_aligned_token_delta": min_aligned_delta,
        "baseline_min_window_pct": baseline_min_window,
        "perturbed_min_window_pct": perturbed_min_window,
        "min_window_delta": min_window_delta,
        "baseline_mean_pct": baseline["ctc_mean_pct"],
        "perturbed_mean_pct": perturbed["ctc_mean_pct"],
        "mean_pct_delta": (
            round(float(baseline["ctc_mean_pct"]) - float(perturbed["ctc_mean_pct"]), 3)
            if baseline["ctc_mean_pct"] is not None and perturbed["ctc_mean_pct"] is not None
            else None
        ),
        "baseline_unaligned_token_count": baseline["unaligned_token_count"],
        "perturbed_unaligned_token_count": perturbed["unaligned_token_count"],
        "unaligned_token_delta": unaligned_delta,
        "baseline_weak_span_count": baseline["weak_span_count"],
        "perturbed_weak_span_count": perturbed["weak_span_count"],
        "weak_span_delta": weak_span_delta,
        "baseline_unaligned_speech_ratio": baseline.get("unaligned_speech_ratio"),
        "perturbed_unaligned_speech_ratio": perturbed.get("unaligned_speech_ratio"),
        "unaligned_speech_ratio_delta": unaligned_speech_ratio_delta,
        "baseline_unexplained_speech_sec": baseline.get("unexplained_speech_sec"),
        "perturbed_unexplained_speech_sec": perturbed.get("unexplained_speech_sec"),
        "unexplained_speech_sec_delta": unexplained_speech_sec_delta,
        "baseline_aligned_speech_ratio": baseline.get("aligned_speech_ratio"),
        "perturbed_aligned_speech_ratio": perturbed.get("aligned_speech_ratio"),
        "aligned_speech_ratio_delta": aligned_speech_ratio_delta,
        "omission_signal": omission_signal,
        "poisoned_by_min_window": poisoned_by_min_window,
        "poisoned_by_min_aligned_token": poisoned_by_min_aligned,
        "poisoned_by_weak_spans": poisoned_by_weak_spans,
        "poisoned_by_unaligned_speech": poisoned_by_unaligned_speech,
        "poisoned_detected": (
            poisoned_by_min_window
            or poisoned_by_min_aligned
            or poisoned_by_weak_spans
            or poisoned_by_unaligned_speech
        ),
        "bucket": perturbed["bucket"],
    }


def score_text_on_clip(
    run_root: Path,
    clip_row: dict[str, Any],
    verifier_text: str,
    bundle: Any,
) -> dict[str, Any]:
    audio_rel = clip_row.get("audio_path")
    if not audio_rel:
        raise ValueError("missing audio_path")
    audio_path = resolve_under_root(run_root, str(audio_rel))
    audio, sample_rate = read_analysis_audio(audio_path)
    return score_clip(audio, sample_rate, verifier_text, bundle)


def run_clip_torture(
    run_root: Path,
    clip_row: dict[str, Any],
    bundle: Any,
    config: TortureConfig,
    rng: random.Random,
) -> dict[str, Any]:
    clip_id = str(clip_row.get("clip_id") or "")
    baseline_text = str(clip_row["verifier_text"])
    words = split_words(baseline_text)
    if len(words) < 3:
        return {
            "clip_id": clip_id,
            "skipped": True,
            "reason": "too_few_words",
            "word_count": len(words),
        }

    baseline_metrics = localized_metrics(score_text_on_clip(run_root, clip_row, baseline_text, bundle))
    baseline_score = float(baseline_metrics["transcript_match_score"])
    trials: list[dict[str, Any]] = []

    for try_index in range(config.omit_tries):
        index = rng.randrange(len(words))
        omitted = omit_word(baseline_text, index)
        if omitted is None:
            continue
        perturbed_text, removed_word = omitted
        perturbed_metrics = localized_metrics(score_text_on_clip(run_root, clip_row, perturbed_text, bundle))
        trials.append(
            build_trial_record(
                perturbation="omit",
                try_index=try_index,
                word_index=index,
                detail={"removed_word": removed_word},
                perturbed_text=perturbed_text,
                baseline=baseline_metrics,
                perturbed=perturbed_metrics,
            )
        )

    for try_index in range(config.add_tries):
        word = rng.choice(FILLER_WORDS)
        index = rng.randrange(len(words) + 1)
        perturbed_text = add_word(baseline_text, word, index)
        perturbed_metrics = localized_metrics(score_text_on_clip(run_root, clip_row, perturbed_text, bundle))
        trials.append(
            build_trial_record(
                perturbation="add",
                try_index=try_index,
                word_index=index,
                detail={"added_word": word},
                perturbed_text=perturbed_text,
                baseline=baseline_metrics,
                perturbed=perturbed_metrics,
            )
        )

    for try_index in range(config.replace_tries):
        index = rng.randrange(len(words))
        new_word = rng.choice(FILLER_WORDS)
        replaced = replace_word(baseline_text, index, new_word)
        if replaced is None:
            continue
        perturbed_text, old_word = replaced
        perturbed_metrics = localized_metrics(score_text_on_clip(run_root, clip_row, perturbed_text, bundle))
        trials.append(
            build_trial_record(
                perturbation="replace",
                try_index=try_index,
                word_index=index,
                detail={"old_word": old_word, "new_word": new_word},
                perturbed_text=perturbed_text,
                baseline=baseline_metrics,
                perturbed=perturbed_metrics,
            )
        )

    deltas = [float(trial["score_delta"]) for trial in trials]
    min_token_deltas = [float(trial["min_token_delta"]) for trial in trials if trial.get("min_token_delta") is not None]
    return {
        "clip_id": clip_id,
        "audio_path": clip_row.get("audio_path"),
        "baseline_text": baseline_text,
        "word_count": len(words),
        "baseline_score": baseline_score,
        "baseline_min_token_pct": baseline_metrics["ctc_min_token_pct"],
        "baseline_min_window_pct": baseline_metrics["ctc_min_window_pct"],
        "baseline_mean_pct": baseline_metrics["ctc_mean_pct"],
        "baseline_unaligned_speech_ratio": baseline_metrics.get("unaligned_speech_ratio"),
        "baseline_unexplained_speech_sec": baseline_metrics.get("unexplained_speech_sec"),
        "baseline_bucket": baseline_metrics["bucket"],
        "cached_score": clip_row.get("transcript_match_score"),
        "trial_count": len(trials),
        "mean_score_delta": round(sum(deltas) / len(deltas), 3) if deltas else None,
        "min_score_delta": round(min(deltas), 3) if deltas else None,
        "max_score_delta": round(max(deltas), 3) if deltas else None,
        "mean_min_token_delta": round(sum(min_token_deltas) / len(min_token_deltas), 3) if min_token_deltas else None,
        "trials": trials,
    }


def summarize_trials(all_trials: list[dict[str, Any]]) -> dict[str, Any]:
    flat: list[dict[str, Any]] = []
    for clip_result in all_trials:
        if clip_result.get("skipped"):
            continue
        for trial in clip_result.get("trials") or []:
            flat.append({**trial, "clip_id": clip_result["clip_id"]})

    if not flat:
        return {
            "trial_count": 0,
            "by_perturbation": {},
            "localized": {},
        }

    by_type: dict[str, list[dict[str, Any]]] = {}
    for trial in flat:
        by_type.setdefault(str(trial["perturbation"]), []).append(trial)

    def mean_score_stats(trials: list[dict[str, Any]]) -> dict[str, float | int]:
        values = [float(trial["score_delta"]) for trial in trials]
        return {
            "count": len(values),
            "mean_delta": round(sum(values) / len(values), 3),
            "median_delta": round(sorted(values)[len(values) // 2], 3),
            "min_delta": round(min(values), 3),
            "max_delta": round(max(values), 3),
            "pct_dropped_any": round(100.0 * sum(value > 0 for value in values) / len(values), 1),
            "pct_dropped_ge_5": round(100.0 * sum(value >= 5 for value in values) / len(values), 1),
            "pct_dropped_ge_10": round(100.0 * sum(value >= 10 for value in values) / len(values), 1),
        }

    def coverage_stats(trials: list[dict[str, Any]]) -> dict[str, float | int | None]:
        ratio_deltas = [
            float(trial["unaligned_speech_ratio_delta"])
            for trial in trials
            if trial.get("unaligned_speech_ratio_delta") is not None
        ]
        sec_deltas = [
            float(trial["unexplained_speech_sec_delta"])
            for trial in trials
            if trial.get("unexplained_speech_sec_delta") is not None
        ]
        perturbed_ratios = [
            float(trial["perturbed_unaligned_speech_ratio"])
            for trial in trials
            if trial.get("perturbed_unaligned_speech_ratio") is not None
        ]
        return {
            "mean_unaligned_speech_ratio_delta": round(sum(ratio_deltas) / len(ratio_deltas), 6) if ratio_deltas else None,
            "median_unaligned_speech_ratio_delta": round(sorted(ratio_deltas)[len(ratio_deltas) // 2], 6) if ratio_deltas else None,
            "max_unaligned_speech_ratio_delta": round(max(ratio_deltas), 6) if ratio_deltas else None,
            "mean_unexplained_speech_sec_delta": round(sum(sec_deltas) / len(sec_deltas), 6) if sec_deltas else None,
            "max_unexplained_speech_sec_delta": round(max(sec_deltas), 6) if sec_deltas else None,
            "pct_unaligned_ratio_increased": round(100.0 * sum(value > 0 for value in ratio_deltas) / len(ratio_deltas), 1) if ratio_deltas else None,
            "pct_unaligned_ratio_ge_0_05": round(100.0 * sum(value >= 0.05 for value in ratio_deltas) / len(ratio_deltas), 1) if ratio_deltas else None,
            "pct_omission_signal": round(100.0 * sum(trial.get("omission_signal") for trial in trials) / len(trials), 1),
            "pct_poisoned_by_unaligned_speech": round(
                100.0 * sum(trial.get("poisoned_by_unaligned_speech") for trial in trials) / len(trials),
                1,
            ),
            "max_perturbed_unaligned_speech_ratio": round(max(perturbed_ratios), 6) if perturbed_ratios else None,
        }

    def localized_stats(trials: list[dict[str, Any]]) -> dict[str, float | int]:
        min_window_deltas = [float(trial["min_window_delta"]) for trial in trials if trial.get("min_window_delta") is not None]
        min_aligned_deltas = [float(trial["min_aligned_token_delta"]) for trial in trials if trial.get("min_aligned_token_delta") is not None]
        perturbed_min_windows = [float(trial["perturbed_min_window_pct"]) for trial in trials if trial.get("perturbed_min_window_pct") is not None]
        perturbed_min_aligned = [float(trial["perturbed_min_aligned_token_pct"]) for trial in trials if trial.get("perturbed_min_aligned_token_pct") is not None]
        return {
            "count": len(trials),
            "mean_min_window_delta": round(sum(min_window_deltas) / len(min_window_deltas), 3) if min_window_deltas else None,
            "median_min_window_delta": round(sorted(min_window_deltas)[len(min_window_deltas) // 2], 3) if min_window_deltas else None,
            "max_min_window_delta": round(max(min_window_deltas), 3) if min_window_deltas else None,
            "pct_min_window_delta_ge_20": round(100.0 * sum(value >= 20 for value in min_window_deltas) / len(min_window_deltas), 1) if min_window_deltas else None,
            "mean_min_aligned_token_delta": round(sum(min_aligned_deltas) / len(min_aligned_deltas), 3) if min_aligned_deltas else None,
            "min_perturbed_min_window_pct": round(min(perturbed_min_windows), 3) if perturbed_min_windows else None,
            "min_perturbed_min_aligned_token_pct": round(min(perturbed_min_aligned), 3) if perturbed_min_aligned else None,
            "pct_poisoned_by_min_window": round(100.0 * sum(trial.get("poisoned_by_min_window") for trial in trials) / len(trials), 1),
            "pct_poisoned_by_min_aligned_token": round(100.0 * sum(trial.get("poisoned_by_min_aligned_token") for trial in trials) / len(trials), 1),
            "pct_poisoned_by_weak_spans": round(100.0 * sum(trial.get("poisoned_by_weak_spans") for trial in trials) / len(trials), 1),
            "pct_poisoned_detected": round(100.0 * sum(trial.get("poisoned_detected") for trial in trials) / len(trials), 1),
            "coverage": coverage_stats(trials),
            "notes": {
                "ctc_min_token_score_is_degenerate": "Raw min over all char_probs includes blank frames and is usually 0; use min_window or min_aligned_token.",
            },
        }

    def hidden_needle_cases(trials: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Average barely moved, but localized min-window score crashed — Linus's mean-score trap."""
        cases = [
            trial
            for trial in trials
            if float(trial.get("score_delta") or 0.0) < 5.0
            and trial.get("min_window_delta") is not None
            and float(trial["min_window_delta"]) >= 20.0
        ]
        return sorted(cases, key=lambda row: -float(row["min_window_delta"]))[:10]

    def omission_coverage_cases(trials: list[dict[str, Any]]) -> list[dict[str, Any]]:
        cases = [
            trial
            for trial in trials
            if trial.get("perturbation") == "omit"
            and trial.get("unaligned_speech_ratio_delta") is not None
        ]
        return sorted(cases, key=lambda row: -float(row["unaligned_speech_ratio_delta"]))[:10]

    overall_localized = localized_stats(flat)
    sharpest_guillotine = sorted(
        flat,
        key=lambda row: (
            -float(row.get("min_window_delta") or 0.0),
            -float(row.get("min_aligned_token_delta") or 0.0),
        ),
    )[:10]

    return {
        "trial_count": len(flat),
        "clip_count": len([row for row in all_trials if not row.get("skipped")]),
        "overall": mean_score_stats(flat),
        "localized": overall_localized,
        "by_perturbation": {
            name: {
                "mean_score": mean_score_stats(trials),
                "localized": localized_stats(trials),
            }
            for name, trials in sorted(by_type.items())
        },
        "sharpest_guillotine": sharpest_guillotine,
        "hidden_needle_cases": hidden_needle_cases(flat),
        "top_omission_coverage_cases": omission_coverage_cases(flat),
        "worst_mean_score_responses": sorted(flat, key=lambda row: float(row["score_delta"]))[:10],
        "best_mean_score_responses": sorted(flat, key=lambda row: -float(row["score_delta"]))[:10],
    }


def run_dataset_torture(
    *,
    dataset_name: str,
    run_root: Path,
    qc_json_path: Path,
    out_dir: Path,
    bundle: Any,
    config: TortureConfig,
    seed_offset: int,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    clips = load_high_scoring_clips(
        qc_json_path,
        min_score=config.min_baseline_score,
        limit=config.clips_per_dataset,
    )
    log(f"{dataset_name}: torturing {len(clips)} clips with score >= {config.min_baseline_score}")

    rng = random.Random(config.seed + seed_offset)
    clip_results: list[dict[str, Any]] = []
    for index, clip_row in enumerate(clips, start=1):
        log(f"{dataset_name}: clip {index}/{len(clips)} {clip_row.get('clip_id')}")
        clip_results.append(run_clip_torture(run_root, clip_row, bundle, config, rng))

    summary = summarize_trials(clip_results)
    payload = {
        "dataset": dataset_name,
        "run_root": str(run_root),
        "qc_json_path": str(qc_json_path),
        "config": {
            "min_baseline_score": config.min_baseline_score,
            "clips_per_dataset": config.clips_per_dataset,
            "omit_tries": config.omit_tries,
            "add_tries": config.add_tries,
            "replace_tries": config.replace_tries,
            "seed": config.seed + seed_offset,
        },
        "summary": summary,
        "clips": clip_results,
    }
    write_json(out_dir / "ctc_torture_test.json", payload)
    write_json(out_dir / "ctc_torture_test_summary.json", {"dataset": dataset_name, **summary})

    csv_rows: list[dict[str, Any]] = []
    for clip_result in clip_results:
        if clip_result.get("skipped"):
            continue
        for trial in clip_result.get("trials") or []:
            csv_rows.append(
                {
                    "clip_id": clip_result["clip_id"],
                    "perturbation": trial["perturbation"],
                    "try_index": trial["try_index"],
                    "word_index": trial["word_index"],
                    "baseline_score": trial["baseline_score"],
                    "perturbed_score": trial["perturbed_score"],
                    "score_delta": trial["score_delta"],
                    "baseline_min_token_pct": trial["baseline_min_token_pct"],
                    "perturbed_min_token_pct": trial["perturbed_min_token_pct"],
                    "min_token_delta": trial["min_token_delta"],
                    "baseline_min_window_pct": trial["baseline_min_window_pct"],
                    "perturbed_min_window_pct": trial["perturbed_min_window_pct"],
                    "min_window_delta": trial["min_window_delta"],
                    "baseline_min_aligned_token_pct": trial["baseline_min_aligned_token_pct"],
                    "perturbed_min_aligned_token_pct": trial["perturbed_min_aligned_token_pct"],
                    "min_aligned_token_delta": trial["min_aligned_token_delta"],
                    "weak_span_delta": trial["weak_span_delta"],
                    "unaligned_token_delta": trial["unaligned_token_delta"],
                    "baseline_unaligned_speech_ratio": trial["baseline_unaligned_speech_ratio"],
                    "perturbed_unaligned_speech_ratio": trial["perturbed_unaligned_speech_ratio"],
                    "unaligned_speech_ratio_delta": trial["unaligned_speech_ratio_delta"],
                    "unexplained_speech_sec_delta": trial["unexplained_speech_sec_delta"],
                    "omission_signal": trial["omission_signal"],
                    "poisoned_detected": trial["poisoned_detected"],
                    "bucket": trial["bucket"],
                    "detail": json.dumps(trial.get("detail") or {}, sort_keys=True),
                }
            )
    with (out_dir / "ctc_torture_test_trials.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "clip_id",
                "perturbation",
                "try_index",
                "word_index",
                "baseline_score",
                "perturbed_score",
                "score_delta",
                "baseline_min_token_pct",
                "perturbed_min_token_pct",
                "min_token_delta",
                "baseline_min_window_pct",
                "perturbed_min_window_pct",
                "min_window_delta",
                "baseline_min_aligned_token_pct",
                "perturbed_min_aligned_token_pct",
                "min_aligned_token_delta",
                "weak_span_delta",
                "unaligned_token_delta",
                "baseline_unaligned_speech_ratio",
                "perturbed_unaligned_speech_ratio",
                "unaligned_speech_ratio_delta",
                "unexplained_speech_sec_delta",
                "omission_signal",
                "poisoned_detected",
                "bucket",
                "detail",
            ],
        )
        writer.writeheader()
        writer.writerows(csv_rows)

    log(
        f"{dataset_name}: {summary['trial_count']} trials, "
        f"mean score delta={summary.get('overall', {}).get('mean_delta')} "
        f"mean min_window delta={summary.get('localized', {}).get('mean_min_window_delta')} "
        f"mean unaligned ratio delta={summary.get('localized', {}).get('coverage', {}).get('mean_unaligned_speech_ratio_delta')} "
        f"poisoned={summary.get('localized', {}).get('pct_poisoned_detected')}%"
    )
    return payload


def run_torture_suite(
    datasets: list[dict[str, str]],
    out_root: Path,
    *,
    model_name: str = DEFAULT_MODEL,
    device: str = "auto",
    config: TortureConfig | None = None,
) -> dict[str, Any]:
    config = config or TortureConfig()
    out_root.mkdir(parents=True, exist_ok=True)
    bundle = load_ctc_model(model_name, device)

    results: dict[str, Any] = {}
    all_clip_results: list[dict[str, Any]] = []
    for offset, dataset in enumerate(datasets):
        name = dataset["name"]
        payload = run_dataset_torture(
            dataset_name=name,
            run_root=Path(dataset["run_root"]).expanduser().resolve(),
            qc_json_path=Path(dataset["qc_json"]).expanduser().resolve(),
            out_dir=out_root / name,
            bundle=bundle,
            config=config,
            seed_offset=offset * 1000,
        )
        results[name] = payload["summary"]
        all_clip_results.extend(payload["clips"])

    suite_summary = {
        "datasets": results,
        "combined": summarize_trials(all_clip_results),
    }
    write_json(out_root / "ctc_torture_test_suite_summary.json", suite_summary)
    return suite_summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Torture-test CTC transcript QC by omitting/adding/replacing words on high-scoring clips.",
    )
    parser.add_argument("--out", required=True)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--device", choices=["auto", "cuda", "cpu"], default="auto")
    parser.add_argument("--min-baseline-score", type=float, default=TortureConfig.min_baseline_score)
    parser.add_argument("--clips-per-dataset", type=int, default=TortureConfig.clips_per_dataset)
    parser.add_argument("--omit-tries", type=int, default=TortureConfig.omit_tries)
    parser.add_argument("--add-tries", type=int, default=TortureConfig.add_tries)
    parser.add_argument("--replace-tries", type=int, default=TortureConfig.replace_tries)
    parser.add_argument("--seed", type=int, default=TortureConfig.seed)
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=["mb", "mc", "poki"],
        help="Dataset keys from the built-in registry (mb, mc, poki).",
    )
    return parser


DATASET_REGISTRY: dict[str, dict[str, str]] = {
    "mb": {
        "run_root": "backend/data/media/dataset-runs/mb-mq3wkz25/dataset-17d22e1684db",
        "qc_json": "backend/data/eval_ctc_qc/mb/ctc_transcript_qc.json",
    },
    "mc": {
        "run_root": "backend/data/media/dataset-runs/mc-mq3xswcx/dataset-4a3f897e9802",
        "qc_json": "backend/data/eval_ctc_qc/mc/ctc_transcript_qc.json",
    },
    "poki": {
        "run_root": "backend/data/media/dataset-runs/pokimanev1-mq76fser/dataset-845dff8c627c",
        "qc_json": "backend/data/eval_ctc_qc/poki/ctc_transcript_qc.json",
    },
}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    config = TortureConfig(
        min_baseline_score=args.min_baseline_score,
        clips_per_dataset=args.clips_per_dataset,
        omit_tries=args.omit_tries,
        add_tries=args.add_tries,
        replace_tries=args.replace_tries,
        seed=args.seed,
    )
    selected = []
    for name in args.datasets:
        if name not in DATASET_REGISTRY:
            raise SystemExit(f"Unknown dataset {name!r}; choose from {sorted(DATASET_REGISTRY)}")
        entry = DATASET_REGISTRY[name]
        selected.append(
            {
                "name": name,
                "run_root": str((repo_root / entry["run_root"]).resolve()),
                "qc_json": str((repo_root / entry["qc_json"]).resolve()),
            }
        )

    summary = run_torture_suite(
        selected,
        Path(args.out).expanduser().resolve(),
        model_name=args.model,
        device=args.device,
        config=config,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
