from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .buffers import read_analysis_audio
from .eval_speaker_purity import DEFAULT_MODEL, SpeakerPurityConfig, load_titanet_model, score_candidate_clip
from .eval_speaker_purity_cocktail_test import (
    CocktailConfig,
    DATASET_REGISTRY,
    append_intruder,
    blend_underneath,
    export_variant_bundle,
    hard_splice_duration,
    intruder_regions,
    load_intruder_source_cache,
    load_voiceprint,
    pick_intruder_excerpt,
    prepend_intruder,
    replace_segment,
    select_clean_clips,
)
from .io import read_json_value, read_jsonl, resolve_under_root, write_json


@dataclass(frozen=True)
class BatteryVariant:
    name: str
    category: str
    duration_sec: float
    description: str


def log(message: str) -> None:
    print(f"[speaker_purity_battery] {message}", flush=True)


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def battery_variants() -> list[BatteryVariant]:
    return [
        BatteryVariant("splice_400ms_mid", "splice", 0.4, "400ms hard splice at midpoint"),
        BatteryVariant("splice_1s_mid", "splice", 1.0, "1s hard splice at midpoint"),
        BatteryVariant("splice_2s_mid", "splice", 2.0, "2s hard splice at midpoint"),
        BatteryVariant("splice_1s_start", "splice", 1.0, "1s hard splice at clip start"),
        BatteryVariant("splice_1s_end", "splice", 1.0, "1s hard splice at clip end"),
        BatteryVariant("blend_25_2s_mid", "blend", 2.0, "2s overlap blend at 25% intruder volume"),
        BatteryVariant("blend_50_2s_mid", "blend", 2.0, "2s overlap blend at 50% intruder volume"),
        BatteryVariant("blend_75_2s_mid", "blend", 2.0, "2s overlap blend at 75% intruder volume"),
        BatteryVariant("replace_2s_mid", "replace", 2.0, "2s segment fully replaced with intruder"),
        BatteryVariant("blend_50_1s_mid", "blend", 1.0, "1s overlap blend at 50%"),
        BatteryVariant("blend_50_4s_mid", "blend", 4.0, "4s overlap blend at 50%"),
        BatteryVariant("blend_50_2s_start", "blend", 2.0, "2s overlap blend at 50%, clip start"),
        BatteryVariant("blend_50_2s_end", "blend", 2.0, "2s overlap blend at 50%, clip end"),
        BatteryVariant("append_1s", "sequential", 1.0, "1s intruder appended after clip"),
        BatteryVariant("prepend_1s", "sequential", 1.0, "1s intruder prepended before clip"),
    ]


def apply_variant(
    variant: BatteryVariant,
    target: np.ndarray,
    intruder: np.ndarray,
    sample_rate: int,
) -> tuple[np.ndarray, int, int]:
    if variant.name == "splice_400ms_mid":
        return hard_splice_duration(target, intruder, sample_rate, duration_sec=0.4, placement="mid")
    if variant.name == "splice_1s_mid":
        return hard_splice_duration(target, intruder, sample_rate, duration_sec=1.0, placement="mid")
    if variant.name == "splice_2s_mid":
        return hard_splice_duration(target, intruder, sample_rate, duration_sec=2.0, placement="mid")
    if variant.name == "splice_1s_start":
        return hard_splice_duration(target, intruder, sample_rate, duration_sec=1.0, placement="start")
    if variant.name == "splice_1s_end":
        return hard_splice_duration(target, intruder, sample_rate, duration_sec=1.0, placement="end")
    if variant.name == "blend_25_2s_mid":
        return blend_underneath(target, intruder, sample_rate, blend_sec=2.0, mix_ratio=0.25, placement="mid")
    if variant.name == "blend_50_2s_mid":
        return blend_underneath(target, intruder, sample_rate, blend_sec=2.0, mix_ratio=0.50, placement="mid")
    if variant.name == "blend_75_2s_mid":
        return blend_underneath(target, intruder, sample_rate, blend_sec=2.0, mix_ratio=0.75, placement="mid")
    if variant.name == "replace_2s_mid":
        return replace_segment(target, intruder, sample_rate, duration_sec=2.0, placement="mid")
    if variant.name == "blend_50_1s_mid":
        return blend_underneath(target, intruder, sample_rate, blend_sec=1.0, mix_ratio=0.50, placement="mid")
    if variant.name == "blend_50_4s_mid":
        return blend_underneath(target, intruder, sample_rate, blend_sec=4.0, mix_ratio=0.50, placement="mid")
    if variant.name == "blend_50_2s_start":
        return blend_underneath(target, intruder, sample_rate, blend_sec=2.0, mix_ratio=0.50, placement="start")
    if variant.name == "blend_50_2s_end":
        return blend_underneath(target, intruder, sample_rate, blend_sec=2.0, mix_ratio=0.50, placement="end")
    if variant.name == "append_1s":
        return append_intruder(target, intruder, sample_rate, duration_sec=1.0)
    if variant.name == "prepend_1s":
        return prepend_intruder(target, intruder, sample_rate, duration_sec=1.0)
    raise ValueError(f"Unknown variant {variant.name}")


def mix_ratio_for_variant(name: str) -> float | None:
    if name.startswith("blend_25"):
        return 0.25
    if name.startswith("blend_50"):
        return 0.50
    if name.startswith("blend_75"):
        return 0.75
    if name.startswith("replace"):
        return 1.0
    return None


def summarize_battery(trials: list[dict[str, Any]], *, purity_threshold: float, overlap_blind_threshold: float) -> dict[str, Any]:
    by_variant: dict[str, list[dict[str, Any]]] = {}
    by_category: dict[str, list[dict[str, Any]]] = {}
    for trial in trials:
        by_variant.setdefault(str(trial["variant"]), []).append(trial)
        by_category.setdefault(str(trial["category"]), []).append(trial)

    def stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
        scores = [float(row["purity_score"]) for row in rows if row.get("purity_score") is not None]
        deltas = [float(row["score_delta"]) for row in rows if row.get("score_delta") is not None]
        return {
            "count": len(rows),
            "score_min": _round(min(scores)) if scores else None,
            "score_p50": _round(sorted(scores)[len(scores) // 2]) if scores else None,
            "mean_score_delta": _round(sum(deltas) / len(deltas)) if deltas else None,
            "pct_below_threshold": round(100.0 * sum(s < purity_threshold for s in scores) / len(scores), 1) if scores else None,
            "pct_overlap_blind": round(
                100.0 * sum(s >= overlap_blind_threshold for s in scores) / len(scores),
                1,
            )
            if scores
            else None,
        }

    natural = [row for row in trials if row.get("variant") == "natural_worst_rescore"]
    return {
        "trial_count": len(trials),
        "by_variant": {name: stats(rows) for name, rows in sorted(by_variant.items())},
        "by_category": {name: stats(rows) for name, rows in sorted(by_category.items())},
        "natural_worst_rescore": stats(natural) if natural else None,
        "overlap_blind_cases": [
            row
            for row in trials
            if row.get("overlap_blind") and row.get("variant") != "natural_worst_rescore"
        ][:15],
        "most_sensitive_variants": sorted(
            [
                {"variant": name, **stats(rows)}
                for name, rows in by_variant.items()
                if name != "natural_worst_rescore"
            ],
            key=lambda row: -(row.get("pct_below_threshold") or 0.0),
        )[:8],
    }


def select_natural_worst_clips(purity_qc_path: Path, *, limit: int = 10) -> list[dict[str, Any]]:
    rows = read_json_value(purity_qc_path)
    if not isinstance(rows, list):
        return []
    worst = [
        row
        for row in rows
        if isinstance(row, dict)
        and row.get("purity_score") is not None
        and row.get("bucket") in {"contaminated", "suspicious"}
        and row.get("audio_path")
    ]
    worst.sort(key=lambda row: (float(row["purity_score"]), str(row.get("clip_id") or "")))
    return worst[: max(0, limit)]


def should_export_variant(trial: dict[str, Any], *, overlap_blind_threshold: float) -> bool:
    score = trial.get("purity_score")
    if score is None:
        return False
    if trial.get("variant") == "baseline":
        return True
    if trial.get("bucket") in {"contaminated", "suspicious", "failed"}:
        return True
    if float(score) >= overlap_blind_threshold and trial.get("category") in {"blend", "replace"}:
        return True
    if trial.get("category") == "blend" and (trial.get("score_delta") or 1.0) < 0.03:
        return True
    return False


def run_battery(
    run_root: Path,
    out_dir: Path,
    *,
    dataset_name: str,
    voiceprint_path: Path,
    purity_qc_path: Path,
    cocktail_config: CocktailConfig,
    model_name: str = DEFAULT_MODEL,
    device: str = "auto",
    include_natural_worst: bool = True,
) -> dict[str, Any]:
    purity_config = SpeakerPurityConfig(purity_threshold=cocktail_config.purity_threshold)
    run_root = run_root.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    clips_dir = out_dir / "battery_clips"
    clips_dir.mkdir(parents=True, exist_ok=True)

    centroid, voiceprint = load_voiceprint(voiceprint_path)
    clean_clips = select_clean_clips(
        purity_qc_path,
        min_purity=cocktail_config.min_baseline_purity,
        min_duration_sec=cocktail_config.min_clip_duration_sec,
        limit=cocktail_config.clips_limit,
    )
    if not clean_clips:
        raise ValueError(f"No eligible clean clips for {dataset_name}")

    speaker_regions = read_jsonl(resolve_under_root(run_root, "artifacts/speaker_regions.jsonl"))
    _paths, _rates, load_source = load_intruder_source_cache(run_root)
    regions = intruder_regions(
        speaker_regions,
        intruder_speaker_id=cocktail_config.intruder_speaker_id,
        min_region_sec=4.0,
    )
    if not regions:
        raise ValueError(f"No intruder regions for {cocktail_config.intruder_speaker_id}")

    intruder_region = regions[0]
    source_samples, sample_rate = load_source(str(intruder_region["source_audio_id"]))
    region_samples = source_samples[int(intruder_region["start_sample"]) : int(intruder_region["end_sample"])]
    max_duration = max(variant.duration_sec for variant in battery_variants())
    intruder_pool, pool_offset = pick_intruder_excerpt(region_samples, sample_rate, duration_sec=max_duration)

    log(f"{dataset_name}: intruder {cocktail_config.intruder_speaker_id} region {intruder_region.get('id')}")
    log(f"{dataset_name}: {len(clean_clips)} clean clips x {len(battery_variants())} synthetic variants")

    model = load_titanet_model(model_name, device)
    variants = battery_variants()
    trials: list[dict[str, Any]] = []

    for rank, clip_row in enumerate(clean_clips, start=1):
        clip_id = str(clip_row["clip_id"])
        target_samples, actual_rate = read_analysis_audio(resolve_under_root(run_root, str(clip_row["audio_path"])))
        if actual_rate != sample_rate:
            raise ValueError(f"Unexpected sample rate for {clip_id}")

        baseline_metrics = score_candidate_clip(model, target_samples, sample_rate, centroid, purity_config)
        baseline_score = float(baseline_metrics["purity_score"])

        if should_export_variant({"variant": "baseline", "purity_score": baseline_score, "bucket": baseline_metrics["bucket"]}, overlap_blind_threshold=cocktail_config.overlap_blind_threshold):
            export_variant_bundle(
                clips_dir,
                rank=rank,
                clip_id=clip_id,
                variant="baseline",
                samples=target_samples,
                sample_rate=sample_rate,
                metrics=baseline_metrics,
                contamination={},
            )

        for variant in variants:
            excerpt, excerpt_offset = pick_intruder_excerpt(region_samples, sample_rate, duration_sec=variant.duration_sec)
            perturbed, start, end = apply_variant(variant, target_samples, excerpt, sample_rate)
            metrics = score_candidate_clip(model, perturbed, sample_rate, centroid, purity_config)
            score = metrics.get("purity_score")
            trial = {
                "dataset": dataset_name,
                "clip_id": clip_id,
                "variant": variant.name,
                "category": variant.category,
                "description": variant.description,
                "baseline_score": baseline_score,
                "purity_score": score,
                "mean_window_similarity": metrics.get("mean_window_similarity"),
                "score_delta": round(baseline_score - float(score), 6) if score is not None else None,
                "bucket": metrics.get("bucket"),
                "worst_window_start_sec": metrics.get("worst_window_start_sec"),
                "intruder_window_count": metrics.get("intruder_window_count"),
                "mix_ratio": mix_ratio_for_variant(variant.name),
                "contamination_start_sec": round(start / sample_rate, 6),
                "contamination_end_sec": round(end / sample_rate, 6),
                "overlap_blind": score is not None and float(score) >= cocktail_config.overlap_blind_threshold,
                "exported_wav": None,
            }
            if should_export_variant(trial, overlap_blind_threshold=cocktail_config.overlap_blind_threshold):
                trial["exported_wav"] = export_variant_bundle(
                    clips_dir,
                    rank=rank,
                    clip_id=clip_id,
                    variant=variant.name,
                    samples=perturbed,
                    sample_rate=sample_rate,
                    metrics=metrics,
                    contamination={
                        "start_sample": start,
                        "end_sample": end,
                        "start_sec": trial["contamination_start_sec"],
                        "end_sec": trial["contamination_end_sec"],
                        "intruder_region_id": intruder_region.get("id"),
                        "intruder_excerpt_start_sample": excerpt_offset,
                    },
                )
            trials.append(trial)

    if include_natural_worst:
        for clip_row in select_natural_worst_clips(purity_qc_path, limit=10):
            clip_id = str(clip_row["clip_id"])
            target_samples, actual_rate = read_analysis_audio(resolve_under_root(run_root, str(clip_row["audio_path"])))
            metrics = score_candidate_clip(model, target_samples, sample_rate, centroid, purity_config)
            score = metrics.get("purity_score")
            trial = {
                "dataset": dataset_name,
                "clip_id": clip_id,
                "variant": "natural_worst_rescore",
                "category": "natural",
                "description": "Re-score real suspicious/contaminated clip from purity QC",
                "baseline_score": float(clip_row["purity_score"]),
                "purity_score": score,
                "mean_window_similarity": metrics.get("mean_window_similarity"),
                "score_delta": round(float(clip_row["purity_score"]) - float(score), 6) if score is not None else None,
                "bucket": metrics.get("bucket"),
                "worst_window_start_sec": metrics.get("worst_window_start_sec"),
                "intruder_window_count": metrics.get("intruder_window_count"),
                "mix_ratio": None,
                "contamination_start_sec": None,
                "contamination_end_sec": None,
                "overlap_blind": False,
                "exported_wav": None,
            }
            if should_export_variant({**trial, "bucket": metrics.get("bucket")}, overlap_blind_threshold=cocktail_config.overlap_blind_threshold):
                trial["exported_wav"] = export_variant_bundle(
                    clips_dir,
                    rank=999,
                    clip_id=clip_id,
                    variant="natural_worst",
                    samples=target_samples,
                    sample_rate=actual_rate,
                    metrics=metrics,
                    contamination={},
                )
            trials.append(trial)

    summary = summarize_battery(
        trials,
        purity_threshold=cocktail_config.purity_threshold,
        overlap_blind_threshold=cocktail_config.overlap_blind_threshold,
    )
    payload = {
        "dataset": dataset_name,
        "run_root": str(run_root),
        "summary": summary,
        "trials": trials,
        "variant_catalog": [variant.__dict__ for variant in variants],
    }
    write_json(out_dir / "speaker_purity_battery.json", payload)
    write_json(out_dir / "speaker_purity_battery_summary.json", summary)
    with (out_dir / "speaker_purity_battery_trials.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "dataset",
                "clip_id",
                "variant",
                "category",
                "baseline_score",
                "purity_score",
                "score_delta",
                "bucket",
                "mix_ratio",
                "worst_window_start_sec",
                "contamination_start_sec",
                "contamination_end_sec",
                "overlap_blind",
                "exported_wav",
            ],
        )
        writer.writeheader()
        for trial in trials:
            writer.writerow({key: trial.get(key) for key in writer.fieldnames})
    return payload


def run_battery_suite(
    datasets: list[str],
    out_root: Path,
    *,
    device: str = "auto",
    clips_limit: int = 15,
    mb_min_duration: float = 6.0,
    mc_min_duration: float = 4.5,
) -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[3]
    results: dict[str, Any] = {}
    for name in datasets:
        entry = DATASET_REGISTRY[name]
        min_duration = mb_min_duration if name == "mb" else mc_min_duration
        payload = run_battery(
            Path(repo_root / entry["run_root"]),
            out_root / name,
            dataset_name=name,
            voiceprint_path=Path(repo_root / entry["voiceprint"]),
            purity_qc_path=Path(repo_root / entry["purity_qc"]),
            cocktail_config=CocktailConfig(
                clips_limit=clips_limit,
                min_clip_duration_sec=min_duration,
            ),
            device=device,
        )
        results[name] = payload["summary"]
    write_json(out_root / "speaker_purity_battery_suite_summary.json", results)
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Multi-variant TitaNet speaker purity torture battery.")
    parser.add_argument("--out", required=True)
    parser.add_argument("--datasets", nargs="*", default=["mb", "mc"])
    parser.add_argument("--device", choices=["auto", "cuda", "cpu"], default="auto")
    parser.add_argument("--clips-limit", type=int, default=15)
    parser.add_argument("--mb-min-duration-sec", type=float, default=6.0)
    parser.add_argument("--mc-min-duration-sec", type=float, default=4.5)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    for name in args.datasets:
        if name not in DATASET_REGISTRY:
            raise SystemExit(f"Unknown dataset {name!r}")
    out_root = Path(args.out).expanduser().resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    summary = run_battery_suite(
        args.datasets,
        out_root,
        device=args.device,
        clips_limit=args.clips_limit,
        mb_min_duration=args.mb_min_duration_sec,
        mc_min_duration=args.mc_min_duration_sec,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
