from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .buffers import read_analysis_audio, sec_to_sample, write_pcm16_mono
from .eval_speaker_purity import (
    DEFAULT_MODEL,
    SpeakerPurityConfig,
    load_titanet_model,
    score_candidate_clip,
)
from .io import read_json_value, read_jsonl, resolve_under_root, write_json

TARGET_SAMPLE_RATE = 16000


@dataclass(frozen=True)
class CocktailConfig:
    min_baseline_purity: float = 0.80
    min_clip_duration_sec: float = 6.0
    clips_limit: int = 15
    intruder_speaker_id: str = "speaker_1"
    splice_ms: float = 400.0
    blend_sec: float = 2.0
    blend_mix_ratio: float = 0.5
    overlap_blind_threshold: float = 0.85
    purity_threshold: float = 0.70


def log(message: str) -> None:
    print(f"[speaker_purity_cocktail] {message}", flush=True)


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def peak_normalize(samples: np.ndarray, *, peak: float = 0.95) -> np.ndarray:
    if samples.size == 0:
        return samples.astype(np.float32, copy=False)
    max_abs = float(np.max(np.abs(samples)))
    if max_abs <= 1e-12:
        return samples.astype(np.float32, copy=False)
    return (samples * (peak / max_abs)).astype(np.float32, copy=False)


def window_rms(samples: np.ndarray) -> float:
    if samples.size == 0:
        return 0.0
    return float(np.sqrt(np.mean(samples * samples)))


def select_clean_clips(
    purity_qc_path: Path,
    *,
    min_purity: float,
    min_duration_sec: float,
    limit: int,
) -> list[dict[str, Any]]:
    rows = read_json_value(purity_qc_path)
    if not isinstance(rows, list):
        raise ValueError(f"{purity_qc_path} must contain a list")
    eligible = [
        row
        for row in rows
        if isinstance(row, dict)
        and row.get("bucket") == "clean"
        and row.get("purity_score") is not None
        and float(row["purity_score"]) >= min_purity
        and float(row.get("duration_sec") or 0.0) >= min_duration_sec
        and row.get("audio_path")
    ]
    eligible.sort(
        key=lambda row: (
            -float(row["purity_score"]),
            str(row.get("clip_id") or ""),
        )
    )
    return eligible[: max(0, limit)]


def load_voiceprint(voiceprint_path: Path) -> tuple[np.ndarray, dict[str, Any]]:
    payload = read_json_value(voiceprint_path)
    if not isinstance(payload, dict) or not payload.get("centroid"):
        raise ValueError(f"Invalid voiceprint: {voiceprint_path}")
    centroid = np.asarray(payload["centroid"], dtype=np.float64)
    return centroid, payload


def load_intruder_source_cache(
    run_root: Path,
) -> tuple[dict[str, Path], dict[str, int], Any]:
    from .io import read_json

    variants = read_json(resolve_under_root(run_root, "artifacts/audio_variants_manifest.json")).get("variants") or []
    analysis_path_by_source = {
        str(variant["source_audio_id"]): resolve_under_root(run_root, str(variant["path"]))
        for variant in variants
    }
    sample_rate_by_source = {
        str(variant["source_audio_id"]): int(variant.get("analysis_sample_rate") or TARGET_SAMPLE_RATE)
        for variant in variants
    }
    cache: dict[str, tuple[np.ndarray, int]] = {}

    def load_source(source_audio_id: str) -> tuple[np.ndarray, int]:
        if source_audio_id not in cache:
            samples, rate = read_analysis_audio(analysis_path_by_source[source_audio_id])
            expected = sample_rate_by_source[source_audio_id]
            if rate != expected:
                raise ValueError(f"Expected sample rate {expected} for {source_audio_id}, got {rate}")
            cache[source_audio_id] = (samples, rate)
        return cache[source_audio_id]

    return analysis_path_by_source, sample_rate_by_source, load_source


def intruder_regions(
    speaker_regions: list[dict[str, Any]],
    *,
    intruder_speaker_id: str,
    min_region_sec: float = 2.5,
) -> list[dict[str, Any]]:
    regions = [
        row
        for row in speaker_regions
        if str(row.get("speaker_id")) == intruder_speaker_id
        and float(row.get("end_sec", 0.0)) - float(row.get("start_sec", 0.0)) >= min_region_sec
    ]
    regions.sort(
        key=lambda row: int(row.get("end_sample", 0)) - int(row.get("start_sample", 0)),
        reverse=True,
    )
    return regions


def pick_intruder_excerpt(
    region_samples: np.ndarray,
    sample_rate: int,
    *,
    duration_sec: float,
) -> tuple[np.ndarray, int]:
    excerpt_samples = sec_to_sample(duration_sec, sample_rate)
    if region_samples.size <= excerpt_samples:
        return region_samples.astype(np.float32, copy=False), 0
    best_start = 0
    best_rms = -1.0
    hop = max(excerpt_samples // 4, sec_to_sample(0.1, sample_rate))
    for start in range(0, region_samples.size - excerpt_samples + 1, hop):
        rms = window_rms(region_samples[start : start + excerpt_samples])
        if rms > best_rms:
            best_rms = rms
            best_start = start
    return region_samples[best_start : best_start + excerpt_samples].astype(np.float32, copy=False), best_start


def hard_splice(
    target: np.ndarray,
    intruder: np.ndarray,
    sample_rate: int,
    *,
    splice_ms: float,
) -> tuple[np.ndarray, int, int]:
    splice_samples = sec_to_sample(splice_ms / 1000.0, sample_rate)
    splice_samples = min(splice_samples, target.size, intruder.size)
    if splice_samples <= 0 or target.size == 0:
        raise ValueError("cannot hard splice empty audio")
    start = max(0, (target.size - splice_samples) // 2)
    end = start + splice_samples
    out = target.astype(np.float32, copy=True)
    out[start:end] = intruder[:splice_samples]
    return peak_normalize(out), start, end


def blend_underneath(
    target: np.ndarray,
    intruder: np.ndarray,
    sample_rate: int,
    *,
    blend_sec: float,
    mix_ratio: float,
    placement: str = "mid",
) -> tuple[np.ndarray, int, int]:
    blend_samples = sec_to_sample(blend_sec, sample_rate)
    blend_samples = min(blend_samples, target.size, intruder.size)
    if blend_samples <= 0 or target.size == 0:
        raise ValueError("cannot blend empty audio")
    if placement == "start":
        start = 0
    elif placement == "end":
        start = max(0, target.size - blend_samples)
    else:
        start = max(0, (target.size - blend_samples) // 2)
    end = start + blend_samples
    out = target.astype(np.float32, copy=True)
    intruder_slice = intruder[:blend_samples]
    out[start:end] = out[start:end] + (mix_ratio * intruder_slice)
    return peak_normalize(out), start, end


def hard_splice_duration(
    target: np.ndarray,
    intruder: np.ndarray,
    sample_rate: int,
    *,
    duration_sec: float,
    placement: str = "mid",
) -> tuple[np.ndarray, int, int]:
    return hard_splice(
        target,
        intruder,
        sample_rate,
        splice_ms=duration_sec * 1000.0,
    ) if placement == "mid" else _splice_at(
        target,
        intruder,
        sample_rate,
        duration_sec=duration_sec,
        placement=placement,
    )


def _splice_at(
    target: np.ndarray,
    intruder: np.ndarray,
    sample_rate: int,
    *,
    duration_sec: float,
    placement: str,
) -> tuple[np.ndarray, int, int]:
    splice_samples = min(sec_to_sample(duration_sec, sample_rate), target.size, intruder.size)
    if splice_samples <= 0 or target.size == 0:
        raise ValueError("cannot hard splice empty audio")
    if placement == "start":
        start = 0
    elif placement == "end":
        start = max(0, target.size - splice_samples)
    else:
        start = max(0, (target.size - splice_samples) // 2)
    end = start + splice_samples
    out = target.astype(np.float32, copy=True)
    out[start:end] = intruder[:splice_samples]
    return peak_normalize(out), start, end


def replace_segment(
    target: np.ndarray,
    intruder: np.ndarray,
    sample_rate: int,
    *,
    duration_sec: float,
    placement: str = "mid",
) -> tuple[np.ndarray, int, int]:
    return _splice_at(target, intruder, sample_rate, duration_sec=duration_sec, placement=placement)


def append_intruder(
    target: np.ndarray,
    intruder: np.ndarray,
    sample_rate: int,
    *,
    duration_sec: float,
) -> tuple[np.ndarray, int, int]:
    append_samples = min(sec_to_sample(duration_sec, sample_rate), intruder.size)
    if append_samples <= 0:
        raise ValueError("cannot append empty intruder audio")
    out = np.concatenate([target.astype(np.float32, copy=False), intruder[:append_samples]])
    start = target.size
    end = out.size
    return peak_normalize(out), start, end


def prepend_intruder(
    target: np.ndarray,
    intruder: np.ndarray,
    sample_rate: int,
    *,
    duration_sec: float,
) -> tuple[np.ndarray, int, int]:
    prepend_samples = min(sec_to_sample(duration_sec, sample_rate), intruder.size)
    if prepend_samples <= 0:
        raise ValueError("cannot prepend empty intruder audio")
    out = np.concatenate([intruder[:prepend_samples], target.astype(np.float32, copy=False)])
    return peak_normalize(out), 0, prepend_samples


def export_variant_bundle(
    out_dir: Path,
    *,
    rank: int,
    clip_id: str,
    variant: str,
    samples: np.ndarray,
    sample_rate: int,
    metrics: dict[str, Any],
    contamination: dict[str, Any],
) -> str:
    score = metrics.get("purity_score")
    score_label = "na" if score is None else f"{int(round(float(score) * 100)):03d}"
    stem = f"{rank:03d}_{variant}_score_{score_label}_{clip_id}"
    wav_dest = out_dir / f"{stem}.wav"
    txt_dest = out_dir / f"{stem}.txt"
    write_pcm16_mono(wav_dest, samples, sample_rate)
    txt_dest.write_text(
        "\n".join(
            [
                f"clip_id: {clip_id}",
                f"variant: {variant}",
                f"purity_score: {score}",
                f"mean_window_similarity: {metrics.get('mean_window_similarity')}",
                f"bucket: {metrics.get('bucket')}",
                f"worst_window_start_sec: {metrics.get('worst_window_start_sec')}",
                f"intruder_window_count: {metrics.get('intruder_window_count')}",
                f"contamination_start_sample: {contamination.get('start_sample')}",
                f"contamination_end_sample: {contamination.get('end_sample')}",
                f"contamination_start_sec: {contamination.get('start_sec')}",
                f"contamination_end_sec: {contamination.get('end_sec')}",
                f"intruder_region_id: {contamination.get('intruder_region_id')}",
                f"intruder_excerpt_start_sample: {contamination.get('intruder_excerpt_start_sample')}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return str(wav_dest)


def summarize_cocktail_trials(trials: list[dict[str, Any]], config: CocktailConfig) -> dict[str, Any]:
    by_variant: dict[str, list[dict[str, Any]]] = {}
    for trial in trials:
        by_variant.setdefault(str(trial["variant"]), []).append(trial)

    def variant_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
        scores = [float(row["purity_score"]) for row in rows if row.get("purity_score") is not None]
        deltas = [float(row["score_delta"]) for row in rows if row.get("score_delta") is not None]
        return {
            "count": len(rows),
            "score_p50": _round(sorted(scores)[len(scores) // 2]) if scores else None,
            "score_min": _round(min(scores)) if scores else None,
            "mean_score_delta": _round(sum(deltas) / len(deltas)) if deltas else None,
            "pct_below_purity_threshold": round(
                100.0 * sum(score < config.purity_threshold for score in scores) / len(scores),
                1,
            )
            if scores
            else None,
            "pct_overlap_blind": round(
                100.0 * sum(score >= config.overlap_blind_threshold for score in scores) / len(scores),
                1,
            )
            if scores
            else None,
        }

    blend_rows = by_variant.get("blend", [])
    splice_rows = by_variant.get("hard_splice", [])
    return {
        "trial_count": len(trials),
        "by_variant": {name: variant_stats(rows) for name, rows in sorted(by_variant.items())},
        "splice_control_pass": all(
            row.get("purity_score") is not None and float(row["purity_score"]) < config.purity_threshold
            for row in splice_rows
        )
        if splice_rows
        else None,
        "blend_overlap_blind_count": sum(
            1
            for row in blend_rows
            if row.get("purity_score") is not None and float(row["purity_score"]) >= config.overlap_blind_threshold
        ),
        "worst_blend_cases": sorted(
            blend_rows,
            key=lambda row: float(row.get("purity_score") or 1.0),
        )[:10],
        "best_blend_cases": sorted(
            blend_rows,
            key=lambda row: -float(row.get("purity_score") or 0.0),
        )[:10],
    }


def run_cocktail_test(
    run_root: Path,
    out_dir: Path,
    *,
    voiceprint_path: Path,
    purity_qc_path: Path,
    model_name: str = DEFAULT_MODEL,
    purity_config: SpeakerPurityConfig | None = None,
    cocktail_config: CocktailConfig | None = None,
    device: str = "auto",
) -> dict[str, Any]:
    cocktail_config = cocktail_config or CocktailConfig()
    purity_config = purity_config or SpeakerPurityConfig(
        purity_threshold=cocktail_config.purity_threshold,
    )
    run_root = run_root.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    clips_dir = out_dir / "cocktail_clips"
    clips_dir.mkdir(parents=True, exist_ok=True)

    centroid, voiceprint = load_voiceprint(voiceprint_path.expanduser().resolve())
    clean_clips = select_clean_clips(
        purity_qc_path.expanduser().resolve(),
        min_purity=cocktail_config.min_baseline_purity,
        min_duration_sec=cocktail_config.min_clip_duration_sec,
        limit=cocktail_config.clips_limit,
    )
    if not clean_clips:
        raise ValueError("No eligible clean clips found for cocktail test")

    speaker_regions = read_jsonl(resolve_under_root(run_root, "artifacts/speaker_regions.jsonl"))
    _paths, _rates, load_source = load_intruder_source_cache(run_root)
    regions = intruder_regions(
        speaker_regions,
        intruder_speaker_id=cocktail_config.intruder_speaker_id,
        min_region_sec=cocktail_config.blend_sec,
    )
    if not regions:
        raise ValueError(f"No intruder regions found for {cocktail_config.intruder_speaker_id}")

    intruder_region = regions[0]
    source_audio_id = str(intruder_region["source_audio_id"])
    source_samples, sample_rate = load_source(source_audio_id)
    region_start = int(intruder_region["start_sample"])
    region_end = int(intruder_region["end_sample"])
    region_samples = source_samples[region_start:region_end]
    intruder_splice, splice_offset = pick_intruder_excerpt(
        region_samples,
        sample_rate,
        duration_sec=cocktail_config.splice_ms / 1000.0,
    )
    intruder_blend, blend_offset = pick_intruder_excerpt(
        region_samples,
        sample_rate,
        duration_sec=cocktail_config.blend_sec,
    )

    log(
        f"using intruder {cocktail_config.intruder_speaker_id} region {intruder_region.get('id')} "
        f"({len(region_samples)/sample_rate:.1f}s)"
    )
    log(f"testing {len(clean_clips)} clean clips")

    model = load_titanet_model(model_name, device)
    trials: list[dict[str, Any]] = []
    clip_results: list[dict[str, Any]] = []

    for rank, clip_row in enumerate(clean_clips, start=1):
        clip_id = str(clip_row["clip_id"])
        audio_path = resolve_under_root(run_root, str(clip_row["audio_path"]))
        target_samples, actual_rate = read_analysis_audio(audio_path)
        if actual_rate != sample_rate:
            raise ValueError(f"Expected sample rate {sample_rate} for {clip_id}, got {actual_rate}")

        baseline_metrics = score_candidate_clip(model, target_samples, sample_rate, centroid, purity_config)
        baseline_score = float(baseline_metrics["purity_score"])
        clip_variants: list[dict[str, Any]] = []

        for variant_name, builder, intruder_audio, contam_sec in [
            (
                "hard_splice",
                lambda t, i: hard_splice(
                    t,
                    i,
                    sample_rate,
                    splice_ms=cocktail_config.splice_ms,
                ),
                intruder_splice,
                cocktail_config.splice_ms / 1000.0,
            ),
            (
                "blend",
                lambda t, i: blend_underneath(
                    t,
                    i,
                    sample_rate,
                    blend_sec=cocktail_config.blend_sec,
                    mix_ratio=cocktail_config.blend_mix_ratio,
                ),
                intruder_blend,
                cocktail_config.blend_sec,
            ),
        ]:
            perturbed_samples, start, end = builder(target_samples, intruder_audio)
            metrics = score_candidate_clip(model, perturbed_samples, sample_rate, centroid, purity_config)
            contamination = {
                "start_sample": start,
                "end_sample": end,
                "start_sec": round(start / sample_rate, 6),
                "end_sec": round(end / sample_rate, 6),
                "duration_sec": contam_sec,
                "intruder_region_id": intruder_region.get("id"),
                "intruder_excerpt_start_sample": splice_offset if variant_name == "hard_splice" else blend_offset,
            }
            exported = export_variant_bundle(
                clips_dir,
                rank=rank,
                clip_id=clip_id,
                variant=variant_name,
                samples=perturbed_samples,
                sample_rate=sample_rate,
                metrics=metrics,
                contamination=contamination,
            )
            trial = {
                "clip_id": clip_id,
                "variant": variant_name,
                "baseline_score": baseline_score,
                "purity_score": metrics.get("purity_score"),
                "mean_window_similarity": metrics.get("mean_window_similarity"),
                "score_delta": (
                    round(baseline_score - float(metrics["purity_score"]), 6)
                    if metrics.get("purity_score") is not None
                    else None
                ),
                "bucket": metrics.get("bucket"),
                "worst_window_start_sec": metrics.get("worst_window_start_sec"),
                "intruder_window_count": metrics.get("intruder_window_count"),
                "overlap_blind": (
                    metrics.get("purity_score") is not None
                    and float(metrics["purity_score"]) >= cocktail_config.overlap_blind_threshold
                ),
                "exported_wav": exported,
                **contamination,
            }
            trials.append(trial)
            clip_variants.append(trial)
            log(
                f"{clip_id} {variant_name}: {baseline_score:.3f} -> {metrics.get('purity_score')} "
                f"({metrics.get('bucket')})"
            )

        baseline_export = export_variant_bundle(
            clips_dir,
            rank=rank,
            clip_id=clip_id,
            variant="baseline",
            samples=target_samples,
            sample_rate=sample_rate,
            metrics=baseline_metrics,
            contamination={},
        )
        clip_results.append(
            {
                "clip_id": clip_id,
                "baseline_score": baseline_score,
                "baseline_export": baseline_export,
                "variants": clip_variants,
            }
        )

    summary = summarize_cocktail_trials(trials, cocktail_config)
    payload = {
        "run_root": str(run_root),
        "voiceprint_path": str(voiceprint_path),
        "purity_qc_path": str(purity_qc_path),
        "intruder_speaker_id": cocktail_config.intruder_speaker_id,
        "intruder_region_id": intruder_region.get("id"),
        "config": {
            "min_baseline_purity": cocktail_config.min_baseline_purity,
            "min_clip_duration_sec": cocktail_config.min_clip_duration_sec,
            "clips_limit": cocktail_config.clips_limit,
            "splice_ms": cocktail_config.splice_ms,
            "blend_sec": cocktail_config.blend_sec,
            "blend_mix_ratio": cocktail_config.blend_mix_ratio,
            "overlap_blind_threshold": cocktail_config.overlap_blind_threshold,
            "purity_threshold": cocktail_config.purity_threshold,
            "window_sec": purity_config.window_sec,
            "window_hop_sec": purity_config.window_hop_sec,
        },
        "voiceprint": {
            "speaker_id": voiceprint.get("speaker_id"),
            "chunk_count_kept": voiceprint.get("chunk_count_kept"),
        },
        "summary": summary,
        "clips": clip_results,
        "trials": trials,
    }
    write_json(out_dir / "speaker_purity_cocktail_test.json", payload)
    write_json(out_dir / "speaker_purity_cocktail_test_summary.json", payload["summary"])

    with (out_dir / "speaker_purity_cocktail_test_trials.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "clip_id",
                "variant",
                "baseline_score",
                "purity_score",
                "score_delta",
                "mean_window_similarity",
                "bucket",
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

    log(
        f"blend overlap-blind (>={cocktail_config.overlap_blind_threshold}): "
        f"{summary['blend_overlap_blind_count']}/{summary['by_variant'].get('blend', {}).get('count', 0)}"
    )
    return payload


DATASET_REGISTRY: dict[str, dict[str, str]] = {
    "mb": {
        "run_root": "backend/data/media/dataset-runs/mb-mq3wkz25/dataset-17d22e1684db",
        "voiceprint": "backend/data/eval_speaker_purity/mb/enrollment/target_voiceprint.json",
        "purity_qc": "backend/data/eval_speaker_purity/mb/speaker_purity_qc.json",
    },
    "mc": {
        "run_root": "backend/data/media/dataset-runs/mc-mq3xswcx/dataset-4a3f897e9802",
        "voiceprint": "backend/data/eval_speaker_purity/mc/enrollment/target_voiceprint.json",
        "purity_qc": "backend/data/eval_speaker_purity/mc/speaker_purity_qc.json",
    },
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TitaNet cocktail-party torture test (hard splice + overlap blend).")
    parser.add_argument("--out", required=True)
    parser.add_argument("--dataset", default="mb", choices=sorted(DATASET_REGISTRY))
    parser.add_argument("--run-root", default=None)
    parser.add_argument("--voiceprint", default=None)
    parser.add_argument("--purity-qc", default=None)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--device", choices=["auto", "cuda", "cpu"], default="auto")
    parser.add_argument("--clips-limit", type=int, default=CocktailConfig.clips_limit)
    parser.add_argument("--min-baseline-purity", type=float, default=CocktailConfig.min_baseline_purity)
    parser.add_argument("--min-clip-duration-sec", type=float, default=CocktailConfig.min_clip_duration_sec)
    parser.add_argument("--intruder-speaker-id", default=CocktailConfig.intruder_speaker_id)
    parser.add_argument("--splice-ms", type=float, default=CocktailConfig.splice_ms)
    parser.add_argument("--blend-sec", type=float, default=CocktailConfig.blend_sec)
    parser.add_argument("--blend-mix-ratio", type=float, default=CocktailConfig.blend_mix_ratio)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    entry = DATASET_REGISTRY[args.dataset]
    run_root = Path(args.run_root or repo_root / entry["run_root"]).expanduser().resolve()
    voiceprint_path = Path(args.voiceprint or repo_root / entry["voiceprint"]).expanduser().resolve()
    purity_qc_path = Path(args.purity_qc or repo_root / entry["purity_qc"]).expanduser().resolve()
    payload = run_cocktail_test(
        run_root,
        Path(args.out).expanduser().resolve(),
        voiceprint_path=voiceprint_path,
        purity_qc_path=purity_qc_path,
        model_name=args.model,
        cocktail_config=CocktailConfig(
            clips_limit=args.clips_limit,
            min_baseline_purity=args.min_baseline_purity,
            min_clip_duration_sec=args.min_clip_duration_sec,
            intruder_speaker_id=args.intruder_speaker_id,
            splice_ms=args.splice_ms,
            blend_sec=args.blend_sec,
            blend_mix_ratio=args.blend_mix_ratio,
        ),
        device=args.device,
    )
    print(json.dumps(payload["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
