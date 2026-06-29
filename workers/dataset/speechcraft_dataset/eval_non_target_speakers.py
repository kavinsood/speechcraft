from __future__ import annotations

import argparse
import json
import random
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .buffers import read_analysis_audio, sec_to_sample
from .eval_speaker_purity import (
    DEFAULT_MODEL,
    SpeakerPurityConfig,
    build_sanitized_target_voiceprint,
    cosine_similarity,
    embed_segment,
    embedding_vector,
    iter_fixed_chunks,
    iter_sliding_windows,
    load_titanet_model,
    prepare_window_samples,
    window_is_scorable,
)
from .io import read_json, read_json_value, read_jsonl, resolve_under_root, write_json

TARGET_SAMPLE_RATE = 16000


@dataclass(frozen=True)
class NonTargetSpeakerConfig:
    enroll: SpeakerPurityConfig = SpeakerPurityConfig()
    control_chunk_sec: float = 3.0
    control_sample_count: int = 10
    control_min_similarity: float = 0.80
    leak_window_sec: float = 1.5
    leak_hop_sec: float = 0.5
    intruder_similarity_threshold: float = 0.70
    seed: int = 42


def log(message: str) -> None:
    print(f"[eval_non_target_speakers] {message}", flush=True)


def _round(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def unique_speaker_ids(speaker_regions: list[dict[str, Any]]) -> list[str]:
    return sorted({str(row.get("speaker_id")) for row in speaker_regions if row.get("speaker_id")})


def load_run_context(run_root: Path) -> dict[str, Any]:
    run_root = run_root.expanduser().resolve()
    selection = read_json_value(resolve_under_root(run_root, "artifacts/speaker_selection.json"))
    speaker_regions = read_jsonl(resolve_under_root(run_root, "artifacts/speaker_regions.jsonl"))
    variants = read_json(resolve_under_root(run_root, "artifacts/audio_variants_manifest.json")).get("variants") or []
    if not variants:
        raise ValueError("audio_variants_manifest.json has no variants")
    analysis_path_by_source = {
        str(variant["source_audio_id"]): resolve_under_root(run_root, str(variant["path"]))
        for variant in variants
    }
    sample_rate_by_source = {
        str(variant["source_audio_id"]): int(variant.get("analysis_sample_rate") or TARGET_SAMPLE_RATE)
        for variant in variants
    }
    target_speaker_id = str(selection.get("target_speaker_id") or "speaker_0")
    return {
        "run_root": run_root,
        "target_speaker_id": target_speaker_id,
        "speaker_regions": speaker_regions,
        "analysis_path_by_source": analysis_path_by_source,
        "sample_rate_by_source": sample_rate_by_source,
    }


def enroll_speaker_centroids(
    model: Any,
    *,
    speaker_regions: list[dict[str, Any]],
    speaker_ids: list[str],
    analysis_path_by_source: dict[str, Path],
    sample_rate_by_source: dict[str, int],
    config: NonTargetSpeakerConfig,
    enrollment_root: Path,
) -> tuple[dict[str, np.ndarray], dict[str, dict[str, Any]], list[dict[str, Any]]]:
    centroids: dict[str, np.ndarray] = {}
    voiceprints: dict[str, dict[str, Any]] = {}
    enrollment_rows: list[dict[str, Any]] = []

    for speaker_id in speaker_ids:
        speaker_dir = enrollment_root / speaker_id
        speaker_dir.mkdir(parents=True, exist_ok=True)
        try:
            voiceprint, chunk_rows = build_sanitized_target_voiceprint(
                model,
                speaker_regions,
                target_speaker_id=speaker_id,
                analysis_path_by_source=analysis_path_by_source,
                sample_rate_by_source=sample_rate_by_source,
                config=config.enroll,
                enrollment_dir=speaker_dir,
            )
        except ValueError as exc:
            log(f"skipping {speaker_id}: {exc}")
            enrollment_rows.append(
                {
                    "speaker_id": speaker_id,
                    "status": "skipped",
                    "error": str(exc),
                }
            )
            continue
        centroid = embedding_vector(np.asarray(voiceprint["centroid"], dtype=np.float64))
        centroids[speaker_id] = centroid
        voiceprints[speaker_id] = voiceprint
        write_json(speaker_dir / "voiceprint.json", voiceprint)
        enrollment_rows.append(
            {
                "speaker_id": speaker_id,
                "status": "ok",
                "chunk_count_kept": voiceprint.get("chunk_count_kept"),
                "chunk_count_killed": voiceprint.get("chunk_count_killed"),
            }
        )
        log(
            f"enrolled {speaker_id}: kept={voiceprint.get('chunk_count_kept')} "
            f"killed={voiceprint.get('chunk_count_killed')}"
        )
    return centroids, voiceprints, enrollment_rows


def centroid_similarity_matrix(centroids: dict[str, np.ndarray]) -> dict[str, Any]:
    speaker_ids = sorted(centroids)
    matrix: dict[str, dict[str, float | None]] = {}
    for left_id in speaker_ids:
        matrix[left_id] = {}
        for right_id in speaker_ids:
            if left_id == right_id:
                matrix[left_id][right_id] = 1.0
            else:
                matrix[left_id][right_id] = _round(cosine_similarity(centroids[left_id], centroids[right_id]))
    return {"speaker_ids": speaker_ids, "cosine_similarity": matrix}


def print_centroid_matrix(matrix_payload: dict[str, Any]) -> None:
    speaker_ids: list[str] = matrix_payload["speaker_ids"]
    matrix: dict[str, dict[str, float | None]] = matrix_payload["cosine_similarity"]
    if not speaker_ids:
        log("centroid similarity matrix: (no centroids)")
        return
    width = max(len(speaker_id) for speaker_id in speaker_ids)
    header = " " * (width + 2) + " ".join(f"{speaker_id:>8}" for speaker_id in speaker_ids)
    print(header)
    for left_id in speaker_ids:
        values = " ".join(f"{matrix[left_id][right_id]!s:>8}" for right_id in speaker_ids)
        print(f"{left_id:<{width}}  {values}")


def regions_for_speaker(
    speaker_regions: list[dict[str, Any]],
    speaker_id: str,
    *,
    min_duration_sec: float,
) -> list[dict[str, Any]]:
    return [
        row
        for row in speaker_regions
        if str(row.get("speaker_id")) == speaker_id
        and float(row.get("end_sec", 0.0)) - float(row.get("start_sec", 0.0)) >= min_duration_sec
    ]


def sample_region_chunk(
    region: dict[str, Any],
    *,
    chunk_sec: float,
    analysis_path_by_source: dict[str, Path],
    sample_rate_by_source: dict[str, int],
    rng: random.Random,
) -> tuple[np.ndarray, int, dict[str, Any]] | None:
    source_audio_id = str(region.get("source_audio_id") or "")
    if source_audio_id not in analysis_path_by_source:
        return None
    samples, sample_rate = read_analysis_audio(analysis_path_by_source[source_audio_id])
    expected_rate = sample_rate_by_source[source_audio_id]
    if sample_rate != expected_rate:
        raise ValueError(f"Expected sample rate {expected_rate} for {source_audio_id}, got {sample_rate}")
    start = int(region["start_sample"])
    end = int(region["end_sample"])
    region_samples = samples[start:end]
    chunk_samples = sec_to_sample(chunk_sec, sample_rate)
    if region_samples.size < chunk_samples:
        return None
    max_start = region_samples.size - chunk_samples
    chunk_start = 0 if max_start <= 0 else int(rng.randint(0, max_start))
    chunk_end = chunk_start + chunk_samples
    return (
        region_samples[chunk_start:chunk_end],
        sample_rate,
        {
            "region_id": str(region.get("id")),
            "source_audio_id": source_audio_id,
            "region_start_sec": float(region.get("start_sec") or 0.0),
            "region_end_sec": float(region.get("end_sec") or 0.0),
            "chunk_start_sec": round(float(region.get("start_sec") or 0.0) + chunk_start / sample_rate, 6),
            "chunk_end_sec": round(float(region.get("start_sec") or 0.0) + chunk_end / sample_rate, 6),
        },
    )


def run_control_tests(
    model: Any,
    *,
    speaker_regions: list[dict[str, Any]],
    centroids: dict[str, np.ndarray],
    analysis_path_by_source: dict[str, Path],
    sample_rate_by_source: dict[str, int],
    config: NonTargetSpeakerConfig,
    rng: random.Random,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for speaker_id, centroid in sorted(centroids.items()):
        regions = regions_for_speaker(
            speaker_regions,
            speaker_id,
            min_duration_sec=config.control_chunk_sec,
        )
        if not regions:
            results.append(
                {
                    "speaker_id": speaker_id,
                    "status": "skipped",
                    "reason": "no_regions_long_enough",
                }
            )
            continue
        rng.shuffle(regions)
        similarities: list[float] = []
        samples_taken = 0
        detail_rows: list[dict[str, Any]] = []
        for region in regions:
            if samples_taken >= config.control_sample_count:
                break
            sampled = sample_region_chunk(
                region,
                chunk_sec=config.control_chunk_sec,
                analysis_path_by_source=analysis_path_by_source,
                sample_rate_by_source=sample_rate_by_source,
                rng=rng,
            )
            if sampled is None:
                continue
            chunk_samples, sample_rate, meta = sampled
            similarity = cosine_similarity(embed_segment(model, chunk_samples), centroid)
            similarities.append(similarity)
            detail_rows.append({**meta, "similarity": _round(similarity)})
            samples_taken += 1
        if not similarities:
            results.append(
                {
                    "speaker_id": speaker_id,
                    "status": "skipped",
                    "reason": "no_sampled_chunks",
                }
            )
            continue
        min_similarity = float(min(similarities))
        mean_similarity = float(np.mean(similarities))
        passed = min_similarity >= config.control_min_similarity
        results.append(
            {
                "speaker_id": speaker_id,
                "status": "ok",
                "sample_count": len(similarities),
                "min_similarity": _round(min_similarity),
                "mean_similarity": _round(mean_similarity),
                "passed": passed,
                "required_min_similarity": config.control_min_similarity,
                "samples": detail_rows,
            }
        )
        log(
            f"control {speaker_id}: n={len(similarities)} "
            f"min={_round(min_similarity)} mean={_round(mean_similarity)} "
            f"{'PASS' if passed else 'FAIL'}"
        )
    return results


def score_windows_against_centroid(
    model: Any,
    samples: np.ndarray,
    sample_rate: int,
    centroid: np.ndarray,
    *,
    window_sec: float,
    hop_sec: float,
    silence_config: SpeakerPurityConfig,
) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    for start, end, start_sec in iter_sliding_windows(
        samples,
        sample_rate,
        window_sec=window_sec,
        hop_sec=hop_sec,
    ):
        window_samples = prepare_window_samples(samples[start:end], sample_rate, window_sec)
        scorable, window_rms, silent_fraction = window_is_scorable(window_samples, sample_rate, silence_config)
        if not scorable:
            continue
        similarity = cosine_similarity(embed_segment(model, window_samples), centroid)
        hits.append(
            {
                "start_sec": round(start_sec, 6),
                "end_sec": round(start_sec + (end - start) / sample_rate, 6),
                "similarity": round(similarity, 6),
                "window_rms": round(window_rms, 6),
                "silent_frame_fraction": round(silent_fraction, 6),
            }
        )
    return hits


def scan_candidate_clips_for_intruders(
    model: Any,
    *,
    run_root: Path,
    centroids: dict[str, np.ndarray],
    target_speaker_id: str,
    config: NonTargetSpeakerConfig,
    intruders_dir: Path,
) -> list[dict[str, Any]]:
    non_target = {
        speaker_id: centroid
        for speaker_id, centroid in centroids.items()
        if speaker_id != target_speaker_id
    }
    candidates = read_json_value(resolve_under_root(run_root, "artifacts/candidate_review_manifest.json"))
    if not isinstance(candidates, list):
        raise ValueError("candidate_review_manifest.json must contain a list")

    intruders_dir.mkdir(parents=True, exist_ok=True)
    flagged: list[dict[str, Any]] = []
    silence_config = config.enroll

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        clip_id = str(candidate.get("id") or candidate.get("clip_id") or "")
        audio_rel = candidate.get("audio_path")
        if not audio_rel:
            continue
        try:
            audio_path = resolve_under_root(run_root, str(audio_rel))
            samples, sample_rate = read_analysis_audio(audio_path)
        except (ValueError, OSError):
            continue

        clip_hits: list[dict[str, Any]] = []
        for speaker_id, centroid in sorted(non_target.items()):
            windows = score_windows_against_centroid(
                model,
                samples,
                sample_rate,
                centroid,
                window_sec=config.leak_window_sec,
                hop_sec=config.leak_hop_sec,
                silence_config=silence_config,
            )
            for window in windows:
                if float(window["similarity"]) < config.intruder_similarity_threshold:
                    continue
                clip_hits.append(
                    {
                        "speaker_id": speaker_id,
                        **window,
                    }
                )
        if not clip_hits:
            continue

        clip_hits.sort(key=lambda row: -float(row["similarity"]))
        best = clip_hits[0]
        stem = f"{clip_id}_{best['speaker_id']}_sim_{int(round(float(best['similarity']) * 100)):03d}"
        wav_dest = intruders_dir / f"{stem}.wav"
        txt_dest = intruders_dir / f"{stem}.txt"
        shutil.copy2(audio_path, wav_dest)
        txt_dest.write_text(
            "\n".join(
                [
                    f"clip_id: {clip_id}",
                    f"target_speaker_id: {target_speaker_id}",
                    f"audio_path: {audio_rel}",
                    f"intruder_threshold: {config.intruder_similarity_threshold}",
                    f"leak_window_sec: {config.leak_window_sec}",
                    f"leak_hop_sec: {config.leak_hop_sec}",
                    "",
                    "spikes:",
                    *[
                        (
                            f"- {hit['speaker_id']} @ {hit['start_sec']}s-{hit['end_sec']}s "
                            f"sim={hit['similarity']}"
                        )
                        for hit in clip_hits
                    ],
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        row = {
            "clip_id": clip_id,
            "audio_path": str(audio_rel),
            "exported_wav": str(wav_dest),
            "exported_txt": str(txt_dest),
            "hit_count": len(clip_hits),
            "best_hit": best,
            "hits": clip_hits,
        }
        flagged.append(row)
        log(
            f"intruder {clip_id}: {best['speaker_id']} "
            f"@ {best['start_sec']}s sim={best['similarity']}"
        )
    return flagged


def evaluate_non_target_speakers(
    run_root: Path,
    out_dir: Path,
    *,
    model_name: str = DEFAULT_MODEL,
    config: NonTargetSpeakerConfig | None = None,
    device: str = "auto",
) -> dict[str, Any]:
    config = config or NonTargetSpeakerConfig()
    out_dir.mkdir(parents=True, exist_ok=True)
    context = load_run_context(run_root)
    speaker_ids = unique_speaker_ids(context["speaker_regions"])
    rng = random.Random(config.seed)

    model = load_titanet_model(model_name, device)
    centroids, voiceprints, enrollment_rows = enroll_speaker_centroids(
        model,
        speaker_regions=context["speaker_regions"],
        speaker_ids=speaker_ids,
        analysis_path_by_source=context["analysis_path_by_source"],
        sample_rate_by_source=context["sample_rate_by_source"],
        config=config,
        enrollment_root=out_dir / "enrollment",
    )

    matrix_payload = centroid_similarity_matrix(centroids)
    print("\n=== Centroid cosine similarity matrix ===")
    print_centroid_matrix(matrix_payload)

    control_results = run_control_tests(
        model,
        speaker_regions=context["speaker_regions"],
        centroids=centroids,
        analysis_path_by_source=context["analysis_path_by_source"],
        sample_rate_by_source=context["sample_rate_by_source"],
        config=config,
        rng=rng,
    )

    intruders_dir = out_dir / "intruders"
    leak_results = scan_candidate_clips_for_intruders(
        model,
        run_root=context["run_root"],
        centroids=centroids,
        target_speaker_id=context["target_speaker_id"],
        config=config,
        intruders_dir=intruders_dir,
    )

    payload = {
        "run_root": str(context["run_root"]),
        "target_speaker_id": context["target_speaker_id"],
        "speaker_ids_seen": speaker_ids,
        "speaker_ids_enrolled": sorted(centroids),
        "enrollment": enrollment_rows,
        "centroid_similarity_matrix": matrix_payload,
        "control_tests": control_results,
        "intruder_scan": {
            "threshold": config.intruder_similarity_threshold,
            "window_sec": config.leak_window_sec,
            "hop_sec": config.leak_hop_sec,
            "flagged_clip_count": len(leak_results),
            "clips": leak_results,
        },
        "voiceprints": voiceprints,
    }
    write_json(out_dir / "eval_non_target_speakers.json", payload)
    write_json(
        out_dir / "eval_non_target_speakers_summary.json",
        {
            "run_root": payload["run_root"],
            "target_speaker_id": payload["target_speaker_id"],
            "speaker_ids_enrolled": payload["speaker_ids_enrolled"],
            "centroid_similarity_matrix": matrix_payload,
            "control_tests": control_results,
            "intruder_flagged_clip_count": len(leak_results),
            "intruders_dir": str(intruders_dir),
        },
    )
    log(f"done: enrolled={len(centroids)} intruders_flagged={len(leak_results)} out={out_dir}")
    return payload


DATASET_REGISTRY: dict[str, str] = {
    "mb": "backend/data/media/dataset-runs/mb-mq3wkz25/dataset-17d22e1684db",
    "mc": "backend/data/media/dataset-runs/mc-mq3xswcx/dataset-4a3f897e9802",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate non-target speaker centroids before contrastive margin work.",
    )
    parser.add_argument("--out", required=True)
    parser.add_argument("--datasets", nargs="*", default=["mb", "mc"])
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--device", choices=["auto", "cuda", "cpu"], default="auto")
    parser.add_argument("--seed", type=int, default=NonTargetSpeakerConfig.seed)
    parser.add_argument("--intruder-threshold", type=float, default=NonTargetSpeakerConfig.intruder_similarity_threshold)
    parser.add_argument("--control-min-similarity", type=float, default=NonTargetSpeakerConfig.control_min_similarity)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    out_root = Path(args.out).expanduser().resolve()
    config = NonTargetSpeakerConfig(
        seed=args.seed,
        intruder_similarity_threshold=args.intruder_threshold,
        control_min_similarity=args.control_min_similarity,
    )
    suite: dict[str, Any] = {}
    for dataset in args.datasets:
        if dataset not in DATASET_REGISTRY:
            raise SystemExit(f"Unknown dataset {dataset!r}; choose from {sorted(DATASET_REGISTRY)}")
        run_root = (repo_root / DATASET_REGISTRY[dataset]).resolve()
        log(f"=== dataset {dataset} ===")
        suite[dataset] = evaluate_non_target_speakers(
            run_root,
            out_root / dataset,
            model_name=args.model,
            config=config,
            device=args.device,
        )
    write_json(out_root / "eval_non_target_speakers_suite.json", suite)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
