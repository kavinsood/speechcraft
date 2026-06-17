from __future__ import annotations

import argparse
import csv
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .buffers import read_analysis_audio, sec_to_sample, write_pcm16_mono
from .io import read_json, read_json_value, read_jsonl, resolve_under_root, write_json

DEFAULT_MODEL = "nvidia/speakerverification_en_titanet_large"
TARGET_SAMPLE_RATE = 16000


@dataclass(frozen=True)
class SpeakerPurityConfig:
    min_region_sec: float = 4.0
    enroll_chunk_sec: float = 3.0
    max_enroll_chunks: int = 20
    outlier_percentile: float = 25.0
    window_sec: float = 3.0
    window_hop_sec: float = 0.5
    purity_threshold: float = 0.70
    suspicious_threshold: float = 0.55
    silence_frame_ms: float = 10.0
    silence_rms_threshold: float = 0.02
    max_silent_frame_fraction: float = 0.70


def log(message: str) -> None:
    print(f"[eval_speaker_purity] {message}", flush=True)


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((quantile / 100.0) * (len(ordered) - 1)))))
    return round(float(ordered[index]), 6)


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def resolve_device(device_arg: str) -> str:
    import torch

    if device_arg == "cpu":
        return "cpu"
    if device_arg == "cuda":
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA requested but not available")
        return "cuda"
    return "cuda" if torch.cuda.is_available() else "cpu"


def load_titanet_model(model_name: str, device_arg: str) -> Any:
    import torch
    from nemo.collections.asr.models import EncDecSpeakerLabelModel

    device = resolve_device(device_arg)
    log(f"loading TitaNet model {model_name!r} on {device}")
    model = EncDecSpeakerLabelModel.from_pretrained(model_name)
    model.eval()
    model.to(device)
    log("TitaNet model ready")
    return model


def embedding_vector(raw: Any) -> np.ndarray:
    if hasattr(raw, "detach"):
        raw = raw.detach().cpu().numpy()
    vector = np.asarray(raw, dtype=np.float64).reshape(-1)
    norm = np.linalg.norm(vector)
    if norm <= 1e-12:
        return vector
    return vector / norm


def cosine_similarity(left: np.ndarray, right: np.ndarray) -> float:
    return float(np.dot(left, right))


def frame_rms_values(samples: np.ndarray, sample_rate: int, *, frame_ms: float) -> np.ndarray:
    frame_len = max(int(round(sample_rate * frame_ms / 1000.0)), 64)
    hop = max(frame_len // 2, 32)
    if samples.size < frame_len:
        return np.asarray([float(np.sqrt(np.mean(samples * samples))) if samples.size else 0.0], dtype=np.float64)
    values: list[float] = []
    for start in range(0, samples.size - frame_len + 1, hop):
        frame = samples[start : start + frame_len]
        values.append(float(np.sqrt(np.mean(frame * frame))))
    return np.asarray(values, dtype=np.float64)


def window_is_scorable(
    samples: np.ndarray,
    sample_rate: int,
    config: SpeakerPurityConfig,
) -> tuple[bool, float, float]:
    """Skip windows that are mostly silence/breath so they do not fake intruder hits."""
    if samples.size == 0:
        return False, 0.0, 1.0
    frame_rms = frame_rms_values(samples, sample_rate, frame_ms=config.silence_frame_ms)
    if frame_rms.size == 0:
        return False, 0.0, 1.0
    silent_fraction = float(np.mean(frame_rms < config.silence_rms_threshold))
    window_rms = float(np.sqrt(np.mean(samples * samples)))
    scorable = silent_fraction < config.max_silent_frame_fraction and window_rms >= config.silence_rms_threshold
    return scorable, window_rms, silent_fraction


def embed_segment(model: Any, samples: np.ndarray) -> np.ndarray:
    emb, _logits = model.infer_segment(samples.astype(np.float32, copy=False))
    return embedding_vector(emb)


def iter_fixed_chunks(
    samples: np.ndarray,
    sample_rate: int,
    *,
    chunk_sec: float,
    max_chunks: int,
) -> list[tuple[int, int]]:
    chunk_samples = sec_to_sample(chunk_sec, sample_rate)
    if chunk_samples <= 0 or samples.size <= 0:
        return []
    spans: list[tuple[int, int]] = []
    cursor = 0
    while cursor + chunk_samples <= samples.size and len(spans) < max_chunks:
        spans.append((cursor, cursor + chunk_samples))
        cursor += chunk_samples
    return spans


def iter_sliding_windows(
    samples: np.ndarray,
    sample_rate: int,
    *,
    window_sec: float,
    hop_sec: float,
) -> list[tuple[int, int, float]]:
    window_samples = sec_to_sample(window_sec, sample_rate)
    hop_samples = sec_to_sample(hop_sec, sample_rate)
    if samples.size <= 0:
        return []
    if samples.size < window_samples:
        return [(0, samples.size, 0.0)]
    spans: list[tuple[int, int, float]] = []
    cursor = 0
    while cursor + window_samples <= samples.size:
        spans.append((cursor, cursor + window_samples, cursor / sample_rate))
        cursor += hop_samples
    if not spans:
        spans.append((0, min(samples.size, window_samples), 0.0))
    return spans


def prepare_window_samples(samples: np.ndarray, sample_rate: int, window_sec: float) -> np.ndarray:
    window_samples = sec_to_sample(window_sec, sample_rate)
    if samples.size >= window_samples:
        return samples
    if samples.size == 0:
        return np.zeros(window_samples, dtype=np.float32)
    padded = np.zeros(window_samples, dtype=np.float32)
    padded[: samples.size] = samples
    return padded


def outlier_guillotine(
    embeddings: list[np.ndarray],
    *,
    percentile: float,
) -> tuple[list[bool], list[float], float | None]:
    count = len(embeddings)
    if count == 0:
        return [], [], None
    if count == 1:
        return [True], [1.0], 1.0
    mean_sims: list[float] = []
    for index, left in enumerate(embeddings):
        others = [cosine_similarity(left, right) for offset, right in enumerate(embeddings) if offset != index]
        mean_sims.append(float(np.mean(others)) if others else 1.0)
    cutoff = float(np.percentile(mean_sims, percentile))
    keep = [score >= cutoff for score in mean_sims]
    return keep, mean_sims, cutoff


def build_sanitized_target_voiceprint(
    model: Any,
    speaker_regions: list[dict[str, Any]],
    *,
    target_speaker_id: str,
    analysis_path_by_source: dict[str, Path],
    sample_rate_by_source: dict[str, int],
    config: SpeakerPurityConfig,
    enrollment_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    regions = [
        row
        for row in speaker_regions
        if str(row.get("speaker_id")) == target_speaker_id
        and float(row.get("end_sec", 0.0)) - float(row.get("start_sec", 0.0)) >= config.min_region_sec
    ]
    regions.sort(
        key=lambda row: int(row.get("end_sample", 0)) - int(row.get("start_sample", 0)),
        reverse=True,
    )

    analysis_cache: dict[str, tuple[np.ndarray, int]] = {}

    def load_source(source_audio_id: str) -> tuple[np.ndarray, int]:
        if source_audio_id not in analysis_cache:
            path = analysis_path_by_source[source_audio_id]
            samples, rate = read_analysis_audio(path)
            expected = sample_rate_by_source[source_audio_id]
            if rate != expected:
                raise ValueError(f"Expected sample rate {expected} for {source_audio_id}, got {rate}")
            analysis_cache[source_audio_id] = (samples, rate)
        return analysis_cache[source_audio_id]

    chunk_rows: list[dict[str, Any]] = []
    chunk_embeddings: list[np.ndarray] = []
    kept_dir = enrollment_dir / "kept_chunks"
    killed_dir = enrollment_dir / "killed_chunks"
    kept_dir.mkdir(parents=True, exist_ok=True)
    killed_dir.mkdir(parents=True, exist_ok=True)

    for region in regions:
        if len(chunk_rows) >= config.max_enroll_chunks:
            break
        source_audio_id = str(region["source_audio_id"])
        if source_audio_id not in analysis_path_by_source:
            continue
        analysis_samples, sample_rate = load_source(source_audio_id)
        start = int(region["start_sample"])
        end = int(region["end_sample"])
        region_samples = analysis_samples[start:end]
        for chunk_start, chunk_end in iter_fixed_chunks(
            region_samples,
            sample_rate,
            chunk_sec=config.enroll_chunk_sec,
            max_chunks=config.max_enroll_chunks - len(chunk_rows),
        ):
            chunk_samples = region_samples[chunk_start:chunk_end]
            chunk_id = f"enroll_{len(chunk_rows):03d}"
            chunk_path = enrollment_dir / "all_chunks" / f"{chunk_id}.wav"
            chunk_path.parent.mkdir(parents=True, exist_ok=True)
            write_pcm16_mono(chunk_path, chunk_samples, sample_rate)
            embedding = embed_segment(model, chunk_samples)
            chunk_rows.append(
                {
                    "chunk_id": chunk_id,
                    "region_id": str(region.get("id")),
                    "source_audio_id": source_audio_id,
                    "source_start_sample": start + chunk_start,
                    "source_end_sample": start + chunk_end,
                    "duration_sec": round(len(chunk_samples) / sample_rate, 6),
                    "audio_path": str(chunk_path),
                    "embedding_dim": int(embedding.size),
                    "guillotine_status": "pending",
                }
            )
            chunk_embeddings.append(embedding)

    keep_flags, mean_sims, cutoff = outlier_guillotine(
        chunk_embeddings,
        percentile=config.outlier_percentile,
    )
    surviving_embeddings: list[np.ndarray] = []
    for row, embedding, keep, mean_sim in zip(chunk_rows, chunk_embeddings, keep_flags, mean_sims):
        row["mean_pairwise_similarity"] = _round(mean_sim)
        row["guillotine_cutoff"] = _round(cutoff)
        if keep:
            row["guillotine_status"] = "kept"
            dest = kept_dir / f"{row['chunk_id']}.wav"
            surviving_embeddings.append(embedding)
        else:
            row["guillotine_status"] = "killed"
            dest = killed_dir / f"{row['chunk_id']}.wav"
        shutil.copy2(row["audio_path"], dest)
        row["exported_wav"] = str(dest)

    if not surviving_embeddings:
        raise ValueError("Enrollment produced zero surviving chunks after outlier guillotine")

    centroid = embedding_vector(np.mean(np.stack(surviving_embeddings, axis=0), axis=0))
    voiceprint = {
        "model": DEFAULT_MODEL,
        "speaker_id": target_speaker_id,
        "embedding_dim": int(centroid.size),
        "centroid": [round(float(value), 8) for value in centroid.tolist()],
        "chunk_count_total": len(chunk_rows),
        "chunk_count_kept": len(surviving_embeddings),
        "chunk_count_killed": len(chunk_rows) - len(surviving_embeddings),
        "guillotine_cutoff": _round(cutoff),
        "guillotine_percentile": config.outlier_percentile,
        "chunk_ids_kept": [row["chunk_id"] for row in chunk_rows if row["guillotine_status"] == "kept"],
        "chunk_ids_killed": [row["chunk_id"] for row in chunk_rows if row["guillotine_status"] == "killed"],
    }
    return voiceprint, chunk_rows


def score_candidate_clip(
    model: Any,
    samples: np.ndarray,
    sample_rate: int,
    centroid: np.ndarray,
    config: SpeakerPurityConfig,
) -> dict[str, Any]:
    windows = iter_sliding_windows(
        samples,
        sample_rate,
        window_sec=config.window_sec,
        hop_sec=config.window_hop_sec,
    )
    scored: list[dict[str, Any]] = []
    skipped = 0
    for start, end, start_sec in windows:
        window_samples = prepare_window_samples(samples[start:end], sample_rate, config.window_sec)
        scorable, window_rms, silent_fraction = window_is_scorable(window_samples, sample_rate, config)
        if not scorable:
            skipped += 1
            continue
        embedding = embed_segment(model, window_samples)
        similarity = cosine_similarity(embedding, centroid)
        scored.append(
            {
                "start_sec": round(start_sec, 6),
                "end_sec": round(start_sec + (end - start) / sample_rate, 6),
                "similarity": round(similarity, 6),
                "window_rms": round(window_rms, 6),
                "silent_frame_fraction": round(silent_fraction, 6),
            }
        )

    if not scored:
        return {
            "purity_score": None,
            "min_window_similarity": None,
            "mean_window_similarity": None,
            "scored_window_count": 0,
            "skipped_window_count": skipped,
            "intruder_window_count": 0,
            "worst_window_start_sec": None,
            "bucket": "failed",
            "reason_codes": ["no_scorable_windows"],
            "windows": [],
        }

    similarities = [float(row["similarity"]) for row in scored]
    min_similarity = min(similarities)
    worst = min(scored, key=lambda row: float(row["similarity"]))
    intruder_count = sum(similarity < config.purity_threshold for similarity in similarities)
    if min_similarity >= config.purity_threshold:
        bucket = "clean"
    elif min_similarity >= config.suspicious_threshold:
        bucket = "suspicious"
    else:
        bucket = "contaminated"

    return {
        "purity_score": round(min_similarity, 6),
        "min_window_similarity": round(min_similarity, 6),
        "mean_window_similarity": round(float(np.mean(similarities)), 6),
        "scored_window_count": len(scored),
        "skipped_window_count": skipped,
        "intruder_window_count": intruder_count,
        "worst_window_start_sec": worst["start_sec"],
        "bucket": bucket,
        "reason_codes": [],
        "windows": scored,
    }


def export_clip_bundle(
    row: dict[str, Any],
    run_root: Path,
    out_dir: Path,
    *,
    rank: int,
) -> str | None:
    audio_rel = row.get("audio_path")
    if not audio_rel:
        return None
    try:
        source_path = resolve_under_root(run_root, str(audio_rel))
    except ValueError:
        return None
    if not source_path.exists():
        return None
    score = row.get("purity_score")
    score_label = "na" if score is None else f"{int(round(float(score) * 100)):03d}"
    clip_id = str(row.get("clip_id") or "clip")
    stem = f"{rank:03d}_score_{score_label}_{clip_id}"
    wav_dest = out_dir / f"{stem}.wav"
    txt_dest = out_dir / f"{stem}.txt"
    shutil.copy2(source_path, wav_dest)
    txt_dest.write_text(
        "\n".join(
            [
                f"clip_id: {clip_id}",
                f"purity_score: {score}",
                f"bucket: {row.get('bucket')}",
                f"duration_sec: {row.get('duration_sec')}",
                f"worst_window_start_sec: {row.get('worst_window_start_sec')}",
                f"intruder_window_count: {row.get('intruder_window_count')}",
                f"skipped_window_count: {row.get('skipped_window_count')}",
                f"reason_codes: {', '.join(row.get('reason_codes') or [])}",
                "audio_path:",
                str(audio_rel),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return str(wav_dest)


def evaluate_speaker_purity(
    run_root: Path,
    out_dir: Path,
    *,
    target_speaker_id: str | None = None,
    model_name: str = DEFAULT_MODEL,
    config: SpeakerPurityConfig | None = None,
    max_clips: int | None = None,
    export_worst: int = 50,
    export_best: int = 20,
    device: str = "auto",
) -> dict[str, Any]:
    config = config or SpeakerPurityConfig()
    run_root = run_root.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    enrollment_dir = out_dir / "enrollment"
    enrollment_dir.mkdir(parents=True, exist_ok=True)

    selection = read_json_value(resolve_under_root(run_root, "artifacts/speaker_selection.json"))
    speaker_id = target_speaker_id or str(selection.get("target_speaker_id") or "speaker_0")
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

    model = load_titanet_model(model_name, device)
    voiceprint, chunk_rows = build_sanitized_target_voiceprint(
        model,
        speaker_regions,
        target_speaker_id=speaker_id,
        analysis_path_by_source=analysis_path_by_source,
        sample_rate_by_source=sample_rate_by_source,
        config=config,
        enrollment_dir=enrollment_dir,
    )
    centroid = np.asarray(voiceprint["centroid"], dtype=np.float64)
    write_json(enrollment_dir / "target_voiceprint.json", voiceprint)
    write_json(enrollment_dir / "enrollment_summary.json", voiceprint)
    (enrollment_dir / "enrollment_chunks.jsonl").write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in chunk_rows) + ("\n" if chunk_rows else ""),
        encoding="utf-8",
    )

    candidates = read_json_value(resolve_under_root(run_root, "artifacts/candidate_review_manifest.json"))
    if not isinstance(candidates, list):
        raise ValueError("candidate_review_manifest.json must contain a list")
    if max_clips is not None:
        candidates = candidates[: max(0, max_clips)]

    clip_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        clip_id = str(candidate.get("id") or "")
        audio_rel = candidate.get("audio_path")
        base_row: dict[str, Any] = {
            "clip_id": clip_id,
            "audio_path": str(audio_rel or ""),
            "duration_sec": candidate.get("duration_sec"),
            "bucket": "failed",
            "reason_codes": [],
        }
        if not audio_rel:
            base_row["reason_codes"] = ["missing_audio"]
            clip_rows.append(base_row)
            continue
        try:
            audio_path = resolve_under_root(run_root, str(audio_rel))
            samples, actual_rate = read_analysis_audio(audio_path)
            metrics = score_candidate_clip(model, samples, actual_rate, centroid, config)
            base_row.update(metrics)
            clip_rows.append(base_row)
        except Exception as exc:  # noqa: BLE001
            base_row["reason_codes"] = ["scoring_failed"]
            base_row["error"] = str(exc)
            clip_rows.append(base_row)

    scored_rows = [row for row in clip_rows if row.get("purity_score") is not None]
    failed_rows = [row for row in clip_rows if row.get("purity_score") is None]
    scores = [float(row["purity_score"]) for row in scored_rows]
    bucket_counts = {
        "clean": sum(row.get("bucket") == "clean" for row in clip_rows),
        "suspicious": sum(row.get("bucket") == "suspicious" for row in clip_rows),
        "contaminated": sum(row.get("bucket") == "contaminated" for row in clip_rows),
        "failed": len(failed_rows),
    }

    ranked = sorted(
        scored_rows,
        key=lambda row: (float(row.get("purity_score") or 0.0), str(row.get("clip_id") or "")),
    )
    worst = ranked[: max(0, export_worst)]
    best = list(reversed(ranked[-max(0, export_best) :])) if ranked else []

    worst_dir = out_dir / "worst_clips"
    best_dir = out_dir / "best_clips"
    if worst_dir.exists():
        shutil.rmtree(worst_dir)
    if best_dir.exists():
        shutil.rmtree(best_dir)
    worst_dir.mkdir(parents=True, exist_ok=True)
    best_dir.mkdir(parents=True, exist_ok=True)

    worst_paths: list[str] = []
    for rank, row in enumerate(worst, start=1):
        exported = export_clip_bundle(row, run_root, worst_dir, rank=rank)
        if exported:
            worst_paths.append(exported)
    for rank, row in enumerate(best, start=1):
        export_clip_bundle(row, run_root, best_dir, rank=rank)

    csv_rows: list[dict[str, Any]] = []
    for rank, row in enumerate(ranked, start=1):
        exported = next((path for path in worst_paths if str(row.get("clip_id") or "") in path), "")
        csv_rows.append(
            {
                "rank": rank,
                "clip_id": row.get("clip_id"),
                "score": row.get("purity_score"),
                "bucket": row.get("bucket"),
                "duration_sec": row.get("duration_sec"),
                "worst_window_start_sec": row.get("worst_window_start_sec"),
                "intruder_window_count": row.get("intruder_window_count"),
                "skipped_window_count": row.get("skipped_window_count"),
                "audio_path": row.get("audio_path"),
                "exported_wav": exported,
                "reason_codes": "|".join(row.get("reason_codes") or []),
            }
        )

    summary = {
        "run_root": str(run_root),
        "model": model_name,
        "target_speaker_id": speaker_id,
        "clip_count": len(clip_rows),
        "scored_count": len(scored_rows),
        "failed_count": len(failed_rows),
        "score_p50": _percentile(scores, 50),
        "score_p10": _percentile(scores, 10),
        "score_p90": _percentile(scores, 90),
        "bucket_counts": bucket_counts,
        "enrollment": voiceprint,
        "window_sec": config.window_sec,
        "window_hop_sec": config.window_hop_sec,
        "silence_skip": {
            "silence_rms_threshold": config.silence_rms_threshold,
            "max_silent_frame_fraction": config.max_silent_frame_fraction,
        },
        "worst_clip_paths": worst_paths,
        "best_clip_export_count": len(best),
        "killed_chunk_paths": [row.get("exported_wav") for row in chunk_rows if row.get("guillotine_status") == "killed"],
    }

    write_json(out_dir / "speaker_purity_qc.json", clip_rows)
    write_json(out_dir / "speaker_purity_qc_summary.json", summary)
    with (out_dir / "speaker_purity_qc_by_score.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "rank",
                "clip_id",
                "score",
                "bucket",
                "duration_sec",
                "worst_window_start_sec",
                "intruder_window_count",
                "skipped_window_count",
                "audio_path",
                "exported_wav",
                "reason_codes",
            ],
        )
        writer.writeheader()
        writer.writerows(csv_rows)

    log(f"enrollment kept {voiceprint['chunk_count_kept']}/{voiceprint['chunk_count_total']} chunks")
    log(f"scored {len(scored_rows)}/{len(clip_rows)} clips -> {out_dir}")
    log(f"buckets: {bucket_counts}")
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline TitaNet speaker purity enrollment + candidate QC eval.")
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--target-speaker-id", default=None)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--max-clips", type=int, default=None)
    parser.add_argument("--export-worst", type=int, default=50)
    parser.add_argument("--export-best", type=int, default=20)
    parser.add_argument("--device", choices=["auto", "cuda", "cpu"], default="auto")
    parser.add_argument("--window-sec", type=float, default=SpeakerPurityConfig.window_sec)
    parser.add_argument("--window-hop-sec", type=float, default=SpeakerPurityConfig.window_hop_sec)
    parser.add_argument("--purity-threshold", type=float, default=SpeakerPurityConfig.purity_threshold)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = SpeakerPurityConfig(
        window_sec=args.window_sec,
        window_hop_sec=args.window_hop_sec,
        purity_threshold=args.purity_threshold,
    )
    summary = evaluate_speaker_purity(
        Path(args.run_root),
        Path(args.out).expanduser().resolve(),
        target_speaker_id=args.target_speaker_id,
        model_name=args.model,
        config=config,
        max_clips=args.max_clips,
        export_worst=args.export_worst,
        export_best=args.export_best,
        device=args.device,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
