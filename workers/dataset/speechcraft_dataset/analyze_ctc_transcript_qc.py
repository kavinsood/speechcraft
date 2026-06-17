from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .buffers import read_analysis_audio
from .io import read_json_value, resolve_under_root, write_json

DEFAULT_MODEL = "facebook/wav2vec2-base-960h"
TARGET_SAMPLE_RATE = 16000
WEAK_CHAR_THRESHOLD = 0.25
WEAK_WINDOW_THRESHOLD = 0.50
WEAK_WINDOW_FRAMES = 5


@dataclass(frozen=True)
class CtcModelBundle:
    model: Any
    processor: Any
    char_list: list[str]
    device: str


def log(message: str) -> None:
    print(f"[ctc_transcript_qc] {message}", flush=True)


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


def load_ctc_model(model_name: str, device_arg: str) -> CtcModelBundle:
    import torch
    from transformers import Wav2Vec2ForCTC, Wav2Vec2Processor

    device = resolve_device(device_arg)
    log(f"loading CTC model {model_name!r} on {device} (download/cache via Hugging Face if needed)")
    processor = Wav2Vec2Processor.from_pretrained(model_name)
    model = Wav2Vec2ForCTC.from_pretrained(model_name)
    model.to(device)
    model.eval()
    vocab = processor.tokenizer.get_vocab()
    inv_vocab = {index: token for token, index in vocab.items()}
    char_list = [inv_vocab[index] for index in range(len(inv_vocab))]
    log(f"model ready: vocab_size={len(char_list)}")
    return CtcModelBundle(model=model, processor=processor, char_list=char_list, device=device)


def select_verifier_source_text(row: dict[str, Any]) -> tuple[str, str]:
    for field in ("verifier_text", "normalized_text", "alignment_text", "training_text"):
        value = row.get(field)
        if isinstance(value, str) and value.strip():
            return field, value.strip()
    return "", ""


def normalize_verifier_text(raw_text: str, char_list: set[str]) -> tuple[str, list[str]]:
    text = raw_text.replace("\n", " ").strip()
    alpha_chars = {char for char in char_list if char.isalpha()}
    if alpha_chars and all(char.isupper() for char in alpha_chars):
        text = text.upper()
    else:
        text = text.lower()
    reason_codes: list[str] = []
    if re.search(r"\d", raw_text):
        reason_codes.append("contains_digits")

    allowed = set(char_list)
    use_pipe = "|" in allowed
    filtered_chars: list[str] = []
    dropped = False
    for char in text:
        if char in allowed:
            filtered_chars.append(char)
        elif char.isspace() and use_pipe:
            filtered_chars.append("|")
        elif char.isspace():
            filtered_chars.append(" ")
        else:
            dropped = True
    normalized = "".join(filtered_chars)
    if use_pipe:
        normalized = re.sub(r"\|+", "|", normalized).strip("|")
    else:
        normalized = re.sub(r"\s+", " ", normalized).strip()
    if dropped:
        reason_codes.extend(["contains_symbols", "verifier_text_may_be_unreliable"])
    return normalized, sorted(set(reason_codes))


def resample_mono(audio: np.ndarray, sample_rate: int, target_rate: int = TARGET_SAMPLE_RATE) -> tuple[np.ndarray, int]:
    if sample_rate == target_rate:
        return audio.astype(np.float32, copy=False), sample_rate
    import librosa

    resampled = librosa.resample(audio.astype(np.float64), orig_sr=sample_rate, target_sr=target_rate)
    return resampled.astype(np.float32, copy=False), target_rate


def _min_window_score(values: np.ndarray, window: int) -> float | None:
    if values.size == 0:
        return None
    if values.size < window:
        return float(np.min(values))
    mins: list[float] = []
    for start in range(0, values.size - window + 1):
        mins.append(float(np.mean(values[start : start + window])))
    return float(min(mins))


def audio_coverage_metrics(
    *,
    audio_duration_sec: float,
    segments: list[Any],
    timings: Any,
    index_duration: float,
) -> dict[str, float | None]:
    """Estimate how much clip audio is consumed by the forced transcript alignment."""
    if audio_duration_sec <= 0:
        return {
            "audio_duration_sec": 0.0,
            "aligned_speech_sec": None,
            "unexplained_speech_sec": None,
            "aligned_speech_ratio": None,
            "unaligned_speech_ratio": None,
            "char_timings_span_sec": None,
        }

    aligned_speech_sec: float | None = None
    if segments:
        start_sec = min(float(segment[0]) for segment in segments)
        end_sec = max(float(segment[1]) for segment in segments)
        aligned_speech_sec = max(0.0, end_sec - start_sec)

    timing_values = np.asarray(timings, dtype=np.float64).reshape(-1)
    char_timings_span_sec = None
    if timing_values.size > 0:
        char_timings_span_sec = float(timing_values.max() - timing_values.min())
        if index_duration > 0:
            char_timings_span_sec = min(
                audio_duration_sec,
                char_timings_span_sec + index_duration,
            )

    if aligned_speech_sec is None and char_timings_span_sec is not None:
        aligned_speech_sec = char_timings_span_sec

    if aligned_speech_sec is None:
        return {
            "audio_duration_sec": round(audio_duration_sec, 6),
            "aligned_speech_sec": None,
            "unexplained_speech_sec": None,
            "aligned_speech_ratio": None,
            "unaligned_speech_ratio": None,
            "char_timings_span_sec": _round(char_timings_span_sec),
        }

    aligned_speech_sec = min(max(0.0, aligned_speech_sec), audio_duration_sec)
    unexplained_speech_sec = max(0.0, audio_duration_sec - aligned_speech_sec)
    aligned_speech_ratio = aligned_speech_sec / audio_duration_sec
    unaligned_speech_ratio = unexplained_speech_sec / audio_duration_sec
    return {
        "audio_duration_sec": round(audio_duration_sec, 6),
        "aligned_speech_sec": round(aligned_speech_sec, 6),
        "unexplained_speech_sec": round(unexplained_speech_sec, 6),
        "aligned_speech_ratio": round(aligned_speech_ratio, 6),
        "unaligned_speech_ratio": round(unaligned_speech_ratio, 6),
        "char_timings_span_sec": _round(char_timings_span_sec),
    }


def score_bucket(transcript_match_score: float | None) -> str:
    if transcript_match_score is None:
        return "failed"
    if transcript_match_score >= 85:
        return "accepted"
    if transcript_match_score >= 70:
        return "review"
    return "rejected"


def score_clip(
    audio: np.ndarray,
    sample_rate: int,
    verifier_text: str,
    bundle: CtcModelBundle,
) -> dict[str, Any]:
    import ctc_segmentation
    import torch

    audio, sample_rate = resample_mono(audio, sample_rate, TARGET_SAMPLE_RATE)
    if audio.size == 0:
        raise ValueError("empty_audio")

    inputs = bundle.processor(audio, sampling_rate=sample_rate, return_tensors="pt", padding="longest")
    input_values = inputs.input_values.to(bundle.device)
    with torch.no_grad():
        logits = bundle.model(input_values).logits.cpu()[0]
        probs = torch.nn.functional.softmax(logits, dim=-1).numpy()

    config = ctc_segmentation.CtcSegmentationParameters(char_list=bundle.char_list)
    config.index_duration = audio.shape[0] / probs.shape[0] / sample_rate
    ground_truth_mat, utt_begin_indices = ctc_segmentation.prepare_text(
        config,
        [verifier_text],
        char_list=bundle.char_list,
    )
    if ground_truth_mat.size == 0:
        raise ValueError("ctc_alignment_failed: empty ground truth matrix")

    timings, char_probs, _state_list = ctc_segmentation.ctc_segmentation(config, probs, ground_truth_mat)
    segments = ctc_segmentation.determine_utterance_segments(
        config,
        utt_begin_indices,
        char_probs,
        timings,
        [verifier_text],
    )
    char_prob_array = np.asarray(char_probs, dtype=np.float64)
    positive = char_prob_array[char_prob_array > 0]
    ctc_mean_score = float(np.mean(positive)) if positive.size else float(np.mean(char_prob_array))
    ctc_min_token_score = float(np.min(char_prob_array)) if char_prob_array.size else None
    ctc_min_aligned_token_score = float(np.min(positive)) if positive.size else None
    ctc_min_window_score = _min_window_score(char_prob_array, WEAK_WINDOW_FRAMES)
    unaligned_token_count = int(np.sum(char_prob_array < WEAK_CHAR_THRESHOLD))
    weak_span_count = 0
    if char_prob_array.size >= WEAK_WINDOW_FRAMES:
        for start in range(0, char_prob_array.size - WEAK_WINDOW_FRAMES + 1):
            if float(np.mean(char_prob_array[start : start + WEAK_WINDOW_FRAMES])) < WEAK_WINDOW_THRESHOLD:
                weak_span_count += 1

    segment_conf = None
    if segments:
        segment_conf = float(segments[0][2])
    raw_score = ctc_mean_score
    if segment_conf is not None and segment_conf > 0:
        raw_score = (ctc_mean_score + segment_conf) / 2.0
    transcript_match_score = round(max(0.0, min(1.0, raw_score)) * 100.0, 3)
    coverage = audio_coverage_metrics(
        audio_duration_sec=float(audio.shape[0]) / float(sample_rate),
        segments=segments,
        timings=timings,
        index_duration=float(config.index_duration),
    )

    return {
        "ctc_mean_score": _round(ctc_mean_score),
        "ctc_min_window_score": _round(ctc_min_window_score),
        "ctc_min_token_score": _round(ctc_min_token_score),
        "ctc_min_aligned_token_score": _round(ctc_min_aligned_token_score),
        "unaligned_token_count": unaligned_token_count,
        "weak_span_count": weak_span_count,
        "segment_confidence": _round(segment_conf),
        "transcript_match_score": transcript_match_score,
        "bucket": score_bucket(transcript_match_score),
        **coverage,
    }


def build_clip_row(
    candidate: dict[str, Any],
    run_root: Path,
    bundle: CtcModelBundle,
) -> dict[str, Any]:
    clip_id = str(candidate.get("id") or candidate.get("clip_id") or "")
    audio_rel = candidate.get("audio_path")
    base_row: dict[str, Any] = {
        "clip_id": clip_id,
        "audio_path": str(audio_rel or ""),
        "duration_sec": candidate.get("duration_sec"),
        "buffer_id": candidate.get("buffer_id"),
        "word_ids": candidate.get("word_ids") or [],
        "review_reason_codes": candidate.get("review_reason_codes") or [],
        "verifier_text": None,
        "verifier_text_source": None,
        "ctc_mean_score": None,
        "ctc_min_window_score": None,
        "ctc_min_token_score": None,
        "unaligned_token_count": None,
        "weak_span_count": None,
        "segment_confidence": None,
        "transcript_match_score": None,
        "audio_duration_sec": candidate.get("duration_sec"),
        "aligned_speech_sec": None,
        "unexplained_speech_sec": None,
        "aligned_speech_ratio": None,
        "unaligned_speech_ratio": None,
        "char_timings_span_sec": None,
        "bucket": "failed",
        "reason_codes": [],
    }

    source_field, source_text = select_verifier_source_text(candidate)
    if not source_text:
        base_row["reason_codes"] = ["empty_transcript"]
        base_row["error"] = "No verifier/normalized/alignment/training text found"
        return base_row
    base_row["verifier_text_source"] = source_field

    normalized, text_reasons = normalize_verifier_text(source_text, set(bundle.char_list))
    base_row["reason_codes"] = list(text_reasons)
    if not normalized:
        base_row["reason_codes"] = sorted(set(base_row["reason_codes"]) | {"empty_transcript", "unsupported_text"})
        base_row["error"] = "Verifier text empty after CTC normalization"
        return base_row
    base_row["verifier_text"] = normalized

    if not audio_rel:
        base_row["reason_codes"] = sorted(set(base_row["reason_codes"]) | {"missing_audio"})
        base_row["error"] = "Candidate row missing audio_path"
        return base_row

    try:
        audio_path = resolve_under_root(run_root, str(audio_rel))
    except ValueError as exc:
        base_row["reason_codes"] = sorted(set(base_row["reason_codes"]) | {"missing_audio"})
        base_row["error"] = str(exc)
        return base_row

    if not audio_path.exists():
        base_row["reason_codes"] = sorted(set(base_row["reason_codes"]) | {"missing_audio"})
        base_row["error"] = f"Audio file not found: {audio_path}"
        return base_row

    try:
        audio, sample_rate = read_analysis_audio(audio_path)
        metrics = score_clip(audio, sample_rate, normalized, bundle)
        base_row.update(metrics)
        return base_row
    except Exception as exc:  # noqa: BLE001 - per-clip failure isolation
        base_row["reason_codes"] = sorted(set(base_row["reason_codes"]) | {"ctc_scoring_failed"})
        base_row["error"] = str(exc)
        return base_row


def export_clip_bundle(
    row: dict[str, Any],
    run_root: Path,
    out_dir: Path,
    *,
    rank: int,
    prefix: str,
) -> str | None:
    audio_rel = row.get("audio_path")
    if not audio_rel or row.get("bucket") == "failed":
        return None
    try:
        audio_path = resolve_under_root(run_root, str(audio_rel))
    except ValueError:
        return None
    if not audio_path.exists():
        return None

    score = row.get("transcript_match_score")
    score_label = "na" if score is None else f"{int(round(float(score))):03d}"
    clip_id = str(row.get("clip_id") or "clip")
    stem = f"{rank:03d}_score_{score_label}_{clip_id}"
    wav_dest = out_dir / f"{stem}.wav"
    txt_dest = out_dir / f"{stem}.txt"
    shutil.copy2(audio_path, wav_dest)
    txt_dest.write_text(
        "\n".join(
            [
                f"clip_id: {clip_id}",
                f"score: {score}",
                f"bucket: {row.get('bucket')}",
                f"duration: {row.get('duration_sec')}",
                f"reason_codes: {', '.join(row.get('reason_codes') or [])}",
                "verifier_text:",
                str(row.get("verifier_text") or ""),
                "audio_path:",
                str(audio_rel),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return str(wav_dest)


def analyze_ctc_transcript_qc(
    run_root: Path,
    out_dir: Path,
    *,
    model_name: str = DEFAULT_MODEL,
    max_clips: int | None = None,
    export_worst: int = 50,
    export_best: int = 20,
    device: str = "auto",
    batch_size: int = 1,
) -> dict[str, Any]:
    del batch_size  # reserved for later batching; keep CLI stable

    manifest_path = resolve_under_root(run_root, "artifacts/candidate_review_manifest.json")
    candidates = read_json_value(manifest_path)
    if not isinstance(candidates, list):
        raise ValueError("candidate_review_manifest.json must contain a list")
    if max_clips is not None:
        candidates = candidates[: max(0, max_clips)]

    out_dir.mkdir(parents=True, exist_ok=True)
    bundle = load_ctc_model(model_name, device)

    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        rows.append(build_clip_row(candidate, run_root, bundle))

    scored_rows = [row for row in rows if row.get("bucket") != "failed"]
    failed_rows = [row for row in rows if row.get("bucket") == "failed"]
    scores = [float(row["transcript_match_score"]) for row in scored_rows if row.get("transcript_match_score") is not None]

    bucket_counts = {
        "accepted": sum(row.get("bucket") == "accepted" for row in rows),
        "review": sum(row.get("bucket") == "review" for row in rows),
        "rejected": sum(row.get("bucket") == "rejected" for row in rows),
        "failed": len(failed_rows),
    }
    reason_counts: dict[str, int] = {}
    for row in rows:
        for reason in row.get("reason_codes") or []:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

    worst_dir = out_dir / "worst_clips"
    best_dir = out_dir / "best_clips"
    if worst_dir.exists():
        shutil.rmtree(worst_dir)
    if best_dir.exists():
        shutil.rmtree(best_dir)
    worst_dir.mkdir(parents=True, exist_ok=True)
    best_dir.mkdir(parents=True, exist_ok=True)

    ranked = sorted(
        scored_rows,
        key=lambda row: (
            float(row.get("transcript_match_score") or 0.0),
            str(row.get("clip_id") or ""),
        ),
    )
    worst = ranked[: max(0, export_worst)]
    best = list(reversed(ranked[-max(0, export_best) :])) if ranked else []

    worst_paths: list[str] = []
    for rank, row in enumerate(worst, start=1):
        exported = export_clip_bundle(row, run_root, worst_dir, rank=rank, prefix="worst")
        if exported:
            worst_paths.append(exported)
    for rank, row in enumerate(best, start=1):
        export_clip_bundle(row, run_root, best_dir, rank=rank, prefix="best")

    csv_rows: list[dict[str, Any]] = []
    for rank, row in enumerate(ranked, start=1):
        exported = next((path for path in worst_paths if str(row.get("clip_id") or "") in path), "")
        csv_rows.append(
            {
                "rank": rank,
                "clip_id": row.get("clip_id"),
                "score": row.get("transcript_match_score"),
                "bucket": row.get("bucket"),
                "duration_sec": row.get("duration_sec"),
                "verifier_text": row.get("verifier_text"),
                "audio_path": row.get("audio_path"),
                "exported_wav": exported,
                "reason_codes": "|".join(row.get("reason_codes") or []),
            }
        )

    summary = {
        "run_root": str(run_root),
        "model": model_name,
        "clip_count": len(rows),
        "scored_count": len(scored_rows),
        "failed_count": len(failed_rows),
        "score_p50": _percentile(scores, 50),
        "score_p10": _percentile(scores, 10),
        "score_p90": _percentile(scores, 90),
        "bucket_counts": bucket_counts,
        "reason_counts": dict(sorted(reason_counts.items())),
        "worst_clip_paths": worst_paths,
        "best_clip_export_count": len(best),
        "notes": {
            "segment_confidence_may_be_zero": "Some clips return segment confidence 0.0; ranking uses mean char_probs.",
            "borderline_review_band": "70 <= score < 85",
        },
    }

    write_json(out_dir / "ctc_transcript_qc.json", rows)
    write_json(out_dir / "ctc_transcript_qc_summary.json", summary)
    with (out_dir / "ctc_transcript_qc_by_score.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "rank",
                "clip_id",
                "score",
                "bucket",
                "duration_sec",
                "verifier_text",
                "audio_path",
                "exported_wav",
                "reason_codes",
            ],
        )
        writer.writeheader()
        writer.writerows(csv_rows)

    log(f"scored {len(scored_rows)}/{len(rows)} clips -> {out_dir}")
    log(f"buckets: {bucket_counts}")
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline CTC transcript QC experiment for candidate review clips.")
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--max-clips", type=int, default=None)
    parser.add_argument("--export-worst", type=int, default=50)
    parser.add_argument("--export-best", type=int, default=20)
    parser.add_argument("--device", choices=["auto", "cuda", "cpu"], default="auto")
    parser.add_argument("--batch-size", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = analyze_ctc_transcript_qc(
        Path(args.run_root).expanduser().resolve(),
        Path(args.out).expanduser().resolve(),
        model_name=args.model,
        max_clips=args.max_clips,
        export_worst=args.export_worst,
        export_best=args.export_best,
        device=args.device,
        batch_size=args.batch_size,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
