#!/usr/bin/env python3
"""Evaluate Linus Safe Gap whitelist tripwires on inter-word audio gaps.

Philosophy (whitelist, not blacklist)
-------------------------------------
We do not try to detect laughs, screams, clicks, or vape hits individually.
We define what a *safe* gap looks like (quiet, unvoiced, smooth) and reject
everything else by default.

Three tripwires
---------------
1. Volume ceiling   — gap RMS must stay below ``volume_ratio_max`` × flanking speech RMS.
2. Voicing detector — librosa spectral flatness (harmonic structure) + pyin pitch confidence.
3. Crest factor     — peak/RMS must stay below ``max_crest_factor`` (transient killer).

Outputs
-------
``out_dir/safe_breaths_and_silence/``   gaps that passed all rules
``out_dir/rejected_garbage/``             gaps that tripped any rule
``out_dir/index.json``                    metrics + reject reasons for every gap
``out_dir/summary.json``                  aggregate counts / rule breakdown

Listen to ``rejected_garbage/``. Perfect breaths there → loosen thresholds.
Chuckles in ``safe_breaths_and_silence/`` → tighten thresholds.

Examples
--------
From a completed dataset run (MFA aligned words + source WAV):

  cd backend && ./.venv/bin/python scripts/eval_safe_gaps.py \\
    --dataset-run-root data/media/dataset-runs/mc-mq3xswcx/dataset-4a3f897e9802 \\
    --out-dir /tmp/safe_gap_eval_mc

From a source WAV + alignment JSON (seconds-based word list):

  ./.venv/bin/python scripts/eval_safe_gaps.py \\
    --source-wav data/media/sources/source-....wav \\
    --alignment-json path/to/alignment.json \\
    --out-dir /tmp/safe_gap_eval_manual
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
import wave
from dataclasses import asdict, dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Any

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.reference_acoustic_signature import crop_mono_samples, mono_pcm16_samples_from_wav_bytes

import librosa


@dataclass(frozen=True)
class SafeGapTripwires:
    """Thresholds for the three whitelist rules (tune after ear-check)."""

    volume_ratio_max: float = 0.15
    volume_only: bool = False
    raw_mfa_gaps: bool = False
    min_spectral_flatness: float = 0.15
    max_pitch_confidence: float = 0.30
    silence_ratio_max: float = 0.05
    max_crest_factor: float = 8.0
    min_gap_ms: float = 40.0
    max_gap_ms: float = 1200.0
    trailing_word_guard_ms: float = 35.0
    leading_word_guard_ms: float = 45.0


@dataclass
class GapInterval:
    gap_id: str
    start_seconds: float
    end_seconds: float
    left_word: str
    right_word: str
    buffer_id: str | None = None
    source_path: str | None = None
    source_start_sample: int | None = None
    source_end_sample: int | None = None
    left_word_start_sample: int | None = None
    left_word_end_sample: int | None = None
    right_word_start_sample: int | None = None
    right_word_end_sample: int | None = None


@dataclass
class GapMetrics:
    gap_rms: float
    speech_rms: float
    volume_ratio: float
    spectral_flatness: float
    pitch_hz: float
    pitch_confidence: float
    peak: float
    crest_factor: float


@dataclass
class GapVerdict:
    gap: GapInterval
    metrics: GapMetrics
    passed: bool
    failed_rules: list[str] = field(default_factory=list)
    wav_path: str | None = None


def log(message: str) -> None:
    print(f"[eval_safe_gaps] {message}", flush=True)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        cleaned = line.strip()
        if cleaned:
            rows.append(json.loads(cleaned))
    return rows


def load_pcm16_mono(path: Path) -> tuple[np.ndarray, int]:
    return mono_pcm16_samples_from_wav_bytes(path.read_bytes())


def write_pcm16_mono(path: Path, samples: np.ndarray, sample_rate: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    clipped = np.clip(samples, -1.0, 1.0)
    pcm = (clipped * 32767.0).astype(np.int16)
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(pcm.tobytes())


def slug(text: str, limit: int = 32) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", (text or "gap").strip()).strip("_").lower()
    return (cleaned[:limit].strip("_") or "gap")


def rms(samples: np.ndarray) -> float:
    if samples.size == 0:
        return 0.0
    return float(np.sqrt(np.mean(samples * samples)))


def crest_factor(samples: np.ndarray) -> tuple[float, float, float]:
    gap_rms = rms(samples)
    peak = float(np.max(np.abs(samples))) if samples.size else 0.0
    if gap_rms <= 1e-12:
        return peak, gap_rms, 0.0
    return peak, gap_rms, peak / gap_rms


def energized_gap_samples(
    gap_samples: np.ndarray,
    sample_rate: int,
    speech_rms: float,
    *,
    frame_ms: float = 10.0,
) -> np.ndarray:
    """Keep only gap frames loud enough to analyze — ignores pure silence tails."""
    if gap_samples.size == 0:
        return gap_samples
    frame_length = max(int(sample_rate * frame_ms / 1000.0), 64)
    hop_length = max(frame_length // 2, 32)
    threshold = max(speech_rms * 0.03, 1e-5)
    chunks: list[np.ndarray] = []
    for start in range(0, len(gap_samples) - frame_length + 1, hop_length):
        frame = gap_samples[start : start + frame_length]
        if rms(frame) >= threshold:
            chunks.append(frame)
    if not chunks:
        return np.zeros(0, dtype=np.float32)
    return np.concatenate(chunks)


def gap_spectral_flatness(samples: np.ndarray, sample_rate: int) -> float:
    """Median frame spectral flatness — noise/breath is flat; chuckles are harmonic."""
    if samples.size < 256:
        return 1.0
    frame_length = min(2048, max(512, len(samples)))
    hop_length = max(frame_length // 4, 128)
    flatness = librosa.feature.spectral_flatness(
        y=samples.astype(np.float64),
        n_fft=frame_length,
        hop_length=hop_length,
    )
    if flatness.size == 0:
        return 1.0
    return float(np.median(flatness))


def gap_pitch_confidence(samples: np.ndarray, sample_rate: int) -> tuple[float, float]:
    """Return (pitch_hz, confidence) from librosa.pyin. Confidence in [0, 1]."""
    if samples.size < sample_rate // 20:
        return 0.0, 0.0
    frame_length = min(2048, max(512, len(samples) // 2))
    hop_length = max(frame_length // 4, 128)
    f0, _voiced_flag, voiced_prob = librosa.pyin(
        samples.astype(np.float64),
        fmin=librosa.note_to_hz("C2"),
        fmax=librosa.note_to_hz("C7"),
        sr=sample_rate,
        frame_length=frame_length,
        hop_length=hop_length,
    )
    voiced_probs = voiced_prob[np.isfinite(voiced_prob)]
    voiced_f0 = f0[np.isfinite(f0)]
    if voiced_probs.size == 0:
        return 0.0, 0.0
    confidence = float(np.max(voiced_probs))
    pitch_hz = float(np.median(voiced_f0)) if voiced_f0.size else 0.0
    return pitch_hz, confidence


def gap_max_crest_factor(samples: np.ndarray, sample_rate: int) -> float:
    """Max short-frame crest factor — catches keyboard clacks that pass average RMS."""
    if samples.size < 64:
        return 0.0
    frame_length = max(int(sample_rate * 0.010), 64)
    hop_length = max(frame_length // 2, 32)
    crests: list[float] = []
    for start in range(0, len(samples) - frame_length + 1, hop_length):
        frame = samples[start : start + frame_length]
        frame_rms = rms(frame)
        if frame_rms <= 1e-12:
            continue
        crests.append(float(np.max(np.abs(frame)) / frame_rms))
    if not crests:
        _, _, crest = crest_factor(samples)
        return crest
    return float(max(crests))


def analyze_gap(
    gap_samples: np.ndarray,
    left_speech_samples: np.ndarray,
    right_speech_samples: np.ndarray,
    tripwires: SafeGapTripwires,
    sample_rate: int,
) -> tuple[GapMetrics, list[str]]:
    gap_rms = rms(gap_samples)
    left_rms = rms(left_speech_samples)
    right_rms = rms(right_speech_samples)
    # Reference speech level: louder of the two flanking MFA word spans.
    speech_rms = max(left_rms, right_rms, 1e-9)
    volume_ratio = gap_rms / speech_rms

    if tripwires.volume_only:
        metrics = GapMetrics(
            gap_rms=round(gap_rms, 6),
            speech_rms=round(speech_rms, 6),
            volume_ratio=round(volume_ratio, 6),
            spectral_flatness=0.0,
            pitch_hz=0.0,
            pitch_confidence=0.0,
            peak=round(float(np.max(np.abs(gap_samples))) if gap_samples.size else 0.0, 6),
            crest_factor=0.0,
        )
        failed: list[str] = []
        if volume_ratio > tripwires.volume_ratio_max:
            failed.append("loud_untranscribed_audio")
        return metrics, failed

    energized = energized_gap_samples(gap_samples, sample_rate, speech_rms)
    is_silence_gap = volume_ratio <= tripwires.silence_ratio_max or energized.size == 0

    if is_silence_gap:
        flatness = 1.0
        pitch_hz, pitch_conf = 0.0, 0.0
        crest = 0.0
        peak = float(np.max(np.abs(gap_samples))) if gap_samples.size else 0.0
    else:
        flatness = gap_spectral_flatness(energized, sample_rate)
        pitch_hz, pitch_conf = gap_pitch_confidence(energized, sample_rate)
        peak = float(np.max(np.abs(gap_samples))) if gap_samples.size else 0.0
        crest = gap_max_crest_factor(energized, sample_rate)

    metrics = GapMetrics(
        gap_rms=round(gap_rms, 6),
        speech_rms=round(speech_rms, 6),
        volume_ratio=round(volume_ratio, 6),
        spectral_flatness=round(flatness, 6),
        pitch_hz=round(pitch_hz, 3),
        pitch_confidence=round(pitch_conf, 6),
        peak=round(peak, 6),
        crest_factor=round(crest, 6),
    )

    failed: list[str] = []
    if volume_ratio > tripwires.volume_ratio_max:
        failed.append("volume_ceiling")
    if not is_silence_gap:
        if flatness < tripwires.min_spectral_flatness:
            failed.append("harmonic_structure")
        if pitch_conf > tripwires.max_pitch_confidence:
            failed.append("pitched_voicing")
        if crest > tripwires.max_crest_factor:
            failed.append("crest_factor")
    return metrics, failed


def gaps_from_alignment_seconds(
    words: list[dict[str, Any]],
    tripwires: SafeGapTripwires,
    *,
    gap_id_prefix: str = "gap",
) -> list[GapInterval]:
    ordered = sorted(words, key=lambda row: float(row["start"]))
    gaps: list[GapInterval] = []
    trailing_guard = tripwires.trailing_word_guard_ms / 1000.0
    leading_guard = tripwires.leading_word_guard_ms / 1000.0
    min_gap = tripwires.min_gap_ms / 1000.0
    max_gap = tripwires.max_gap_ms / 1000.0

    for index, (left, right) in enumerate(zip(ordered, ordered[1:])):
        gap_start = float(left["end"]) + trailing_guard
        gap_end = float(right["start"]) - leading_guard
        duration = gap_end - gap_start
        if duration < min_gap or duration > max_gap:
            continue
        left_word = str(left.get("word") or left.get("raw_token") or "")
        right_word = str(right.get("word") or right.get("raw_token") or "")
        gaps.append(
            GapInterval(
                gap_id=f"{gap_id_prefix}-{index:05d}",
                start_seconds=round(gap_start, 6),
                end_seconds=round(gap_end, 6),
                left_word=left_word,
                right_word=right_word,
            )
        )
    return gaps


def gaps_from_dataset_run(
    run_root: Path,
    tripwires: SafeGapTripwires,
) -> tuple[list[GapInterval], dict[str, tuple[Path, int]]]:
    """Build gap intervals from aligned_words.jsonl at native source sample times."""
    artifacts = run_root / "artifacts"
    words = read_jsonl(artifacts / "aligned_words.jsonl")
    if not words:
        raise ValueError(f"No aligned words found under {artifacts / 'aligned_words.jsonl'}")

    manifest = json.loads((artifacts / "source_audio_manifest.json").read_text(encoding="utf-8"))
    sources = manifest.get("sources") or []
    if not sources:
        raise ValueError("source_audio_manifest.json has no sources")
    source_by_audio_id = {
        str(source["source_audio_id"]): (
            Path(str(source["path"])).expanduser().resolve(),
            int(source["sample_rate"]),
        )
        for source in sources
    }

    words_by_buffer: dict[str, list[dict[str, Any]]] = {}
    for word in words:
        words_by_buffer.setdefault(str(word["buffer_id"]), []).append(word)

    buffers = json.loads((artifacts / "processing_buffers.json").read_text(encoding="utf-8"))
    if not isinstance(buffers, list):
        raise ValueError("processing_buffers.json must be a list")

    gaps: list[GapInterval] = []
    for buffer in buffers:
        buffer_id = str(buffer["buffer_id"])
        source_audio_id = str(buffer["source_audio_id"])
        if source_audio_id not in source_by_audio_id:
            continue
        _source_path, sample_rate = source_by_audio_id[source_audio_id]
        min_gap_samples = 0 if tripwires.raw_mfa_gaps else int(round(tripwires.min_gap_ms / 1000.0 * sample_rate))
        max_gap_samples = (
            10**12 if tripwires.raw_mfa_gaps else int(round(tripwires.max_gap_ms / 1000.0 * sample_rate))
        )
        trailing_guard = 0 if tripwires.raw_mfa_gaps else int(round(tripwires.trailing_word_guard_ms / 1000.0 * sample_rate))
        leading_guard = 0 if tripwires.raw_mfa_gaps else int(round(tripwires.leading_word_guard_ms / 1000.0 * sample_rate))
        buffer_words = sorted(
            words_by_buffer.get(buffer_id, []),
            key=lambda row: int(row["source_start_sample"]),
        )
        for index, (left, right) in enumerate(zip(buffer_words, buffer_words[1:])):
            gap_start = int(left["source_end_sample"]) + trailing_guard
            gap_end = int(right["source_start_sample"]) - leading_guard
            duration_samples = gap_end - gap_start
            if duration_samples <= 0 or duration_samples < min_gap_samples or duration_samples > max_gap_samples:
                continue
            source_path, _rate = source_by_audio_id[source_audio_id]
            gaps.append(
                GapInterval(
                    gap_id=f"{buffer_id}-gap-{index:04d}",
                    start_seconds=round(gap_start / sample_rate, 6),
                    end_seconds=round(gap_end / sample_rate, 6),
                    left_word=str(left.get("word") or left.get("raw_token") or ""),
                    right_word=str(right.get("word") or right.get("raw_token") or ""),
                    buffer_id=buffer_id,
                    source_path=str(source_path),
                    source_start_sample=gap_start,
                    source_end_sample=gap_end,
                    left_word_start_sample=int(left["source_start_sample"]),
                    left_word_end_sample=int(left["source_end_sample"]),
                    right_word_start_sample=int(right["source_start_sample"]),
                    right_word_end_sample=int(right["source_end_sample"]),
                )
            )
    return gaps, source_by_audio_id


def gaps_from_safe_cutpoints(
    run_root: Path,
) -> tuple[list[GapInterval], dict[str, tuple[Path, int]]]:
    """Build gap intervals from accepted safe_cutpoints.jsonl MFA gap spans."""
    artifacts = run_root / "artifacts"
    cutpoints = read_jsonl(artifacts / "safe_cutpoints.jsonl")
    if not cutpoints:
        raise ValueError(f"No safe cutpoints found under {artifacts / 'safe_cutpoints.jsonl'}")

    words = read_jsonl(artifacts / "aligned_words.jsonl")
    words_by_id = {str(word["id"]): word for word in words}

    manifest = json.loads((artifacts / "source_audio_manifest.json").read_text(encoding="utf-8"))
    sources = manifest.get("sources") or []
    if not sources:
        raise ValueError("source_audio_manifest.json has no sources")
    source_by_audio_id = {
        str(source["source_audio_id"]): (
            Path(str(source["path"])).expanduser().resolve(),
            int(source["sample_rate"]),
        )
        for source in sources
    }

    buffers = json.loads((artifacts / "processing_buffers.json").read_text(encoding="utf-8"))
    buffer_by_id = {str(buffer["buffer_id"]): buffer for buffer in buffers}

    gaps: list[GapInterval] = []
    for cutpoint in cutpoints:
        buffer_id = str(cutpoint["buffer_id"])
        buffer = buffer_by_id.get(buffer_id)
        if buffer is None:
            continue
        source_audio_id = str(buffer["source_audio_id"])
        if source_audio_id not in source_by_audio_id:
            continue
        source_path, sample_rate = source_by_audio_id[source_audio_id]
        source_start = int(buffer["source_start_sample"])
        gap_start = source_start + int(cutpoint["gap_start_local_sample"])
        gap_end = source_start + int(cutpoint["gap_end_local_sample"])
        if gap_end <= gap_start:
            continue

        left = words_by_id.get(str(cutpoint["left_word_id"]))
        right = words_by_id.get(str(cutpoint["right_word_id"]))
        if left is None or right is None:
            continue

        gaps.append(
            GapInterval(
                gap_id=str(cutpoint["id"]),
                start_seconds=round(gap_start / sample_rate, 6),
                end_seconds=round(gap_end / sample_rate, 6),
                left_word=str(cutpoint.get("left_word") or left.get("word") or left.get("raw_token") or ""),
                right_word=str(cutpoint.get("right_word") or right.get("word") or right.get("raw_token") or ""),
                buffer_id=buffer_id,
                source_path=str(source_path),
                source_start_sample=gap_start,
                source_end_sample=gap_end,
                left_word_start_sample=int(left["source_start_sample"]),
                left_word_end_sample=int(left["source_end_sample"]),
                right_word_start_sample=int(right["source_start_sample"]),
                right_word_end_sample=int(right["source_end_sample"]),
            )
        )
    return gaps, source_by_audio_id


def crop_word_speech(samples: np.ndarray, sample_rate: int, start_seconds: float, end_seconds: float) -> np.ndarray:
    return crop_mono_samples(samples, sample_rate, start_seconds, end_seconds)


def evaluate_gaps(
    gaps: list[GapInterval],
    samples_by_source: dict[str, tuple[np.ndarray, int]],
    tripwires: SafeGapTripwires,
    out_dir: Path,
    *,
    max_gaps: int | None = None,
) -> tuple[list[GapVerdict], dict[str, Any]]:
    safe_dir = out_dir / "safe_breaths_and_silence"
    reject_dir = out_dir / "rejected_garbage"
    safe_dir.mkdir(parents=True, exist_ok=True)
    reject_dir.mkdir(parents=True, exist_ok=True)

    selected = gaps if max_gaps is None else gaps[:max_gaps]
    verdicts: list[GapVerdict] = []
    rule_hits: dict[str, int] = {
        "loud_untranscribed_audio": 0,
        "volume_ceiling": 0,
        "harmonic_structure": 0,
        "pitched_voicing": 0,
        "crest_factor": 0,
    }

    for rank, gap in enumerate(selected, start=1):
        if gap.source_path is None:
            raise ValueError(f"Gap {gap.gap_id} is missing source_path")
        samples, sample_rate = samples_by_source[gap.source_path]
        gap_samples = samples[gap.source_start_sample : gap.source_end_sample]
        if gap_samples.size == 0:
            continue

        if (
            gap.left_word_start_sample is None
            or gap.left_word_end_sample is None
            or gap.right_word_start_sample is None
            or gap.right_word_end_sample is None
        ):
            raise ValueError(f"Gap {gap.gap_id} is missing MFA word bounds for speech RMS")
        left_speech = samples[gap.left_word_start_sample : gap.left_word_end_sample]
        right_speech = samples[gap.right_word_start_sample : gap.right_word_end_sample]

        metrics, failed_rules = analyze_gap(gap_samples, left_speech, right_speech, tripwires, sample_rate)
        passed = not failed_rules
        for rule in failed_rules:
            rule_hits[rule] += 1

        label = f"{rank:04d}_{gap.start_seconds:08.3f}s-{gap.end_seconds:08.3f}s_{slug(gap.left_word)}__{slug(gap.right_word)}"
        dest_dir = safe_dir if passed else reject_dir
        wav_path = dest_dir / f"{label}.wav"
        write_pcm16_mono(wav_path, gap_samples, sample_rate)

        verdicts.append(
            GapVerdict(
                gap=gap,
                metrics=metrics,
                passed=passed,
                failed_rules=failed_rules,
                wav_path=str(wav_path),
            )
        )

    summary = {
        "gap_count": len(verdicts),
        "safe_count": sum(1 for verdict in verdicts if verdict.passed),
        "rejected_count": sum(1 for verdict in verdicts if not verdict.passed),
        "rule_hits": rule_hits,
        "tripwires": asdict(tripwires),
        "librosa_version": librosa.__version__,
        "safe_dir": str(safe_dir),
        "rejected_dir": str(reject_dir),
    }
    return verdicts, summary


def verdict_to_json(verdict: GapVerdict) -> dict[str, Any]:
    return {
        "gap_id": verdict.gap.gap_id,
        "passed": verdict.passed,
        "failed_rules": verdict.failed_rules,
        "start_seconds": verdict.gap.start_seconds,
        "end_seconds": verdict.gap.end_seconds,
        "duration_ms": round((verdict.gap.end_seconds - verdict.gap.start_seconds) * 1000.0, 3),
        "left_word": verdict.gap.left_word,
        "right_word": verdict.gap.right_word,
        "buffer_id": verdict.gap.buffer_id,
        "metrics": asdict(verdict.metrics),
        "wav_path": verdict.wav_path,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate Safe Gap whitelist tripwires and dump audio for ear-tuning.",
    )
    parser.add_argument("--out-dir", required=True, help="Output directory for wav folders + JSON reports.")
    parser.add_argument(
        "--dataset-run-root",
        help="Path to a completed dataset run root (contains artifacts/aligned_words.jsonl).",
    )
    parser.add_argument("--source-wav", help="Native source WAV path (required for manual mode).")
    parser.add_argument(
        "--alignment-json",
        help="Alignment JSON list with {word,start,end} in seconds (manual mode).",
    )
    parser.add_argument("--max-gaps", type=int, help="Optional cap for quick smoke tests.")
    parser.add_argument(
        "--volume-only",
        action="store_true",
        help="Linus Rule 1 only: gap_rms / max(flanking word RMS) > threshold → loud_untranscribed_audio.",
    )
    parser.add_argument(
        "--safe-cutpoints-only",
        action="store_true",
        help="Evaluate only MFA gaps that have an accepted safe cutpoint.",
    )
    parser.add_argument(
        "--raw-mfa-gaps",
        action="store_true",
        help="Use full MFA inter-word spans (no guard margins or duration filters).",
    )
    parser.add_argument("--volume-ratio-max", type=float, default=None)
    parser.add_argument("--min-spectral-flatness", type=float, default=SafeGapTripwires.min_spectral_flatness)
    parser.add_argument("--max-pitch-confidence", type=float, default=SafeGapTripwires.max_pitch_confidence)
    parser.add_argument("--max-crest-factor", type=float, default=SafeGapTripwires.max_crest_factor)
    parser.add_argument("--min-gap-ms", type=float, default=SafeGapTripwires.min_gap_ms)
    parser.add_argument("--max-gap-ms", type=float, default=SafeGapTripwires.max_gap_ms)
    parser.add_argument("--silence-ratio-max", type=float, default=SafeGapTripwires.silence_ratio_max)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    volume_only = bool(args.volume_only)
    raw_mfa_gaps = bool(args.raw_mfa_gaps) or volume_only
    volume_ratio_max = (
        args.volume_ratio_max
        if args.volume_ratio_max is not None
        else (0.65 if volume_only else SafeGapTripwires.volume_ratio_max)
    )
    tripwires = SafeGapTripwires(
        volume_ratio_max=volume_ratio_max,
        volume_only=volume_only,
        raw_mfa_gaps=raw_mfa_gaps,
        min_spectral_flatness=args.min_spectral_flatness,
        max_pitch_confidence=args.max_pitch_confidence,
        max_crest_factor=args.max_crest_factor,
        min_gap_ms=args.min_gap_ms,
        max_gap_ms=args.max_gap_ms,
        silence_ratio_max=args.silence_ratio_max,
    )

    if args.dataset_run_root:
        run_root = Path(args.dataset_run_root).expanduser().resolve()
        if args.safe_cutpoints_only:
            gaps, source_by_audio_id = gaps_from_safe_cutpoints(run_root)
        else:
            gaps, source_by_audio_id = gaps_from_dataset_run(run_root, tripwires)
        samples_by_source: dict[str, tuple[np.ndarray, int]] = {}
        for _audio_id, (source_path, sample_rate) in source_by_audio_id.items():
            key = str(source_path)
            if key not in samples_by_source:
                samples_by_source[key] = load_pcm16_mono(source_path)
        log(f"dataset run: {run_root}")
        log(f"sources loaded: {len(samples_by_source)}")
    elif args.source_wav and args.alignment_json:
        source_path = Path(args.source_wav).expanduser().resolve()
        alignment = json.loads(Path(args.alignment_json).expanduser().resolve().read_text(encoding="utf-8"))
        if not isinstance(alignment, list):
            raise ValueError("--alignment-json must be a JSON list of {word,start,end}")
        ordered = sorted(alignment, key=lambda row: float(row["start"]))
        gaps = gaps_from_alignment_seconds(ordered, tripwires)
        samples, sample_rate = load_pcm16_mono(source_path)
        for gap, (left, right) in zip(gaps, zip(ordered, ordered[1:])):
            gap.source_path = str(source_path)
            gap.source_start_sample = int(round(gap.start_seconds * sample_rate))
            gap.source_end_sample = int(round(gap.end_seconds * sample_rate))
            gap.left_word_start_sample = int(round(float(left["start"]) * sample_rate))
            gap.left_word_end_sample = int(round(float(left["end"]) * sample_rate))
            gap.right_word_start_sample = int(round(float(right["start"]) * sample_rate))
            gap.right_word_end_sample = int(round(float(right["end"]) * sample_rate))
        samples_by_source = {str(source_path): (samples, sample_rate)}
    else:
        raise SystemExit("Provide --dataset-run-root OR (--source-wav AND --alignment-json).")

    log(f"gap candidates: {len(gaps)}")
    if volume_only:
        gap_mode = "safe_cutpoints" if args.safe_cutpoints_only else f"raw_mfa_gaps={raw_mfa_gaps}"
        log(f"mode: volume-only (Linus Rule 1), {gap_mode}, threshold={tripwires.volume_ratio_max}")
    else:
        log(f"librosa: {librosa.__version__}")
        log(
            "tripwires: "
            f"volume_ratio<={tripwires.volume_ratio_max}, "
            f"flatness>={tripwires.min_spectral_flatness}, "
            f"pitch_conf<={tripwires.max_pitch_confidence}, "
            f"crest<={tripwires.max_crest_factor}"
        )

    verdicts, summary = evaluate_gaps(
        gaps,
        samples_by_source,
        tripwires,
        out_dir,
        max_gaps=args.max_gaps,
    )

    index_path = out_dir / "index.json"
    summary_path = out_dir / "summary.json"
    index_path.write_text(json.dumps([verdict_to_json(v) for v in verdicts], indent=2), encoding="utf-8")
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    log(f"safe: {summary['safe_count']} -> {summary['safe_dir']}")
    log(f"rejected: {summary['rejected_count']} -> {summary['rejected_dir']}")
    log(f"rule hits: {summary['rule_hits']}")
    log(f"wrote {index_path}")
    log(f"wrote {summary_path}")


if __name__ == "__main__":
    main()
