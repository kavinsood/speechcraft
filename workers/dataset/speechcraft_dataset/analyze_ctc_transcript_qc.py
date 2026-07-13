from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import numpy as np

from .buffers import read_analysis_audio
from .io import read_json_value, resolve_under_root, sha256_file, write_json

DEFAULT_MODEL = "facebook/wav2vec2-base-960h"
TARGET_SAMPLE_RATE = 16000
WEAK_CHAR_THRESHOLD = 0.25
WEAK_WINDOW_THRESHOLD = 0.50
WEAK_WINDOW_FRAMES = 5
TRANSCRIPT_QC_SCHEMA_VERSION = 1
TRANSCRIPT_SCORE_METHOD = "min_alignment_and_confirmed_greedy_insertion_v1"
WORKER_TRANSCRIPT_THRESHOLD_HINT = 85
GREEDY_INTEGRITY_PASS_SCORE = 100.0
GREEDY_INTEGRITY_FAIL_SCORE = 0.0
GREEDY_INSERTION_MIN_WORD_LEN = 3
INSERTION_INTERIOR_CONFIDENCE_THRESHOLD = 0.70
INSERTION_EDGE_CONFIDENCE_THRESHOLD = 0.85
AUDIO_EDGE_FRACTION = 0.10
UNTRANSCRIBED_SPEECH_REASON = "untranscribed_speech_detected"
CONFIRMED_GREEDY_INSERTION_REASON = "confirmed_greedy_insertion"


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


def _round_score_0_100(value: float | None) -> float | None:
    if value is None:
        return None
    return round(max(0.0, min(100.0, float(value) * 100.0)), 2)


def resolve_device(device_arg: str) -> str:
    import torch

    if device_arg == "cpu":
        return "cpu"
    if device_arg == "cuda":
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA requested but not available")
        return "cuda"
    return "cuda" if torch.cuda.is_available() else "cpu"


def load_ctc_model(
    model_name: str,
    device_arg: str,
    *,
    local_files_only: bool = True,
) -> CtcModelBundle:
    import torch
    from transformers import Wav2Vec2ForCTC, Wav2Vec2Processor

    device = resolve_device(device_arg)
    source_mode = "local cache only" if local_files_only else "download/cache via Hugging Face if needed"
    log(f"loading CTC model {model_name!r} on {device} ({source_mode})")
    processor = Wav2Vec2Processor.from_pretrained(model_name, local_files_only=local_files_only)
    model = Wav2Vec2ForCTC.from_pretrained(model_name, local_files_only=local_files_only)
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


def score_bucket_hint(transcript_match_score: float | None) -> str:
    if transcript_match_score is None:
        return "unscored"
    if transcript_match_score >= WORKER_TRANSCRIPT_THRESHOLD_HINT:
        return "pass"
    if transcript_match_score >= 70:
        return "review"
    return "fail"


def _meaningful_word_spans(
    verifier_text: str,
    char_probs: np.ndarray,
    timings: Any,
    index_duration: float,
) -> list[dict[str, Any]]:
    timing_values = np.asarray(timings, dtype=np.float64).reshape(-1)
    if char_probs.size == 0 or timing_values.size == 0:
        return []

    length = min(len(verifier_text), char_probs.size, timing_values.size)
    words: list[dict[str, Any]] = []
    current_chars: list[str] = []
    current_probs: list[float] = []
    current_times: list[float] = []

    def flush() -> None:
        if not current_chars:
            return
        text = "".join(current_chars)
        if len([char for char in text if char.isalnum()]) < 2:
            current_chars.clear()
            current_probs.clear()
            current_times.clear()
            return
        start_sec = float(min(current_times))
        end_sec = float(max(current_times) + max(index_duration, 0.0))
        words.append(
            {
                "text": text,
                "score": float(np.mean(np.asarray(current_probs, dtype=np.float64))),
                "start_sec": round(start_sec, 6),
                "end_sec": round(end_sec, 6),
                "char_count": len(current_probs),
            }
        )
        current_chars.clear()
        current_probs.clear()
        current_times.clear()

    for index in range(length):
        char = verifier_text[index]
        if char in {"|", " "}:
            flush()
            continue
        current_chars.append(char)
        current_probs.append(float(char_probs[index]))
        current_times.append(float(timing_values[index]))
    flush()
    return words


def _meaningful_span_metrics(
    verifier_text: str,
    char_probs: np.ndarray,
    timings: Any,
    index_duration: float,
) -> tuple[float | None, list[dict[str, Any]]]:
    words = _meaningful_word_spans(verifier_text, char_probs, timings, index_duration)
    if not words:
        return None, []

    span_candidates: list[dict[str, Any]] = []
    max_words_per_span = 3
    for start in range(len(words)):
        total_chars = 0
        total_score = 0.0
        for end in range(start, min(len(words), start + max_words_per_span)):
            word = words[end]
            total_chars += int(word["char_count"])
            total_score += float(word["score"]) * int(word["char_count"])
            if total_chars < 2:
                continue
            span_candidates.append(
                {
                    "text": " ".join(str(words[index]["text"]) for index in range(start, end + 1)),
                    "score": total_score / total_chars,
                    "start_sec": float(words[start]["start_sec"]),
                    "end_sec": float(words[end]["end_sec"]),
                    "word_count": end - start + 1,
                }
            )

    if not span_candidates:
        return None, []

    span_candidates.sort(
        key=lambda span: (
            float(span["score"]),
            -int(span["word_count"]),
            float(span["start_sec"]),
        )
    )
    min_span_score = float(span_candidates[0]["score"])
    weak_cutoff = min(min_span_score + 0.08, 0.92)
    weak_spans: list[dict[str, Any]] = []
    for span in span_candidates:
        if float(span["score"]) > weak_cutoff:
            break
        weak_spans.append(
            {
                "start_sec": round(float(span["start_sec"]), 6),
                "end_sec": round(float(span["end_sec"]), 6),
                "text": str(span["text"]),
                "score": _round(float(span["score"])),
            }
        )
        if len(weak_spans) >= 3:
            break
    return _round(min_span_score), weak_spans


def split_verifier_words(text: str) -> list[str]:
    if "|" in text:
        return [word for word in text.split("|") if word]
    return [word for word in text.split() if word]


def word_char_spans(text: str) -> list[tuple[int, int]]:
    words = split_verifier_words(text)
    if not words:
        return []
    spans: list[tuple[int, int]] = []
    search_from = 0
    for word in words:
        start = text.find(word, search_from)
        if start < 0:
            raise ValueError(f"word {word!r} not found in verifier text")
        end = start + len(word)
        spans.append((start, end))
        search_from = end
    return spans


def find_sequence_insertion_blocks(expected_text: str, greedy_text: str) -> list[tuple[int, int, list[str]]]:
    expected_words = split_verifier_words(expected_text)
    greedy_words = split_verifier_words(greedy_text)
    if not greedy_words:
        return []
    matcher = SequenceMatcher(a=expected_words, b=greedy_words)
    blocks: list[tuple[int, int, list[str]]] = []
    for tag, _i1, _i2, j1, j2 in matcher.get_opcodes():
        if tag != "insert" or j2 <= j1:
            continue
        block_words = greedy_words[j1:j2]
        if any(len(word) >= GREEDY_INSERTION_MIN_WORD_LEN for word in block_words):
            blocks.append((j1, j2, block_words))
    return blocks


def _insertion_location(
    *,
    start_sec: float,
    end_sec: float,
    audio_duration_sec: float,
) -> str:
    edge_sec = max(0.0, float(audio_duration_sec) * AUDIO_EDGE_FRACTION)
    if start_sec <= edge_sec or end_sec >= max(0.0, float(audio_duration_sec) - edge_sec):
        return "edge"
    return "interior"


def _align_text_char_probs(
    text: str,
    probs: np.ndarray,
    bundle: CtcModelBundle,
    config: Any,
) -> tuple[Any, np.ndarray]:
    import ctc_segmentation

    ground_truth_mat, utt_begin_indices = ctc_segmentation.prepare_text(
        config,
        [text],
        char_list=bundle.char_list,
    )
    if ground_truth_mat.size == 0:
        raise ValueError("ctc_alignment_failed: empty ground truth matrix")
    timings, char_probs, _state_list = ctc_segmentation.ctc_segmentation(config, probs, ground_truth_mat)
    _segments = ctc_segmentation.determine_utterance_segments(
        config,
        utt_begin_indices,
        char_probs,
        timings,
        [text],
    )
    return timings, np.asarray(char_probs, dtype=np.float64)


def confirm_greedy_insertions(
    expected_text: str,
    greedy_text: str,
    *,
    probs: np.ndarray,
    bundle: CtcModelBundle,
    config: Any,
    index_duration: float,
    audio_duration_sec: float,
) -> tuple[list[dict[str, Any]], list[str]]:
    if not greedy_text.strip():
        return [], []

    insertion_blocks = find_sequence_insertion_blocks(expected_text, greedy_text)
    if not insertion_blocks:
        return [], []

    try:
        timings, char_probs = _align_text_char_probs(greedy_text, probs, bundle, config)
    except ValueError:
        return [], []

    spans = word_char_spans(greedy_text)
    confirmed: list[dict[str, Any]] = []
    confirmed_words: list[str] = []
    for word_start, word_end, block_words in insertion_blocks:
        if word_end > len(spans):
            continue
        char_start = spans[word_start][0]
        char_end = spans[word_end - 1][1]
        if char_end > char_probs.size:
            continue
        block_probs = char_probs[char_start:char_end]
        if block_probs.size == 0:
            continue
        confidence = float(np.min(block_probs))
        timing_slice = np.asarray(timings[char_start:char_end], dtype=np.float64)
        if timing_slice.size == 0:
            continue
        start_sec = round(float(np.min(timing_slice)) * float(index_duration), 3)
        end_sec = round(float(np.max(timing_slice)) * float(index_duration), 3)
        location = _insertion_location(
            start_sec=start_sec,
            end_sec=end_sec,
            audio_duration_sec=audio_duration_sec,
        )
        required_confidence = (
            INSERTION_EDGE_CONFIDENCE_THRESHOLD
            if location == "edge"
            else INSERTION_INTERIOR_CONFIDENCE_THRESHOLD
        )
        if confidence < required_confidence:
            continue
        text = "|".join(block_words)
        confirmed.append(
            {
                "text": text,
                "start_sec": start_sec,
                "end_sec": end_sec,
                "confidence": round(confidence, 4),
                "location": location,
            }
        )
        confirmed_words.append(text)

    return confirmed, confirmed_words


def composite_transcript_match_score(
    forced_alignment_score: float | None,
    *,
    confirmed_insertions: list[dict[str, Any]],
) -> tuple[float, float | None]:
    greedy_integrity_score = (
        GREEDY_INTEGRITY_FAIL_SCORE if confirmed_insertions else GREEDY_INTEGRITY_PASS_SCORE
    )
    if forced_alignment_score is None:
        return greedy_integrity_score, None
    return greedy_integrity_score, min(float(forced_alignment_score), greedy_integrity_score)


def greedy_decode_text_from_logits(logits: Any, bundle: CtcModelBundle) -> str:
    import torch

    predicted_ids = torch.argmax(logits, dim=-1)
    if predicted_ids.dim() == 1:
        predicted_ids = predicted_ids.unsqueeze(0)
    decoded = bundle.processor.batch_decode(predicted_ids)
    if not decoded:
        return ""
    return str(decoded[0]).strip()


def detect_greedy_insertions(expected_text: str, greedy_text: str) -> tuple[int, list[str]]:
    """Text-only helper for unit tests; production uses confirm_greedy_insertions()."""

    blocks = find_sequence_insertion_blocks(expected_text, greedy_text)
    insertion_words = ["|".join(block_words) for _j1, _j2, block_words in blocks]
    return (1 if insertion_words else 0), insertion_words


def select_transcript_gate_score(
    *,
    ctc_min_span_score: float | None,
    ctc_min_aligned_token_score: float | None,
    ctc_min_window_score: float | None,
    ctc_mean_score: float | None,
) -> float | None:
    if ctc_min_span_score is not None:
        return ctc_min_span_score
    if ctc_min_aligned_token_score is not None:
        return ctc_min_aligned_token_score
    if ctc_min_window_score is not None:
        return ctc_min_window_score
    return ctc_mean_score


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
    audio_duration_sec = float(audio.shape[0]) / float(sample_rate)

    greedy_raw = greedy_decode_text_from_logits(logits, bundle)
    greedy_normalized, _ = normalize_verifier_text(greedy_raw, set(bundle.char_list))
    confirmed_insertions, confirmed_insertion_words = confirm_greedy_insertions(
        verifier_text,
        greedy_normalized,
        probs=probs,
        bundle=bundle,
        config=config,
        index_duration=float(config.index_duration),
        audio_duration_sec=audio_duration_sec,
    )
    ctc_greedy_insertions = 1 if confirmed_insertions else 0
    untranscribed_speech_detected = ctc_greedy_insertions > 0

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
    ctc_min_span_score, weak_spans = _meaningful_span_metrics(
        verifier_text,
        char_prob_array,
        timings,
        float(config.index_duration),
    )
    meaningful_mask = np.asarray([char not in {"|", " "} for char in verifier_text[: char_prob_array.size]], dtype=bool)
    meaningful_probs = char_prob_array[: meaningful_mask.size][meaningful_mask]
    unaligned_token_count = int(np.sum(meaningful_probs < WEAK_CHAR_THRESHOLD))
    weak_span_count = len(weak_spans)

    segment_conf = None
    if segments:
        segment_conf = float(segments[0][2])
    raw_score = select_transcript_gate_score(
        ctc_min_span_score=ctc_min_span_score,
        ctc_min_aligned_token_score=ctc_min_aligned_token_score,
        ctc_min_window_score=ctc_min_window_score,
        ctc_mean_score=ctc_mean_score,
    )
    forced_alignment_score = _round_score_0_100(raw_score)
    greedy_integrity_score, transcript_match_score = composite_transcript_match_score(
        forced_alignment_score,
        confirmed_insertions=confirmed_insertions,
    )
    coverage = audio_coverage_metrics(
        audio_duration_sec=audio_duration_sec,
        segments=segments,
        timings=timings,
        index_duration=float(config.index_duration),
    )
    reason_codes: list[str] = []
    if transcript_match_score is not None and transcript_match_score < WORKER_TRANSCRIPT_THRESHOLD_HINT:
        reason_codes.append("low_transcript_match")
    if weak_spans and transcript_match_score is not None and transcript_match_score < WORKER_TRANSCRIPT_THRESHOLD_HINT:
        reason_codes.append("weak_transcript_span")
    if untranscribed_speech_detected:
        reason_codes.append(UNTRANSCRIBED_SPEECH_REASON)
        reason_codes.append(CONFIRMED_GREEDY_INSERTION_REASON)
    bucket = score_bucket(transcript_match_score)
    bucket_hint = score_bucket_hint(transcript_match_score)

    return {
        "transcript_score_method": TRANSCRIPT_SCORE_METHOD,
        "forced_alignment_score": forced_alignment_score,
        "greedy_integrity_score": greedy_integrity_score,
        "confirmed_insertions": confirmed_insertions,
        "untranscribed_speech_detected": untranscribed_speech_detected,
        "ctc_greedy_insertions": ctc_greedy_insertions,
        "ctc_greedy_insertion_words": confirmed_insertion_words,
        "ctc_greedy_decode_text": greedy_normalized or None,
        "ctc_mean_score": _round(ctc_mean_score),
        "ctc_min_span_score": _round(ctc_min_span_score),
        "ctc_min_window_score": _round(ctc_min_window_score),
        "ctc_min_token_score": _round(ctc_min_token_score),
        "ctc_min_aligned_token_score": _round(ctc_min_aligned_token_score),
        "unaligned_token_count": unaligned_token_count,
        "weak_span_count": weak_span_count,
        "weak_spans": weak_spans,
        "segment_confidence": _round(segment_conf),
        "transcript_match_score": transcript_match_score,
        "bucket": bucket,
        "bucket_hint": bucket_hint,
        "reason_codes": reason_codes,
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
        "transcript_score_method": TRANSCRIPT_SCORE_METHOD,
        "ctc_mean_score": None,
        "ctc_min_span_score": None,
        "ctc_min_window_score": None,
        "ctc_min_token_score": None,
        "unaligned_token_count": None,
        "weak_span_count": None,
        "weak_spans": [],
        "segment_confidence": None,
        "forced_alignment_score": None,
        "greedy_integrity_score": None,
        "confirmed_insertions": [],
        "untranscribed_speech_detected": False,
        "transcript_match_score": None,
        "ctc_greedy_insertions": None,
        "ctc_greedy_insertion_words": [],
        "ctc_greedy_decode_text": None,
        "bucket_hint": "unscored",
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
    local_files_only: bool = True,
) -> dict[str, Any]:
    del batch_size  # reserved for later batching; keep CLI stable

    manifest_path = resolve_under_root(run_root, "artifacts/candidate_review_manifest.json")
    candidates = read_json_value(manifest_path)
    if not isinstance(candidates, list):
        raise ValueError("candidate_review_manifest.json must contain a list")
    if max_clips is not None:
        candidates = candidates[: max(0, max_clips)]

    out_dir.mkdir(parents=True, exist_ok=True)
    bundle = load_ctc_model(model_name, device, local_files_only=local_files_only)

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
                "bucket_hint": row.get("bucket_hint"),
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
                "bucket_hint",
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


def run_transcript_qc(
    run_root: Path,
    config: dict[str, Any],
    *,
    model_name: str | None = None,
    device: str = "auto",
) -> dict[str, Any]:
    model = str(model_name or config.get("transcript_qc_model") or DEFAULT_MODEL)
    local_files_only = bool(
        config.get(
            "transcript_qc_local_files_only",
            config.get("asr_local_files_only", True),
        )
    )
    bundle = load_ctc_model(model, device, local_files_only=local_files_only)
    manifest_path = resolve_under_root(run_root, "artifacts/candidate_review_manifest.json")
    candidates = read_json_value(manifest_path)
    if not isinstance(candidates, list):
        raise ValueError("candidate_review_manifest.json must contain a list")

    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        rows.append(build_clip_row(candidate, run_root, bundle))

    scored_rows = [row for row in rows if row.get("transcript_match_score") is not None]
    scores = [
        float(row["transcript_match_score"])
        for row in scored_rows
        if isinstance(row.get("transcript_match_score"), (int, float))
    ]
    reason_counts: dict[str, int] = {}
    for row in rows:
        for reason in row.get("reason_codes") or []:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
    bucket_hint_counts = {
        "pass": sum(row.get("bucket_hint") == "pass" for row in rows),
        "review": sum(row.get("bucket_hint") == "review" for row in rows),
        "fail": sum(row.get("bucket_hint") == "fail" for row in rows),
        "unscored": sum(row.get("bucket_hint") == "unscored" for row in rows),
    }

    artifact_payload = {
        "schema_version": TRANSCRIPT_QC_SCHEMA_VERSION,
        "stage": "transcript_qc",
        "model": model,
        "score_method": TRANSCRIPT_SCORE_METHOD,
        "clips": rows,
    }
    artifact_path = resolve_under_root(run_root, "artifacts/transcript_qc.json")
    write_json(artifact_path, artifact_payload)
    summary = {
        "schema_version": TRANSCRIPT_QC_SCHEMA_VERSION,
        "stage": "transcript_qc",
        "model": model,
        "score_method": TRANSCRIPT_SCORE_METHOD,
        "clip_count": len(rows),
        "scored_count": len(scored_rows),
        "failed_count": len(rows) - len(scored_rows),
        "score_p50": _percentile(scores, 50),
        "score_p10": _percentile(scores, 10),
        "score_p90": _percentile(scores, 90),
        "bucket_hint_counts": bucket_hint_counts,
        "reason_counts": dict(sorted(reason_counts.items())),
        "input_artifact_hashes": {
            "candidate_review_manifest_json": sha256_file(manifest_path),
        },
    }
    summary_path = resolve_under_root(run_root, "artifacts/transcript_qc_summary.json")
    write_json(summary_path, summary)
    summary["output_hashes"] = {
        "transcript_qc_json": sha256_file(artifact_path),
        "transcript_qc_summary_json": sha256_file(summary_path),
    }
    write_json(summary_path, summary)
    log(f"wrote transcript QC artifacts for {len(rows)} clips")
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
    parser.add_argument(
        "--allow-download",
        action="store_true",
        help="Allow Hugging Face downloads instead of requiring the model to already exist in the local cache.",
    )
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
        local_files_only=not args.allow_download,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
