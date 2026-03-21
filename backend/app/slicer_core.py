from __future__ import annotations

import math
from collections.abc import Iterable, Iterator, Mapping
from typing import Any

import numpy as np


TERMINAL_PUNCTUATION = (".", "?", "!")
SOFT_PUNCTUATION = (",", "-", ";")
TRIMMABLE_TRAILING_PUNCTUATION = "\"'”’)]}"


def find_energy_trough(audio_samples: np.ndarray, sample_rate: int) -> float:
    """
    Return the relative timestamp of the quietest energy valley inside a gap.

    RMS is computed over a 10 ms sliding window. If the minimum spans a flat
    silence plateau, the midpoint of the longest minimum-energy run is used so
    silent gaps cut in the acoustic middle instead of at the first silent frame.
    """

    if sample_rate <= 0:
        raise ValueError("sample_rate must be positive")

    samples = np.asarray(audio_samples, dtype=np.float64).reshape(-1)
    if samples.size == 0:
        return 0.0

    window_size = max(int(round(sample_rate * 0.01)), 1)
    if samples.size <= window_size:
        return samples.size / (2.0 * sample_rate)

    squared = samples * samples
    window = np.ones(window_size, dtype=np.float64)
    rms = np.sqrt(np.convolve(squared, window, mode="valid") / window_size)

    minimum = float(rms.min())
    minimum_indices = np.flatnonzero(np.isclose(rms, minimum, rtol=1e-6, atol=1e-12))
    trough_index = _midpoint_of_longest_run(minimum_indices)
    return (trough_index + (window_size / 2.0)) / sample_rate


def pack_aligned_words(
    alignment_units: Iterable[Mapping[str, Any]],
    audio_samples: np.ndarray,
    sample_rate: int,
) -> Iterator[dict[str, float | str]]:
    """
    Deterministically pack aligned words into trainable slices.

    The cut policy is:
    - >= 6s and terminal punctuation with a > 0.4s following gap
    - >= 8s and soft punctuation with a > 0.3s following gap
    - >= 12s and any > 0.2s following gap
    - >= 15s hard cut at the current word end
    """

    if sample_rate <= 0:
        raise ValueError("sample_rate must be positive")

    samples = np.asarray(audio_samples, dtype=np.float64).reshape(-1)
    words = _normalize_alignment_units(alignment_units)
    if not words:
        return

    slice_start_index = 0
    slice_start_seconds = words[0]["start"]

    for current_index, current_word in enumerate(words):
        next_word = words[current_index + 1] if current_index + 1 < len(words) else None
        current_duration = current_word["end"] - slice_start_seconds
        cut_end_seconds: float | None = None

        if current_duration >= 15.0:
            cut_end_seconds = current_word["end"]
        elif next_word is not None:
            gap_to_next = next_word["start"] - current_word["end"]

            if current_duration >= 12.0 and gap_to_next > 0.2:
                cut_end_seconds = _cut_at_gap_trough(
                    samples,
                    sample_rate,
                    current_word["end"],
                    next_word["start"],
                )
            elif (
                current_duration >= 8.0
                and _ends_with_punctuation(current_word["word"], SOFT_PUNCTUATION)
                and gap_to_next > 0.3
            ):
                cut_end_seconds = _cut_at_gap_trough(
                    samples,
                    sample_rate,
                    current_word["end"],
                    next_word["start"],
                )
            elif (
                current_duration >= 6.0
                and _ends_with_punctuation(current_word["word"], TERMINAL_PUNCTUATION)
                and gap_to_next > 0.4
            ):
                cut_end_seconds = _cut_at_gap_trough(
                    samples,
                    sample_rate,
                    current_word["end"],
                    next_word["start"],
                )

        if cut_end_seconds is None:
            continue

        yield _packed_slice(
            start_seconds=slice_start_seconds,
            end_seconds=cut_end_seconds,
            words=words[slice_start_index : current_index + 1],
        )
        slice_start_index = current_index + 1
        slice_start_seconds = cut_end_seconds

    if slice_start_index < len(words):
        yield _packed_slice(
            start_seconds=slice_start_seconds,
            end_seconds=words[-1]["end"],
            words=words[slice_start_index:],
        )


def _normalize_alignment_units(
    alignment_units: Iterable[Mapping[str, Any]],
) -> list[dict[str, float | str]]:
    normalized: list[dict[str, float | str]] = []
    previous_end = -math.inf

    for unit in alignment_units:
        word = str(unit["word"])
        start = float(unit["start"])
        end = float(unit["end"])

        if end <= start:
            raise ValueError("Alignment units must have end > start")
        if start < previous_end:
            raise ValueError("Alignment units must be sorted and non-overlapping")

        normalized.append({"word": word, "start": start, "end": end})
        previous_end = end

    return normalized


def _packed_slice(
    *,
    start_seconds: float,
    end_seconds: float,
    words: list[dict[str, float | str]],
) -> dict[str, float | str]:
    return {
        "start_s": round(start_seconds, 6),
        "end_s": round(end_seconds, 6),
        "transcript_text": " ".join(str(word["word"]) for word in words).strip(),
    }


def _cut_at_gap_trough(
    audio_samples: np.ndarray,
    sample_rate: int,
    gap_start_seconds: float,
    gap_end_seconds: float,
) -> float:
    if gap_end_seconds <= gap_start_seconds:
        return gap_end_seconds

    start_index = max(int(math.floor(gap_start_seconds * sample_rate)), 0)
    end_index = min(int(math.ceil(gap_end_seconds * sample_rate)), audio_samples.size)
    if end_index <= start_index:
        return (gap_start_seconds + gap_end_seconds) / 2.0

    return gap_start_seconds + find_energy_trough(audio_samples[start_index:end_index], sample_rate)


def _ends_with_punctuation(word: str, punctuation: tuple[str, ...]) -> bool:
    trimmed = word.rstrip(TRIMMABLE_TRAILING_PUNCTUATION)
    return trimmed.endswith(punctuation)


def _midpoint_of_longest_run(indices: np.ndarray) -> int:
    if indices.size == 0:
        return 0
    if indices.size == 1:
        return int(indices[0])

    best_start = int(indices[0])
    best_end = int(indices[0])
    run_start = int(indices[0])
    run_end = int(indices[0])

    for raw_index in indices[1:]:
        index = int(raw_index)
        if index == run_end + 1:
            run_end = index
            continue
        if (run_end - run_start) > (best_end - best_start):
            best_start = run_start
            best_end = run_end
        run_start = index
        run_end = index

    if (run_end - run_start) > (best_end - best_start):
        best_start = run_start
        best_end = run_end

    return (best_start + best_end) // 2


def mock_forced_align(text: str, duration_s: float) -> list[dict[str, float | str]]:
    """
    Temporary stand-in for real forced alignment.

    Words are evenly distributed across the provided duration.
    """

    normalized_text = text.strip()
    if not normalized_text:
        return []
    if duration_s <= 0:
        raise ValueError("duration_s must be positive")

    words = normalized_text.split()
    segment_duration = duration_s / len(words)
    aligned_words: list[dict[str, float | str]] = []

    for index, word in enumerate(words):
        start = index * segment_duration
        end = duration_s if index == len(words) - 1 else (index + 1) * segment_duration
        aligned_words.append(
            {
                "word": word,
                "start": round(start, 6),
                "end": round(end, 6),
            }
        )

    return aligned_words


__all__ = ["find_energy_trough", "mock_forced_align", "pack_aligned_words"]
