from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np


logger = logging.getLogger(__name__)


@dataclass
class SlicerConfig:
    target_duration: float = 7.0
    min_duration: float = 2.0
    max_duration: float = 15.0
    soft_max: float = 10.0
    min_gap_for_boundary: float = 0.12
    preferred_gap_for_boundary: float = 0.24
    snap_collar_ms: float = 150.0
    rms_frame_ms: float = 10.0
    padding_ms: float = 150.0
    leading_word_guard_ms: float = 35.0
    trailing_word_guard_ms: float = 45.0
    boundary_context_ms: float = 80.0
    boundary_acoustic_weight: float = 0.65
    min_boundary_acoustic_score: float = 0.35
    edge_window_ms: float = 25.0
    edge_energy_ratio_threshold: float = 0.65
    max_leading_silence_ms: float = 650.0
    max_trailing_silence_ms: float = 650.0
    min_speech_ratio: float = 0.72
    min_pause_for_comma_ms: float = 300.0
    max_pause_for_terminal_punct_ms: float = 50.0
    breath_min_duration_ms: float = 80.0
    breath_max_duration_ms: float = 400.0
    breath_energy_floor: float = 0.001
    breath_energy_ceiling: float = 0.05
    flag_long_threshold: float = 12.0


@dataclass
class AlignedWord:
    word: str
    start: float
    end: float
    confidence: float = 1.0

    @property
    def trailing_punct(self) -> str:
        punct = ""
        for character in reversed(self.word):
            if character in ".,?!;:—-'\"":
                punct = character + punct
            else:
                break
        return punct


@dataclass
class BoundaryCandidate:
    word_index: int
    timestamp: float
    gap_duration: float
    boundary_type: str
    strength: float
    safe_start: float
    safe_end: float
    acoustic_score: float = 0.0
    valley_energy: float = 0.0


@dataclass
class SliceSpec:
    start_word_index: int
    end_word_index: int
    end_boundary: BoundaryCandidate | None = None
    raw_start: float = 0.0
    raw_end: float = 0.0
    snapped_start: float = 0.0
    snapped_end: float = 0.0
    training_start: float = 0.0
    training_end: float = 0.0
    padded_start: float = 0.0
    padded_end: float = 0.0
    transcript: str = ""
    transcript_original: str = ""
    word_count: int = 0
    duration: float = 0.0
    avg_confidence: float = 0.0
    breath_at_start: bool = False
    breath_at_end: bool = False
    is_flagged: bool = False
    flag_reason: str = ""
    flag_reasons: list[str] = field(default_factory=list)
    forced_cut: bool = False
    edge_start_energy: float = 0.0
    edge_end_energy: float = 0.0


def plan_slices(
    alignment_data: list[dict[str, Any]],
    audio_samples: np.ndarray,
    sample_rate: int,
    config: SlicerConfig | None = None,
) -> dict[str, Any]:
    if config is None:
        config = SlicerConfig()
    if sample_rate <= 0:
        raise ValueError("sample_rate must be positive")

    audio = _normalize_audio(audio_samples)
    words = parse_alignment(alignment_data)
    if not words:
        raise ValueError("Alignment data produced zero valid words.")

    audio_duration = len(audio) / sample_rate
    word_span = words[-1].end - words[0].start
    alignment_coverage = word_span / audio_duration * 100 if audio_duration > 0 else 0.0

    rms, rms_times = _compute_rms_frames(audio, sample_rate, config.rms_frame_ms)
    candidates = find_candidate_boundaries(words, config, rms=rms, rms_times=rms_times)
    specs = greedy_group(words, candidates, config)
    refined_specs = apply_acoustic_refinement(specs, audio, sample_rate, config, rms=rms, rms_times=rms_times)
    slices = build_slice_entries(refined_specs, words, sample_rate)
    stats = build_slice_stats(slices, audio_duration, word_span, alignment_coverage)

    return {
        "slices": slices,
        "stats": stats,
        "candidate_boundaries": len(candidates),
    }


def parse_alignment(raw: list[dict[str, Any]]) -> list[AlignedWord]:
    words: list[AlignedWord] = []
    previous_end = -1.0

    for index, item in enumerate(raw):
        word = AlignedWord(
            word=str(item["word"]),
            start=float(item["start"]),
            end=float(item["end"]),
            confidence=float(item.get("confidence", 1.0)),
        )

        if word.start < previous_end - 0.001:
            logger.warning(
                "Word [%s] %r starts at %.4fs but prev ended at %.4fs; clamping start forward.",
                index,
                word.word,
                word.start,
                previous_end,
            )
            word.start = previous_end

        if word.end <= word.start:
            logger.warning("Word [%s] %r has zero/negative duration; skipping.", index, word.word)
            continue

        words.append(word)
        previous_end = word.end

    return words


def find_candidate_boundaries(
    words: list[AlignedWord],
    config: SlicerConfig,
    *,
    rms: np.ndarray | None = None,
    rms_times: np.ndarray | None = None,
) -> list[BoundaryCandidate]:
    candidates: list[BoundaryCandidate] = []

    for index, word in enumerate(words[:-1]):
        next_word = words[index + 1]
        safe_start, safe_end = _safe_gap_window(word, next_word, config)
        safe_gap = max(0.0, safe_end - safe_start)
        if safe_gap < config.min_gap_for_boundary:
            continue

        timestamp = (safe_start + safe_end) / 2.0
        acoustic_score = 0.0
        valley_energy = 0.0
        if rms is not None and rms_times is not None and len(rms_times) > 0:
            timestamp = _snap_boundary_in_gap(safe_start, safe_end, config, rms, rms_times)
            acoustic_score, valley_energy = _boundary_acoustic_score(
                timestamp,
                word,
                next_word,
                rms,
                rms_times,
                config,
            )
            if acoustic_score < config.min_boundary_acoustic_score:
                continue

        candidates.append(
            BoundaryCandidate(
                word_index=index,
                timestamp=timestamp,
                gap_duration=safe_gap,
                boundary_type="safe_gap",
                strength=_boundary_strength(safe_gap, acoustic_score, config),
                safe_start=safe_start,
                safe_end=safe_end,
                acoustic_score=acoustic_score,
                valley_energy=valley_energy,
            )
        )

    return candidates


def greedy_group(
    words: list[AlignedWord],
    candidates: list[BoundaryCandidate],
    config: SlicerConfig,
) -> list[SliceSpec]:
    boundary_map = {candidate.word_index: candidate for candidate in candidates}
    specs: list[SliceSpec] = []
    start_index = 0

    while start_index < len(words):
        end_index, boundary, forced = _find_slice_end(words, boundary_map, start_index, config)
        specs.append(_build_spec(words, start_index, end_index, boundary, forced, config))
        start_index = end_index + 1

    return _merge_short_clips(specs, words, config)


def apply_acoustic_refinement(
    specs: list[SliceSpec],
    audio: np.ndarray,
    sample_rate: int,
    config: SlicerConfig,
    *,
    rms: np.ndarray | None = None,
    rms_times: np.ndarray | None = None,
) -> list[SliceSpec]:
    if not specs:
        return specs

    audio_duration = len(audio) / sample_rate
    padding_seconds = config.padding_ms / 1000.0
    if rms is None or rms_times is None:
        rms, rms_times = _compute_rms_frames(audio, sample_rate, config.rms_frame_ms)

    for spec in specs:
        spec.breath_at_start = False
        spec.breath_at_end = False
        spec.snapped_start = spec.raw_start
        spec.snapped_end = spec.raw_end
        spec.training_start = spec.raw_start
        spec.training_end = spec.raw_end
        spec.edge_start_energy = 0.0
        spec.edge_end_energy = 0.0

    specs[0].snapped_start = specs[0].raw_start
    specs[-1].snapped_end = specs[-1].raw_end

    for left, right in zip(specs, specs[1:]):
        boundary = left.end_boundary
        if boundary is not None:
            gap_start = boundary.safe_start
            gap_end = boundary.safe_end
        else:
            gap_start, gap_end = _safe_gap_window(
                AlignedWord(word="", start=left.raw_start, end=left.raw_end),
                AlignedWord(word="", start=right.raw_start, end=right.raw_end),
                config,
            )
        breath_present = _detect_breath(audio, sample_rate, left.raw_end, max(0.0, right.raw_start - left.raw_end), config)
        if breath_present:
            shared_boundary = _choose_breath_boundary(gap_start, gap_end, rms, rms_times, config)
        else:
            shared_boundary = _snap_boundary_in_gap(gap_start, gap_end, config, rms, rms_times)

        left.snapped_end = shared_boundary
        right.snapped_start = shared_boundary
        left.breath_at_end = breath_present
        right.breath_at_start = breath_present

    for spec in specs:
        spec.training_start = spec.snapped_start
        spec.training_end = spec.snapped_end
        start_pad = padding_seconds * (1.5 if spec.breath_at_start else 1.0)
        end_pad = padding_seconds * (1.5 if spec.breath_at_end else 1.0)
        spec.padded_start = max(0.0, spec.snapped_start - start_pad)
        spec.padded_end = min(audio_duration, spec.snapped_end + end_pad)
        spec.duration = max(0.0, spec.training_end - spec.training_start)
        spec.edge_start_energy = _window_mean_rms(
            spec.training_start,
            min(spec.training_start + config.edge_window_ms / 1000.0, audio_duration),
            rms,
            rms_times,
        )
        spec.edge_end_energy = _window_mean_rms(
            max(0.0, spec.training_end - config.edge_window_ms / 1000.0),
            spec.training_end,
            rms,
            rms_times,
        )
        clip_mean_rms = _window_mean_rms(spec.training_start, spec.training_end, rms, rms_times)
        if clip_mean_rms > 0:
            if spec.edge_start_energy / clip_mean_rms > config.edge_energy_ratio_threshold:
                _add_flag(spec, f"high_start_edge_energy_{spec.edge_start_energy / clip_mean_rms:.2f}")
            if spec.edge_end_energy / clip_mean_rms > config.edge_energy_ratio_threshold:
                _add_flag(spec, f"high_end_edge_energy_{spec.edge_end_energy / clip_mean_rms:.2f}")

        leading_silence = max(0.0, spec.raw_start - spec.training_start)
        trailing_silence = max(0.0, spec.training_end - spec.raw_end)
        if leading_silence > config.max_leading_silence_ms / 1000.0:
            _add_flag(spec, f"leading_silence_{leading_silence:.2f}s")
        if trailing_silence > config.max_trailing_silence_ms / 1000.0:
            _add_flag(spec, f"trailing_silence_{trailing_silence:.2f}s")
        if spec.duration > 0:
            speech_ratio = max(0.0, min(1.0, (spec.raw_end - spec.raw_start) / spec.duration))
            if speech_ratio < config.min_speech_ratio:
                _add_flag(spec, f"low_speech_ratio_{speech_ratio:.2f}")

    return specs


def build_slice_entries(
    specs: list[SliceSpec],
    words: list[AlignedWord],
    sample_rate: int,
) -> list[dict[str, Any]]:
    slices: list[dict[str, Any]] = []

    for index, spec in enumerate(specs):
        clip_words = words[spec.start_word_index : spec.end_word_index + 1]
        clip_start = spec.padded_start
        relative_words = [
            {
                "word": word.word,
                "start": round(word.start - clip_start, 4),
                "end": round(word.end - clip_start, 4),
                "confidence": round(word.confidence, 4),
            }
            for word in clip_words
        ]

        overlap_prev = 0.0
        overlap_next = 0.0
        if index > 0:
            overlap_prev = max(0.0, specs[index - 1].padded_end - spec.padded_start)
        if index + 1 < len(specs):
            overlap_next = max(0.0, spec.padded_end - specs[index + 1].padded_start)

        slices.append(
            {
                "slice_index": index,
                "transcript": spec.transcript,
                "transcript_original": spec.transcript_original,
                "duration": round(spec.duration, 4),
                "training_duration": round(max(0.0, spec.training_end - spec.training_start), 4),
                "review_duration": round(max(0.0, spec.padded_end - spec.padded_start), 4),
                "sample_rate": sample_rate,
                "boundary_type": spec.end_boundary.boundary_type if spec.end_boundary else "end_of_recording",
                "boundary_gap_s": round(spec.end_boundary.gap_duration, 4) if spec.end_boundary else 0.0,
                "raw_start": round(spec.raw_start, 4),
                "raw_end": round(spec.raw_end, 4),
                "snapped_start": round(spec.snapped_start, 4),
                "snapped_end": round(spec.snapped_end, 4),
                "training_start": round(spec.training_start, 4),
                "training_end": round(spec.training_end, 4),
                "padded_start": round(spec.padded_start, 4),
                "padded_end": round(spec.padded_end, 4),
                "overlap_with_previous_s": round(overlap_prev, 4),
                "overlap_with_next_s": round(overlap_next, 4),
                "word_count": spec.word_count,
                "avg_alignment_confidence": round(spec.avg_confidence, 4),
                "forced_cut": spec.forced_cut,
                "is_flagged": spec.is_flagged,
                "flag_reason": spec.flag_reason,
                "flag_reasons": list(spec.flag_reasons),
                "breath_at_start": spec.breath_at_start,
                "breath_at_end": spec.breath_at_end,
                "edge_start_energy": round(spec.edge_start_energy, 6),
                "edge_end_energy": round(spec.edge_end_energy, 6),
                "words": relative_words,
            }
        )

    return slices


def build_slice_stats(
    slices: list[dict[str, Any]],
    audio_duration: float,
    word_span: float,
    alignment_coverage: float,
) -> dict[str, Any]:
    durations = [float(entry["training_duration"]) for entry in slices]
    intervals = [(float(entry["training_start"]), float(entry["training_end"])) for entry in slices]
    review_durations = [float(entry["review_duration"]) for entry in slices]
    review_intervals = [(float(entry["padded_start"]), float(entry["padded_end"])) for entry in slices]
    exported_audio = sum(durations)
    unique_covered = _compute_interval_union(intervals)
    overlap_audio = max(0.0, exported_audio - unique_covered)
    review_overlap_audio = max(0.0, sum(review_durations) - _compute_interval_union(review_intervals))

    def pct_in_range(low: float, high: float) -> float:
        if not durations:
            return 0.0
        return round(sum(1 for duration in durations if low <= duration < high) / len(durations) * 100, 1)

    return {
        "source_duration_s": round(audio_duration, 2),
        "alignment_span_s": round(word_span, 2),
        "alignment_coverage_pct": round(alignment_coverage, 1),
        "total_clips": len(slices),
        "total_clip_s": round(exported_audio, 2),
        "unique_covered_audio_s": round(unique_covered, 2),
        "overlap_audio_s": round(overlap_audio, 2),
        "review_overlap_audio_s": round(review_overlap_audio, 2),
        "coverage_pct": round((unique_covered / audio_duration * 100), 1) if audio_duration > 0 else 0.0,
        "exported_vs_source_pct": round((exported_audio / audio_duration * 100), 1) if audio_duration > 0 else 0.0,
        "avg_duration_s": round(sum(durations) / len(durations), 2) if durations else 0.0,
        "min_duration_s": round(min(durations), 2) if durations else 0.0,
        "max_duration_s": round(max(durations), 2) if durations else 0.0,
        "pct_under_4s": pct_in_range(0, 4),
        "pct_4_to_6s": pct_in_range(4, 6),
        "pct_6_to_8s": pct_in_range(6, 8),
        "pct_8_to_12s": pct_in_range(8, 12),
        "pct_over_12s": pct_in_range(12, float("inf")),
        "flagged_clips": sum(1 for entry in slices if entry["is_flagged"]),
        "forced_cuts": sum(1 for entry in slices if entry["forced_cut"]),
        "breath_at_end": sum(1 for entry in slices if entry["breath_at_end"]),
        "breath_at_start": sum(1 for entry in slices if entry["breath_at_start"]),
        "avg_confidence": round(
            sum(float(entry["avg_alignment_confidence"]) for entry in slices) / len(slices),
            3,
        ) if slices else 0.0,
    }


def _normalize_audio(audio_samples: np.ndarray) -> np.ndarray:
    audio = np.asarray(audio_samples, dtype=np.float64)
    if audio.ndim == 2:
        audio = audio.mean(axis=1)
    return audio.reshape(-1)


def _boundary_strength(gap: float, acoustic_score: float, config: SlicerConfig) -> float:
    gap_score = min(gap / config.preferred_gap_for_boundary, 1.0)
    acoustic_weight = min(max(config.boundary_acoustic_weight, 0.0), 1.0)
    return ((1.0 - acoustic_weight) * gap_score) + (acoustic_weight * acoustic_score)


def _find_slice_end(
    words: list[AlignedWord],
    boundary_map: dict[int, BoundaryCandidate],
    start_index: int,
    config: SlicerConfig,
) -> tuple[int, BoundaryCandidate | None, bool]:
    start_time = words[start_index].start
    best_under_max: BoundaryCandidate | None = None
    best_under_max_index = -1
    best_under_max_score = float("-inf")
    first_over_max: tuple[int, BoundaryCandidate] | None = None

    for index in range(start_index, len(words) - 1):
        boundary = boundary_map.get(index)
        if boundary is None:
            continue

        duration = words[index].end - start_time
        if duration < config.min_duration:
            continue

        if duration <= config.max_duration:
            score = _boundary_selection_score(duration, boundary, config)
            if score > best_under_max_score or (
                score == best_under_max_score
                and _is_better_boundary(boundary, best_under_max)
            ):
                best_under_max = boundary
                best_under_max_index = index
                best_under_max_score = score
            continue

        if first_over_max is None:
            first_over_max = (index, boundary)
            break

    if best_under_max is not None:
        return best_under_max_index, best_under_max, False
    if first_over_max is not None:
        return first_over_max[0], first_over_max[1], False
    return len(words) - 1, None, False


def _build_spec(
    words: list[AlignedWord],
    start_index: int,
    end_index: int,
    boundary: BoundaryCandidate | None,
    forced: bool,
    config: SlicerConfig,
) -> SliceSpec:
    slice_words = words[start_index : end_index + 1]
    raw_start = slice_words[0].start
    raw_end = slice_words[-1].end
    duration = raw_end - raw_start
    original_transcript = " ".join(word.word for word in slice_words).strip()
    transcript = _render_pause_faithful_transcript(slice_words, config)
    avg_confidence = sum(word.confidence for word in slice_words) / len(slice_words)

    spec = SliceSpec(
        start_word_index=start_index,
        end_word_index=end_index,
        end_boundary=boundary,
        raw_start=raw_start,
        raw_end=raw_end,
        transcript=transcript,
        transcript_original=original_transcript,
        word_count=len(slice_words),
        duration=duration,
        avg_confidence=avg_confidence,
        forced_cut=forced,
    )

    if forced:
        _add_flag(spec, "forced_cut_no_punctuation")
    if duration > config.flag_long_threshold:
        _add_flag(spec, f"long_{duration:.1f}s")
    if boundary is None and duration > config.max_duration:
        _add_flag(spec, f"no_safe_boundary_{duration:.1f}s")
    if avg_confidence < 0.5:
        _add_flag(spec, f"low_confidence_{avg_confidence:.2f}")
    return spec


def _merge_short_clips(
    specs: list[SliceSpec],
    words: list[AlignedWord],
    config: SlicerConfig,
) -> list[SliceSpec]:
    merged = list(specs)
    index = 0

    while index < len(merged):
        if merged[index].duration >= config.min_duration:
            index += 1
            continue
        if len(merged) == 1:
            break
        if index == 0:
            merged[1] = _merge_two(merged[0], merged[1], words, config)
            merged.pop(0)
            continue
        if index == len(merged) - 1:
            merged[-2] = _merge_two(merged[-2], merged[-1], words, config)
            merged.pop()
            index -= 1
            continue

        previous_combined = merged[index - 1].duration + merged[index].duration
        next_combined = merged[index + 1].duration + merged[index].duration
        previous_distance = abs(previous_combined - config.target_duration)
        next_distance = abs(next_combined - config.target_duration)

        if previous_distance <= next_distance:
            merged[index - 1] = _merge_two(merged[index - 1], merged[index], words, config)
            merged.pop(index)
            index = max(0, index - 1)
        else:
            merged[index + 1] = _merge_two(merged[index], merged[index + 1], words, config)
            merged.pop(index)

    return merged


def _merge_two(
    left: SliceSpec,
    right: SliceSpec,
    words: list[AlignedWord],
    config: SlicerConfig,
) -> SliceSpec:
    merged_words = words[left.start_word_index : right.end_word_index + 1]
    duration = merged_words[-1].end - merged_words[0].start
    avg_confidence = sum(word.confidence for word in merged_words) / len(merged_words)
    spec = SliceSpec(
        start_word_index=left.start_word_index,
        end_word_index=right.end_word_index,
        end_boundary=right.end_boundary,
        raw_start=merged_words[0].start,
        raw_end=merged_words[-1].end,
        transcript=_render_pause_faithful_transcript(merged_words, config),
        transcript_original=" ".join(word.word for word in merged_words).strip(),
        word_count=len(merged_words),
        duration=duration,
        avg_confidence=avg_confidence,
        forced_cut=left.forced_cut or right.forced_cut,
    )

    _copy_flags(spec, left, right)
    if duration > config.flag_long_threshold:
        _add_flag(spec, f"merged_long_{duration:.1f}s")
    if avg_confidence < 0.5:
        _add_flag(spec, f"low_confidence_{avg_confidence:.2f}")
    if spec.forced_cut:
        _add_flag(spec, "contains_forced_cut")
    return spec


def _is_better_boundary(candidate: BoundaryCandidate, current: BoundaryCandidate | None) -> bool:
    if current is None:
        return True
    if candidate.strength > current.strength:
        return True
    if candidate.strength < current.strength:
        return False
    return candidate.gap_duration > current.gap_duration


def _safe_gap_window(
    word: AlignedWord,
    next_word: AlignedWord,
    config: SlicerConfig,
) -> tuple[float, float]:
    left_guard = config.trailing_word_guard_ms / 1000.0
    right_guard = config.leading_word_guard_ms / 1000.0
    return word.end + left_guard, next_word.start - right_guard


def _render_pause_faithful_transcript(words: list[AlignedWord], config: SlicerConfig) -> str:
    if not words:
        return ""

    rendered: list[str] = []
    comma_pause = config.min_pause_for_comma_ms / 1000.0
    tiny_pause = config.max_pause_for_terminal_punct_ms / 1000.0

    for index, word in enumerate(words):
        base_word, trailing = _split_trailing_pause_punct(word.word)
        if index < len(words) - 1:
            gap = max(0.0, words[index + 1].start - word.end)
            if trailing and gap <= tiny_pause:
                trailing = ""
            elif not trailing and gap >= comma_pause:
                trailing = ","
        rendered.append(f"{base_word}{trailing}".strip())

    return " ".join(token for token in rendered if token).strip()


def _split_trailing_pause_punct(token: str) -> tuple[str, str]:
    index = len(token)
    while index > 0 and token[index - 1] in ".,;:?!":
        index -= 1
    return token[:index], token[index:]


def _boundary_acoustic_score(
    timestamp: float,
    word: AlignedWord,
    next_word: AlignedWord,
    rms: np.ndarray,
    rms_times: np.ndarray,
    config: SlicerConfig,
) -> tuple[float, float]:
    context_seconds = config.boundary_context_ms / 1000.0
    valley_energy = _window_mean_rms(
        max(timestamp - context_seconds / 2.0, word.end),
        min(timestamp + context_seconds / 2.0, next_word.start),
        rms,
        rms_times,
    )
    left_reference = _window_mean_rms(max(word.start, word.end - context_seconds), word.end, rms, rms_times)
    right_reference = _window_mean_rms(next_word.start, min(next_word.end, next_word.start + context_seconds), rms, rms_times)
    speech_reference = max((left_reference + right_reference) / 2.0, valley_energy)
    if speech_reference <= 0:
        return 1.0, valley_energy
    ratio = valley_energy / speech_reference
    return max(0.0, min(1.0, 1.0 - ratio)), valley_energy


def _boundary_selection_score(duration: float, boundary: BoundaryCandidate, config: SlicerConfig) -> float:
    duration_span = max(config.target_duration - config.min_duration, 0.5)
    duration_penalty = min(abs(duration - config.target_duration) / duration_span, 1.0)
    overshoot_penalty = 0.0
    if duration > config.soft_max:
        overshoot_span = max(config.max_duration - config.soft_max, 0.5)
        overshoot_penalty = min((duration - config.soft_max) / overshoot_span, 1.0)
    duration_score = 1.0 - duration_penalty
    return (0.55 * duration_score) + (0.45 * boundary.strength) - (0.35 * overshoot_penalty)


def _compute_rms_frames(
    audio: np.ndarray,
    sample_rate: int,
    frame_ms: float,
) -> tuple[np.ndarray, np.ndarray]:
    if len(audio) == 0:
        empty = np.array([], dtype=np.float32)
        return empty, empty

    frame_samples = max(1, int(sample_rate * frame_ms / 1000.0))
    frame_count = len(audio) // frame_samples

    if frame_count == 0:
        rms = np.array([float(np.sqrt(np.mean(audio**2)))], dtype=np.float32)
        times = np.array([len(audio) / (2.0 * sample_rate)], dtype=np.float32)
        return rms, times

    frames = audio[: frame_count * frame_samples].reshape(frame_count, frame_samples)
    rms = np.sqrt(np.mean(frames**2, axis=1))
    times = (np.arange(frame_count) * frame_samples + frame_samples / 2) / sample_rate
    return rms, times


def _window_mean_rms(
    start_time: float,
    end_time: float,
    rms: np.ndarray,
    rms_times: np.ndarray,
) -> float:
    if len(rms_times) == 0 or end_time <= start_time:
        return 0.0
    mask = (rms_times >= start_time) & (rms_times <= end_time)
    if not np.any(mask):
        nearest_index = int(np.argmin(np.abs(rms_times - ((start_time + end_time) / 2.0))))
        return float(rms[nearest_index])
    return float(np.mean(rms[mask]))


def _snap_boundary_in_gap(
    gap_start: float,
    gap_end: float,
    config: SlicerConfig,
    rms: np.ndarray,
    rms_times: np.ndarray,
) -> float:
    if gap_end <= gap_start:
        return gap_start
    if len(rms_times) == 0:
        return (gap_start + gap_end) / 2.0

    midpoint = (gap_start + gap_end) / 2.0
    mask = (rms_times >= gap_start) & (rms_times <= gap_end)
    if not np.any(mask):
        return midpoint

    local_rms = rms[mask]
    local_times = rms_times[mask]
    min_rms = float(np.min(local_rms))
    tolerance = max(min_rms * 1.15, min_rms + 1e-6)
    candidate_indices = np.where(local_rms <= tolerance)[0]
    if len(candidate_indices) == 0:
        return float(local_times[np.argmin(local_rms)])

    best_index = min(candidate_indices, key=lambda idx: abs(float(local_times[idx]) - midpoint))
    return float(local_times[best_index])


def _choose_breath_boundary(
    gap_start: float,
    gap_end: float,
    rms: np.ndarray,
    rms_times: np.ndarray,
    config: SlicerConfig,
) -> float:
    if gap_end <= gap_start:
        return gap_start

    edge_span = max(config.edge_window_ms / 1000.0, 0.02)
    left_energy = _window_mean_rms(gap_start, min(gap_start + edge_span, gap_end), rms, rms_times)
    right_energy = _window_mean_rms(max(gap_end - edge_span, gap_start), gap_end, rms, rms_times)
    return gap_start if left_energy <= right_energy else gap_end


def _detect_breath(
    audio: np.ndarray,
    sample_rate: int,
    gap_start: float,
    gap_duration: float,
    config: SlicerConfig,
) -> bool:
    min_gap = config.breath_min_duration_ms / 1000.0
    max_gap = config.breath_max_duration_ms / 1000.0
    if gap_duration < min_gap or gap_duration > max_gap:
        return False

    start_index = max(0, int(gap_start * sample_rate))
    end_index = min(len(audio), int((gap_start + gap_duration) * sample_rate))
    if end_index <= start_index:
        return False

    region = audio[start_index:end_index]
    rms = float(np.sqrt(np.mean(region**2)))
    if rms < config.breath_energy_floor or rms > config.breath_energy_ceiling:
        return False

    frame_samples = max(1, int(sample_rate * 0.020))
    frame_count = len(region) // frame_samples
    if frame_count < 2:
        return True

    frame_rms = np.sqrt(np.mean(region[: frame_count * frame_samples].reshape(frame_count, frame_samples) ** 2, axis=1))
    return float(np.std(frame_rms)) > 1e-5


def _compute_interval_union(intervals: list[tuple[float, float]]) -> float:
    if not intervals:
        return 0.0

    total = 0.0
    current_start, current_end = sorted(intervals)[0]
    for start, end in sorted(intervals)[1:]:
        if start <= current_end:
            current_end = max(current_end, end)
            continue
        total += max(0.0, current_end - current_start)
        current_start, current_end = start, end
    total += max(0.0, current_end - current_start)
    return total


def _copy_flags(target: SliceSpec, *sources: SliceSpec) -> None:
    for source in sources:
        for reason in source.flag_reasons:
            _add_flag(target, reason)


def _add_flag(spec: SliceSpec, reason: str) -> None:
    if reason not in spec.flag_reasons:
        spec.flag_reasons.append(reason)
    spec.is_flagged = bool(spec.flag_reasons)
    spec.flag_reason = " | ".join(spec.flag_reasons)


__all__ = [
    "AlignedWord",
    "BoundaryCandidate",
    "SliceSpec",
    "SlicerConfig",
    "apply_acoustic_refinement",
    "build_slice_entries",
    "build_slice_stats",
    "find_candidate_boundaries",
    "greedy_group",
    "parse_alignment",
    "plan_slices",
]
