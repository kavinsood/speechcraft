from __future__ import annotations

from unittest import TestCase

import numpy as np

from app.slicer_algo import (
    SlicerConfig,
    apply_acoustic_refinement,
    find_candidate_boundaries,
    greedy_group,
    parse_alignment,
    plan_slices,
)


class SlicerAlgoTests(TestCase):
    def test_parse_alignment_clamps_overlap_and_drops_invalid_words(self) -> None:
        words = parse_alignment(
            [
                {"word": "alpha", "start": 0.0, "end": 0.5},
                {"word": "beta", "start": 0.49, "end": 1.0},
                {"word": "gamma", "start": 1.2, "end": 1.2},
            ]
        )

        self.assertEqual(len(words), 2)
        self.assertAlmostEqual(words[1].start, 0.5, places=6)
        self.assertAlmostEqual(words[1].end, 1.0, places=6)

    def test_find_candidate_boundaries_only_emit_safe_inter_word_gaps(self) -> None:
        words = parse_alignment(
            [
                {"word": "one", "start": 0.0, "end": 0.5},
                {"word": "two", "start": 0.75, "end": 1.2},
                {"word": "three", "start": 1.31, "end": 1.7},
                {"word": "four", "start": 2.2, "end": 2.6},
            ]
        )

        candidates = find_candidate_boundaries(words, SlicerConfig())

        self.assertEqual(len(candidates), 2)
        self.assertEqual([candidate.boundary_type for candidate in candidates], ["safe_gap", "safe_gap"])
        self.assertGreater(candidates[0].safe_end, candidates[0].safe_start)
        self.assertGreater(candidates[1].safe_end, candidates[1].safe_start)
        self.assertGreater(candidates[1].gap_duration, candidates[0].gap_duration)

    def test_find_candidate_boundaries_respect_word_guard_collars(self) -> None:
        config = SlicerConfig(min_gap_for_boundary=0.05, leading_word_guard_ms=35, trailing_word_guard_ms=45)
        words = parse_alignment(
            [
                {"word": "one", "start": 0.0, "end": 1.0},
                {"word": "two", "start": 1.09, "end": 1.8},
            ]
        )

        candidates = find_candidate_boundaries(words, config)

        self.assertEqual(candidates, [])

    def test_find_candidate_boundaries_drop_noisy_gap_candidates(self) -> None:
        config = SlicerConfig(min_duration=0.1, min_boundary_acoustic_score=0.5)
        words = parse_alignment(
            [
                {"word": "one", "start": 0.0, "end": 0.5},
                {"word": "two", "start": 0.8, "end": 1.3},
            ]
        )
        rms = np.array([0.5, 0.48, 0.46, 0.44, 0.46, 0.49], dtype=np.float32)
        rms_times = np.array([0.53, 0.58, 0.63, 0.68, 0.73, 0.78], dtype=np.float32)

        candidates = find_candidate_boundaries(words, config, rms=rms, rms_times=rms_times)

        self.assertEqual(candidates, [])

    def test_greedy_group_prefers_safe_boundary_closest_to_target_duration(self) -> None:
        config = SlicerConfig(min_duration=2.5, max_duration=9.0, target_duration=6.5)
        words = parse_alignment(
            [
                {"word": "one", "start": 0.0, "end": 1.2},
                {"word": "two", "start": 1.6, "end": 2.7},
                {"word": "three", "start": 3.1, "end": 4.1},
                {"word": "four", "start": 4.5, "end": 5.5},
                {"word": "five", "start": 6.0, "end": 7.0},
                {"word": "six", "start": 7.5, "end": 8.6},
                {"word": "tail", "start": 9.2, "end": 10.0},
            ]
        )

        specs = greedy_group(words, find_candidate_boundaries(words, config), config)

        self.assertEqual(len(specs), 2)
        self.assertEqual(specs[0].transcript, "one, two, three, four, five")
        self.assertEqual(specs[0].end_boundary.boundary_type if specs[0].end_boundary else None, "safe_gap")
        self.assertFalse(specs[0].forced_cut)

    def test_greedy_group_keeps_long_clip_when_no_safe_boundary_exists(self) -> None:
        config = SlicerConfig(min_duration=0.5)
        words = parse_alignment(
            [
                {"word": "a", "start": 0.0, "end": 2.0},
                {"word": "b", "start": 2.06, "end": 4.0},
                {"word": "c", "start": 4.07, "end": 6.0},
                {"word": "d", "start": 6.08, "end": 8.0},
                {"word": "e", "start": 8.09, "end": 10.0},
                {"word": "f", "start": 10.1, "end": 12.0},
                {"word": "g", "start": 12.11, "end": 14.0},
                {"word": "h", "start": 14.12, "end": 16.0},
            ]
        )

        specs = greedy_group(words, find_candidate_boundaries(words, config), config)

        self.assertEqual(len(specs), 1)
        self.assertFalse(specs[0].forced_cut)
        self.assertEqual(specs[0].transcript, "a b c d e f g h")
        self.assertIn("no_safe_boundary_16.0s", specs[0].flag_reasons)

    def test_merge_short_clips_preserves_existing_flags(self) -> None:
        config = SlicerConfig(min_duration=2.5, max_duration=4.0, target_duration=3.0)
        words = parse_alignment(
            [
                {"word": "a", "start": 0.0, "end": 2.6, "confidence": 0.1},
                {"word": "b", "start": 3.2, "end": 4.0},
                {"word": "c", "start": 4.6, "end": 5.2},
            ]
        )

        specs = greedy_group(words, find_candidate_boundaries(words, config), config)

        self.assertEqual(len(specs), 1)
        self.assertIn("low_confidence_0.10", specs[0].flag_reasons)

    def test_acoustic_refinement_uses_shared_boundary_and_reports_overlap(self) -> None:
        sample_rate = 1000
        audio = np.zeros(13000, dtype=np.float64)
        config = SlicerConfig(padding_ms=150, min_duration=0.1)
        alignment = [
            {"word": "one", "start": 0.0, "end": 2.0},
            {"word": "two", "start": 2.05, "end": 4.0},
            {"word": "three", "start": 4.05, "end": 6.0},
            {"word": "four", "start": 6.05, "end": 8.0},
            {"word": "five", "start": 8.05, "end": 10.0},
            {"word": "six.", "start": 10.2, "end": 12.0},
            {"word": "tail", "start": 12.5, "end": 13.0},
        ]
        words = parse_alignment(alignment)

        specs = greedy_group(words, find_candidate_boundaries(words, config), config)
        refined = apply_acoustic_refinement(specs, audio, sample_rate, config)

        self.assertAlmostEqual(refined[0].snapped_end, refined[1].snapped_start, places=6)
        self.assertGreaterEqual(refined[0].snapped_end, specs[0].end_boundary.safe_start)
        self.assertLessEqual(refined[0].snapped_end, specs[0].end_boundary.safe_end)

        result = plan_slices(alignment, audio, sample_rate, config)

        self.assertEqual(result["stats"]["total_clips"], 2)
        self.assertEqual(result["stats"]["overlap_audio_s"], 0.0)
        self.assertGreater(result["stats"]["review_overlap_audio_s"], 0.0)
        self.assertEqual(result["slices"][0]["overlap_with_next_s"], 0.0)
        self.assertEqual(result["slices"][1]["overlap_with_previous_s"], 0.0)
        self.assertGreater(result["slices"][0]["review_overlap_with_next_s"], 0.0)
        self.assertGreater(result["slices"][1]["review_overlap_with_previous_s"], 0.0)

    def test_plan_slices_words_are_training_relative_not_padded_relative(self) -> None:
        sample_rate = 1000
        audio = np.zeros(3000, dtype=np.float64)
        result = plan_slices(
            [
                {"word": "one", "start": 1.0, "end": 1.4},
            ],
            audio,
            sample_rate,
            SlicerConfig(min_duration=0.1, padding_ms=150),
        )

        self.assertEqual(result["slices"][0]["relative_word_offsets_from"], "training_start")
        self.assertEqual(result["slices"][0]["words"][0]["start"], 0.0)
        self.assertEqual(result["slices"][0]["training_start"], 1.0)
        self.assertLess(result["slices"][0]["padded_start"], result["slices"][0]["training_start"])

    def test_plan_slices_uses_union_for_coverage_not_padded_sum(self) -> None:
        sample_rate = 1000
        audio = np.zeros(13000, dtype=np.float64)
        config = SlicerConfig(padding_ms=200, min_duration=0.1)
        result = plan_slices(
            [
                {"word": "one", "start": 0.0, "end": 2.0},
                {"word": "two", "start": 2.05, "end": 4.0},
                {"word": "three", "start": 4.05, "end": 6.0},
                {"word": "four", "start": 6.05, "end": 8.0},
                {"word": "five", "start": 8.05, "end": 10.0},
                {"word": "six.", "start": 10.2, "end": 12.0},
                {"word": "tail", "start": 12.5, "end": 13.0},
            ],
            audio,
            sample_rate,
            config,
        )

        self.assertEqual(result["stats"]["total_clip_s"], result["stats"]["unique_covered_audio_s"])
        self.assertEqual(result["stats"]["overlap_audio_s"], 0.0)
        self.assertGreater(result["stats"]["review_overlap_audio_s"], 0.0)

    def test_plan_slices_normalizes_pause_punctuation(self) -> None:
        result = plan_slices(
            [
                {"word": "hello,", "start": 0.0, "end": 0.4},
                {"word": "world", "start": 0.43, "end": 0.8},
                {"word": "again", "start": 1.25, "end": 1.7},
            ],
            np.zeros(3000, dtype=np.float64),
            1000,
            SlicerConfig(min_duration=2.0, max_duration=5.0, min_boundary_acoustic_score=0.0),
        )

        self.assertEqual(result["slices"][0]["transcript"], "hello world, again")
        self.assertEqual(result["slices"][0]["transcript_original"], "hello, world again")

    def test_plan_slices_flags_high_edge_energy(self) -> None:
        sample_rate = 1000
        audio = np.zeros(5000, dtype=np.float64)
        audio[480:500] = 1.0
        audio[500:800] = 0.8
        audio[800:1200] = 0.05
        audio[1200:1500] = 0.8
        audio[1500:1520] = 1.0

        result = plan_slices(
            [
                {"word": "one", "start": 0.5, "end": 0.8},
                {"word": "two", "start": 1.2, "end": 1.5},
            ],
            audio,
            sample_rate,
            SlicerConfig(min_duration=0.1, padding_ms=0, min_boundary_acoustic_score=0.0),
        )

        self.assertTrue(result["slices"][0]["is_flagged"])
        self.assertTrue(
            any(reason.startswith("high_") for reason in result["slices"][0]["flag_reasons"])
        )

    def test_plan_slices_defaults_missing_confidence_to_one(self) -> None:
        result = plan_slices(
            [{"word": "hello.", "start": 0.0, "end": 0.8}],
            np.zeros(1000, dtype=np.float64),
            1000,
            SlicerConfig(min_duration=0.1),
        )

        self.assertEqual(result["stats"]["avg_confidence"], 1.0)
        self.assertEqual(result["slices"][0]["avg_alignment_confidence"], 1.0)
