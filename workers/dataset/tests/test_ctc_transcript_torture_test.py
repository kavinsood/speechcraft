from __future__ import annotations

import unittest

from speechcraft_dataset.analyze_ctc_transcript_qc import audio_coverage_metrics
from speechcraft_dataset.ctc_transcript_torture_test import (
    add_word,
    build_trial_record,
    join_words,
    localized_metrics,
    omit_word,
    replace_word,
    split_words,
    summarize_trials,
)


class CtcTranscriptTortureTestTests(unittest.TestCase):
    def test_split_and_join_words(self) -> None:
        self.assertEqual(split_words("HELLO|WORLD"), ["HELLO", "WORLD"])
        self.assertEqual(join_words(["HELLO", "WORLD"]), "HELLO|WORLD")

    def test_omit_word(self) -> None:
        result = omit_word("A|B|C|D", 1)
        self.assertIsNotNone(result)
        text, removed = result
        self.assertEqual(text, "A|C|D")
        self.assertEqual(removed, "B")

    def test_add_word(self) -> None:
        self.assertEqual(add_word("A|C", "B", 1), "A|B|C")

    def test_replace_word(self) -> None:
        result = replace_word("A|B|C", 1, "ZEBRA")
        self.assertIsNotNone(result)
        text, old = result
        self.assertEqual(text, "A|ZEBRA|C")
        self.assertEqual(old, "B")

    def test_audio_coverage_metrics_from_segment_span(self) -> None:
        coverage = audio_coverage_metrics(
            audio_duration_sec=5.0,
            segments=[(0.5, 3.5, 0.9)],
            timings=[0.5, 1.0, 2.0, 3.0],
            index_duration=0.02,
        )
        self.assertEqual(coverage["aligned_speech_sec"], 3.0)
        self.assertEqual(coverage["unexplained_speech_sec"], 2.0)
        self.assertEqual(coverage["unaligned_speech_ratio"], 0.4)

    def test_summarize_trials_counts_drops_and_poison(self) -> None:
        baseline = {
            "transcript_match_score": 95.0,
            "ctc_mean_pct": 95.0,
            "ctc_min_token_pct": 80.0,
            "ctc_min_aligned_token_pct": 80.0,
            "ctc_min_window_pct": 75.0,
            "unaligned_token_count": 0,
            "weak_span_count": 0,
            "segment_confidence_pct": 95.0,
            "bucket": "accepted",
        }
        perturbed = {
            "transcript_match_score": 92.0,
            "ctc_mean_pct": 92.0,
            "ctc_min_token_pct": 0.0,
            "ctc_min_aligned_token_pct": 10.0,
            "ctc_min_window_pct": 12.0,
            "unaligned_token_count": 3,
            "weak_span_count": 2,
            "segment_confidence_pct": 90.0,
            "bucket": "accepted",
        }
        baseline["unaligned_speech_ratio"] = 0.08
        baseline["unexplained_speech_sec"] = 0.4
        baseline["aligned_speech_ratio"] = 0.92
        perturbed["unaligned_speech_ratio"] = 0.22
        perturbed["unexplained_speech_sec"] = 1.1
        perturbed["aligned_speech_ratio"] = 0.78
        trial = build_trial_record(
            perturbation="omit",
            try_index=0,
            word_index=1,
            detail={"added_word": "BANANA"},
            perturbed_text="A|BANANA|B",
            baseline=baseline,
            perturbed=perturbed,
        )
        summary = summarize_trials([{"clip_id": "clip-1", "trials": [trial]}])
        self.assertEqual(summary["trial_count"], 1)
        self.assertEqual(summary["overall"]["mean_delta"], 3.0)
        self.assertEqual(summary["localized"]["pct_poisoned_detected"], 100.0)
        self.assertEqual(summary["localized"]["coverage"]["mean_unaligned_speech_ratio_delta"], 0.14)
        self.assertTrue(trial["omission_signal"])

    def test_build_trial_record_flags_greedy_insertions_on_omit(self) -> None:
        baseline = {
            "transcript_match_score": 95.0,
            "ctc_mean_pct": 95.0,
            "ctc_min_token_pct": 80.0,
            "ctc_min_aligned_token_pct": 80.0,
            "ctc_min_window_pct": 75.0,
            "unaligned_token_count": 0,
            "weak_span_count": 0,
            "segment_confidence_pct": 95.0,
            "unaligned_speech_ratio": 0.08,
            "unexplained_speech_sec": 0.4,
            "aligned_speech_ratio": 0.92,
            "ctc_greedy_insertions": 0,
            "ctc_greedy_insertion_words": [],
            "untranscribed_speech_detected": False,
            "bucket": "accepted",
        }
        perturbed = {
            **baseline,
            "transcript_match_score": 0.0,
            "forced_alignment_score": 94.0,
            "greedy_integrity_score": 0.0,
            "ctc_greedy_insertions": 1,
            "ctc_greedy_insertion_words": ["LIKE"],
            "untranscribed_speech_detected": True,
            "bucket": "rejected",
        }
        trial = build_trial_record(
            perturbation="omit",
            try_index=0,
            word_index=0,
            detail={"removed_word": "LIKE"},
            perturbed_text="I|VE|BEEN|THINKING",
            baseline=baseline,
            perturbed=perturbed,
        )
        self.assertTrue(trial["caught_by_greedy_decode"])
        self.assertTrue(trial["poisoned_by_greedy_insertions"])
        self.assertTrue(trial["poisoned_detected"])
        self.assertEqual(trial["perturbed_ctc_greedy_insertions"], 1)
        self.assertEqual(trial["perturbed_greedy_insertion_words"], ["LIKE"])
        self.assertEqual(trial["greedy_insertion_delta"], 1)

        summary = summarize_trials([{"clip_id": "clip-1", "trials": [trial]}])
        self.assertEqual(summary["localized"]["pct_poisoned_by_greedy_insertions"], 100.0)
        self.assertEqual(summary["localized"]["coverage"]["pct_caught_by_greedy_decode"], 100.0)
        self.assertEqual(len(summary["top_greedy_insertion_cases"]), 1)

    def test_localized_metrics_converts_to_percent(self) -> None:
        metrics = localized_metrics(
            {
                "transcript_match_score": 91.0,
                "ctc_mean_score": 0.91,
                "ctc_min_token_score": 0.0,
                "ctc_min_aligned_token_score": 0.05,
                "ctc_min_window_score": 0.12,
                "unaligned_token_count": 1,
                "weak_span_count": 2,
                "segment_confidence": 0.9,
                "ctc_greedy_insertions": 1,
                "ctc_greedy_insertion_words": ["LIKE|WELL"],
                "bucket": "rejected",
            }
        )
        self.assertEqual(metrics["ctc_greedy_insertions"], 1)
        self.assertEqual(metrics["ctc_greedy_insertion_words"], ["LIKE|WELL"])


if __name__ == "__main__":
    unittest.main()
