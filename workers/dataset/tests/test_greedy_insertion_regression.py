from __future__ import annotations

import unittest

from speechcraft_dataset.analyze_ctc_transcript_qc import (
    GREEDY_INTEGRITY_FAIL_SCORE,
    GREEDY_INTEGRITY_PASS_SCORE,
    composite_transcript_match_score,
    detect_greedy_insertions,
    find_sequence_insertion_blocks,
)


class GreedyInsertionRegressionTests(unittest.TestCase):
    def test_sequence_diff_finds_middle_insertion_with_repeated_words(self) -> None:
        blocks = find_sequence_insertion_blocks(
            "I|KNOW|I|KNOW",
            "I|KNOW|THAT|I|KNOW",
        )
        self.assertEqual(blocks, [(2, 3, ["THAT"])])

    def test_sequence_diff_ignores_substitutions(self) -> None:
        blocks = find_sequence_insertion_blocks(
            "I|VE|BEEN|THINKING",
            "I|HAVE|BEEN|THINKING",
        )
        self.assertEqual(blocks, [])

    def test_sequence_diff_finds_leading_insertion(self) -> None:
        blocks = find_sequence_insertion_blocks(
            "I|VE|BEEN|THINKING",
            "LIKE|I|VE|BEEN|THINKING",
        )
        self.assertEqual(blocks, [(0, 1, ["LIKE"])])

    def test_sequence_diff_finds_trailing_insertion(self) -> None:
        blocks = find_sequence_insertion_blocks(
            "I|HAVE|BEEN",
            "I|HAVE|BEEN|THINKING",
        )
        self.assertEqual(blocks, [(3, 4, ["THINKING"])])

    def test_sequence_diff_ignores_short_insertions(self) -> None:
        blocks = find_sequence_insertion_blocks(
            "I|VE|BEEN|THINKING",
            "I|AM|VE|BEEN|THINKING",
        )
        self.assertEqual(blocks, [])

    def test_sequence_diff_finds_like_insertion(self) -> None:
        count, words = detect_greedy_insertions(
            "I|VE|BEEN|THINKING",
            "LIKE|I|VE|BEEN|THINKING",
        )
        self.assertEqual(count, 1)
        self.assertEqual(words, ["LIKE"])

    def test_sequence_diff_no_false_positive_on_clean_pair(self) -> None:
        count, words = detect_greedy_insertions(
            "I|VE|BEEN|THINKING",
            "I|VE|BEEN|THINKING",
        )
        self.assertEqual(count, 0)
        self.assertEqual(words, [])

    def test_composite_score_keeps_alignment_when_greedy_is_clean(self) -> None:
        integrity, final = composite_transcript_match_score(94.0, confirmed_insertions=[])
        self.assertEqual(integrity, GREEDY_INTEGRITY_PASS_SCORE)
        self.assertEqual(final, 94.0)

    def test_composite_score_drops_to_zero_on_confirmed_insertion(self) -> None:
        integrity, final = composite_transcript_match_score(
            96.0,
            confirmed_insertions=[{"text": "LIKE", "start_sec": 0.1, "end_sec": 0.4, "confidence": 0.91, "location": "interior"}],
        )
        self.assertEqual(integrity, GREEDY_INTEGRITY_FAIL_SCORE)
        self.assertEqual(final, 0.0)

    def test_stage3_regression_matrix(self) -> None:
        """Synthetic omission cases from the reviewer checklist."""

        cases = [
            ("LIKE|I|VE|BEEN|THINKING", "I|VE|BEEN|THINKING", True, "missing first word"),
            ("I|VE|BEEN|LIKE|THINKING", "I|VE|BEEN|THINKING", True, "missing middle word"),
            ("I|VE|BEEN|THINKING|ABOUT", "I|VE|BEEN|THINKING", True, "missing final word"),
            ("I|KNOW|I|KNOW", "KNOW|I|KNOW", False, "missing short leading function word is ignored"),
            ("LIKE|I|VE|BEEN|THINKING", "I|VE|BEEN|THINKING", True, "missing like"),
            ("I|KNOW|THAT|I|KNOW", "I|KNOW|I|KNOW", True, "missing repeated-word neighbor"),
            ("I|HAVE|BEEN|THINKING", "I|VE|BEEN|THINKING", False, "contraction spelling mismatch is not an insertion"),
            ("I|VE|BEEN|THINKING", "I|VE|BEEN|THINKING", False, "unchanged clean pair"),
        ]
        for greedy, expected, should_flag, label in cases:
            count, _words = detect_greedy_insertions(expected, greedy)
            self.assertEqual(
                count > 0,
                should_flag,
                msg=f"{label}: greedy={greedy!r} expected={expected!r}",
            )


if __name__ == "__main__":
    unittest.main()
