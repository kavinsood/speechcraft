from __future__ import annotations

import unittest

import numpy as np

from speechcraft_dataset.eval_speaker_purity_cocktail_test import (
    CocktailConfig,
    blend_underneath,
    hard_splice,
    peak_normalize,
    summarize_cocktail_trials,
)


class EvalSpeakerPurityCocktailTestTests(unittest.TestCase):
    def test_hard_splice_replaces_middle_segment(self) -> None:
        target = np.ones(16000, dtype=np.float32) * 0.2
        intruder = np.ones(6400, dtype=np.float32) * 0.9
        out, start, end = hard_splice(target, intruder, 16000, splice_ms=400.0)
        self.assertEqual(end - start, 6400)
        self.assertTrue(np.allclose(out[start:end], intruder, atol=0.2))
        self.assertTrue(np.allclose(out[:start], 0.2 * (0.95 / 0.9), atol=0.05))

    def test_blend_adds_intruder_underneath(self) -> None:
        target = np.ones(32000, dtype=np.float32) * 0.2
        intruder = np.ones(32000, dtype=np.float32) * 0.4
        out, start, end = blend_underneath(target, intruder, 16000, blend_sec=2.0, mix_ratio=0.5)
        self.assertEqual(end - start, 32000)
        self.assertGreater(float(np.mean(out[start:end])), 0.2)

    def test_peak_normalize_caps_amplitude(self) -> None:
        samples = np.asarray([0.0, 2.0, -3.0], dtype=np.float32)
        normalized = peak_normalize(samples)
        self.assertAlmostEqual(float(np.max(np.abs(normalized))), 0.95, places=5)

    def test_summarize_flags_overlap_blind_blend(self) -> None:
        summary = summarize_cocktail_trials(
            [
                {"variant": "hard_splice", "purity_score": 0.55, "score_delta": 0.3},
                {"variant": "blend", "purity_score": 0.88, "score_delta": 0.02},
                {"variant": "blend", "purity_score": 0.62, "score_delta": 0.25},
            ],
            CocktailConfig(),
        )
        self.assertEqual(summary["blend_overlap_blind_count"], 1)
        self.assertTrue(summary["splice_control_pass"])


if __name__ == "__main__":
    unittest.main()
