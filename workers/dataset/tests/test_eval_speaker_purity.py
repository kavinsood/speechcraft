from __future__ import annotations

import unittest

import numpy as np

from speechcraft_dataset.eval_speaker_purity import (
    SpeakerPurityConfig,
    outlier_guillotine,
    prepare_window_samples,
    window_is_scorable,
)


class EvalSpeakerPurityTests(unittest.TestCase):
    def test_outlier_guillotine_drops_lowest_quartile(self) -> None:
        embeddings = [
            np.array([1.0, 0.0, 0.0]),
            np.array([0.99, 0.01, 0.0]),
            np.array([0.98, 0.02, 0.0]),
            np.array([0.0, 1.0, 0.0]),
        ]
        keep, mean_sims, cutoff = outlier_guillotine(embeddings, percentile=25.0)
        self.assertEqual(len(keep), 4)
        self.assertIsNotNone(cutoff)
        self.assertFalse(keep[3])
        self.assertEqual(sum(keep), 3)

    def test_window_is_scorable_skips_mostly_silent_window(self) -> None:
        config = SpeakerPurityConfig()
        silent = np.zeros(16000, dtype=np.float32)
        scorable, _, silent_fraction = window_is_scorable(silent, 16000, config)
        self.assertFalse(scorable)
        self.assertGreaterEqual(silent_fraction, config.max_silent_frame_fraction)

    def test_window_is_scorable_accepts_loud_speech_like_window(self) -> None:
        config = SpeakerPurityConfig()
        t = np.linspace(0.0, 1.0, 16000, endpoint=False)
        speech = (0.25 * np.sin(2 * np.pi * 220 * t)).astype(np.float32)
        scorable, window_rms, silent_fraction = window_is_scorable(speech, 16000, config)
        self.assertTrue(scorable)
        self.assertGreaterEqual(window_rms, config.silence_rms_threshold)
        self.assertLess(silent_fraction, config.max_silent_frame_fraction)

    def test_prepare_window_samples_zero_pads_short_clip(self) -> None:
        short = np.ones(8000, dtype=np.float32) * 0.1
        padded = prepare_window_samples(short, 16000, window_sec=3.0)
        self.assertEqual(padded.size, 48000)
        self.assertTrue(np.allclose(padded[8000:], 0.0))


if __name__ == "__main__":
    unittest.main()
