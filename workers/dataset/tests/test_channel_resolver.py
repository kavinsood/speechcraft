"""Unit tests for the intelligent channel resolver decision core (task 2)."""

import math
import random
import unittest

from speechcraft_dataset.channel_resolver import decide_channel


def _sine(freq: float, sr: int, seconds: float, amp: float = 0.6) -> list[int]:
    n = int(sr * seconds)
    return [int(amp * math.sin(2 * math.pi * freq * i / sr) * 32767) for i in range(n)]


def _noise(sr: int, seconds: float, amp: float, seed: int) -> list[int]:
    rng = random.Random(seed)
    n = int(sr * seconds)
    return [int(rng.uniform(-amp, amp) * 32767) for _ in range(n)]


class ChannelResolverTest(unittest.TestCase):
    def setUp(self):
        self.sr = 16000

    def test_matched_stereo_downmixes(self):
        ch = _sine(220.0, self.sr, 1.0)
        res = decide_channel(list(ch), list(ch), sample_rate=self.sr)
        self.assertEqual(res.decision, "downmix")
        self.assertEqual(res.reason, "matched_stereo")
        self.assertGreaterEqual(res.correlation, 0.98)

    def test_dominant_left_channel_picked(self):
        left = _sine(180.0, self.sr, 1.0, amp=0.8)
        right = [0] * len(left)
        res = decide_channel(left, right, sample_rate=self.sr)
        self.assertEqual(res.decision, "left")
        self.assertEqual(res.reason, "dominant_speech_channel")

    def test_dominant_right_channel_picked(self):
        right = _sine(180.0, self.sr, 1.0, amp=0.8)
        left = [0] * len(right)
        res = decide_channel(left, right, sample_rate=self.sr)
        self.assertEqual(res.decision, "right")

    def test_both_silent_downmixes(self):
        z = [0] * self.sr
        res = decide_channel(z, list(z), sample_rate=self.sr)
        # Identical silence -> safe to downmix (reached via the matched branch,
        # since two equal silent channels are perfectly correlated).
        self.assertEqual(res.decision, "downmix")

    def test_ambiguous_divergent_channels_escalate(self):
        # Independent, similar-loudness signals: decorrelated but neither
        # dominates -> the resolver escalates rather than guess.
        left = _noise(self.sr, 1.0, 0.5, seed=1)
        right = _noise(self.sr, 1.0, 0.5, seed=2)
        res = decide_channel(left, right, sample_rate=self.sr)
        self.assertEqual(res.decision, "prompt")
        self.assertIn("channel_selection_ambiguous", res.reason_codes)
        self.assertLess(res.correlation, 0.98)


if __name__ == "__main__":
    unittest.main()
