from __future__ import annotations

from unittest import TestCase

import numpy as np

from app.slicer_core import find_energy_trough, pack_aligned_words


class SlicerCoreTests(TestCase):
    def test_find_energy_trough_prefers_middle_of_silence_plateau(self) -> None:
        sample_rate = 1000
        tone_duration = 0.2
        silence_duration = 0.2

        tone_times = np.arange(int(sample_rate * tone_duration), dtype=np.float64) / sample_rate
        leading_tone = 0.8 * np.sin(2 * np.pi * 8 * tone_times)
        silence = np.zeros(int(sample_rate * silence_duration), dtype=np.float64)
        trailing_tone = 0.8 * np.sin(2 * np.pi * 8 * tone_times)
        gap_audio = np.concatenate([leading_tone, silence, trailing_tone])

        trough_seconds = find_energy_trough(gap_audio, sample_rate)

        self.assertAlmostEqual(trough_seconds, 0.3, places=2)

    def test_pack_aligned_words_uses_terminal_soft_and_hard_cut_rules(self) -> None:
        sample_rate = 1000
        audio_duration_seconds = 33.0
        audio = np.zeros(int(sample_rate * audio_duration_seconds), dtype=np.float64)
        alignment_units = [
            {"word": "Alpha", "start": 0.0, "end": 2.0},
            {"word": "bravo", "start": 2.1, "end": 4.0},
            {"word": "charlie.", "start": 4.1, "end": 6.2},
            {"word": "Delta", "start": 6.8, "end": 9.2},
            {"word": "echo", "start": 9.3, "end": 11.0},
            {"word": "foxtrot,", "start": 11.1, "end": 14.9},
            {"word": "golf", "start": 15.4, "end": 18.0},
            {"word": "hotel", "start": 18.1, "end": 21.0},
            {"word": "india", "start": 21.1, "end": 24.0},
            {"word": "juliet", "start": 24.1, "end": 27.0},
            {"word": "kilo", "start": 27.1, "end": 30.3},
            {"word": "lima", "start": 30.6, "end": 33.0},
        ]

        packed = list(pack_aligned_words(alignment_units, audio, sample_rate))

        self.assertEqual(
            packed,
            [
                {
                    "start_s": 0.0,
                    "end_s": 6.5,
                    "transcript_text": "Alpha bravo charlie.",
                },
                {
                    "start_s": 6.5,
                    "end_s": 15.15,
                    "transcript_text": "Delta echo foxtrot,",
                },
                {
                    "start_s": 15.15,
                    "end_s": 30.3,
                    "transcript_text": "golf hotel india juliet kilo",
                },
                {
                    "start_s": 30.3,
                    "end_s": 33.0,
                    "transcript_text": "lima",
                },
            ],
        )
