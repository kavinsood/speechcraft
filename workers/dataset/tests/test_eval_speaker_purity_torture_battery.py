from __future__ import annotations

import unittest

import numpy as np

from speechcraft_dataset.eval_speaker_purity_cocktail_test import (
    append_intruder,
    blend_underneath,
    prepend_intruder,
    replace_segment,
)
from speechcraft_dataset.eval_speaker_purity_torture_battery import apply_variant, battery_variants


class EvalSpeakerPurityTortureBatteryTests(unittest.TestCase):
    def test_battery_has_expected_variant_count(self) -> None:
        self.assertEqual(len(battery_variants()), 15)

    def test_blend_placement_start(self) -> None:
        target = np.ones(32000, dtype=np.float32) * 0.2
        intruder = np.ones(32000, dtype=np.float32) * 0.4
        out, start, _end = blend_underneath(target, intruder, 16000, blend_sec=2.0, mix_ratio=0.5, placement="start")
        self.assertEqual(start, 0)
        self.assertGreater(float(np.mean(out[:32000])), 0.2)

    def test_replace_segment(self) -> None:
        target = np.ones(32000, dtype=np.float32) * 0.1
        intruder = np.ones(32000, dtype=np.float32) * 0.8
        out, start, end = replace_segment(target, intruder, 16000, duration_sec=1.0, placement="mid")
        self.assertGreater(end - start, 0)
        self.assertGreater(float(np.max(np.abs(out[start:end]))), 0.5)

    def test_append_and_prepend_change_length(self) -> None:
        target = np.ones(16000, dtype=np.float32) * 0.2
        intruder = np.ones(8000, dtype=np.float32) * 0.5
        appended, _, _ = append_intruder(target, intruder, 16000, duration_sec=0.5)
        prepended, _, _ = prepend_intruder(target, intruder, 16000, duration_sec=0.5)
        self.assertEqual(appended.size, 24000)
        self.assertEqual(prepended.size, 24000)

    def test_apply_variant_splice_2s(self) -> None:
        variant = next(v for v in battery_variants() if v.name == "splice_2s_mid")
        target = np.ones(64000, dtype=np.float32) * 0.1
        intruder = np.ones(64000, dtype=np.float32) * 0.9
        out, start, end = apply_variant(variant, target, intruder, 16000)
        self.assertEqual(end - start, 32000)


if __name__ == "__main__":
    unittest.main()
