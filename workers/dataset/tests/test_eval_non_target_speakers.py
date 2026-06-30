from __future__ import annotations

import unittest

import numpy as np

from speechcraft_dataset.eval_non_target_speakers import (
    NonTargetSpeakerConfig,
    centroid_similarity_matrix,
    regions_for_speaker,
    unique_speaker_ids,
)


class EvalNonTargetSpeakersTests(unittest.TestCase):
    def test_unique_speaker_ids(self) -> None:
        regions = [
            {"speaker_id": "speaker_1"},
            {"speaker_id": "speaker_0"},
            {"speaker_id": "speaker_1"},
        ]
        self.assertEqual(unique_speaker_ids(regions), ["speaker_0", "speaker_1"])

    def test_regions_for_speaker_filters_short_regions(self) -> None:
        regions = [
            {"speaker_id": "speaker_1", "start_sec": 0.0, "end_sec": 2.0},
            {"speaker_id": "speaker_1", "start_sec": 0.0, "end_sec": 5.0},
        ]
        filtered = regions_for_speaker(regions, "speaker_1", min_duration_sec=3.0)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["end_sec"], 5.0)

    def test_centroid_similarity_matrix_is_symmetric(self) -> None:
        raw = {
            "speaker_0": np.array([1.0, 0.0, 0.0]),
            "speaker_1": np.array([0.0, 1.0, 0.0]),
            "speaker_2": np.array([0.6, 0.8, 0.0]),
        }
        centroids = {key: value / np.linalg.norm(value) for key, value in raw.items()}
        payload = centroid_similarity_matrix(centroids)
        matrix = payload["cosine_similarity"]
        self.assertEqual(matrix["speaker_0"]["speaker_0"], 1.0)
        self.assertAlmostEqual(
            matrix["speaker_0"]["speaker_1"],
            matrix["speaker_1"]["speaker_0"],
        )
        self.assertLess(matrix["speaker_0"]["speaker_1"], 0.2)

    def test_non_target_config_defaults(self) -> None:
        config = NonTargetSpeakerConfig()
        self.assertEqual(config.leak_window_sec, 1.5)
        self.assertEqual(config.leak_hop_sec, 0.5)
        self.assertEqual(config.intruder_similarity_threshold, 0.70)


if __name__ == "__main__":
    unittest.main()
