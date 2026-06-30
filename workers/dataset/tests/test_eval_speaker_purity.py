from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from speechcraft_dataset.eval_speaker_purity import (
    SpeakerPurityConfig,
    _percentile_score_0_100,
    outlier_guillotine,
    prepare_window_samples,
    run_speaker_purity,
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

    def test_percentile_score_0_100_stays_in_range_for_multiple_scores(self) -> None:
        scores = [50.0, 75.0, 100.0]
        p10 = _percentile_score_0_100(scores, 10)
        p50 = _percentile_score_0_100(scores, 50)
        p90 = _percentile_score_0_100(scores, 90)
        self.assertIsNotNone(p10)
        self.assertIsNotNone(p50)
        self.assertIsNotNone(p90)
        for value in (p10, p50, p90):
            assert value is not None
            self.assertTrue(0.0 <= value <= 100.0)
        self.assertEqual(p10, 50.0)
        self.assertEqual(p50, 75.0)
        self.assertEqual(p90, 100.0)

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

    def test_run_speaker_purity_writes_phase3_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            (artifacts / "candidate_review_manifest.json").write_text(
                json.dumps(
                    [
                        {
                            "id": "candidate_review_clip_000000",
                            "audio_path": "artifacts/candidate_review_clips/candidate_review_clip_000000.wav",
                            "duration_sec": 3.0,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            (artifacts / "speaker_selection.json").write_text(
                json.dumps({"target_speaker_id": "speaker_0"}),
                encoding="utf-8",
            )
            (artifacts / "speaker_regions.jsonl").write_text("{}", encoding="utf-8")
            (artifacts / "audio_variants_manifest.json").write_text(
                json.dumps({"variants": [{"source_audio_id": "source_audio_0000", "path": "analysis.wav", "analysis_sample_rate": 16000}]}),
                encoding="utf-8",
            )

            stage_dir = artifacts / "_speaker_purity_stage"

            def fake_evaluate(*_args, **_kwargs):
                stage_dir.mkdir(parents=True, exist_ok=True)
                (stage_dir / "speaker_purity_qc.json").write_text(
                    json.dumps(
                        [
                            {
                                "clip_id": "candidate_review_clip_000000",
                                "audio_path": "artifacts/candidate_review_clips/candidate_review_clip_000000.wav",
                                "duration_sec": 3.0,
                                "min_window_similarity": 0.74,
                                "mean_window_similarity": 0.86,
                                "p10_window_similarity": 0.78,
                                "scored_window_count": 8,
                                "skipped_window_count": 0,
                                "intruder_window_count": 1,
                                "worst_window_start_sec": 2.5,
                                "reason_codes": [],
                                "windows": [
                                    {"start_sec": 2.5, "end_sec": 5.5, "similarity": 0.74},
                                ],
                            }
                        ]
                    ),
                    encoding="utf-8",
                )
                enrollment = stage_dir / "enrollment"
                enrollment.mkdir(parents=True, exist_ok=True)
                (enrollment / "target_voiceprint.json").write_text(
                    json.dumps({"speaker_id": "speaker_0", "embedding_dim": 3}),
                    encoding="utf-8",
                )
                return {"target_speaker_id": "speaker_0"}

            with patch("speechcraft_dataset.eval_speaker_purity.evaluate_speaker_purity", side_effect=fake_evaluate):
                summary = run_speaker_purity(run_root, {})

            artifact = json.loads((artifacts / "speaker_purity.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["stage"], "speaker_purity")
            self.assertEqual(artifact["schema_version"], 1)
            self.assertEqual(artifact["stage"], "speaker_purity")
            clip = artifact["clips"][0]
            self.assertEqual(clip["speaker_check_score"], 74.0)
            self.assertEqual(clip["speaker_score_method"], "min_valid_window_similarity")
            self.assertEqual(clip["bucket_hint"], "pass")
            self.assertEqual(clip["suspicious_spans"][0]["similarity"], 0.74)
            summary_artifact = json.loads((artifacts / "speaker_purity_summary.json").read_text(encoding="utf-8"))
            self.assertTrue(0.0 <= summary_artifact["score_p10"] <= 100.0)
            self.assertTrue(0.0 <= summary_artifact["score_p50"] <= 100.0)
            self.assertTrue(0.0 <= summary_artifact["score_p90"] <= 100.0)
            self.assertEqual(summary_artifact["score_p10"], 74.0)
            self.assertEqual(summary_artifact["score_p50"], 74.0)
            self.assertEqual(summary_artifact["score_p90"], 74.0)
            self.assertTrue((artifacts / "speaker_purity_summary.json").exists())
            self.assertTrue((artifacts / "target_voiceprint.json").exists())


if __name__ == "__main__":
    unittest.main()
