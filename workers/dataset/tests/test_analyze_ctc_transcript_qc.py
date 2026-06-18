from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from speechcraft_dataset.analyze_ctc_transcript_qc import (
    TRANSCRIPT_SCORE_METHOD,
    _meaningful_span_metrics,
    normalize_verifier_text,
    run_transcript_qc,
    select_transcript_gate_score,
    score_bucket_hint,
    score_bucket,
    select_verifier_source_text,
)
from speechcraft_dataset.buffers import write_pcm16_mono


class AnalyzeCtcTranscriptQcTests(unittest.TestCase):
    def test_select_verifier_source_text_prefers_verifier_text(self) -> None:
        field, text = select_verifier_source_text(
            {
                "verifier_text": " hello ",
                "normalized_text": "world",
                "alignment_text": "foo",
                "training_text": "bar",
            }
        )
        self.assertEqual(field, "verifier_text")
        self.assertEqual(text, "hello")

    def test_normalize_verifier_text_uppercases_for_wav2vec2_vocab(self) -> None:
        char_list = {"A", "B", "C", "I", "S", "N", "T", "H", "E", "L", "K", "R", "V", "G", "M", "U", "W", "O", "P", "F", "D", "Y", "J", "Q", "X", "Z", "|", "'"}
        normalized, reasons = normalize_verifier_text("isn't this fine", char_list)
        self.assertEqual(normalized, "ISN'T|THIS|FINE")
        self.assertEqual(reasons, [])

    def test_normalize_verifier_text_flags_digits_and_symbols(self) -> None:
        char_list = {chr(code) for code in range(ord("A"), ord("Z") + 1)} | {"|"}
        normalized, reasons = normalize_verifier_text("$20 deal", char_list)
        self.assertEqual(normalized, "DEAL")
        self.assertIn("contains_digits", reasons)
        self.assertIn("contains_symbols", reasons)
        self.assertIn("verifier_text_may_be_unreliable", reasons)

    def test_score_bucket_thresholds(self) -> None:
        self.assertEqual(score_bucket(90.0), "accepted")
        self.assertEqual(score_bucket(80.0), "review")
        self.assertEqual(score_bucket(50.0), "rejected")
        self.assertEqual(score_bucket(None), "failed")

    def test_score_bucket_hint_thresholds(self) -> None:
        self.assertEqual(score_bucket_hint(90), "pass")
        self.assertEqual(score_bucket_hint(80), "review")
        self.assertEqual(score_bucket_hint(50), "fail")
        self.assertEqual(score_bucket_hint(None), "unscored")

    def test_meaningful_span_metrics_uses_bad_span_not_good_mean(self) -> None:
        verifier_text = "HELLO|WORLD|AGAIN"
        char_probs = np.array(
            [0.95, 0.95, 0.95, 0.95, 0.95, 0.99, 0.2, 0.2, 0.2, 0.2, 0.2, 0.99, 0.97, 0.97, 0.97, 0.97, 0.97],
            dtype=np.float64,
        )
        timings = np.linspace(0.0, 1.6, num=char_probs.size)
        min_span_score, weak_spans = _meaningful_span_metrics(verifier_text, char_probs, timings, 0.1)
        self.assertAlmostEqual(min_span_score or 0.0, 0.2, places=3)
        self.assertEqual(weak_spans[0]["text"], "WORLD")

    def test_meaningful_span_metrics_ignores_pipe_only_boundaries(self) -> None:
        verifier_text = "A|B"
        char_probs = np.array([0.4, 0.9, 0.3], dtype=np.float64)
        timings = np.array([0.0, 0.4, 0.8], dtype=np.float64)
        min_span_score, weak_spans = _meaningful_span_metrics(verifier_text, char_probs, timings, 0.1)
        self.assertIsNone(min_span_score)
        self.assertEqual(weak_spans, [])

    def test_select_transcript_gate_score_fallback_order(self) -> None:
        self.assertEqual(
            select_transcript_gate_score(
                ctc_min_span_score=0.2,
                ctc_min_aligned_token_score=0.8,
                ctc_min_window_score=0.7,
                ctc_mean_score=0.95,
            ),
            0.2,
        )
        self.assertEqual(
            select_transcript_gate_score(
                ctc_min_span_score=None,
                ctc_min_aligned_token_score=0.8,
                ctc_min_window_score=0.7,
                ctc_mean_score=0.95,
            ),
            0.8,
        )
        self.assertEqual(
            select_transcript_gate_score(
                ctc_min_span_score=None,
                ctc_min_aligned_token_score=None,
                ctc_min_window_score=0.7,
                ctc_mean_score=0.95,
            ),
            0.7,
        )

    def test_build_clip_row_missing_audio_is_non_fatal(self) -> None:
        from speechcraft_dataset.analyze_ctc_transcript_qc import CtcModelBundle, build_clip_row

        bundle = CtcModelBundle(model=object(), processor=object(), char_list=["A", "|"], device="cpu")
        row = build_clip_row(
            {
                "id": "clip-1",
                "alignment_text": "a",
                "duration_sec": 1.0,
            },
            Path("/tmp/does-not-exist"),
            bundle,
        )
        self.assertEqual(row["bucket"], "failed")
        self.assertIn("missing_audio", row["reason_codes"])

    def test_analyze_ctc_transcript_qc_smoke(self) -> None:
        from speechcraft_dataset.analyze_ctc_transcript_qc import CtcModelBundle, analyze_ctc_transcript_qc

        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            audio_dir = artifacts / "candidate_review_clips"
            audio_dir.mkdir(parents=True)
            clip_path = audio_dir / "candidate_review_clip_000000.wav"
            write_pcm16_mono(clip_path, [0.0] * 16000, 16000)
            manifest = [
                {
                    "id": "candidate_review_clip_000000",
                    "audio_path": "artifacts/candidate_review_clips/candidate_review_clip_000000.wav",
                    "alignment_text": "hello",
                    "duration_sec": 1.0,
                    "buffer_id": "buffer_000000",
                    "word_ids": [],
                    "review_reason_codes": [],
                }
            ]
            (artifacts / "candidate_review_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            fake_metrics = {
                "ctc_mean_score": 0.91,
                "ctc_min_window_score": 0.88,
                "ctc_min_token_score": 0.75,
                "unaligned_token_count": 0,
                "weak_span_count": 0,
                "segment_confidence": 0.9,
                "transcript_match_score": 91.0,
                "bucket": "accepted",
            }
            out_dir = run_root / "ctc-out"
            with patch(
                "speechcraft_dataset.analyze_ctc_transcript_qc.load_ctc_model",
                return_value=CtcModelBundle(model=object(), processor=object(), char_list=["H", "E", "L", "O", "|"], device="cpu"),
            ), patch(
                "speechcraft_dataset.analyze_ctc_transcript_qc.score_clip",
                return_value=fake_metrics,
            ):
                summary = analyze_ctc_transcript_qc(
                    run_root,
                    out_dir,
                    export_worst=1,
                    export_best=1,
                )

            self.assertEqual(summary["clip_count"], 1)
            self.assertEqual(summary["scored_count"], 1)
            self.assertTrue((out_dir / "ctc_transcript_qc.json").exists())
            self.assertTrue((out_dir / "ctc_transcript_qc_summary.json").exists())
            self.assertTrue((out_dir / "ctc_transcript_qc_by_score.csv").exists())
            self.assertTrue((out_dir / "worst_clips").exists())
            self.assertTrue((out_dir / "best_clips").exists())

    def test_run_transcript_qc_writes_phase2_artifacts(self) -> None:
        from speechcraft_dataset.analyze_ctc_transcript_qc import CtcModelBundle

        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            audio_dir = artifacts / "candidate_review_clips"
            audio_dir.mkdir(parents=True)
            clip_path = audio_dir / "candidate_review_clip_000000.wav"
            write_pcm16_mono(clip_path, [0.0] * 16000, 16000)
            manifest = [
                {
                    "id": "candidate_review_clip_000000",
                    "audio_path": "artifacts/candidate_review_clips/candidate_review_clip_000000.wav",
                    "alignment_text": "hello",
                    "duration_sec": 1.0,
                    "buffer_id": "buffer_000000",
                    "word_ids": [],
                    "review_reason_codes": [],
                }
            ]
            (artifacts / "candidate_review_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            fake_metrics = {
                "transcript_score_method": TRANSCRIPT_SCORE_METHOD,
                "ctc_mean_score": 0.91,
                "ctc_min_span_score": 0.88,
                "ctc_min_window_score": 0.88,
                "ctc_min_token_score": 0.75,
                "unaligned_token_count": 0,
                "weak_span_count": 1,
                "weak_spans": [{"start_sec": 0.1, "end_sec": 0.4, "text": "HELLO", "score": 0.88}],
                "segment_confidence": 0.9,
                "transcript_match_score": 88.0,
                "bucket": "accepted",
                "bucket_hint": "pass",
                "reason_codes": [],
            }
            with patch(
                "speechcraft_dataset.analyze_ctc_transcript_qc.load_ctc_model",
                return_value=CtcModelBundle(model=object(), processor=object(), char_list=["H", "E", "L", "O", "|"], device="cpu"),
            ), patch(
                "speechcraft_dataset.analyze_ctc_transcript_qc.score_clip",
                return_value=fake_metrics,
            ):
                summary = run_transcript_qc(run_root, {})

            artifact = json.loads((artifacts / "transcript_qc.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["stage"], "transcript_qc")
            self.assertEqual(artifact["schema_version"], 1)
            self.assertEqual(artifact["score_method"], TRANSCRIPT_SCORE_METHOD)
            self.assertEqual(artifact["clips"][0]["transcript_match_score"], 88.0)
            self.assertEqual(artifact["clips"][0]["ctc_min_span_score"], 0.88)
            self.assertEqual(artifact["clips"][0]["transcript_score_method"], TRANSCRIPT_SCORE_METHOD)
            self.assertEqual(artifact["clips"][0]["bucket_hint"], "pass")
            self.assertTrue((artifacts / "transcript_qc_summary.json").exists())


if __name__ == "__main__":
    unittest.main()
