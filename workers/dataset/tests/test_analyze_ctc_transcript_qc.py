from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from speechcraft_dataset.analyze_ctc_transcript_qc import (
    normalize_verifier_text,
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


if __name__ == "__main__":
    unittest.main()
