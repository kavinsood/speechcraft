from __future__ import annotations

import json
import tempfile
import unittest
import wave
from pathlib import Path
from unittest.mock import patch

from speechcraft_dataset.run import main


def write_silent_wav(path: Path, *, sample_rate: int = 16000, duration_sec: float = 0.1) -> None:
    frames = int(sample_rate * duration_sec)
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(b"\x00\x00" * frames)


def _stage_summary(stage: str) -> dict:
    return {"stage": stage, "config_hash": "sha256:test"}


class StopAfterBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self._temp_path = Path(self._temp_dir.name)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _run_with_stop_after(
        self,
        stop_after: str,
        *,
        safecut_side_effect=None,
        assembly_side_effect=None,
        transcript_qc_side_effect=None,
        speaker_purity_side_effect=None,
        export_side_effect=None,
    ):
        source = self._temp_path / "source.wav"
        run_root = self._temp_path / "run"
        write_silent_wav(source)

        safecut = patch(
            "speechcraft_dataset.run.generate_safe_cutpoint_diagnostics",
            side_effect=safecut_side_effect or (lambda root, config: _stage_summary("safe_cutpoints")),
        )
        assembly = patch(
            "speechcraft_dataset.run.assemble_candidate_review_clips",
            side_effect=assembly_side_effect or (lambda root, config: _stage_summary("candidate_review_clips")),
        )
        native_export = patch(
            "speechcraft_dataset.run.export_native_candidate_clips",
            side_effect=export_side_effect or (lambda root, config: _stage_summary("native_export")),
        )

        with (
            patch("speechcraft_dataset.run.run_prepare_sources", return_value=_stage_summary("source_audio")),
            patch("speechcraft_dataset.run.run_audio_variants", return_value=_stage_summary("audio_variants")),
            patch("speechcraft_dataset.run.run_silero_vad", return_value=_stage_summary("vad")),
            patch("speechcraft_dataset.run.run_diarization", return_value=_stage_summary("diarization")),
            patch("speechcraft_dataset.run.run_processing_buffers", return_value=_stage_summary("buffers")),
            patch("speechcraft_dataset.run.build_asr_queue", return_value=_stage_summary("asr_queue")),
            patch("speechcraft_dataset.run.run_asr", return_value=_stage_summary("asr")),
            patch("speechcraft_dataset.run.normalize_transcripts", return_value=_stage_summary("normalization")),
            patch("speechcraft_dataset.run.run_mfa_alignment", return_value=_stage_summary("mfa")),
            patch("speechcraft_dataset.run.run_alignment_qc", return_value=_stage_summary("alignment_qc")),
            patch(
                "speechcraft_dataset.run.run_transcript_qc_stage",
                side_effect=transcript_qc_side_effect or (lambda root, config: _stage_summary("transcript_qc")),
            ),
            patch(
                "speechcraft_dataset.run.run_speaker_purity_stage",
                side_effect=speaker_purity_side_effect or (lambda root, config: _stage_summary("speaker_purity")),
            ),
            safecut as safecut_mock,
            assembly as assembly_mock,
            native_export as export_mock,
        ):
            exit_code = main(
                [
                    "--run-root",
                    str(run_root),
                    "--source-wav",
                    str(source),
                    "--single-speaker",
                    "--stop-after",
                    stop_after,
                ]
            )
        return run_root, exit_code, safecut_mock, assembly_mock, export_mock

    def test_stop_after_alignment_qc_does_not_run_slicer_or_export_stages(self) -> None:
        run_root, exit_code, safecut, assembly, export = self._run_with_stop_after("alignment_qc")
        self.assertEqual(exit_code, 0)
        artifacts = run_root / "artifacts"
        self.assertFalse((artifacts / "safe_cutpoints.jsonl").exists())
        self.assertFalse((artifacts / "candidate_review_manifest.json").exists())
        self.assertFalse((artifacts / "export_manifest.json").exists())
        safecut.assert_not_called()
        assembly.assert_not_called()
        export.assert_not_called()
        status = json.loads((run_root / "status.json").read_text(encoding="utf-8"))
        self.assertTrue(status["ok"])
        self.assertEqual(status["stage"], "alignment_qc")
        self.assertEqual(status["summary"]["stage"], "alignment_qc")

    def test_stop_after_diarization_stops_before_processing_buffers(self) -> None:
        run_root, exit_code, safecut, assembly, export = self._run_with_stop_after("diarization")
        self.assertEqual(exit_code, 0)
        self.assertFalse((run_root / "artifacts" / "processing_buffers.json").exists())
        safecut.assert_not_called()
        assembly.assert_not_called()
        export.assert_not_called()
        status = json.loads((run_root / "status.json").read_text(encoding="utf-8"))
        self.assertTrue(status["ok"])
        self.assertEqual(status["stage"], "diarization")
        self.assertEqual(status["summary"]["stage"], "diarization")

    def test_stop_after_safe_cutpoints_skips_candidate_and_export(self) -> None:
        def _write_safecut(root, config):
            artifacts = root / "artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            (artifacts / "safe_cutpoints.jsonl").write_text("", encoding="utf-8")
            (artifacts / "safe_cutpoint_summary.json").write_text("{}", encoding="utf-8")
            return _stage_summary("safe_cutpoints")

        run_root, exit_code, safecut, assembly, export = self._run_with_stop_after(
            "safe_cutpoints",
            safecut_side_effect=_write_safecut,
        )
        self.assertEqual(exit_code, 0)
        self.assertTrue((run_root / "artifacts" / "safe_cutpoints.jsonl").exists())
        self.assertFalse((run_root / "artifacts" / "candidate_review_manifest.json").exists())
        safecut.assert_called_once()
        assembly.assert_not_called()
        export.assert_not_called()
        status = json.loads((run_root / "status.json").read_text(encoding="utf-8"))
        self.assertEqual(status["stage"], "safe_cutpoints")
        self.assertEqual(status["summary"]["stage"], "safe_cutpoints")

    def test_stop_after_candidate_review_clips_skips_native_export(self) -> None:
        def _write_safecut(root, config):
            artifacts = root / "artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            (artifacts / "safe_cutpoints.jsonl").write_text("", encoding="utf-8")
            (artifacts / "safe_cutpoint_summary.json").write_text("{}", encoding="utf-8")
            return _stage_summary("safe_cutpoints")

        def _write_clips(root, config):
            artifacts = root / "artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            (artifacts / "candidate_review_manifest.json").write_text("[]", encoding="utf-8")
            (artifacts / "candidate_review_summary.json").write_text("{}", encoding="utf-8")
            (artifacts / "candidate_review_rejected.json").write_text("[]", encoding="utf-8")
            return _stage_summary("candidate_review_clips")

        run_root, exit_code, safecut, assembly, export = self._run_with_stop_after(
            "candidate_review_clips",
            safecut_side_effect=_write_safecut,
            assembly_side_effect=_write_clips,
        )
        self.assertEqual(exit_code, 0)
        self.assertTrue((run_root / "artifacts" / "candidate_review_manifest.json").exists())
        self.assertFalse((run_root / "artifacts" / "export_manifest.json").exists())
        assembly.assert_called_once()
        export.assert_not_called()
        status = json.loads((run_root / "status.json").read_text(encoding="utf-8"))
        self.assertEqual(status["stage"], "candidate_review_clips")
        self.assertEqual(status["summary"]["stage"], "candidate_review_clips")

    def test_stop_after_native_export_writes_export_manifest(self) -> None:
        def _write_safecut(root, config):
            artifacts = root / "artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            (artifacts / "safe_cutpoints.jsonl").write_text("", encoding="utf-8")
            (artifacts / "safe_cutpoint_summary.json").write_text("{}", encoding="utf-8")
            return _stage_summary("safe_cutpoints")

        def _write_clips(root, config):
            artifacts = root / "artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            (artifacts / "candidate_review_manifest.json").write_text("[]", encoding="utf-8")
            (artifacts / "candidate_review_summary.json").write_text("{}", encoding="utf-8")
            (artifacts / "candidate_review_rejected.json").write_text("[]", encoding="utf-8")
            return _stage_summary("candidate_review_clips")

        def _write_export(root, config):
            artifacts = root / "artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            (artifacts / "export_manifest.json").write_text("[]", encoding="utf-8")
            (artifacts / "export_summary.json").write_text("{}", encoding="utf-8")
            return _stage_summary("native_export")

        run_root, exit_code, safecut, assembly, export = self._run_with_stop_after(
            "native_export",
            safecut_side_effect=_write_safecut,
            assembly_side_effect=_write_clips,
            export_side_effect=_write_export,
        )
        self.assertEqual(exit_code, 0)
        self.assertTrue((run_root / "artifacts" / "export_manifest.json").exists())
        safecut.assert_called_once()
        assembly.assert_called_once()
        export.assert_called_once()
        status = json.loads((run_root / "status.json").read_text(encoding="utf-8"))
        self.assertEqual(status["stage"], "native_export")
        self.assertEqual(status["summary"]["stage"], "native_export")

    def test_stop_after_speaker_purity_skips_native_export(self) -> None:
        def _write_safecut(root, config):
            artifacts = root / "artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            (artifacts / "safe_cutpoints.jsonl").write_text("", encoding="utf-8")
            (artifacts / "safe_cutpoint_summary.json").write_text("{}", encoding="utf-8")
            return _stage_summary("safe_cutpoints")

        def _write_clips(root, config):
            artifacts = root / "artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            (artifacts / "candidate_review_manifest.json").write_text("[]", encoding="utf-8")
            (artifacts / "candidate_review_summary.json").write_text("{}", encoding="utf-8")
            (artifacts / "candidate_review_rejected.json").write_text("[]", encoding="utf-8")
            return _stage_summary("candidate_review_clips")

        run_root, exit_code, safecut, assembly, export = self._run_with_stop_after(
            "speaker_purity",
            safecut_side_effect=_write_safecut,
            assembly_side_effect=_write_clips,
        )
        self.assertEqual(exit_code, 0)
        self.assertFalse((run_root / "artifacts" / "export_manifest.json").exists())
        safecut.assert_called_once()
        assembly.assert_called_once()
        export.assert_not_called()
        status = json.loads((run_root / "status.json").read_text(encoding="utf-8"))
        self.assertEqual(status["stage"], "speaker_purity")
        self.assertEqual(status["summary"]["stage"], "speaker_purity")
