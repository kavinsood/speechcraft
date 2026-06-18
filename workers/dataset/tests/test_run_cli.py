from __future__ import annotations

import json
import importlib.util
import sys
import tempfile
import types
import unittest
import wave
from pathlib import Path
from unittest.mock import patch

from speechcraft_dataset.alignment_qc import run_alignment_qc
from speechcraft_dataset.analyze_vad_mfa_gaps import analyze_vad_mfa_gaps
from speechcraft_dataset.assembly import assemble_candidate_review_clips
from speechcraft_dataset.asr import run_asr
from speechcraft_dataset.buffers import run_processing_buffers
from speechcraft_dataset.export import export_native_candidate_clips
from speechcraft_dataset.generate_qc_scores import main as generate_qc_scores_main
from speechcraft_dataset.mfa import parse_mfa_textgrids, run_mfa_command
from speechcraft_dataset.models import check_asr_model
from speechcraft_dataset.rerun_slicer import main as rerun_slicer_main
from speechcraft_dataset.run import main
from speechcraft_dataset.safecut import generate_safe_cutpoint_diagnostics


HAS_WORKER_AUDIO_DEPS = bool(importlib.util.find_spec("numpy") and importlib.util.find_spec("soundfile"))


def write_silent_wav(path: Path, *, sample_rate: int = 16000, duration_sec: float = 0.1) -> None:
    frames = int(sample_rate * duration_sec)
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(b"\x00\x00" * frames)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_minimal_native_export_fixture(
    run_root: Path,
    temp_dir: Path,
    candidates: list[dict],
    *,
    dataset_qc: dict | None = None,
) -> Path:
    artifacts = run_root / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    source = temp_dir / "source_48k.wav"
    write_silent_wav(source, sample_rate=48000, duration_sec=5.0)
    (artifacts / "source_audio_manifest.json").write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "source_audio_id": "source_audio_0000",
                        "path": str(source),
                        "sample_rate": 48000,
                        "num_channels": 1,
                        "sample_width_bytes": 2,
                        "num_samples": 240000,
                        "duration_sec": 5.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (artifacts / "audio_variants_manifest.json").write_text(
        json.dumps(
            {
                "variants": [
                    {
                        "source_audio_id": "source_audio_0000",
                        "kind": "analysis_audio",
                        "source_sample_rate": 48000,
                        "analysis_sample_rate": 16000,
                        "source_num_samples": 240000,
                        "analysis_num_samples": 80000,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (artifacts / "candidate_review_manifest.json").write_text(json.dumps(candidates), encoding="utf-8")
    if dataset_qc is not None:
        (artifacts / "dataset_qc.json").write_text(json.dumps(dataset_qc), encoding="utf-8")
    return artifacts


class DatasetWorkerRunCliTests(unittest.TestCase):
    def test_slicer_rerun_runs_only_safecut_and_candidate_assembly(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw) / "run"
            run_root.mkdir()
            config_path = run_root / "input_config.json"
            config_path.write_text(json.dumps({"cutpoint_min_gap_ms": 40}), encoding="utf-8")

            with (
                patch(
                    "speechcraft_dataset.rerun_slicer.generate_safe_cutpoint_diagnostics",
                    return_value={"accepted_cutpoints": 3},
                ) as safecut,
                patch(
                    "speechcraft_dataset.rerun_slicer.assemble_candidate_review_clips",
                    return_value={"candidate_review_clips": 2},
                ) as assembly,
                patch(
                    "speechcraft_dataset.rerun_slicer.run_transcript_qc_stage",
                    return_value={"clip_count": 2, "scored_count": 2},
                ) as transcript_qc,
                patch(
                    "speechcraft_dataset.rerun_slicer.run_speaker_purity_stage",
                    return_value={"clip_count": 2, "scored_count": 2},
                ) as speaker_purity,
            ):
                exit_code = rerun_slicer_main(
                    ["--run-root", str(run_root), "--config", str(config_path)]
                )

            self.assertEqual(exit_code, 0)
            safecut.assert_called_once()
            assembly.assert_called_once()
            transcript_qc.assert_called_once()
            speaker_purity.assert_called_once()
            status = read_json(run_root / "status.json")
            self.assertTrue(status["ok"])
            self.assertEqual(status["stage"], "speaker_purity")
            self.assertEqual(status["summary"]["clip_count"], 2)
            self.assertTrue(read_json(run_root / "config.json")["config_hash"])

    def test_slicer_rerun_keeps_candidate_clips_when_qc_artifact_generation_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw) / "run"
            run_root.mkdir()
            config_path = run_root / "input_config.json"
            config_path.write_text(json.dumps({"cutpoint_min_gap_ms": 40}), encoding="utf-8")

            with (
                patch(
                    "speechcraft_dataset.rerun_slicer.generate_safe_cutpoint_diagnostics",
                    return_value={"accepted_cutpoints": 3},
                ),
                patch(
                    "speechcraft_dataset.rerun_slicer.assemble_candidate_review_clips",
                    return_value={"candidate_review_clips": 2},
                ),
                patch(
                    "speechcraft_dataset.rerun_slicer.run_transcript_qc_stage",
                    return_value={"clip_count": 2, "scored_count": 2},
                ),
                patch(
                    "speechcraft_dataset.rerun_slicer.run_speaker_purity_stage",
                    side_effect=RuntimeError("speaker model unavailable"),
                ),
            ):
                exit_code = rerun_slicer_main(
                    ["--run-root", str(run_root), "--config", str(config_path)]
                )

            self.assertEqual(exit_code, 0)
            status = read_json(run_root / "status.json")
            self.assertTrue(status["ok"])
            self.assertEqual(status["stage"], "candidate_review_clips")
            self.assertEqual(status["summary"]["candidate_review_clips"], 2)
            self.assertEqual(status["reason_codes"], ["qc_artifacts_failed"])

    def test_slicer_rerun_clears_stale_downstream_qc_artifacts_before_regeneration(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw) / "run"
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True)
            config_path = run_root / "input_config.json"
            config_path.write_text(json.dumps({"cutpoint_min_gap_ms": 40}), encoding="utf-8")
            (artifacts / "speaker_purity.json").write_text("{}", encoding="utf-8")
            (artifacts / "dataset_qc.json").write_text("{}", encoding="utf-8")

            with (
                patch(
                    "speechcraft_dataset.rerun_slicer.generate_safe_cutpoint_diagnostics",
                    return_value={"accepted_cutpoints": 3},
                ),
                patch(
                    "speechcraft_dataset.rerun_slicer.assemble_candidate_review_clips",
                    return_value={"candidate_review_clips": 2},
                ),
                patch(
                    "speechcraft_dataset.rerun_slicer.run_transcript_qc_stage",
                    return_value={"clip_count": 2, "scored_count": 2},
                ),
                patch(
                    "speechcraft_dataset.rerun_slicer.run_speaker_purity_stage",
                    side_effect=RuntimeError("speaker model unavailable"),
                ),
            ):
                exit_code = rerun_slicer_main(
                    ["--run-root", str(run_root), "--config", str(config_path)]
                )

            self.assertEqual(exit_code, 0)
            self.assertFalse((artifacts / "speaker_purity.json").exists())
            self.assertFalse((artifacts / "dataset_qc.json").exists())

    def test_generate_qc_scores_clears_stale_speaker_artifact_when_speaker_stage_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw) / "run"
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True)
            config_path = run_root / "config.json"
            config_path.write_text(json.dumps({}), encoding="utf-8")
            (artifacts / "candidate_review_manifest.json").write_text("[]", encoding="utf-8")
            (artifacts / "speaker_selection.json").write_text(json.dumps({"target_speaker_id": "speaker_0"}), encoding="utf-8")
            (artifacts / "speaker_regions.jsonl").write_text("", encoding="utf-8")
            (artifacts / "audio_variants_manifest.json").write_text(json.dumps({"variants": []}), encoding="utf-8")
            (artifacts / "speaker_purity.json").write_text("{}", encoding="utf-8")

            with (
                patch(
                    "speechcraft_dataset.generate_qc_scores.run_transcript_qc_stage",
                    return_value={"clip_count": 0, "scored_count": 0},
                ),
                patch(
                    "speechcraft_dataset.generate_qc_scores.run_speaker_purity_stage",
                    side_effect=RuntimeError("speaker model unavailable"),
                ),
            ):
                exit_code = generate_qc_scores_main(
                    ["--run-root", str(run_root), "--config", str(config_path)]
                )

            self.assertEqual(exit_code, 1)
            self.assertFalse((artifacts / "speaker_purity.json").exists())
            status = read_json(run_root / "status.json")
            self.assertFalse(status["ok"])
            self.assertEqual(status["reason_codes"], ["dataset_qc_score_generation_failed"])

    def test_generate_qc_scores_refuses_finalized_qc_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw) / "run"
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True)
            config_path = run_root / "config.json"
            config_path.write_text(json.dumps({}), encoding="utf-8")
            (artifacts / "candidate_review_manifest.json").write_text("[]", encoding="utf-8")
            (artifacts / "speaker_selection.json").write_text(json.dumps({"target_speaker_id": "speaker_0"}), encoding="utf-8")
            (artifacts / "speaker_regions.jsonl").write_text("", encoding="utf-8")
            (artifacts / "audio_variants_manifest.json").write_text(json.dumps({"variants": []}), encoding="utf-8")
            (artifacts / "dataset_qc.json").write_text("{}", encoding="utf-8")

            exit_code = generate_qc_scores_main(
                ["--run-root", str(run_root), "--config", str(config_path)]
            )

            self.assertEqual(exit_code, 1)
            self.assertTrue((artifacts / "dataset_qc.json").exists())
            status = read_json(run_root / "status.json")
            self.assertFalse(status["ok"])
            self.assertIn("dataset_qc_already_finalized", status["error"])

    def test_generate_qc_scores_clears_export_and_dataset_qc_artifacts_with_force(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw) / "run"
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True)
            config_path = run_root / "config.json"
            config_path.write_text(json.dumps({}), encoding="utf-8")
            (artifacts / "candidate_review_manifest.json").write_text("[]", encoding="utf-8")
            (artifacts / "speaker_selection.json").write_text(json.dumps({"target_speaker_id": "speaker_0"}), encoding="utf-8")
            (artifacts / "speaker_regions.jsonl").write_text("", encoding="utf-8")
            (artifacts / "audio_variants_manifest.json").write_text(json.dumps({"variants": []}), encoding="utf-8")
            (artifacts / "dataset_qc.json").write_text("{}", encoding="utf-8")
            (artifacts / "export_manifest.json").write_text("[]", encoding="utf-8")
            (artifacts / "export_audit.json").write_text("[]", encoding="utf-8")
            (artifacts / "export_summary.json").write_text("{}", encoding="utf-8")
            (artifacts / "native_export_clips").mkdir()

            with (
                patch(
                    "speechcraft_dataset.generate_qc_scores.run_transcript_qc_stage",
                    return_value={"clip_count": 0, "scored_count": 0},
                ),
                patch(
                    "speechcraft_dataset.generate_qc_scores.run_speaker_purity_stage",
                    return_value={"clip_count": 0, "scored_count": 0},
                ),
            ):
                exit_code = generate_qc_scores_main(
                    ["--run-root", str(run_root), "--config", str(config_path), "--force"]
                )

            self.assertEqual(exit_code, 0)
            self.assertFalse((artifacts / "dataset_qc.json").exists())
            self.assertFalse((artifacts / "export_manifest.json").exists())
            self.assertFalse((artifacts / "export_audit.json").exists())
            self.assertFalse((artifacts / "export_summary.json").exists())
            self.assertFalse((artifacts / "native_export_clips").exists())

    def test_single_speaker_run_writes_source_artifacts_status_and_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            source = temp_dir / "source.wav"
            run_root = temp_dir / "run"
            write_silent_wav(source)

            exit_code = main(
                [
                    "--run-root",
                    str(run_root),
                    "--source-wav",
                    str(source),
                    "--single-speaker",
                    "--stop-after",
                    "source_audio",
                ]
            )

            self.assertEqual(exit_code, 0)
            status = read_json(run_root / "status.json")
            summary = read_json(run_root / "artifacts" / "source_audio_summary.json")
            manifest = read_json(run_root / "artifacts" / "source_audio_manifest.json")
            config = read_json(run_root / "config.json")

            self.assertTrue(status["ok"])
            self.assertEqual(status["stage"], "source_audio")
            self.assertEqual(summary["source_count"], 1)
            self.assertEqual(manifest["sources"][0]["sample_rate"], 16000)
            self.assertEqual(config["mode"], "single_speaker")
            self.assertTrue((run_root / "runtime_versions.json").exists())
            self.assertIn("dataset worker started", (run_root / "logs" / "dataset_worker.log").read_text())
            self.assertTrue(config["config_hash"].startswith("sha256:"))

    def test_run_accepts_multiple_source_wavs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            source_a = temp_dir / "a.wav"
            source_b = temp_dir / "b.wav"
            run_root = temp_dir / "run"
            write_silent_wav(source_a, sample_rate=16000, duration_sec=0.1)
            write_silent_wav(source_b, sample_rate=48000, duration_sec=0.2)

            exit_code = main(
                [
                    "--run-root",
                    str(run_root),
                    "--source-wav",
                    str(source_a),
                    "--source-wav",
                    str(source_b),
                    "--single-speaker",
                    "--stop-after",
                    "source_audio",
                ]
            )

            self.assertEqual(exit_code, 0)
            summary = read_json(run_root / "artifacts" / "source_audio_summary.json")
            self.assertEqual(summary["source_count"], 2)
            self.assertEqual(summary["sample_rates"], [16000, 48000])
            self.assertAlmostEqual(summary["total_duration_sec"], 0.3, places=3)

    def test_missing_source_writes_failure_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            run_root = temp_dir / "run"

            exit_code = main(["--run-root", str(run_root), "--source-wav", str(temp_dir / "missing.wav")])

            self.assertEqual(exit_code, 1)
            status = read_json(run_root / "status.json")
            self.assertFalse(status["ok"])
            self.assertEqual(status["reason_codes"], ["dataset_worker_failed"])
            self.assertIn("Source WAV not found", status["error"])

    def test_audio_variants_stage_writes_mono_analysis_audio(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            source = temp_dir / "source.wav"
            run_root = temp_dir / "run"
            write_silent_wav(source, sample_rate=48000, duration_sec=0.1)

            exit_code = main(
                [
                    "--run-root",
                    str(run_root),
                    "--source-wav",
                    str(source),
                    "--single-speaker",
                    "--stop-after",
                    "audio_variants",
                ]
            )

            self.assertEqual(exit_code, 0)
            status = read_json(run_root / "status.json")
            manifest = read_json(run_root / "artifacts" / "audio_variants_manifest.json")
            summary = read_json(run_root / "artifacts" / "audio_variants_summary.json")
            analysis_path = run_root / manifest["variants"][0]["path"]

            self.assertEqual(status["stage"], "audio_variants")
            self.assertEqual(summary["analysis_sample_rate"], 16000)
            self.assertTrue(summary["all_mono"])
            self.assertTrue(analysis_path.exists())
            variant = manifest["variants"][0]
            self.assertEqual(variant["source_sample_rate"], 48000)
            self.assertEqual(variant["analysis_sample_rate"], 16000)
            self.assertEqual(variant["source_start_sample"], 0)
            self.assertEqual(variant["analysis_start_sample"], 0)
            self.assertEqual(variant["recipe"]["channel_mode"], "mono_average")
            self.assertIn("source_audio", variant["input_artifact_hashes"])
            with wave.open(str(analysis_path), "rb") as handle:
                self.assertEqual(handle.getframerate(), 16000)
                self.assertEqual(handle.getnchannels(), 1)

    def test_vad_stage_writes_segments_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            source = temp_dir / "source.wav"
            run_root = temp_dir / "run"
            write_silent_wav(source, sample_rate=16000, duration_sec=0.2)

            fake_summary = {
                "backend": "silero_vad",
                "segment_count": 1,
                "source_count": 1,
            }
            with patch("speechcraft_dataset.run.run_silero_vad", return_value=fake_summary) as vad:
                exit_code = main(
                    [
                        "--run-root",
                        str(run_root),
                        "--source-wav",
                        str(source),
                        "--single-speaker",
                        "--stop-after",
                        "vad",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(read_json(run_root / "status.json")["stage"], "vad")
            self.assertEqual(read_json(run_root / "status.json")["summary"], fake_summary)
            self.assertTrue((run_root / "artifacts" / "audio_variants_manifest.json").exists())
            vad.assert_called_once()

    def test_missing_vad_dependency_writes_stage_specific_reason(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            source = temp_dir / "source.wav"
            run_root = temp_dir / "run"
            write_silent_wav(source, sample_rate=16000, duration_sec=0.2)

            with patch(
                "speechcraft_dataset.run.run_silero_vad",
                side_effect=RuntimeError("Silero VAD dependencies are unavailable: ModuleNotFoundError"),
            ):
                exit_code = main(
                    [
                        "--run-root",
                        str(run_root),
                        "--source-wav",
                        str(source),
                        "--single-speaker",
                        "--stop-after",
                        "vad",
                    ]
                )

            self.assertEqual(exit_code, 1)
            status = read_json(run_root / "status.json")
            self.assertEqual(status["stage"], "vad")
            self.assertEqual(status["reason_codes"], ["missing_silero_vad_dependency"])

    @unittest.skipUnless(HAS_WORKER_AUDIO_DEPS, "requires worker audio deps")
    def test_buffers_stage_writes_padded_processing_buffers(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            source = temp_dir / "source.wav"
            run_root = temp_dir / "run"
            write_silent_wav(source, sample_rate=16000, duration_sec=1.0)

            def fake_vad(fake_run_root: Path, _config: dict) -> dict:
                (fake_run_root / "artifacts" / "vad_segments.jsonl").write_text(
                    json.dumps(
                        {
                            "id": "source_audio_0000_vad_000000",
                            "source_audio_id": "source_audio_0000",
                            "analysis_start_sample": 1600,
                            "analysis_end_sample": 12800,
                            "analysis_start_sec": 0.1,
                            "analysis_end_sec": 0.8,
                            "start_sample": 1600,
                            "end_sample": 12800,
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
                (fake_run_root / "artifacts" / "vad_summary.json").write_text(
                    json.dumps({"segment_count": 1}),
                    encoding="utf-8",
                )
                return {"segment_count": 1}

            with patch("speechcraft_dataset.run.run_silero_vad", side_effect=fake_vad):
                exit_code = main(
                    [
                        "--run-root",
                        str(run_root),
                        "--source-wav",
                        str(source),
                        "--single-speaker",
                        "--stop-after",
                        "buffers",
                    ]
                )

            self.assertEqual(exit_code, 0)
            status = read_json(run_root / "status.json")
            buffers = read_json(run_root / "artifacts" / "processing_buffers.json")
            summary = read_json(run_root / "artifacts" / "processing_buffer_summary.json")
            selection = read_json(run_root / "artifacts" / "speaker_selection.json")
            buffer = buffers[0]

            self.assertEqual(status["stage"], "buffers")
            self.assertEqual(summary["buffer_count"], 1)
            self.assertEqual(selection["target_speaker_id"], "speaker_0")
            self.assertTrue(selection["selected"])
            self.assertEqual(buffer["trusted_start_sample"], 1600)
            self.assertEqual(buffer["trusted_end_sample"], 12800)
            self.assertEqual(buffer["source_start_sample"], 0)
            self.assertEqual(buffer["trusted_local_start_sample"], 1600)
            self.assertTrue((run_root / buffer["audio_path"]).exists())
            self.assertIn("input_artifact_hashes", summary)
            self.assertIn("output_hashes", summary)

    def test_multi_speaker_run_stops_after_diarization_until_speaker_is_selected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            source = temp_dir / "source.wav"
            run_root = temp_dir / "run"
            write_silent_wav(source, sample_rate=16000, duration_sec=1.0)

            fake_diarization_summary = {
                "stage": "diarization",
                "speaker_count": 2,
                "speaker_ids": ["speaker_0", "speaker_1"],
                "reason_codes": ["speaker_selection_required"],
            }
            with (
                patch("speechcraft_dataset.run.run_silero_vad", return_value={"segment_count": 2}),
                patch("speechcraft_dataset.run.run_diarization", return_value=fake_diarization_summary),
                patch("speechcraft_dataset.run.run_processing_buffers") as buffers,
            ):
                exit_code = main(
                    [
                        "--run-root",
                        str(run_root),
                        "--source-wav",
                        str(source),
                        "--stop-after",
                        "alignment_qc",
                    ]
                )

            self.assertEqual(exit_code, 0)
            status = read_json(run_root / "status.json")
            self.assertEqual(status["stage"], "diarization")
            self.assertEqual(status["reason_codes"], ["speaker_selection_required"])
            buffers.assert_not_called()

    @unittest.skipUnless(HAS_WORKER_AUDIO_DEPS, "requires worker audio deps")
    def test_buffers_stage_skips_sources_when_vad_detects_no_speech(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            source = temp_dir / "source.wav"
            run_root = temp_dir / "run"
            write_silent_wav(source, sample_rate=16000, duration_sec=1.0)

            def fake_vad(fake_run_root: Path, _config: dict) -> dict:
                (fake_run_root / "artifacts" / "vad_segments.jsonl").write_text("", encoding="utf-8")
                (fake_run_root / "artifacts" / "vad_summary.json").write_text(
                    json.dumps({"segment_count": 0}),
                    encoding="utf-8",
                )
                return {"segment_count": 0}

            with patch("speechcraft_dataset.run.run_silero_vad", side_effect=fake_vad):
                exit_code = main(
                    [
                        "--run-root",
                        str(run_root),
                        "--source-wav",
                        str(source),
                        "--single-speaker",
                        "--stop-after",
                        "buffers",
                    ]
                )

            self.assertEqual(exit_code, 0)
            buffers = read_json(run_root / "artifacts" / "processing_buffers.json")
            summary = read_json(run_root / "artifacts" / "processing_buffer_summary.json")
            self.assertEqual(buffers, [])
            self.assertEqual(summary["buffer_count"], 0)
            self.assertEqual(summary["skipped_sources"][0]["reason_codes"], ["no_speech_detected"])

    @unittest.skipUnless(HAS_WORKER_AUDIO_DEPS, "requires worker audio deps")
    def test_buffers_stage_rejects_wrong_analysis_sample_rate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            run_root = temp_dir / "run"
            analysis = run_root / "audio" / "analysis" / "bad.wav"
            analysis.parent.mkdir(parents=True)
            write_silent_wav(analysis, sample_rate=8000, duration_sec=1.0)
            artifacts = run_root / "artifacts"
            artifacts.mkdir()
            (artifacts / "audio_variants_manifest.json").write_text(
                json.dumps(
                    {
                        "variants": [
                            {
                                "source_audio_id": "source_audio_0000",
                                "path": "audio/analysis/bad.wav",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (artifacts / "vad_segments.jsonl").write_text(
                json.dumps(
                    {
                        "source_audio_id": "source_audio_0000",
                        "analysis_start_sample": 0,
                        "analysis_end_sample": 8000,
                        "analysis_start_sec": 0.0,
                        "analysis_end_sec": 1.0,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "speaker_regions.jsonl").write_text(
                json.dumps(
                    {
                        "id": "speaker_0-a",
                        "source_audio_id": "source_audio_0000",
                        "speaker_id": "speaker_0",
                        "start_sample": 0,
                        "end_sample": 8000,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "speaker_selection.json").write_text(
                json.dumps(
                    {
                        "mode": "single_speaker",
                        "selected": True,
                        "target_speaker_id": "speaker_0",
                        "source": "auto",
                        "available_speaker_ids": ["speaker_0"],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(ValueError):
                run_processing_buffers(run_root, {"analysis_sample_rate": 16000})

    @unittest.skipUnless(HAS_WORKER_AUDIO_DEPS, "requires worker audio deps")
    def test_buffers_stage_uses_selected_speaker_regions_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            run_root = temp_dir / "run"
            analysis = run_root / "audio" / "analysis" / "source_audio_0000.mono16000.wav"
            analysis.parent.mkdir(parents=True)
            write_silent_wav(analysis, sample_rate=16000, duration_sec=2.0)
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True)
            (artifacts / "audio_variants_manifest.json").write_text(
                json.dumps(
                    {
                        "variants": [
                            {
                                "source_audio_id": "source_audio_0000",
                                "path": "audio/analysis/source_audio_0000.mono16000.wav",
                                "analysis_sample_rate": 16000,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (artifacts / "vad_segments.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({"source_audio_id": "source_audio_0000", "analysis_start_sample": 1000, "analysis_end_sample": 6000, "analysis_start_sec": 0.0625, "analysis_end_sec": 0.375}),
                        json.dumps({"source_audio_id": "source_audio_0000", "analysis_start_sample": 9000, "analysis_end_sample": 14000, "analysis_start_sec": 0.5625, "analysis_end_sec": 0.875}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "speaker_regions.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({"id": "speaker_0-a", "source_audio_id": "source_audio_0000", "speaker_id": "speaker_0", "start_sample": 1000, "end_sample": 6000}),
                        json.dumps({"id": "speaker_1-a", "source_audio_id": "source_audio_0000", "speaker_id": "speaker_1", "start_sample": 9000, "end_sample": 14000}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "speaker_selection.json").write_text(
                json.dumps(
                    {
                        "mode": "diarization",
                        "selected": True,
                        "target_speaker_id": "speaker_1",
                        "source": "user",
                        "available_speaker_ids": ["speaker_0", "speaker_1"],
                    }
                ),
                encoding="utf-8",
            )

            summary = run_processing_buffers(run_root, {"analysis_sample_rate": 16000, "mode": "diarization"})
            buffers = read_json(artifacts / "processing_buffers.json")

            self.assertEqual(summary["buffer_count"], 1)
            self.assertEqual(buffers[0]["target_speaker_id"], "speaker_1")
            self.assertEqual(buffers[0]["trusted_start_sample"], 9000)
            self.assertEqual(buffers[0]["trusted_end_sample"], 14000)

    def test_asr_empty_queue_does_not_import_or_load_whisper(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir()
            (artifacts / "asr_mfa_queue.json").write_text("[]", encoding="utf-8")

            with patch("builtins.__import__") as import_hook:
                summary = run_asr(run_root, {"config_hash": "sha256:test", "faster_whisper_model": "small.en"})

            self.assertEqual(summary["buffer_count"], 0)
            self.assertEqual(summary["reason_codes"], ["empty_asr_queue"])
            self.assertEqual(read_json(artifacts / "transcripts.json"), [])
            imported_names = [call.args[0] for call in import_hook.call_args_list]
            self.assertNotIn("faster_whisper", imported_names)
            self.assertNotIn("torch", imported_names)

    def test_asr_transcribe_uses_config_options_without_initial_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            audio_dir = artifacts / "asr_mfa_queue"
            audio_dir.mkdir(parents=True)
            write_silent_wav(audio_dir / "buffer_000000.wav", sample_rate=16000, duration_sec=6.0)
            model_dir = run_root / "models" / "fake-whisper"
            model_dir.mkdir(parents=True)
            (artifacts / "asr_mfa_queue.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "queue_audio_path": "artifacts/asr_mfa_queue/buffer_000000.wav",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            captured_kwargs: dict = {}

            class FakeModel:
                def __init__(self, *_args, **_kwargs) -> None:
                    pass

                def transcribe(self, _path: str, **kwargs):
                    captured_kwargs.update(kwargs)
                    segment = types.SimpleNamespace(
                        id=1,
                        seek=0,
                        start=0.0,
                        end=1.0,
                        text="hello",
                        avg_logprob=-0.1,
                        compression_ratio=1.0,
                        no_speech_prob=0.0,
                    )
                    info = types.SimpleNamespace(
                        language="en",
                        language_probability=1.0,
                        duration=1.0,
                        duration_after_vad=None,
                    )
                    return [segment], info

            fake_torch = types.ModuleType("torch")
            fake_torch.cuda = types.SimpleNamespace(is_available=lambda: False, empty_cache=lambda: None)
            fake_fw = types.ModuleType("faster_whisper")
            fake_fw.WhisperModel = FakeModel
            old_torch = sys.modules.get("torch")
            old_fw = sys.modules.get("faster_whisper")
            sys.modules["torch"] = fake_torch
            sys.modules["faster_whisper"] = fake_fw
            try:
                summary = run_asr(
                    run_root,
                    {
                        "config_hash": "sha256:test",
                        "faster_whisper_model": "fake",
                        "faster_whisper_model_path": str(model_dir),
                        "faster_whisper_device": "cpu",
                        "faster_whisper_compute_type": "int8",
                        "faster_whisper_beam_size": 3,
                        "asr_language": "en",
                        "asr_task": "transcribe",
                        "asr_vad_filter": True,
                        "asr_word_timestamps": False,
                        "asr_condition_on_previous_text": False,
                    },
                )
            finally:
                if old_torch is None:
                    sys.modules.pop("torch", None)
                else:
                    sys.modules["torch"] = old_torch
                if old_fw is None:
                    sys.modules.pop("faster_whisper", None)
                else:
                    sys.modules["faster_whisper"] = old_fw

            self.assertEqual(summary["buffer_count"], 1)
            self.assertEqual(captured_kwargs["language"], "en")
            self.assertEqual(captured_kwargs["task"], "transcribe")
            self.assertTrue(captured_kwargs["vad_filter"])
            self.assertFalse(captured_kwargs["condition_on_previous_text"])
            self.assertNotIn("initial_prompt", captured_kwargs)

    def test_asr_uses_explicit_local_model_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            model_dir = run_root / "local-model"
            model_dir.mkdir()
            artifacts = run_root / "artifacts"
            audio_dir = artifacts / "asr_mfa_queue"
            audio_dir.mkdir(parents=True)
            write_silent_wav(audio_dir / "buffer_000000.wav", sample_rate=16000, duration_sec=6.0)
            (artifacts / "asr_mfa_queue.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "queue_audio_path": "artifacts/asr_mfa_queue/buffer_000000.wav",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            captured_init: dict = {}

            class FakeModel:
                def __init__(self, model_ref: str, **kwargs) -> None:
                    captured_init["model_ref"] = model_ref
                    captured_init.update(kwargs)

                def transcribe(self, _path: str, **_kwargs):
                    segment = types.SimpleNamespace(
                        id=1,
                        seek=0,
                        start=0.0,
                        end=1.0,
                        text="hello",
                        avg_logprob=-0.1,
                        compression_ratio=1.0,
                        no_speech_prob=0.0,
                    )
                    info = types.SimpleNamespace(
                        language="en",
                        language_probability=1.0,
                        duration=1.0,
                        duration_after_vad=None,
                    )
                    return [segment], info

            fake_torch = types.ModuleType("torch")
            fake_torch.cuda = types.SimpleNamespace(is_available=lambda: False, empty_cache=lambda: None)
            fake_fw = types.ModuleType("faster_whisper")
            fake_fw.WhisperModel = FakeModel
            old_torch = sys.modules.get("torch")
            old_fw = sys.modules.get("faster_whisper")
            sys.modules["torch"] = fake_torch
            sys.modules["faster_whisper"] = fake_fw
            try:
                summary = run_asr(
                    run_root,
                    {
                        "config_hash": "sha256:test",
                        "faster_whisper_model": "small.en",
                        "faster_whisper_model_path": str(model_dir),
                        "faster_whisper_device": "cpu",
                        "faster_whisper_compute_type": "int8",
                    },
                )
            finally:
                if old_torch is None:
                    sys.modules.pop("torch", None)
                else:
                    sys.modules["torch"] = old_torch
                if old_fw is None:
                    sys.modules.pop("faster_whisper", None)
                else:
                    sys.modules["faster_whisper"] = old_fw

            self.assertEqual(summary["buffer_count"], 1)
            self.assertEqual(captured_init["model_ref"], str(model_dir.resolve()))
            transcript = read_json(artifacts / "transcripts.json")[0]
            self.assertEqual(transcript["asr_model_reference"], str(model_dir.resolve()))

    def test_asr_requested_model_failure_raises_without_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            audio_dir = artifacts / "asr_mfa_queue"
            audio_dir.mkdir(parents=True)
            write_silent_wav(audio_dir / "buffer_000000.wav", sample_rate=16000, duration_sec=6.0)
            (artifacts / "asr_mfa_queue.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "queue_audio_path": "artifacts/asr_mfa_queue/buffer_000000.wav",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            init_refs: list[str] = []

            class FakeModel:
                def __init__(self, model_ref: str, **_kwargs) -> None:
                    init_refs.append(model_ref)
                    raise RuntimeError("missing model.bin")

                def transcribe(self, _path: str, **_kwargs):
                    raise AssertionError("transcribe should not be reached when model load fails")

            def fake_resolve_asr_model_reference(model_config: dict) -> str:
                model = str(model_config.get("faster_whisper_model"))
                self.assertEqual(model, "medium.en")
                return "broken-medium-ref"

            fake_torch = types.ModuleType("torch")
            fake_torch.cuda = types.SimpleNamespace(is_available=lambda: False, empty_cache=lambda: None)
            fake_fw = types.ModuleType("faster_whisper")
            fake_fw.WhisperModel = FakeModel
            old_torch = sys.modules.get("torch")
            old_fw = sys.modules.get("faster_whisper")
            sys.modules["torch"] = fake_torch
            sys.modules["faster_whisper"] = fake_fw
            try:
                with patch("speechcraft_dataset.asr.resolve_asr_model_reference", side_effect=fake_resolve_asr_model_reference):
                    with self.assertRaises(RuntimeError) as context:
                        run_asr(
                            run_root,
                            {
                                "config_hash": "sha256:test",
                                "faster_whisper_model": "medium.en",
                                "faster_whisper_device": "cpu",
                                "faster_whisper_compute_type": "int8",
                            },
                        )
            finally:
                if old_torch is None:
                    sys.modules.pop("torch", None)
                else:
                    sys.modules["torch"] = old_torch
                if old_fw is None:
                    sys.modules.pop("faster_whisper", None)
                else:
                    sys.modules["faster_whisper"] = old_fw

            self.assertIn("requested='medium.en'", str(context.exception))
            self.assertIn("resolved='broken-medium-ref'", str(context.exception))
            self.assertIn("missing model.bin", str(context.exception))
            self.assertEqual(init_refs, ["broken-medium-ref"])
            self.assertFalse((artifacts / "transcripts.json").exists())

    def test_asr_model_check_reports_missing_local_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            missing = Path(temp_dir_raw) / "missing-model"
            result = check_asr_model(model="tiny.en", model_path=str(missing), local_only=True)

            self.assertFalse(result["ok"])
            self.assertEqual(result["source"], "local_path")
            self.assertIn("does not exist", result["error"])

    def test_asr_model_check_reports_incomplete_snapshot_before_load(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            snapshot = Path(temp_dir_raw) / "broken-medium"
            snapshot.mkdir(parents=True, exist_ok=True)
            (snapshot / "config.json").write_text("{}", encoding="utf-8")

            result = check_asr_model(model="medium.en", model_path=str(snapshot), local_only=True, load_model=True)

            self.assertFalse(result["ok"])
            self.assertEqual(result["source"], "local_path")
            self.assertIn("snapshot_check", result)
            self.assertEqual(result["snapshot_check"]["missing_files"], ["model.bin"])
            self.assertEqual(result["error"], "ASR model snapshot is incomplete: missing model.bin")
            self.assertFalse(result["load_checked"])

    @unittest.skipUnless(HAS_WORKER_AUDIO_DEPS, "requires worker audio deps")
    def test_mfa_stage_handles_empty_corpus_without_calling_mfa(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            source = temp_dir / "source.wav"
            run_root = temp_dir / "run"
            write_silent_wav(source, sample_rate=16000, duration_sec=1.0)

            def fake_vad(fake_run_root: Path, _config: dict) -> dict:
                (fake_run_root / "artifacts" / "vad_segments.jsonl").write_text("", encoding="utf-8")
                return {"segment_count": 0}

            with patch("speechcraft_dataset.run.run_silero_vad", side_effect=fake_vad):
                exit_code = main(
                    [
                        "--run-root",
                        str(run_root),
                        "--source-wav",
                        str(source),
                        "--single-speaker",
                        "--stop-after",
                        "mfa",
                    ]
                )

            self.assertEqual(exit_code, 0)
            status = read_json(run_root / "status.json")
            summary = read_json(run_root / "artifacts" / "aligned_words_summary.json")
            mfa_summary = read_json(run_root / "artifacts" / "mfa_summary.json")
            self.assertEqual(status["stage"], "mfa")
            self.assertEqual(mfa_summary["status"], "skipped")
            self.assertEqual(mfa_summary["reason_codes"], ["empty_mfa_corpus"])
            self.assertEqual(summary["aligned_word_count"], 0)
            self.assertEqual(summary["reason_codes"], ["empty_mfa_corpus"])
            self.assertEqual((run_root / "artifacts" / "aligned_words.jsonl").read_text(encoding="utf-8"), "")

    def test_mfa_missing_binary_is_stage_specific_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            (run_root / "artifacts" / "mfa_corpus").mkdir(parents=True)

            with patch("speechcraft_dataset.mfa.shutil.which", return_value=None):
                with self.assertRaises(RuntimeError) as error:
                    run_mfa_command(run_root, {})

            self.assertIn("MFA binary not configured", str(error.exception))

    def test_mfa_bad_configured_binary_path_is_clean_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            (run_root / "artifacts" / "mfa_corpus").mkdir(parents=True)
            missing = Path(temp_dir_raw) / "missing-mfa"

            with self.assertRaises(RuntimeError) as error:
                run_mfa_command(run_root, {"mfa_bin": str(missing)})

            self.assertIn("MFA binary path does not exist", str(error.exception))

    def test_mfa_command_uses_single_speaker_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            (artifacts / "mfa_corpus").mkdir(parents=True)
            (artifacts / "mfa_corpus_manifest.json").write_text("{}", encoding="utf-8")
            captured_command: list[str] = []

            def fake_run(command, **_kwargs):
                captured_command.extend(command)
                return types.SimpleNamespace(returncode=0, stdout="", stderr="")

            with patch("speechcraft_dataset.mfa.shutil.which", return_value="/usr/bin/mfa"):
                with patch("speechcraft_dataset.mfa.subprocess.run", side_effect=fake_run):
                    run_mfa_command(run_root, {})

            self.assertIn("--single_speaker", captured_command)

    def test_mfa_command_prepends_binary_directory_to_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            bin_dir = Path(temp_dir_raw) / "mfa-env" / "bin"
            bin_dir.mkdir(parents=True)
            mfa_path = bin_dir / "mfa"
            mfa_path.write_text("", encoding="utf-8")
            (run_root / "artifacts" / "mfa_corpus").mkdir(parents=True)
            (run_root / "artifacts" / "mfa_corpus_manifest.json").write_text("{}", encoding="utf-8")
            captured_env: dict[str, str] = {}

            def fake_run(_command, **_kwargs):
                captured_env.update(_kwargs.get("env") or {})
                return types.SimpleNamespace(returncode=0, stdout="", stderr="")

            with patch("speechcraft_dataset.mfa.subprocess.run", side_effect=fake_run):
                run_mfa_command(run_root, {"mfa_bin": str(mfa_path)})

            self.assertTrue(captured_env["PATH"].startswith(str(bin_dir.resolve())))

    def test_mfa_textgrid_parse_attaches_token_hazards_and_oovs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            output_dir = artifacts / "mfa_output"
            output_dir.mkdir(parents=True)
            (output_dir / "buffer_000000.TextGrid").write_text("fake", encoding="utf-8")
            (artifacts / "oovs_found.txt").write_text("world 2\n", encoding="utf-8")
            (artifacts / "asr_mfa_queue.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "source_start_sample": 16000,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            (artifacts / "normalized_transcripts.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "alignment_tokens": [
                                {
                                    "alignment": "hello",
                                    "raw_token_id": "raw-0000",
                                    "raw": "Hello",
                                    "contains_numeric": False,
                                    "contains_danger_symbol": False,
                                    "danger_symbols": [],
                                    "reason_codes": [],
                                },
                                {
                                    "alignment": "world",
                                    "raw_token_id": "raw-0001",
                                    "raw": "$20",
                                    "contains_numeric": True,
                                    "contains_danger_symbol": True,
                                    "danger_symbols": ["$"],
                                    "reason_codes": ["contains_currency_symbol", "contains_numeric_token"],
                                },
                            ],
                        }
                    ]
                ),
                encoding="utf-8",
            )

            class FakeTextgridModule:
                @staticmethod
                def openTextgrid(_path: str, includeEmptyIntervals: bool = False):
                    entries = [
                        types.SimpleNamespace(start=0.1, end=0.4, label="hello"),
                        types.SimpleNamespace(start=0.5, end=0.8, label="world"),
                    ]
                    tier = types.SimpleNamespace(entries=entries)
                    return types.SimpleNamespace(tierNames=["words"], getTier=lambda _name: tier)

            fake_praatio = types.ModuleType("praatio")
            fake_praatio.textgrid = FakeTextgridModule
            old_praatio = sys.modules.get("praatio")
            old_textgrid = sys.modules.get("praatio.textgrid")
            sys.modules["praatio"] = fake_praatio
            sys.modules["praatio.textgrid"] = FakeTextgridModule
            try:
                summary = parse_mfa_textgrids(run_root, {"analysis_sample_rate": 16000})
            finally:
                if old_praatio is None:
                    sys.modules.pop("praatio", None)
                else:
                    sys.modules["praatio"] = old_praatio
                if old_textgrid is None:
                    sys.modules.pop("praatio.textgrid", None)
                else:
                    sys.modules["praatio.textgrid"] = old_textgrid

            rows = [
                json.loads(line)
                for line in (artifacts / "aligned_words.jsonl").read_text(encoding="utf-8").splitlines()
                if line
            ]
            self.assertEqual(summary["aligned_word_count"], 2)
            self.assertEqual(summary["expected_textgrid_count"], 1)
            self.assertEqual(summary["missing_textgrid_count"], 0)
            self.assertEqual(summary["unexpected_textgrid_count"], 0)
            self.assertEqual(rows[0]["source_start_sample"], 17600)
            self.assertTrue(rows[1]["is_oov"])
            self.assertTrue(rows[1]["contains_numeric"])
            self.assertTrue(rows[1]["contains_danger_symbol"])
            self.assertIn("contains_oov", rows[1]["review_reason_codes"])
            self.assertIn("contains_currency_symbol", rows[1]["review_reason_codes"])

    def test_mfa_textgrid_summary_reports_missing_and_unexpected_textgrids(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            output_dir = artifacts / "mfa_output"
            output_dir.mkdir(parents=True)
            (output_dir / "unexpected.TextGrid").write_text("fake", encoding="utf-8")
            (artifacts / "asr_mfa_queue.json").write_text(
                json.dumps([{"buffer_id": "buffer_000000", "source_start_sample": 0}]),
                encoding="utf-8",
            )
            (artifacts / "normalized_transcripts.json").write_text(
                json.dumps([{"buffer_id": "buffer_000000", "alignment_tokens": []}]),
                encoding="utf-8",
            )

            class FakeTextgridModule:
                @staticmethod
                def openTextgrid(_path: str, includeEmptyIntervals: bool = False):
                    tier = types.SimpleNamespace(entries=[])
                    return types.SimpleNamespace(tierNames=["words"], getTier=lambda _name: tier)

            fake_praatio = types.ModuleType("praatio")
            fake_praatio.textgrid = FakeTextgridModule
            old_praatio = sys.modules.get("praatio")
            old_textgrid = sys.modules.get("praatio.textgrid")
            sys.modules["praatio"] = fake_praatio
            sys.modules["praatio.textgrid"] = FakeTextgridModule
            try:
                summary = parse_mfa_textgrids(run_root, {})
            finally:
                if old_praatio is None:
                    sys.modules.pop("praatio", None)
                else:
                    sys.modules["praatio"] = old_praatio
                if old_textgrid is None:
                    sys.modules.pop("praatio.textgrid", None)
                else:
                    sys.modules["praatio.textgrid"] = old_textgrid

            self.assertEqual(summary["missing_textgrid_buffer_ids"], ["buffer_000000"])
            self.assertEqual(summary["unexpected_textgrid_stems"], ["unexpected"])
            self.assertIn("missing_textgrids", summary["reason_codes"])
            self.assertIn("unexpected_textgrids", summary["reason_codes"])

    def test_alignment_qc_summarizes_clean_aligned_words(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir()
            (artifacts / "asr_mfa_queue.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "source_start_sample": 0,
                            "source_end_sample": 160000,
                            "trusted_start_sample": 8000,
                            "trusted_end_sample": 152000,
                            "duration_sec": 10.0,
                            "split_strategy": "whole_region",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            (artifacts / "normalized_transcripts.json").write_text(
                json.dumps([{"buffer_id": "buffer_000000", "tokens": ["hello", "world"]}]),
                encoding="utf-8",
            )
            (artifacts / "transcripts.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "segments": [
                                {
                                    "avg_logprob": -0.1,
                                    "no_speech_prob": 0.01,
                                    "compression_ratio": 1.1,
                                }
                            ],
                        }
                    ]
                ),
                encoding="utf-8",
            )
            (artifacts / "aligned_words.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "buffer_id": "buffer_000000",
                                "source_start_sample": 16000,
                                "source_end_sample": 24000,
                                "contains_numeric": False,
                                "contains_danger_symbol": False,
                                "is_oov": False,
                                "alignment_token_word_mismatch": False,
                            }
                        ),
                        json.dumps(
                            {
                                "buffer_id": "buffer_000000",
                                "source_start_sample": 32000,
                                "source_end_sample": 40000,
                                "contains_numeric": False,
                                "contains_danger_symbol": False,
                                "is_oov": False,
                                "alignment_token_word_mismatch": False,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            summary = run_alignment_qc(run_root, {"analysis_sample_rate": 16000, "config_hash": "sha256:test"})
            by_buffer = read_json(artifacts / "alignment_qc_by_buffer.json")[0]

            self.assertEqual(summary["buffer_count"], 1)
            self.assertEqual(summary["aligned_word_count"], 2)
            self.assertEqual(summary["buffers_with_alignment_mismatch"], 0)
            self.assertEqual(summary["buffers_with_words_outside_buffer"], 0)
            self.assertEqual(summary["unexpected_aligned_word_buffer_count"], 0)
            self.assertEqual(summary["buffers_with_fatal_reasons"], 0)
            self.assertFalse(by_buffer["automatic_cutpoints_disabled"])
            self.assertEqual(by_buffer["fatal_reason_codes"], [])
            self.assertEqual(by_buffer["reason_codes"], [])
            self.assertEqual(by_buffer["asr_min_avg_logprob"], -0.1)

    def test_alignment_qc_flags_bad_alignment_conditions(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir()
            (artifacts / "asr_mfa_queue.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "source_start_sample": 0,
                            "source_end_sample": 160000,
                            "trusted_start_sample": 8000,
                            "trusted_end_sample": 152000,
                            "duration_sec": 10.0,
                            "split_strategy": "whole_region",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            (artifacts / "normalized_transcripts.json").write_text(
                json.dumps([{"buffer_id": "buffer_000000", "tokens": ["one", "two", "three"]}]),
                encoding="utf-8",
            )
            (artifacts / "transcripts.json").write_text(
                json.dumps([{"buffer_id": "buffer_000000", "segments": []}]),
                encoding="utf-8",
            )
            (artifacts / "aligned_words.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "buffer_id": "buffer_000000",
                                "source_start_sample": 1000,
                                "source_end_sample": 1100,
                                "contains_numeric": True,
                                "contains_danger_symbol": True,
                                "is_oov": True,
                                "alignment_token_word_mismatch": True,
                            }
                        ),
                        json.dumps(
                            {
                                "buffer_id": "buffer_000000",
                                "source_start_sample": 900,
                                "source_end_sample": 400000,
                                "contains_numeric": False,
                                "contains_danger_symbol": False,
                                "is_oov": False,
                                "alignment_token_word_mismatch": False,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            summary = run_alignment_qc(run_root, {"analysis_sample_rate": 16000, "config_hash": "sha256:test"})
            by_buffer = read_json(artifacts / "alignment_qc_by_buffer.json")[0]

            self.assertEqual(summary["buffers_with_alignment_mismatch"], 1)
            self.assertEqual(summary["buffers_with_words_outside_buffer"], 1)
            self.assertEqual(summary["buffers_with_words_outside_trusted_chunk"], 1)
            self.assertEqual(summary["buffers_with_absurdly_short_words"], 1)
            self.assertEqual(summary["buffers_with_absurdly_long_words"], 1)
            self.assertEqual(summary["buffers_with_backwards_word_order"], 1)
            self.assertEqual(summary["buffers_with_oovs"], 1)
            self.assertEqual(summary["buffers_with_fatal_reasons"], 1)
            self.assertEqual(summary["buffers_with_automatic_cutpoints_disabled"], 1)
            self.assertTrue(by_buffer["automatic_cutpoints_disabled"])
            self.assertIn("alignment_token_word_mismatch", by_buffer["fatal_reason_codes"])
            self.assertIn("word_is_oov", by_buffer["warning_reason_codes"])
            self.assertIn("alignment_token_word_mismatch", by_buffer["reason_codes"])
            self.assertIn("word_order_backwards", by_buffer["reason_codes"])

    def test_alignment_qc_accounts_for_unexpected_word_buffer_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir()
            (artifacts / "asr_mfa_queue.json").write_text("[]", encoding="utf-8")
            (artifacts / "normalized_transcripts.json").write_text("[]", encoding="utf-8")
            (artifacts / "transcripts.json").write_text("[]", encoding="utf-8")
            (artifacts / "aligned_words.jsonl").write_text(
                json.dumps(
                    {
                        "buffer_id": "stale_buffer",
                        "source_start_sample": 0,
                        "source_end_sample": 100,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            summary = run_alignment_qc(run_root, {"analysis_sample_rate": 16000, "config_hash": "sha256:test"})

            self.assertEqual(summary["buffer_count"], 0)
            self.assertEqual(summary["aligned_word_count"], 0)
            self.assertEqual(summary["unexpected_aligned_word_count"], 1)
            self.assertEqual(summary["unexpected_aligned_word_buffer_ids"], ["stale_buffer"])
            self.assertNotIn("alignment_qc_summary_json", summary["output_hashes"])

    def test_alignment_qc_treats_words_in_padded_context_outside_trusted_chunk_as_warning(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir()
            (artifacts / "asr_mfa_queue.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "source_start_sample": 1000,
                            "source_end_sample": 65000,
                            "trusted_start_sample": 9000,
                            "trusted_end_sample": 57000,
                            "duration_sec": 4.0,
                            "split_strategy": "whole_region",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            (artifacts / "normalized_transcripts.json").write_text(
                json.dumps([{"buffer_id": "buffer_000000", "tokens": ["hello", "world"]}]),
                encoding="utf-8",
            )
            (artifacts / "transcripts.json").write_text(
                json.dumps([{"buffer_id": "buffer_000000", "segments": []}]),
                encoding="utf-8",
            )
            (artifacts / "aligned_words.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "buffer_id": "buffer_000000",
                                "source_start_sample": 1000,
                                "source_end_sample": 8000,
                                "contains_numeric": False,
                                "contains_danger_symbol": False,
                                "is_oov": False,
                                "alignment_token_word_mismatch": False,
                            }
                        ),
                        json.dumps(
                            {
                                "buffer_id": "buffer_000000",
                                "source_start_sample": 12000,
                                "source_end_sample": 20000,
                                "contains_numeric": False,
                                "contains_danger_symbol": False,
                                "is_oov": False,
                                "alignment_token_word_mismatch": False,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            summary = run_alignment_qc(run_root, {"analysis_sample_rate": 16000, "config_hash": "sha256:test"})
            by_buffer = read_json(artifacts / "alignment_qc_by_buffer.json")[0]

            self.assertEqual(summary["buffers_with_words_outside_trusted_chunk"], 1)
            self.assertEqual(summary["buffers_with_fatal_reasons"], 0)
            self.assertEqual(summary["buffers_with_automatic_cutpoints_disabled"], 0)
            self.assertFalse(by_buffer["automatic_cutpoints_disabled"])
            self.assertNotIn("word_outside_trusted_chunk", by_buffer["fatal_reason_codes"])
            self.assertIn("word_outside_trusted_chunk", by_buffer["warning_reason_codes"])

    @unittest.skipUnless(HAS_WORKER_AUDIO_DEPS, "requires worker audio deps")
    def test_safecut_accepts_quiet_gap_with_integer_sample_cut(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            audio_dir = artifacts / "asr_mfa_queue"
            audio_dir.mkdir(parents=True)
            write_silent_wav(audio_dir / "buffer_000000.wav", sample_rate=16000, duration_sec=3.0)
            (artifacts / "asr_mfa_queue.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "queue_audio_path": "artifacts/asr_mfa_queue/buffer_000000.wav",
                            "source_start_sample": 0,
                            "trusted_local_start_sample": 0,
                            "trusted_local_end_sample": 48000,
                            "left_provisional_boundary": False,
                            "right_provisional_boundary": False,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            (artifacts / "aligned_words.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "id": "word-0",
                                "buffer_id": "buffer_000000",
                                "word": "hello",
                                "source_start_sample": 8000,
                                "source_end_sample": 16000,
                                "local_start_sample": 8000,
                                "local_end_sample": 16000,
                                "is_oov": False,
                                "contains_numeric": False,
                                "contains_danger_symbol": False,
                                "alignment_token_word_mismatch": False,
                            }
                        ),
                        json.dumps(
                            {
                                "id": "word-1",
                                "buffer_id": "buffer_000000",
                                "word": "world",
                                "source_start_sample": 24000,
                                "source_end_sample": 32000,
                                "local_start_sample": 24000,
                                "local_end_sample": 32000,
                                "is_oov": False,
                                "contains_numeric": False,
                                "contains_danger_symbol": False,
                                "alignment_token_word_mismatch": False,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "alignment_qc_by_buffer.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "fatal_reason_codes": [],
                            "warning_reason_codes": ["word_near_trusted_edge"],
                            "automatic_cutpoints_disabled": False,
                        }
                    ]
                ),
                encoding="utf-8",
            )

            summary = generate_safe_cutpoint_diagnostics(
                run_root,
                {
                    "analysis_sample_rate": 16000,
                    "config_hash": "sha256:test",
                    "cutpoint_left_word_edge_guard_ms": 30,
                    "cutpoint_min_gap_ms": 80,
                    "cutpoint_right_word_edge_guard_ms": 30,
                    "cutpoint_noise_margin_db": 6.0,
                },
            )
            accepted = [
                json.loads(line)
                for line in (artifacts / "safe_cutpoints.jsonl").read_text(encoding="utf-8").splitlines()
                if line
            ]

            self.assertEqual(summary["accepted_cutpoints"], 1)
            self.assertEqual(summary["rejected_cutpoint_candidates"], 0)
            self.assertIsInstance(accepted[0]["cut_local_sample"], int)
            self.assertEqual(accepted[0]["source_sample"], accepted[0]["cut_local_sample"])
            self.assertEqual(accepted[0]["buffer_warning_reason_codes"], ["word_near_trusted_edge"])
            self.assertEqual(accepted[0]["reason_codes"], [])

    @unittest.skipUnless(HAS_WORKER_AUDIO_DEPS, "requires worker audio deps")
    def test_safecut_rejects_buffer_disabled_by_alignment_qc(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            audio_dir = artifacts / "asr_mfa_queue"
            audio_dir.mkdir(parents=True)
            write_silent_wav(audio_dir / "buffer_000000.wav", sample_rate=16000, duration_sec=3.0)
            (artifacts / "asr_mfa_queue.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "queue_audio_path": "artifacts/asr_mfa_queue/buffer_000000.wav",
                            "source_start_sample": 0,
                            "trusted_local_start_sample": 0,
                            "trusted_local_end_sample": 48000,
                            "left_provisional_boundary": False,
                            "right_provisional_boundary": False,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            words = [
                {
                    "id": f"word-{index}",
                    "buffer_id": "buffer_000000",
                    "word": word,
                    "source_start_sample": start,
                    "source_end_sample": end,
                    "local_start_sample": start,
                    "local_end_sample": end,
                    "is_oov": False,
                    "contains_numeric": False,
                    "contains_danger_symbol": False,
                    "alignment_token_word_mismatch": False,
                }
                for index, (word, start, end) in enumerate([("hello", 8000, 16000), ("world", 24000, 32000)])
            ]
            (artifacts / "aligned_words.jsonl").write_text(
                "\n".join(json.dumps(row) for row in words) + "\n",
                encoding="utf-8",
            )
            (artifacts / "alignment_qc_by_buffer.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "fatal_reason_codes": ["alignment_token_word_mismatch"],
                            "automatic_cutpoints_disabled": True,
                        }
                    ]
                ),
                encoding="utf-8",
            )

            summary = generate_safe_cutpoint_diagnostics(run_root, {"analysis_sample_rate": 16000})
            rejected = [
                json.loads(line)
                for line in (artifacts / "rejected_cutpoint_candidates.jsonl").read_text(encoding="utf-8").splitlines()
                if line
            ]

            self.assertEqual(summary["accepted_cutpoints"], 0)
            self.assertEqual(summary["rejected_cutpoint_candidates"], 1)
            self.assertIn("buffer_automatic_cutpoints_disabled", rejected[0]["reason_codes"])
            self.assertIn("alignment_qc_fatal:alignment_token_word_mismatch", rejected[0]["reason_codes"])

    @unittest.skipUnless(HAS_WORKER_AUDIO_DEPS, "requires worker audio deps")
    def test_safecut_missing_alignment_qc_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            audio_dir = artifacts / "asr_mfa_queue"
            audio_dir.mkdir(parents=True)
            write_silent_wav(audio_dir / "buffer_000000.wav", sample_rate=16000, duration_sec=3.0)
            (artifacts / "asr_mfa_queue.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "queue_audio_path": "artifacts/asr_mfa_queue/buffer_000000.wav",
                            "source_start_sample": 0,
                            "trusted_local_start_sample": 0,
                            "trusted_local_end_sample": 48000,
                            "left_provisional_boundary": False,
                            "right_provisional_boundary": False,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            words = [
                {
                    "id": f"word-{index}",
                    "buffer_id": "buffer_000000",
                    "word": word,
                    "source_start_sample": start,
                    "source_end_sample": end,
                    "local_start_sample": start,
                    "local_end_sample": end,
                    "is_oov": False,
                    "contains_numeric": False,
                    "contains_danger_symbol": False,
                    "alignment_token_word_mismatch": False,
                }
                for index, (word, start, end) in enumerate([("hello", 8000, 16000), ("world", 24000, 32000)])
            ]
            (artifacts / "aligned_words.jsonl").write_text(
                "\n".join(json.dumps(row) for row in words) + "\n",
                encoding="utf-8",
            )
            (artifacts / "alignment_qc_by_buffer.json").write_text("[]", encoding="utf-8")

            summary = generate_safe_cutpoint_diagnostics(run_root, {"analysis_sample_rate": 16000})
            rejected = [
                json.loads(line)
                for line in (artifacts / "rejected_cutpoint_candidates.jsonl").read_text(encoding="utf-8").splitlines()
                if line
            ]

            self.assertEqual(summary["accepted_cutpoints"], 0)
            self.assertEqual(summary["rejected_cutpoint_candidates"], 1)
            self.assertEqual(summary["missing_alignment_qc_buffer_ids"], ["buffer_000000"])
            self.assertEqual(summary["missing_alignment_qc_buffer_count"], 1)
            self.assertEqual(summary["buffers_with_automatic_cutpoints_disabled"], 1)
            self.assertIn("buffer_automatic_cutpoints_disabled", rejected[0]["reason_codes"])
            self.assertIn("alignment_qc_fatal:missing_alignment_qc_for_buffer", rejected[0]["reason_codes"])

    @unittest.skipUnless(HAS_WORKER_AUDIO_DEPS, "requires worker audio deps")
    def test_safecut_reports_unexpected_alignment_qc_buffers(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir()
            (artifacts / "asr_mfa_queue.json").write_text("[]", encoding="utf-8")
            (artifacts / "aligned_words.jsonl").write_text("", encoding="utf-8")
            (artifacts / "alignment_qc_by_buffer.json").write_text(
                json.dumps([{"buffer_id": "stale_buffer", "fatal_reason_codes": [], "automatic_cutpoints_disabled": False}]),
                encoding="utf-8",
            )

            summary = generate_safe_cutpoint_diagnostics(run_root, {"analysis_sample_rate": 16000})

            self.assertEqual(summary["unexpected_alignment_qc_buffer_ids"], ["stale_buffer"])
            self.assertEqual(summary["unexpected_alignment_qc_buffer_count"], 1)

    @unittest.skipUnless(HAS_WORKER_AUDIO_DEPS, "requires worker audio deps")
    def test_candidate_review_assembly_writes_wav_manifest_and_safe_refs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            audio_dir = artifacts / "asr_mfa_queue"
            audio_dir.mkdir(parents=True)
            write_silent_wav(audio_dir / "buffer_000000.wav", sample_rate=16000, duration_sec=12.0)
            (artifacts / "asr_mfa_queue.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "source_audio_id": "source_audio_0000",
                            "queue_audio_path": "artifacts/asr_mfa_queue/buffer_000000.wav",
                            "source_start_sample": 16000,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            words = [
                {
                    "id": "word-0",
                    "buffer_id": "buffer_000000",
                    "word": "hello",
                    "source_start_sample": 32000,
                    "source_end_sample": 48000,
                    "raw_token_id": "raw-0",
                    "raw_token": "Hello,",
                    "contains_numeric": False,
                    "contains_danger_symbol": False,
                    "is_oov": False,
                    "review_reason_codes": [],
                },
                {
                    "id": "word-1",
                    "buffer_id": "buffer_000000",
                    "word": "twenty",
                    "source_start_sample": 64000,
                    "source_end_sample": 80000,
                    "raw_token_id": "raw-1",
                    "raw_token": "$20",
                    "contains_numeric": True,
                    "contains_danger_symbol": True,
                    "is_oov": False,
                    "review_reason_codes": ["contains_numeric_token", "contains_currency_symbol"],
                },
            ]
            (artifacts / "aligned_words.jsonl").write_text(
                "\n".join(json.dumps(row) for row in words) + "\n",
                encoding="utf-8",
            )
            cutpoints = [
                {
                    "id": "cut-0",
                    "buffer_id": "buffer_000000",
                    "cut_local_sample": 8000,
                    "source_sample": 24000,
                    "buffer_warning_reason_codes": [],
                },
                {
                    "id": "cut-1",
                    "buffer_id": "buffer_000000",
                    "cut_local_sample": 136000,
                    "source_sample": 152000,
                    "buffer_warning_reason_codes": ["word_near_trusted_edge"],
                },
            ]
            (artifacts / "safe_cutpoints.jsonl").write_text(
                "\n".join(json.dumps(row) for row in cutpoints) + "\n",
                encoding="utf-8",
            )
            (artifacts / "alignment_qc_by_buffer.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "fatal_reason_codes": [],
                            "warning_reason_codes": [],
                            "automatic_cutpoints_disabled": False,
                        }
                    ]
                ),
                encoding="utf-8",
            )

            summary = assemble_candidate_review_clips(run_root, {"analysis_sample_rate": 16000})
            manifest = read_json(artifacts / "candidate_review_manifest.json")
            clip = manifest[0]

            self.assertEqual(summary["candidate_review_clips"], 1)
            self.assertEqual(clip["start_cutpoint_ref"], "cut-0")
            self.assertEqual(clip["end_cutpoint_ref"], "cut-1")
            self.assertEqual(clip["duration_samples"], 128000)
            self.assertEqual(clip["duration_sec"], 8.0)
            self.assertEqual(clip["training_text"], "Hello, $20")
            self.assertTrue(clip["needs_review"])
            self.assertIn("clip_contains_symbol_hazard", clip["review_reason_codes"])
            self.assertIn("clip_contains_numeric_token", clip["review_reason_codes"])
            self.assertNotIn("word_near_trusted_edge", clip["review_reason_codes"])
            self.assertIn("word_near_trusted_edge", clip["buffer_warning_reason_codes"])
            clip_path = run_root / clip["audio_path"]
            self.assertTrue(clip_path.exists())

            cutpoints_by_id = {row["id"]: row for row in cutpoints}
            words_by_id = {row["id"]: row for row in words}
            start_cutpoint = cutpoints_by_id[clip["start_cutpoint_ref"]]
            end_cutpoint = cutpoints_by_id[clip["end_cutpoint_ref"]]
            self.assertEqual(clip["source_start_sample"], start_cutpoint["source_sample"])
            self.assertEqual(clip["source_end_sample"], end_cutpoint["source_sample"])
            self.assertGreaterEqual(clip["duration_sec"], 3.0)
            self.assertLessEqual(clip["duration_sec"], 15.0)
            for word_id in clip["word_ids"]:
                word = words_by_id[word_id]
                self.assertGreaterEqual(word["source_start_sample"], clip["source_start_sample"])
                self.assertLessEqual(word["source_end_sample"], clip["source_end_sample"])
            with wave.open(str(clip_path), "rb") as handle:
                self.assertEqual(handle.getnframes(), clip["duration_samples"])

    @unittest.skipUnless(HAS_WORKER_AUDIO_DEPS, "requires worker audio deps")
    def test_candidate_review_assembly_fails_closed_without_qc(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            run_root = Path(temp_dir_raw)
            artifacts = run_root / "artifacts"
            artifacts.mkdir()
            (artifacts / "asr_mfa_queue.json").write_text(
                json.dumps([{"buffer_id": "buffer_000000", "source_start_sample": 0}]),
                encoding="utf-8",
            )
            (artifacts / "aligned_words.jsonl").write_text("", encoding="utf-8")
            (artifacts / "safe_cutpoints.jsonl").write_text("", encoding="utf-8")
            (artifacts / "alignment_qc_by_buffer.json").write_text("[]", encoding="utf-8")

            summary = assemble_candidate_review_clips(run_root, {"analysis_sample_rate": 16000})
            rejected = read_json(artifacts / "candidate_review_rejected.json")

            self.assertEqual(summary["candidate_review_clips"], 0)
            self.assertEqual(rejected[0]["reason_codes"], ["missing_alignment_qc_for_buffer", "buffer_excluded_from_clip_assembly"])

    def test_native_export_maps_analysis_samples_to_original_rate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            run_root = temp_dir / "run"
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True)
            source = temp_dir / "source_48k.wav"
            write_silent_wav(source, sample_rate=48000, duration_sec=5.0)
            (artifacts / "source_audio_manifest.json").write_text(
                json.dumps(
                    {
                        "sources": [
                            {
                                "source_audio_id": "source_audio_0000",
                                "source_recording_id": "source",
                                "path": str(source),
                                "sample_rate": 48000,
                                "num_channels": 1,
                                "sample_width_bytes": 2,
                                "num_samples": 240000,
                                "duration_sec": 5.0,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (artifacts / "audio_variants_manifest.json").write_text(
                json.dumps(
                    {
                        "variants": [
                            {
                                "source_audio_id": "source_audio_0000",
                                "kind": "analysis_audio",
                                "source_sample_rate": 48000,
                                "analysis_sample_rate": 16000,
                                "source_num_samples": 240000,
                                "analysis_num_samples": 80000,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (artifacts / "candidate_review_manifest.json").write_text(
                json.dumps(
                    [
                        {
                            "id": "candidate_review_clip_000000",
                            "source_audio_id": "source_audio_0000",
                            "source_start_sample": 16000,
                            "source_end_sample": 48000,
                            "audio_path": "artifacts/candidate_review_clips/candidate_review_clip_000000.wav",
                            "training_text": "native rate please",
                            "alignment_text": "native rate please",
                            "status": "candidate_review",
                            "needs_review": False,
                            "review_reason_codes": [],
                            "start_cutpoint_ref": "cut-0",
                            "end_cutpoint_ref": "cut-1",
                            "word_ids": ["word-0"],
                        }
                    ]
                ),
                encoding="utf-8",
            )

            summary = export_native_candidate_clips(run_root, {"config_hash": "sha256:test"})
            manifest = read_json(artifacts / "export_manifest.json")
            exported = manifest[0]

            self.assertEqual(summary["exported_clip_count"], 1)
            self.assertEqual(summary["sample_rates"], [48000])
            self.assertEqual(exported["native_start_sample"], 48000)
            self.assertEqual(exported["native_end_sample"], 144000)
            self.assertEqual(exported["duration_samples"], 96000)
            self.assertEqual(exported["duration_sec"], 2.0)
            with wave.open(str(run_root / exported["audio_path"]), "rb") as handle:
                self.assertEqual(handle.getframerate(), 48000)
                self.assertEqual(handle.getnframes(), 96000)

    def test_export_native_falls_back_to_manifest_status_without_dataset_qc(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            run_root = temp_dir / "run"
            artifacts = write_minimal_native_export_fixture(
                run_root,
                temp_dir,
                [
                    {
                        "id": "candidate_review_clip_000000",
                        "source_audio_id": "source_audio_0000",
                        "source_start_sample": 16000,
                        "source_end_sample": 32000,
                        "status": "candidate_review",
                        "training_text": "fallback export",
                        "alignment_text": "fallback export",
                    },
                    {
                        "id": "candidate_review_clip_000001",
                        "source_audio_id": "source_audio_0000",
                        "source_start_sample": 32000,
                        "source_end_sample": 48000,
                        "status": "rejected",
                        "training_text": "do not export",
                        "alignment_text": "do not export",
                    },
                ],
            )

            summary = export_native_candidate_clips(run_root, {"config_hash": "sha256:test"})
            manifest = read_json(artifacts / "export_manifest.json")
            audit = read_json(artifacts / "export_audit.json")

            self.assertEqual(summary["qc_source"], "artifacts/candidate_review_manifest.json")
            self.assertEqual(summary["exported_clip_count"], 1)
            self.assertEqual(manifest[0]["id"], "candidate_review_clip_000000")
            self.assertEqual(audit[0]["candidate_id"], "candidate_review_clip_000001")
            self.assertEqual(audit[0]["reason_codes"], ["candidate_status_not_exportable"])

    def test_export_native_prefers_dataset_qc_statuses_over_manifest_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            run_root = temp_dir / "run"
            artifacts = write_minimal_native_export_fixture(
                run_root,
                temp_dir,
                [
                    {
                        "id": "candidate_review_clip_000000",
                        "source_audio_id": "source_audio_0000",
                        "source_start_sample": 16000,
                        "source_end_sample": 32000,
                        "status": "candidate_review",
                        "training_text": "manifest says keep",
                        "alignment_text": "manifest says keep",
                    },
                    {
                        "id": "candidate_review_clip_000001",
                        "source_audio_id": "source_audio_0000",
                        "source_start_sample": 32000,
                        "source_end_sample": 48000,
                        "status": "rejected",
                        "training_text": "manifest says reject",
                        "alignment_text": "manifest says reject",
                    },
                ],
                dataset_qc={
                    "schema_version": 1,
                    "stage": "dataset_qc",
                    "thresholds": {
                        "transcript_match_min": 85,
                        "speaker_check_min": 70,
                    },
                    "score_methods": {
                        "transcript_match": "min_meaningful_ctc_span",
                        "speaker_check": "min_valid_window_similarity",
                    },
                    "manual_overrides": [],
                    "clips": [
                        {
                            "clip_id": "candidate_review_clip_000000",
                            "status": "rejected",
                            "manual_override": None,
                        },
                        {
                            "clip_id": "candidate_review_clip_000001",
                            "status": "accepted",
                            "manual_override": None,
                        },
                    ],
                },
            )

            summary = export_native_candidate_clips(run_root, {"config_hash": "sha256:test"})
            manifest = read_json(artifacts / "export_manifest.json")
            audit = read_json(artifacts / "export_audit.json")

            self.assertEqual(summary["qc_source"], "artifacts/dataset_qc.json")
            self.assertEqual(summary["qc_thresholds"]["transcript_match_min"], 85)
            self.assertEqual(summary["qc_score_methods"]["speaker_check"], "min_valid_window_similarity")
            self.assertEqual(summary["manual_override_counts"], {"force_keep": 0, "force_reject": 0})
            self.assertIsNotNone(summary["input_artifact_hashes"]["dataset_qc_json"])
            self.assertEqual(summary["exported_clip_count"], 1)
            self.assertEqual(manifest[0]["id"], "candidate_review_clip_000001")
            self.assertEqual(manifest[0]["qc_source"], "artifacts/dataset_qc.json")
            self.assertEqual(manifest[0]["qc_status"], "accepted")
            self.assertEqual(audit[0]["candidate_id"], "candidate_review_clip_000000")
            self.assertEqual(audit[0]["reason_codes"], ["dataset_qc_status_not_accepted"])

    def test_export_native_uses_finalized_overrides_only_via_dataset_qc(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            run_root = temp_dir / "run"
            artifacts = write_minimal_native_export_fixture(
                run_root,
                temp_dir,
                [
                    {
                        "id": "candidate_review_clip_000000",
                        "source_audio_id": "source_audio_0000",
                        "source_start_sample": 16000,
                        "source_end_sample": 32000,
                        "status": "candidate_review",
                        "training_text": "force kept clip",
                        "alignment_text": "force kept clip",
                    },
                    {
                        "id": "candidate_review_clip_000001",
                        "source_audio_id": "source_audio_0000",
                        "source_start_sample": 32000,
                        "source_end_sample": 48000,
                        "status": "candidate_review",
                        "training_text": "force rejected clip",
                        "alignment_text": "force rejected clip",
                    },
                ],
                dataset_qc={
                    "schema_version": 1,
                    "stage": "dataset_qc",
                    "thresholds": {
                        "transcript_match_min": 85,
                        "speaker_check_min": 70,
                    },
                    "score_methods": {
                        "transcript_match": "min_meaningful_ctc_span",
                        "speaker_check": "min_valid_window_similarity",
                    },
                    "manual_overrides": [
                        {"clip_id": "candidate_review_clip_000000", "override": "force_keep"},
                        {"clip_id": "candidate_review_clip_000001", "override": "force_reject"},
                    ],
                    "clips": [
                        {
                            "clip_id": "candidate_review_clip_000000",
                            "status": "accepted",
                            "manual_override": "force_keep",
                        },
                        {
                            "clip_id": "candidate_review_clip_000001",
                            "status": "rejected",
                            "manual_override": "force_reject",
                        },
                    ],
                },
            )

            summary = export_native_candidate_clips(run_root, {"config_hash": "sha256:test"})
            manifest = read_json(artifacts / "export_manifest.json")
            audit = read_json(artifacts / "export_audit.json")

            self.assertEqual(summary["manual_override_counts"]["force_keep"], 1)
            self.assertEqual(summary["manual_override_counts"]["force_reject"], 1)
            self.assertEqual(manifest[0]["id"], "candidate_review_clip_000000")
            self.assertEqual(manifest[0]["manual_override"], "force_keep")
            self.assertEqual(audit[0]["candidate_id"], "candidate_review_clip_000001")
            self.assertEqual(audit[0]["manual_override"], "force_reject")

    def test_export_native_rejects_duplicate_dataset_qc_clip_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            run_root = temp_dir / "run"
            write_minimal_native_export_fixture(
                run_root,
                temp_dir,
                [
                    {
                        "id": "candidate_review_clip_000000",
                        "source_audio_id": "source_audio_0000",
                        "source_start_sample": 16000,
                        "source_end_sample": 32000,
                        "status": "candidate_review",
                    }
                ],
                dataset_qc={
                    "schema_version": 1,
                    "stage": "dataset_qc",
                    "thresholds": {"transcript_match_min": 85, "speaker_check_min": 70},
                    "score_methods": {
                        "transcript_match": "min_meaningful_ctc_span",
                        "speaker_check": "min_valid_window_similarity",
                    },
                    "manual_overrides": [],
                    "clips": [
                        {"clip_id": "candidate_review_clip_000000", "status": "accepted", "manual_override": None},
                        {"clip_id": "candidate_review_clip_000000", "status": "rejected", "manual_override": None},
                    ],
                },
            )

            with self.assertRaisesRegex(ValueError, "duplicate clip_id in dataset_qc.json"):
                export_native_candidate_clips(run_root, {"config_hash": "sha256:test"})

    def test_export_native_rejects_invalid_dataset_qc_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            run_root = temp_dir / "run"
            write_minimal_native_export_fixture(
                run_root,
                temp_dir,
                [
                    {
                        "id": "candidate_review_clip_000000",
                        "source_audio_id": "source_audio_0000",
                        "source_start_sample": 16000,
                        "source_end_sample": 32000,
                        "status": "candidate_review",
                    }
                ],
                dataset_qc={
                    "schema_version": 1,
                    "stage": "dataset_qc",
                    "thresholds": {"transcript_match_min": 85, "speaker_check_min": 70},
                    "score_methods": {
                        "transcript_match": "min_meaningful_ctc_span",
                        "speaker_check": "min_valid_window_similarity",
                    },
                    "manual_overrides": [],
                    "clips": [
                        {"clip_id": "candidate_review_clip_000000", "status": "maybe", "manual_override": None},
                    ],
                },
            )

            with self.assertRaisesRegex(ValueError, "invalid status"):
                export_native_candidate_clips(run_root, {"config_hash": "sha256:test"})

    @unittest.skipUnless(HAS_WORKER_AUDIO_DEPS, "requires worker audio deps")
    def test_asr_queue_rejects_short_buffers(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            source = temp_dir / "source.wav"
            run_root = temp_dir / "run"
            write_silent_wav(source, sample_rate=16000, duration_sec=1.0)

            def fake_vad(fake_run_root: Path, _config: dict) -> dict:
                (fake_run_root / "artifacts" / "vad_segments.jsonl").write_text(
                    json.dumps(
                        {
                            "source_audio_id": "source_audio_0000",
                            "analysis_start_sample": 1600,
                            "analysis_end_sample": 12800,
                            "analysis_start_sec": 0.1,
                            "analysis_end_sec": 0.8,
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
                return {"segment_count": 1}

            with patch("speechcraft_dataset.run.run_silero_vad", side_effect=fake_vad):
                exit_code = main(
                    [
                        "--run-root",
                        str(run_root),
                        "--source-wav",
                        str(source),
                        "--single-speaker",
                        "--stop-after",
                        "asr_queue",
                    ]
                )

            self.assertEqual(exit_code, 0)
            summary = read_json(run_root / "artifacts" / "asr_mfa_queue_summary.json")
            queue = read_json(run_root / "artifacts" / "asr_mfa_queue.json")
            rejected = read_json(run_root / "artifacts" / "rejected_buffers.json")
            self.assertEqual(summary["ready_buffers"], 0)
            self.assertEqual(summary["rejected_buffers"], 1)
            self.assertEqual(queue, [])
            self.assertEqual(rejected[0]["queue_reason_codes"], ["buffer_under_min_asr_mfa_sec"])

    @unittest.skipUnless(HAS_WORKER_AUDIO_DEPS, "requires worker audio deps")
    def test_normalization_preserves_raw_text_and_flags_hazards(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            source = temp_dir / "source.wav"
            run_root = temp_dir / "run"
            write_silent_wav(source, sample_rate=16000, duration_sec=6.0)

            def fake_vad(fake_run_root: Path, _config: dict) -> dict:
                (fake_run_root / "artifacts" / "vad_segments.jsonl").write_text(
                    json.dumps(
                        {
                            "source_audio_id": "source_audio_0000",
                            "analysis_start_sample": 0,
                            "analysis_end_sample": 96000,
                            "analysis_start_sec": 0.0,
                            "analysis_end_sec": 6.0,
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
                return {"segment_count": 1}

            fake_asr_summary = {"buffer_count": 1, "empty_transcripts": 0}

            def fake_asr(fake_run_root: Path, _config: dict) -> dict:
                (fake_run_root / "artifacts" / "transcripts.json").write_text(
                    json.dumps(
                        [
                            {
                                "buffer_id": "buffer_000000",
                                "audio_path": "artifacts/asr_mfa_queue/buffer_000000.wav",
                                "text": "I spent $20 and saved 50%.",
                                "segments": [],
                            }
                        ]
                    ),
                    encoding="utf-8",
                )
                return fake_asr_summary

            with patch("speechcraft_dataset.run.run_silero_vad", side_effect=fake_vad):
                with patch("speechcraft_dataset.run.run_asr", side_effect=fake_asr):
                    exit_code = main(
                        [
                            "--run-root",
                            str(run_root),
                            "--source-wav",
                            str(source),
                            "--single-speaker",
                            "--stop-after",
                            "normalization",
                        ]
                    )

            self.assertEqual(exit_code, 0)
            normalized = read_json(run_root / "artifacts" / "normalized_transcripts.json")[0]
            summary = read_json(run_root / "artifacts" / "normalization_summary.json")
            self.assertEqual(normalized["raw_asr_text"], "I spent $20 and saved 50%.")
            self.assertEqual(normalized["alignment_text"], "i spent 20 and saved 50")
            self.assertTrue(normalized["needs_review"])
            self.assertIn("$", normalized["symbols"])
            self.assertIn("%", normalized["symbols"])
            self.assertEqual(summary["buffers_with_symbol_hazards"], 1)
            self.assertEqual(summary["symbol_buffer_counts"]["$"], 1)

    @unittest.skipUnless(HAS_WORKER_AUDIO_DEPS, "requires worker audio deps")
    def test_gap_analysis_reports_vad_mfa_agreement(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            run_root = temp_dir / "run"
            artifacts = run_root / "artifacts"
            queue_dir = artifacts / "asr_mfa_queue"
            queue_dir.mkdir(parents=True)
            buffer_audio = queue_dir / "buffer_000000.wav"
            write_silent_wav(buffer_audio, duration_sec=0.35)

            (artifacts / "asr_mfa_queue.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "source_audio_id": "source_0",
                            "audio_path": "artifacts/buffers/buffer_000000.wav",
                            "queue_audio_path": "artifacts/asr_mfa_queue/buffer_000000.wav",
                            "source_start_sample": 0,
                            "source_end_sample": 5600,
                            "trusted_start_sample": 0,
                            "trusted_end_sample": 5600,
                            "trusted_local_start_sample": 0,
                            "trusted_local_end_sample": 5600,
                            "duration_sec": 0.35,
                            "sample_rate": 16000,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            (artifacts / "vad_segments.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "id": "s0_vad_0",
                                "source_audio_id": "source_0",
                                "analysis_start_sample": 0,
                                "analysis_end_sample": 1600,
                            }
                        ),
                        json.dumps(
                            {
                                "id": "s0_vad_1",
                                "source_audio_id": "source_0",
                                "analysis_start_sample": 2400,
                                "analysis_end_sample": 4000,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "aligned_words.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "id": "w0",
                                "buffer_id": "buffer_000000",
                                "word": "hello",
                                "local_start_sample": 0,
                                "local_end_sample": 1200,
                                "source_start_sample": 0,
                                "source_end_sample": 1200,
                            }
                        ),
                        json.dumps(
                            {
                                "id": "w1",
                                "buffer_id": "buffer_000000",
                                "word": "there",
                                "local_start_sample": 1800,
                                "local_end_sample": 2200,
                                "source_start_sample": 1800,
                                "source_end_sample": 2200,
                            }
                        ),
                        json.dumps(
                            {
                                "id": "w2",
                                "buffer_id": "buffer_000000",
                                "word": "friend",
                                "local_start_sample": 3000,
                                "local_end_sample": 3400,
                                "source_start_sample": 3000,
                                "source_end_sample": 3400,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "safe_cutpoints.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "id": "buffer_000000-cut-0000",
                                "buffer_id": "buffer_000000",
                                "cut_local_sample": 1700,
                                "source_sample": 1700,
                            }
                        ),
                        json.dumps(
                            {
                                "id": "buffer_000000-cut-0001",
                                "buffer_id": "buffer_000000",
                                "cut_local_sample": 2300,
                                "source_sample": 2300,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "rejected_cutpoint_candidates.jsonl").write_text(
                json.dumps({"buffer_id": "buffer_000000", "reason_codes": ["usable_gap_too_short"]}) + "\n",
                encoding="utf-8",
            )
            (artifacts / "candidate_review_manifest.json").write_text(
                json.dumps(
                    [
                        {
                            "id": "candidate_review_clip_000000",
                            "buffer_id": "buffer_000000",
                            "word_ids": ["w0", "w1"],
                            "training_text": "hello there",
                            "needs_review": False,
                            "review_reason_codes": [],
                        }
                    ]
                ),
                encoding="utf-8",
            )
            (artifacts / "alignment_qc_by_buffer.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "fatal_reason_codes": [],
                            "warning_reason_codes": [],
                            "automatic_cutpoints_disabled": False,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            (artifacts / "safe_cutpoint_summary.json").write_text(
                json.dumps({"thresholds": {"min_gap_sec": 0.08}}),
                encoding="utf-8",
            )

            out_dir = temp_dir / "report"
            summary = analyze_vad_mfa_gaps(run_root, out_dir, max_examples_per_bucket=5)

            self.assertEqual(summary["mfa_gap_count"], 2)
            self.assertEqual(summary["vad_gap_count"], 2)
            self.assertEqual(summary["mfa_gap_with_vad_gap_count"], 2)
            self.assertEqual(summary["vad_gap_with_mfa_gap_count"], 1)
            self.assertEqual(summary["safe_cutpoints_inside_vad_gap_count"], 2)
            self.assertEqual(summary["vad_cuttable_gap_count"], 1)
            self.assertEqual(summary["vad_cuttable_gap_without_mfa_gap_count"], 1)
            self.assertAlmostEqual(summary["mfa_gap_vad_coverage_ratio"], 1.0, places=6)
            self.assertAlmostEqual(summary["vad_gap_mfa_coverage_ratio"], 0.5, places=6)
            self.assertAlmostEqual(summary["safe_cutpoints_inside_vad_gap_ratio"], 1.0, places=6)
            self.assertAlmostEqual(summary["false_safe_vad_gap_ratio"], 1.0, places=6)
            self.assertTrue((out_dir / "gap_agreement_summary.json").exists())
            self.assertTrue((out_dir / "gap_agreement_examples.json").exists())
            self.assertTrue((out_dir / "gap_agreement_by_buffer.csv").exists())
            self.assertTrue((out_dir / "gap_context_wavs").exists())


if __name__ == "__main__":
    unittest.main()
