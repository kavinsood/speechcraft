from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from sqlmodel import Session

from app.dataset_runs import (
    create_dataset_run,
    generate_dataset_qc_scores,
    get_candidate_review_media_bytes,
    get_candidate_review_media_path,
    get_dataset_export_results,
    get_dataset_run_log,
    get_dataset_speaker_results,
    get_dataset_slicer_results,
    get_native_export_media_path,
    get_speaker_sample_media_path,
    list_dataset_runs,
    refresh_dataset_run,
    resume_dataset_run_processing,
    rerun_dataset_native_export,
    rerun_dataset_slicer,
    save_dataset_speaker_selection,
    start_dataset_run,
)
from app.models import (
    DatasetExportRerunRequest,
    DatasetRunCreateRequest,
    DatasetSpeakerSelectionUpdateRequest,
    DatasetSlicerRerunRequest,
    ImportBatch,
    ProcessingRun,
    ProcessingRunStatus,
    RfcStage,
    RunArtifactKind,
    SourceRecording,
)
from app.repository import SQLiteRepository


class DatasetRunTests(TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.repository = SQLiteRepository(
            db_path=root / "project.db",
            legacy_seed_path=root / "missing-seed.json",
            media_root=root / "media",
            exports_root=root / "exports",
        )
        source_path = root / "source.wav"
        source_path.write_bytes(b"RIFF")
        with Session(self.repository.engine) as session:
            session.add(ImportBatch(id="project-1", name="Project 1"))
            session.add(
                SourceRecording(
                    id="recording-1",
                    batch_id="project-1",
                    file_path=str(source_path),
                    sample_rate=48000,
                    num_channels=1,
                    num_samples=48000,
                )
            )
            session.commit()

    def tearDown(self) -> None:
        self.repository.close()
        self.temp_dir.cleanup()

    def test_create_run_uses_relative_root_and_indexes_config(self) -> None:
        run = create_dataset_run(
            self.repository,
            "project-1",
            DatasetRunCreateRequest(source_recording_ids=["recording-1"], config={"vad_threshold": 0.6}),
        )

        self.assertTrue(run.artifact_root.startswith("dataset-runs/project-1/dataset-"))
        self.assertEqual(run.status, ProcessingRunStatus.PENDING)
        self.assertEqual(run.input_summary["source_recording_ids"], ["recording-1"])
        self.assertIn(RunArtifactKind.RUN_CONFIG_JSON, {artifact.kind for artifact in run.artifacts})
        self.assertFalse(Path(run.artifact_root).is_absolute())
        config = json.loads((self.repository.media_root / run.artifact_root / "config.json").read_text(encoding="utf-8"))
        self.assertEqual(config["vad_threshold"], 0.6)
        self.assertEqual(config["faster_whisper_beam_size"], 5)
        self.assertEqual(config["mfa_dictionary"], "english_us_mfa")

    def test_repository_migrates_old_processingrun_schema(self) -> None:
        self.repository.close()
        self.temp_dir.cleanup()
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        db_path = root / "project.db"
        with sqlite3.connect(db_path) as connection:
            connection.execute(
                """
                CREATE TABLE importbatch (
                    id TEXT PRIMARY KEY NOT NULL,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    export_status TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE processingrun (
                    id TEXT PRIMARY KEY NOT NULL,
                    project_id TEXT NOT NULL,
                    pipeline_version TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL,
                    config_hash TEXT,
                    input_summary JSON,
                    output_summary JSON,
                    reason_codes JSON,
                    created_at TIMESTAMP NOT NULL,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP
                )
                """
            )
            connection.execute(
                "INSERT INTO importbatch (id, name, created_at, updated_at) VALUES ('project-1', 'Project 1', '2026-06-05T00:00:00', '2026-06-05T00:00:00')"
            )
            connection.commit()

        self.repository = SQLiteRepository(
            db_path=db_path,
            legacy_seed_path=root / "missing-seed.json",
            media_root=root / "media",
            exports_root=root / "exports",
        )
        with self.repository.engine.begin() as connection:
            columns = {row[1] for row in connection.exec_driver_sql("PRAGMA table_info(processingrun)").fetchall()}
        self.assertIn("artifact_root", columns)
        self.assertEqual(list_dataset_runs(self.repository, "project-1"), [])

    def test_start_run_uses_explicit_worker_python_and_sources(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        process = Mock(pid=4321)
        with (
            patch("app.dataset_runs.run_dataset_worker_preflight", return_value={"ok": True}),
            patch("app.dataset_runs.dataset_worker_python", return_value=Path("/bin/true")),
            patch("app.dataset_runs.dataset_worker_root", return_value=Path("/tmp/worker")),
            patch("app.dataset_runs.subprocess.Popen", return_value=process) as popen,
        ):
            started = start_dataset_run(self.repository, run.id)

        command = popen.call_args.args[0]
        self.assertEqual(command[0], "/bin/true")
        self.assertIn("speechcraft_dataset.run", command)
        self.assertIn("--source-wav", command)
        self.assertEqual(started.status, ProcessingRunStatus.RUNNING)
        self.assertEqual(started.input_summary["worker_pid"], 4321)

    def test_start_multi_speaker_run_uses_diarization_first_pass(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest(single_speaker=False))
        process = Mock(pid=5432)
        with (
            patch("app.dataset_runs.run_dataset_worker_preflight") as preflight,
            patch("app.dataset_runs.dataset_worker_python", return_value=Path("/bin/true")),
            patch("app.dataset_runs.dataset_worker_root", return_value=Path("/tmp/worker")),
            patch("app.dataset_runs.subprocess.Popen", return_value=process) as popen,
        ):
            started = start_dataset_run(self.repository, run.id)

        command = popen.call_args.args[0]
        self.assertNotIn("--single-speaker", command)
        self.assertEqual(started.input_summary["active_stop_after"], "diarization")
        preflight.assert_not_called()
        config = json.loads((self.repository.media_root / started.artifact_root / "config.json").read_text(encoding="utf-8"))
        self.assertEqual(config["mode"], "diarization")

    def test_start_run_rejects_unavailable_selected_asr_model_before_launch(self) -> None:
        run = create_dataset_run(
            self.repository,
            "project-1",
            DatasetRunCreateRequest(config={"faster_whisper_model": "medium.en"}),
        )

        with (
            patch(
                "app.dataset_runs.run_dataset_worker_preflight",
                return_value={
                    "ok": False,
                    "error": "ASR model snapshot is incomplete: missing model.bin",
                    "asr_model": {"ok": False, "error": "ASR model snapshot is incomplete: missing model.bin"},
                },
            ),
            patch("app.dataset_runs.subprocess.Popen") as popen,
        ):
            with self.assertRaisesRegex(ValueError, "missing model.bin"):
                start_dataset_run(self.repository, run.id)

        popen.assert_not_called()

    def test_create_run_rejects_backend_controlled_config(self) -> None:
        with self.assertRaisesRegex(ValueError, "backend-controlled keys"):
            create_dataset_run(
                self.repository,
                "project-1",
                DatasetRunCreateRequest(config={"source_wavs": ["/tmp/not-managed.wav"]}),
            )

    def test_create_run_allows_single_source_multi_speaker(self) -> None:
        run = create_dataset_run(
            self.repository,
            "project-1",
            DatasetRunCreateRequest(single_speaker=False),
        )
        self.assertFalse(run.input_summary["single_speaker"])
        self.assertEqual(run.input_summary["requested_stop_after"], "alignment_qc")

    def test_create_run_rejects_multi_speaker_multi_source_until_cross_file_identity_exists(self) -> None:
        root = Path(self.temp_dir.name)
        source_path = root / "source-2.wav"
        source_path.write_bytes(b"RIFF")
        with Session(self.repository.engine) as session:
            session.add(
                SourceRecording(
                    id="recording-2",
                    batch_id="project-1",
                    file_path=str(source_path),
                    sample_rate=48000,
                    num_channels=1,
                    num_samples=48000,
                )
            )
            session.commit()

        # Option C: multi-file diarization is supported (worker concatenates the
        # per-source analysis variants, diarizes once, remaps regions per file).
        run = create_dataset_run(
            self.repository,
            "project-1",
            DatasetRunCreateRequest(single_speaker=False, source_recording_ids=["recording-1", "recording-2"]),
        )
        self.assertFalse(run.input_summary["single_speaker"])
        self.assertEqual(
            run.input_summary["source_recording_ids"], ["recording-1", "recording-2"]
        )

    def test_refresh_indexes_rejected_manifest_and_completed_status(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        run_root = self.repository.media_root / str(run.artifact_root)
        artifacts = run_root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        (run_root / "status.json").write_text(
            json.dumps(
                {
                    "ok": True,
                    "stage": "candidate_review_clips",
                    "summary": {"candidate_review_clips": 2},
                    "completed_at": "2026-06-04T12:00:00+00:00",
                }
            ),
            encoding="utf-8",
        )
        (artifacts / "candidate_review_rejected.json").write_text("[]", encoding="utf-8")

        refreshed = refresh_dataset_run(self.repository, run.id)

        self.assertEqual(refreshed.status, ProcessingRunStatus.COMPLETED)
        self.assertEqual(refreshed.completed_at.isoformat(), "2026-06-04T12:00:00+00:00")
        self.assertEqual(refreshed.output_summary["candidate_review_clips"], 2)
        self.assertIn(RunArtifactKind.CANDIDATE_REVIEW_REJECTED_JSON, {artifact.kind for artifact in refreshed.artifacts})
        self.assertIn(RunArtifactKind.RUN_STATUS_JSON, {artifact.kind for artifact in refreshed.artifacts})

    def test_candidate_review_media_path_accepts_clip_id_rows(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        run_root = self.repository.media_root / str(run.artifact_root)
        artifacts = run_root / "artifacts"
        clip_dir = artifacts / "candidate_review_clips"
        clip_dir.mkdir(parents=True, exist_ok=True)
        clip_path = clip_dir / "candidate_review_clip_000000.wav"
        clip_path.write_bytes(b"RIFF")
        (artifacts / "candidate_review_manifest.json").write_text(
            json.dumps(
                [
                    {
                        "clip_id": "candidate_review_clip_000000",
                        "audio_path": "artifacts/candidate_review_clips/candidate_review_clip_000000.wav",
                        "duration_sec": 1.0,
                    }
                ]
            ),
            encoding="utf-8",
        )

        resolved = get_candidate_review_media_path(
            self.repository,
            run.id,
            "candidate_review_clip_000000",
        )

        self.assertEqual(resolved, clip_path)

    def test_candidate_review_media_bytes_snapshot_is_stable_after_file_changes(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        run_root = self.repository.media_root / str(run.artifact_root)
        artifacts = run_root / "artifacts"
        clip_dir = artifacts / "candidate_review_clips"
        clip_dir.mkdir(parents=True, exist_ok=True)
        clip_path = clip_dir / "candidate_review_clip_000000.wav"
        original_bytes = b"RIFF-original"
        clip_path.write_bytes(original_bytes)
        (artifacts / "candidate_review_manifest.json").write_text(
            json.dumps(
                [
                    {
                        "clip_id": "candidate_review_clip_000000",
                        "audio_path": "artifacts/candidate_review_clips/candidate_review_clip_000000.wav",
                        "duration_sec": 1.0,
                    }
                ]
            ),
            encoding="utf-8",
        )

        captured = get_candidate_review_media_bytes(
            self.repository,
            run.id,
            "candidate_review_clip_000000",
        )
        clip_path.write_bytes(b"RIFF-replaced-on-disk")
        self.assertEqual(captured, original_bytes)
        self.assertNotEqual(clip_path.read_bytes(), captured)

    def test_log_response_is_bounded_and_falls_back_to_process_log(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        run_root = self.repository.media_root / str(run.artifact_root)
        process_log = run_root / "logs" / "dataset_worker_process.log"
        process_log.parent.mkdir(parents=True, exist_ok=True)
        process_log.write_text("x" * 60000, encoding="utf-8")

        result = get_dataset_run_log(self.repository, run.id)

        self.assertEqual(result.path, "logs/dataset_worker_process.log")
        self.assertEqual(len(result.text), 50000)
        self.assertTrue(result.truncated)

    def test_refresh_marks_dead_worker_without_status_failed(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        with Session(self.repository.engine) as session:
            stored = session.get(ProcessingRun, run.id)
            assert stored is not None
            stored.status = ProcessingRunStatus.RUNNING
            stored.input_summary = {**stored.input_summary, "worker_pid": 999999999}
            session.add(stored)
            session.commit()

        refreshed = refresh_dataset_run(self.repository, run.id)

        self.assertEqual(refreshed.status, ProcessingRunStatus.FAILED)
        self.assertEqual(refreshed.reason_codes, ["dataset_worker_exited_without_status"])
    def test_manifest_payload_is_not_stored_in_artifact_summary(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        run_root = self.repository.media_root / str(run.artifact_root)
        artifacts = run_root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        (artifacts / "source_audio_manifest.json").write_text(
            json.dumps({"sources": [{"id": f"source-{index}"} for index in range(100)]}),
            encoding="utf-8",
        )
        (artifacts / "source_audio_summary.json").write_text(
            json.dumps({"source_count": 100}),
            encoding="utf-8",
        )

        refreshed = refresh_dataset_run(self.repository, run.id)
        by_kind = {artifact.kind: artifact for artifact in refreshed.artifacts}

        self.assertEqual(by_kind[RunArtifactKind.SOURCE_AUDIO_MANIFEST_JSON].summary, {})
        self.assertTrue(by_kind[RunArtifactKind.SOURCE_AUDIO_MANIFEST_JSON].content_hash.startswith("sha256:"))
        self.assertGreater(by_kind[RunArtifactKind.SOURCE_AUDIO_MANIFEST_JSON].byte_size, 0)
        self.assertEqual(by_kind[RunArtifactKind.SOURCE_AUDIO_SUMMARY_JSON].summary, {"source_count": 100})

    def test_slicer_rerun_updates_only_allowed_config_and_launches_worker(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        run_root = self.repository.media_root / str(run.artifact_root)
        artifacts = run_root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        for relative in ("asr_mfa_queue.json", "aligned_words.jsonl", "alignment_qc_by_buffer.json"):
            (artifacts / relative).write_text("[]" if relative.endswith(".json") else "", encoding="utf-8")
        process = Mock(pid=5432)
        with (
            patch("app.dataset_runs.dataset_worker_python", return_value=Path("/bin/true")),
            patch("app.dataset_runs.dataset_worker_root", return_value=Path("/tmp/worker")),
            patch("app.dataset_runs.subprocess.Popen", return_value=process) as popen,
        ):
            started = rerun_dataset_slicer(
                self.repository,
                run.id,
                DatasetSlicerRerunRequest(config={"cutpoint_min_gap_ms": 40, "candidate_target_clip_sec": 8}),
            )

        self.assertEqual(started.status, ProcessingRunStatus.RUNNING)
        self.assertIn("speechcraft_dataset.rerun_slicer", popen.call_args.args[0])
        config = json.loads((run_root / "config.json").read_text(encoding="utf-8"))
        self.assertEqual(config["cutpoint_min_gap_ms"], 40)
        self.assertEqual(config["candidate_target_clip_sec"], 8)
        self.assertEqual(config["cutpoint_frame_ms"], 10)
        self.assertEqual(config["cutpoint_hop_ms"], 5)
        status = json.loads((run_root / "status.json").read_text(encoding="utf-8"))
        self.assertIsNone(status["ok"])
        self.assertEqual(status["stage"], "safe_cutpoints")

    def test_slicer_rerun_rejects_hardcoded_config_keys(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        run_root = self.repository.media_root / str(run.artifact_root)
        artifacts = run_root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        for relative in ("asr_mfa_queue.json", "aligned_words.jsonl", "alignment_qc_by_buffer.json"):
            (artifacts / relative).write_text("[]" if relative.endswith(".json") else "", encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "Unsupported slicer config keys: cutpoint_frame_ms"):
            rerun_dataset_slicer(
                self.repository,
                run.id,
                DatasetSlicerRerunRequest(config={"cutpoint_frame_ms": 99}),
            )

    def test_slicer_rerun_rejects_when_clip_lab_lock_is_held(self) -> None:
        from app.clip_lab_state import clip_lab_run_lock

        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        run_root = self.repository.media_root / str(run.artifact_root)
        artifacts = run_root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        for relative in ("asr_mfa_queue.json", "aligned_words.jsonl", "alignment_qc_by_buffer.json"):
            (artifacts / relative).write_text("[]" if relative.endswith(".json") else "", encoding="utf-8")

        with clip_lab_run_lock(run_root):
            with self.assertRaisesRegex(ValueError, "Clip Lab state is busy"):
                rerun_dataset_slicer(self.repository, run.id, DatasetSlicerRerunRequest())

    def test_qc_score_generation_launches_worker(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        run_root = self.repository.media_root / str(run.artifact_root)
        artifacts = run_root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        (artifacts / "candidate_review_manifest.json").write_text("[]", encoding="utf-8")
        (artifacts / "speaker_selection.json").write_text(json.dumps({"target_speaker_id": "speaker_0"}), encoding="utf-8")
        (artifacts / "speaker_regions.jsonl").write_text("", encoding="utf-8")
        (artifacts / "audio_variants_manifest.json").write_text(json.dumps({"variants": []}), encoding="utf-8")
        process = Mock(pid=9876)
        with (
            patch("app.dataset_runs.dataset_worker_python", return_value=Path("/bin/true")),
            patch("app.dataset_runs.dataset_worker_root", return_value=Path("/tmp/worker")),
            patch("app.dataset_runs.subprocess.Popen", return_value=process) as popen,
        ):
            started = generate_dataset_qc_scores(self.repository, run.id)

        self.assertEqual(started.status, ProcessingRunStatus.RUNNING)
        self.assertEqual(started.stage, RfcStage.TRANSCRIPT_QC)
        self.assertIn("speechcraft_dataset.generate_qc_scores", popen.call_args.args[0])
        status = json.loads((run_root / "status.json").read_text(encoding="utf-8"))
        self.assertIsNone(status["ok"])
        self.assertEqual(status["stage"], "transcript_qc")

    def test_qc_score_generation_rejects_finalized_qc_without_force(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        run_root = self.repository.media_root / str(run.artifact_root)
        artifacts = run_root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        (artifacts / "candidate_review_manifest.json").write_text("[]", encoding="utf-8")
        (artifacts / "speaker_selection.json").write_text(json.dumps({"target_speaker_id": "speaker_0"}), encoding="utf-8")
        (artifacts / "speaker_regions.jsonl").write_text("", encoding="utf-8")
        (artifacts / "audio_variants_manifest.json").write_text(json.dumps({"variants": []}), encoding="utf-8")
        (artifacts / "dataset_qc.json").write_text(
            json.dumps({"schema_version": 1, "stage": "dataset_qc", "clips": [], "thresholds": {}, "score_methods": {}}),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "dataset_qc_already_finalized"):
            generate_dataset_qc_scores(self.repository, run.id)

    def test_qc_score_generation_allows_force_when_finalized_qc_exists(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        run_root = self.repository.media_root / str(run.artifact_root)
        artifacts = run_root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        (artifacts / "candidate_review_manifest.json").write_text("[]", encoding="utf-8")
        (artifacts / "speaker_selection.json").write_text(json.dumps({"target_speaker_id": "speaker_0"}), encoding="utf-8")
        (artifacts / "speaker_regions.jsonl").write_text("", encoding="utf-8")
        (artifacts / "audio_variants_manifest.json").write_text(json.dumps({"variants": []}), encoding="utf-8")
        (artifacts / "dataset_qc.json").write_text(
            json.dumps({"schema_version": 1, "stage": "dataset_qc", "clips": [], "thresholds": {}, "score_methods": {}}),
            encoding="utf-8",
        )
        process = Mock(pid=9876)
        with (
            patch("app.dataset_runs.dataset_worker_python", return_value=Path("/bin/true")),
            patch("app.dataset_runs.dataset_worker_root", return_value=Path("/tmp/worker")),
            patch("app.dataset_runs.subprocess.Popen", return_value=process) as popen,
        ):
            started = generate_dataset_qc_scores(self.repository, run.id, force=True)

        self.assertEqual(started.status, ProcessingRunStatus.RUNNING)
        command = popen.call_args.args[0]
        self.assertIn("--force", command)

    def test_slicer_results_and_candidate_media_are_manifest_guarded(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        run_root = self.repository.media_root / str(run.artifact_root)
        artifacts = run_root / "artifacts"
        clips = artifacts / "candidate_review_clips"
        clips.mkdir(parents=True, exist_ok=True)
        clip_path = clips / "clip-1.wav"
        clip_path.write_bytes(b"RIFF")
        (artifacts / "safe_cutpoint_summary.json").write_text(json.dumps({"accepted_cutpoints": 2}), encoding="utf-8")
        (artifacts / "candidate_review_summary.json").write_text(json.dumps({"candidate_review_clips": 1}), encoding="utf-8")
        (artifacts / "candidate_review_manifest.json").write_text(json.dumps([{"id": "clip-1", "audio_path": "artifacts/candidate_review_clips/clip-1.wav"}]), encoding="utf-8")
        (artifacts / "candidate_review_rejected.json").write_text("[]", encoding="utf-8")

        results = get_dataset_slicer_results(self.repository, run.id)

        self.assertEqual(results.safe_cutpoint_summary["accepted_cutpoints"], 2)
        self.assertEqual(get_candidate_review_media_path(self.repository, run.id, "clip-1"), clip_path)
        with self.assertRaises(KeyError):
            get_candidate_review_media_path(self.repository, run.id, "not-in-manifest")

    def test_export_results_and_native_media_are_manifest_guarded(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())
        run_root = self.repository.media_root / str(run.artifact_root)
        artifacts = run_root / "artifacts"
        exports_dir = artifacts / "native_export_clips"
        exports_dir.mkdir(parents=True, exist_ok=True)
        clip_path = exports_dir / "clip-1.wav"
        clip_path.write_bytes(b"RIFF")
        for relative in ("candidate_review_manifest.json", "source_audio_manifest.json", "audio_variants_manifest.json"):
            (artifacts / relative).write_text("[]", encoding="utf-8")
        (artifacts / "export_summary.json").write_text(json.dumps({"exported_clip_count": 1}), encoding="utf-8")
        (artifacts / "export_manifest.json").write_text(
            json.dumps([{"id": "clip-1", "audio_path": "artifacts/native_export_clips/clip-1.wav"}]),
            encoding="utf-8",
        )
        (artifacts / "export_audit.json").write_text("[]", encoding="utf-8")

        with (
            patch("app.dataset_runs.dataset_worker_python", return_value=Path("/bin/true")),
            patch("app.dataset_runs.dataset_worker_root", return_value=Path("/tmp/worker")),
            patch("app.dataset_runs.subprocess.Popen", return_value=Mock(pid=6789)) as popen,
        ):
            started = rerun_dataset_native_export(self.repository, run.id, DatasetExportRerunRequest())

        self.assertEqual(started.status, ProcessingRunStatus.RUNNING)
        self.assertIn("speechcraft_dataset.rerun_export", popen.call_args.args[0])
        results = get_dataset_export_results(self.repository, run.id)
        self.assertEqual(results.export_summary["exported_clip_count"], 1)
        self.assertEqual(get_native_export_media_path(self.repository, run.id, "clip-1"), clip_path)
        with self.assertRaises(KeyError):
            get_native_export_media_path(self.repository, run.id, "not-in-manifest")

    def test_speaker_results_selection_and_media_are_manifest_guarded(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest(single_speaker=False))
        run_root = self.repository.media_root / str(run.artifact_root)
        artifacts = run_root / "artifacts"
        samples_dir = artifacts / "speaker_samples"
        samples_dir.mkdir(parents=True, exist_ok=True)
        sample_path = samples_dir / "speaker_0_00.wav"
        sample_path.write_bytes(b"RIFF")
        (artifacts / "speaker_regions_summary.json").write_text(
            json.dumps({"speaker_ids": ["speaker_0", "speaker_1"], "speaker_count": 2}),
            encoding="utf-8",
        )
        (artifacts / "speaker_samples_manifest.json").write_text(
            json.dumps(
                [
                    {
                        "sample_id": "speaker_0_00",
                        "speaker_id": "speaker_0",
                        "source_audio_id": "source_audio_0000",
                        "audio_path": "artifacts/speaker_samples/speaker_0_00.wav",
                        "start_sample": 0,
                        "end_sample": 16000,
                        "duration_sec": 1.0,
                    }
                ]
            ),
            encoding="utf-8",
        )
        (artifacts / "speaker_selection.json").write_text(
            json.dumps(
                {
                    "mode": "diarization",
                    "selected": False,
                    "target_speaker_id": None,
                    "source": "pending_user_selection",
                    "available_speaker_ids": ["speaker_0", "speaker_1"],
                }
            ),
            encoding="utf-8",
        )

        results = get_dataset_speaker_results(self.repository, run.id)
        selection = save_dataset_speaker_selection(
            self.repository,
            run.id,
            DatasetSpeakerSelectionUpdateRequest(target_speaker_id="speaker_0"),
        )

        self.assertEqual(results.speaker_regions_summary["speaker_count"], 2)
        self.assertEqual(results.speaker_samples_manifest[0].sample_id, "speaker_0_00")
        self.assertEqual(selection.target_speaker_id, "speaker_0")
        self.assertTrue(selection.selected)
        self.assertEqual(get_speaker_sample_media_path(self.repository, run.id, "speaker_0_00"), sample_path)
        with self.assertRaises(KeyError):
            get_speaker_sample_media_path(self.repository, run.id, "missing")

    def test_resume_multi_speaker_processing_requires_selection_then_uses_requested_stop_after(self) -> None:
        run = create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest(single_speaker=False))
        run_root = self.repository.media_root / str(run.artifact_root)
        artifacts = run_root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        (artifacts / "speaker_selection.json").write_text(
            json.dumps(
                {
                    "mode": "diarization",
                    "selected": False,
                    "target_speaker_id": None,
                    "source": "pending_user_selection",
                    "available_speaker_ids": ["speaker_0", "speaker_1"],
                }
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "does not select a target speaker"):
            resume_dataset_run_processing(self.repository, run.id)

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
        process = Mock(pid=7777)
        with (
            patch("app.dataset_runs.run_dataset_worker_preflight", return_value={"ok": True}),
            patch("app.dataset_runs.dataset_worker_python", return_value=Path("/bin/true")),
            patch("app.dataset_runs.dataset_worker_root", return_value=Path("/tmp/worker")),
            patch("app.dataset_runs.subprocess.Popen", return_value=process) as popen,
        ):
            resumed = resume_dataset_run_processing(self.repository, run.id)

        self.assertEqual(resumed.status, ProcessingRunStatus.RUNNING)
        self.assertEqual(resumed.input_summary["active_stop_after"], "alignment_qc")
        self.assertIn("alignment_qc", popen.call_args.args[0])
        config = json.loads((self.repository.media_root / resumed.artifact_root / "config.json").read_text(encoding="utf-8"))
        self.assertEqual(config["mode"], "selected_speaker")
        self.assertEqual(config["target_speaker_label"], "speaker_1")
