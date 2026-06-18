from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlmodel import Session

from app.dataset_runs import WORKER_STAGE_MAP, create_dataset_run, refresh_dataset_run
from app.models import (
    DatasetRunCreateRequest,
    ImportBatch,
    ProcessingRun,
    ProcessingRunStatus,
    RfcStage,
    RunArtifactKind,
    SourceRecording,
)
from app.repository import SQLiteRepository


class DatasetRunRefreshStateTests(unittest.TestCase):
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

    def _create_run(self):
        return create_dataset_run(self.repository, "project-1", DatasetRunCreateRequest())

    def _run_root(self, run) -> Path:
        return self.repository.media_root / str(run.artifact_root)

    def _mark_running(self, run, *, pid: int = 4242) -> None:
        with Session(self.repository.engine) as session:
            stored = session.get(ProcessingRun, run.id)
            assert stored is not None
            stored.status = ProcessingRunStatus.RUNNING
            stored.input_summary = {**stored.input_summary, "worker_pid": pid}
            session.add(stored)
            session.commit()

    def _write_status(self, run, payload: dict) -> None:
        run_root = self._run_root(run)
        run_root.mkdir(parents=True, exist_ok=True)
        (run_root / "status.json").write_text(json.dumps(payload), encoding="utf-8")

    def test_pending_run_without_status_stays_pending(self) -> None:
        run = self._create_run()
        refreshed = refresh_dataset_run(self.repository, run.id)
        self.assertEqual(refreshed.status, ProcessingRunStatus.PENDING)

    def test_pending_run_with_in_progress_status_becomes_running(self) -> None:
        run = self._create_run()
        self._write_status(
            run,
            {"stage": "mfa", "ok": None, "summary": {"textgrid_count": 1}},
        )
        with patch("app.dataset_runs._pid_is_running", return_value=False):
            refreshed = refresh_dataset_run(self.repository, run.id)
        self.assertEqual(refreshed.status, ProcessingRunStatus.RUNNING)
        self.assertEqual(refreshed.stage, RfcStage.MFA)

    def test_running_run_without_status_and_dead_pid_fails(self) -> None:
        run = self._create_run()
        self._mark_running(run, pid=999999999)
        with patch("app.dataset_runs._pid_is_running", return_value=False):
            refreshed = refresh_dataset_run(self.repository, run.id)
        self.assertEqual(refreshed.status, ProcessingRunStatus.FAILED)
        self.assertEqual(refreshed.reason_codes, ["dataset_worker_exited_without_status"])

    def test_in_progress_mfa_with_alive_pid_stays_running(self) -> None:
        run = self._create_run()
        self._mark_running(run)
        self._write_status(
            run,
            {"stage": "mfa", "ok": None, "summary": {"textgrid_count": 1}},
        )
        with patch("app.dataset_runs._pid_is_running", return_value=True):
            refreshed = refresh_dataset_run(self.repository, run.id)
        self.assertEqual(refreshed.status, ProcessingRunStatus.RUNNING)
        self.assertEqual(refreshed.stage, RfcStage.MFA)

    def test_in_progress_mfa_with_dead_pid_fails(self) -> None:
        run = self._create_run()
        self._mark_running(run)
        self._write_status(
            run,
            {"stage": "mfa", "ok": None, "summary": {"textgrid_count": 1}},
        )
        with patch("app.dataset_runs._pid_is_running", return_value=False):
            refreshed = refresh_dataset_run(self.repository, run.id)
        self.assertEqual(refreshed.status, ProcessingRunStatus.FAILED)
        self.assertEqual(refreshed.reason_codes, ["dataset_worker_exited_before_terminal_status"])

    def test_malformed_ok_false_without_completed_at_and_dead_pid_fails(self) -> None:
        run = self._create_run()
        self._mark_running(run)
        self._write_status(
            run,
            {"stage": "asr", "ok": False, "summary": {"buffer_count": 2}},
        )
        with patch("app.dataset_runs._pid_is_running", return_value=False):
            refreshed = refresh_dataset_run(self.repository, run.id)
        self.assertEqual(refreshed.status, ProcessingRunStatus.FAILED)
        self.assertEqual(refreshed.reason_codes, ["dataset_worker_exited_before_terminal_status"])

    def test_malformed_ok_true_without_completed_at_fails(self) -> None:
        run = self._create_run()
        self._mark_running(run)
        self._write_status(
            run,
            {"stage": "alignment_qc", "ok": True, "summary": {"buffer_count": 3}},
        )
        refreshed = refresh_dataset_run(self.repository, run.id)
        self.assertEqual(refreshed.status, ProcessingRunStatus.FAILED)
        self.assertEqual(refreshed.reason_codes, ["dataset_worker_malformed_status"])

    def test_malformed_ok_true_with_invalid_completed_at_fails(self) -> None:
        run = self._create_run()
        self._mark_running(run)
        self._write_status(
            run,
            {
                "stage": "alignment_qc",
                "ok": True,
                "summary": {"buffer_count": 3},
                "completed_at": "not-a-timestamp",
            },
        )
        refreshed = refresh_dataset_run(self.repository, run.id)
        self.assertEqual(refreshed.status, ProcessingRunStatus.FAILED)
        self.assertEqual(refreshed.reason_codes, ["dataset_worker_malformed_status"])

    def test_terminal_success_marks_completed(self) -> None:
        run = self._create_run()
        self._mark_running(run)
        self._write_status(
            run,
            {
                "stage": "alignment_qc",
                "ok": True,
                "summary": {"buffer_count": 3},
                "completed_at": "2026-06-05T11:30:52+00:00",
            },
        )
        refreshed = refresh_dataset_run(self.repository, run.id)
        self.assertEqual(refreshed.status, ProcessingRunStatus.COMPLETED)
        self.assertEqual(refreshed.stage, RfcStage.MFA)
        self.assertEqual(refreshed.completed_at.isoformat(), "2026-06-05T11:30:52+00:00")

    def test_terminal_failure_marks_failed(self) -> None:
        run = self._create_run()
        self._mark_running(run)
        self._write_status(
            run,
            {
                "stage": "asr",
                "ok": False,
                "reason_codes": ["asr_failed"],
                "summary": {},
                "completed_at": "2026-06-05T11:30:52+00:00",
            },
        )
        refreshed = refresh_dataset_run(self.repository, run.id)
        self.assertEqual(refreshed.status, ProcessingRunStatus.FAILED)
        self.assertEqual(refreshed.reason_codes, ["asr_failed"])

    def test_native_export_success_maps_to_export_stage(self) -> None:
        run = self._create_run()
        self._mark_running(run)
        self._write_status(
            run,
            {
                "stage": "native_export",
                "ok": True,
                "summary": {"exported_clips": 2},
                "completed_at": "2026-06-05T11:30:52+00:00",
            },
        )
        refreshed = refresh_dataset_run(self.repository, run.id)
        self.assertEqual(refreshed.status, ProcessingRunStatus.COMPLETED)
        self.assertEqual(refreshed.stage, RfcStage.EXPORT)

    def test_transcript_qc_success_maps_to_transcript_qc_stage(self) -> None:
        run = self._create_run()
        self._mark_running(run)
        self._write_status(
            run,
            {
                "stage": "transcript_qc",
                "ok": True,
                "summary": {"clip_count": 2},
                "completed_at": "2026-06-05T11:30:52+00:00",
            },
        )
        refreshed = refresh_dataset_run(self.repository, run.id)
        self.assertEqual(refreshed.status, ProcessingRunStatus.COMPLETED)
        self.assertEqual(refreshed.stage, RfcStage.TRANSCRIPT_QC)

    def test_unknown_worker_stage_adds_diagnostic_reason(self) -> None:
        run = self._create_run()
        self._mark_running(run)
        self._write_status(
            run,
            {"stage": "totally_new_stage", "ok": None, "summary": {}},
        )
        with patch("app.dataset_runs._pid_is_running", return_value=True):
            refreshed = refresh_dataset_run(self.repository, run.id)
        self.assertEqual(refreshed.status, ProcessingRunStatus.RUNNING)
        self.assertIn("unknown_worker_stage", refreshed.reason_codes)

    def test_unknown_stage_reason_codes_clear_on_terminal_success(self) -> None:
        run = self._create_run()
        self._mark_running(run)
        self._write_status(
            run,
            {"stage": "totally_new_stage", "ok": None, "summary": {}},
        )
        with patch("app.dataset_runs._pid_is_running", return_value=True):
            unknown = refresh_dataset_run(self.repository, run.id)
        self.assertIn("unknown_worker_stage", unknown.reason_codes)

        self._write_status(
            run,
            {
                "stage": "alignment_qc",
                "ok": True,
                "summary": {"buffer_count": 3},
                "completed_at": "2026-06-05T11:30:52+00:00",
            },
        )
        refreshed = refresh_dataset_run(self.repository, run.id)
        self.assertEqual(refreshed.status, ProcessingRunStatus.COMPLETED)
        self.assertEqual(refreshed.reason_codes, [])

    def test_refresh_indexes_artifacts_as_files_appear(self) -> None:
        run = self._create_run()
        run_root = self._run_root(run)
        run_root.mkdir(parents=True, exist_ok=True)
        (run_root / "status.json").write_text(
            json.dumps({"stage": "starting", "ok": None}),
            encoding="utf-8",
        )

        baseline = refresh_dataset_run(self.repository, run.id)
        baseline_kinds = {artifact.kind for artifact in baseline.artifacts}
        self.assertIn(RunArtifactKind.RUN_CONFIG_JSON, baseline_kinds)
        self.assertIn(RunArtifactKind.RUN_STATUS_JSON, baseline_kinds)
        self.assertEqual(len(baseline.artifacts), 2)

        artifacts = run_root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        (artifacts / "vad_summary.json").write_text(json.dumps({"segment_count": 1}), encoding="utf-8")
        after_vad = refresh_dataset_run(self.repository, run.id)
        self.assertGreater(len(after_vad.artifacts), len(baseline.artifacts))
        self.assertIn(RunArtifactKind.VAD_SUMMARY_JSON, {artifact.kind for artifact in after_vad.artifacts})

        (artifacts / "aligned_words.jsonl").write_text("", encoding="utf-8")
        after_words = refresh_dataset_run(self.repository, run.id)
        self.assertIn(RunArtifactKind.ALIGNED_WORDS_JSONL, {artifact.kind for artifact in after_words.artifacts})

        (artifacts / "alignment_qc_summary.json").write_text(json.dumps({"buffer_count": 1}), encoding="utf-8")
        after_qc = refresh_dataset_run(self.repository, run.id)
        self.assertIn(RunArtifactKind.ALIGNMENT_QC_SUMMARY_JSON, {artifact.kind for artifact in after_qc.artifacts})


class WorkerStageContractTests(unittest.TestCase):
    def test_worker_stage_names_are_mapped_in_backend(self) -> None:
        worker_stages = {
            "source_audio",
            "audio_variants",
            "vad",
            "buffers",
            "asr_queue",
            "asr",
            "normalization",
            "mfa",
            "alignment_qc",
            "safe_cutpoints",
            "candidate_review_clips",
            "native_export",
        }
        self.assertTrue(worker_stages <= set(WORKER_STAGE_MAP))
