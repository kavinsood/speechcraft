import json
import math
import os
import subprocess
import wave
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from sqlmodel import Session, select

from app.models import (
    ReferenceAsset,
    ReferenceAssetCreateFromCandidate,
    ReferenceEmbeddingEvaluationProbe,
    ReferenceEmbeddingEvaluationRequest,
    ReferenceCandidateSummary,
    ReferenceRunCreate,
    ReferenceRunRerankRequest,
    ReferencePickerRun,
    ReferenceRunStatus,
    ReferenceSourceKind,
    ReferenceVariant,
    ProjectPreparationRequest,
    ProcessingJob,
    JobKind,
    JobStatus,
    SourceAlignmentRequest,
    SourceTranscriptionRequest,
    SourceRecording,
    SourceRecordingCreate,
    utc_now,
)
from app.repository import SQLiteRepository
from app.worker import process_next_job

def read_wav_duration_seconds(path: Path) -> float:
    with wave.open(str(path), "rb") as wav_file:
        return wav_file.getnframes() / wav_file.getframerate()


def read_reference_embeddings(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def read_reference_manifest(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def mark_recording_aligned(repository: SQLiteRepository, recording_id: str) -> None:
    artifact_dir = repository.exports_root / "test-artifacts" / recording_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    transcript_path = artifact_dir / "transcript.txt"
    alignment_path = artifact_dir / "alignment.json"
    transcript_path.write_text("alpha beta gamma delta\n", encoding="utf-8")
    alignment_path.write_text(
        json.dumps(
            [
                {"word": "alpha", "start": 0.0, "end": 0.5},
                {"word": "beta", "start": 0.7, "end": 1.2},
                {"word": "gamma", "start": 1.5, "end": 2.0},
                {"word": "delta", "start": 2.4, "end": 2.9},
            ],
            indent=2,
        ),
        encoding="utf-8",
    )
    repository.set_source_recording_artifact_paths(
        recording_id,
        transcript_text_path=str(transcript_path),
        alignment_json_path=str(alignment_path),
        transcript_status="ok",
        alignment_status="ok",
        transcript_word_count=4,
        alignment_word_count=4,
    )


def build_legacy_clip(
    project_id: str,
    clip_id: str,
    source_file_id: str,
    order_index: int,
    *,
    audio_path: str | None = None,
) -> dict[str, object]:
    timestamp = "2026-03-18T15:16:30Z"
    return {
        "audio_path": audio_path,
        "channels": 1,
        "clip_edl": [],
        "created_at": timestamp,
        "duration_seconds": 1.5,
        "edit_state": "clean",
        "id": clip_id,
        "is_superseded": False,
        "language": "en",
        "order_index": order_index,
        "original_end_time": 1.5,
        "original_start_time": 0.0,
        "project_id": project_id,
        "review_status": "candidate",
        "sample_rate": 48000,
        "source_file_id": source_file_id,
        "speaker_name": "speaker_a",
        "tags": [],
        "transcript": {
            "text_current": f"Transcript for {clip_id}",
            "text_initial": f"Transcript for {clip_id}",
            "source": "manual",
            "confidence": 1.0,
            "updated_at": timestamp,
        },
        "updated_at": timestamp,
        "working_asset_id": f"working-{clip_id}",
    }


def write_silence_then_tone_wav(
    path: Path,
    sample_rate: int,
    leading_silence_seconds: float,
    tone_seconds: float,
    trailing_silence_seconds: float,
) -> int:
    amplitude = int(32767 * 0.28)
    frequency = 220.0
    total_frames = 0
    with wave.open(str(path), "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        for _ in range(int(sample_rate * leading_silence_seconds)):
            wav_file.writeframes((0).to_bytes(2, "little", signed=True))
            total_frames += 1
        tone_frames = int(sample_rate * tone_seconds)
        for frame_index in range(tone_frames):
            sample = int(amplitude * math.sin((2 * math.pi * frequency * frame_index) / sample_rate))
            wav_file.writeframes(sample.to_bytes(2, "little", signed=True))
            total_frames += 1
        for _ in range(int(sample_rate * trailing_silence_seconds)):
            wav_file.writeframes((0).to_bytes(2, "little", signed=True))
            total_frames += 1
    return total_frames


class RepositoryMediaTests(TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.repository = SQLiteRepository(
            db_path=root / "project.db",
            legacy_seed_path=root / "missing-seed.json",
            media_root=root / "media",
            exports_root=root / "exports",
        )
        self.addCleanup(self.repository.close)

    def tearDown(self) -> None:
        self.repository.close()
        self.temp_dir.cleanup()

    def test_sqlite_connections_enable_wal_and_busy_timeout(self) -> None:
        with self.repository.engine.begin() as connection:
            journal_mode = connection.exec_driver_sql("PRAGMA journal_mode").scalar_one()
            busy_timeout = connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one()
            synchronous = connection.exec_driver_sql("PRAGMA synchronous").scalar_one()

        self.assertEqual(str(journal_mode).lower(), "wal")
        self.assertEqual(int(busy_timeout), 30000)
        self.assertEqual(int(synchronous), 1)

    def _fake_forced_align_subprocess(
        self,
        command: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess:
        output_index = command.index("--output") + 1
        output_path = Path(command[output_index])
        transcript_text = str(command[command.index("--text") + 1]).strip()
        words = transcript_text.split() or ["placeholder"]
        word_duration = 0.6
        alignment_payload = []
        for index, word in enumerate(words):
            start = round(index * word_duration, 6)
            end = round((index + 1) * word_duration, 6)
            alignment_payload.append({"word": word, "start": start, "end": end})
        output_path.write_text(json.dumps(alignment_payload), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
    def test_project_preparation_worker_creates_derived_recordings_and_active_group(self) -> None:
        raw_recordings = [
            recording
            for recording in self.repository.list_source_recordings("phase1-demo")
            if recording.parent_recording_id is None
        ]
        self.assertGreaterEqual(len(raw_recordings), 1)

        run = self.repository.run_project_preparation(
            "phase1-demo",
            ProjectPreparationRequest(target_sample_rate=16000, channel_mode="mono"),
        )
        self.assertEqual(run.job.status, "pending")

        processed = process_next_job(self.repository, "test-worker")

        self.assertTrue(processed)
        jobs = self.repository.list_project_preparation_jobs("phase1-demo")
        self.assertEqual(jobs[0].status, "completed")
        output_group_id = jobs[0].output_payload["output_group_id"]
        project = self.repository.get_project("phase1-demo")
        self.assertEqual(project.active_prepared_output_group_id, output_group_id)
        self.assertEqual(project.active_preparation_job_id, jobs[0].id)

        recordings = self.repository.list_source_recordings("phase1-demo")
        derived_recordings = [
            recording
            for recording in recordings
            if recording.parent_recording_id is not None and output_group_id in (recording.processing_recipe or "")
        ]
        self.assertEqual(len(derived_recordings), len(raw_recordings))
        for recording in derived_recordings:
            self.assertEqual(recording.sample_rate, 16000)
            self.assertEqual(recording.num_channels, 1)


    def test_project_preparation_rejects_right_channel_for_mono_source(self) -> None:
        run = self.repository.run_project_preparation(
            "phase1-demo",
            ProjectPreparationRequest(target_sample_rate=16000, channel_mode="right"),
        )

        processed = process_next_job(self.repository, "test-worker")

        self.assertTrue(processed)
        job = self.repository.get_processing_job(run.job.id)
        self.assertEqual(job.status, "failed")
        self.assertIn("Right channel selection requires a stereo source recording", job.error_message or "")
        derived_recordings = [
            recording
            for recording in self.repository.list_source_recordings("phase1-demo")
            if recording.parent_recording_id is not None and run.job.id in (recording.processing_recipe or "")
        ]
        self.assertEqual(derived_recordings, [])


    def test_project_preparation_cleans_written_files_when_materialization_fails(self) -> None:
        def fail_after_writing(
            source_path: Path,
            target_path: Path,
            **kwargs: object,
        ) -> tuple[int, int, int]:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(b"orphan candidate")
            raise ValueError("forced preparation failure")

        run = self.repository.run_project_preparation(
            "phase1-demo",
            ProjectPreparationRequest(target_sample_rate=16000, channel_mode="mono"),
        )

        with patch.object(self.repository, "_write_prepared_wav", side_effect=fail_after_writing):
            processed = process_next_job(self.repository, "test-worker")

        self.assertTrue(processed)
        job = self.repository.get_processing_job(run.job.id)
        self.assertEqual(job.status, "failed")
        prepared_files = list((self.repository.media_root / "prepared").glob("*.wav"))
        self.assertEqual(prepared_files, [])


    def test_legacy_seed_imports_all_projects_without_source_collisions(self) -> None:
        root = Path(self.temp_dir.name)
        legacy_path = root / "legacy-seed.json"
        legacy_payload = {
            "projects": {
                "project-a": {
                    "id": "project-a",
                    "name": "Project A",
                    "status": "ready",
                    "export_status": "idle",
                    "created_at": "2026-03-18T15:16:30Z",
                    "updated_at": "2026-03-18T15:16:30Z",
                },
                "project-b": {
                    "id": "project-b",
                    "name": "Project B",
                    "status": "ready",
                    "export_status": "idle",
                    "created_at": "2026-03-18T15:16:30Z",
                    "updated_at": "2026-03-18T15:16:30Z",
                },
            },
            "clips_by_project": {
                "project-a": [build_legacy_clip("project-a", "clip-a", "shared-source", 0)],
                "project-b": [build_legacy_clip("project-b", "clip-b", "shared-source", 0)],
            },
            "commits_by_clip": {},
            "history_by_clip": {},
            "exports_by_project": {
                "project-a": [],
                "project-b": [],
            },
        }
        legacy_path.write_text(json.dumps(legacy_payload))

        migrated_repository = SQLiteRepository(
            db_path=root / "legacy-project.db",
            legacy_seed_path=legacy_path,
            media_root=root / "legacy-media",
            exports_root=root / "legacy-exports",
        )
        self.addCleanup(migrated_repository.close)

        projects = migrated_repository.list_projects()

        self.assertEqual({project.id for project in projects}, {"project-a", "project-b"})
        self.assertEqual(len(migrated_repository.list_source_recordings("project-a")), 1)
        self.assertEqual(len(migrated_repository.list_source_recordings("project-b")), 1)

        with Session(migrated_repository.engine, expire_on_commit=False) as session:
            source_recordings = session.exec(select(SourceRecording)).all()

        self.assertEqual(len(source_recordings), 2)
        self.assertEqual({recording.batch_id for recording in source_recordings}, {"project-a", "project-b"})


    def test_seed_from_legacy_json_uses_real_audio_for_one_to_one_source_recordings(self) -> None:
        root = Path(self.temp_dir.name)
        legacy_path = root / "legacy-seed.json"
        real_audio_path = root / "legacy-real.wav"
        frame_count = write_silence_then_tone_wav(real_audio_path, 48000, 0.2, 0.8, 0.5)
        legacy_payload = {
            "projects": {
                "project-a": {
                    "id": "project-a",
                    "name": "Project A",
                    "status": "ready",
                    "export_status": "idle",
                    "created_at": "2026-03-18T15:16:30Z",
                    "updated_at": "2026-03-18T15:16:30Z",
                }
            },
            "clips_by_project": {
                "project-a": [
                    build_legacy_clip(
                        "project-a",
                        "clip-a",
                        "source-a",
                        0,
                        audio_path=str(real_audio_path),
                    )
                ]
            },
            "commits_by_clip": {},
            "history_by_clip": {},
            "exports_by_project": {"project-a": []},
        }
        legacy_path.write_text(json.dumps(legacy_payload))

        migrated_repository = SQLiteRepository(
            db_path=root / "legacy-project.db",
            legacy_seed_path=legacy_path,
            media_root=root / "legacy-media",
            exports_root=root / "legacy-exports",
        )

        with Session(migrated_repository.engine, expire_on_commit=False) as session:
            source_recording = session.exec(select(SourceRecording)).one()

        self.assertEqual(
            Path(source_recording.file_path).read_bytes(),
            real_audio_path.read_bytes(),
        )
        self.assertEqual(source_recording.num_samples, frame_count)
        self.assertEqual(source_recording.processing_recipe, "legacy_seed_clip_source")


    def test_migrate_legacy_seed_source_recordings_backfills_real_audio(self) -> None:
        root = Path(self.temp_dir.name)
        legacy_path = root / "legacy-seed.json"
        real_audio_path = root / "legacy-real.wav"
        write_silence_then_tone_wav(real_audio_path, 48000, 0.1, 0.6, 0.4)
        legacy_payload = {
            "projects": {
                "project-a": {
                    "id": "project-a",
                    "name": "Project A",
                    "status": "ready",
                    "export_status": "idle",
                    "created_at": "2026-03-18T15:16:30Z",
                    "updated_at": "2026-03-18T15:16:30Z",
                }
            },
            "clips_by_project": {
                "project-a": [
                    build_legacy_clip(
                        "project-a",
                        "clip-a",
                        "source-a",
                        0,
                        audio_path=str(real_audio_path),
                    )
                ]
            },
            "commits_by_clip": {},
            "history_by_clip": {},
            "exports_by_project": {"project-a": []},
        }
        legacy_path.write_text(json.dumps(legacy_payload))

        migrated_repository = SQLiteRepository(
            db_path=root / "legacy-project.db",
            legacy_seed_path=legacy_path,
            media_root=root / "legacy-media",
            exports_root=root / "legacy-exports",
        )

        with Session(migrated_repository.engine, expire_on_commit=False) as session:
            source_recording = session.exec(select(SourceRecording)).one()
            source_recording_path = Path(source_recording.file_path)
            source_recording_path.write_bytes(
                migrated_repository._render_synthetic_wave_bytes(48000, 1, 1.1, "synthetic-source")
            )
            source_recording.processing_recipe = None
            session.add(source_recording)
            session.commit()

        self.assertNotEqual(source_recording_path.read_bytes(), real_audio_path.read_bytes())

        migrated_repository._migrate_legacy_seed_source_recording_media()

        with Session(migrated_repository.engine, expire_on_commit=False) as session:
            repaired_recording = session.exec(select(SourceRecording)).one()

        self.assertEqual(source_recording_path.read_bytes(), real_audio_path.read_bytes())
        self.assertEqual(repaired_recording.processing_recipe, "legacy_seed_clip_source")


    def test_create_source_recording_creates_source_artifact_row(self) -> None:
        source_path = self.repository.media_root / "sources" / "artifact-test.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(
            self.repository._render_synthetic_wave_bytes(48000, 1, 1.5, "artifact-test")
        )

        recording = self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-artifact-test",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=72000,
            )
        )

        artifact = self.repository.get_source_recording_artifact(recording.id)

        self.assertEqual(artifact.source_recording_id, recording.id)
        self.assertEqual(artifact.transcript_status, "missing")
        self.assertEqual(artifact.alignment_status, "missing")
        self.assertEqual(artifact.transcript_word_count, 0)
        self.assertEqual(artifact.alignment_word_count, 0)


    def test_list_project_recordings_reports_recording_level_processing_state(self) -> None:
        source_path = self.repository.media_root / "sources" / "queue-state.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(
            self.repository._render_synthetic_wave_bytes(48000, 1, 1.25, "queue-state")
        )

        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-queue-state",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=60000,
            )
        )
        self.repository.enqueue_source_transcription(
            "src-queue-state",
            SourceTranscriptionRequest(model_name="stub-source-asr", model_version="2026.04", language_hint="en"),
        )

        recordings = self.repository.list_project_recordings("phase1-demo")
        queue_state = next(recording for recording in recordings if recording.id == "src-queue-state")

        self.assertEqual(queue_state.processing_state, "transcribing")
        self.assertEqual(queue_state.processing_message, "Transcribing audio...")
        self.assertEqual(queue_state.slice_count, 0)
        self.assertIsNotNone(queue_state.active_job)
        self.assertEqual(queue_state.active_job.kind, "source_transcription")
        self.assertEqual(queue_state.artifact.transcript_status, "missing")


    def test_source_transcription_job_writes_recording_artifacts(self) -> None:
        source_path = self.repository.media_root / "sources" / "source-asr.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(
            self.repository._render_synthetic_wave_bytes(48000, 1, 2.0, "source-asr")
        )
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-source-asr",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=96000,
            )
        )

        job = self.repository.enqueue_source_transcription(
            "src-source-asr",
            SourceTranscriptionRequest(model_name="stub-source-asr", model_version="2026.04", language_hint="en"),
        )

        with patch.dict(os.environ, {"ASR_BACKEND": "stub", "SPEECHCRAFT_ALLOW_STUB_ASR": "1"}):
            processed = process_next_job(self.repository, worker_id="test-worker")

        self.assertTrue(processed)
        latest = self.repository.get_processing_job(job.id)
        self.assertEqual(latest.status, "completed")
        artifact = self.repository.get_source_recording_artifact("src-source-asr")
        self.assertEqual(artifact.transcript_status, "ok")
        self.assertIsNotNone(artifact.transcript_text_path)
        self.assertIsNotNone(artifact.transcript_json_path)
        self.assertTrue(Path(artifact.transcript_text_path).exists())
        self.assertTrue(Path(artifact.transcript_json_path).exists())


    def test_source_alignment_job_writes_alignment_artifact(self) -> None:
        source_path = self.repository.media_root / "sources" / "source-align.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(
            self.repository._render_synthetic_wave_bytes(48000, 1, 2.5, "source-align")
        )
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-source-align",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=120000,
            )
        )
        self.repository.enqueue_source_transcription(
            "src-source-align",
            SourceTranscriptionRequest(model_name="stub-source-asr", model_version="2026.04", language_hint="en"),
        )
        with patch.dict(os.environ, {"ASR_BACKEND": "stub", "SPEECHCRAFT_ALLOW_STUB_ASR": "1"}):
            process_next_job(self.repository, worker_id="test-worker")
        artifact = self.repository.get_source_recording_artifact("src-source-align")
        Path(artifact.transcript_text_path).write_text("alpha beta gamma\n", encoding="utf-8")

        def fake_align(_audio_bytes: bytes, transcript_text: str) -> list[dict[str, object]]:
            words = transcript_text.split() or ["stub"]
            return [
                {"word": word, "start": round(index * 0.2, 6), "end": round(index * 0.2 + 0.15, 6)}
                for index, word in enumerate(words)
            ]

        with patch.object(self.repository, "_run_forced_align_worker", side_effect=fake_align):
            job = self.repository.enqueue_source_alignment(
                "src-source-align",
                SourceAlignmentRequest(alignment_backend="stub-align"),
            )
            processed = process_next_job(self.repository, worker_id="test-worker")

        self.assertTrue(processed)
        latest = self.repository.get_processing_job(job.id)
        self.assertEqual(latest.status, "completed")
        artifact = self.repository.get_source_recording_artifact("src-source-align")
        self.assertEqual(artifact.alignment_status, "ok")
        self.assertEqual(artifact.alignment_backend, "stub-align")
        self.assertIsNotNone(artifact.alignment_json_path)
        self.assertTrue(Path(artifact.alignment_json_path).exists())


    def test_source_transcript_patch_marks_alignment_stale(self) -> None:
        source_path = self.repository.media_root / "sources" / "source-patch.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(
            self.repository._render_synthetic_wave_bytes(48000, 1, 3.0, "source-patch")
        )
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-source-patch",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=144000,
            )
        )
        artifact_dir = self.repository.exports_root / "recording-artifacts" / "src-source-patch"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        transcript_base = artifact_dir / "transcript.base.txt"
        transcript_text = artifact_dir / "transcript.txt"
        transcript_base.write_text("alpha beta gamma delta\n", encoding="utf-8")
        transcript_text.write_text("alpha beta gamma delta\n", encoding="utf-8")
        self.repository.set_source_recording_artifact_paths(
            "src-source-patch",
            transcript_text_path=str(transcript_text),
            transcript_json_path=None,
            alignment_json_path=None,
            transcript_status="ok",
            alignment_status="ok",
            transcript_word_count=4,
            artifact_metadata={"base_transcript_text_path": str(transcript_base)},
        )

        with Session(self.repository.engine, expire_on_commit=False) as session:
            from app.models import SourceRecordingArtifact

            artifact = session.get(SourceRecordingArtifact, "src-source-patch")
            self.assertIsNotNone(artifact)
            metadata = self.repository._source_artifact_metadata(artifact)
            metadata["transcript_patches"] = [
                {
                    "slice_id": "",
                    "start_word_index": 0,
                    "end_word_index": 1,
                    "text": "alpha better",
                    "updated_at": utc_now().isoformat(),
                }
            ]
            artifact.artifact_metadata = metadata
            artifact.transcript_status = "patched"
            artifact.alignment_status = "stale"
            self.repository._persist_effective_source_transcript(artifact)
            session.add(artifact)
            session.commit()

        artifact = self.repository.get_source_recording_artifact("src-source-patch")
        self.assertEqual(artifact.transcript_status, "patched")
        self.assertEqual(artifact.alignment_status, "stale")
        self.assertEqual(Path(artifact.transcript_text_path).read_text(encoding="utf-8").strip(), "alpha better gamma delta")


    def test_source_recording_window_media_path_materializes_audio_from_master_recording(self) -> None:
        media_path = self.repository.get_source_recording_window_media_path("src-001", 12.4, 15.68)

        self.assertTrue(media_path.exists())
        self.assertAlmostEqual(read_wav_duration_seconds(media_path), 3.28, places=2)


    def test_list_source_recordings_exposes_duration_seconds(self) -> None:
        recordings = self.repository.list_source_recordings("phase1-demo")

        self.assertEqual(len(recordings), 1)
        self.assertEqual(recordings[0].id, "src-001")
        self.assertAlmostEqual(recordings[0].duration_seconds, 20.0, places=2)


    def test_reference_run_processes_candidates_and_materializes_preview_lazily(self) -> None:
        created = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=12),
        )
        self.assertEqual(created.status, ReferenceRunStatus.QUEUED)

        completed = self.repository.process_reference_run(created.id)
        self.assertEqual(completed.status, ReferenceRunStatus.COMPLETED)
        self.assertGreater(completed.candidate_count, 0)

        candidates = self.repository.list_reference_run_candidates(created.id, limit=12)
        self.assertGreater(len(candidates), 0)
        first_candidate = candidates[0]
        self.assertEqual(first_candidate.run_id, created.id)
        self.assertEqual(first_candidate.source_media_kind, "source_recording")
        self.assertGreater(first_candidate.source_end_seconds, first_candidate.source_start_seconds)

        preview_path = self.repository.get_reference_candidate_media_path(created.id, first_candidate.candidate_id)
        self.assertTrue(preview_path.exists())
        preview_duration = read_wav_duration_seconds(preview_path)
        self.assertAlmostEqual(preview_duration, first_candidate.duration_seconds, places=2)

        preview_path_again = self.repository.get_reference_candidate_media_path(created.id, first_candidate.candidate_id)
        self.assertEqual(preview_path_again, preview_path)


    def test_reference_run_ids_are_derived_from_project_id(self) -> None:
        first_run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=8),
        )
        second_run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=8),
        )
        self.assertEqual(first_run.id, "reference-run-phase1-demo")
        self.assertEqual(second_run.id, "reference-run-phase1-demo-2")

    def test_reference_run_candidate_ids_are_stable_for_same_config(self) -> None:
        first_run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=8),
        )
        second_run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=8),
        )

        self.repository.process_reference_run(first_run.id)
        self.repository.process_reference_run(second_run.id)

        first_candidates = self.repository.list_reference_run_candidates(first_run.id, limit=8)
        second_candidates = self.repository.list_reference_run_candidates(second_run.id, limit=8)

        self.assertEqual(
            [candidate.candidate_id for candidate in first_candidates],
            [candidate.candidate_id for candidate in second_candidates],
        )


    def test_reference_run_writes_embedding_artifact_for_candidates(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=8),
        )
        self.repository.process_reference_run(run.id)

        candidates = self.repository.list_reference_run_candidates(run.id, limit=8)
        self.assertGreater(len(candidates), 0)
        self.assertTrue(all(candidate.embedding_index is not None for candidate in candidates))

        with Session(self.repository.engine, expire_on_commit=False) as session:
            run_row = session.get(ReferencePickerRun, run.id)
            self.assertIsNotNone(run_row)
            embeddings_path = self.repository._reference_run_embeddings_path(Path(run_row.artifact_root))
            manifest_path = Path(run_row.artifact_root) / "manifest.json"

        self.assertTrue(embeddings_path.exists())
        payload = read_reference_embeddings(embeddings_path)
        self.assertEqual(payload["artifact_schema_version"], 2)
        self.assertEqual(payload["space"]["id"], "acoustic_signature_v1:normalized_16bit_pcm_wav:v1")
        self.assertEqual(payload["space"]["name"], "acoustic_signature_v1")
        self.assertEqual(payload["space"]["version"], 1)
        entries = payload["entries"]
        self.assertEqual(len(entries), len(candidates))
        self.assertEqual(payload["space"]["dimension"], len(entries[0]["vector"]))
        self.assertTrue(all(len(entry["vector"]) == payload["space"]["dimension"] for entry in entries))
        self.assertEqual(
            [entry["candidate_id"] for entry in entries],
            [candidate.candidate_id for candidate in candidates],
        )
        self.assertTrue(
            all(candidate.embedding_space_id == payload["space"]["id"] for candidate in candidates)
        )

        manifest = read_reference_manifest(manifest_path)
        self.assertEqual(manifest["embedding_extractor"], "acoustic_signature_v1")
        self.assertEqual(manifest["embedding_extractor_version"], 1)
        self.assertEqual(manifest["embedding_artifact_schema_version"], 2)
        self.assertEqual(manifest["embedding_space_id"], payload["space"]["id"])


    def test_promoted_reference_asset_summary_exposes_ready_embedding_state(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=6),
        )
        self.repository.process_reference_run(run.id)
        candidate = self.repository.list_reference_run_candidates(run.id, limit=1)[0]

        promoted = self.repository.create_reference_asset_from_candidate(
            ReferenceAssetCreateFromCandidate(
                run_id=run.id,
                candidate_id=candidate.candidate_id,
            )
        )
        summary = self.repository.list_reference_assets("phase1-demo")[0]

        self.assertEqual(summary.id, promoted.id)
        self.assertEqual(summary.embedding_status, "ready")
        self.assertEqual(summary.embedding_space_id, "acoustic_signature_v1:normalized_16bit_pcm_wav:v1")
        self.assertEqual(summary.embedding_variant_id, summary.active_variant_id)
        self.assertIsNotNone(summary.embedding_updated_at)
        self.assertIsNone(summary.embedding_error_message)


    def test_reference_asset_embedding_becomes_stale_when_active_variant_changes(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=6),
        )
        self.repository.process_reference_run(run.id)
        candidate = self.repository.list_reference_run_candidates(run.id, limit=1)[0]
        promoted = self.repository.create_reference_asset_from_candidate(
            ReferenceAssetCreateFromCandidate(
                run_id=run.id,
                candidate_id=candidate.candidate_id,
            )
        )

        with Session(self.repository.engine, expire_on_commit=False) as session:
            asset = session.get(ReferenceAsset, promoted.id)
            self.assertIsNotNone(asset)
            active_variant = session.get(ReferenceVariant, asset.active_variant_id)
            self.assertIsNotNone(active_variant)
            new_variant_id = self.repository._new_id("reference-variant")
            new_storage_key = self.repository._reference_variant_storage_key(new_variant_id)
            new_path = self.repository._managed_reference_variant_path(new_variant_id)
            new_path.parent.mkdir(parents=True, exist_ok=True)
            new_path.write_bytes(self.repository.get_reference_variant_media_path(active_variant.id).read_bytes())
            new_variant = ReferenceVariant(
                id=new_variant_id,
                reference_asset_id=asset.id,
                source_kind=ReferenceSourceKind.REFERENCE_VARIANT,
                source_reference_variant_id=active_variant.id,
                file_path=new_storage_key,
                is_original=False,
                generator_model="manual-copy",
                sample_rate=active_variant.sample_rate,
                num_samples=active_variant.num_samples,
            )
            session.add(new_variant)
            session.flush()
            asset.active_variant_id = new_variant.id
            session.add(asset)
            session.commit()

        summary = self.repository.get_reference_asset(promoted.id)
        self.assertEqual(summary.embedding_status, "stale")
        self.assertNotEqual(summary.embedding_variant_id, summary.active_variant_id)


    def test_reference_run_rerank_accepts_saved_reference_asset_anchors(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=8),
        )
        self.repository.process_reference_run(run.id)
        candidates = self.repository.list_reference_run_candidates(run.id, limit=8)
        self.assertGreaterEqual(len(candidates), 2)
        promoted = self.repository.create_reference_asset_from_candidate(
            ReferenceAssetCreateFromCandidate(
                run_id=run.id,
                candidate_id=candidates[0].candidate_id,
            )
        )

        reranked = self.repository.rerank_reference_run_candidates(
            run.id,
            ReferenceRunRerankRequest(
                positive_reference_asset_ids=[promoted.id],
                mode="both",
            ),
        )

        self.assertEqual(reranked.embedding_space_id, "acoustic_signature_v1:normalized_16bit_pcm_wav:v1")
        self.assertEqual(reranked.positive_reference_asset_ids, [promoted.id])
        self.assertEqual(reranked.candidates[0].candidate_id, candidates[0].candidate_id)


    def test_reference_run_rerank_rejects_reference_anchor_with_incompatible_embedding_space(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=8),
        )
        self.repository.process_reference_run(run.id)
        candidate = self.repository.list_reference_run_candidates(run.id, limit=1)[0]
        promoted = self.repository.create_reference_asset_from_candidate(
            ReferenceAssetCreateFromCandidate(
                run_id=run.id,
                candidate_id=candidate.candidate_id,
            )
        )

        with Session(self.repository.engine, expire_on_commit=False) as session:
            asset = session.get(ReferenceAsset, promoted.id)
            self.assertIsNotNone(asset)
            metadata = dict(asset.model_metadata or {})
            embedding_cache = dict(metadata.get("embedding_cache") or {})
            embedding_cache["space_id"] = "incompatible-space-v1"
            metadata["embedding_cache"] = embedding_cache
            asset.model_metadata = metadata
            session.add(asset)
            session.commit()

        with self.assertRaisesRegex(ValueError, "space is incompatible"):
            self.repository.rerank_reference_run_candidates(
                run.id,
                ReferenceRunRerankRequest(
                    positive_reference_asset_ids=[promoted.id],
                    mode="both",
                ),
            )


    def test_reference_run_embedding_evaluation_reports_recall_at_k(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=6),
        )
        self.repository.process_reference_run(run.id)
        candidates = self.repository.list_reference_run_candidates(run.id, limit=3)
        self.assertGreaterEqual(len(candidates), 3)

        with Session(self.repository.engine, expire_on_commit=False) as session:
            run_row = session.get(ReferencePickerRun, run.id)
            self.assertIsNotNone(run_row)
            embeddings_path = self.repository._reference_run_embeddings_path(Path(run_row.artifact_root))
        payload = read_reference_embeddings(embeddings_path)
        payload["space"]["dimension"] = 3
        for entry in payload["entries"]:
            entry["vector"] = [0.0, 0.0, 1.0]
        payload["entries"][0]["vector"] = [1.0, 0.0, 0.0]
        payload["entries"][1]["vector"] = [0.99, 0.01, 0.0]
        payload["entries"][2]["vector"] = [0.0, 1.0, 0.0]
        embeddings_path.write_text(json.dumps(payload))

        evaluation = self.repository.evaluate_reference_run_embeddings(
            run.id,
            ReferenceEmbeddingEvaluationRequest(
                probes=[
                    ReferenceEmbeddingEvaluationProbe(
                        anchor_candidate_id=candidates[0].candidate_id,
                        expected_neighbor_candidate_ids=[candidates[1].candidate_id],
                        top_k=1,
                    )
                ]
            ),
        )

        self.assertEqual(evaluation.embedding_space_id, "acoustic_signature_v1:normalized_16bit_pcm_wav:v1")
        self.assertEqual(evaluation.probe_count, 1)
        self.assertEqual(evaluation.average_recall_at_k, 1.0)
        self.assertEqual(
            evaluation.probes[0].retrieved_neighbor_candidate_ids,
            [candidates[1].candidate_id],
        )


    def test_reference_run_rerank_returns_separate_intent_scores_without_mutating_baseline_scores(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=8),
        )
        self.repository.process_reference_run(run.id)
        candidates = self.repository.list_reference_run_candidates(run.id, limit=8)

        self.assertGreaterEqual(len(candidates), 2)
        positive = candidates[0]
        negative = candidates[1]

        reranked = self.repository.rerank_reference_run_candidates(
            run.id,
            ReferenceRunRerankRequest(
                positive_candidate_ids=[positive.candidate_id],
                negative_candidate_ids=[negative.candidate_id],
                mode="both",
            ),
        )

        self.assertEqual(reranked.run_id, run.id)
        self.assertEqual(reranked.positive_candidate_ids, [positive.candidate_id])
        self.assertEqual(reranked.negative_candidate_ids, [negative.candidate_id])
        self.assertEqual(len(reranked.candidates), len(candidates))
        self.assertEqual(reranked.candidates[0].candidate_id, positive.candidate_id)

        result_by_id = {candidate.candidate_id: candidate for candidate in reranked.candidates}
        positive_result = result_by_id[positive.candidate_id]
        negative_result = result_by_id[negative.candidate_id]

        self.assertAlmostEqual(positive_result.base_score, positive.default_scores["both"], places=6)
        self.assertAlmostEqual(negative_result.base_score, negative.default_scores["both"], places=6)
        self.assertGreater(positive_result.intent_score, 0.0)
        self.assertLess(negative_result.intent_score, positive_result.intent_score)
        self.assertGreater(positive_result.rerank_score, positive_result.base_score)
        self.assertEqual(positive.default_scores["both"], result_by_id[positive.candidate_id].default_scores["both"])


    def test_reference_run_rerank_rejects_overlapping_positive_and_negative_anchors(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=6),
        )
        self.repository.process_reference_run(run.id)
        candidate = self.repository.list_reference_run_candidates(run.id, limit=1)[0]

        with self.assertRaisesRegex(ValueError, "both positive and negative"):
            self.repository.rerank_reference_run_candidates(
                run.id,
                ReferenceRunRerankRequest(
                    positive_candidate_ids=[candidate.candidate_id],
                    negative_candidate_ids=[candidate.candidate_id],
                    mode="both",
                ),
            )


    def test_reference_run_rerank_rejects_anchor_ids_outside_the_run(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=6),
        )
        self.repository.process_reference_run(run.id)

        with self.assertRaisesRegex(ValueError, "does not belong to the run"):
            self.repository.rerank_reference_run_candidates(
                run.id,
                ReferenceRunRerankRequest(
                    positive_candidate_ids=["cand-missing"],
                    negative_candidate_ids=[],
                    mode="both",
                ),
            )


    def test_reference_run_rerank_fails_if_embedding_artifact_is_corrupt(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=6),
        )
        self.repository.process_reference_run(run.id)

        with Session(self.repository.engine, expire_on_commit=False) as session:
            run_row = session.get(ReferencePickerRun, run.id)
            self.assertIsNotNone(run_row)
            embeddings_path = self.repository._reference_run_embeddings_path(Path(run_row.artifact_root))

        payload = read_reference_embeddings(embeddings_path)
        payload["entries"][0]["vector"] = payload["entries"][0]["vector"][:-1]
        embeddings_path.write_text(json.dumps(payload))
        candidate = self.repository.list_reference_run_candidates(run.id, limit=1)[0]

        with self.assertRaisesRegex(ValueError, "dimension mismatch"):
            self.repository.rerank_reference_run_candidates(
                run.id,
                ReferenceRunRerankRequest(
                    positive_candidate_ids=[candidate.candidate_id],
                    negative_candidate_ids=[],
                    mode="both",
                ),
            )


    def test_reference_run_uses_speech_first_scaffold_instead_of_whole_recording_sweep(self) -> None:
        recording_path = self.repository.media_root / "imports" / "src-speechy.wav"
        recording_path.parent.mkdir(parents=True, exist_ok=True)
        sample_rate = 16000
        num_samples = write_silence_then_tone_wav(
            recording_path,
            sample_rate=sample_rate,
            leading_silence_seconds=1.0,
            tone_seconds=1.4,
            trailing_silence_seconds=1.1,
        )
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-speechy",
                batch_id="phase1-demo",
                file_path=str(recording_path),
                sample_rate=sample_rate,
                num_channels=1,
                num_samples=num_samples,
            )
        )

        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-speechy"], mode="both", candidate_count_cap=12),
        )
        self.repository.process_reference_run(run.id)
        candidates = self.repository.list_reference_run_candidates(run.id, limit=12)

        self.assertGreater(len(candidates), 0)
        self.assertTrue(all(candidate.source_start_seconds >= 0.75 for candidate in candidates))
        self.assertTrue(all(candidate.source_end_seconds <= 2.7 for candidate in candidates))


    def test_promote_reference_candidate_creates_durable_reference_asset(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=6),
        )
        self.repository.process_reference_run(run.id)
        candidate = self.repository.list_reference_run_candidates(run.id, limit=1)[0]

        promoted = self.repository.create_reference_asset_from_candidate(
            ReferenceAssetCreateFromCandidate(
                run_id=run.id,
                candidate_id=candidate.candidate_id,
                mood_label="picker",
            )
        )

        self.assertEqual(promoted.created_from_run_id, run.id)
        self.assertEqual(promoted.created_from_candidate_id, candidate.candidate_id)
        self.assertEqual(promoted.mood_label, "picker")
        self.assertEqual(promoted.active_variant.source_recording_id, candidate.source_recording_id)
        self.assertAlmostEqual(promoted.active_variant.source_start_seconds, candidate.source_start_seconds, places=3)
        self.assertAlmostEqual(promoted.active_variant.source_end_seconds, candidate.source_end_seconds, places=3)

        reference_audio_path = self.repository.get_reference_variant_media_path(promoted.active_variant.id)
        self.assertTrue(reference_audio_path.exists())
        self.assertAlmostEqual(read_wav_duration_seconds(reference_audio_path), candidate.duration_seconds, places=2)

        preview_path = self.repository.get_reference_candidate_media_path(run.id, candidate.candidate_id)
        preview_bytes = preview_path.read_bytes()
        reference_bytes = reference_audio_path.read_bytes()
        self.assertEqual(reference_bytes, preview_bytes)


    def test_promote_reference_candidate_accepts_trimmed_subspan_inside_candidate_bounds(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=6),
        )
        self.repository.process_reference_run(run.id)
        candidate = self.repository.list_reference_run_candidates(run.id, limit=1)[0]
        trimmed_start = round(candidate.source_start_seconds + 0.25, 3)
        trimmed_end = round(candidate.source_end_seconds - 0.35, 3)

        promoted = self.repository.create_reference_asset_from_candidate(
            ReferenceAssetCreateFromCandidate(
                run_id=run.id,
                candidate_id=candidate.candidate_id,
                source_start_seconds=trimmed_start,
                source_end_seconds=trimmed_end,
                mood_label="trimmed",
            )
        )

        self.assertEqual(promoted.created_from_candidate_id, candidate.candidate_id)
        self.assertAlmostEqual(promoted.active_variant.source_start_seconds, trimmed_start, places=3)
        self.assertAlmostEqual(promoted.active_variant.source_end_seconds, trimmed_end, places=3)
        self.assertEqual(promoted.model_metadata["trim_applied"], True)
        self.assertAlmostEqual(
            read_wav_duration_seconds(self.repository.get_reference_variant_media_path(promoted.active_variant.id)),
            trimmed_end - trimmed_start,
            places=2,
        )


    def test_promote_reference_candidate_rejects_trim_outside_candidate_bounds(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=6),
        )
        self.repository.process_reference_run(run.id)
        candidate = self.repository.list_reference_run_candidates(run.id, limit=1)[0]

        with self.assertRaisesRegex(ValueError, "candidate's canonical bounds"):
            self.repository.create_reference_asset_from_candidate(
                ReferenceAssetCreateFromCandidate(
                    run_id=run.id,
                    candidate_id=candidate.candidate_id,
                    source_start_seconds=round(candidate.source_start_seconds - 0.1, 3),
                    source_end_seconds=round(candidate.source_end_seconds - 0.1, 3),
                )
            )


    def test_promote_reference_candidate_rejects_missing_one_trim_bound(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=6),
        )
        self.repository.process_reference_run(run.id)
        candidate = self.repository.list_reference_run_candidates(run.id, limit=1)[0]

        with self.assertRaisesRegex(ValueError, "requires both source_start_seconds and source_end_seconds"):
            self.repository.create_reference_asset_from_candidate(
                ReferenceAssetCreateFromCandidate(
                    run_id=run.id,
                    candidate_id=candidate.candidate_id,
                    source_start_seconds=round(candidate.source_start_seconds + 0.1, 3),
                )
            )


    def test_promote_reference_candidate_rejects_non_finite_trim_bounds(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=6),
        )
        self.repository.process_reference_run(run.id)
        candidate = self.repository.list_reference_run_candidates(run.id, limit=1)[0]

        with self.assertRaisesRegex(ValueError, "finite"):
            self.repository.create_reference_asset_from_candidate(
                ReferenceAssetCreateFromCandidate(
                    run_id=run.id,
                    candidate_id=candidate.candidate_id,
                    source_start_seconds=float("nan"),
                    source_end_seconds=candidate.source_end_seconds,
                )
            )


    def test_promote_reference_candidate_rejects_non_positive_trim_duration(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=6),
        )
        self.repository.process_reference_run(run.id)
        candidate = self.repository.list_reference_run_candidates(run.id, limit=1)[0]

        with self.assertRaisesRegex(ValueError, "positive duration"):
            self.repository.create_reference_asset_from_candidate(
                ReferenceAssetCreateFromCandidate(
                    run_id=run.id,
                    candidate_id=candidate.candidate_id,
                    source_start_seconds=candidate.source_start_seconds + 0.5,
                    source_end_seconds=candidate.source_start_seconds + 0.5,
                )
            )


    def test_promote_reference_candidate_marks_trim_applied_false_for_exact_candidate_bounds(self) -> None:
        run = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(recording_ids=["src-001"], mode="both", candidate_count_cap=6),
        )
        self.repository.process_reference_run(run.id)
        candidate = self.repository.list_reference_run_candidates(run.id, limit=1)[0]

        promoted = self.repository.create_reference_asset_from_candidate(
            ReferenceAssetCreateFromCandidate(
                run_id=run.id,
                candidate_id=candidate.candidate_id,
                source_start_seconds=candidate.source_start_seconds,
                source_end_seconds=candidate.source_end_seconds,
            )
        )

        self.assertEqual(promoted.model_metadata["trim_applied"], False)


    def test_reference_picker_migration_rehomes_legacy_reference_assets(self) -> None:
        with self.repository.engine.begin() as connection:
            connection.exec_driver_sql("DROP TABLE IF EXISTS referencevariant")
            connection.exec_driver_sql("DROP TABLE IF EXISTS referencepickerrun")
            connection.exec_driver_sql("DROP TABLE IF EXISTS referenceasset")
            connection.exec_driver_sql(
                """
                CREATE TABLE referenceasset (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    audio_variant_id TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.exec_driver_sql(
                """
                INSERT INTO referenceasset (id, name, audio_variant_id, created_at)
                VALUES (?, ?, ?, ?)
                """,
                ("legacy-ref-001", "Legacy Ref", "variant-legacy-001", "2026-03-18T15:16:30+00:00"),
            )
            connection.exec_driver_sql("PRAGMA user_version = 2")

        restarted = SQLiteRepository(
            db_path=Path(self.temp_dir.name) / "project.db",
            legacy_seed_path=Path(self.temp_dir.name) / "missing-seed.json",
            media_root=Path(self.temp_dir.name) / "media",
            exports_root=Path(self.temp_dir.name) / "exports",
        )

        library = restarted.list_reference_assets("phase1-demo")
        self.assertEqual(library, [])

        reports_root = Path(self.temp_dir.name) / "migration-reports"
        report_files = sorted(reports_root.glob("referenceasset-migration-*.json"))
        self.assertEqual(len(report_files), 1)
        report_payload = json.loads(report_files[0].read_text())
        self.assertEqual(report_payload["issues"][0]["legacy_asset_id"], "legacy-ref-001")
        self.assertEqual(report_payload["issues"][0]["reason"], "legacy_slice_reference_removed")


    def test_reference_picker_migration_preserves_legacy_table_and_writes_report_for_unresolved_rows(self) -> None:
        with self.repository.engine.begin() as connection:
            connection.exec_driver_sql("DROP TABLE IF EXISTS referencevariant")
            connection.exec_driver_sql("DROP TABLE IF EXISTS referencepickerrun")
            connection.exec_driver_sql("DROP TABLE IF EXISTS referenceasset")
            connection.exec_driver_sql(
                """
                CREATE TABLE referenceasset (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    audio_variant_id TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.exec_driver_sql(
                """
                INSERT INTO referenceasset (id, name, audio_variant_id, created_at)
                VALUES (?, ?, ?, ?)
                """,
                ("legacy-ref-missing", "Missing Ref", "variant-does-not-exist", "2026-03-18T15:16:30+00:00"),
            )
            connection.exec_driver_sql("PRAGMA user_version = 2")

        restarted = SQLiteRepository(
            db_path=Path(self.temp_dir.name) / "project.db",
            legacy_seed_path=Path(self.temp_dir.name) / "missing-seed.json",
            media_root=Path(self.temp_dir.name) / "media",
            exports_root=Path(self.temp_dir.name) / "exports",
        )

        with restarted.engine.begin() as connection:
            legacy_tables = connection.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='referenceasset_legacy'"
            ).fetchall()

        self.assertEqual(len(legacy_tables), 1)

        reports_root = Path(self.temp_dir.name) / "migration-reports"
        report_files = sorted(reports_root.glob("referenceasset-migration-*.json"))
        self.assertEqual(len(report_files), 1)
        report_payload = json.loads(report_files[0].read_text())
        self.assertEqual(report_payload["issue_count"], 1)
        self.assertEqual(report_payload["issues"][0]["legacy_asset_id"], "legacy-ref-missing")
        self.assertEqual(report_payload["issues"][0]["reason"], "legacy_slice_reference_removed")
