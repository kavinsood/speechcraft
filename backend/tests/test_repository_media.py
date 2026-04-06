import json
import os
import subprocess
import wave
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from sqlmodel import Session, select

from app.models import (
    ActiveVariantUpdate,
    AudioVariant,
    AudioVariantCreate,
    AudioVariantRunRequest,
    EditCommit,
    ReviewStatus,
    Slice,
    SliceEdlUpdate,
    SliceSaveRequest,
    SliceSplitRequest,
    SliceStatusUpdate,
    SliceTagUpdate,
    SliceTranscriptUpdate,
    SourceAlignmentRequest,
    SourceTranscriptionRequest,
    SourceSlicingRequest,
    SourceRecording,
    SourceRecordingCreate,
    TagPayload,
)
from app.repository import SQLiteRepository, SliceSaveValidationError
from app.worker import process_next_job


def read_wav_duration_seconds(path: Path) -> float:
    with wave.open(str(path), "rb") as wav_file:
        return wav_file.getnframes() / wav_file.getframerate()


def build_legacy_clip(project_id: str, clip_id: str, source_file_id: str, order_index: int) -> dict[str, object]:
    timestamp = "2026-03-18T15:16:30Z"
    return {
        "audio_path": None,
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

    def test_slice_duration_is_returned_from_backend(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]
        updated = self.repository.append_edl_operation(
            initial.id,
            SliceEdlUpdate(
                op="insert_silence",
                range={"start_seconds": 0.1, "end_seconds": 0.1},
                duration_seconds=0.2,
            ),
        )

        self.assertAlmostEqual(updated.duration_seconds, round(initial.duration_seconds + 0.2, 2), places=2)

    def test_slice_media_path_reflects_edl_audio(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]
        updated = self.repository.append_edl_operation(
            initial.id,
            SliceEdlUpdate(
                op="insert_silence",
                range={"start_seconds": 0.1, "end_seconds": 0.1},
                duration_seconds=0.2,
            ),
        )

        self.assertIsNotNone(updated.active_variant_id)
        rendered_path = self.repository.get_slice_media_path(initial.id)
        raw_variant_path = self.repository.get_variant_media_path(updated.active_variant_id)

        rendered_duration = read_wav_duration_seconds(rendered_path)
        raw_variant_duration = read_wav_duration_seconds(raw_variant_path)

        self.assertAlmostEqual(rendered_duration, updated.duration_seconds, places=2)
        self.assertAlmostEqual(raw_variant_duration, initial.duration_seconds, places=2)
        self.assertGreater(rendered_duration, raw_variant_duration)

    def test_cleanup_project_media_removes_superseded_slices_and_unused_variants(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]
        original_variant_id = initial.active_variant_id
        self.assertIsNotNone(original_variant_id)

        updated = self.repository.run_audio_variant(
            initial.id,
            AudioVariantRunRequest(generator_model="deepfilternet"),
        )
        self.repository.set_active_variant(
            initial.id,
            ActiveVariantUpdate(active_variant_id=original_variant_id),
        )
        self.repository.split_slice(initial.id, SliceSplitRequest(split_at_seconds=0.5))

        result = self.repository.cleanup_project_media("phase1-demo")

        self.assertEqual(result.deleted_slice_count, 1)
        self.assertGreaterEqual(result.deleted_variant_count, 2)
        self.assertGreaterEqual(result.deleted_file_count, 1)
        self.assertIn(updated.active_variant_id, result.deleted_variant_ids)

        with Session(self.repository.engine, expire_on_commit=False) as session:
            remaining_slices = session.exec(select(AudioVariant.slice_id)).all()
            self.assertNotIn(initial.id, remaining_slices)

    def test_variant_media_path_rejects_paths_outside_media_root(self) -> None:
        with Session(self.repository.engine, expire_on_commit=False) as session:
            variant = session.exec(select(AudioVariant)).first()
            self.assertIsNotNone(variant)
            variant.file_path = "/etc/passwd"
            session.add(variant)
            session.commit()
            variant_id = variant.id

        with self.assertRaises(ValueError):
            self.repository.get_variant_media_path(variant_id)

    def test_migrate_external_variant_media_rehomes_legacy_files(self) -> None:
        with TemporaryDirectory() as external_dir:
            external_path = Path(external_dir) / "legacy-variant.wav"
            external_path.write_bytes(
                self.repository._render_synthetic_wave_bytes(48000, 1, 157440 / 48000, "legacy-variant")
            )

            with Session(self.repository.engine, expire_on_commit=False) as session:
                variant = session.exec(select(AudioVariant)).first()
                self.assertIsNotNone(variant)
                variant.file_path = str(external_path)
                session.add(variant)
                session.commit()
                variant_id = variant.id

            self.repository._migrate_external_variant_media()
            materialized_path = self.repository.get_variant_media_path(variant_id)
            self.assertTrue(materialized_path.is_relative_to(self.repository.media_root.resolve()))
            self.assertTrue(materialized_path.exists())

            with Session(self.repository.engine, expire_on_commit=False) as session:
                updated_variant = session.get(AudioVariant, variant_id)
                self.assertIsNotNone(updated_variant)
                self.assertEqual(updated_variant.file_path, str(materialized_path))

    def test_create_audio_variant_ignores_client_supplied_id(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]
        detail = self.repository.get_slice_detail(initial.id)
        self.assertIsNotNone(detail.active_variant)
        self.assertIsNotNone(detail.active_variant_id)

        updated = self.repository.create_audio_variant(
            initial.id,
            AudioVariantCreate(
                id="../../escape-test",
                file_path=str(self.repository.get_variant_media_path(detail.active_variant_id)),
                sample_rate=detail.active_variant.sample_rate,
                num_samples=detail.active_variant.num_samples,
                generator_model="manual-copy",
            ),
        )

        self.assertIsNotNone(updated.active_variant_id)
        self.assertNotEqual(updated.active_variant_id, "../../escape-test")

        with Session(self.repository.engine, expire_on_commit=False) as session:
            variant = session.get(AudioVariant, updated.active_variant_id)
            self.assertIsNotNone(variant)
            variant_path = Path(variant.file_path).resolve()

        self.assertTrue(variant_path.is_relative_to(self.repository.media_root.resolve()))
        self.assertEqual(variant_path.name, f"{updated.active_variant_id}.wav")

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
        self.assertEqual(len(migrated_repository.get_project_slices("project-a")), 1)
        self.assertEqual(len(migrated_repository.get_project_slices("project-b")), 1)

        with Session(migrated_repository.engine, expire_on_commit=False) as session:
            source_recordings = session.exec(select(SourceRecording)).all()

        self.assertEqual(len(source_recordings), 2)
        self.assertEqual({recording.batch_id for recording in source_recordings}, {"project-a", "project-b"})

    def test_blank_modified_transcript_stays_blank_for_export(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]

        self.repository.update_slice_status(initial.id, SliceStatusUpdate(status="accepted"))
        preview_before = self.repository.get_export_preview("phase1-demo")
        self.assertTrue(any(f"{initial.id}.wav|" in line for line in preview_before.lines))

        self.repository.update_slice_transcript(initial.id, SliceTranscriptUpdate(modified_text=""))
        preview_after = self.repository.get_export_preview("phase1-demo")

        self.assertFalse(any(f"{initial.id}.wav|" in line for line in preview_after.lines))

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

    def test_undo_redo_restores_metadata_history(self) -> None:
        initial = self.repository.get_slice_detail(self.repository.get_project_slices("phase1-demo")[0].id)
        original_text = initial.transcript.original_text if initial.transcript is not None else ""

        updated_transcript = self.repository.update_slice_transcript(
            initial.id,
            SliceTranscriptUpdate(modified_text="Corrected transcript text"),
        )
        updated_tags = self.repository.update_slice_tags(
            initial.id,
            SliceTagUpdate(tags=[TagPayload(name="qa-pass", color="#336699")]),
        )
        updated_status = self.repository.update_slice_status(
            initial.id,
            SliceStatusUpdate(status="accepted"),
        )

        self.assertEqual(updated_transcript.transcript.modified_text, "Corrected transcript text")
        self.assertEqual([tag.name for tag in updated_tags.tags], ["qa-pass"])
        self.assertEqual(updated_status.status, "accepted")

        undo_status = self.repository.undo_slice(initial.id)
        self.assertEqual(undo_status.status, "unresolved")
        self.assertEqual([tag.name for tag in undo_status.tags], ["qa-pass"])
        self.assertEqual(undo_status.transcript.modified_text, "Corrected transcript text")

        undo_tags = self.repository.undo_slice(initial.id)
        self.assertEqual(undo_tags.tags, [])
        self.assertEqual(undo_tags.transcript.modified_text, "Corrected transcript text")

        undo_transcript = self.repository.undo_slice(initial.id)
        self.assertEqual(undo_transcript.transcript.modified_text, None)
        self.assertEqual(undo_transcript.transcript.original_text, original_text)

        redo_transcript = self.repository.redo_slice(initial.id)
        self.assertEqual(redo_transcript.transcript.modified_text, "Corrected transcript text")

        redo_tags = self.repository.redo_slice(initial.id)
        self.assertEqual([tag.name for tag in redo_tags.tags], ["qa-pass"])

        redo_status = self.repository.redo_slice(initial.id)
        self.assertEqual(redo_status.status, "accepted")

    def test_human_transcript_edit_locks_slice(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]

        updated = self.repository.update_slice_transcript(
            initial.id,
            SliceTranscriptUpdate(modified_text="Human corrected transcript."),
        )

        self.assertTrue(updated.is_locked)
        refreshed = self.repository.get_slice_detail(initial.id)
        self.assertTrue(refreshed.is_locked)

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
        process_next_job(self.repository, worker_id="test-worker")

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

    def test_source_slicing_job_creates_direct_slices(self) -> None:
        source_path = self.repository.media_root / "sources" / "source-slice.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(
            self.repository._render_synthetic_wave_bytes(48000, 1, 12.0, "source-slice")
        )
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-source-slice",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=576000,
            )
        )
        artifact_dir = self.repository.exports_root / "recording-artifacts" / "src-source-slice"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        transcript_base = artifact_dir / "transcript.base.txt"
        transcript_text = artifact_dir / "transcript.txt"
        alignment_path = artifact_dir / "alignment.json"
        transcript_text.write_text("alpha beta gamma delta epsilon zeta eta theta\n", encoding="utf-8")
        transcript_base.write_text("alpha beta gamma delta epsilon zeta eta theta\n", encoding="utf-8")
        alignment_path.write_text(
            json.dumps(
                [
                    {"word": "alpha", "start": 0.5, "end": 1.0},
                    {"word": "beta", "start": 1.4, "end": 1.9},
                    {"word": "gamma", "start": 2.4, "end": 2.9},
                    {"word": "delta", "start": 3.8, "end": 4.3},
                    {"word": "epsilon", "start": 5.1, "end": 5.6},
                    {"word": "zeta", "start": 6.6, "end": 7.1},
                    {"word": "eta", "start": 8.0, "end": 8.5},
                    {"word": "theta", "start": 9.4, "end": 9.9},
                ],
                indent=2,
            ),
            encoding="utf-8",
        )
        self.repository.set_source_recording_artifact_paths(
            "src-source-slice",
            transcript_text_path=str(transcript_text),
            transcript_json_path=None,
            alignment_json_path=str(alignment_path),
            transcript_status="ok",
            alignment_status="ok",
            transcript_word_count=8,
            alignment_word_count=8,
            artifact_metadata={"base_transcript_text_path": str(transcript_base), "language": "en"},
        )

        job = self.repository.enqueue_source_slicing(
            "src-source-slice",
            SourceSlicingRequest(),
        )
        processed = process_next_job(self.repository, worker_id="test-worker")

        self.assertTrue(processed)
        latest = self.repository.get_processing_job(job.id)
        self.assertEqual(latest.status, "completed")
        slices = [slice_row for slice_row in self.repository.get_project_slices("phase1-demo") if slice_row.source_recording_id == "src-source-slice"]
        self.assertGreaterEqual(len(slices), 1)
        detail = self.repository.get_slice_detail(slices[0].id)
        self.assertIn("source_word_start_index", detail.model_metadata)
        self.assertIn("training_start", detail.model_metadata)
        self.assertEqual(detail.transcript.alignment_data["kind"], "source_slicer_alignment")

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
            recording = session.get(SourceRecording, "src-source-patch")
            self.assertIsNotNone(recording)
            slice_row = self.repository._create_slice_from_source_span(
                session,
                recording,
                slice_id="slice-source-patch",
                start_seconds=0.0,
                end_seconds=1.0,
                transcript_text="alpha beta",
                order_index=0,
                extra_metadata={
                    "source_word_start_index": 0,
                    "source_word_end_index": 1,
                },
            )
            session.commit()

        updated = self.repository.update_slice_transcript(
            "slice-source-patch",
            SliceTranscriptUpdate(modified_text="alpha better"),
        )

        self.assertTrue(updated.is_locked)
        artifact = self.repository.get_source_recording_artifact("src-source-patch")
        self.assertEqual(artifact.transcript_status, "patched")
        self.assertEqual(artifact.alignment_status, "stale")
        self.assertEqual(Path(artifact.transcript_text_path).read_text(encoding="utf-8").strip(), "alpha better gamma delta")

    def test_metadata_only_revisions_reuse_audio_cache_and_public_models_hide_paths(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]
        self.repository.get_waveform_peaks(initial.id, 960)
        media_before = self.repository.get_slice_media_path(initial.id)

        with Session(self.repository.engine, expire_on_commit=False) as session:
            slice_row_before = self.repository._get_loaded_slice(session, initial.id)
            peaks_before = self.repository._waveform_peaks_cache_path(slice_row_before, 960)

        updated = self.repository.update_slice_transcript(
            initial.id,
            SliceTranscriptUpdate(modified_text="Metadata-only update"),
        )
        media_after = self.repository.get_slice_media_path(initial.id)

        with Session(self.repository.engine, expire_on_commit=False) as session:
            slice_row_after = self.repository._get_loaded_slice(session, initial.id)
            peaks_after = self.repository._waveform_peaks_cache_path(slice_row_after, 960)

        self.assertEqual(media_before, media_after)
        self.assertEqual(peaks_before, peaks_after)
        self.assertTrue(peaks_after.exists())

        summary_payload = initial.model_dump(mode="json")
        detail_payload = updated.model_dump(mode="json")
        self.assertNotIn("variants", summary_payload)
        self.assertNotIn("alignment_data", summary_payload["transcript"])
        self.assertNotIn("file_path", json.dumps(summary_payload))
        self.assertNotIn("file_path", json.dumps(detail_payload))

    def test_cleanup_project_media_prunes_stale_slice_and_peak_caches(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]
        self.repository.get_waveform_peaks(initial.id, 960)
        old_media_path = self.repository.get_slice_media_path(initial.id)

        with Session(self.repository.engine, expire_on_commit=False) as session:
            old_slice_row = self.repository._get_loaded_slice(session, initial.id)
            old_peaks_path = self.repository._waveform_peaks_cache_path(old_slice_row, 960)

        updated = self.repository.append_edl_operation(
            initial.id,
            SliceEdlUpdate(
                op="insert_silence",
                range={"start_seconds": 0.1, "end_seconds": 0.1},
                duration_seconds=0.2,
            ),
        )
        new_media_path = self.repository.get_slice_media_path(initial.id)
        self.repository.get_waveform_peaks(updated.id, 960)

        with Session(self.repository.engine, expire_on_commit=False) as session:
            new_slice_row = self.repository._get_loaded_slice(session, initial.id)
            new_peaks_path = self.repository._waveform_peaks_cache_path(new_slice_row, 960)

        self.assertNotEqual(old_media_path, new_media_path)
        self.assertTrue(new_media_path.exists())
        self.assertTrue(new_peaks_path.exists())

        result = self.repository.cleanup_project_media("phase1-demo")

        self.assertGreaterEqual(result.deleted_file_count, 1)
        self.assertFalse(old_media_path.exists())
        self.assertFalse(old_peaks_path.exists())
        self.assertTrue(new_media_path.exists())
        self.repository.get_waveform_peaks(updated.id, 960)
        self.assertTrue(new_peaks_path.exists())

    def test_restart_does_not_rewrite_intentional_blank_revision_history(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]

        self.repository.update_slice_transcript(initial.id, SliceTranscriptUpdate(modified_text=""))
        self.repository.update_slice_transcript(initial.id, SliceTranscriptUpdate(modified_text="nonblank now"))
        self.repository.update_slice_tags(initial.id, SliceTagUpdate(tags=[]))
        self.repository.update_slice_tags(
            initial.id,
            SliceTagUpdate(tags=[TagPayload(name="later", color="#111111")]),
        )

        with Session(self.repository.engine, expire_on_commit=False) as session:
            commits = session.exec(
                select(EditCommit).where(EditCommit.slice_id == initial.id).order_by(EditCommit.created_at)
            ).all()
            blank_commit = [commit for commit in commits if commit.transcript_text == ""][-1]
            empty_tags_commit = [commit for commit in commits if commit.tags_payload == []][-1]
            blank_id = blank_commit.id
            empty_tags_id = empty_tags_commit.id

        with self.repository.engine.begin() as connection:
            connection.exec_driver_sql("PRAGMA user_version = 0")

        restarted = SQLiteRepository(
            db_path=Path(self.temp_dir.name) / "project.db",
            legacy_seed_path=Path(self.temp_dir.name) / "missing-seed.json",
            media_root=Path(self.temp_dir.name) / "media",
            exports_root=Path(self.temp_dir.name) / "exports",
        )
        self.addCleanup(restarted.close)

        with Session(restarted.engine, expire_on_commit=False) as session:
            blank_commit = session.get(EditCommit, blank_id)
            empty_tags_commit = session.get(EditCommit, empty_tags_id)
            self.assertEqual(blank_commit.transcript_text, "")
            self.assertEqual(empty_tags_commit.tags_payload, [])

    def test_legacy_revision_migration_backfills_only_legacy_rows(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]
        self.repository.update_slice_status(initial.id, SliceStatusUpdate(status="accepted"))

        with Session(self.repository.engine, expire_on_commit=False) as session:
            commit = session.exec(
                select(EditCommit).where(EditCommit.slice_id == initial.id).order_by(EditCommit.created_at)
            ).first()
            self.assertIsNotNone(commit)
            commit.transcript_text = ""
            commit.tags_payload = []
            commit.status = ReviewStatus.UNRESOLVED
            commit.active_variant_id_snapshot = None
            commit.message = None
            session.add(commit)
            session.commit()

        with self.repository.engine.begin() as connection:
            connection.exec_driver_sql("PRAGMA user_version = 0")

        restarted = SQLiteRepository(
            db_path=Path(self.temp_dir.name) / "project.db",
            legacy_seed_path=Path(self.temp_dir.name) / "missing-seed.json",
            media_root=Path(self.temp_dir.name) / "media",
            exports_root=Path(self.temp_dir.name) / "exports",
        )
        self.addCleanup(restarted.close)

        with Session(restarted.engine, expire_on_commit=False) as session:
            commit = session.exec(
                select(EditCommit).where(EditCommit.slice_id == initial.id).order_by(EditCommit.created_at)
            ).first()
            self.assertEqual(commit.transcript_text, "The workstation should make this painless.")
            self.assertEqual(commit.tags_payload, [])
            self.assertEqual(commit.status, ReviewStatus.ACCEPTED)
            self.assertEqual(commit.active_variant_id_snapshot, initial.active_variant_id)
            self.assertEqual(commit.message, "Imported slice baseline")

        restarted_again = SQLiteRepository(
            db_path=Path(self.temp_dir.name) / "project.db",
            legacy_seed_path=Path(self.temp_dir.name) / "missing-seed.json",
            media_root=Path(self.temp_dir.name) / "media",
            exports_root=Path(self.temp_dir.name) / "exports",
        )
        self.addCleanup(restarted_again.close)

        with Session(restarted_again.engine, expire_on_commit=False) as session:
            commit = session.exec(
                select(EditCommit).where(EditCommit.slice_id == initial.id).order_by(EditCommit.created_at)
            ).first()
            self.assertEqual(commit.status, ReviewStatus.ACCEPTED)
            self.assertEqual(commit.message, "Imported slice baseline")

    def test_cleanup_preserves_variants_referenced_by_surviving_revisions(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]
        original_variant_id = initial.active_variant_id
        self.assertIsNotNone(original_variant_id)

        generated = self.repository.run_audio_variant(
            initial.id,
            AudioVariantRunRequest(generator_model="deepfilternet"),
        )
        generated_variant_id = generated.active_variant_id
        self.repository.set_active_variant(
            initial.id,
            ActiveVariantUpdate(active_variant_id=original_variant_id),
        )

        result = self.repository.cleanup_project_media("phase1-demo")

        self.assertNotIn(generated_variant_id, result.deleted_variant_ids)
        restored = self.repository.undo_slice(initial.id)
        self.assertEqual(restored.active_variant_id, generated_variant_id)
        self.assertIsNotNone(restored.active_variant)
        self.assertTrue(self.repository.get_variant_media_path(generated_variant_id).exists())

    def test_save_slice_state_skips_no_op_non_milestone_revision(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]

        with Session(self.repository.engine, expire_on_commit=False) as session:
            before_count = len(
                session.exec(select(EditCommit).where(EditCommit.slice_id == initial.id)).all()
            )

        self.repository.save_slice_state(initial.id, SliceSaveRequest())

        with Session(self.repository.engine, expire_on_commit=False) as session:
            after_count = len(
                session.exec(select(EditCommit).where(EditCommit.slice_id == initial.id)).all()
            )

        self.assertEqual(after_count, before_count)

    def test_save_slice_state_rejects_no_op_message_without_milestone(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]

        with Session(self.repository.engine, expire_on_commit=False) as session:
            before_count = len(
                session.exec(select(EditCommit).where(EditCommit.slice_id == initial.id)).all()
            )

        with self.assertRaisesRegex(SliceSaveValidationError, "message requires milestone or state change"):
            self.repository.save_slice_state(initial.id, SliceSaveRequest(message="note only"))

        with Session(self.repository.engine, expire_on_commit=False) as session:
            after_count = len(
                session.exec(select(EditCommit).where(EditCommit.slice_id == initial.id)).all()
            )

        self.assertEqual(after_count, before_count)

    def test_save_slice_state_allows_no_op_milestone_with_message(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]

        with Session(self.repository.engine, expire_on_commit=False) as session:
            before_commits = session.exec(
                select(EditCommit).where(EditCommit.slice_id == initial.id).order_by(EditCommit.created_at)
            ).all()

        saved = self.repository.save_slice_state(
            initial.id,
            SliceSaveRequest(message="Saved slice milestone", is_milestone=True),
        )

        with Session(self.repository.engine, expire_on_commit=False) as session:
            after_commits = session.exec(
                select(EditCommit).where(EditCommit.slice_id == initial.id).order_by(EditCommit.created_at)
            ).all()

        self.assertEqual(len(after_commits), len(before_commits) + 1)
        latest_commit = after_commits[-1]
        self.assertEqual(saved.active_commit_id, latest_commit.id)
        self.assertEqual(latest_commit.message, "Saved slice milestone")
        self.assertTrue(latest_commit.is_milestone)

    def test_source_recording_window_media_path_materializes_audio_from_master_recording(self) -> None:
        media_path = self.repository.get_source_recording_window_media_path("src-001", 12.4, 15.68)

        self.assertTrue(media_path.exists())
        self.assertAlmostEqual(read_wav_duration_seconds(media_path), 3.28, places=2)

    def test_clip_lab_item_loads_slice_with_slice_capabilities(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]

        item = self.repository.get_slice_clip_lab_item(initial.id)

        self.assertEqual(item.kind, "slice")
        self.assertEqual(item.id, initial.id)
        self.assertEqual(item.source_recording_id, initial.source_recording_id)
        self.assertEqual(item.status, initial.status)
        self.assertIsNotNone(item.transcript)
        self.assertIn("/media/slices/", item.audio_url)
        self.assertTrue(item.capabilities.can_save)
        self.assertFalse(item.capabilities.can_split)
        self.assertFalse(item.capabilities.can_merge)
        self.assertTrue(item.capabilities.can_edit_waveform)
        self.assertTrue(item.capabilities.can_run_processing)
        self.assertTrue(item.capabilities.can_switch_variants)
        self.assertFalse(item.capabilities.can_export)
        self.assertFalse(item.capabilities.can_finalize)
