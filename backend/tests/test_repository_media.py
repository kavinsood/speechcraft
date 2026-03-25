import json
import os
import subprocess
import time
import wave
from datetime import timedelta
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
    DatasetProcessingRunRequest,
    EditCommit,
    ForcedAlignAndPackRequest,
    JobStatus,
    ReviewWindow,
    ReviewWindowAsrRequest,
    ReviewWindowRevision,
    ReviewWindowVariant,
    ReviewStatus,
    Slice,
    SliceEdlUpdate,
    SliceSaveRequest,
    SliceSplitRequest,
    SliceStatusUpdate,
    SliceTagUpdate,
    SliceTranscriptUpdate,
    SlicerHandoffRequest,
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

    def _register_processing_run_windows(
        self,
        recording_id: str,
        *,
        windows: list[dict[str, object]] | None = None,
    ) -> list[ReviewWindow]:
        window_specs = windows or [
            {
                "start_seconds": 0.0,
                "end_seconds": 2.2,
                "rough_transcript": "First review window",
                "order_index": 0,
            },
            {
                "start_seconds": 2.4,
                "end_seconds": 4.9,
                "rough_transcript": "Second review window",
                "order_index": 1,
            },
        ]
        total_duration = max(float(spec["end_seconds"]) for spec in window_specs) + 1.0
        total_samples = int(48000 * total_duration)
        source_path = self.repository.media_root / "sources" / f"{recording_id}.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(
            self.repository._render_synthetic_wave_bytes(48000, 1, total_duration, recording_id)
        )
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id=recording_id,
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=total_samples,
            )
        )
        return self.repository.register_slicer_chunks(
            recording_id,
            SlicerHandoffRequest(
                windows=window_specs,
                pre_padding_ms=0,
                post_padding_ms=0,
                merge_gap_threshold_ms=0,
                minimum_window_duration_ms=100,
            ),
        )

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

    def test_slicer_handoff_registers_review_windows_without_creating_canonical_slices(self) -> None:
        source_path = self.repository.media_root / "sources" / "src-review-window.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(self.repository._render_synthetic_wave_bytes(48000, 1, 8.0, "src-review-window"))
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-review-window",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=384000,
            )
        )

        before_slice_count = len(self.repository.get_project_slices("phase1-demo"))
        windows = self.repository.register_slicer_chunks(
            "src-review-window",
            SlicerHandoffRequest(
                windows=[
                    {
                        "start_seconds": 0.0,
                        "end_seconds": 2.0,
                        "rough_transcript": "First rough window.",
                        "order_index": 0,
                    },
                    {
                        "start_seconds": 2.0,
                        "end_seconds": 5.0,
                        "rough_transcript": "Second rough window.",
                        "order_index": 1,
                    },
                ],
                pre_padding_ms=0,
                post_padding_ms=0,
                merge_gap_threshold_ms=0,
                minimum_window_duration_ms=100,
            ),
        )

        self.assertEqual(len(windows), 2)
        self.assertEqual([window.rough_transcript for window in windows], ["First rough window.", "Second rough window."])
        self.assertEqual(len(self.repository.list_review_windows("src-review-window")), 2)
        self.assertEqual(len(self.repository.get_project_slices("phase1-demo")), before_slice_count)

        with Session(self.repository.engine, expire_on_commit=False) as session:
            slice_count = len(session.exec(select(Slice).where(Slice.source_recording_id == "src-review-window")).all())
            revision_count = len(
                session.exec(
                    select(ReviewWindowRevision).join(ReviewWindow, ReviewWindow.id == ReviewWindowRevision.review_window_id).where(
                        ReviewWindow.source_recording_id == "src-review-window"
                    )
                ).all()
            )
            variant_count = len(
                session.exec(
                    select(ReviewWindowVariant).join(ReviewWindow, ReviewWindow.id == ReviewWindowVariant.review_window_id).where(
                        ReviewWindow.source_recording_id == "src-review-window"
                    )
                ).all()
            )
        self.assertEqual(slice_count, 0)
        self.assertEqual(revision_count, 2)
        self.assertEqual(variant_count, 2)

    def test_slicer_handoff_applies_conservative_padding_and_clamps_to_source_bounds(self) -> None:
        source_path = self.repository.media_root / "sources" / "src-review-window-padding.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(self.repository._render_synthetic_wave_bytes(48000, 1, 3.0, "src-review-window-padding"))
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-review-window-padding",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=144000,
            )
        )

        windows = self.repository.register_slicer_chunks(
            "src-review-window-padding",
            SlicerHandoffRequest(
                windows=[
                    {
                        "start_seconds": 0.05,
                        "end_seconds": 2.9,
                        "rough_transcript": "Pad this review window safely.",
                        "order_index": 4,
                    }
                ]
            ),
        )

        self.assertEqual(len(windows), 1)
        self.assertAlmostEqual(windows[0].start_seconds, 0.0, places=3)
        self.assertAlmostEqual(windows[0].end_seconds, 3.0, places=3)
        self.assertEqual(windows[0].order_index, 0)
        self.assertEqual(windows[0].window_metadata["generation_mode"], "slicer_handoff_normalized")
        self.assertEqual(windows[0].window_metadata["boundary_mode"], "coarse_review_window_padded")
        self.assertEqual(windows[0].window_metadata["pre_padding_ms"], 150)
        self.assertEqual(windows[0].window_metadata["post_padding_ms"], 250)
        self.assertEqual(windows[0].window_metadata["merged_gap_threshold_ms"], 200)
        self.assertEqual(windows[0].window_metadata["source_start_seconds"], 0.05)
        self.assertEqual(windows[0].window_metadata["source_end_seconds"], 2.9)
        self.assertFalse(windows[0].window_metadata["was_merged"])

    def test_slicer_handoff_merges_short_gap_windows_and_records_provenance(self) -> None:
        source_path = self.repository.media_root / "sources" / "src-review-window-merge.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(self.repository._render_synthetic_wave_bytes(48000, 1, 6.0, "src-review-window-merge"))
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-review-window-merge",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=288000,
            )
        )

        windows = self.repository.register_slicer_chunks(
            "src-review-window-merge",
            SlicerHandoffRequest(
                windows=[
                    {
                        "start_seconds": 1.0,
                        "end_seconds": 1.4,
                        "rough_transcript": "Hello",
                        "order_index": 9,
                        "model_metadata": {"speaker_name": "operator", "language": "en"},
                    },
                    {
                        "start_seconds": 1.55,
                        "end_seconds": 2.0,
                        "rough_transcript": "world",
                        "order_index": 2,
                        "model_metadata": {"speaker_name": "operator", "language": "en"},
                    },
                ]
            ),
        )

        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0].rough_transcript, "Hello world")
        self.assertAlmostEqual(windows[0].start_seconds, 0.85, places=2)
        self.assertAlmostEqual(windows[0].end_seconds, 2.25, places=2)
        self.assertEqual(windows[0].window_metadata["merged_input_count"], 2)
        self.assertTrue(windows[0].window_metadata["was_merged"])
        self.assertEqual(windows[0].window_metadata["source_order_indices"], [9, 2])
        self.assertEqual(windows[0].window_metadata["speaker_name"], "operator")
        self.assertEqual(windows[0].window_metadata["language"], "en")

    def test_slicer_handoff_rejects_pathological_tiny_windows_after_normalization(self) -> None:
        source_path = self.repository.media_root / "sources" / "src-review-window-tiny.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(self.repository._render_synthetic_wave_bytes(48000, 1, 2.0, "src-review-window-tiny"))
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-review-window-tiny",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=96000,
            )
        )

        with self.assertRaisesRegex(ValueError, "pathological tiny window"):
            self.repository.register_slicer_chunks(
                "src-review-window-tiny",
                SlicerHandoffRequest(
                    windows=[
                        {
                            "start_seconds": 1.0,
                            "end_seconds": 1.05,
                            "rough_transcript": "uh",
                            "order_index": 0,
                        }
                    ]
                ),
            )

    def test_slicer_handoff_sorts_and_stores_non_overlapping_windows(self) -> None:
        source_path = self.repository.media_root / "sources" / "src-review-window-order.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(self.repository._render_synthetic_wave_bytes(48000, 1, 8.0, "src-review-window-order"))
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-review-window-order",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=384000,
            )
        )

        windows = self.repository.register_slicer_chunks(
            "src-review-window-order",
            SlicerHandoffRequest(
                windows=[
                    {"start_seconds": 4.0, "end_seconds": 4.8, "rough_transcript": "Later", "order_index": 5},
                    {"start_seconds": 1.0, "end_seconds": 1.4, "rough_transcript": "First", "order_index": 8},
                    {"start_seconds": 1.45, "end_seconds": 1.8, "rough_transcript": "Second", "order_index": 1},
                ]
            ),
        )

        self.assertEqual(len(windows), 2)
        self.assertEqual([window.order_index for window in windows], [0, 1])
        self.assertLess(windows[0].start_seconds, windows[1].start_seconds)
        self.assertLessEqual(windows[0].end_seconds, windows[1].start_seconds)
        self.assertEqual(windows[0].rough_transcript, "First Second")
        self.assertEqual(windows[1].rough_transcript, "Later")

    def test_source_recording_window_media_path_materializes_audio_from_master_recording(self) -> None:
        media_path = self.repository.get_source_recording_window_media_path("src-001", 12.4, 15.68)

        self.assertTrue(media_path.exists())
        self.assertAlmostEqual(read_wav_duration_seconds(media_path), 3.28, places=2)

    def test_clip_lab_item_loads_slice_with_slice_capabilities(self) -> None:
        initial = self.repository.get_project_slices("phase1-demo")[0]

        item = self.repository.get_clip_lab_item("slice", initial.id)

        self.assertEqual(item.kind, "slice")
        self.assertEqual(item.id, initial.id)
        self.assertEqual(item.source_recording_id, initial.source_recording_id)
        self.assertEqual(item.status, initial.status)
        self.assertIsNotNone(item.transcript)
        self.assertIn("/media/slices/", item.audio_url)
        self.assertTrue(item.capabilities.can_save)
        self.assertTrue(item.capabilities.can_split)
        self.assertTrue(item.capabilities.can_merge)
        self.assertTrue(item.capabilities.can_edit_waveform)
        self.assertTrue(item.capabilities.can_run_processing)
        self.assertTrue(item.capabilities.can_switch_variants)
        self.assertFalse(item.capabilities.can_export)
        self.assertFalse(item.capabilities.can_finalize)

    def test_clip_lab_item_loads_review_window_with_honest_capabilities(self) -> None:
        source_path = self.repository.media_root / "sources" / "src-clip-lab-review-window.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(
            self.repository._render_synthetic_wave_bytes(48000, 1, 6.0, "src-clip-lab-review-window")
        )
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-clip-lab-review-window",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=288000,
            )
        )
        windows = self.repository.register_slicer_chunks(
            "src-clip-lab-review-window",
            SlicerHandoffRequest(
                windows=[
                    {
                        "start_seconds": 1.0,
                        "end_seconds": 4.5,
                        "rough_transcript": "Review this before packing.",
                        "order_index": 0,
                        "model_metadata": {"language": "en", "speaker_name": "reviewer"},
                    }
                ],
                pre_padding_ms=0,
                post_padding_ms=0,
                merge_gap_threshold_ms=0,
                minimum_window_duration_ms=100,
            ),
        )

        item = self.repository.get_clip_lab_item("review_window", windows[0].id)

        self.assertEqual(item.kind, "review_window")
        self.assertEqual(item.id, windows[0].id)
        self.assertEqual(item.source_recording_id, "src-clip-lab-review-window")
        self.assertAlmostEqual(item.start_seconds, 1.0, places=2)
        self.assertAlmostEqual(item.end_seconds, 4.5, places=2)
        self.assertEqual(item.status, "unresolved")
        self.assertIsNotNone(item.transcript)
        self.assertEqual(item.transcript.original_text, "Review this before packing.")
        self.assertEqual(item.speaker_name, "reviewer")
        self.assertEqual(item.language, "en")
        self.assertIn("/media/review-windows/", item.audio_url)
        self.assertTrue(item.capabilities.can_save)
        self.assertTrue(item.capabilities.can_split)
        self.assertTrue(item.capabilities.can_merge)
        self.assertTrue(item.capabilities.can_edit_waveform)
        self.assertTrue(item.capabilities.can_run_processing)
        self.assertTrue(item.capabilities.can_switch_variants)
        self.assertFalse(item.capabilities.can_export)
        self.assertFalse(item.capabilities.can_finalize)
        self.assertEqual(item.transcript_source, "review_window_seed")
        self.assertTrue(item.can_run_asr)
        self.assertIsNotNone(item.asr_placeholder_message)
        self.assertIsNone(item.asr_draft_transcript)
        self.assertEqual(len(item.variants), 1)
        self.assertEqual(len(item.commits), 1)

    def test_review_window_transcript_save_persists_reviewed_state(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        updated = self.repository.update_review_window_transcript(
            windows[0].id,
            SliceTranscriptUpdate(modified_text="Corrected review window transcript."),
        )

        self.assertEqual(updated.kind, "review_window")
        self.assertIsNotNone(updated.transcript)
        self.assertEqual(updated.transcript.modified_text, "Corrected review window transcript.")
        self.assertEqual(updated.transcript.original_text, "The workstation should make this painless.")
        self.assertEqual(updated.transcript_source, "manual")
        self.assertTrue(updated.can_undo)

        listed = self.repository.list_project_review_windows("phase1-demo")
        matching = next(window for window in listed if window.id == windows[0].id)
        self.assertEqual(matching.reviewed_transcript, "Corrected review window transcript.")
        self.assertEqual(matching.review_status, "unresolved")

    def test_enqueue_review_window_asr_creates_pending_job(self) -> None:
        windows = self.repository.list_review_windows("src-001")

        job = self.repository.enqueue_review_window_asr(
            "src-001",
            ReviewWindowAsrRequest(
                review_window_ids=[windows[0].id],
                model_name="stub-review-window-asr",
                model_version="2026.03",
                language_hint="en",
            ),
        )

        self.assertEqual(job.status, "pending")
        self.assertEqual(job.kind, "review_window_asr")
        self.assertEqual(job.input_payload["target_kind"], "review_window")
        self.assertEqual(job.input_payload["review_window_ids"], [windows[0].id])
        self.assertEqual(job.input_payload["model_name"], "stub-review-window-asr")
        self.assertEqual(job.input_payload["model_version"], "2026.03")
        self.assertEqual(job.input_payload["language_hint"], "en")

    def test_worker_executes_review_window_asr_and_stores_draft_transcript(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        job = self.repository.enqueue_review_window_asr(
            "src-001",
            ReviewWindowAsrRequest(
                review_window_ids=[windows[0].id],
                model_name="stub-review-window-asr",
                model_version="2026.03",
                language_hint="en",
            ),
        )

        with patch.dict(os.environ, {"ASR_BACKEND": "stub"}, clear=False):
            processed = process_next_job(self.repository, "worker-asr")

        self.assertTrue(processed)
        latest = self.repository.get_processing_job(job.id)
        self.assertEqual(latest.status, "completed")
        self.assertEqual(latest.output_payload["backend"], "stub")
        self.assertEqual(latest.output_payload["stored_as"], "review_window_asr_draft")
        self.assertEqual(latest.output_payload["processed_review_window_count"], 1)
        self.assertEqual(latest.output_payload["review_window_results"][0]["backend"], "stub")
        self.assertEqual(
            latest.output_payload["review_window_results"][0]["segments"],
            [{"start": 0.0, "end": 3.28, "text": "The workstation should make this painless."}],
        )

        item = self.repository.get_clip_lab_item("review_window", windows[0].id)
        self.assertEqual(item.transcript_source, "review_window_seed")
        self.assertIsNotNone(item.transcript)
        self.assertEqual(item.transcript.draft_text, "The workstation should make this painless.")
        self.assertEqual(item.transcript.draft_source, "review_window_asr")
        self.assertEqual(item.asr_draft_transcript, "The workstation should make this painless.")
        self.assertEqual(item.last_asr_job_id, job.id)
        self.assertIsNotNone(item.last_asr_at)
        self.assertEqual(item.asr_model_name, "stub-review-window-asr")
        self.assertEqual(item.asr_model_version, "2026.03")
        self.assertEqual(item.asr_language, "en")

        listed = self.repository.list_review_windows("src-001")
        matching = next(window for window in listed if window.id == windows[0].id)
        self.assertEqual(matching.transcript_source, "review_window_seed")
        self.assertEqual(matching.asr_draft_transcript, "The workstation should make this painless.")
        self.assertEqual(matching.last_asr_job_id, job.id)
        self.assertEqual(matching.asr_model_name, "stub-review-window-asr")

    def test_start_dataset_processing_run_creates_asr_jobs(self) -> None:
        windows = self._register_processing_run_windows("src-dataset-run-start")

        run = self.repository.start_dataset_processing_run(
            "src-dataset-run-start",
            DatasetProcessingRunRequest(
                review_window_ids=[window.id for window in windows],
                model_name="stub-review-window-asr",
                model_version="2026.03",
                language_hint="en",
            ),
        )

        self.assertEqual(run.status, "asr_running")
        self.assertEqual(run.phase, "asr")
        self.assertEqual(run.total_review_windows, 2)
        self.assertEqual(run.asr_total, 2)
        self.assertEqual(run.alignment_total, 0)
        self.assertEqual(run.asr_completed, 0)
        self.assertEqual(run.asr_failed, 0)
        self.assertFalse(run.health_page_ready)

        jobs = self.repository.list_source_recording_jobs("src-dataset-run-start")
        self.assertEqual(len(jobs), 2)
        self.assertTrue(all(job.kind == "review_window_asr" for job in jobs))
        self.assertTrue(all(job.status == "pending" for job in jobs))

    def test_dataset_processing_run_tracks_progress_and_auto_starts_alignment(self) -> None:
        windows = self._register_processing_run_windows("src-dataset-run-progress")
        run = self.repository.start_dataset_processing_run(
            "src-dataset-run-progress",
            DatasetProcessingRunRequest(review_window_ids=[window.id for window in windows]),
        )
        self.assertEqual(run.phase, "asr")

        with patch.dict(os.environ, {"ASR_BACKEND": "stub"}, clear=False):
            self.assertTrue(process_next_job(self.repository, "worker-dataset-asr-1"))
        after_first_asr = self.repository.get_source_recording_processing_status("src-dataset-run-progress")
        self.assertEqual(after_first_asr.status, "asr_running")
        self.assertEqual(after_first_asr.phase, "asr")
        self.assertEqual(after_first_asr.asr_completed, 1)
        self.assertEqual(after_first_asr.asr_failed, 0)
        self.assertEqual(after_first_asr.alignment_total, 0)
        self.assertFalse(after_first_asr.health_page_ready)

        with patch.dict(os.environ, {"ASR_BACKEND": "stub"}, clear=False):
            self.assertTrue(process_next_job(self.repository, "worker-dataset-asr-2"))
        after_asr_terminal = self.repository.get_source_recording_processing_status("src-dataset-run-progress")
        self.assertEqual(after_asr_terminal.status, "alignment_running")
        self.assertEqual(after_asr_terminal.phase, "alignment")
        self.assertEqual(after_asr_terminal.asr_completed, 2)
        self.assertEqual(after_asr_terminal.asr_failed, 0)
        self.assertEqual(after_asr_terminal.alignment_total, 2)
        self.assertEqual(after_asr_terminal.alignment_completed, 0)
        self.assertEqual(after_asr_terminal.alignment_failed, 0)
        self.assertFalse(after_asr_terminal.health_page_ready)

        with patch("app.repository.subprocess.run", side_effect=self._fake_forced_align_subprocess):
            self.assertTrue(process_next_job(self.repository, "worker-dataset-align-1"))
        after_first_alignment = self.repository.get_source_recording_processing_status("src-dataset-run-progress")
        self.assertEqual(after_first_alignment.status, "alignment_running")
        self.assertEqual(after_first_alignment.phase, "alignment")
        self.assertEqual(after_first_alignment.alignment_total, 2)
        self.assertEqual(after_first_alignment.alignment_completed, 1)
        self.assertEqual(after_first_alignment.alignment_failed, 0)
        self.assertFalse(after_first_alignment.health_page_ready)

        with patch("app.repository.subprocess.run", side_effect=self._fake_forced_align_subprocess):
            self.assertTrue(process_next_job(self.repository, "worker-dataset-align-2"))
        completed = self.repository.get_source_recording_processing_status("src-dataset-run-progress")
        self.assertEqual(completed.status, "completed")
        self.assertEqual(completed.phase, "done")
        self.assertEqual(completed.alignment_total, 2)
        self.assertEqual(completed.alignment_completed, 2)
        self.assertEqual(completed.alignment_failed, 0)
        self.assertTrue(completed.health_page_ready)
        self.assertIsNotNone(completed.completed_at)

    def test_dataset_processing_run_reports_partial_failure_when_alignment_fails(self) -> None:
        windows = self._register_processing_run_windows("src-dataset-run-partial")
        self.repository.start_dataset_processing_run(
            "src-dataset-run-partial",
            DatasetProcessingRunRequest(review_window_ids=[window.id for window in windows]),
        )

        with patch.dict(os.environ, {"ASR_BACKEND": "stub"}, clear=False):
            self.assertTrue(process_next_job(self.repository, "worker-partial-asr-1"))
            self.assertTrue(process_next_job(self.repository, "worker-partial-asr-2"))

        align_call_count = {"count": 0}

        def fail_second_alignment(command: list[str], **kwargs: object) -> subprocess.CompletedProcess:
            align_call_count["count"] += 1
            if align_call_count["count"] == 2:
                raise ValueError("alignment failed for second review window")
            return self._fake_forced_align_subprocess(command, **kwargs)

        with patch("app.repository.subprocess.run", side_effect=fail_second_alignment):
            self.assertTrue(process_next_job(self.repository, "worker-partial-align-1"))
            self.assertTrue(process_next_job(self.repository, "worker-partial-align-2"))

        run = self.repository.get_source_recording_processing_status("src-dataset-run-partial")
        self.assertEqual(run.status, "partially_failed")
        self.assertEqual(run.phase, "done")
        self.assertEqual(run.asr_completed, 2)
        self.assertEqual(run.asr_failed, 0)
        self.assertEqual(run.alignment_total, 2)
        self.assertEqual(run.alignment_completed, 1)
        self.assertEqual(run.alignment_failed, 1)
        self.assertTrue(run.health_page_ready)
        self.assertIsNotNone(run.completed_at)

    def test_review_window_asr_does_not_overwrite_reviewed_human_transcript(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        updated = self.repository.update_review_window_transcript(
            windows[0].id,
            SliceTranscriptUpdate(modified_text="Human reviewed transcript."),
        )
        self.assertEqual(updated.transcript_source, "manual")

        job = self.repository.enqueue_review_window_asr(
            "src-001",
            ReviewWindowAsrRequest(review_window_ids=[windows[0].id]),
        )

        with patch.dict(os.environ, {"ASR_BACKEND": "stub"}, clear=False):
            processed = process_next_job(self.repository, "worker-asr-human")

        self.assertTrue(processed)
        latest = self.repository.get_processing_job(job.id)
        self.assertEqual(latest.status, "completed")

        item = self.repository.get_clip_lab_item("review_window", windows[0].id)
        self.assertEqual(item.transcript_source, "manual")
        self.assertIsNotNone(item.transcript)
        self.assertEqual(item.transcript.modified_text, "Human reviewed transcript.")
        self.assertEqual(item.transcript.draft_text, "The workstation should make this painless.")
        self.assertEqual(item.asr_draft_transcript, "The workstation should make this painless.")
        self.assertNotEqual(item.transcript.modified_text, item.asr_draft_transcript)
        self.assertEqual(
            latest.output_payload["review_window_results"][0]["reviewed_transcript_protected"],
            True,
        )

    def test_enqueue_review_window_asr_rejects_duplicate_and_wrong_recording_windows(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        source_path = self.repository.media_root / "sources" / "src-review-window-asr-other.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(self.repository._render_synthetic_wave_bytes(48000, 1, 4.0, "src-review-window-asr-other"))
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-review-window-asr-other",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=192000,
            )
        )
        other_windows = self.repository.register_slicer_chunks(
            "src-review-window-asr-other",
            SlicerHandoffRequest(
                windows=[
                    {"start_seconds": 0.2, "end_seconds": 2.0, "rough_transcript": "Other recording", "order_index": 0}
                ],
                pre_padding_ms=0,
                post_padding_ms=0,
                merge_gap_threshold_ms=0,
                minimum_window_duration_ms=100,
            ),
        )

        with self.assertRaisesRegex(ValueError, "cannot contain duplicates"):
            self.repository.enqueue_review_window_asr(
                "src-001",
                ReviewWindowAsrRequest(review_window_ids=[windows[0].id, windows[0].id]),
            )

        with self.assertRaisesRegex(ValueError, "do not belong to the source recording"):
            self.repository.enqueue_review_window_asr(
                "src-001",
                ReviewWindowAsrRequest(review_window_ids=[windows[0].id, other_windows[0].id]),
            )

    def test_review_window_asr_backend_selection_uses_faster_whisper_when_configured(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        model_dir = Path(self.temp_dir.name) / "fake-fw-model"
        model_dir.mkdir(parents=True, exist_ok=True)
        job = self.repository.enqueue_review_window_asr(
            "src-001",
            ReviewWindowAsrRequest(
                review_window_ids=[windows[0].id],
                model_name="local-fw",
                model_version="dev-build",
                language_hint="en",
            ),
        )

        with patch.dict(
            os.environ,
            {
                "ASR_BACKEND": "faster_whisper",
                "ASR_MODEL_PATH": str(model_dir),
                "ASR_DEVICE": "cpu",
                "ASR_COMPUTE_TYPE": "int8",
            },
            clear=False,
        ):
            class FakeInfo:
                language = "en"

            class FakeSegment:
                start = 0.0
                end = 1.0
                text = "Local faster whisper result"

            class FakeModel:
                def transcribe(self, audio_path: str, **kwargs: object):
                    return iter([FakeSegment()]), FakeInfo()

            with patch.object(
                self.repository,
                "_load_faster_whisper_model",
                return_value=FakeModel(),
            ) as mocked_loader:
                processed = process_next_job(self.repository, "worker-fw")

        self.assertTrue(processed)
        mocked_loader.assert_called_once_with(
            model_path=str(model_dir),
            device="cpu",
            compute_type="int8",
        )
        latest = self.repository.get_processing_job(job.id)
        self.assertEqual(latest.status, "completed")
        self.assertEqual(latest.output_payload["backend"], "faster_whisper")
        self.assertEqual(latest.output_payload["review_window_results"][0]["segments"][0]["text"], "Local faster whisper result")

    def test_review_window_asr_faster_whisper_loads_model_once_per_job(self) -> None:
        source_path = self.repository.media_root / "sources" / "src-review-window-fw-multi.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(self.repository._render_synthetic_wave_bytes(48000, 1, 6.0, "src-review-window-fw-multi"))
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-review-window-fw-multi",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=288000,
            )
        )
        windows = self.repository.register_slicer_chunks(
            "src-review-window-fw-multi",
            SlicerHandoffRequest(
                windows=[
                    {"start_seconds": 0.0, "end_seconds": 2.0, "rough_transcript": "first", "order_index": 0},
                    {"start_seconds": 2.2, "end_seconds": 4.4, "rough_transcript": "second", "order_index": 1},
                ],
                pre_padding_ms=0,
                post_padding_ms=0,
                merge_gap_threshold_ms=0,
                minimum_window_duration_ms=100,
            ),
        )
        model_dir = Path(self.temp_dir.name) / "fake-fw-model-multi"
        model_dir.mkdir(parents=True, exist_ok=True)
        job = self.repository.enqueue_review_window_asr(
            "src-review-window-fw-multi",
            ReviewWindowAsrRequest(
                review_window_ids=[windows[0].id, windows[1].id],
                model_name="local-fw",
                model_version="dev-build",
                language_hint="en",
            ),
        )

        class FakeInfo:
            language = "en"

        class FakeSegment:
            def __init__(self, text: str) -> None:
                self.start = 0.0
                self.end = 1.0
                self.text = text

        class FakeModel:
            def transcribe(self, audio_path: str, **kwargs: object):
                basename = Path(audio_path).name
                return iter([FakeSegment(f"fw {basename}")]), FakeInfo()

        with patch.dict(
            os.environ,
            {
                "ASR_BACKEND": "faster_whisper",
                "ASR_MODEL_PATH": str(model_dir),
                "ASR_DEVICE": "cpu",
                "ASR_COMPUTE_TYPE": "int8",
            },
            clear=False,
        ):
            with patch.object(self.repository, "_load_faster_whisper_model", return_value=FakeModel()) as mocked_loader:
                processed = process_next_job(self.repository, "worker-fw-multi")

        self.assertTrue(processed)
        mocked_loader.assert_called_once_with(
            model_path=str(model_dir),
            device="cpu",
            compute_type="int8",
        )
        latest = self.repository.get_processing_job(job.id)
        self.assertEqual(latest.status, "completed")
        self.assertEqual(latest.output_payload["backend"], "faster_whisper")
        self.assertEqual(len(latest.output_payload["review_window_results"]), 2)
        self.assertTrue(
            all(result["backend"] == "faster_whisper" for result in latest.output_payload["review_window_results"])
        )

    def test_review_window_asr_faster_whisper_requires_model_path(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        job = self.repository.enqueue_review_window_asr(
            "src-001",
            ReviewWindowAsrRequest(review_window_ids=[windows[0].id]),
        )

        with patch.dict(os.environ, {"ASR_BACKEND": "faster_whisper"}, clear=False):
            processed = process_next_job(self.repository, "worker-fw-missing-model")

        self.assertTrue(processed)
        latest = self.repository.get_processing_job(job.id)
        self.assertEqual(latest.status, "failed")
        self.assertIn("ASR_MODEL_PATH is required", latest.error_message or "")

    def test_review_window_asr_faster_whisper_local_smoke(self) -> None:
        if os.getenv("SPEECHCRAFT_RUN_FASTER_WHISPER_SMOKE") != "1":
            self.skipTest("Set SPEECHCRAFT_RUN_FASTER_WHISPER_SMOKE=1 to exercise the real faster-whisper adapter")
        if os.getenv("ASR_BACKEND") != "faster_whisper":
            self.skipTest("Set ASR_BACKEND=faster_whisper for the local smoke run")
        if not os.getenv("ASR_MODEL_PATH"):
            self.skipTest("Set ASR_MODEL_PATH to a local faster-whisper model for the smoke run")

        windows = self.repository.list_review_windows("src-001")
        job = self.repository.enqueue_review_window_asr(
            "src-001",
            ReviewWindowAsrRequest(
                review_window_ids=[windows[0].id],
                language_hint="en",
            ),
        )

        processed = process_next_job(self.repository, "worker-fw-smoke")

        self.assertTrue(processed)
        latest = self.repository.get_processing_job(job.id)
        self.assertEqual(latest.status, "completed")
        self.assertEqual(latest.output_payload["backend"], "faster_whisper")
        self.assertEqual(latest.output_payload["processed_review_window_count"], 1)
        self.assertIn("segments", latest.output_payload["review_window_results"][0])
        self.assertEqual(latest.output_payload["review_window_results"][0]["backend"], "faster_whisper")

    def test_review_window_status_tags_split_and_merge_work(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        window_id = windows[0].id

        tagged = self.repository.update_review_window_tags(
            window_id,
            SliceTagUpdate(tags=[TagPayload(name="qa-pass", color="#3c8452")]),
        )
        self.assertEqual([tag.name for tag in tagged.tags], ["qa-pass"])

        accepted = self.repository.update_review_window_status(
            window_id,
            SliceStatusUpdate(status=ReviewStatus.ACCEPTED),
        )
        self.assertEqual(accepted.status, "accepted")

        split_windows = self.repository.split_review_window(window_id, SliceSplitRequest(split_at_seconds=1.5))
        self.assertEqual(len(split_windows), 2)
        self.assertTrue(all(window.review_status == "unresolved" for window in split_windows))
        self.assertTrue(all(window.tags and window.tags[0].name == "qa-pass" for window in split_windows))
        self.assertAlmostEqual(split_windows[0].start_seconds, windows[0].start_seconds, places=2)
        self.assertAlmostEqual(split_windows[-1].end_seconds, windows[0].end_seconds, places=2)
        self.assertAlmostEqual(split_windows[0].end_seconds, split_windows[1].start_seconds, places=3)
        self.assertTrue(split_windows[0].reviewed_transcript.strip())
        self.assertTrue(split_windows[1].reviewed_transcript.strip())

        merged_windows = self.repository.merge_with_next_review_window(split_windows[0].id)
        self.assertEqual(len(merged_windows), 1)
        self.assertEqual(merged_windows[0].tags[0].name, "qa-pass")
        self.assertAlmostEqual(merged_windows[0].start_seconds, windows[0].start_seconds, places=2)
        self.assertAlmostEqual(merged_windows[0].end_seconds, windows[0].end_seconds, places=2)
        self.assertTrue(merged_windows[0].reviewed_transcript.strip())

    def test_forced_align_and_pack_rejects_non_contiguous_review_window_selection(self) -> None:
        source_path = self.repository.media_root / "sources" / "src-noncontiguous.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(self.repository._render_synthetic_wave_bytes(48000, 1, 8.0, "src-noncontiguous"))
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-noncontiguous",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=384000,
            )
        )
        windows = self.repository.register_slicer_chunks(
            "src-noncontiguous",
            SlicerHandoffRequest(
                windows=[
                    {"start_seconds": 0.0, "end_seconds": 1.0, "rough_transcript": "First", "order_index": 0},
                    {"start_seconds": 2.0, "end_seconds": 3.0, "rough_transcript": "Second", "order_index": 1},
                ],
                pre_padding_ms=0,
                post_padding_ms=0,
                merge_gap_threshold_ms=0,
                minimum_window_duration_ms=100,
            ),
        )

        with self.assertRaisesRegex(ValueError, "contiguous review windows"):
            self.repository.enqueue_forced_align_and_pack(
                "src-noncontiguous",
                ForcedAlignAndPackRequest(
                    transcript_text="First Second",
                    review_window_ids=[windows[0].id, windows[1].id],
                ),
            )

    def test_forced_align_and_pack_rejects_unsupported_minimum_duration_override(self) -> None:
        with self.assertRaisesRegex(ValueError, "minimum_duration_seconds is fixed at 6.0"):
            self.repository.enqueue_forced_align_and_pack(
                "src-001",
                ForcedAlignAndPackRequest(
                    transcript_text="The workstation should make this painless.",
                    minimum_duration_seconds=5.0,
                ),
            )

    def test_review_window_read_paths_do_not_create_baseline_state(self) -> None:
        source_path = self.repository.media_root / "sources" / "src-read-only-window.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(self.repository._render_synthetic_wave_bytes(48000, 1, 4.0, "src-read-only-window"))
        self.repository.create_source_recording(
            SourceRecordingCreate(
                id="src-read-only-window",
                batch_id="phase1-demo",
                file_path=str(source_path),
                sample_rate=48000,
                num_channels=1,
                num_samples=192000,
            )
        )
        with Session(self.repository.engine, expire_on_commit=False) as session:
            window = ReviewWindow(
                id="review-window-manual",
                source_recording_id="src-read-only-window",
                start_seconds=0.5,
                end_seconds=2.5,
                rough_transcript="Manual read path test",
                order_index=0,
            )
            session.add(window)
            session.commit()

        item = self.repository.get_clip_lab_item("review_window", "review-window-manual")
        peaks = self.repository.get_clip_lab_waveform_peaks("review_window", "review-window-manual", 64)
        media_path = self.repository.get_review_window_media_path("review-window-manual")

        self.assertEqual(item.transcript.original_text, "Manual read path test")
        self.assertEqual(peaks.clip_id, "review-window-manual")
        self.assertTrue(media_path.exists())

        with Session(self.repository.engine, expire_on_commit=False) as session:
            self.assertEqual(
                len(session.exec(select(ReviewWindowRevision).where(ReviewWindowRevision.review_window_id == "review-window-manual")).all()),
                0,
            )
            self.assertEqual(
                len(session.exec(select(ReviewWindowVariant).where(ReviewWindowVariant.review_window_id == "review-window-manual")).all()),
                0,
            )

    def test_cleanup_project_media_prunes_stale_review_window_files(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        window_id = windows[0].id
        original_render_path = self.repository.get_review_window_media_path(window_id)
        with Session(self.repository.engine, expire_on_commit=False) as session:
            original_variant_path = Path(
                session.exec(
                    select(ReviewWindowVariant.file_path).where(ReviewWindowVariant.review_window_id == window_id)
                ).first()
            )
        self.assertTrue(original_render_path.exists())
        self.assertTrue(original_variant_path.exists())

        split_windows = self.repository.split_review_window(window_id, SliceSplitRequest(split_at_seconds=1.2))
        self.assertTrue(all(self.repository.get_review_window_media_path(window.id).exists() for window in split_windows))

        cleanup = self.repository.cleanup_project_media("phase1-demo")

        self.assertFalse(original_variant_path.exists())
        self.assertFalse(original_render_path.exists())
        self.assertGreaterEqual(cleanup.deleted_file_count, 2)

        with Session(self.repository.engine, expire_on_commit=False) as session:
            surviving_paths = {
                Path(raw_path)
                for raw_path in session.exec(select(ReviewWindowVariant.file_path)).all()
            }
        self.assertTrue(all(path.exists() for path in surviving_paths))

    def test_enqueue_forced_align_and_pack_creates_pending_job_without_executing_it(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        self.assertEqual(len(windows), 1)
        before_slices = self.repository.get_project_slices("phase1-demo")
        before_source_slices = [slice_row for slice_row in before_slices if slice_row.source_recording_id == "src-001"]

        job = self.repository.enqueue_forced_align_and_pack(
            "src-001",
            ForcedAlignAndPackRequest(
                transcript_text="The workstation should make this painless.",
                review_window_ids=[windows[0].id],
            ),
        )

        self.assertEqual(job.status, "pending")
        self.assertIsNone(job.started_at)
        self.assertIsNone(job.completed_at)
        self.assertEqual(self.repository.get_processing_job(job.id).status, "pending")
        self.assertEqual(self.repository.list_source_recording_jobs("src-001")[0].status, "pending")
        after_slices = self.repository.get_project_slices("phase1-demo")
        after_source_slices = [slice_row for slice_row in after_slices if slice_row.source_recording_id == "src-001"]
        self.assertEqual(len(after_source_slices), len(before_source_slices))

    def test_worker_claims_and_runs_pending_job(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        before_slices = self.repository.get_project_slices("phase1-demo")
        before_source_slices = [slice_row for slice_row in before_slices if slice_row.source_recording_id == "src-001"]

        def fake_aligner_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess:
            output_index = command.index("--output") + 1
            output_path = Path(command[output_index])
            output_path.write_text(
                json.dumps(
                    [
                        {"word": "The", "start": 0.0, "end": 0.45},
                        {"word": "workstation", "start": 0.45, "end": 1.3},
                        {"word": "should", "start": 1.3, "end": 1.85},
                        {"word": "make", "start": 1.85, "end": 2.25},
                        {"word": "this", "start": 2.25, "end": 2.6},
                        {"word": "painless.", "start": 2.6, "end": 3.28},
                    ]
                ),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        with patch("app.repository.subprocess.run", side_effect=fake_aligner_run):
            job = self.repository.enqueue_forced_align_and_pack(
                "src-001",
                ForcedAlignAndPackRequest(
                    transcript_text="The workstation should make this painless.",
                    review_window_ids=[windows[0].id],
                ),
            )

            self.assertEqual(job.status, "pending")
            claimed = self.repository.claim_next_processing_job("worker-test")
            self.assertIsNotNone(claimed)
            assert claimed is not None
            self.assertEqual(claimed.id, job.id)
            self.assertEqual(claimed.status, "running")
            self.assertEqual(claimed.claimed_by, "worker-test")
            self.assertIsNotNone(claimed.heartbeat_at)

            latest = self.repository.run_claimed_processing_job(claimed.id, worker_id="worker-test")

        self.assertEqual(latest.status, "completed")
        self.assertIsNone(latest.error_message)
        self.assertIsNotNone(latest.output_payload)
        self.assertEqual(latest.output_payload["created_slice_count"], 1)
        created_slice_ids = latest.output_payload["created_slice_ids"]
        self.assertEqual(len(created_slice_ids), 1)
        self.assertEqual(self.repository.list_source_recording_jobs("src-001")[0].id, job.id)

        after_slices = self.repository.get_project_slices("phase1-demo")
        after_source_slices = [slice_row for slice_row in after_slices if slice_row.source_recording_id == "src-001"]
        self.assertEqual(len(after_source_slices), len(before_source_slices) + 1)

        created_slice = self.repository.get_slice_detail(created_slice_ids[0])
        self.assertEqual(created_slice.transcript.original_text, "The workstation should make this painless.")
        self.assertEqual(created_slice.source_recording.id, "src-001")
        self.assertAlmostEqual(created_slice.model_metadata["original_start_time"], 12.4, places=2)
        self.assertAlmostEqual(created_slice.model_metadata["original_end_time"], 15.68, places=2)

    def test_worker_marks_failing_job_failed(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        job = self.repository.enqueue_forced_align_and_pack(
            "src-001",
            ForcedAlignAndPackRequest(
                transcript_text="The workstation should make this painless.",
                review_window_ids=[windows[0].id],
            ),
        )

        with patch.object(
            self.repository,
            "_run_forced_align_worker",
            side_effect=ValueError("aligner exploded"),
        ):
            claimed = self.repository.claim_next_processing_job("worker-fail")
            self.assertIsNotNone(claimed)
            assert claimed is not None
            latest = self.repository.run_claimed_processing_job(claimed.id, worker_id="worker-fail")

        self.assertEqual(latest.status, "failed")
        self.assertIn("aligner exploded", latest.error_message or "")
        self.assertIsNotNone(latest.heartbeat_at)
        self.assertIsNotNone(latest.completed_at)

    def test_worker_does_not_reclaim_running_or_completed_jobs(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        first_job = self.repository.enqueue_forced_align_and_pack(
            "src-001",
            ForcedAlignAndPackRequest(
                transcript_text="The workstation should make this painless.",
                review_window_ids=[windows[0].id],
            ),
        )
        second_job = self.repository.enqueue_forced_align_and_pack(
            "src-001",
            ForcedAlignAndPackRequest(
                transcript_text="The workstation should make this painless.",
                review_window_ids=[windows[0].id],
            ),
        )

        claimed_first = self.repository.claim_next_processing_job("worker-a")
        self.assertIsNotNone(claimed_first)
        assert claimed_first is not None
        self.assertEqual(claimed_first.id, first_job.id)

        claimed_second = self.repository.claim_next_processing_job("worker-b")
        self.assertIsNotNone(claimed_second)
        assert claimed_second is not None
        self.assertEqual(claimed_second.id, second_job.id)

        self.assertIsNone(self.repository.claim_next_processing_job("worker-c"))

        with Session(self.repository.engine, expire_on_commit=False) as session:
            job = self.repository._get_processing_job(session, first_job.id)
            job.status = JobStatus.COMPLETED
            job.completed_at = self.repository._as_utc(job.created_at)
            session.add(job)
            session.commit()

        self.assertIsNone(self.repository.claim_next_processing_job("worker-d"))

    def test_process_next_job_helper_processes_single_pending_job(self) -> None:
        windows = self.repository.list_review_windows("src-001")

        def fake_aligner_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess:
            output_index = command.index("--output") + 1
            output_path = Path(command[output_index])
            output_path.write_text(
                json.dumps(
                    [
                        {"word": "The", "start": 0.0, "end": 0.45},
                        {"word": "workstation", "start": 0.45, "end": 1.3},
                        {"word": "should", "start": 1.3, "end": 1.85},
                        {"word": "make", "start": 1.85, "end": 2.25},
                        {"word": "this", "start": 2.25, "end": 2.6},
                        {"word": "painless.", "start": 2.6, "end": 3.28},
                    ]
                ),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        with patch("app.repository.subprocess.run", side_effect=fake_aligner_run):
            job = self.repository.enqueue_forced_align_and_pack(
                "src-001",
                ForcedAlignAndPackRequest(
                    transcript_text="The workstation should make this painless.",
                    review_window_ids=[windows[0].id],
                ),
            )

            processed = process_next_job(self.repository, "worker-helper")

        self.assertTrue(processed)
        latest = self.repository.get_processing_job(job.id)
        self.assertEqual(latest.status, "completed")
        self.assertEqual(latest.claimed_by, "worker-helper")

    def test_process_next_job_updates_heartbeat_while_running(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        job = self.repository.enqueue_forced_align_and_pack(
            "src-001",
            ForcedAlignAndPackRequest(
                transcript_text="The workstation should make this painless.",
                review_window_ids=[windows[0].id],
            ),
        )

        def slow_dispatch(job_id: str) -> dict[str, object]:
            time.sleep(0.2)
            return {"job_id": job_id, "ok": True}

        with patch.object(self.repository, "_dispatch_processing_job", side_effect=slow_dispatch):
            processed = process_next_job(
                self.repository,
                "worker-heartbeat",
                heartbeat_interval_seconds=0.05,
                stale_after_seconds=60.0,
            )

        self.assertTrue(processed)
        latest = self.repository.get_processing_job(job.id)
        self.assertEqual(latest.status, "completed")
        self.assertIsNotNone(latest.started_at)
        self.assertIsNotNone(latest.heartbeat_at)
        assert latest.started_at is not None
        assert latest.heartbeat_at is not None
        self.assertGreater(latest.heartbeat_at, latest.started_at)

    def test_claim_next_processing_job_fails_stale_running_job_before_claiming_pending(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        stale_job = self.repository.enqueue_forced_align_and_pack(
            "src-001",
            ForcedAlignAndPackRequest(
                transcript_text="The workstation should make this painless.",
                review_window_ids=[windows[0].id],
            ),
        )
        fresh_job = self.repository.enqueue_forced_align_and_pack(
            "src-001",
            ForcedAlignAndPackRequest(
                transcript_text="The workstation should make this painless.",
                review_window_ids=[windows[0].id],
            ),
        )

        stale_now = self.repository._as_utc(stale_job.created_at) + timedelta(seconds=120)
        claimed_stale = self.repository.claim_next_processing_job(
            "dead-worker",
            stale_after_seconds=60.0,
            now=stale_now,
        )
        self.assertIsNotNone(claimed_stale)
        assert claimed_stale is not None
        self.assertEqual(claimed_stale.id, stale_job.id)

        reclaimed = self.repository.claim_next_processing_job(
            "fresh-worker",
            stale_after_seconds=60.0,
            now=stale_now + timedelta(seconds=61),
        )
        self.assertIsNotNone(reclaimed)
        assert reclaimed is not None
        self.assertEqual(reclaimed.id, fresh_job.id)

        stale_latest = self.repository.get_processing_job(stale_job.id)
        self.assertEqual(stale_latest.status, "failed")
        self.assertIn("heartbeat timed out", stale_latest.error_message or "")
        self.assertIsNotNone(stale_latest.completed_at)

    def test_claim_next_processing_job_does_not_steal_non_stale_running_job(self) -> None:
        windows = self.repository.list_review_windows("src-001")
        job = self.repository.enqueue_forced_align_and_pack(
            "src-001",
            ForcedAlignAndPackRequest(
                transcript_text="The workstation should make this painless.",
                review_window_ids=[windows[0].id],
            ),
        )
        claim_time = self.repository._as_utc(job.created_at) + timedelta(seconds=5)
        claimed = self.repository.claim_next_processing_job(
            "worker-live",
            stale_after_seconds=60.0,
            now=claim_time,
        )
        self.assertIsNotNone(claimed)

        self.assertIsNone(
            self.repository.claim_next_processing_job(
                "worker-other",
                stale_after_seconds=60.0,
                now=claim_time + timedelta(seconds=30),
            )
        )
