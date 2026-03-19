import json
import wave
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from sqlmodel import Session, select

from app.models import (
    ActiveVariantUpdate,
    AudioVariant,
    AudioVariantCreate,
    AudioVariantRunRequest,
    EditCommit,
    SliceEdlUpdate,
    SliceSplitRequest,
    SliceStatusUpdate,
    SliceTagUpdate,
    SliceTranscriptUpdate,
    SourceRecording,
    TagPayload,
)
from app.repository import SQLiteRepository


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

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

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

        with Session(self.repository.engine, expire_on_commit=False) as session:
            new_slice_row = self.repository._get_loaded_slice(session, initial.id)
            new_peaks_path = self.repository._waveform_peaks_cache_path(new_slice_row, 960)

        self.assertNotEqual(old_media_path, new_media_path)
        self.assertTrue(new_media_path.exists())
        self.assertTrue(new_peaks_path.exists())

        result = self.repository.cleanup_project_media("phase1-demo")

        self.assertGreaterEqual(result.deleted_file_count, 2)
        self.assertFalse(old_media_path.exists())
        self.assertFalse(old_peaks_path.exists())
        self.assertTrue(new_media_path.exists())
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

        with Session(restarted.engine, expire_on_commit=False) as session:
            blank_commit = session.get(EditCommit, blank_id)
            empty_tags_commit = session.get(EditCommit, empty_tags_id)
            self.assertEqual(blank_commit.transcript_text, "")
            self.assertEqual(empty_tags_commit.tags_payload, [])

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

