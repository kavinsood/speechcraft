from __future__ import annotations

import io
import json
import math
import shutil
import wave
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy.orm import selectinload
from sqlmodel import Session, SQLModel, create_engine, delete, select

from .models import (
    AudioVariant,
    AudioVariantCreate,
    AudioVariantRunRequest,
    ActiveVariantUpdate,
    EditCommit,
    ExportRun,
    ExportPreview,
    ImportBatch,
    ImportBatchCreate,
    JobStatus,
    MediaCleanupResult,
    ReferenceAsset,
    ReferenceAssetCreate,
    ReviewStatus,
    SliceDetail,
    Slice,
    SliceEdlUpdate,
    SliceSplitRequest,
    SliceTagLink,
    SliceTagUpdate,
    SliceStatusUpdate,
    SliceTranscriptUpdate,
    TagPayload,
    SourceRecording,
    SourceRecordingCreate,
    RecordingDerivativeCreate,
    SlicerHandoffRequest,
    Tag,
    Transcript,
    WaveformPeaks,
    utc_now,
)


@dataclass
class SQLiteRepository:
    db_path: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "project.db"
    )
    legacy_seed_path: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "phase1-demo.json"
    )
    media_root: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "media"
    )
    exports_root: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "exports"
    )
    engine: Any = field(init=False)

    def __post_init__(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.media_root.mkdir(parents=True, exist_ok=True)
        self.exports_root.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine(
            f"sqlite:///{self.db_path}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)
        with self.engine.begin() as connection:
            connection.exec_driver_sql("DROP TABLE IF EXISTS slicecommit")
        self._seed_if_needed()

    def list_projects(self) -> list[ImportBatch]:
        with self._session() as session:
            batches = session.exec(select(ImportBatch)).all()
            return [self._normalize_batch(batch) for batch in sorted(batches, key=lambda item: item.created_at, reverse=True)]

    def get_project(self, project_id: str) -> ImportBatch:
        with self._session() as session:
            return self._normalize_batch(self._get_batch(session, project_id))

    def get_project_slices(self, project_id: str) -> list[SliceDetail]:
        with self._session() as session:
            self._get_batch(session, project_id)
            return [self._to_slice_detail(slice_row) for slice_row in self._get_batch_slices(session, project_id)]

    def list_export_runs(self, project_id: str) -> list[ExportRun]:
        with self._session() as session:
            self._get_batch(session, project_id)
            runs = session.exec(
                select(ExportRun).where(ExportRun.batch_id == project_id).order_by(ExportRun.created_at)
            ).all()
            return [self._normalize_export_run(run) for run in runs]

    def cleanup_project_media(self, project_id: str) -> MediaCleanupResult:
        with self._session() as session:
            self._get_batch(session, project_id)
            referenced_variant_ids = set(session.exec(select(ReferenceAsset.audio_variant_id)).all())
            deleted_slice_ids: list[str] = []
            deleted_variant_ids: list[str] = []
            deleted_paths: list[str] = []
            skipped_reference_count = 0

            superseded_slices = [
                slice_row
                for slice_row in self._get_all_batch_slices(session, project_id)
                if self._slice_metadata(slice_row).get("is_superseded", False)
            ]
            for slice_row in superseded_slices:
                variant_ids = {variant.id for variant in slice_row.variants}
                referenced_ids = variant_ids & referenced_variant_ids
                if referenced_ids:
                    skipped_reference_count += len(referenced_ids)
                    continue
                deleted_slice_ids.append(slice_row.id)
                deleted_variant_ids.extend(variant.id for variant in slice_row.variants)
                deleted_paths.extend(variant.file_path for variant in slice_row.variants)
                variant_ids = [variant.id for variant in slice_row.variants]
                commit_ids = [commit.id for commit in slice_row.commits]
                session.exec(delete(SliceTagLink).where(SliceTagLink.slice_id == slice_row.id))
                if variant_ids:
                    session.exec(delete(AudioVariant).where(AudioVariant.id.in_(variant_ids)))
                if commit_ids:
                    session.exec(delete(EditCommit).where(EditCommit.id.in_(commit_ids)))
                session.exec(delete(Transcript).where(Transcript.slice_id == slice_row.id))
                session.exec(delete(Slice).where(Slice.id == slice_row.id))

            session.flush()

            remaining_slices = self._get_all_batch_slices(session, project_id)
            for slice_row in remaining_slices:
                for variant in list(slice_row.variants):
                    if variant.id in referenced_variant_ids:
                        continue
                    if variant.id == slice_row.active_variant_id or variant.is_original:
                        continue
                    deleted_variant_ids.append(variant.id)
                    deleted_paths.append(variant.file_path)
                    session.delete(variant)

            session.commit()

        deleted_file_count = self._delete_unreferenced_media_files(deleted_paths)
        return MediaCleanupResult(
            project_id=project_id,
            deleted_slice_count=len(deleted_slice_ids),
            deleted_variant_count=len(deleted_variant_ids),
            deleted_file_count=deleted_file_count,
            skipped_reference_count=skipped_reference_count,
            deleted_slice_ids=deleted_slice_ids,
            deleted_variant_ids=deleted_variant_ids,
        )

    def update_slice_status(self, slice_id: str, payload: SliceStatusUpdate) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_slice(session, slice_id)
            slice_row.status = payload.status
            self._touch_slice(slice_row)
            session.add(slice_row)
            session.commit()
            session.expire_all()
            return self._get_slice_detail(session, slice_id)

    def update_slice_transcript(self, slice_id: str, payload: SliceTranscriptUpdate) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_slice(session, slice_id)
            transcript = self._get_transcript(session, slice_row.id)
            transcript.modified_text = payload.modified_text
            transcript.is_modified = payload.modified_text != transcript.original_text
            self._touch_slice(slice_row)
            session.add(transcript)
            session.add(slice_row)
            session.commit()
            session.expire_all()
            return self._get_slice_detail(session, slice_id)

    def update_slice_tags(self, slice_id: str, payload: SliceTagUpdate) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_slice(session, slice_id)
            self._replace_slice_tags(session, slice_row, payload.tags)
            self._touch_slice(slice_row)
            session.add(slice_row)
            session.commit()
            session.expire_all()
            return self._get_slice_detail(session, slice_id)

    def append_edl_operation(self, slice_id: str, payload: SliceEdlUpdate) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_slice(session, slice_id)
            next_operations = [
                *self._collect_edl_operations(session, slice_row),
                payload.model_dump(mode="json"),
            ]
            edit_commit = EditCommit(
                id=self._new_id("edit"),
                slice_id=slice_row.id,
                parent_commit_id=slice_row.active_commit_id,
                edl_operations=next_operations,
            )
            session.add(edit_commit)
            session.flush()
            slice_row.active_commit_id = edit_commit.id
            self._touch_slice(slice_row, edit_commit.created_at)
            session.add(slice_row)
            session.commit()
            session.expire_all()
            return self._get_slice_detail(session, slice_id)

    def undo_slice(self, slice_id: str) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_slice(session, slice_id)
            if slice_row.active_commit_id is None:
                raise ValueError("No earlier edit state is available")
            active_commit = self._get_edit_commit(session, slice_row.active_commit_id)
            slice_row.active_commit_id = active_commit.parent_commit_id
            self._touch_slice(slice_row)
            session.add(slice_row)
            session.commit()
            session.expire_all()
            return self._get_slice_detail(session, slice_id)

    def redo_slice(self, slice_id: str) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_slice(session, slice_id)
            redo_target = self._get_redo_target(session, slice_row)
            if redo_target is None:
                raise ValueError("No newer edit state is available")
            slice_row.active_commit_id = redo_target.id
            self._touch_slice(slice_row)
            session.add(slice_row)
            session.commit()
            session.expire_all()
            return self._get_slice_detail(session, slice_id)

    def split_slice(self, slice_id: str, payload: SliceSplitRequest) -> list[SliceDetail]:
        with self._session() as session:
            source_slice = self._get_loaded_slice(session, slice_id)
            recording = source_slice.source_recording
            current_duration = self._slice_duration(source_slice)
            split_at = payload.split_at_seconds
            if split_at <= 0 or split_at >= current_duration:
                raise ValueError("Split point must be inside the clip duration")

            transcript_text = self._transcript_text(source_slice.transcript)
            left_text, right_text = self._split_transcript_text(
                transcript_text,
                split_at / current_duration if current_duration > 0 else 0.5,
            )
            base_variant = source_slice.active_variant
            if base_variant is None:
                raise ValueError("Slice has no active variant to split")

            now = utc_now()
            source_metadata = self._slice_metadata(source_slice)
            order_index = int(source_metadata.get("order_index", 0))
            original_start = float(source_metadata.get("original_start_time", 0.0))
            original_end = float(source_metadata.get("original_end_time", current_duration))
            speaker_name = str(source_metadata.get("speaker_name", "speaker_a"))
            language = str(source_metadata.get("language", "en"))

            left_id = self._new_id("slice")
            right_id = self._new_id("slice")
            left_variant_id = self._new_id("variant")
            right_variant_id = self._new_id("variant")
            left_commit_id = self._new_id("edit")
            right_commit_id = self._new_id("edit")
            inherited_ops = self._collect_edl_operations(session, source_slice)

            left_slice = Slice(
                id=left_id,
                source_recording_id=source_slice.source_recording_id,
                active_variant_id=left_variant_id,
                active_commit_id=left_commit_id,
                status=ReviewStatus.UNRESOLVED,
                model_metadata={
                    **source_metadata,
                    "order_index": order_index,
                    "working_asset_id": left_variant_id,
                    "original_start_time": original_start,
                    "original_end_time": round(original_start + split_at, 2),
                    "speaker_name": speaker_name,
                    "language": language,
                    "is_superseded": False,
                    "updated_at": now.isoformat(),
                },
                created_at=now,
            )
            right_slice = Slice(
                id=right_id,
                source_recording_id=source_slice.source_recording_id,
                active_variant_id=right_variant_id,
                active_commit_id=right_commit_id,
                status=ReviewStatus.UNRESOLVED,
                model_metadata={
                    **source_metadata,
                    "order_index": order_index + 1,
                    "working_asset_id": right_variant_id,
                    "original_start_time": round(original_start + split_at, 2),
                    "original_end_time": original_end,
                    "speaker_name": speaker_name,
                    "language": language,
                    "is_superseded": False,
                    "updated_at": now.isoformat(),
                },
                created_at=now,
            )
            left_variant = AudioVariant(
                id=left_variant_id,
                slice_id=left_id,
                file_path=base_variant.file_path,
                is_original=False,
                generator_model="split-view",
                sample_rate=max(base_variant.sample_rate, 1),
                num_samples=max(base_variant.num_samples, 1),
            )
            right_variant = AudioVariant(
                id=right_variant_id,
                slice_id=right_id,
                file_path=base_variant.file_path,
                is_original=False,
                generator_model="split-view",
                sample_rate=max(base_variant.sample_rate, 1),
                num_samples=max(base_variant.num_samples, 1),
            )
            left_commit = EditCommit(
                id=left_commit_id,
                slice_id=left_id,
                edl_operations=[
                    *inherited_ops,
                    {
                        "op": "crop",
                        "range": {"start_seconds": 0.0, "end_seconds": split_at},
                        "duration_seconds": None,
                    },
                ],
            )
            right_commit = EditCommit(
                id=right_commit_id,
                slice_id=right_id,
                edl_operations=[
                    *inherited_ops,
                    {
                        "op": "crop",
                        "range": {"start_seconds": split_at, "end_seconds": current_duration},
                        "duration_seconds": None,
                    },
                ],
            )
            left_transcript = Transcript(
                id=self._new_id("transcript"),
                slice_id=left_id,
                original_text=left_text,
                modified_text=left_text,
                is_modified=False,
                alignment_data=(source_slice.transcript.alignment_data if source_slice.transcript else None),
            )
            right_transcript = Transcript(
                id=self._new_id("transcript"),
                slice_id=right_id,
                original_text=right_text,
                modified_text=right_text,
                is_modified=False,
                alignment_data=(source_slice.transcript.alignment_data if source_slice.transcript else None),
            )
            source_metadata["is_superseded"] = True
            source_metadata["updated_at"] = now.isoformat()
            source_slice.model_metadata = source_metadata
            session.add(source_slice)
            for item in [
                left_slice,
                right_slice,
                left_variant,
                right_variant,
                left_commit,
                right_commit,
                left_transcript,
                right_transcript,
            ]:
                session.add(item)
            session.flush()
            self._replace_slice_tags(session, left_slice, [TagPayload(name=tag.name, color=tag.color) for tag in source_slice.tags])
            self._replace_slice_tags(session, right_slice, [TagPayload(name=tag.name, color=tag.color) for tag in source_slice.tags])
            self._shift_order_indices(session, recording.batch_id, order_index, 2, exclude_ids={source_slice.id})
            session.commit()
            session.expire_all()
            return [self._to_slice_detail(item) for item in self._get_batch_slices(session, recording.batch_id)]

    def merge_with_next_slice(self, slice_id: str) -> list[SliceDetail]:
        with self._session() as session:
            first_slice = self._get_loaded_slice(session, slice_id)
            batch_id = first_slice.source_recording.batch_id
            active_slices = self._get_batch_slices(session, batch_id)
            next_slice = None
            for index, candidate in enumerate(active_slices):
                if candidate.id == slice_id and index + 1 < len(active_slices):
                    next_slice = active_slices[index + 1]
                    break
            if next_slice is None:
                raise ValueError("No next active slice is available for merge")
            if first_slice.source_recording_id != next_slice.source_recording_id:
                raise ValueError("Merge is currently limited to slices from the same source recording")
            if first_slice.active_variant is None or next_slice.active_variant is None:
                raise ValueError("Both slices must have active variants before merge")

            merged_id = self._new_id("slice")
            merged_variant_id = self._new_id("variant")
            merged_path = self.media_root / "variants" / f"{merged_variant_id}.wav"
            merged_path.parent.mkdir(parents=True, exist_ok=True)
            merged_bytes = self._merge_wav_bytes(
                self.get_clip_audio_bytes(first_slice.id),
                self.get_clip_audio_bytes(next_slice.id),
            )
            merged_path.write_bytes(merged_bytes)
            sample_rate, channels, num_samples = self._wav_metadata(merged_bytes)
            now = utc_now()
            first_metadata = self._slice_metadata(first_slice)
            second_metadata = self._slice_metadata(next_slice)
            merged_slice = Slice(
                id=merged_id,
                source_recording_id=first_slice.source_recording_id,
                active_variant_id=merged_variant_id,
                status=ReviewStatus.UNRESOLVED,
                model_metadata={
                    "order_index": min(int(first_metadata.get("order_index", 0)), int(second_metadata.get("order_index", 0))),
                    "source_file_id": first_metadata.get("source_file_id", first_slice.source_recording_id),
                    "working_asset_id": merged_variant_id,
                    "original_start_time": min(
                        float(first_metadata.get("original_start_time", 0.0)),
                        float(second_metadata.get("original_start_time", 0.0)),
                    ),
                    "original_end_time": max(
                        float(first_metadata.get("original_end_time", self._slice_duration(first_slice))),
                        float(second_metadata.get("original_end_time", self._slice_duration(next_slice))),
                    ),
                    "speaker_name": first_metadata.get("speaker_name", "speaker_a"),
                    "language": first_metadata.get("language", "en"),
                    "is_superseded": False,
                    "updated_at": now.isoformat(),
                },
                created_at=now,
            )
            variant = AudioVariant(
                id=merged_variant_id,
                slice_id=merged_id,
                file_path=str(merged_path),
                is_original=False,
                generator_model="merge",
                sample_rate=sample_rate,
                num_samples=num_samples,
            )
            merged_text = self._merge_transcript_text(
                self._transcript_text(first_slice.transcript),
                self._transcript_text(next_slice.transcript),
            )
            transcript = Transcript(
                id=self._new_id("transcript"),
                slice_id=merged_id,
                original_text=merged_text,
                modified_text=merged_text,
                is_modified=False,
                alignment_data=first_slice.transcript.alignment_data if first_slice.transcript else None,
            )
            for source in [first_slice, next_slice]:
                metadata = self._slice_metadata(source)
                metadata["is_superseded"] = True
                metadata["updated_at"] = now.isoformat()
                source.model_metadata = metadata
                session.add(source)
            session.add(merged_slice)
            session.add(variant)
            session.add(transcript)
            session.flush()
            merged_tags = {tag.name.lower(): TagPayload(name=tag.name, color=tag.color) for tag in [*first_slice.tags, *next_slice.tags]}
            self._replace_slice_tags(session, merged_slice, list(merged_tags.values()))
            session.commit()
            session.expire_all()
            return [self._to_slice_detail(item) for item in self._get_batch_slices(session, batch_id)]

    def get_export_preview(self, project_id: str) -> ExportPreview:
        with self._session() as session:
            batch = self._get_batch(session, project_id)
            slices = self._get_export_eligible_slices(session, batch.id)
            lines = []
            for slice_row in slices:
                rendered_name = f"{slice_row.id}.wav"
                lines.append(
                    f"exports/{batch.id}/rendered/{rendered_name}|{self._speaker_name(slice_row)}|{self._language(slice_row)}|{self._transcript_text(slice_row.transcript)}"
                )
            return ExportPreview(
                project_id=batch.id,
                manifest_path=f"exports/{batch.id}/dataset.list",
                accepted_slice_count=len(slices),
                lines=lines,
            )

    def export_project(self, project_id: str) -> ExportRun:
        with self._session() as session:
            self._get_batch(session, project_id)
            export_id = self._new_id("export")
            output_root = self.exports_root / project_id / export_id
            rendered_root = output_root / "rendered"
            manifest_path = output_root / "dataset.list"
            export_run = ExportRun(
                id=export_id,
                batch_id=project_id,
                status=JobStatus.RUNNING,
                output_root=str(output_root),
                manifest_path=str(manifest_path),
            )
            session.add(export_run)
            session.commit()
            slices = self._get_export_eligible_slices(session, project_id)
            try:
                rendered_root.mkdir(parents=True, exist_ok=True)
                manifest_lines: list[str] = []
                for slice_row in slices:
                    rendered_path = rendered_root / f"{slice_row.id}.wav"
                    rendered_path.write_bytes(self.get_clip_audio_bytes(slice_row.id))
                    manifest_lines.append(
                        f"{rendered_path}|{self._speaker_name(slice_row)}|{self._language(slice_row)}|{self._transcript_text(slice_row.transcript)}"
                    )
                manifest_path.write_text("\n".join(manifest_lines))
                export_run.status = JobStatus.COMPLETED
                export_run.accepted_clip_count = len(slices)
                export_run.completed_at = utc_now()
                session.add(export_run)
                session.commit()
                return self._normalize_export_run(export_run)
            except Exception:
                export_run.status = JobStatus.FAILED
                export_run.failed_clip_count = len(slices)
                export_run.completed_at = utc_now()
                session.add(export_run)
                session.commit()
                raise

    def get_waveform_peaks(self, slice_id: str, bins: int = 120) -> WaveformPeaks:
        audio_bytes = self.get_clip_audio_bytes(slice_id)
        safe_bins = max(32, min(bins, 2048))
        peaks = self._extract_waveform_peaks_from_bytes(audio_bytes, safe_bins)
        return WaveformPeaks(clip_id=slice_id, bins=safe_bins, peaks=peaks)

    def get_clip_audio_bytes(self, slice_id: str) -> bytes:
        with self._session() as session:
            slice_row = self._get_loaded_slice(session, slice_id)
            active_variant = slice_row.active_variant
            if active_variant is None:
                return self._render_synthetic_wave_bytes(48000, 1, 2.0, slice_id)
            try:
                audio_path = self._materialize_variant_media(
                    session,
                    active_variant,
                    slice_row.source_recording.num_channels if slice_row.source_recording is not None else None,
                    persist=True,
                )
            except FileNotFoundError as exc:
                raise ValueError(f"Active variant media is missing on disk: {exc}") from exc
            return self._apply_edl_to_wav_bytes(audio_path.read_bytes(), self._collect_edl_operations(session, slice_row))

    def get_variant_media_path(self, variant_id: str) -> Path:
        with self._session() as session:
            variant = session.exec(
                select(AudioVariant)
                .where(AudioVariant.id == variant_id)
                .options(selectinload(AudioVariant.parent_slice).selectinload(Slice.source_recording))
            ).first()
            if variant is None:
                raise KeyError(variant_id)
            expected_channels = (
                variant.parent_slice.source_recording.num_channels
                if variant.parent_slice is not None and variant.parent_slice.source_recording is not None
                else None
            )
            return self._materialize_variant_media(
                session,
                variant,
                expected_channels,
                persist=True,
            )

    def create_import_batch(self, payload: ImportBatchCreate) -> ImportBatch:
        with self._session() as session:
            batch = ImportBatch(id=payload.id, name=payload.name)
            session.add(batch)
            session.commit()
            session.refresh(batch)
            return self._normalize_batch(batch)

    def create_source_recording(self, payload: SourceRecordingCreate) -> SourceRecording:
        with self._session() as session:
            self._get_batch(session, payload.batch_id)
            self._validate_audio_asset(Path(payload.file_path), payload.sample_rate, payload.num_channels, payload.num_samples)
            recording = SourceRecording(**payload.model_dump())
            session.add(recording)
            session.commit()
            session.refresh(recording)
            return recording

    def create_preprocessed_recording(self, recording_id: str, payload: RecordingDerivativeCreate) -> SourceRecording:
        with self._session() as session:
            parent = self._get_source_recording(session, recording_id)
            self._validate_audio_asset(Path(payload.file_path), payload.sample_rate, payload.num_channels, payload.num_samples)
            recording = SourceRecording(
                id=payload.id,
                batch_id=parent.batch_id,
                parent_recording_id=parent.id,
                file_path=payload.file_path,
                sample_rate=payload.sample_rate,
                num_channels=payload.num_channels,
                num_samples=payload.num_samples,
                processing_recipe=payload.processing_recipe,
            )
            session.add(recording)
            session.commit()
            session.refresh(recording)
            return recording

    def register_slicer_chunks(self, recording_id: str, payload: SlicerHandoffRequest) -> list[SliceDetail]:
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            created_ids: list[str] = []
            for chunk in payload.chunks:
                self._validate_audio_asset(Path(chunk.file_path), chunk.sample_rate, recording.num_channels, chunk.num_samples)
                variant_id = self._new_id("variant")
                managed_variant_path = self._ingest_variant_asset(Path(chunk.file_path), variant_id)
                transcript_id = self._new_id("transcript")
                metadata = {
                    "order_index": chunk.order_index,
                    "source_file_id": recording.id,
                    "working_asset_id": variant_id,
                    "original_start_time": chunk.original_start_time,
                    "original_end_time": chunk.original_end_time,
                    "speaker_name": chunk.speaker_name,
                    "language": chunk.language,
                    "is_superseded": False,
                    "updated_at": utc_now().isoformat(),
                    **(chunk.model_metadata or {}),
                }
                slice_row = Slice(
                    id=chunk.id,
                    source_recording_id=recording.id,
                    active_variant_id=variant_id,
                    status=ReviewStatus.UNRESOLVED,
                    model_metadata=metadata,
                )
                variant = AudioVariant(
                    id=variant_id,
                    slice_id=slice_row.id,
                    file_path=str(managed_variant_path),
                    is_original=True,
                    generator_model="slicer",
                    sample_rate=chunk.sample_rate,
                    num_samples=chunk.num_samples,
                )
                transcript = Transcript(
                    id=transcript_id,
                    slice_id=slice_row.id,
                    original_text=chunk.transcript_text,
                    alignment_data={
                        "source": chunk.transcript_source,
                        "confidence": chunk.transcript_confidence,
                    },
                )
                session.add(slice_row)
                session.add(variant)
                session.add(transcript)
                session.flush()
                self._replace_slice_tags(session, slice_row, chunk.tags)
                created_ids.append(slice_row.id)
            session.commit()
            session.expire_all()
            return [self._get_slice_detail(session, slice_id) for slice_id in created_ids]

    def create_audio_variant(self, slice_id: str, payload: AudioVariantCreate) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_slice(session, slice_id)
            recording = self._get_source_recording(session, slice_row.source_recording_id)
            self._validate_audio_asset(Path(payload.file_path), payload.sample_rate, recording.num_channels, payload.num_samples)
            managed_variant_path = self._ingest_variant_asset(Path(payload.file_path), payload.id)
            variant = AudioVariant(
                id=payload.id,
                slice_id=slice_row.id,
                file_path=str(managed_variant_path),
                is_original=False,
                generator_model=payload.generator_model,
                sample_rate=payload.sample_rate,
                num_samples=payload.num_samples,
            )
            session.add(variant)
            session.flush()
            slice_row.active_variant_id = variant.id
            metadata = self._slice_metadata(slice_row)
            metadata["working_asset_id"] = variant.id
            metadata["updated_at"] = utc_now().isoformat()
            slice_row.model_metadata = metadata
            session.add(slice_row)
            session.commit()
            session.expire_all()
            return self._get_slice_detail(session, slice_id)

    def run_audio_variant(self, slice_id: str, payload: AudioVariantRunRequest) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_loaded_slice(session, slice_id)
            recording = slice_row.source_recording
            active_variant = slice_row.active_variant
            if active_variant is None:
                raise ValueError("Slice has no active variant to process")
            try:
                source_path = self._materialize_variant_media(
                    session,
                    active_variant,
                    recording.num_channels,
                    persist=False,
                )
            except FileNotFoundError as exc:
                raise ValueError(f"Active variant media is missing on disk: {exc}") from exc
            variant_id = self._new_id("variant")
            target_path = self.media_root / "variants" / f"{variant_id}.wav"
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_path, target_path)
            variant = AudioVariant(
                id=variant_id,
                slice_id=slice_row.id,
                file_path=str(target_path),
                is_original=False,
                generator_model=payload.generator_model,
                sample_rate=active_variant.sample_rate,
                num_samples=active_variant.num_samples,
            )
            self._validate_audio_asset(target_path, active_variant.sample_rate, recording.num_channels, active_variant.num_samples)
            session.add(variant)
            session.flush()
            slice_row.active_variant_id = variant.id
            metadata = self._slice_metadata(slice_row)
            metadata["working_asset_id"] = variant.id
            metadata["updated_at"] = utc_now().isoformat()
            slice_row.model_metadata = metadata
            session.add(slice_row)
            session.commit()
            session.expire_all()
            return self._get_slice_detail(session, slice_id)

    def set_active_variant(self, slice_id: str, payload: ActiveVariantUpdate) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_loaded_slice(session, slice_id)
            matching_variant = next((variant for variant in slice_row.variants if variant.id == payload.active_variant_id), None)
            if matching_variant is None:
                raise KeyError(payload.active_variant_id)
            slice_row.active_variant_id = matching_variant.id
            metadata = self._slice_metadata(slice_row)
            metadata["working_asset_id"] = matching_variant.id
            metadata["updated_at"] = utc_now().isoformat()
            slice_row.model_metadata = metadata
            session.add(slice_row)
            session.commit()
            session.expire_all()
            return self._get_slice_detail(session, slice_id)

    def create_reference_asset(self, payload: ReferenceAssetCreate) -> ReferenceAsset:
        with self._session() as session:
            if session.get(AudioVariant, payload.audio_variant_id) is None:
                raise KeyError(payload.audio_variant_id)
            asset = ReferenceAsset(**payload.model_dump())
            session.add(asset)
            session.commit()
            session.refresh(asset)
            asset.created_at = self._as_utc(asset.created_at)
            return asset

    def _session(self) -> Session:
        return Session(self.engine, expire_on_commit=False)

    def _seed_if_needed(self) -> None:
        with self._session() as session:
            if session.exec(select(ImportBatch)).first() is not None:
                return
            if self.legacy_seed_path.exists():
                self._seed_from_legacy_json(session)
            else:
                self._seed_demo(session)
            session.commit()

    def _seed_from_legacy_json(self, session: Session) -> None:
        legacy = json.loads(self.legacy_seed_path.read_text())
        batch_payload = legacy["projects"]["phase1-demo"]
        batch = ImportBatch(
            id=batch_payload["id"],
            name=batch_payload["name"],
            created_at=datetime.fromisoformat(batch_payload["created_at"].replace("Z", "+00:00")),
        )
        session.add(batch)
        session.flush()

        source_recordings: dict[str, SourceRecording] = {}
        active_clips = legacy.get("clips_by_project", {}).get(batch.id, [])
        for clip_payload in active_clips:
            source_id = clip_payload["source_file_id"]
            if source_id in source_recordings:
                continue
            source_duration = max(
                item["original_end_time"]
                for item in active_clips
                if item["source_file_id"] == source_id
            )
            source_path = self.media_root / "sources" / f"{source_id}.wav"
            source_path.parent.mkdir(parents=True, exist_ok=True)
            if not source_path.exists():
                source_path.write_bytes(
                    self._render_synthetic_wave_bytes(
                        clip_payload["sample_rate"],
                        clip_payload["channels"],
                        max(source_duration + 1.0, 1.0),
                        source_id,
                    )
                )
            source_recording = SourceRecording(
                id=source_id,
                batch_id=batch.id,
                file_path=str(source_path),
                sample_rate=clip_payload["sample_rate"],
                num_channels=clip_payload["channels"],
                num_samples=max(int((source_duration + 1.0) * clip_payload["sample_rate"]), 1),
            )
            session.add(source_recording)
            source_recordings[source_id] = source_recording

        session.flush()
        for clip_payload in active_clips:
            variant_path = self.media_root / "variants" / f"{clip_payload['id']}.wav"
            variant_path.parent.mkdir(parents=True, exist_ok=True)
            if clip_payload.get("audio_path") and Path(clip_payload["audio_path"]).exists():
                shutil.copyfile(Path(clip_payload["audio_path"]).expanduser(), variant_path)
            elif not variant_path.exists():
                variant_path.write_bytes(
                    self._render_synthetic_wave_bytes(
                        clip_payload["sample_rate"],
                        clip_payload["channels"],
                        max(clip_payload["duration_seconds"], 0.2),
                        clip_payload["id"],
                    )
                )
            variant_id = f"variant-{clip_payload['id']}"
            transcript_id = f"transcript-{clip_payload['id']}"
            active_commit_id = f"edit-{clip_payload['id']}" if clip_payload.get("clip_edl") else None
            slice_row = Slice(
                id=clip_payload["id"],
                source_recording_id=clip_payload["source_file_id"],
                active_variant_id=variant_id,
                active_commit_id=active_commit_id,
                status=self._legacy_status_to_storage(clip_payload["review_status"]),
                model_metadata={
                    "order_index": clip_payload["order_index"],
                    "source_file_id": clip_payload["source_file_id"],
                    "working_asset_id": clip_payload["working_asset_id"],
                    "original_start_time": clip_payload["original_start_time"],
                    "original_end_time": clip_payload["original_end_time"],
                    "speaker_name": clip_payload["speaker_name"],
                    "language": clip_payload["language"],
                    "is_superseded": clip_payload["is_superseded"],
                    "updated_at": clip_payload["updated_at"],
                },
                created_at=datetime.fromisoformat(clip_payload["created_at"].replace("Z", "+00:00")),
            )
            variant = AudioVariant(
                id=variant_id,
                slice_id=slice_row.id,
                file_path=str(variant_path),
                is_original=True,
                generator_model="slicer",
                sample_rate=clip_payload["sample_rate"],
                num_samples=max(int(clip_payload["duration_seconds"] * clip_payload["sample_rate"]), 1),
            )
            transcript = Transcript(
                id=transcript_id,
                slice_id=slice_row.id,
                original_text=clip_payload["transcript"]["text_initial"],
                modified_text=clip_payload["transcript"]["text_current"],
                is_modified=clip_payload["transcript"]["text_current"] != clip_payload["transcript"]["text_initial"],
                alignment_data={
                    "source": clip_payload["transcript"].get("source", "manual"),
                    "confidence": clip_payload["transcript"].get("confidence"),
                },
                )
            session.add(slice_row)
            session.add(variant)
            session.add(transcript)
            if active_commit_id is not None:
                session.add(
                    EditCommit(
                        id=active_commit_id,
                        slice_id=slice_row.id,
                        edl_operations=clip_payload["clip_edl"],
                    )
                )
            session.flush()
            self._replace_slice_tags(session, slice_row, [TagPayload(**tag) for tag in clip_payload["tags"]])
        for export_payload in legacy.get("exports_by_project", {}).get(batch.id, []):
            session.add(
                ExportRun(
                    id=export_payload["id"],
                    batch_id=batch.id,
                    status=self._job_status_from_legacy(export_payload["status"]),
                    output_root=export_payload["output_root"],
                    manifest_path=export_payload["manifest_path"],
                    accepted_clip_count=export_payload["accepted_clip_count"],
                    failed_clip_count=export_payload["failed_clip_count"],
                    created_at=datetime.fromisoformat(export_payload["created_at"].replace("Z", "+00:00")),
                    completed_at=(
                        datetime.fromisoformat(export_payload["completed_at"].replace("Z", "+00:00"))
                        if export_payload.get("completed_at")
                        else None
                    ),
                )
            )

    def _seed_demo(self, session: Session) -> None:
        batch = ImportBatch(id="phase1-demo", name="Phase 1 Demo Project")
        session.add(batch)
        source_path = self.media_root / "sources" / "src-001.wav"
        chunk_path = self.media_root / "seed" / "clip-001.wav"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        chunk_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(self._render_synthetic_wave_bytes(48000, 1, 20.0, "src-001"))
        chunk_path.write_bytes(self._render_synthetic_wave_bytes(48000, 1, 157440 / 48000, "clip-001"))
        recording = SourceRecording(
            id="src-001",
            batch_id=batch.id,
            file_path=str(source_path),
            sample_rate=48000,
            num_channels=1,
            num_samples=960000,
        )
        session.add(recording)
        session.commit()
        self.register_slicer_chunks(
            recording.id,
            SlicerHandoffRequest(
                chunks=[
                    {
                        "id": "clip-001",
                        "file_path": str(chunk_path),
                        "sample_rate": 48000,
                        "num_samples": 157440,
                        "original_start_time": 12.4,
                        "original_end_time": 15.68,
                        "transcript_text": "The workstation should make this painless.",
                        "order_index": 10,
                    }
                ]
            ),
        )

    def _get_batch(self, session: Session, batch_id: str) -> ImportBatch:
        batch = session.get(ImportBatch, batch_id)
        if batch is None:
            raise KeyError(batch_id)
        return batch

    def _get_slice(self, session: Session, slice_id: str) -> Slice:
        slice_row = session.exec(
            select(Slice)
            .where(Slice.id == slice_id)
            .options(
                selectinload(Slice.source_recording),
                selectinload(Slice.transcript),
                selectinload(Slice.tags),
                selectinload(Slice.variants),
                selectinload(Slice.commits),
                selectinload(Slice.active_variant),
                selectinload(Slice.active_commit),
            )
        ).first()
        if slice_row is None:
            raise KeyError(slice_id)
        return slice_row

    def _get_loaded_slice(self, session: Session, slice_id: str) -> Slice:
        return self._get_slice(session, slice_id)

    def _get_edit_commit(self, session: Session, commit_id: str) -> EditCommit:
        commit = session.get(EditCommit, commit_id)
        if commit is None:
            raise KeyError(commit_id)
        return commit

    def _get_source_recording(self, session: Session, recording_id: str) -> SourceRecording:
        recording = session.get(SourceRecording, recording_id)
        if recording is None:
            raise KeyError(recording_id)
        return recording

    def _get_transcript(self, session: Session, slice_id: str) -> Transcript:
        transcript = session.exec(select(Transcript).where(Transcript.slice_id == slice_id)).first()
        if transcript is None:
            raise KeyError(slice_id)
        return transcript

    def _get_batch_slices(self, session: Session, batch_id: str) -> list[Slice]:
        return [
            item
            for item in self._get_all_batch_slices(session, batch_id)
            if not self._slice_metadata(item).get("is_superseded", False)
        ]

    def _get_all_batch_slices(self, session: Session, batch_id: str) -> list[Slice]:
        recording_ids = session.exec(
            select(SourceRecording.id).where(SourceRecording.batch_id == batch_id)
        ).all()
        if not recording_ids:
            return []
        slices = session.exec(
            select(Slice)
            .where(Slice.source_recording_id.in_(recording_ids))
            .options(
                selectinload(Slice.source_recording),
                selectinload(Slice.transcript),
                selectinload(Slice.tags),
                selectinload(Slice.variants),
                selectinload(Slice.commits),
                selectinload(Slice.active_variant),
                selectinload(Slice.active_commit),
            )
        ).all()
        return sorted(
            list(slices),
            key=lambda slice_row: (
                int(self._slice_metadata(slice_row).get("order_index", 0)),
                self._as_utc(slice_row.created_at),
            ),
        )

    def _slice_metadata(self, slice_row: Slice) -> dict[str, Any]:
        return dict(slice_row.model_metadata or {})

    def _get_redo_target(self, session: Session, slice_row: Slice) -> EditCommit | None:
        if slice_row.active_commit_id is None:
            return session.exec(
                select(EditCommit)
                .where(EditCommit.slice_id == slice_row.id, EditCommit.parent_commit_id.is_(None))
                .order_by(EditCommit.created_at.desc())
            ).first()
        return session.exec(
            select(EditCommit)
            .where(EditCommit.parent_commit_id == slice_row.active_commit_id)
            .order_by(EditCommit.created_at.desc())
        ).first()

    def _collect_edl_operations(self, session: Session, slice_row: Slice) -> list[dict[str, Any]]:
        if slice_row.active_commit is None:
            return []
        del session
        commits_by_id = {commit.id: commit for commit in slice_row.commits}
        active_commit = commits_by_id.get(slice_row.active_commit_id or "")
        if active_commit is None:
            return []
        if active_commit.parent_commit_id:
            parent_commit = commits_by_id.get(active_commit.parent_commit_id)
            if parent_commit and self._commit_extends_parent(active_commit, parent_commit):
                return list(active_commit.edl_operations or [])
        chain: list[EditCommit] = []
        current = active_commit
        while current is not None:
            chain.append(current)
            current = commits_by_id.get(current.parent_commit_id or "")
        operations: list[dict[str, Any]] = []
        for commit in reversed(chain):
            operations.extend(commit.edl_operations or [])
        return operations

    def _get_slice_tags(self, session: Session, slice_id: str) -> list[Tag]:
        statement = (
            select(Tag)
            .join(SliceTagLink, SliceTagLink.tag_id == Tag.id)
            .where(SliceTagLink.slice_id == slice_id)
            .order_by(Tag.name)
        )
        return session.exec(statement).all()

    def _replace_slice_tags(self, session: Session, slice_row: Slice, tags: list[TagPayload]) -> None:
        existing_links = session.exec(
            select(SliceTagLink).where(SliceTagLink.slice_id == slice_row.id)
        ).all()
        for link in existing_links:
            session.delete(link)
        for tag_payload in tags:
            tag = session.exec(select(Tag).where(Tag.name == tag_payload.name)).first()
            if tag is None:
                tag = Tag(id=self._new_id("tag"), name=tag_payload.name, color=tag_payload.color)
                session.add(tag)
                session.flush()
            elif tag.color != tag_payload.color:
                tag.color = tag_payload.color
                session.add(tag)
            session.add(SliceTagLink(slice_id=slice_row.id, tag_id=tag.id))

    def _as_utc(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _normalize_batch(self, batch: ImportBatch) -> ImportBatch:
        batch.created_at = self._as_utc(batch.created_at)
        return batch

    def _normalize_export_run(self, run: ExportRun) -> ExportRun:
        run.created_at = self._as_utc(run.created_at)
        if run.completed_at is not None:
            run.completed_at = self._as_utc(run.completed_at)
        return run

    def _get_slice_detail(self, session: Session, slice_id: str) -> SliceDetail:
        return self._to_slice_detail(self._get_slice(session, slice_id))

    def _to_slice_detail(self, slice_row: Slice) -> SliceDetail:
        transcript = slice_row.transcript
        active_variant = slice_row.active_variant
        active_commit = slice_row.active_commit
        source_recording = slice_row.source_recording
        if source_recording is None:
            raise ValueError(f"Slice {slice_row.id} is missing its source recording")
        payload = {
            "id": slice_row.id,
            "source_recording_id": slice_row.source_recording_id,
            "active_variant_id": slice_row.active_variant_id,
            "active_commit_id": slice_row.active_commit_id,
            "status": slice_row.status,
            "duration_seconds": self._slice_duration(slice_row),
            "model_metadata": self._slice_metadata(slice_row),
            "created_at": self._as_utc(slice_row.created_at),
            "source_recording": source_recording,
            "transcript": transcript,
            "tags": list(sorted(slice_row.tags, key=lambda tag: tag.name.lower())),
            "variants": list(sorted(slice_row.variants, key=lambda variant: (not variant.is_original, variant.id))),
            "commits": [
                commit.model_copy(update={"created_at": self._as_utc(commit.created_at)})
                for commit in sorted(slice_row.commits, key=lambda commit: commit.created_at)
            ],
            "active_variant": active_variant,
            "active_commit": (
                active_commit.model_copy(
                    update={
                        "created_at": self._as_utc(active_commit.created_at),
                        "edl_operations": self._collect_edl_operations(None, slice_row),
                    }
                )
                if active_commit is not None
                else None
            ),
        }
        return SliceDetail.model_validate(payload)

    def _touch_slice(self, slice_row: Slice, when: datetime | None = None) -> None:
        metadata = self._slice_metadata(slice_row)
        metadata["updated_at"] = self._as_utc(when or utc_now()).isoformat()
        slice_row.model_metadata = metadata

    def _transcript_text(self, transcript: Transcript | None) -> str:
        if transcript is None:
            return ""
        return transcript.modified_text or transcript.original_text

    def _speaker_name(self, slice_row: Slice) -> str:
        return str(self._slice_metadata(slice_row).get("speaker_name", "speaker_a"))

    def _language(self, slice_row: Slice) -> str:
        return str(self._slice_metadata(slice_row).get("language", "en"))

    def _slice_duration(self, slice_row: Slice) -> float:
        if slice_row.active_variant is not None:
            base_duration = slice_row.active_variant.duration_s
        else:
            base_duration = slice_row.source_recording.duration_s if slice_row.source_recording is not None else 0.0
        return round(self._apply_edl_to_duration(base_duration, self._collect_edl_operations(None, slice_row)), 2)

    def _legacy_status_to_storage(self, status: str) -> ReviewStatus:
        mapping = {
            "accepted": ReviewStatus.ACCEPTED,
            "rejected": ReviewStatus.REJECTED,
            "needs_attention": ReviewStatus.QUARANTINED,
        }
        return mapping.get(status, ReviewStatus.UNRESOLVED)

    def _get_export_eligible_slices(self, session: Session, batch_id: str) -> list[Slice]:
        return [
            slice_row
            for slice_row in self._get_batch_slices(session, batch_id)
            if slice_row.status == ReviewStatus.ACCEPTED and self._transcript_text(slice_row.transcript).strip()
        ]

    def _commit_extends_parent(self, commit: EditCommit, parent_commit: EditCommit) -> bool:
        parent_ops = list(parent_commit.edl_operations or [])
        commit_ops = list(commit.edl_operations or [])
        if len(commit_ops) < len(parent_ops):
            return False
        return commit_ops[: len(parent_ops)] == parent_ops

    def _clip_updated_at(self, slice_row: Slice) -> datetime:
        metadata = self._slice_metadata(slice_row)
        raw = metadata.get("updated_at")
        if isinstance(raw, str):
            try:
                return self._as_utc(datetime.fromisoformat(raw))
            except ValueError:
                return self._as_utc(slice_row.created_at)
        return self._as_utc(slice_row.created_at)

    def _apply_edl_to_duration(self, duration: float, operations: list[dict[str, Any]]) -> float:
        current = max(duration, 0.0)
        for operation in operations:
            op = operation.get("op")
            range_payload = operation.get("range") or {}
            if op == "delete_range":
                removed = max(float(range_payload.get("end_seconds", 0.0)) - float(range_payload.get("start_seconds", 0.0)), 0.0)
                current = max(current - removed, 0.1)
            elif op == "insert_silence":
                current = max(current + max(float(operation.get("duration_seconds") or 0.0), 0.0), 0.1)
            elif op == "crop":
                current = max(float(range_payload.get("end_seconds", current)) - float(range_payload.get("start_seconds", 0.0)), 0.1)
        return current

    def _split_transcript_text(self, text: str, split_ratio: float) -> tuple[str, str]:
        words = text.split()
        if len(words) <= 1:
            midpoint = max(min(int(round(len(text) * split_ratio)), len(text) - 1), 1)
            return text[:midpoint].strip() or text.strip(), text[midpoint:].strip() or text.strip()
        split_index = max(1, min(len(words) - 1, round(len(words) * split_ratio)))
        return " ".join(words[:split_index]).strip(), " ".join(words[split_index:]).strip()

    def _merge_transcript_text(self, first: str, second: str) -> str:
        return " ".join(part.strip() for part in [first, second] if part.strip())

    def _apply_edl_to_wav_bytes(self, audio_bytes: bytes, operations: list[dict[str, Any]]) -> bytes:
        if not operations:
            return audio_bytes
        channels, sample_width, sample_rate, _frame_count, raw = self._read_pcm_wav(audio_bytes)
        bytes_per_frame = max(channels * sample_width, 1)
        working_raw = raw
        for operation in operations:
            op = operation.get("op")
            range_payload = operation.get("range") or {}
            if op == "delete_range":
                start_frame = max(int(float(range_payload.get("start_seconds", 0.0)) * sample_rate), 0)
                end_frame = max(int(float(range_payload.get("end_seconds", 0.0)) * sample_rate), start_frame)
                working_raw = working_raw[: start_frame * bytes_per_frame] + working_raw[end_frame * bytes_per_frame :]
            elif op == "insert_silence":
                insert_frame = max(int(float(range_payload.get("start_seconds", 0.0)) * sample_rate), 0)
                silence_frames = max(int(float(operation.get("duration_seconds") or 0.0) * sample_rate), 1)
                silence = b"\x00" * (silence_frames * bytes_per_frame)
                offset = insert_frame * bytes_per_frame
                working_raw = working_raw[:offset] + silence + working_raw[offset:]
            elif op == "crop":
                start_frame = max(int(float(range_payload.get("start_seconds", 0.0)) * sample_rate), 0)
                end_frame = max(int(float(range_payload.get("end_seconds", 0.0)) * sample_rate), start_frame + 1)
                working_raw = working_raw[start_frame * bytes_per_frame : end_frame * bytes_per_frame]
        output = io.BytesIO()
        with wave.open(output, "wb") as wav_file:
            wav_file.setnchannels(channels)
            wav_file.setsampwidth(sample_width)
            wav_file.setframerate(sample_rate)
            wav_file.writeframes(working_raw or (b"\x00" * bytes_per_frame))
        return output.getvalue()

    def _extract_waveform_peaks_from_bytes(self, audio_bytes: bytes, bins: int) -> list[float]:
        channels, sample_width, _sample_rate, frame_count, raw = self._read_pcm_wav(audio_bytes)
        if frame_count <= 0 or channels <= 0 or sample_width != 2:
            return [0.04] * bins
        samples_per_bin = max(frame_count // bins, 1)
        peaks: list[float] = []
        for bin_index in range(bins):
            start_frame = bin_index * samples_per_bin
            end_frame = min((bin_index + 1) * samples_per_bin, frame_count)
            max_abs = 0
            for frame_index in range(start_frame, end_frame):
                frame_peak = 0
                for channel_index in range(channels):
                    offset = (frame_index * channels + channel_index) * sample_width
                    sample = int.from_bytes(raw[offset : offset + sample_width], "little", signed=True)
                    frame_peak = max(frame_peak, abs(sample))
                max_abs = max(max_abs, frame_peak)
            peaks.append(round(max(max_abs / 32767.0, 0.04), 4))
        return peaks

    def _render_synthetic_wave_bytes(
        self,
        sample_rate: int,
        channels: int,
        duration_seconds: float,
        seed_text: str,
    ) -> bytes:
        sample_rate = max(sample_rate, 1)
        channels = max(channels, 1)
        duration_seconds = max(duration_seconds, 0.1)
        frame_count = max(int(sample_rate * duration_seconds), 1)
        base_frequency = 180 + (sum(ord(char) for char in seed_text) % 120)
        harmonic_frequency = base_frequency * 2.1
        amplitude = 0.34
        buffer = io.BytesIO()
        with wave.open(buffer, "wb") as wav_file:
            wav_file.setnchannels(channels)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate)
            for frame_index in range(frame_count):
                t = frame_index / sample_rate
                sample_value = (
                    amplitude * math.sin(2 * math.pi * base_frequency * t)
                    + 0.16 * math.sin(2 * math.pi * harmonic_frequency * t)
                )
                pcm_value = max(min(int(sample_value * 32767), 32767), -32768)
                wav_file.writeframesraw(pcm_value.to_bytes(2, "little", signed=True) * channels)
        return buffer.getvalue()

    def _merge_wav_bytes(self, first: bytes, second: bytes) -> bytes:
        first_rate, first_channels, _ = self._wav_metadata(first)
        second_rate, second_channels, _ = self._wav_metadata(second)
        if first_rate != second_rate or first_channels != second_channels:
            raise ValueError("Merged clips must have matching sample rates and channel counts")
        with wave.open(io.BytesIO(first), "rb") as first_wav, wave.open(io.BytesIO(second), "rb") as second_wav:
            frames = first_wav.readframes(first_wav.getnframes()) + second_wav.readframes(second_wav.getnframes())
        output = io.BytesIO()
        with wave.open(output, "wb") as wav_file:
            wav_file.setnchannels(first_channels)
            wav_file.setsampwidth(2)
            wav_file.setframerate(first_rate)
            wav_file.writeframes(frames)
        return output.getvalue()

    def _wav_metadata(self, audio_bytes: bytes) -> tuple[int, int, int]:
        channels, _sample_width, sample_rate, frames, _raw = self._read_pcm_wav(audio_bytes)
        return sample_rate, channels, frames

    def _new_id(self, prefix: str) -> str:
        return f"{prefix}-{uuid4().hex}"

    def _validate_audio_asset(
        self,
        path: Path,
        expected_sample_rate: int,
        expected_channels: int,
        expected_num_samples: int,
    ) -> None:
        resolved = path.expanduser()
        if not resolved.exists():
            raise ValueError(f"Audio asset not found: {resolved}")
        if resolved.suffix.lower() != ".wav":
            raise ValueError("Phase 1 requires normalized 16-bit PCM WAV assets")
        channels, _sample_width, sample_rate, frames = self._read_pcm_wav_header(resolved)
        if sample_rate != expected_sample_rate:
            raise ValueError(
                f"Audio asset sample rate mismatch: expected {expected_sample_rate}, got {sample_rate}"
            )
        if channels != expected_channels:
            raise ValueError(
                f"Audio asset channel mismatch: expected {expected_channels}, got {channels}"
            )
        if frames != expected_num_samples:
            raise ValueError(
                f"Audio asset sample-count mismatch: expected {expected_num_samples}, got {frames}"
            )

    def _ingest_variant_asset(self, path: Path, variant_id: str) -> Path:
        source_path = path.expanduser().resolve()
        if not source_path.exists():
            raise ValueError(f"Audio asset not found: {source_path}")
        target_path = (self.media_root / "variants" / f"{variant_id}.wav").resolve()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if source_path != target_path:
            shutil.copyfile(source_path, target_path)
        return target_path

    def _materialize_variant_media(
        self,
        session: Session,
        variant: AudioVariant,
        expected_channels: int | None,
        *,
        persist: bool,
    ) -> Path:
        raw_path = Path(variant.file_path).expanduser().resolve(strict=False)
        try:
            return self._resolve_variant_media_path(raw_path)
        except ValueError:
            if not raw_path.exists():
                raise FileNotFoundError(raw_path)
            if expected_channels is None:
                channels, _sample_width, sample_rate, frames = self._read_pcm_wav_header(raw_path)
                if sample_rate != variant.sample_rate or frames != variant.num_samples:
                    raise ValueError("Variant media metadata does not match the stored database record")
                if channels <= 0:
                    raise ValueError("Variant media has invalid channel metadata")
            else:
                self._validate_audio_asset(raw_path, variant.sample_rate, expected_channels, variant.num_samples)
            managed_path = (self.media_root / "variants" / f"{variant.id}.wav").resolve()
            managed_path.parent.mkdir(parents=True, exist_ok=True)
            if not managed_path.exists():
                shutil.copyfile(raw_path, managed_path)
            if persist and variant.file_path != str(managed_path):
                variant.file_path = str(managed_path)
                session.add(variant)
                session.commit()
            return managed_path

    def _resolve_variant_media_path(self, path: Path) -> Path:
        resolved = path.expanduser().resolve(strict=False)
        media_root = self.media_root.resolve()
        if not resolved.is_relative_to(media_root):
            raise ValueError(f"Managed media path escapes media root: {resolved}")
        if not resolved.exists():
            raise FileNotFoundError(resolved)
        return resolved

    def _delete_unreferenced_media_files(self, paths: list[str]) -> int:
        candidate_paths: list[Path] = []
        for raw_path in paths:
            try:
                candidate_paths.append(self._resolve_variant_media_path(Path(raw_path)))
            except (FileNotFoundError, ValueError):
                continue

        with self._session() as session:
            retained_paths = set()
            for raw_path in session.exec(select(AudioVariant.file_path)).all():
                try:
                    retained_paths.add(self._resolve_variant_media_path(Path(raw_path)))
                except (FileNotFoundError, ValueError):
                    continue

        deleted_count = 0
        for path in sorted(set(candidate_paths)):
            if path in retained_paths or not path.exists():
                continue
            path.unlink()
            deleted_count += 1
        return deleted_count

    def _read_pcm_wav_header(self, path: Path) -> tuple[int, int, int, int]:
        try:
            with wave.open(str(path), "rb") as wav_file:
                channels = wav_file.getnchannels()
                sample_width = wav_file.getsampwidth()
                sample_rate = wav_file.getframerate()
                frame_count = wav_file.getnframes()
                compression = wav_file.getcomptype()
        except wave.Error as exc:
            raise ValueError("Phase 1 only supports normalized 16-bit PCM WAV assets") from exc
        if compression != "NONE" or sample_width != 2:
            raise ValueError("Phase 1 only supports normalized 16-bit PCM WAV assets")
        if channels <= 0 or sample_rate <= 0 or frame_count < 0:
            raise ValueError("Invalid WAV metadata")
        return channels, sample_width, sample_rate, frame_count

    def _read_pcm_wav(self, audio_bytes: bytes) -> tuple[int, int, int, int, bytes]:
        try:
            with wave.open(io.BytesIO(audio_bytes), "rb") as wav_file:
                channels = wav_file.getnchannels()
                sample_width = wav_file.getsampwidth()
                sample_rate = wav_file.getframerate()
                frame_count = wav_file.getnframes()
                compression = wav_file.getcomptype()
                raw = wav_file.readframes(frame_count)
        except wave.Error as exc:
            raise ValueError("Phase 1 only supports normalized 16-bit PCM WAV assets") from exc
        if compression != "NONE" or sample_width != 2:
            raise ValueError("Phase 1 only supports normalized 16-bit PCM WAV assets")
        if channels <= 0 or sample_rate <= 0 or frame_count < 0:
            raise ValueError("Invalid WAV metadata")
        return channels, sample_width, sample_rate, frame_count, raw

    def _shift_order_indices(
        self,
        session: Session,
        project_id: str,
        from_index: int,
        amount: int,
        exclude_ids: set[str] | None = None,
    ) -> None:
        exclude_ids = exclude_ids or set()
        for slice_row in self._get_batch_slices(session, project_id):
            if slice_row.id in exclude_ids:
                continue
            metadata = self._slice_metadata(slice_row)
            order_index = int(metadata.get("order_index", 0))
            if order_index > from_index:
                metadata["order_index"] = order_index + amount
                slice_row.model_metadata = metadata
                session.add(slice_row)

    def _job_status_from_legacy(self, status: str) -> JobStatus:
        mapping = {
            "queued": JobStatus.PENDING,
            "running": JobStatus.RUNNING,
            "succeeded": JobStatus.COMPLETED,
            "failed": JobStatus.FAILED,
        }
        return mapping.get(status, JobStatus.PENDING)

    def _job_status_to_api(self, status: JobStatus) -> str:
        mapping = {
            JobStatus.PENDING: "queued",
            JobStatus.RUNNING: "running",
            JobStatus.COMPLETED: "succeeded",
            JobStatus.FAILED: "failed",
        }
        return mapping[status]

    def _export_status_from_job(self, status: JobStatus) -> str:
        mapping = {
            JobStatus.PENDING: "export_in_progress",
            JobStatus.RUNNING: "export_in_progress",
            JobStatus.COMPLETED: "export_succeeded",
            JobStatus.FAILED: "export_failed",
        }
        return mapping[status]


repository = SQLiteRepository()
