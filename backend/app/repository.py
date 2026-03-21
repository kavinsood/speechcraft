from __future__ import annotations

import hashlib
import io
import json
import math
import os
import re
import shutil
import threading
import wave
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
from sqlalchemy.orm import selectinload
from sqlmodel import Session, SQLModel, create_engine, delete, select

from .models import (
    AudioVariant,
    AudioVariantCreate,
    AudioVariantRunRequest,
    AudioVariantView,
    ActiveVariantUpdate,
    EditCommit,
    ExportRun,
    ExportPreview,
    ForcedAlignAndPackRequest,
    ImportBatch,
    ImportBatchCreate,
    JobKind,
    JobStatus,
    MediaCleanupResult,
    ProcessingJob,
    ProcessingJobView,
    ProjectSummary,
    ReferenceAsset,
    ReferenceAssetCreate,
    ReviewWindow,
    ReviewWindowView,
    ReviewStatus,
    SliceRevision,
    SliceSaveRequest,
    SliceDetail,
    SliceSummary,
    Slice,
    SliceEdlUpdate,
    SliceSplitRequest,
    SliceTagLink,
    SliceTagUpdate,
    SliceStatusUpdate,
    SliceTranscriptUpdate,
    SourceRecordingView,
    TagPayload,
    TagView,
    SourceRecording,
    SourceRecordingCreate,
    RecordingDerivativeCreate,
    SlicerHandoffRequest,
    Tag,
    TranscriptSummaryView,
    TranscriptView,
    Transcript,
    WaveformPeaks,
    utc_now,
)
from .slicer_core import mock_forced_align, pack_aligned_words

DATA_VERSION_EXTERNAL_VARIANT_REHOME = 1
DATA_VERSION_SLICE_REVISION_HISTORY = 2
LATEST_DATA_VERSION = DATA_VERSION_SLICE_REVISION_HISTORY


class SliceSaveValidationError(ValueError):
    """Raised when a slice save request is syntactically valid but semantically invalid."""


def _configured_path(env_name: str, fallback: Path) -> Path:
    raw_value = os.getenv(env_name)
    if raw_value is None or not raw_value.strip():
        return fallback
    return Path(raw_value).expanduser()


@dataclass
class SQLiteRepository:
    db_path: Path = field(
        default_factory=lambda: _configured_path(
            "SPEECHCRAFT_DB_PATH",
            Path(__file__).resolve().parent.parent / "data" / "project.db",
        )
    )
    legacy_seed_path: Path = field(
        default_factory=lambda: _configured_path(
            "SPEECHCRAFT_LEGACY_SEED_PATH",
            Path(__file__).resolve().parent.parent / "data" / "phase1-demo.json",
        )
    )
    media_root: Path = field(
        default_factory=lambda: _configured_path(
            "SPEECHCRAFT_MEDIA_ROOT",
            Path(__file__).resolve().parent.parent / "data" / "media",
        )
    )
    exports_root: Path = field(
        default_factory=lambda: _configured_path(
            "SPEECHCRAFT_EXPORTS_ROOT",
            Path(__file__).resolve().parent.parent / "exports",
        )
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
        self._migrate_enum_storage()
        self._migrate_editcommit_schema()
        self._seed_if_needed()
        self._run_data_migrations()

    def list_projects(self) -> list[ProjectSummary]:
        with self._session() as session:
            batches = session.exec(select(ImportBatch)).all()
            summaries = [self._project_summary(session, batch) for batch in batches]
            return sorted(summaries, key=lambda item: item.updated_at, reverse=True)

    def get_project(self, project_id: str) -> ProjectSummary:
        with self._session() as session:
            return self._project_summary(session, self._get_batch(session, project_id))

    def list_review_windows(self, recording_id: str) -> list[ReviewWindowView]:
        with self._session() as session:
            self._get_source_recording(session, recording_id)
            windows = session.exec(
                select(ReviewWindow)
                .where(ReviewWindow.source_recording_id == recording_id)
                .order_by(ReviewWindow.order_index, ReviewWindow.created_at)
            ).all()
            return [self._review_window_view(window) for window in windows]

    def get_project_slices(self, project_id: str) -> list[SliceSummary]:
        with self._session() as session:
            self._get_batch(session, project_id)
            return [self._to_slice_summary(slice_row) for slice_row in self._get_batch_slice_summaries(session, project_id)]

    def get_slice_detail(self, slice_id: str) -> SliceDetail:
        with self._session() as session:
            return self._get_slice_detail(session, slice_id)

    def get_processing_job(self, job_id: str) -> ProcessingJobView:
        with self._session() as session:
            return self._processing_job_view(self._get_processing_job(session, job_id))

    def list_source_recording_jobs(self, recording_id: str) -> list[ProcessingJobView]:
        with self._session() as session:
            self._get_source_recording(session, recording_id)
            jobs = session.exec(
                select(ProcessingJob)
                .where(ProcessingJob.source_recording_id == recording_id)
                .order_by(ProcessingJob.created_at.desc())
            ).all()
            return [self._processing_job_view(job) for job in jobs]

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
            protected_variant_ids = set(session.exec(select(ReferenceAsset.audio_variant_id)).all())
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
                referenced_ids = variant_ids & protected_variant_ids
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
            protected_variant_ids |= self._get_revision_referenced_variant_ids(
                session,
                [slice_row.id for slice_row in remaining_slices],
            )
            for slice_row in remaining_slices:
                for variant in list(slice_row.variants):
                    if variant.id in protected_variant_ids:
                        continue
                    if variant.id == slice_row.active_variant_id or variant.is_original:
                        continue
                    deleted_variant_ids.append(variant.id)
                    deleted_paths.append(variant.file_path)
                    session.delete(variant)

            session.commit()

        deleted_file_count = self._delete_unreferenced_media_files(deleted_paths)
        deleted_file_count += self._prune_derived_media_cache()
        return MediaCleanupResult(
            project_id=project_id,
            deleted_slice_count=len(deleted_slice_ids),
            deleted_variant_count=len(deleted_variant_ids),
            deleted_file_count=deleted_file_count,
            skipped_reference_count=skipped_reference_count,
            deleted_slice_ids=deleted_slice_ids,
            deleted_variant_ids=deleted_variant_ids,
        )

    def save_slice_state(self, slice_id: str, payload: SliceSaveRequest) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_loaded_slice(session, slice_id)
            transcript = self._get_transcript(session, slice_row.id)
            current_transcript = self._transcript_text(transcript)
            current_tags = self._normalized_tag_payloads(self._current_tag_payloads(session, slice_row))
            next_transcript = payload.modified_text if payload.modified_text is not None else current_transcript
            next_tags = (
                self._normalized_tag_payloads(payload.tags)
                if payload.tags is not None
                else current_tags
            )
            next_status = payload.status if payload.status is not None else slice_row.status

            state_changed = (
                next_transcript != current_transcript
                or next_tags != current_tags
                or next_status != slice_row.status
            )
            if not state_changed and not payload.is_milestone:
                message_only_request = (
                    payload.message is not None
                    and payload.modified_text is None
                    and payload.tags is None
                    and payload.status is None
                )
                if message_only_request:
                    raise SliceSaveValidationError("message requires milestone or state change")
                return self._get_slice_detail(session, slice_id)

            if payload.modified_text is not None:
                transcript.modified_text = payload.modified_text
                transcript.is_modified = payload.modified_text != transcript.original_text
                session.add(transcript)

            if payload.tags is not None:
                self._replace_slice_tags(session, slice_row, payload.tags)

            if payload.status is not None:
                slice_row.status = payload.status

            self._touch_slice(slice_row)
            session.add(slice_row)
            session.flush()
            self._append_slice_revision(
                session,
                slice_row,
                message=payload.message,
                is_milestone=payload.is_milestone,
            )
            session.commit()
            session.expire_all()
            detail = self._get_slice_detail(session, slice_id)
        self._warm_slice_artifacts_for_id(slice_id)
        return detail

    def update_slice_status(self, slice_id: str, payload: SliceStatusUpdate) -> SliceDetail:
        return self.save_slice_state(
            slice_id,
            SliceSaveRequest(
                status=payload.status,
                message=f"Status: {payload.status.value.replace('_', ' ')}",
            ),
        )

    def update_slice_transcript(self, slice_id: str, payload: SliceTranscriptUpdate) -> SliceDetail:
        return self.save_slice_state(
            slice_id,
            SliceSaveRequest(modified_text=payload.modified_text, message="Transcript updated"),
        )

    def update_slice_tags(self, slice_id: str, payload: SliceTagUpdate) -> SliceDetail:
        return self.save_slice_state(
            slice_id,
            SliceSaveRequest(tags=payload.tags, message="Tags updated"),
        )

    def append_edl_operation(self, slice_id: str, payload: SliceEdlUpdate) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_loaded_slice(session, slice_id)
            next_operations = [
                *self._collect_edl_operations(slice_row),
                payload.model_dump(mode="json"),
            ]
            self._touch_slice(slice_row)
            session.add(slice_row)
            session.flush()
            self._append_slice_revision(
                session,
                slice_row,
                edl_operations=next_operations,
                message=self._edl_message(payload),
            )
            session.commit()
            session.expire_all()
            detail = self._get_slice_detail(session, slice_id)
        self._warm_slice_artifacts_for_id(slice_id)
        return detail

    def undo_slice(self, slice_id: str) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_loaded_slice(session, slice_id)
            if slice_row.active_commit_id is None:
                raise ValueError("No earlier edit state is available")
            active_commit = self._get_edit_commit(session, slice_row.active_commit_id)
            if active_commit.parent_commit_id is None:
                raise ValueError("No earlier edit state is available")
            target_commit = self._get_edit_commit(session, active_commit.parent_commit_id)
            self._restore_slice_from_revision(session, slice_row, target_commit)
            session.commit()
            session.expire_all()
            detail = self._get_slice_detail(session, slice_id)
        self._warm_slice_artifacts_for_id(slice_id)
        return detail

    def redo_slice(self, slice_id: str) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_loaded_slice(session, slice_id)
            redo_target = self._get_redo_target(session, slice_row)
            if redo_target is None:
                raise ValueError("No newer edit state is available")
            self._restore_slice_from_revision(session, slice_row, redo_target)
            session.commit()
            session.expire_all()
            detail = self._get_slice_detail(session, slice_id)
        self._warm_slice_artifacts_for_id(slice_id)
        return detail

    def split_slice(self, slice_id: str, payload: SliceSplitRequest) -> list[SliceSummary]:
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
            inherited_ops = self._collect_edl_operations(source_slice)

            left_slice = Slice(
                id=left_id,
                source_recording_id=source_slice.source_recording_id,
                active_variant_id=left_variant_id,
                active_commit_id=None,
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
                active_commit_id=None,
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
                left_transcript,
                right_transcript,
            ]:
                session.add(item)
            session.flush()
            self._replace_slice_tags(session, left_slice, [TagPayload(name=tag.name, color=tag.color) for tag in source_slice.tags])
            self._replace_slice_tags(session, right_slice, [TagPayload(name=tag.name, color=tag.color) for tag in source_slice.tags])
            session.flush()
            self._append_slice_revision(
                session,
                left_slice,
                edl_operations=[
                    *inherited_ops,
                    {
                        "op": "crop",
                        "range": {"start_seconds": 0.0, "end_seconds": split_at},
                        "duration_seconds": None,
                    },
                ],
                message="Split slice (left)",
                created_at=now,
            )
            self._append_slice_revision(
                session,
                right_slice,
                edl_operations=[
                    *inherited_ops,
                    {
                        "op": "crop",
                        "range": {"start_seconds": split_at, "end_seconds": current_duration},
                        "duration_seconds": None,
                    },
                ],
                message="Split slice (right)",
                created_at=now,
            )
            self._shift_order_indices(session, recording.batch_id, order_index, 2, exclude_ids={source_slice.id})
            session.commit()
            session.expire_all()
            summaries = [self._to_slice_summary(item) for item in self._get_batch_slices(session, recording.batch_id)]
        for created_id in [left_id, right_id]:
            self._warm_slice_artifacts_for_id(created_id)
        return summaries

    def merge_with_next_slice(self, slice_id: str) -> list[SliceSummary]:
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
                active_commit_id=None,
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
            session.flush()
            self._append_slice_revision(
                session,
                merged_slice,
                edl_operations=[],
                message="Merged slice baseline",
                created_at=now,
            )
            session.commit()
            session.expire_all()
            summaries = [self._to_slice_summary(item) for item in self._get_batch_slices(session, batch_id)]
        self._warm_slice_artifacts_for_id(merged_id)
        return summaries

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
        safe_bins = max(32, min(bins, 2048))
        with self._session() as session:
            slice_row = self._get_loaded_slice(session, slice_id)
            cache_path = self._waveform_peaks_cache_path(slice_row, safe_bins)
            if cache_path.exists():
                cached = json.loads(cache_path.read_text())
                return WaveformPeaks.model_validate(cached)
            media_path = self._materialize_slice_media_path(session, slice_row)
            return self._ensure_waveform_peaks_cache(slice_row, media_path, safe_bins)

    def get_clip_audio_bytes(self, slice_id: str) -> bytes:
        with self._session() as session:
            slice_row = self._get_loaded_slice(session, slice_id)
            return self._render_slice_audio_bytes(session, slice_row)

    def get_slice_media_path(self, slice_id: str) -> Path:
        with self._session() as session:
            slice_row = self._get_loaded_slice(session, slice_id)
            return self._materialize_slice_media_path(session, slice_row)

    def get_source_recording_window_media_path(
        self,
        recording_id: str,
        start_seconds: float,
        end_seconds: float,
    ) -> Path:
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            return self._materialize_review_window_media_path(recording, start_seconds, end_seconds)

    def get_variant_media_path(self, variant_id: str) -> Path:
        with self._session() as session:
            variant = session.exec(
                select(AudioVariant)
                .where(AudioVariant.id == variant_id)
                .options(selectinload(AudioVariant.parent_slice).selectinload(Slice.source_recording))
            ).first()
            if variant is None:
                raise KeyError(variant_id)
            return self._resolve_variant_media_path(Path(variant.file_path))

    def create_import_batch(self, payload: ImportBatchCreate) -> ProjectSummary:
        with self._session() as session:
            batch = ImportBatch(id=payload.id, name=payload.name)
            session.add(batch)
            session.commit()
            session.refresh(batch)
            return self._project_summary(session, batch)

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

    def register_slicer_chunks(self, recording_id: str, payload: SlicerHandoffRequest) -> list[ReviewWindowView]:
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            session.exec(delete(ReviewWindow).where(ReviewWindow.source_recording_id == recording.id))
            created_windows: list[ReviewWindow] = []
            for chunk in payload.windows:
                self._validate_review_window_bounds(recording, chunk.start_seconds, chunk.end_seconds)
                window = ReviewWindow(
                    id=self._new_id("review-window"),
                    source_recording_id=recording.id,
                    start_seconds=chunk.start_seconds,
                    end_seconds=chunk.end_seconds,
                    rough_transcript=chunk.rough_transcript,
                    order_index=chunk.order_index,
                    window_metadata=chunk.model_metadata,
                )
                session.add(window)
                created_windows.append(window)
            session.commit()
            return [self._review_window_view(window) for window in created_windows]

    def enqueue_forced_align_and_pack(
        self,
        recording_id: str,
        payload: ForcedAlignAndPackRequest,
    ) -> ProcessingJobView:
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            if payload.review_window_ids is not None:
                matching_windows = session.exec(
                    select(ReviewWindow).where(
                        ReviewWindow.source_recording_id == recording.id,
                        ReviewWindow.id.in_(payload.review_window_ids),
                    )
                ).all()
                if len(matching_windows) != len(payload.review_window_ids):
                    raise ValueError("One or more review windows do not belong to the source recording")
            job = ProcessingJob(
                id=self._new_id("job"),
                kind=JobKind.FORCED_ALIGN_AND_PACK,
                status=JobStatus.PENDING,
                source_recording_id=recording.id,
                input_payload=payload.model_dump(mode="json"),
            )
            session.add(job)
            session.commit()
            session.refresh(job)
            job_view = self._processing_job_view(job)
        threading.Thread(
            target=self._run_processing_job,
            args=(job_view.id,),
            daemon=True,
            name=f"speechcraft-job-{job_view.id}",
        ).start()
        return job_view

    def create_audio_variant(self, slice_id: str, payload: AudioVariantCreate) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_loaded_slice(session, slice_id)
            recording = self._get_source_recording(session, slice_row.source_recording_id)
            self._validate_audio_asset(Path(payload.file_path), payload.sample_rate, recording.num_channels, payload.num_samples)
            variant_id = self._new_id("variant")
            managed_variant_path = self._ingest_variant_asset(Path(payload.file_path), variant_id)
            variant = AudioVariant(
                id=variant_id,
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
            session.flush()
            self._append_slice_revision(
                session,
                slice_row,
                message=f"Created variant via {payload.generator_model}",
            )
            session.commit()
            session.expire_all()
            detail = self._get_slice_detail(session, slice_id)
        self._warm_slice_artifacts_for_id(slice_id)
        return detail

    def run_audio_variant(self, slice_id: str, payload: AudioVariantRunRequest) -> SliceDetail:
        with self._session() as session:
            slice_row = self._get_loaded_slice(session, slice_id)
            recording = slice_row.source_recording
            active_variant = slice_row.active_variant
            if active_variant is None:
                raise ValueError("Slice has no active variant to process")
            try:
                source_path = self._get_variant_audio_path(active_variant, recording.num_channels)
            except FileNotFoundError as exc:
                raise ValueError(f"Active variant media is missing on disk: {exc}") from exc
            variant_id = self._new_id("variant")
            target_path = self._managed_variant_path(variant_id)
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
            session.flush()
            self._append_slice_revision(
                session,
                slice_row,
                message=f"Ran {payload.generator_model}",
            )
            session.commit()
            session.expire_all()
            detail = self._get_slice_detail(session, slice_id)
        self._warm_slice_artifacts_for_id(slice_id)
        return detail

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
            session.flush()
            self._append_slice_revision(
                session,
                slice_row,
                message=f"Activated variant {matching_variant.generator_model or matching_variant.id}",
            )
            session.commit()
            session.expire_all()
            detail = self._get_slice_detail(session, slice_id)
        self._warm_slice_artifacts_for_id(slice_id)
        return detail

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

    def _run_data_migrations(self) -> None:
        version = self._get_data_version()
        if version < DATA_VERSION_EXTERNAL_VARIANT_REHOME:
            self._migrate_external_variant_media()
            self._set_data_version(DATA_VERSION_EXTERNAL_VARIANT_REHOME)
            version = DATA_VERSION_EXTERNAL_VARIANT_REHOME
        if version < DATA_VERSION_SLICE_REVISION_HISTORY:
            self._migrate_legacy_slice_revision_history()
            self._set_data_version(DATA_VERSION_SLICE_REVISION_HISTORY)

    def _get_data_version(self) -> int:
        with self.engine.begin() as connection:
            raw = connection.exec_driver_sql("PRAGMA user_version").scalar()
        return int(raw or 0)

    def _set_data_version(self, version: int) -> None:
        with self.engine.begin() as connection:
            connection.exec_driver_sql(f"PRAGMA user_version = {version}")

    def _migrate_editcommit_schema(self) -> None:
        with self.engine.begin() as connection:
            columns = {
                row[1]
                for row in connection.exec_driver_sql("PRAGMA table_info(editcommit)").fetchall()
            }
            if not columns:
                return
            if "transcript_text" not in columns:
                connection.exec_driver_sql("ALTER TABLE editcommit ADD COLUMN transcript_text TEXT NOT NULL DEFAULT ''")
            if "status" not in columns:
                connection.exec_driver_sql(
                    "ALTER TABLE editcommit ADD COLUMN status TEXT NOT NULL DEFAULT 'unresolved'"
                )
            if "tags_payload" not in columns:
                connection.exec_driver_sql("ALTER TABLE editcommit ADD COLUMN tags_payload JSON")
            if "active_variant_id_snapshot" not in columns:
                connection.exec_driver_sql("ALTER TABLE editcommit ADD COLUMN active_variant_id_snapshot TEXT")
            if "message" not in columns:
                connection.exec_driver_sql("ALTER TABLE editcommit ADD COLUMN message TEXT")
            if "is_milestone" not in columns:
                connection.exec_driver_sql(
                    "ALTER TABLE editcommit ADD COLUMN is_milestone INTEGER NOT NULL DEFAULT 0"
                )

    def _migrate_enum_storage(self) -> None:
        replacements = {
            "slice": {"status": {status.name: status.value for status in ReviewStatus}},
            "editcommit": {"status": {status.name: status.value for status in ReviewStatus}},
            "exportrun": {"status": {status.name: status.value for status in JobStatus}},
            "processingjob": {
                "status": {status.name: status.value for status in JobStatus},
                "kind": {kind.name: kind.value for kind in JobKind},
            },
        }
        with self.engine.begin() as connection:
            tables = {
                row[0]
                for row in connection.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            for table_name, columns in replacements.items():
                if table_name not in tables:
                    continue
                existing_columns = {
                    row[1]
                    for row in connection.exec_driver_sql(f"PRAGMA table_info({table_name})").fetchall()
                }
                for column_name, mapping in columns.items():
                    if column_name not in existing_columns:
                        continue
                    for legacy_value, normalized_value in mapping.items():
                        connection.exec_driver_sql(
                            f"UPDATE {table_name} SET {column_name} = ? WHERE {column_name} = ?",
                            (normalized_value, legacy_value),
                        )

    def _migrate_legacy_slice_revision_history(self) -> None:
        with self._session() as session:
            slices = session.exec(
                select(Slice).options(
                    selectinload(Slice.source_recording),
                    selectinload(Slice.transcript),
                    selectinload(Slice.tags),
                    selectinload(Slice.variants),
                    selectinload(Slice.commits),
                    selectinload(Slice.active_variant),
                    selectinload(Slice.active_commit),
                )
            ).all()
            changed = False
            for slice_row in slices:
                current_tags = [tag.model_dump(mode="json") for tag in self._current_tag_payloads(session, slice_row)]
                current_transcript = self._transcript_text(slice_row.transcript)
                baseline_message = "Imported slice baseline"
                for commit in slice_row.commits:
                    commit_changed = False
                    legacy_snapshot_commit = (
                        commit.active_variant_id_snapshot is None and commit.message is None
                    )
                    if legacy_snapshot_commit and commit.transcript_text == "":
                        commit.transcript_text = current_transcript
                        commit_changed = True
                    if legacy_snapshot_commit and not commit.tags_payload:
                        commit.tags_payload = list(current_tags)
                        commit_changed = True
                    if commit.active_variant_id_snapshot is None:
                        commit.active_variant_id_snapshot = slice_row.active_variant_id
                        commit_changed = True
                    if commit.message is None:
                        commit.message = baseline_message
                        commit_changed = True
                    if commit_changed:
                        if legacy_snapshot_commit:
                            commit.status = slice_row.status
                        session.add(commit)
                        changed = True
                if slice_row.active_commit_id is None:
                    self._append_slice_revision(
                        session,
                        slice_row,
                        edl_operations=[],
                        message=baseline_message,
                        created_at=self._clip_updated_at(slice_row),
                        parent_commit_id=None,
                    )
                    changed = True
                elif slice_row.active_commit is None and slice_row.commits:
                    latest_commit = sorted(slice_row.commits, key=lambda commit: commit.created_at)[-1]
                    slice_row.active_commit_id = latest_commit.id
                    session.add(slice_row)
                    changed = True
            if changed:
                session.commit()

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
        source_recordings: dict[tuple[str, str], SourceRecording] = {}
        projects = legacy.get("projects", {})
        clips_by_project = legacy.get("clips_by_project", {})
        exports_by_project = legacy.get("exports_by_project", {})

        for project_id, batch_payload in projects.items():
            batch = ImportBatch(
                id=batch_payload["id"],
                name=batch_payload["name"],
                created_at=datetime.fromisoformat(batch_payload["created_at"].replace("Z", "+00:00")),
            )
            session.add(batch)
            session.flush()

            active_clips = clips_by_project.get(project_id, [])
            for clip_payload in active_clips:
                source_key = (batch.id, clip_payload["source_file_id"])
                if source_key in source_recordings:
                    continue
                source_duration = max(
                    item["original_end_time"]
                    for item in active_clips
                    if item["source_file_id"] == clip_payload["source_file_id"]
                )
                recording_id = f"source-{batch.id}-{clip_payload['source_file_id']}"
                source_path = self._managed_media_path("sources", recording_id)
                source_path.parent.mkdir(parents=True, exist_ok=True)
                if not source_path.exists():
                    source_path.write_bytes(
                        self._render_synthetic_wave_bytes(
                            clip_payload["sample_rate"],
                            clip_payload["channels"],
                            max(source_duration + 1.0, 1.0),
                            recording_id,
                        )
                    )
                source_recording = SourceRecording(
                    id=recording_id,
                    batch_id=batch.id,
                    file_path=str(source_path),
                    sample_rate=clip_payload["sample_rate"],
                    num_channels=clip_payload["channels"],
                    num_samples=max(int((source_duration + 1.0) * clip_payload["sample_rate"]), 1),
                )
                session.add(source_recording)
                source_recordings[source_key] = source_recording

            session.flush()
            for clip_payload in active_clips:
                variant_id = f"variant-{clip_payload['id']}"
                transcript_id = f"transcript-{clip_payload['id']}"
                recording_id = f"source-{batch.id}-{clip_payload['source_file_id']}"
                variant_path = self._managed_variant_path(variant_id)
                variant_path.parent.mkdir(parents=True, exist_ok=True)
                raw_audio_path = Path(clip_payload["audio_path"]).expanduser() if clip_payload.get("audio_path") else None
                if raw_audio_path is not None and raw_audio_path.exists():
                    resolved_audio_path = raw_audio_path.resolve()
                    if resolved_audio_path != variant_path:
                        shutil.copyfile(resolved_audio_path, variant_path)
                elif not variant_path.exists():
                    variant_path.write_bytes(
                        self._render_synthetic_wave_bytes(
                            clip_payload["sample_rate"],
                            clip_payload["channels"],
                            max(clip_payload["duration_seconds"], 0.2),
                            clip_payload["id"],
                        )
                    )
                slice_row = Slice(
                    id=clip_payload["id"],
                    source_recording_id=recording_id,
                    active_variant_id=variant_id,
                    active_commit_id=None,
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
                session.flush()
                self._replace_slice_tags(session, slice_row, [TagPayload(**tag) for tag in clip_payload["tags"]])
                session.flush()
                self._append_slice_revision(
                    session,
                    slice_row,
                    edl_operations=list(clip_payload["clip_edl"]),
                    message="Imported legacy slice",
                    created_at=datetime.fromisoformat(clip_payload["updated_at"].replace("Z", "+00:00")),
                )
            for export_payload in exports_by_project.get(batch.id, []):
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
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_bytes(self._render_synthetic_wave_bytes(48000, 1, 20.0, "src-001"))
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
                windows=[
                    {
                        "start_seconds": 12.4,
                        "end_seconds": 15.68,
                        "rough_transcript": "The workstation should make this painless.",
                        "order_index": 10,
                    }
                ]
            ),
        )
        with self._session() as seed_session:
            seeded_recording = self._get_source_recording(seed_session, recording.id)
            self._create_slice_from_source_span(
                seed_session,
                seeded_recording,
                slice_id="clip-001",
                start_seconds=12.4,
                end_seconds=15.68,
                transcript_text="The workstation should make this painless.",
                order_index=10,
            )
            seed_session.commit()

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

    def _get_review_window(self, session: Session, review_window_id: str) -> ReviewWindow:
        window = session.get(ReviewWindow, review_window_id)
        if window is None:
            raise KeyError(review_window_id)
        return window

    def _get_processing_job(self, session: Session, job_id: str) -> ProcessingJob:
        job = session.get(ProcessingJob, job_id)
        if job is None:
            raise KeyError(job_id)
        return job

    def _get_transcript(self, session: Session, slice_id: str) -> Transcript:
        transcript = session.exec(select(Transcript).where(Transcript.slice_id == slice_id)).first()
        if transcript is None:
            raise KeyError(slice_id)
        return transcript

    def _validate_review_window_bounds(
        self,
        recording: SourceRecording,
        start_seconds: float,
        end_seconds: float,
    ) -> None:
        if start_seconds < 0:
            raise ValueError("Review window start must be non-negative")
        if end_seconds <= start_seconds:
            raise ValueError("Review window end must be greater than the start")
        if end_seconds > recording.duration_s:
            raise ValueError("Review window end exceeds the source recording duration")

    def _create_slice_from_source_span(
        self,
        session: Session,
        recording: SourceRecording,
        *,
        slice_id: str,
        start_seconds: float,
        end_seconds: float,
        transcript_text: str,
        order_index: int,
        speaker_name: str = "speaker_a",
        language: str = "en",
        alignment_data: dict[str, Any] | None = None,
        created_at: datetime | None = None,
    ) -> Slice:
        self._validate_review_window_bounds(recording, start_seconds, end_seconds)
        variant_id = self._new_id("variant")
        variant_path = self._managed_variant_path(variant_id)
        variant_path.parent.mkdir(parents=True, exist_ok=True)
        variant_bytes = self._extract_source_window_wav_bytes(recording, start_seconds, end_seconds)
        variant_path.write_bytes(variant_bytes)
        sample_rate, _channels, num_samples = self._wav_metadata(variant_bytes)
        now = created_at or utc_now()
        slice_row = Slice(
            id=slice_id,
            source_recording_id=recording.id,
            active_variant_id=variant_id,
            active_commit_id=None,
            status=ReviewStatus.UNRESOLVED,
            model_metadata={
                "order_index": order_index,
                "source_file_id": recording.id,
                "working_asset_id": variant_id,
                "original_start_time": start_seconds,
                "original_end_time": end_seconds,
                "speaker_name": speaker_name,
                "language": language,
                "is_superseded": False,
                "updated_at": now.isoformat(),
            },
            created_at=now,
        )
        variant = AudioVariant(
            id=variant_id,
            slice_id=slice_row.id,
            file_path=str(variant_path),
            is_original=True,
            generator_model="slicer",
            sample_rate=sample_rate,
            num_samples=num_samples,
        )
        transcript = Transcript(
            id=self._new_id("transcript"),
            slice_id=slice_row.id,
            original_text=transcript_text,
            alignment_data=alignment_data,
        )
        session.add(slice_row)
        session.add(variant)
        session.add(transcript)
        session.flush()
        self._append_slice_revision(
            session,
            slice_row,
            edl_operations=[],
            message="Imported slice baseline",
            created_at=now,
        )
        return slice_row

    def _get_batch_slices(self, session: Session, batch_id: str) -> list[Slice]:
        return [
            item
            for item in self._get_all_batch_slices(session, batch_id)
            if not self._slice_metadata(item).get("is_superseded", False)
        ]

    def _get_batch_slice_summaries(self, session: Session, batch_id: str) -> list[Slice]:
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
                selectinload(Slice.active_variant),
                selectinload(Slice.active_commit),
            )
        ).all()
        return sorted(
            [
                item
                for item in slices
                if not self._slice_metadata(item).get("is_superseded", False)
            ],
            key=lambda slice_row: (
                int(self._slice_metadata(slice_row).get("order_index", 0)),
                self._as_utc(slice_row.created_at),
            ),
        )

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

    def _collect_edl_operations(self, slice_row: Slice) -> list[dict[str, Any]]:
        if slice_row.active_commit is None:
            return []
        return list(slice_row.active_commit.edl_operations or [])

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

    def _current_tag_payloads(self, session: Session, slice_row: Slice) -> list[TagPayload]:
        return [
            TagPayload(name=tag.name, color=tag.color)
            for tag in self._get_slice_tags(session, slice_row.id)
        ]

    def _normalized_tag_payloads(self, tags: list[TagPayload] | None) -> list[dict[str, str]]:
        return [
            {"name": tag.name, "color": tag.color}
            for tag in sorted(tags or [], key=lambda item: (item.name.lower(), item.color))
        ]

    def _append_slice_revision(
        self,
        session: Session,
        slice_row: Slice,
        *,
        edl_operations: list[dict[str, Any]] | None = None,
        message: str | None = None,
        is_milestone: bool = False,
        created_at: datetime | None = None,
        parent_commit_id: str | None | object = ...,
    ) -> EditCommit:
        transcript = slice_row.transcript or self._get_transcript(session, slice_row.id)
        revision = EditCommit(
            id=self._new_id("edit"),
            slice_id=slice_row.id,
            parent_commit_id=slice_row.active_commit_id if parent_commit_id is ... else parent_commit_id,
            edl_operations=list(edl_operations if edl_operations is not None else self._collect_edl_operations(slice_row)),
            transcript_text=self._transcript_text(transcript),
            status=slice_row.status,
            tags_payload=[tag.model_dump(mode="json") for tag in self._current_tag_payloads(session, slice_row)],
            active_variant_id_snapshot=slice_row.active_variant_id,
            message=message,
            is_milestone=is_milestone,
            created_at=created_at or utc_now(),
        )
        session.add(revision)
        session.flush()
        slice_row.active_commit_id = revision.id
        session.add(slice_row)
        return revision

    def _restore_slice_from_revision(self, session: Session, slice_row: Slice, revision: EditCommit) -> None:
        transcript = slice_row.transcript or self._get_transcript(session, slice_row.id)
        slice_row.active_variant_id = revision.active_variant_id_snapshot
        slice_row.active_commit_id = revision.id
        slice_row.status = revision.status
        transcript.modified_text = (
            None if revision.transcript_text == transcript.original_text else revision.transcript_text
        )
        transcript.is_modified = transcript.modified_text != transcript.original_text
        self._replace_slice_tags(
            session,
            slice_row,
            [TagPayload.model_validate(tag) for tag in revision.tags_payload or []],
        )
        self._touch_slice(slice_row)
        session.add(transcript)
        session.add(slice_row)

    def _edl_message(self, payload: SliceEdlUpdate) -> str:
        if payload.op == "delete_range" and payload.range is not None:
            return f"Deleted {payload.range.end_seconds - payload.range.start_seconds:.2f}s"
        if payload.op == "insert_silence":
            return f"Inserted {float(payload.duration_seconds or 0.0):.2f}s silence"
        if payload.op == "crop" and payload.range is not None:
            return (
                f"Cropped to {payload.range.start_seconds:.2f}s-{payload.range.end_seconds:.2f}s"
            )
        return f"Applied {payload.op}"

    def _as_utc(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _project_summary(self, session: Session, batch: ImportBatch) -> ProjectSummary:
        created_at = self._as_utc(batch.created_at)
        updated_at = created_at
        recording_ids = session.exec(
            select(SourceRecording.id).where(SourceRecording.batch_id == batch.id)
        ).all()
        if recording_ids:
            for slice_row in session.exec(
                select(Slice).where(Slice.source_recording_id.in_(recording_ids))
            ).all():
                updated_at = max(updated_at, self._clip_updated_at(slice_row))
        latest_export = session.exec(
            select(ExportRun).where(ExportRun.batch_id == batch.id).order_by(ExportRun.created_at.desc())
        ).first()
        export_status = None
        if latest_export is not None:
            export_status = latest_export.status
            updated_at = max(
                updated_at,
                self._as_utc(latest_export.completed_at or latest_export.created_at),
            )
        return ProjectSummary(
            id=batch.id,
            name=batch.name,
            created_at=created_at,
            updated_at=updated_at,
            export_status=export_status,
        )

    def _normalize_export_run(self, run: ExportRun) -> ExportRun:
        run.created_at = self._as_utc(run.created_at)
        if run.completed_at is not None:
            run.completed_at = self._as_utc(run.completed_at)
        return run

    def _get_slice_detail(self, session: Session, slice_id: str) -> SliceDetail:
        return self._to_slice_detail(self._get_slice(session, slice_id))

    def _tag_view(self, tag: Tag) -> TagView:
        return TagView(id=tag.id, name=tag.name, color=tag.color)

    def _transcript_view(self, transcript: Transcript | None) -> TranscriptView | None:
        if transcript is None:
            return None
        return TranscriptView(
            id=transcript.id,
            slice_id=transcript.slice_id,
            original_text=transcript.original_text,
            modified_text=transcript.modified_text,
            is_modified=transcript.is_modified,
            alignment_data=transcript.alignment_data,
        )

    def _transcript_summary_view(self, transcript: Transcript | None) -> TranscriptSummaryView | None:
        if transcript is None:
            return None
        return TranscriptSummaryView(
            id=transcript.id,
            slice_id=transcript.slice_id,
            original_text=transcript.original_text,
            modified_text=transcript.modified_text,
            is_modified=transcript.is_modified,
        )

    def _audio_variant_view(self, variant: AudioVariant | None) -> AudioVariantView | None:
        if variant is None:
            return None
        return AudioVariantView(
            id=variant.id,
            slice_id=variant.slice_id,
            is_original=variant.is_original,
            generator_model=variant.generator_model,
            sample_rate=variant.sample_rate,
            num_samples=variant.num_samples,
        )

    def _source_recording_view(self, recording: SourceRecording | None) -> SourceRecordingView:
        if recording is None:
            raise ValueError("Slice is missing its source recording")
        return SourceRecordingView(
            id=recording.id,
            batch_id=recording.batch_id,
            parent_recording_id=recording.parent_recording_id,
            sample_rate=recording.sample_rate,
            num_channels=recording.num_channels,
            num_samples=recording.num_samples,
            processing_recipe=recording.processing_recipe,
        )

    def _review_window_view(self, window: ReviewWindow) -> ReviewWindowView:
        return ReviewWindowView(
            id=window.id,
            source_recording_id=window.source_recording_id,
            start_seconds=window.start_seconds,
            end_seconds=window.end_seconds,
            rough_transcript=window.rough_transcript,
            order_index=window.order_index,
            window_metadata=window.window_metadata,
            created_at=self._as_utc(window.created_at),
        )

    def _processing_job_view(self, job: ProcessingJob) -> ProcessingJobView:
        return ProcessingJobView(
            id=job.id,
            kind=job.kind,
            status=job.status,
            source_recording_id=job.source_recording_id,
            input_payload=job.input_payload,
            output_payload=job.output_payload,
            error_message=job.error_message,
            created_at=self._as_utc(job.created_at),
            started_at=self._as_utc(job.started_at) if job.started_at is not None else None,
            completed_at=self._as_utc(job.completed_at) if job.completed_at is not None else None,
        )

    def _revision_view(self, commit: EditCommit) -> SliceRevision:
        return SliceRevision(
            id=commit.id,
            slice_id=commit.slice_id,
            parent_commit_id=commit.parent_commit_id,
            edl_operations=list(commit.edl_operations or []),
            transcript_text=commit.transcript_text,
            status=commit.status,
            tags=[
                TagPayload.model_validate(tag)
                for tag in sorted(commit.tags_payload or [], key=lambda item: str(item.get("name", "")).lower())
            ],
            active_variant_id=commit.active_variant_id_snapshot,
            message=commit.message,
            is_milestone=commit.is_milestone,
            created_at=self._as_utc(commit.created_at),
        )

    def _to_slice_summary(self, slice_row: Slice) -> SliceSummary:
        active_commit = slice_row.active_commit
        return SliceSummary.model_validate(
            {
                "id": slice_row.id,
                "source_recording_id": slice_row.source_recording_id,
                "active_variant_id": slice_row.active_variant_id,
                "active_commit_id": slice_row.active_commit_id,
                "status": slice_row.status,
                "duration_seconds": self._slice_duration(slice_row),
                "model_metadata": self._slice_metadata(slice_row),
                "created_at": self._as_utc(slice_row.created_at),
                "transcript": self._transcript_summary_view(slice_row.transcript),
                "tags": [self._tag_view(tag) for tag in sorted(slice_row.tags, key=lambda tag: tag.name.lower())],
                "active_variant_generator_model": (
                    slice_row.active_variant.generator_model if slice_row.active_variant is not None else None
                ),
                "can_undo": active_commit.parent_commit_id is not None if active_commit is not None else False,
                "can_redo": False,
            }
        )

    def _to_slice_detail(self, slice_row: Slice) -> SliceDetail:
        active_commit = slice_row.active_commit
        commits = list(sorted(slice_row.commits, key=lambda commit: commit.created_at))
        return SliceDetail.model_validate(
            {
                **self._to_slice_summary(slice_row).model_dump(mode="json"),
                "transcript": self._transcript_view(slice_row.transcript),
                "source_recording": self._source_recording_view(slice_row.source_recording),
                "variants": [
                    self._audio_variant_view(variant)
                    for variant in sorted(slice_row.variants, key=lambda variant: (not variant.is_original, variant.id))
                ],
                "commits": [self._revision_view(commit) for commit in commits],
                "active_variant": self._audio_variant_view(slice_row.active_variant),
                "active_commit": self._revision_view(active_commit) if active_commit is not None else None,
                "can_undo": active_commit.parent_commit_id is not None if active_commit is not None else False,
                "can_redo": any(commit.parent_commit_id == slice_row.active_commit_id for commit in commits),
            }
        )

    def _touch_slice(self, slice_row: Slice, when: datetime | None = None) -> None:
        metadata = self._slice_metadata(slice_row)
        metadata["updated_at"] = self._as_utc(when or utc_now()).isoformat()
        slice_row.model_metadata = metadata

    def _transcript_text(self, transcript: Transcript | None) -> str:
        if transcript is None:
            return ""
        return transcript.modified_text if transcript.modified_text is not None else transcript.original_text

    def _speaker_name(self, slice_row: Slice) -> str:
        return str(self._slice_metadata(slice_row).get("speaker_name", "speaker_a"))

    def _language(self, slice_row: Slice) -> str:
        return str(self._slice_metadata(slice_row).get("language", "en"))

    def _slice_duration(self, slice_row: Slice) -> float:
        if slice_row.active_variant is not None:
            base_duration = slice_row.active_variant.duration_s
        else:
            base_duration = slice_row.source_recording.duration_s if slice_row.source_recording is not None else 0.0
        return round(self._apply_edl_to_duration(base_duration, self._collect_edl_operations(slice_row)), 2)

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

    def _extract_source_window_wav_bytes(
        self,
        recording: SourceRecording,
        start_seconds: float,
        end_seconds: float,
    ) -> bytes:
        self._validate_review_window_bounds(recording, start_seconds, end_seconds)
        source_path = Path(recording.file_path).expanduser().resolve(strict=False)
        self._validate_audio_asset(
            source_path,
            recording.sample_rate,
            recording.num_channels,
            recording.num_samples,
        )
        start_frame = max(int(round(start_seconds * recording.sample_rate)), 0)
        end_frame = min(int(round(end_seconds * recording.sample_rate)), recording.num_samples)
        frame_count = max(end_frame - start_frame, 1)
        output = io.BytesIO()
        with wave.open(str(source_path), "rb") as source_wav:
            source_wav.setpos(start_frame)
            frames = source_wav.readframes(frame_count)
            with wave.open(output, "wb") as target_wav:
                target_wav.setnchannels(source_wav.getnchannels())
                target_wav.setsampwidth(source_wav.getsampwidth())
                target_wav.setframerate(source_wav.getframerate())
                target_wav.writeframes(frames)
        return output.getvalue()

    def _wav_bytes_to_mono_samples(self, audio_bytes: bytes) -> tuple[np.ndarray, int]:
        channels, sample_width, sample_rate, frame_count, raw = self._read_pcm_wav(audio_bytes)
        if sample_width != 2:
            raise ValueError("Phase 1 only supports normalized 16-bit PCM WAV assets")
        if frame_count <= 0:
            return np.zeros(0, dtype=np.float64), sample_rate

        pcm = np.frombuffer(raw, dtype="<i2").astype(np.float64)
        if channels > 1:
            pcm = pcm.reshape(frame_count, channels).mean(axis=1)
        samples = pcm / 32768.0
        return samples, sample_rate

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

    def _render_slice_audio_bytes(self, session: Session, slice_row: Slice) -> bytes:
        active_variant = slice_row.active_variant
        if active_variant is None:
            if slice_row.active_variant_id is not None:
                raise ValueError(
                    f"Active variant {slice_row.active_variant_id} is missing for slice {slice_row.id}"
                )
            return self._render_synthetic_wave_bytes(48000, 1, 2.0, slice_row.id)
        try:
            audio_path = self._get_variant_audio_path(
                active_variant,
                slice_row.source_recording.num_channels if slice_row.source_recording is not None else None,
            )
        except FileNotFoundError as exc:
            raise ValueError(f"Active variant media is missing on disk: {exc}") from exc
        return self._apply_edl_to_wav_bytes(audio_path.read_bytes(), self._collect_edl_operations(slice_row))

    def _materialize_slice_media_path(self, session: Session, slice_row: Slice) -> Path:
        target_path = self._slice_render_cache_path(slice_row)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if not target_path.exists():
            target_path.write_bytes(self._render_slice_audio_bytes(session, slice_row))
        return target_path

    def _slice_audio_cache_identifier(self, slice_row: Slice) -> str:
        state_key = json.dumps(
            {
                "slice_id": slice_row.id,
                "active_variant_id": slice_row.active_variant_id,
                "edl_operations": self._collect_edl_operations(slice_row),
            },
            sort_keys=True,
        )
        fingerprint = hashlib.sha1(state_key.encode("utf-8")).hexdigest()[:12]
        return f"{slice_row.id}-{fingerprint}"

    def _slice_render_cache_path(self, slice_row: Slice) -> Path:
        return self._managed_media_path("slices", self._slice_audio_cache_identifier(slice_row))

    def _waveform_peaks_cache_path(self, slice_row: Slice, bins: int) -> Path:
        identifier = self._validate_managed_media_id(
            f"{self._slice_audio_cache_identifier(slice_row)}-bins-{bins}"
        )
        return (self.media_root / "peaks" / f"{identifier}.json").resolve()

    def _ensure_waveform_peaks_cache(self, slice_row: Slice, media_path: Path, bins: int) -> WaveformPeaks:
        cache_path = self._waveform_peaks_cache_path(slice_row, bins)
        if cache_path.exists():
            return WaveformPeaks.model_validate(json.loads(cache_path.read_text()))
        peaks = self._extract_waveform_peaks_from_bytes(media_path.read_bytes(), bins)
        payload = WaveformPeaks(clip_id=slice_row.id, bins=bins, peaks=peaks)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(payload.model_dump_json())
        return payload

    def _warm_slice_artifacts_for_id(self, slice_id: str, bins_values: tuple[int, ...] = (960,)) -> None:
        try:
            with self._session() as session:
                slice_row = self._get_loaded_slice(session, slice_id)
                media_path = self._materialize_slice_media_path(session, slice_row)
                for bins in bins_values:
                    self._ensure_waveform_peaks_cache(slice_row, media_path, bins)
        except (FileNotFoundError, ValueError):
            # Imported projects can temporarily point at offline media; cache warming should
            # never block transcript/status/tag persistence in that state.
            return

    def _validate_managed_media_id(self, identifier: str) -> str:
        normalized = identifier.strip()
        if not normalized:
            raise ValueError("Managed media identifier cannot be empty")
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", normalized) is None:
            raise ValueError(f"Managed media identifier contains unsafe characters: {identifier}")
        return normalized

    def _managed_media_path(self, category: str, identifier: str) -> Path:
        safe_identifier = self._validate_managed_media_id(identifier)
        return (self.media_root / category / f"{safe_identifier}.wav").resolve()

    def _managed_variant_path(self, variant_id: str) -> Path:
        return self._managed_media_path("variants", variant_id)

    def _review_window_cache_path(
        self,
        recording_id: str,
        start_seconds: float,
        end_seconds: float,
    ) -> Path:
        fingerprint_input = json.dumps(
            {
                "recording_id": recording_id,
                "start_seconds": round(start_seconds, 4),
                "end_seconds": round(end_seconds, 4),
            },
            sort_keys=True,
        )
        identifier = hashlib.sha1(fingerprint_input.encode("utf-8")).hexdigest()[:16]
        return self._managed_media_path("review-windows", f"{recording_id}-{identifier}")

    def _materialize_review_window_media_path(
        self,
        recording: SourceRecording,
        start_seconds: float,
        end_seconds: float,
    ) -> Path:
        target_path = self._review_window_cache_path(recording.id, start_seconds, end_seconds)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if not target_path.exists():
            target_path.write_bytes(
                self._extract_source_window_wav_bytes(recording, start_seconds, end_seconds)
            )
        return target_path

    def _prune_derived_media_cache(self) -> int:
        with self._session() as session:
            slices = session.exec(select(Slice)).all()
            keep_cache_ids = {self._slice_audio_cache_identifier(slice_row) for slice_row in slices}
        deleted_count = 0
        slices_root = self.media_root / "slices"
        peaks_root = self.media_root / "peaks"
        for path in slices_root.glob("*.wav"):
            cache_id = path.stem
            if cache_id in keep_cache_ids:
                continue
            path.unlink()
            deleted_count += 1
        for path in peaks_root.glob("*.json"):
            cache_id, _separator, _bins = path.stem.rpartition("-bins-")
            if cache_id in keep_cache_ids:
                continue
            path.unlink()
            deleted_count += 1
        return deleted_count

    def _ingest_variant_asset(self, path: Path, variant_id: str) -> Path:
        source_path = path.expanduser().resolve()
        if not source_path.exists():
            raise ValueError(f"Audio asset not found: {source_path}")
        target_path = self._managed_variant_path(variant_id)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if source_path != target_path:
            shutil.copyfile(source_path, target_path)
        return target_path

    def _get_variant_audio_path(self, variant: AudioVariant, expected_channels: int | None) -> Path:
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
            return raw_path

    def _migrate_external_variant_media(self) -> None:
        with self._session() as session:
            variants = session.exec(
                select(AudioVariant).options(selectinload(AudioVariant.parent_slice).selectinload(Slice.source_recording))
            ).all()
            updated = False
            for variant in variants:
                raw_path = Path(variant.file_path).expanduser().resolve(strict=False)
                try:
                    self._resolve_variant_media_path(raw_path)
                    continue
                except ValueError:
                    if not raw_path.exists():
                        continue
                expected_channels = (
                    variant.parent_slice.source_recording.num_channels
                    if variant.parent_slice is not None and variant.parent_slice.source_recording is not None
                    else None
                )
                self._get_variant_audio_path(variant, expected_channels)
                managed_path = self._managed_variant_path(variant.id)
                managed_path.parent.mkdir(parents=True, exist_ok=True)
                if raw_path != managed_path and not managed_path.exists():
                    shutil.copyfile(raw_path, managed_path)
                if variant.file_path != str(managed_path):
                    variant.file_path = str(managed_path)
                    session.add(variant)
                    updated = True
            if updated:
                session.commit()

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

    def _get_revision_referenced_variant_ids(self, session: Session, slice_ids: list[str]) -> set[str]:
        if not slice_ids:
            return set()
        return {
            variant_id
            for variant_id in session.exec(
                select(EditCommit.active_variant_id_snapshot).where(
                    EditCommit.slice_id.in_(slice_ids),
                    EditCommit.active_variant_id_snapshot.is_not(None),
                )
            ).all()
            if variant_id is not None
        }

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

    def _run_processing_job(self, job_id: str) -> None:
        with self._session() as session:
            job = self._get_processing_job(session, job_id)
            job.status = JobStatus.RUNNING
            job.started_at = utc_now()
            job.error_message = None
            session.add(job)
            session.commit()

        try:
            with self._session() as session:
                job = self._get_processing_job(session, job_id)
                if job.kind == JobKind.FORCED_ALIGN_AND_PACK:
                    self._run_forced_align_and_pack_job(session, job)
                else:
                    raise ValueError(f"Unsupported processing job kind: {job.kind}")
        except Exception as exc:
            with self._session() as session:
                job = self._get_processing_job(session, job_id)
                job.status = JobStatus.FAILED
                job.error_message = str(exc)
                job.completed_at = utc_now()
                session.add(job)
                session.commit()

    def _run_forced_align_and_pack_job(self, session: Session, job: ProcessingJob) -> None:
        if job.source_recording_id is None:
            raise ValueError("Forced align and pack job is missing its source recording")
        recording = self._get_source_recording(session, job.source_recording_id)
        payload = job.input_payload or {}
        requested_window_ids = payload.get("review_window_ids")
        if requested_window_ids:
            windows = session.exec(
                select(ReviewWindow)
                .where(
                    ReviewWindow.source_recording_id == recording.id,
                    ReviewWindow.id.in_(requested_window_ids),
                )
                .order_by(ReviewWindow.order_index, ReviewWindow.created_at)
            ).all()
        else:
            windows = session.exec(
                select(ReviewWindow)
                .where(ReviewWindow.source_recording_id == recording.id)
                .order_by(ReviewWindow.order_index, ReviewWindow.created_at)
            ).all()
        if not windows:
            raise ValueError("Forced align and pack job requires at least one review window")
        windows = list(sorted(windows, key=lambda window: (window.order_index, window.created_at)))

        master_transcript = " ".join(window.rough_transcript.strip() for window in windows if window.rough_transcript.strip())
        explicit_transcript = str(payload.get("transcript_text", "")).strip()
        if explicit_transcript:
            master_transcript = explicit_transcript
        if not master_transcript:
            raise ValueError("Forced align and pack job requires transcript_text")

        absolute_start = windows[0].start_seconds
        absolute_end = windows[-1].end_seconds
        audio_bytes = self._extract_source_window_wav_bytes(recording, absolute_start, absolute_end)
        audio_samples, sample_rate = self._wav_bytes_to_mono_samples(audio_bytes)
        if sample_rate <= 0:
            raise ValueError("Forced align and pack job produced invalid sample rate")

        relative_duration = max(absolute_end - absolute_start, 0.0)
        alignment_units = mock_forced_align(master_transcript, relative_duration)
        packed_slices = list(pack_aligned_words(alignment_units, audio_samples, sample_rate))

        existing_slices = self._get_batch_slices(session, recording.batch_id)
        next_order_index = (
            max(int(self._slice_metadata(slice_row).get("order_index", 0)) for slice_row in existing_slices) + 1
            if existing_slices
            else 0
        )
        created_slice_ids: list[str] = []

        for packed_slice in packed_slices:
            slice_row = self._create_slice_from_source_span(
                session,
                recording,
                slice_id=self._new_id("slice"),
                start_seconds=absolute_start + float(packed_slice["start_s"]),
                end_seconds=absolute_start + float(packed_slice["end_s"]),
                transcript_text=str(packed_slice["transcript_text"]),
                order_index=next_order_index,
                alignment_data={
                    "source": "mock_forced_align",
                    "relative_start_seconds": float(packed_slice["start_s"]),
                    "relative_end_seconds": float(packed_slice["end_s"]),
                },
            )
            created_slice_ids.append(slice_row.id)
            next_order_index += 1

        job.status = JobStatus.COMPLETED
        job.output_payload = {
            "created_slice_count": len(created_slice_ids),
            "created_slice_ids": created_slice_ids,
            "window_span": {
                "start_seconds": absolute_start,
                "end_seconds": absolute_end,
            },
        }
        job.error_message = None
        job.completed_at = utc_now()
        session.add(job)
        session.commit()

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
