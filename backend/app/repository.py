from __future__ import annotations

import hashlib
import io
import json
import math
import os
import re
import shutil
import subprocess
import tempfile
import wave
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
from sqlalchemy import func
from sqlalchemy import update as sql_update
from sqlalchemy.orm import selectinload
from sqlmodel import Session, SQLModel, create_engine, delete, select

WAVEFORM_PEAKS_CACHE_VERSION = 2

from .models import (
    AudioVariant,
    AudioVariantCreate,
    AudioVariantRunRequest,
    AudioVariantView,
    ActiveVariantUpdate,
    ClipLabCapabilitiesView,
    ClipLabCommitView,
    ClipLabItemView,
    ClipLabTranscriptView,
    ClipLabVariantView,
    EditCommit,
    ExportRun,
    ExportPreview,
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
    SourceAlignmentRequest,
    SourceRecordingArtifact,
    SourceRecordingArtifactView,
    SourceRecordingQueueView,
    SourceSlicingRequest,
    SourceTranscriptionRequest,
    SourceRecordingView,
    TagPayload,
    TagView,
    SourceRecording,
    SourceRecordingCreate,
    RecordingDerivativeCreate,
    Tag,
    TranscriptSummaryView,
    TranscriptView,
    Transcript,
    WaveformPeaks,
    utc_now,
)
from .slicer_algo import SlicerConfig, plan_slices

DEFAULT_PROCESSING_JOB_STALE_AFTER_SECONDS = 60.0
LOCKED_SLICE_OVERLAP_RATIO_THRESHOLD = 0.10
LOCKED_SLICE_DRIFT_WARNING_SECONDS = 0.08

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
        self._migrate_processingjob_schema()
        self._purge_legacy_review_window_schema()
        self._migrate_slice_schema()
        self._migrate_source_recording_artifact_rows()
        self._seed_if_needed()
        self._run_data_migrations()

    def close(self) -> None:
        self.engine.dispose()

    def __del__(self) -> None:  # pragma: no cover - best effort cleanup
        try:
            self.close()
        except Exception:
            pass

    def list_projects(self) -> list[ProjectSummary]:
        with self._session() as session:
            batches = session.exec(select(ImportBatch)).all()
            summaries = [self._project_summary(session, batch) for batch in batches]
            return sorted(summaries, key=lambda item: item.updated_at, reverse=True)

    def get_project(self, project_id: str) -> ProjectSummary:
        with self._session() as session:
            return self._project_summary(session, self._get_batch(session, project_id))

    def get_project_slices(self, project_id: str) -> list[SliceSummary]:
        with self._session() as session:
            self._get_batch(session, project_id)
            return [self._to_slice_summary(slice_row) for slice_row in self._get_batch_slice_summaries(session, project_id)]

    def get_slice_detail(self, slice_id: str) -> SliceDetail:
        with self._session() as session:
            return self._get_slice_detail(session, slice_id)

    def get_slice_clip_lab_item(self, slice_id: str) -> ClipLabItemView:
        with self._session() as session:
            return self._to_clip_lab_item_from_slice(self._get_loaded_slice(session, slice_id))

    def get_processing_job(self, job_id: str) -> ProcessingJobView:
        with self._session() as session:
            return self._processing_job_view(self._get_processing_job(session, job_id))

    def get_source_recording_artifact(self, recording_id: str) -> SourceRecordingArtifactView:
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            artifact = self._ensure_source_recording_artifact(session, recording)
            return self._source_recording_artifact_view(artifact)  # type: ignore[return-value]

    def list_project_recordings(self, project_id: str) -> list[SourceRecordingQueueView]:
        with self._session() as session:
            self._get_batch(session, project_id)
            recordings = session.exec(
                select(SourceRecording)
                .where(SourceRecording.batch_id == project_id)
                .options(
                    selectinload(SourceRecording.source_artifact),
                    selectinload(SourceRecording.processing_jobs),
                )
                .order_by(SourceRecording.id.asc())
            ).all()
            if not recordings:
                return []

            slice_counts = dict(
                session.exec(
                    select(Slice.source_recording_id, func.count(Slice.id))
                    .where(Slice.source_recording_id.in_([recording.id for recording in recordings]))
                    .group_by(Slice.source_recording_id)
                ).all()
            )
            return [
                self._source_recording_queue_view(
                    recording,
                    slice_count=int(slice_counts.get(recording.id, 0) or 0),
                )
                for recording in recordings
            ]

    def set_source_recording_artifact_paths(
        self,
        recording_id: str,
        *,
        transcript_text_path: str | None = None,
        transcript_json_path: str | None = None,
        alignment_json_path: str | None = None,
        transcript_status: str | None = None,
        alignment_status: str | None = None,
        transcript_word_count: int | None = None,
        alignment_word_count: int | None = None,
        alignment_backend: str | None = None,
        artifact_metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> SourceRecordingArtifactView:
        timestamp = self._as_utc(now) if now is not None else utc_now()
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            artifact = self._ensure_source_recording_artifact(session, recording)
            if transcript_text_path is not None:
                artifact.transcript_text_path = transcript_text_path
                artifact.transcript_updated_at = timestamp
            if transcript_json_path is not None:
                artifact.transcript_json_path = transcript_json_path
                artifact.transcript_updated_at = timestamp
            if alignment_json_path is not None:
                artifact.alignment_json_path = alignment_json_path
                artifact.aligned_at = timestamp
            if transcript_status is not None:
                artifact.transcript_status = transcript_status
            if alignment_status is not None:
                artifact.alignment_status = alignment_status
            if transcript_word_count is not None:
                artifact.transcript_word_count = transcript_word_count
            if alignment_word_count is not None:
                artifact.alignment_word_count = alignment_word_count
            if alignment_backend is not None:
                artifact.alignment_backend = alignment_backend
            if artifact_metadata is not None:
                artifact.artifact_metadata = artifact_metadata
            session.add(artifact)
            session.commit()
            session.refresh(artifact)
            return self._source_recording_artifact_view(artifact)  # type: ignore[return-value]

    def enqueue_source_transcription(
        self,
        recording_id: str,
        payload: SourceTranscriptionRequest,
    ) -> ProcessingJobView:
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            job = ProcessingJob(
                id=self._new_id("job"),
                kind=JobKind.SOURCE_TRANSCRIPTION,
                status=JobStatus.PENDING,
                source_recording_id=recording.id,
                input_payload=payload.model_dump(mode="json"),
            )
            session.add(job)
            session.commit()
            session.refresh(job)
            return self._processing_job_view(job)

    def enqueue_source_alignment(
        self,
        recording_id: str,
        payload: SourceAlignmentRequest,
    ) -> ProcessingJobView:
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            job = ProcessingJob(
                id=self._new_id("job"),
                kind=JobKind.SOURCE_ALIGNMENT,
                status=JobStatus.PENDING,
                source_recording_id=recording.id,
                input_payload=payload.model_dump(mode="json"),
            )
            session.add(job)
            session.commit()
            session.refresh(job)
            return self._processing_job_view(job)

    def enqueue_source_slicing(
        self,
        recording_id: str,
        payload: SourceSlicingRequest,
    ) -> ProcessingJobView:
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            job = ProcessingJob(
                id=self._new_id("job"),
                kind=JobKind.SOURCE_SLICING,
                status=JobStatus.PENDING,
                source_recording_id=recording.id,
                input_payload=payload.model_dump(mode="json"),
            )
            session.add(job)
            session.commit()
            session.refresh(job)
            return self._processing_job_view(job)

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
                self._patch_source_transcript_for_slice(
                    session,
                    slice_row,
                    payload.modified_text,
                    remove_patch=not transcript.is_modified,
                )

            if payload.tags is not None:
                self._replace_slice_tags(session, slice_row, payload.tags)

            if payload.status is not None:
                slice_row.status = payload.status

            if state_changed:
                slice_row.is_locked = True

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
            slice_row.is_locked = True
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
            return self._materialize_source_window_media_path(recording, start_seconds, end_seconds)

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
            session.flush()
            self._ensure_source_recording_artifact(session, recording)
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
            session.flush()
            self._ensure_source_recording_artifact(session, recording)
            session.commit()
            session.refresh(recording)
            return recording

    def _ensure_source_recording_artifact(
        self,
        session: Session,
        recording: SourceRecording,
    ) -> SourceRecordingArtifact:
        artifact = session.get(SourceRecordingArtifact, recording.id)
        if artifact is not None:
            return artifact
        artifact = SourceRecordingArtifact(
            source_recording_id=recording.id,
            transcript_status="missing",
            alignment_status="missing",
            artifact_metadata={},
        )
        session.add(artifact)
        session.flush()
        return artifact

    def _source_recording_transcript_base_path(self, recording_id: str) -> Path:
        return self._source_recording_artifact_dir(recording_id) / "transcript.base.txt"

    def _source_recording_transcript_effective_path(self, recording_id: str) -> Path:
        return self._source_recording_artifact_dir(recording_id) / "transcript.txt"

    def _source_recording_alignment_path(self, recording_id: str) -> Path:
        return self._source_recording_artifact_dir(recording_id) / "alignment.json"

    def _source_recording_alignment_summary_path(self, recording_id: str) -> Path:
        return self._source_recording_artifact_dir(recording_id) / "alignment.summary.json"

    def _source_artifact_metadata(self, artifact: SourceRecordingArtifact) -> dict[str, Any]:
        return dict(artifact.artifact_metadata or {})

    def _source_artifact_patches(self, artifact: SourceRecordingArtifact) -> list[dict[str, Any]]:
        metadata = self._source_artifact_metadata(artifact)
        raw_patches = metadata.get("transcript_patches")
        if not isinstance(raw_patches, list):
            return []
        normalized: list[dict[str, Any]] = []
        for patch in raw_patches:
            if not isinstance(patch, dict):
                continue
            try:
                start_index = int(patch["start_word_index"])
                end_index = int(patch["end_word_index"])
            except (KeyError, TypeError, ValueError):
                continue
            normalized.append(
                {
                    "slice_id": str(patch.get("slice_id") or ""),
                    "start_word_index": start_index,
                    "end_word_index": end_index,
                    "text": str(patch.get("text") or ""),
                    "updated_at": str(patch.get("updated_at") or ""),
                }
            )
        normalized.sort(key=lambda item: (item["start_word_index"], item["end_word_index"], item["slice_id"]))
        return normalized

    def _render_effective_source_transcript(
        self,
        artifact: SourceRecordingArtifact,
    ) -> tuple[str, int]:
        metadata = self._source_artifact_metadata(artifact)
        base_path_raw = metadata.get("base_transcript_text_path") or artifact.transcript_text_path
        if not base_path_raw:
            raise ValueError("Source transcript artifact is missing a base transcript path")
        base_path = Path(str(base_path_raw)).expanduser().resolve()
        if not base_path.exists():
            raise ValueError(f"Source transcript base file not found: {base_path}")

        base_tokens = base_path.read_text(encoding="utf-8").split()
        patches = self._source_artifact_patches(artifact)
        if not patches:
            rendered = " ".join(base_tokens).strip()
            return rendered, len(base_tokens)

        output_tokens: list[str] = []
        cursor = 0
        for patch in patches:
            start_index = max(0, min(int(patch["start_word_index"]), len(base_tokens)))
            end_index = max(start_index, min(int(patch["end_word_index"]), len(base_tokens) - 1))
            if start_index < cursor:
                continue
            output_tokens.extend(base_tokens[cursor:start_index])
            replacement_tokens = str(patch.get("text") or "").split()
            output_tokens.extend(replacement_tokens)
            cursor = end_index + 1
        output_tokens.extend(base_tokens[cursor:])
        rendered = " ".join(token for token in output_tokens if token).strip()
        return rendered, len(output_tokens)

    def _persist_effective_source_transcript(
        self,
        artifact: SourceRecordingArtifact,
    ) -> tuple[str, int]:
        rendered_text, token_count = self._render_effective_source_transcript(artifact)
        target_path = self._source_recording_transcript_effective_path(artifact.source_recording_id)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text((rendered_text + "\n") if rendered_text else "", encoding="utf-8")
        artifact.transcript_text_path = str(target_path)
        artifact.transcript_word_count = token_count
        artifact.transcript_updated_at = utc_now()
        return rendered_text, token_count

    def _patch_source_transcript_for_slice(
        self,
        session: Session,
        slice_row: Slice,
        modified_text: str,
        *,
        remove_patch: bool = False,
    ) -> None:
        metadata = self._slice_metadata(slice_row)
        try:
            start_index = int(metadata["source_word_start_index"])
            end_index = int(metadata["source_word_end_index"])
        except (KeyError, TypeError, ValueError):
            return

        recording = slice_row.source_recording or self._get_source_recording(session, slice_row.source_recording_id)
        artifact = self._ensure_source_recording_artifact(session, recording)
        artifact_metadata = self._source_artifact_metadata(artifact)
        if not artifact_metadata.get("base_transcript_text_path") and artifact.transcript_text_path:
            current_path = Path(str(artifact.transcript_text_path)).expanduser().resolve()
            if current_path.exists():
                base_path = self._source_recording_transcript_base_path(recording.id)
                base_path.parent.mkdir(parents=True, exist_ok=True)
                if current_path != base_path:
                    shutil.copyfile(current_path, base_path)
                artifact_metadata["base_transcript_text_path"] = str(base_path)
        patches = [
            patch
            for patch in self._source_artifact_patches(artifact)
            if str(patch.get("slice_id") or "") != slice_row.id
        ]
        if not remove_patch:
            patches.append(
                {
                    "slice_id": slice_row.id,
                    "start_word_index": start_index,
                    "end_word_index": end_index,
                    "text": modified_text,
                    "updated_at": utc_now().isoformat(),
                }
            )
        patches.sort(key=lambda item: (item["start_word_index"], item["end_word_index"], item["slice_id"]))
        artifact_metadata["transcript_patches"] = patches
        artifact_metadata["last_transcript_patch_slice_id"] = slice_row.id
        artifact.artifact_metadata = artifact_metadata
        self._persist_effective_source_transcript(artifact)
        artifact.transcript_status = "patched"
        artifact.alignment_status = "stale"
        session.add(artifact)

    def _active_recording_slices(
        self,
        session: Session,
        recording_id: str,
    ) -> list[Slice]:
        slices = session.exec(
            select(Slice)
            .where(Slice.source_recording_id == recording_id)
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
            [slice_row for slice_row in slices if not self._slice_metadata(slice_row).get("is_superseded", False)],
            key=lambda slice_row: (
                int(self._slice_metadata(slice_row).get("order_index", 0)),
                self._as_utc(slice_row.created_at),
            ),
        )

    def fail_stale_processing_jobs(
        self,
        stale_after_seconds: float = DEFAULT_PROCESSING_JOB_STALE_AFTER_SECONDS,
        *,
        now: datetime | None = None,
    ) -> list[str]:
        if stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        now = self._as_utc(now) if now is not None else utc_now()
        stale_ids: list[str] = []
        with self._session() as session:
            running_jobs = session.exec(
                select(ProcessingJob)
                .where(ProcessingJob.status == JobStatus.RUNNING)
                .order_by(ProcessingJob.started_at, ProcessingJob.id)
            ).all()
            for job in running_jobs:
                reference_time = self._processing_job_reference_time(job)
                if reference_time is None:
                    continue
                age_seconds = (now - reference_time).total_seconds()
                if age_seconds < stale_after_seconds:
                    continue
                job.status = JobStatus.FAILED
                job.error_message = (
                    f"Worker heartbeat timed out after {stale_after_seconds:.1f}s; "
                    "marking RUNNING job as failed"
                )
                job.completed_at = now
                session.add(job)
                stale_ids.append(job.id)
            if stale_ids:
                session.commit()
        return stale_ids

    def claim_next_processing_job(
        self,
        worker_id: str,
        *,
        stale_after_seconds: float = DEFAULT_PROCESSING_JOB_STALE_AFTER_SECONDS,
        now: datetime | None = None,
    ) -> ProcessingJobView | None:
        now = self._as_utc(now) if now is not None else utc_now()
        self.fail_stale_processing_jobs(stale_after_seconds=stale_after_seconds, now=now)
        with self._session() as session:
            pending_job_ids = session.exec(
                select(ProcessingJob.id)
                .where(ProcessingJob.status == JobStatus.PENDING)
                .order_by(ProcessingJob.created_at, ProcessingJob.id)
            ).all()
            for job_id in pending_job_ids:
                result = session.exec(
                    sql_update(ProcessingJob)
                    .where(
                        ProcessingJob.id == job_id,
                        ProcessingJob.status == JobStatus.PENDING,
                    )
                    .values(
                        status=JobStatus.RUNNING,
                        claimed_by=worker_id,
                        started_at=now,
                        heartbeat_at=now,
                        completed_at=None,
                        error_message=None,
                    )
                )
                if result.rowcount:
                    session.commit()
                    claimed_job = self._get_processing_job(session, job_id)
                    return self._processing_job_view(claimed_job)
            return None

    def heartbeat_processing_job(
        self,
        job_id: str,
        *,
        worker_id: str | None = None,
        now: datetime | None = None,
    ) -> ProcessingJobView:
        now = self._as_utc(now) if now is not None else utc_now()
        with self._session() as session:
            job = self._get_processing_job(session, job_id)
            if job.status != JobStatus.RUNNING:
                raise ValueError(f"Processing job {job_id} is not in RUNNING state")
            if worker_id is not None and job.claimed_by not in {None, worker_id}:
                raise ValueError(f"Processing job {job_id} is claimed by {job.claimed_by}, not {worker_id}")
            job.heartbeat_at = now
            session.add(job)
            session.commit()
            session.refresh(job)
            return self._processing_job_view(job)

    def run_claimed_processing_job(self, job_id: str, worker_id: str | None = None) -> ProcessingJobView:
        try:
            self._validate_claimed_processing_job(job_id, worker_id=worker_id)
            output_payload = self._dispatch_processing_job(job_id)
            return self._complete_processing_job(job_id, output_payload=output_payload)
        except Exception as exc:
            return self._fail_processing_job(job_id, str(exc))

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

    def _validate_claimed_processing_job(self, job_id: str, worker_id: str | None = None) -> ProcessingJob:
        with self._session() as session:
            job = self._get_processing_job(session, job_id)
            if job.status != JobStatus.RUNNING:
                raise ValueError(f"Processing job {job_id} is not in RUNNING state")
            if worker_id is not None and job.claimed_by not in {None, worker_id}:
                raise ValueError(f"Processing job {job_id} is claimed by {job.claimed_by}, not {worker_id}")
            return job

    def _complete_processing_job(
        self,
        job_id: str,
        *,
        output_payload: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> ProcessingJobView:
        now = self._as_utc(now) if now is not None else utc_now()
        with self._session() as session:
            job = self._get_processing_job(session, job_id)
            if job.status != JobStatus.RUNNING:
                raise ValueError(f"Processing job {job_id} is not in RUNNING state")
            job.status = JobStatus.COMPLETED
            job.error_message = None
            if output_payload is not None:
                job.output_payload = output_payload
            job.heartbeat_at = now
            job.completed_at = now
            session.add(job)
            session.commit()
            session.refresh(job)
            return self._processing_job_view(job)

    def _fail_processing_job(
        self,
        job_id: str,
        error_message: str,
        *,
        now: datetime | None = None,
    ) -> ProcessingJobView:
        now = self._as_utc(now) if now is not None else utc_now()
        with self._session() as session:
            job = self._get_processing_job(session, job_id)
            job.status = JobStatus.FAILED
            job.error_message = error_message
            job.heartbeat_at = now
            job.completed_at = now
            session.add(job)
            session.commit()
            session.refresh(job)
            return self._processing_job_view(job)

    def _processing_job_reference_time(self, job: ProcessingJob) -> datetime | None:
        reference = job.heartbeat_at or job.started_at or job.created_at
        if reference is None:
            return None
        return self._as_utc(reference)

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

    def _migrate_processingjob_schema(self) -> None:
        with self.engine.begin() as connection:
            columns = {
                row[1]
                for row in connection.exec_driver_sql("PRAGMA table_info(processingjob)").fetchall()
            }
            if not columns:
                return
            if "claimed_by" not in columns:
                connection.exec_driver_sql("ALTER TABLE processingjob ADD COLUMN claimed_by TEXT")
            if "heartbeat_at" not in columns:
                connection.exec_driver_sql("ALTER TABLE processingjob ADD COLUMN heartbeat_at TIMESTAMP")

    def _purge_legacy_review_window_schema(self) -> None:
        with self.engine.begin() as connection:
            table_names = {
                row[0]
                for row in connection.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "processingjob" in table_names:
                connection.exec_driver_sql(
                    "DELETE FROM processingjob WHERE kind IN ('review_window_asr', 'forced_align_and_pack')"
                )
                columns = [
                    row[1]
                    for row in connection.exec_driver_sql("PRAGMA table_info(processingjob)").fetchall()
                ]
                if "dataset_processing_run_id" in columns or "target_review_window_id" in columns:
                    connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
                    connection.exec_driver_sql(
                        """
                        CREATE TABLE processingjob_new (
                            id TEXT PRIMARY KEY NOT NULL,
                            kind VARCHAR(20),
                            status VARCHAR(20),
                            source_recording_id TEXT,
                            input_payload JSON,
                            output_payload JSON,
                            error_message TEXT,
                            claimed_by TEXT,
                            created_at TIMESTAMP NOT NULL,
                            started_at TIMESTAMP,
                            heartbeat_at TIMESTAMP,
                            completed_at TIMESTAMP,
                            FOREIGN KEY(source_recording_id) REFERENCES sourcerecording (id)
                        )
                        """
                    )
                    connection.exec_driver_sql(
                        """
                        INSERT INTO processingjob_new (
                            id, kind, status, source_recording_id, input_payload, output_payload,
                            error_message, claimed_by, created_at, started_at, heartbeat_at, completed_at
                        )
                        SELECT
                            id, kind, status, source_recording_id, input_payload, output_payload,
                            error_message, claimed_by, created_at, started_at, heartbeat_at, completed_at
                        FROM processingjob
                        """
                    )
                    connection.exec_driver_sql("DROP TABLE processingjob")
                    connection.exec_driver_sql("ALTER TABLE processingjob_new RENAME TO processingjob")
                    connection.exec_driver_sql(
                        "CREATE INDEX IF NOT EXISTS ix_processingjob_kind ON processingjob (kind)"
                    )
                    connection.exec_driver_sql(
                        "CREATE INDEX IF NOT EXISTS ix_processingjob_status ON processingjob (status)"
                    )
                    connection.exec_driver_sql("PRAGMA foreign_keys=ON")
            if "datasetprocessingrun" in table_names:
                connection.exec_driver_sql("DROP TABLE datasetprocessingrun")
            if "reviewwindowrevision" in table_names:
                connection.exec_driver_sql("DROP TABLE reviewwindowrevision")
            if "reviewwindowvariant" in table_names:
                connection.exec_driver_sql("DROP TABLE reviewwindowvariant")
            if "reviewwindow" in table_names:
                connection.exec_driver_sql("DROP TABLE reviewwindow")

    def _migrate_slice_schema(self) -> None:
        with self.engine.begin() as connection:
            columns = {
                row[1]
                for row in connection.exec_driver_sql("PRAGMA table_info(slice)").fetchall()
            }
            if not columns:
                return
            if "is_locked" not in columns:
                connection.exec_driver_sql(
                    "ALTER TABLE slice ADD COLUMN is_locked INTEGER NOT NULL DEFAULT 0"
                )

    def _migrate_source_recording_artifact_rows(self) -> None:
        with self.engine.begin() as connection:
            table_names = {
                row[0]
                for row in connection.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            if "sourcerecordingartifact" not in table_names:
                return
            recording_ids = [
                row[0]
                for row in connection.exec_driver_sql("SELECT id FROM sourcerecording").fetchall()
            ]
            existing_artifact_ids = {
                row[0]
                for row in connection.exec_driver_sql(
                    "SELECT source_recording_id FROM sourcerecordingartifact"
                ).fetchall()
            }
            for recording_id in recording_ids:
                if recording_id in existing_artifact_ids:
                    continue
                connection.exec_driver_sql(
                    """
                    INSERT INTO sourcerecordingartifact (
                        source_recording_id,
                        transcript_status,
                        alignment_status,
                        transcript_word_count,
                        alignment_word_count,
                        artifact_metadata
                    ) VALUES (?, 'missing', 'missing', 0, 0, '{}')
                    """,
                    (recording_id,),
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

    def _validate_source_window_bounds(
        self,
        recording: SourceRecording,
        start_seconds: float,
        end_seconds: float,
    ) -> None:
        if start_seconds < 0:
            raise ValueError("Source span start must be non-negative")
        if end_seconds <= start_seconds:
            raise ValueError("Source span end must be greater than the start")
        if end_seconds > recording.duration_s:
            raise ValueError("Source span end exceeds the source recording duration")

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
        extra_metadata: dict[str, Any] | None = None,
        created_at: datetime | None = None,
    ) -> Slice:
        self._validate_source_window_bounds(recording, start_seconds, end_seconds)
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
                **dict(extra_metadata or {}),
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

    def _clip_lab_transcript_from_transcript(self, transcript: Transcript | None) -> ClipLabTranscriptView | None:
        if transcript is None:
            return None
        return ClipLabTranscriptView(
            id=transcript.id,
            original_text=transcript.original_text,
            modified_text=transcript.modified_text,
            is_modified=transcript.is_modified,
            draft_text=None,
            draft_source=None,
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

    def _source_recording_artifact_view(
        self,
        artifact: SourceRecordingArtifact | None,
    ) -> SourceRecordingArtifactView | None:
        if artifact is None:
            return None
        return SourceRecordingArtifactView(
            source_recording_id=artifact.source_recording_id,
            transcript_text_path=artifact.transcript_text_path,
            transcript_json_path=artifact.transcript_json_path,
            alignment_json_path=artifact.alignment_json_path,
            transcript_status=artifact.transcript_status,
            alignment_status=artifact.alignment_status,
            transcript_word_count=artifact.transcript_word_count,
            alignment_word_count=artifact.alignment_word_count,
            transcript_updated_at=self._as_utc(artifact.transcript_updated_at)
            if artifact.transcript_updated_at is not None
            else None,
            aligned_at=self._as_utc(artifact.aligned_at) if artifact.aligned_at is not None else None,
            alignment_backend=artifact.alignment_backend,
            artifact_metadata=artifact.artifact_metadata,
        )

    def _source_recording_queue_view(
        self,
        recording: SourceRecording,
        *,
        slice_count: int,
    ) -> SourceRecordingQueueView:
        active_job = self._select_active_source_recording_job(recording.processing_jobs)
        latest_source_job = self._select_latest_source_recording_job(recording.processing_jobs)
        processing_state, processing_message = self._derive_source_recording_processing_state(
            recording.source_artifact,
            active_job=active_job,
            latest_source_job=latest_source_job,
            slice_count=slice_count,
        )
        return SourceRecordingQueueView(
            id=recording.id,
            batch_id=recording.batch_id,
            parent_recording_id=recording.parent_recording_id,
            sample_rate=recording.sample_rate,
            num_channels=recording.num_channels,
            num_samples=recording.num_samples,
            processing_recipe=recording.processing_recipe,
            duration_seconds=recording.duration_s,
            slice_count=slice_count,
            processing_state=processing_state,
            processing_message=processing_message,
            active_job=self._processing_job_view(active_job) if active_job is not None else None,
            artifact=self._source_recording_artifact_view(recording.source_artifact),
        )

    def _select_active_source_recording_job(
        self,
        jobs: list[ProcessingJob],
    ) -> ProcessingJob | None:
        source_jobs = [
            job
            for job in jobs
            if job.kind in {JobKind.SOURCE_TRANSCRIPTION, JobKind.SOURCE_ALIGNMENT, JobKind.SOURCE_SLICING}
            and job.status in {JobStatus.PENDING, JobStatus.RUNNING}
        ]
        source_jobs.sort(key=lambda job: (self._as_utc(job.created_at), job.id), reverse=True)
        return source_jobs[0] if source_jobs else None

    def _select_latest_source_recording_job(
        self,
        jobs: list[ProcessingJob],
    ) -> ProcessingJob | None:
        source_jobs = [
            job
            for job in jobs
            if job.kind in {JobKind.SOURCE_TRANSCRIPTION, JobKind.SOURCE_ALIGNMENT, JobKind.SOURCE_SLICING}
        ]
        source_jobs.sort(key=lambda job: (self._as_utc(job.created_at), job.id), reverse=True)
        return source_jobs[0] if source_jobs else None

    def _derive_source_recording_processing_state(
        self,
        artifact: SourceRecordingArtifact | None,
        *,
        active_job: ProcessingJob | None,
        latest_source_job: ProcessingJob | None,
        slice_count: int,
    ) -> tuple[str, str | None]:
        if active_job is not None:
            if active_job.kind == JobKind.SOURCE_TRANSCRIPTION:
                return ("transcribing", "Transcribing audio...")
            if active_job.kind == JobKind.SOURCE_ALIGNMENT:
                return ("aligning", "Aligning transcript...")
            if active_job.kind == JobKind.SOURCE_SLICING:
                return ("slicing", "Generating slices...")
        if latest_source_job is not None and latest_source_job.status == JobStatus.FAILED:
            return (
                "failed",
                latest_source_job.error_message
                or f"{latest_source_job.kind.value.replace('_', ' ').title()} failed.",
            )
        if artifact is not None and artifact.alignment_status == "stale":
            return ("alignment_stale", "Transcript changed. Re-run alignment before reslicing.")
        if slice_count > 0:
            return ("sliced", f"{slice_count} slice{'s' if slice_count != 1 else ''} ready for review.")
        if artifact is not None and artifact.alignment_status == "ok":
            return ("aligned", "Alignment is ready. Run slicing to generate review clips.")
        if artifact is not None and artifact.transcript_status in {"ok", "patched"}:
            return ("transcribed", "Transcript is ready. Run alignment next.")
        return ("idle", "Recording is ready for transcription.")

    def _processing_job_view(self, job: ProcessingJob) -> ProcessingJobView:
        return ProcessingJobView(
            id=job.id,
            kind=job.kind,
            status=job.status,
            source_recording_id=job.source_recording_id,
            input_payload=job.input_payload,
            output_payload=job.output_payload,
            error_message=job.error_message,
            claimed_by=job.claimed_by,
            created_at=self._as_utc(job.created_at),
            started_at=self._as_utc(job.started_at) if job.started_at is not None else None,
            heartbeat_at=self._as_utc(job.heartbeat_at) if job.heartbeat_at is not None else None,
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
                "is_locked": slice_row.is_locked,
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

    def _slice_clip_lab_capabilities(self) -> ClipLabCapabilitiesView:
        return ClipLabCapabilitiesView(
            can_edit_transcript=True,
            can_edit_tags=True,
            can_set_status=True,
            can_save=True,
            can_split=False,
            can_merge=False,
            can_edit_waveform=True,
            can_run_processing=True,
            can_switch_variants=True,
            can_export=False,
            can_finalize=False,
        )

    def _slice_audio_url(self, slice_row: Slice) -> str:
        active_commit_id = slice_row.active_commit_id or "base"
        active_variant_id = slice_row.active_variant_id or "source"
        return f"/media/slices/{slice_row.id}.wav?rev={active_variant_id}:{active_commit_id}"

    def _to_clip_lab_item_from_slice(self, slice_row: Slice) -> ClipLabItemView:
        detail = self._to_slice_detail(slice_row)
        return ClipLabItemView(
            id=detail.id,
            kind="slice",
            source_recording_id=detail.source_recording_id,
            source_recording=detail.source_recording,
            start_seconds=float((detail.model_metadata or {}).get("original_start_time", 0.0)),
            end_seconds=float((detail.model_metadata or {}).get("original_end_time", detail.duration_seconds)),
            duration_seconds=detail.duration_seconds,
            status=detail.status,
            is_locked=slice_row.is_locked,
            created_at=detail.created_at,
            transcript=self._clip_lab_transcript_from_transcript(slice_row.transcript),
            tags=detail.tags,
            speaker_name=str((detail.model_metadata or {}).get("speaker_name", "speaker_a")),
            language=str((detail.model_metadata or {}).get("language", "en")),
            audio_url=self._slice_audio_url(slice_row),
            item_metadata=detail.model_metadata,
            transcript_source="manual" if detail.transcript and detail.transcript.is_modified else "slice_seed",
            can_run_asr=False,
            asr_placeholder_message="ASR execution is not wired in Clip Lab yet.",
            asr_draft_transcript=None,
            last_asr_job_id=None,
            last_asr_at=None,
            asr_model_name=None,
            asr_model_version=None,
            asr_language=None,
            active_variant_generator_model=detail.active_variant_generator_model,
            can_undo=detail.can_undo,
            can_redo=detail.can_redo,
            capabilities=self._slice_clip_lab_capabilities(),
            variants=detail.variants,
            commits=detail.commits,
            active_variant=detail.active_variant,
            active_commit=detail.active_commit,
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
            return [0.0] * bins
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
            peaks.append(round(max_abs / 32767.0, 4))
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
        self._validate_source_window_bounds(recording, start_seconds, end_seconds)
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
            f"{self._slice_audio_cache_identifier(slice_row)}-peaks-v{WAVEFORM_PEAKS_CACHE_VERSION}-bins-{bins}"
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

    def _source_window_cache_path(
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
        return self._managed_media_path("source-windows", f"{recording_id}-{identifier}")

    def _materialize_source_window_media_path(
        self,
        recording: SourceRecording,
        start_seconds: float,
        end_seconds: float,
    ) -> Path:
        target_path = self._source_window_cache_path(recording.id, start_seconds, end_seconds)
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
        legacy_roots = [
            self.media_root / "review-window-renders",
            self.media_root / "review-window-variants",
            self.media_root / "review-windows",
        ]
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
        for root in legacy_roots:
            if not root.exists():
                continue
            for path in root.glob("*.wav"):
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

    def _dispatch_processing_job(self, job_id: str) -> dict[str, Any] | None:
        with self._session() as session:
            job = self._get_processing_job(session, job_id)
            job_kind = job.kind
        if job_kind == JobKind.SOURCE_TRANSCRIPTION:
            return self._run_source_transcription_job(job_id)
        if job_kind == JobKind.SOURCE_ALIGNMENT:
            return self._run_source_alignment_job(job_id)
        if job_kind == JobKind.SOURCE_SLICING:
            return self._run_source_slicing_job(job_id)
        raise ValueError(f"Unsupported processing job kind: {job_kind}")

    def _run_source_transcription_job(self, job_id: str) -> dict[str, Any]:
        with self._session() as session:
            job = self._get_processing_job(session, job_id)
            if job.source_recording_id is None:
                raise ValueError("Source transcription job is missing its source recording")
            recording = self._get_source_recording(session, job.source_recording_id)
            payload = dict(job.input_payload or {})
            adapter_config = self._source_asr_backend_config(payload)
            language_hint = payload.get("language_hint")
            completed_at = utc_now()
            backend_client = self._create_source_asr_backend_client(adapter_config)
            draft_result = self._run_source_recording_asr_adapter(
                recording,
                adapter_config=adapter_config,
                backend_client=backend_client,
                language_hint=str(language_hint) if language_hint is not None else None,
            )

            artifact_dir = self._source_recording_artifact_dir(recording.id)
            artifact_dir.mkdir(parents=True, exist_ok=True)
            transcript_base_path = artifact_dir / "transcript.base.txt"
            transcript_text_path = artifact_dir / "transcript.txt"
            transcript_json_path = artifact_dir / "transcript.json"
            transcript_base_path.write_text(draft_result["transcript_text"] + "\n", encoding="utf-8")
            transcript_text_path.write_text(draft_result["transcript_text"] + "\n", encoding="utf-8")
            transcript_json_path.write_text(
                json.dumps(
                    {
                        "backend": draft_result["backend"],
                        "model_name": draft_result["model_name"],
                        "model_version": draft_result["model_version"],
                        "language": draft_result["language"],
                        "segments": list(draft_result["segments"]),
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            artifact = self._ensure_source_recording_artifact(session, recording)
            artifact.transcript_text_path = str(transcript_text_path)
            artifact.transcript_json_path = str(transcript_json_path)
            artifact.transcript_status = "ok"
            artifact.alignment_status = "stale" if artifact.alignment_json_path else artifact.alignment_status
            artifact.transcript_word_count = len(str(draft_result["transcript_text"]).split())
            artifact.transcript_updated_at = completed_at
            metadata = dict(artifact.artifact_metadata or {})
            metadata["base_transcript_text_path"] = str(transcript_base_path)
            metadata["transcript_patches"] = []
            metadata["last_transcription_job_id"] = job.id
            metadata["language"] = draft_result["language"]
            artifact.artifact_metadata = metadata
            session.add(artifact)
            session.commit()

        return {
            "target_kind": "source_recording",
            "backend": adapter_config["backend"],
            "source_recording_id": recording.id,
            "transcript_text": draft_result["transcript_text"],
            "transcript_text_path": str(transcript_text_path),
            "transcript_json_path": str(transcript_json_path),
            "transcript_word_count": len(str(draft_result["transcript_text"]).split()),
            "language": draft_result["language"],
            "stored_as": "source_recording_transcript_artifact",
        }

    def _run_source_alignment_job(self, job_id: str) -> dict[str, Any]:
        with self._session() as session:
            job = self._get_processing_job(session, job_id)
            if job.source_recording_id is None:
                raise ValueError("Source alignment job is missing its source recording")
            recording = self._get_source_recording(session, job.source_recording_id)
            artifact = self._ensure_source_recording_artifact(session, recording)
            payload = dict(job.input_payload or {})

            if payload.get("transcript_text_path"):
                artifact.transcript_text_path = str(payload["transcript_text_path"])
            if payload.get("transcript_json_path"):
                artifact.transcript_json_path = str(payload["transcript_json_path"])

            transcript_text_path = (
                Path(str(artifact.transcript_text_path)).expanduser().resolve()
                if artifact.transcript_text_path
                else None
            )
            transcript_json_path = (
                Path(str(artifact.transcript_json_path)).expanduser().resolve()
                if artifact.transcript_json_path
                else None
            )
            if transcript_text_path is None or not transcript_text_path.exists():
                raise ValueError("Source alignment requires a transcript_text_path artifact")

            transcript_json_payload: dict[str, Any] | None = None
            transcript_patches = self._source_artifact_patches(artifact)
            if transcript_json_path is not None and transcript_json_path.exists() and not transcript_patches:
                transcript_json_payload = json.loads(transcript_json_path.read_text(encoding="utf-8"))

            alignment_backend = str(
                payload.get("alignment_backend")
                or artifact.alignment_backend
                or "torchaudio_forced_align_worker"
            ).strip() or "torchaudio_forced_align_worker"

            if transcript_json_payload is not None:
                alignment_units, summary = self._run_segmented_source_alignment(recording, transcript_json_payload)
                alignment_mode = "segmented_transcript_json"
            else:
                transcript_text = transcript_text_path.read_text(encoding="utf-8").strip()
                if not transcript_text:
                    raise ValueError("Source alignment transcript artifact is empty")
                alignment_units = self._run_forced_align_worker_on_file(Path(recording.file_path), transcript_text)
                summary = {
                    "segment_count": 1,
                    "processed_segments": 1,
                    "skipped_segments": 0,
                }
                alignment_mode = "full_transcript_text"

            completed_at = utc_now()
            alignment_path = self._source_recording_alignment_path(recording.id)
            summary_path = self._source_recording_alignment_summary_path(recording.id)
            alignment_path.parent.mkdir(parents=True, exist_ok=True)
            alignment_path.write_text(json.dumps(alignment_units, indent=2), encoding="utf-8")
            summary_payload = {
                "source_recording_id": recording.id,
                "status": "ok",
                "alignment_backend": alignment_backend,
                "alignment_mode": alignment_mode,
                "alignment_word_count": len(alignment_units),
                **summary,
            }
            summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

            artifact.alignment_json_path = str(alignment_path)
            artifact.alignment_status = "ok"
            artifact.alignment_word_count = len(alignment_units)
            artifact.aligned_at = completed_at
            artifact.alignment_backend = alignment_backend
            metadata = self._source_artifact_metadata(artifact)
            metadata["alignment_summary_path"] = str(summary_path)
            metadata["last_alignment_job_id"] = job.id
            metadata["alignment_mode"] = alignment_mode
            artifact.artifact_metadata = metadata
            session.add(artifact)
            session.commit()

        return {
            "target_kind": "source_recording",
            "source_recording_id": recording.id,
            "alignment_json_path": str(alignment_path),
            "alignment_summary_path": str(summary_path),
            "alignment_word_count": len(alignment_units),
            "alignment_backend": alignment_backend,
            "alignment_mode": alignment_mode,
        }

    def _run_source_slicing_job(self, job_id: str) -> dict[str, Any]:
        with self._session() as session:
            job = self._get_processing_job(session, job_id)
            if job.source_recording_id is None:
                raise ValueError("Source slicing job is missing its source recording")
            recording = self._get_source_recording(session, job.source_recording_id)
            artifact = self._ensure_source_recording_artifact(session, recording)
            payload = dict(job.input_payload or {})
            if artifact.alignment_status != "ok":
                raise ValueError("Source slicing requires an up-to-date alignment artifact")
            if not artifact.alignment_json_path:
                raise ValueError("Source slicing requires an alignment_json_path artifact")
            alignment_path = Path(str(artifact.alignment_json_path)).expanduser().resolve()
            if not alignment_path.exists():
                raise ValueError(f"Source alignment artifact not found: {alignment_path}")
            alignment_units = json.loads(alignment_path.read_text(encoding="utf-8"))
            if not isinstance(alignment_units, list) or not alignment_units:
                raise ValueError("Source slicing requires non-empty alignment data")

            config_overrides = payload.get("config_overrides") or {}
            if not isinstance(config_overrides, dict):
                raise ValueError("config_overrides must be an object")
            config = SlicerConfig(**config_overrides)
            full_audio_bytes = self._extract_source_window_wav_bytes(recording, 0.0, recording.duration_s)
            audio_samples, sample_rate = self._wav_bytes_to_mono_samples(full_audio_bytes)
            slicer_result = plan_slices(alignment_units, audio_samples, sample_rate, config)
            generated_slices = list(slicer_result["slices"])

            active_slices = self._active_recording_slices(session, recording.id)
            locked_slices = [slice_row for slice_row in active_slices if slice_row.is_locked] if payload.get("preserve_locked_slices", True) else []
            replaced_slices = [slice_row for slice_row in active_slices if slice_row not in locked_slices]

            if payload.get("replace_unlocked_slices", True):
                for slice_row in replaced_slices:
                    metadata = self._slice_metadata(slice_row)
                    metadata["is_superseded"] = True
                    metadata["superseded_by_job_id"] = job.id
                    metadata["updated_at"] = utc_now().isoformat()
                    slice_row.model_metadata = metadata
                    session.add(slice_row)

            kept_locked_ids = {slice_row.id for slice_row in locked_slices}
            dropped_overlap_count = 0
            created_slice_rows: list[Slice] = []
            language = str(self._source_artifact_metadata(artifact).get("language") or "en")
            for slice_payload in generated_slices:
                if self._candidate_overlaps_locked_slice(slice_payload, locked_slices):
                    dropped_overlap_count += 1
                    continue
                created_slice_rows.append(
                    self._create_generated_source_slice(
                        session,
                        recording,
                        slice_payload=slice_payload,
                        source_alignment_backend=artifact.alignment_backend,
                        language=language,
                    )
                )

            final_active_slices = [*locked_slices, *created_slice_rows]
            self._reindex_recording_slice_block(
                session,
                recording=recording,
                previous_active_slices=active_slices,
                final_active_slices=final_active_slices,
            )
            for locked_slice in locked_slices:
                self._update_locked_slice_alignment_drift(locked_slice, alignment_units, job_id=job.id)
                session.add(locked_slice)
            session.commit()

        return {
            "target_kind": "source_recording",
            "source_recording_id": recording.id,
            "created_slice_count": len(created_slice_rows),
            "preserved_locked_slice_count": len(kept_locked_ids),
            "dropped_overlap_count": dropped_overlap_count,
            "replace_unlocked_slices": bool(payload.get("replace_unlocked_slices", True)),
            "preserve_locked_slices": bool(payload.get("preserve_locked_slices", True)),
            "slicer_stats": slicer_result["stats"],
        }

    def _run_segmented_source_alignment(
        self,
        recording: SourceRecording,
        transcript_json_payload: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        segments = transcript_json_payload.get("segments")
        if not isinstance(segments, list):
            raise ValueError("Transcript JSON missing segments array for segmented alignment")

        merged_alignment: list[dict[str, Any]] = []
        processed_segments = 0
        skipped_segments = 0
        for segment in segments:
            if not isinstance(segment, dict):
                skipped_segments += 1
                continue
            text = str(segment.get("text") or "").strip()
            start = max(0.0, float(segment.get("start") or 0.0))
            end = min(recording.duration_s, float(segment.get("end") or 0.0))
            if not text or end <= start:
                skipped_segments += 1
                continue
            chunk_bytes = self._extract_source_window_wav_bytes(recording, start, end)
            alignment = self._run_forced_align_worker(chunk_bytes, text)
            for word in alignment:
                merged_alignment.append(
                    {
                        "word": str(word["word"]),
                        "start": round(start + float(word["start"]), 6),
                        "end": round(start + float(word["end"]), 6),
                        **({"confidence": float(word["confidence"])} if word.get("confidence") is not None else {}),
                        **({"interpolated": True} if bool(word.get("interpolated")) else {}),
                    }
                )
            processed_segments += 1

        return merged_alignment, {
            "segment_count": len(segments),
            "processed_segments": processed_segments,
            "skipped_segments": skipped_segments,
        }

    def _create_generated_source_slice(
        self,
        session: Session,
        recording: SourceRecording,
        *,
        slice_payload: dict[str, Any],
        source_alignment_backend: str | None,
        language: str = "en",
    ) -> Slice:
        extra_metadata = {
            "generation_mode": "source_slicer_v2",
            "raw_start": float(slice_payload["raw_start"]),
            "raw_end": float(slice_payload["raw_end"]),
            "snapped_start": float(slice_payload["snapped_start"]),
            "snapped_end": float(slice_payload["snapped_end"]),
            "training_start": float(slice_payload["training_start"]),
            "training_end": float(slice_payload["training_end"]),
            "padded_start": float(slice_payload["padded_start"]),
            "padded_end": float(slice_payload["padded_end"]),
            "source_word_start_index": int(slice_payload["start_word_index"]),
            "source_word_end_index": int(slice_payload["end_word_index"]),
            "boundary_type": str(slice_payload["boundary_type"]),
            "boundary_gap_s": float(slice_payload["boundary_gap_s"]),
            "avg_alignment_confidence": float(slice_payload["avg_alignment_confidence"]),
            "relative_word_offsets_from": str(slice_payload["relative_word_offsets_from"]),
            "is_flagged": bool(slice_payload["is_flagged"]),
            "flag_reason": str(slice_payload["flag_reason"] or ""),
            "flag_reasons": list(slice_payload["flag_reasons"] or []),
            "breath_at_start": bool(slice_payload["breath_at_start"]),
            "breath_at_end": bool(slice_payload["breath_at_end"]),
            "edge_start_energy": float(slice_payload["edge_start_energy"]),
            "edge_end_energy": float(slice_payload["edge_end_energy"]),
            "forced_cut": bool(slice_payload["forced_cut"]),
            "source_alignment_backend": source_alignment_backend,
        }
        transcript_alignment_data = {
            "kind": "source_slicer_alignment",
            "relative_word_offsets_from": str(slice_payload["relative_word_offsets_from"]),
            "words": list(slice_payload["words"]),
            "source_word_start_index": int(slice_payload["start_word_index"]),
            "source_word_end_index": int(slice_payload["end_word_index"]),
            "raw_start": float(slice_payload["raw_start"]),
            "raw_end": float(slice_payload["raw_end"]),
            "snapped_start": float(slice_payload["snapped_start"]),
            "snapped_end": float(slice_payload["snapped_end"]),
            "training_start": float(slice_payload["training_start"]),
            "training_end": float(slice_payload["training_end"]),
            "padded_start": float(slice_payload["padded_start"]),
            "padded_end": float(slice_payload["padded_end"]),
            "flag_reasons": list(slice_payload["flag_reasons"] or []),
        }
        return self._create_slice_from_source_span(
            session,
            recording,
            slice_id=self._new_id("slice"),
            start_seconds=float(slice_payload["training_start"]),
            end_seconds=float(slice_payload["training_end"]),
            transcript_text=str(slice_payload["transcript"]),
            order_index=0,
            language=language,
            alignment_data=transcript_alignment_data,
            extra_metadata=extra_metadata,
        )

    def _slice_training_interval(self, slice_row: Slice) -> tuple[float, float]:
        metadata = self._slice_metadata(slice_row)
        start = float(metadata.get("training_start", metadata.get("original_start_time", 0.0)))
        end = float(metadata.get("training_end", metadata.get("original_end_time", start)))
        return start, end

    def _candidate_overlaps_locked_slice(
        self,
        candidate_slice: dict[str, Any],
        locked_slices: list[Slice],
    ) -> bool:
        candidate_start = float(candidate_slice["training_start"])
        candidate_end = float(candidate_slice["training_end"])
        candidate_duration = max(candidate_end - candidate_start, 1e-6)
        for locked_slice in locked_slices:
            locked_start, locked_end = self._slice_training_interval(locked_slice)
            overlap = max(0.0, min(candidate_end, locked_end) - max(candidate_start, locked_start))
            if overlap <= 0:
                continue
            locked_duration = max(locked_end - locked_start, 1e-6)
            overlap_ratio = overlap / min(candidate_duration, locked_duration)
            if overlap_ratio >= LOCKED_SLICE_OVERLAP_RATIO_THRESHOLD:
                return True
        return False

    def _reindex_recording_slice_block(
        self,
        session: Session,
        *,
        recording: SourceRecording,
        previous_active_slices: list[Slice],
        final_active_slices: list[Slice],
    ) -> None:
        batch_slices = self._get_batch_slices(session, recording.batch_id)
        previous_orders = [
            int(self._slice_metadata(slice_row).get("order_index", 0))
            for slice_row in previous_active_slices
        ]
        base_order = min(previous_orders) if previous_orders else (
            max((int(self._slice_metadata(slice_row).get("order_index", 0)) for slice_row in batch_slices), default=-1) + 1
        )
        old_count = len(previous_active_slices)
        new_count = len(final_active_slices)
        tail_start = base_order + old_count
        delta = new_count - old_count
        if delta != 0:
            for slice_row in batch_slices:
                if slice_row.source_recording_id == recording.id:
                    continue
                metadata = self._slice_metadata(slice_row)
                order_index = int(metadata.get("order_index", 0))
                if order_index < tail_start:
                    continue
                metadata["order_index"] = order_index + delta
                slice_row.model_metadata = metadata
                session.add(slice_row)

        final_active_slices.sort(key=lambda slice_row: self._slice_training_interval(slice_row))
        for index, slice_row in enumerate(final_active_slices):
            metadata = self._slice_metadata(slice_row)
            metadata["order_index"] = base_order + index
            slice_row.model_metadata = metadata
            session.add(slice_row)

    def _update_locked_slice_alignment_drift(
        self,
        slice_row: Slice,
        alignment_units: list[dict[str, Any]],
        *,
        job_id: str,
    ) -> None:
        metadata = self._slice_metadata(slice_row)
        try:
            start_index = int(metadata["source_word_start_index"])
            end_index = int(metadata["source_word_end_index"])
        except (KeyError, TypeError, ValueError):
            return
        if start_index < 0 or end_index >= len(alignment_units) or end_index < start_index:
            return
        old_start = float(metadata.get("raw_start", metadata.get("original_start_time", 0.0)))
        old_end = float(metadata.get("raw_end", metadata.get("original_end_time", old_start)))
        new_start = float(alignment_units[start_index]["start"])
        new_end = float(alignment_units[end_index]["end"])
        drift_seconds = max(abs(new_start - old_start), abs(new_end - old_end))
        if drift_seconds >= LOCKED_SLICE_DRIFT_WARNING_SECONDS:
            metadata["alignment_drift_warning"] = {
                "drift_seconds": round(drift_seconds, 4),
                "checked_by_job_id": job_id,
                "previous_raw_start": round(old_start, 4),
                "previous_raw_end": round(old_end, 4),
                "current_raw_start": round(new_start, 4),
                "current_raw_end": round(new_end, 4),
            }
        else:
            metadata.pop("alignment_drift_warning", None)
        slice_row.model_metadata = metadata

    def _run_source_recording_asr_adapter(
        self,
        recording: SourceRecording,
        *,
        adapter_config: dict[str, str],
        backend_client: Any = None,
        language_hint: str | None,
    ) -> dict[str, Any]:
        backend = adapter_config["backend"]
        if backend == "stub":
            return self._run_source_recording_asr_stub_adapter(
                recording,
                model_name=adapter_config["model_name"],
                model_version=adapter_config["model_version"],
                language_hint=language_hint,
            )
        if backend == "faster_whisper":
            return self._run_source_recording_asr_faster_whisper_adapter(
                recording,
                model=backend_client,
                model_name=adapter_config["model_name"],
                model_version=adapter_config["model_version"],
                language_hint=language_hint,
            )
        raise ValueError(f"Unsupported ASR backend: {backend}")

    def _create_source_asr_backend_client(self, adapter_config: dict[str, str]) -> Any:
        backend = adapter_config["backend"]
        if backend == "stub":
            return None
        if backend == "faster_whisper":
            return self._load_faster_whisper_model(
                model_path=adapter_config["model_path"],
                device=adapter_config["device"],
                compute_type=adapter_config["compute_type"],
            )
        raise ValueError(f"Unsupported ASR backend: {backend}")

    def _source_asr_backend_config(self, payload: dict[str, Any]) -> dict[str, str]:
        # Dev/local smoke path:
        # ASR_BACKEND=faster_whisper ASR_MODEL_PATH=/abs/path/to/local/model
        # ASR_DEVICE=cpu|cuda ASR_COMPUTE_TYPE=int8|float16 uv run --directory backend python -m app.worker --once
        backend = str(os.getenv("ASR_BACKEND", "stub")).strip().lower() or "stub"
        requested_model_name = str(payload.get("model_name") or "").strip()
        requested_model_version = str(payload.get("model_version") or "").strip()
        if backend == "stub":
            return {
                "backend": "stub",
                "model_name": requested_model_name or "stub-source-recording-asr",
                "model_version": requested_model_version or "stub-v1",
            }
        if backend == "faster_whisper":
            model_path = str(os.getenv("ASR_MODEL_PATH", "")).strip()
            if not model_path:
                raise ValueError("ASR_MODEL_PATH is required when ASR_BACKEND=faster_whisper")
            model_path_obj = Path(model_path)
            if not model_path_obj.exists():
                raise ValueError(f"ASR_MODEL_PATH does not exist: {model_path_obj}")
            return {
                "backend": "faster_whisper",
                "model_path": str(model_path_obj),
                "model_name": requested_model_name or model_path_obj.name,
                "model_version": requested_model_version or "local",
                "device": str(os.getenv("ASR_DEVICE", "cpu")).strip() or "cpu",
                "compute_type": str(os.getenv("ASR_COMPUTE_TYPE", "int8")).strip() or "int8",
            }
        raise ValueError(f"Unsupported ASR_BACKEND: {backend}")

    def _run_source_recording_asr_stub_adapter(
        self,
        recording: SourceRecording,
        *,
        model_name: str,
        model_version: str,
        language_hint: str | None,
    ) -> dict[str, Any]:
        transcript_text = f"stub asr source recording {recording.id}"
        language = language_hint or "en"
        return {
            "backend": "stub",
            "transcript_text": transcript_text,
            "model_name": model_name,
            "model_version": model_version,
            "language": language,
            "segments": [
                {
                    "start": 0.0,
                    "end": round(recording.duration_s, 6),
                    "text": transcript_text,
                }
            ],
        }

    def _run_source_recording_asr_faster_whisper_adapter(
        self,
        recording: SourceRecording,
        *,
        model: Any,
        model_name: str,
        model_version: str,
        language_hint: str | None,
    ) -> dict[str, Any]:
        segments_iter, info = model.transcribe(
            str(Path(recording.file_path)),
            language=language_hint or None,
            condition_on_previous_text=False,
        )
        segments = [
            {
                "start": round(float(segment.start), 6),
                "end": round(float(segment.end), 6),
                "text": str(segment.text).strip(),
            }
            for segment in segments_iter
        ]
        transcript_text = " ".join(
            segment["text"] for segment in segments if str(segment.get("text") or "").strip()
        ).strip()
        detected_language = str(getattr(info, "language", "") or language_hint or "")
        return {
            "backend": "faster_whisper",
            "transcript_text": transcript_text,
            "model_name": model_name,
            "model_version": model_version,
            "language": detected_language,
            "segments": segments,
        }

    def _source_recording_artifact_dir(self, recording_id: str) -> Path:
        return self.exports_root / "recording-artifacts" / recording_id

    def _load_faster_whisper_model(
        self,
        *,
        model_path: str,
        device: str,
        compute_type: str,
    ) -> Any:
        try:
            from faster_whisper import WhisperModel
        except ImportError as exc:  # pragma: no cover - depends on local dev env
            raise ValueError(
                "ASR_BACKEND=faster_whisper requires the faster-whisper package in the local environment"
            ) from exc
        return WhisperModel(model_path, device=device, compute_type=compute_type)

    def _run_forced_align_worker(
        self,
        audio_bytes: bytes,
        transcript_text: str,
    ) -> list[dict[str, Any]]:
        if not transcript_text.strip():
            raise ValueError("Forced align worker requires transcript_text")

        with tempfile.TemporaryDirectory(prefix="speechcraft-aligner-") as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            audio_path = temp_dir / "input.wav"
            audio_path.write_bytes(audio_bytes)
            return self._run_forced_align_worker_on_file(audio_path, transcript_text)

    def _run_forced_align_worker_on_file(
        self,
        audio_path: Path,
        transcript_text: str,
    ) -> list[dict[str, Any]]:
        if not transcript_text.strip():
            raise ValueError("Forced align worker requires transcript_text")

        worker_python, worker_script = self._resolve_forced_align_worker_paths()
        with tempfile.TemporaryDirectory(prefix="speechcraft-aligner-") as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            output_path = temp_dir / "alignment.json"
            transcript_path = temp_dir / "transcript.txt"
            transcript_path.write_text(transcript_text, encoding="utf-8")
            try:
                completed = subprocess.run(
                    [
                        str(worker_python),
                        str(worker_script),
                        "--audio",
                        str(audio_path),
                        "--text",
                        str(transcript_path),
                        "--output",
                        str(output_path),
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as exc:
                stderr = (exc.stderr or exc.stdout or "").strip()
                raise ValueError(stderr or "Forced align worker failed") from exc
            except OSError as exc:
                raise ValueError(f"Forced align worker failed to launch: {exc}") from exc

            if not output_path.exists():
                stdout = (completed.stdout or "").strip()
                stderr = (completed.stderr or "").strip()
                detail = stderr or stdout or "Forced align worker did not produce alignment output"
                raise ValueError(detail)

            alignment_payload = json.loads(output_path.read_text(encoding="utf-8"))

        if not isinstance(alignment_payload, list):
            raise ValueError("Forced align worker returned invalid alignment payload")
        return alignment_payload

    def _resolve_forced_align_worker_paths(self) -> tuple[Path, Path]:
        repo_root = Path(__file__).resolve().parents[2]
        worker_root = repo_root / "workers" / "aligner"
        worker_python = worker_root / ".venv" / "bin" / "python"
        worker_script = worker_root / "run_aligner.py"

        if not worker_python.exists():
            raise ValueError(f"Forced align worker python not found: {worker_python}")
        if not worker_script.exists():
            raise ValueError(f"Forced align worker script not found: {worker_script}")
        return worker_python, worker_script

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
