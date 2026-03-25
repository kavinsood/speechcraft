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
    DatasetProcessingRun,
    DatasetProcessingRunRequest,
    DatasetProcessingRunView,
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
    ReviewWindowAsrRequest,
    ReviewWindowRevision,
    ReviewWindowVariant,
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
from .slicer_core import pack_aligned_words

DEFAULT_PROCESSING_JOB_STALE_AFTER_SECONDS = 60.0

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
        self._migrate_dataset_processing_run_schema()
        self._migrate_reviewwindow_schema()
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

    def list_review_windows(self, recording_id: str) -> list[ReviewWindowView]:
        with self._session() as session:
            self._get_source_recording(session, recording_id)
            windows = session.exec(
                select(ReviewWindow)
                .where(ReviewWindow.source_recording_id == recording_id)
                .order_by(ReviewWindow.order_index, ReviewWindow.created_at)
            ).all()
            return [self._review_window_view(window, session) for window in windows]

    def list_project_review_windows(self, project_id: str) -> list[ReviewWindowView]:
        with self._session() as session:
            self._get_batch(session, project_id)
            windows = session.exec(
                select(ReviewWindow)
                .join(SourceRecording, SourceRecording.id == ReviewWindow.source_recording_id)
                .where(SourceRecording.batch_id == project_id)
                .order_by(SourceRecording.id, ReviewWindow.order_index, ReviewWindow.created_at)
            ).all()
            return [self._review_window_view(window, session) for window in windows]

    def get_project_slices(self, project_id: str) -> list[SliceSummary]:
        with self._session() as session:
            self._get_batch(session, project_id)
            return [self._to_slice_summary(slice_row) for slice_row in self._get_batch_slice_summaries(session, project_id)]

    def get_slice_detail(self, slice_id: str) -> SliceDetail:
        with self._session() as session:
            return self._get_slice_detail(session, slice_id)

    def get_clip_lab_item(self, item_kind: str, item_id: str) -> ClipLabItemView:
        with self._session() as session:
            if item_kind == "slice":
                return self._to_clip_lab_item_from_slice(self._get_loaded_slice(session, item_id))
            if item_kind == "review_window":
                window = self._get_review_window(session, item_id)
                recording = self._get_source_recording(session, window.source_recording_id)
                return self._to_clip_lab_item_from_review_window(window, recording, session)
            raise ValueError(f"Unsupported Clip Lab item kind: {item_kind}")

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

    def start_dataset_processing_run(
        self,
        recording_id: str,
        payload: DatasetProcessingRunRequest,
    ) -> DatasetProcessingRunView:
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            existing_active_run = session.exec(
                select(DatasetProcessingRun)
                .where(
                    DatasetProcessingRun.source_recording_id == recording.id,
                    DatasetProcessingRun.status.in_(["pending", "asr_running", "alignment_running"]),
                )
                .order_by(DatasetProcessingRun.started_at.desc())
            ).first()
            if existing_active_run is not None:
                raise ValueError("A dataset processing run is already active for this source recording")

            if payload.review_window_ids:
                ordered_windows = self._load_selected_review_windows_for_recording(
                    session,
                    recording,
                    payload.review_window_ids,
                    empty_selection_message="Dataset processing run requires at least one review window",
                )
            else:
                ordered_windows = self._list_recording_review_windows(session, recording.id)
                if not ordered_windows:
                    raise ValueError("Dataset processing run requires at least one review window")

            run = DatasetProcessingRun(
                id=self._new_id("dataset-run"),
                source_recording_id=recording.id,
                status="asr_running",
                phase="asr",
                total_review_windows=len(ordered_windows),
                alignment_total=0,
                current_message=f"ASR queued for {len(ordered_windows)} review windows",
            )
            session.add(run)
            session.flush()

            for window in ordered_windows:
                session.add(
                    ProcessingJob(
                        id=self._new_id("job"),
                        kind=JobKind.REVIEW_WINDOW_ASR,
                        status=JobStatus.PENDING,
                        source_recording_id=recording.id,
                        dataset_processing_run_id=run.id,
                        target_review_window_id=window.id,
                        input_payload={
                            "target_kind": "review_window",
                            "review_window_ids": [window.id],
                            "model_name": payload.model_name,
                            "model_version": payload.model_version,
                            "language_hint": payload.language_hint,
                        },
                    )
                )

            session.commit()
            session.refresh(run)
            return self._dataset_processing_run_view(run)

    def get_source_recording_processing_status(self, recording_id: str) -> DatasetProcessingRunView:
        with self._session() as session:
            self._get_source_recording(session, recording_id)
            run = session.exec(
                select(DatasetProcessingRun)
                .where(DatasetProcessingRun.source_recording_id == recording_id)
                .order_by(DatasetProcessingRun.started_at.desc(), DatasetProcessingRun.id.desc())
            ).first()
            if run is None:
                raise KeyError(recording_id)
            return self._dataset_processing_run_view(run)

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

    def save_review_window_state(self, review_window_id: str, payload: SliceSaveRequest) -> ClipLabItemView:
        with self._session() as session:
            window = self._get_review_window(session, review_window_id)
            recording = self._get_source_recording(session, window.source_recording_id)
            self._ensure_review_window_baseline(session, window, recording)
            active_revision = self._get_active_review_window_revision(session, window)
            current_transcript = active_revision.transcript_text
            current_tags = self._normalized_tag_payloads(
                [TagPayload.model_validate(tag) for tag in active_revision.tags_payload or []]
            )
            next_transcript = payload.modified_text if payload.modified_text is not None else current_transcript
            next_tags = (
                self._normalized_tag_payloads(payload.tags)
                if payload.tags is not None
                else current_tags
            )
            next_status = payload.status if payload.status is not None else active_revision.status

            state_changed = (
                next_transcript != current_transcript
                or next_tags != current_tags
                or next_status != active_revision.status
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
                return self._to_clip_lab_item_from_review_window(window, recording, session)

            self._append_review_window_revision(
                session,
                window,
                transcript_text=next_transcript,
                status=next_status,
                tags=[TagPayload.model_validate(tag) for tag in next_tags],
                edl_operations=list(active_revision.edl_operations or []),
                active_variant_id=active_revision.active_variant_id_snapshot,
                message=payload.message,
                is_milestone=payload.is_milestone,
            )
            session.commit()
            session.expire_all()
            window = self._get_review_window(session, review_window_id)
            recording = self._get_source_recording(session, window.source_recording_id)
            return self._to_clip_lab_item_from_review_window(window, recording, session)

    def update_review_window_status(self, review_window_id: str, payload: SliceStatusUpdate) -> ClipLabItemView:
        return self.save_review_window_state(
            review_window_id,
            SliceSaveRequest(
                status=payload.status,
                message=f"Status: {payload.status.value.replace('_', ' ')}",
            ),
        )

    def update_review_window_transcript(
        self,
        review_window_id: str,
        payload: SliceTranscriptUpdate,
    ) -> ClipLabItemView:
        return self.save_review_window_state(
            review_window_id,
            SliceSaveRequest(modified_text=payload.modified_text, message="Transcript updated"),
        )

    def update_review_window_tags(self, review_window_id: str, payload: SliceTagUpdate) -> ClipLabItemView:
        return self.save_review_window_state(
            review_window_id,
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

    def append_review_window_edl_operation(
        self,
        review_window_id: str,
        payload: SliceEdlUpdate,
    ) -> ClipLabItemView:
        with self._session() as session:
            window = self._get_review_window(session, review_window_id)
            recording = self._get_source_recording(session, window.source_recording_id)
            self._ensure_review_window_baseline(session, window, recording)
            active_revision = self._get_active_review_window_revision(session, window)
            next_operations = [
                *list(active_revision.edl_operations or []),
                payload.model_dump(mode="json"),
            ]
            self._append_review_window_revision(
                session,
                window,
                transcript_text=active_revision.transcript_text,
                status=active_revision.status,
                tags=self._review_window_tag_payloads(active_revision),
                edl_operations=next_operations,
                active_variant_id=active_revision.active_variant_id_snapshot,
                message=self._edl_message(payload),
            )
            session.commit()
            session.expire_all()
            window = self._get_review_window(session, review_window_id)
            recording = self._get_source_recording(session, window.source_recording_id)
            return self._to_clip_lab_item_from_review_window(window, recording, session)

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

    def undo_review_window(self, review_window_id: str) -> ClipLabItemView:
        with self._session() as session:
            window = self._get_review_window(session, review_window_id)
            recording = self._get_source_recording(session, window.source_recording_id)
            self._ensure_review_window_baseline(session, window, recording)
            active_revision = self._get_active_review_window_revision(session, window)
            if active_revision.parent_revision_id is None:
                raise ValueError("No earlier edit state is available")
            target_revision = self._get_review_window_revision(session, active_revision.parent_revision_id)
            self._set_active_review_window_revision(session, window, target_revision)
            session.commit()
            session.expire_all()
            window = self._get_review_window(session, review_window_id)
            recording = self._get_source_recording(session, window.source_recording_id)
            return self._to_clip_lab_item_from_review_window(window, recording, session)

    def redo_review_window(self, review_window_id: str) -> ClipLabItemView:
        with self._session() as session:
            window = self._get_review_window(session, review_window_id)
            recording = self._get_source_recording(session, window.source_recording_id)
            self._ensure_review_window_baseline(session, window, recording)
            active_revision = self._get_active_review_window_revision(session, window)
            redo_target = self._get_review_window_redo_target(session, window, active_revision)
            if redo_target is None:
                raise ValueError("No newer edit state is available")
            self._set_active_review_window_revision(session, window, redo_target)
            session.commit()
            session.expire_all()
            window = self._get_review_window(session, review_window_id)
            recording = self._get_source_recording(session, window.source_recording_id)
            return self._to_clip_lab_item_from_review_window(window, recording, session)

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

    def split_review_window(self, review_window_id: str, payload: SliceSplitRequest) -> list[ReviewWindowView]:
        with self._session() as session:
            window = self._get_review_window(session, review_window_id)
            recording = self._get_source_recording(session, window.source_recording_id)
            self._ensure_review_window_baseline(session, window, recording)
            active_revision = self._get_active_review_window_revision(session, window)
            active_variant = self._get_active_review_window_variant(session, window, active_revision)
            current_duration = self._review_window_duration(window, active_revision, active_variant)
            split_at = payload.split_at_seconds
            if split_at <= 0 or split_at >= current_duration:
                raise ValueError("Split point must be inside the clip duration")
            if active_revision.edl_operations:
                raise ValueError("Split is only supported before ReviewWindow waveform edits change timing")

            transcript_text = active_revision.transcript_text
            left_text, right_text = self._split_transcript_text(
                transcript_text,
                split_at / current_duration if current_duration > 0 else 0.5,
            )
            current_audio = self._render_review_window_audio_bytes(session, window)
            left_bytes = self._apply_edl_to_wav_bytes(
                current_audio,
                [{"op": "crop", "range": {"start_seconds": 0.0, "end_seconds": split_at}}],
            )
            right_bytes = self._apply_edl_to_wav_bytes(
                current_audio,
                [{"op": "crop", "range": {"start_seconds": split_at, "end_seconds": current_duration}}],
            )
            source_duration = max(window.end_seconds - window.start_seconds, 0.001)
            source_split = window.start_seconds + (source_duration * (split_at / max(current_duration, 0.001)))
            left_window = ReviewWindow(
                id=self._new_id("review-window"),
                source_recording_id=window.source_recording_id,
                start_seconds=window.start_seconds,
                end_seconds=min(max(source_split, window.start_seconds + 0.001), window.end_seconds),
                rough_transcript=left_text,
                order_index=window.order_index,
                window_metadata={
                    **dict(window.window_metadata or {}),
                    "parent_review_window_id": window.id,
                    "split_side": "left",
                    "source_boundary_mode": "proportional_unedited_mapping",
                    "source_split_seconds": round(source_split, 6),
                },
                created_at=utc_now(),
            )
            right_window = ReviewWindow(
                id=self._new_id("review-window"),
                source_recording_id=window.source_recording_id,
                start_seconds=max(min(source_split, window.end_seconds - 0.001), window.start_seconds),
                end_seconds=window.end_seconds,
                rough_transcript=right_text,
                order_index=window.order_index + 1,
                window_metadata={
                    **dict(window.window_metadata or {}),
                    "parent_review_window_id": window.id,
                    "split_side": "right",
                    "source_boundary_mode": "proportional_unedited_mapping",
                    "source_split_seconds": round(source_split, 6),
                },
                created_at=utc_now(),
            )
            session.add(left_window)
            session.add(right_window)
            session.flush()
            self._shift_review_window_order_indices(
                session,
                window.source_recording_id,
                window.order_index,
                1,
                exclude_ids={window.id, left_window.id, right_window.id},
            )
            left_variant = self._create_review_window_variant_from_bytes(
                session,
                left_window,
                left_bytes,
                generator_model="split-view",
                is_original=True,
            )
            right_variant = self._create_review_window_variant_from_bytes(
                session,
                right_window,
                right_bytes,
                generator_model="split-view",
                is_original=True,
            )
            inherited_tags = self._review_window_tag_payloads(active_revision)
            self._append_review_window_revision(
                session,
                left_window,
                transcript_text=left_text,
                status=ReviewStatus.UNRESOLVED,
                tags=inherited_tags,
                edl_operations=[],
                active_variant_id=left_variant.id,
                message="Split review window (left)",
            )
            self._append_review_window_revision(
                session,
                right_window,
                transcript_text=right_text,
                status=ReviewStatus.UNRESOLVED,
                tags=inherited_tags,
                edl_operations=[],
                active_variant_id=right_variant.id,
                message="Split review window (right)",
            )
            session.delete(window)
            session.commit()
            return self._list_recording_review_window_views(session, recording.id)

    def merge_with_next_review_window(self, review_window_id: str) -> list[ReviewWindowView]:
        with self._session() as session:
            window = self._get_review_window(session, review_window_id)
            recording = self._get_source_recording(session, window.source_recording_id)
            current_windows = self._list_recording_review_windows(session, recording.id)
            next_window = None
            for index, candidate in enumerate(current_windows):
                if candidate.id == review_window_id and index + 1 < len(current_windows):
                    next_window = current_windows[index + 1]
                    break
            if next_window is None:
                raise ValueError("No next review window is available for merge")
            self._ensure_review_window_baseline(session, window, recording)
            self._ensure_review_window_baseline(session, next_window, recording)
            first_revision = self._get_active_review_window_revision(session, window)
            second_revision = self._get_active_review_window_revision(session, next_window)
            merged_bytes = self._merge_wav_bytes(
                self._render_review_window_audio_bytes(session, window),
                self._render_review_window_audio_bytes(session, next_window),
            )
            merged_window = ReviewWindow(
                id=self._new_id("review-window"),
                source_recording_id=window.source_recording_id,
                start_seconds=min(window.start_seconds, next_window.start_seconds),
                end_seconds=max(window.end_seconds, next_window.end_seconds),
                rough_transcript=self._merge_transcript_text(first_revision.transcript_text, second_revision.transcript_text),
                order_index=min(window.order_index, next_window.order_index),
                window_metadata={
                    **dict(window.window_metadata or {}),
                    "merged_review_window_ids": [window.id, next_window.id],
                    "merge_boundary_mode": "concatenate_current_rendered_audio",
                },
                created_at=utc_now(),
            )
            session.add(merged_window)
            session.flush()
            merged_variant = self._create_review_window_variant_from_bytes(
                session,
                merged_window,
                merged_bytes,
                generator_model="merge",
                is_original=True,
            )
            merged_tags_map = {
                tag.name.lower(): tag
                for tag in [*self._review_window_tag_payloads(first_revision), *self._review_window_tag_payloads(second_revision)]
            }
            self._append_review_window_revision(
                session,
                merged_window,
                transcript_text=self._merge_transcript_text(first_revision.transcript_text, second_revision.transcript_text),
                status=ReviewStatus.UNRESOLVED,
                tags=list(merged_tags_map.values()),
                edl_operations=[],
                active_variant_id=merged_variant.id,
                message="Merged review window baseline",
            )
            session.delete(window)
            session.delete(next_window)
            self._shift_review_window_order_indices(
                session,
                recording.id,
                next_window.order_index,
                -1,
                exclude_ids={merged_window.id},
            )
            session.commit()
            return self._list_recording_review_window_views(session, recording.id)

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

    def get_clip_lab_waveform_peaks(self, item_kind: str, item_id: str, bins: int = 120) -> WaveformPeaks:
        safe_bins = max(32, min(bins, 2048))
        if item_kind == "slice":
            return self.get_waveform_peaks(item_id, safe_bins)
        if item_kind != "review_window":
            raise ValueError(f"Unsupported Clip Lab item kind: {item_kind}")

        with self._session() as session:
            window = self._get_review_window(session, item_id)
            audio_bytes = self._render_review_window_audio_bytes(session, window)
            return WaveformPeaks(clip_id=item_id, bins=safe_bins, peaks=self._extract_waveform_peaks_from_bytes(audio_bytes, safe_bins))

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

    def get_review_window_media_path(self, review_window_id: str) -> Path:
        with self._session() as session:
            window = self._get_review_window(session, review_window_id)
            return self._materialize_review_window_render_path(session, window)

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
            normalized_windows = self._normalize_review_window_chunks(recording, payload)
            session.exec(delete(ReviewWindow).where(ReviewWindow.source_recording_id == recording.id))
            created_windows: list[ReviewWindow] = []
            for chunk in normalized_windows:
                window = ReviewWindow(
                    id=self._new_id("review-window"),
                    source_recording_id=recording.id,
                    start_seconds=float(chunk["start_seconds"]),
                    end_seconds=float(chunk["end_seconds"]),
                    rough_transcript=str(chunk["rough_transcript"]),
                    order_index=int(chunk["order_index"]),
                    window_metadata=dict(chunk["window_metadata"] or {}),
                )
                session.add(window)
                created_windows.append(window)
            session.flush()
            for window in created_windows:
                self._ensure_review_window_baseline(session, window, recording)
            session.commit()
            return [self._review_window_view(window, session) for window in created_windows]

    def enqueue_forced_align_and_pack(
        self,
        recording_id: str,
        payload: ForcedAlignAndPackRequest,
    ) -> ProcessingJobView:
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            self._validate_forced_align_request(payload)
            self._load_forced_align_review_windows(session, recording, payload.review_window_ids)
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
            return self._processing_job_view(job)

    def enqueue_review_window_asr(
        self,
        recording_id: str,
        payload: ReviewWindowAsrRequest,
    ) -> ProcessingJobView:
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            ordered_windows = self._load_selected_review_windows_for_recording(
                session,
                recording,
                payload.review_window_ids,
                empty_selection_message="ReviewWindow ASR job requires at least one review window",
            )
            job = ProcessingJob(
                id=self._new_id("job"),
                kind=JobKind.REVIEW_WINDOW_ASR,
                status=JobStatus.PENDING,
                source_recording_id=recording.id,
                input_payload={
                    "target_kind": "review_window",
                    "review_window_ids": [window.id for window in ordered_windows],
                    "model_name": payload.model_name,
                    "model_version": payload.model_version,
                    "language_hint": payload.language_hint,
                },
            )
            session.add(job)
            session.commit()
            session.refresh(job)
            return self._processing_job_view(job)

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

    def run_review_window_variant(
        self,
        review_window_id: str,
        payload: AudioVariantRunRequest,
    ) -> ClipLabItemView:
        with self._session() as session:
            window = self._get_review_window(session, review_window_id)
            recording = self._get_source_recording(session, window.source_recording_id)
            self._ensure_review_window_baseline(session, window, recording)
            active_revision = self._get_active_review_window_revision(session, window)
            audio_bytes = self._render_review_window_audio_bytes(session, window)
            variant = self._create_review_window_variant_from_bytes(
                session,
                window,
                audio_bytes,
                generator_model=payload.generator_model,
                is_original=False,
            )
            self._append_review_window_revision(
                session,
                window,
                transcript_text=active_revision.transcript_text,
                status=active_revision.status,
                tags=self._review_window_tag_payloads(active_revision),
                edl_operations=list(active_revision.edl_operations or []),
                active_variant_id=variant.id,
                message=f"Ran {payload.generator_model}",
            )
            session.commit()
            session.expire_all()
            window = self._get_review_window(session, review_window_id)
            recording = self._get_source_recording(session, window.source_recording_id)
            return self._to_clip_lab_item_from_review_window(window, recording, session)

    def set_active_review_window_variant(
        self,
        review_window_id: str,
        payload: ActiveVariantUpdate,
    ) -> ClipLabItemView:
        with self._session() as session:
            window = self._get_review_window(session, review_window_id)
            recording = self._get_source_recording(session, window.source_recording_id)
            self._ensure_review_window_baseline(session, window, recording)
            variants = self._get_review_window_variants(session, window.id)
            matching_variant = next((variant for variant in variants if variant.id == payload.active_variant_id), None)
            if matching_variant is None:
                raise KeyError(payload.active_variant_id)
            active_revision = self._get_active_review_window_revision(session, window)
            self._append_review_window_revision(
                session,
                window,
                transcript_text=active_revision.transcript_text,
                status=active_revision.status,
                tags=self._review_window_tag_payloads(active_revision),
                edl_operations=list(active_revision.edl_operations or []),
                active_variant_id=matching_variant.id,
                message=f"Activated variant {matching_variant.generator_model or matching_variant.id}",
            )
            session.commit()
            session.expire_all()
            window = self._get_review_window(session, review_window_id)
            recording = self._get_source_recording(session, window.source_recording_id)
            return self._to_clip_lab_item_from_review_window(window, recording, session)

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
        dataset_processing_run_id: str | None = None
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
            dataset_processing_run_id = job.dataset_processing_run_id
            session.add(job)
            session.commit()
            session.refresh(job)
            result = self._processing_job_view(job)
        if dataset_processing_run_id is not None:
            self._refresh_dataset_processing_run(dataset_processing_run_id)
        return result

    def _fail_processing_job(
        self,
        job_id: str,
        error_message: str,
        *,
        now: datetime | None = None,
    ) -> ProcessingJobView:
        now = self._as_utc(now) if now is not None else utc_now()
        dataset_processing_run_id: str | None = None
        with self._session() as session:
            job = self._get_processing_job(session, job_id)
            job.status = JobStatus.FAILED
            job.error_message = error_message
            job.heartbeat_at = now
            job.completed_at = now
            dataset_processing_run_id = job.dataset_processing_run_id
            session.add(job)
            session.commit()
            session.refresh(job)
            result = self._processing_job_view(job)
        if dataset_processing_run_id is not None:
            self._refresh_dataset_processing_run(dataset_processing_run_id)
        return result

    def _processing_job_reference_time(self, job: ProcessingJob) -> datetime | None:
        reference = job.heartbeat_at or job.started_at or job.created_at
        if reference is None:
            return None
        return self._as_utc(reference)

    def _refresh_dataset_processing_run(self, run_id: str) -> DatasetProcessingRunView:
        with self._session() as session:
            run = session.get(DatasetProcessingRun, run_id)
            if run is None:
                raise KeyError(run_id)
            jobs = session.exec(
                select(ProcessingJob)
                .where(ProcessingJob.dataset_processing_run_id == run.id)
                .order_by(ProcessingJob.created_at, ProcessingJob.id)
            ).all()
            asr_jobs = [job for job in jobs if job.kind == JobKind.REVIEW_WINDOW_ASR]
            alignment_jobs = [job for job in jobs if job.kind == JobKind.FORCED_ALIGN_AND_PACK]

            run.asr_completed = sum(1 for job in asr_jobs if job.status == JobStatus.COMPLETED)
            run.asr_failed = sum(1 for job in asr_jobs if job.status == JobStatus.FAILED)
            run.alignment_completed = sum(1 for job in alignment_jobs if job.status == JobStatus.COMPLETED)
            run.alignment_failed = sum(1 for job in alignment_jobs if job.status == JobStatus.FAILED)

            asr_terminal = len(asr_jobs) > 0 and (run.asr_completed + run.asr_failed == len(asr_jobs))
            alignment_terminal = len(alignment_jobs) > 0 and (
                run.alignment_completed + run.alignment_failed == len(alignment_jobs)
            )

            if not asr_terminal:
                run.status = "asr_running"
                run.phase = "asr"
                run.completed_at = None
                run.current_message = (
                    f"ASR {run.asr_completed}/{run.total_review_windows} completed, "
                    f"{run.asr_failed} failed"
                )
            elif not alignment_jobs:
                successful_asr_jobs = [job for job in asr_jobs if job.status == JobStatus.COMPLETED]
                if not successful_asr_jobs:
                    run.status = "failed"
                    run.phase = "done"
                    run.completed_at = utc_now()
                    run.current_message = "ASR failed for all selected review windows"
                else:
                    source_recording = self._get_source_recording(session, run.source_recording_id)
                    for asr_job in successful_asr_jobs:
                        if asr_job.target_review_window_id is None:
                            continue
                        transcript_text = self._dataset_processing_alignment_transcript(
                            session,
                            asr_job.target_review_window_id,
                        )
                        session.add(
                            ProcessingJob(
                                id=self._new_id("job"),
                                kind=JobKind.FORCED_ALIGN_AND_PACK,
                                status=JobStatus.PENDING,
                                source_recording_id=source_recording.id,
                                dataset_processing_run_id=run.id,
                                target_review_window_id=asr_job.target_review_window_id,
                                input_payload={
                                    "review_window_ids": [asr_job.target_review_window_id],
                                    "transcript_text": transcript_text,
                                    "minimum_duration_seconds": 6.0,
                                    "orchestrated_by_dataset_processing_run_id": run.id,
                                },
                            )
                        )
                    run.alignment_total = len(successful_asr_jobs)
                    run.status = "alignment_running"
                    run.phase = "alignment"
                    run.completed_at = None
                    run.current_message = (
                        f"Alignment queued for {len(successful_asr_jobs)}/{run.total_review_windows} review windows"
                    )
            elif not alignment_terminal:
                run.status = "alignment_running"
                run.phase = "alignment"
                run.completed_at = None
                run.current_message = (
                    f"Alignment {run.alignment_completed}/{len(alignment_jobs)} completed, "
                    f"{run.alignment_failed} failed"
                )
            else:
                run.phase = "done"
                run.completed_at = utc_now()
                any_failures = run.asr_failed > 0 or run.alignment_failed > 0
                any_successes = run.alignment_completed > 0 or run.asr_completed > 0
                if not any_failures:
                    run.status = "completed"
                elif any_successes:
                    run.status = "partially_failed"
                else:
                    run.status = "failed"
                run.current_message = (
                    f"Alignment finished with {run.alignment_completed} completed and {run.alignment_failed} failed; "
                    f"ASR failures: {run.asr_failed}"
                )

            session.add(run)
            session.commit()
            session.refresh(run)
            return self._dataset_processing_run_view(run)

    def _dataset_processing_alignment_transcript(
        self,
        session: Session,
        review_window_id: str,
    ) -> str:
        window = self._get_review_window(session, review_window_id)
        active_revision = self._get_active_review_window_revision(session, window, allow_transient=True)
        if active_revision.transcript_text != window.rough_transcript:
            transcript_text = active_revision.transcript_text
        elif window.asr_draft_transcript:
            transcript_text = window.asr_draft_transcript
        else:
            transcript_text = window.rough_transcript
        transcript_text = transcript_text.strip()
        if not transcript_text:
            raise ValueError(f"ReviewWindow {review_window_id} does not have transcript text for alignment")
        return transcript_text

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
            if "dataset_processing_run_id" not in columns:
                connection.exec_driver_sql("ALTER TABLE processingjob ADD COLUMN dataset_processing_run_id TEXT")
            if "target_review_window_id" not in columns:
                connection.exec_driver_sql("ALTER TABLE processingjob ADD COLUMN target_review_window_id TEXT")

    def _migrate_dataset_processing_run_schema(self) -> None:
        with self.engine.begin() as connection:
            columns = {
                row[1]
                for row in connection.exec_driver_sql("PRAGMA table_info(datasetprocessingrun)").fetchall()
            }
            if not columns:
                return
            if "alignment_total" not in columns:
                connection.exec_driver_sql(
                    "ALTER TABLE datasetprocessingrun ADD COLUMN alignment_total INTEGER NOT NULL DEFAULT 0"
                )

    def _migrate_reviewwindow_schema(self) -> None:
        with self.engine.begin() as connection:
            columns = {
                row[1]
                for row in connection.exec_driver_sql("PRAGMA table_info(reviewwindow)").fetchall()
            }
            if not columns:
                return
            additions = {
                "asr_draft_transcript": "TEXT",
                "last_asr_job_id": "TEXT",
                "last_asr_at": "TIMESTAMP",
                "asr_model_name": "TEXT",
                "asr_model_version": "TEXT",
                "asr_language": "TEXT",
            }
            for column_name, column_sql in additions.items():
                if column_name not in columns:
                    connection.exec_driver_sql(f"ALTER TABLE reviewwindow ADD COLUMN {column_name} {column_sql}")

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
                ],
                pre_padding_ms=0,
                post_padding_ms=0,
                merge_gap_threshold_ms=0,
                minimum_window_duration_ms=100,
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

    def _get_review_window_revision(
        self,
        session: Session,
        revision_id: str,
    ) -> ReviewWindowRevision:
        revision = session.get(ReviewWindowRevision, revision_id)
        if revision is None:
            raise KeyError(revision_id)
        return revision

    def _get_review_window_variants(
        self,
        session: Session,
        review_window_id: str,
    ) -> list[ReviewWindowVariant]:
        return session.exec(
            select(ReviewWindowVariant)
            .where(ReviewWindowVariant.review_window_id == review_window_id)
            .order_by(ReviewWindowVariant.created_at, ReviewWindowVariant.id)
        ).all()

    def _list_recording_review_windows(
        self,
        session: Session,
        recording_id: str,
    ) -> list[ReviewWindow]:
        return session.exec(
            select(ReviewWindow)
            .where(ReviewWindow.source_recording_id == recording_id)
            .order_by(ReviewWindow.order_index, ReviewWindow.created_at)
        ).all()

    def _list_recording_review_window_views(
        self,
        session: Session,
        recording_id: str,
    ) -> list[ReviewWindowView]:
        return [self._review_window_view(window, session) for window in self._list_recording_review_windows(session, recording_id)]

    def _get_active_review_window_revision(
        self,
        session: Session,
        window: ReviewWindow,
        *,
        allow_transient: bool = False,
    ) -> ReviewWindowRevision:
        revision = session.exec(
            select(ReviewWindowRevision)
            .where(
                ReviewWindowRevision.review_window_id == window.id,
                ReviewWindowRevision.is_active.is_(True),
            )
            .order_by(ReviewWindowRevision.created_at.desc())
        ).first()
        if revision is None:
            if allow_transient:
                return self._transient_review_window_revision(window)
            raise ValueError(f"Review window {window.id} has no active revision")
        return revision

    def _transient_review_window_revision(self, window: ReviewWindow) -> ReviewWindowRevision:
        return ReviewWindowRevision(
            id=f"transient-review-window-{window.id}",
            review_window_id=window.id,
            parent_revision_id=None,
            transcript_text=window.rough_transcript,
            status=ReviewStatus.UNRESOLVED,
            tags_payload=[],
            edl_operations=[],
            active_variant_id_snapshot=None,
            message="Transient review window baseline",
            is_milestone=False,
            is_active=True,
            created_at=window.created_at,
        )

    def _get_review_window_redo_target(
        self,
        session: Session,
        window: ReviewWindow,
        active_revision: ReviewWindowRevision,
    ) -> ReviewWindowRevision | None:
        return session.exec(
            select(ReviewWindowRevision)
            .where(ReviewWindowRevision.parent_revision_id == active_revision.id)
            .order_by(ReviewWindowRevision.created_at.desc())
        ).first()

    def _set_active_review_window_revision(
        self,
        session: Session,
        window: ReviewWindow,
        target_revision: ReviewWindowRevision,
    ) -> None:
        revisions = session.exec(
            select(ReviewWindowRevision).where(ReviewWindowRevision.review_window_id == window.id)
        ).all()
        for revision in revisions:
            revision.is_active = revision.id == target_revision.id
            session.add(revision)

    def _review_window_variant_path(self, variant_id: str) -> Path:
        return self._managed_media_path("review-window-variants", variant_id)

    def _create_review_window_variant_from_bytes(
        self,
        session: Session,
        window: ReviewWindow,
        audio_bytes: bytes,
        *,
        generator_model: str | None,
        is_original: bool,
    ) -> ReviewWindowVariant:
        variant_id = self._new_id("review-variant")
        target_path = self._review_window_variant_path(variant_id)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(audio_bytes)
        sample_rate, _channels, num_samples = self._wav_metadata(audio_bytes)
        variant = ReviewWindowVariant(
            id=variant_id,
            review_window_id=window.id,
            file_path=str(target_path),
            is_original=is_original,
            generator_model=generator_model,
            sample_rate=sample_rate,
            num_samples=num_samples,
        )
        session.add(variant)
        session.flush()
        return variant

    def _ensure_review_window_baseline(
        self,
        session: Session,
        window: ReviewWindow,
        recording: SourceRecording,
    ) -> bool:
        existing_revision = session.exec(
            select(ReviewWindowRevision)
            .where(ReviewWindowRevision.review_window_id == window.id)
            .order_by(ReviewWindowRevision.created_at)
        ).first()
        if existing_revision is not None:
            if not session.exec(
                select(ReviewWindowRevision)
                .where(
                    ReviewWindowRevision.review_window_id == window.id,
                    ReviewWindowRevision.is_active.is_(True),
                )
            ).first():
                existing_revision.is_active = True
                session.add(existing_revision)
                session.flush()
                return True
            return False

        baseline_bytes = self._extract_source_window_wav_bytes(
            recording,
            window.start_seconds,
            window.end_seconds,
        )
        baseline_variant = self._create_review_window_variant_from_bytes(
            session,
            window,
            baseline_bytes,
            generator_model="review-window-source",
            is_original=True,
        )
        self._append_review_window_revision(
            session,
            window,
            transcript_text=window.rough_transcript,
            status=ReviewStatus.UNRESOLVED,
            tags=[],
            edl_operations=[],
            active_variant_id=baseline_variant.id,
            message="Imported review window baseline",
        )
        session.flush()
        return True

    def _validate_forced_align_request(self, payload: ForcedAlignAndPackRequest | dict[str, Any]) -> None:
        minimum_duration = payload.minimum_duration_seconds if isinstance(payload, ForcedAlignAndPackRequest) else float(payload.get("minimum_duration_seconds", 6.0))
        if not math.isclose(float(minimum_duration), 6.0, rel_tol=0.0, abs_tol=1e-9):
            raise ValueError("minimum_duration_seconds is fixed at 6.0 in the current packer and cannot be overridden")

    def _load_selected_review_windows_for_recording(
        self,
        session: Session,
        recording: SourceRecording,
        requested_window_ids: list[str] | None,
        *,
        empty_selection_message: str,
    ) -> list[ReviewWindow]:
        if requested_window_ids is not None and len(set(requested_window_ids)) != len(requested_window_ids):
            raise ValueError("review_window_ids cannot contain duplicates")
        if not requested_window_ids:
            raise ValueError(empty_selection_message)
        ordered_windows: list[ReviewWindow] = []
        for review_window_id in requested_window_ids:
            window = self._get_review_window(session, review_window_id)
            if window.source_recording_id != recording.id:
                raise ValueError("One or more review windows do not belong to the source recording")
            ordered_windows.append(window)
        ordered_windows.sort(key=lambda window: (window.order_index, window.created_at, window.id))
        return ordered_windows

    def _load_forced_align_review_windows(
        self,
        session: Session,
        recording: SourceRecording,
        requested_window_ids: list[str] | None,
    ) -> list[ReviewWindow]:
        if requested_window_ids:
            windows = self._load_selected_review_windows_for_recording(
                session,
                recording,
                requested_window_ids,
                empty_selection_message="Forced align and pack job requires at least one review window",
            )
        else:
            windows = session.exec(
                select(ReviewWindow)
                .where(ReviewWindow.source_recording_id == recording.id)
                .order_by(ReviewWindow.order_index, ReviewWindow.created_at)
            ).all()
        ordered_windows = list(sorted(windows, key=lambda window: (window.order_index, window.created_at)))
        if not ordered_windows:
            raise ValueError("Forced align and pack job requires at least one review window")
        self._validate_contiguous_review_windows(ordered_windows)
        return ordered_windows

    def _validate_contiguous_review_windows(self, windows: list[ReviewWindow]) -> None:
        tolerance_seconds = 0.01
        for current_window, next_window in zip(windows, windows[1:]):
            gap_seconds = next_window.start_seconds - current_window.end_seconds
            if abs(gap_seconds) > tolerance_seconds:
                relation = "gap" if gap_seconds > 0 else "overlap"
                raise ValueError(
                    f"Forced align and pack currently requires contiguous review windows; found {relation} of {gap_seconds:.3f}s between {current_window.id} and {next_window.id}"
                )

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

    def _validate_review_window_generation_policy(self, payload: SlicerHandoffRequest) -> None:
        if payload.pre_padding_ms < 0:
            raise ValueError("pre_padding_ms must be non-negative")
        if payload.post_padding_ms < 0:
            raise ValueError("post_padding_ms must be non-negative")
        if payload.merge_gap_threshold_ms < 0:
            raise ValueError("merge_gap_threshold_ms must be non-negative")
        if payload.minimum_window_duration_ms <= 0:
            raise ValueError("minimum_window_duration_ms must be positive")

    def _normalize_review_window_chunks(
        self,
        recording: SourceRecording,
        payload: SlicerHandoffRequest,
    ) -> list[dict[str, Any]]:
        # ReviewWindows are coarse, conservative review/ASR decode spans, not final train/export slices.
        self._validate_review_window_generation_policy(payload)
        if not payload.windows:
            return []

        pre_padding_seconds = payload.pre_padding_ms / 1000.0
        post_padding_seconds = payload.post_padding_ms / 1000.0
        merge_gap_seconds = payload.merge_gap_threshold_ms / 1000.0
        minimum_window_duration_seconds = payload.minimum_window_duration_ms / 1000.0

        ordered_inputs = sorted(
            list(enumerate(payload.windows)),
            key=lambda item: (
                float(item[1].start_seconds),
                float(item[1].end_seconds),
                int(item[1].order_index),
                item[0],
            ),
        )
        normalized: list[dict[str, Any]] = []

        for original_position, chunk in ordered_inputs:
            raw_start = float(chunk.start_seconds)
            raw_end = float(chunk.end_seconds)
            self._validate_review_window_bounds(recording, raw_start, raw_end)
            normalized_start = max(0.0, raw_start - pre_padding_seconds)
            normalized_end = min(recording.duration_s, raw_end + post_padding_seconds)
            self._validate_review_window_bounds(recording, normalized_start, normalized_end)
            candidate = {
                "start_seconds": normalized_start,
                "end_seconds": normalized_end,
                "rough_transcript": str(chunk.rough_transcript or "").strip(),
                "source_order_indices": [int(chunk.order_index)],
                "source_start_seconds": raw_start,
                "source_end_seconds": raw_end,
                "merged_input_count": 1,
                "coalesced_tiny_window": False,
                "boundary_mode": "coarse_review_window_padded",
                "generation_mode": "slicer_handoff_normalized",
                "seed_metadata": dict(chunk.model_metadata or {}),
                "original_positions": [original_position],
            }
            if normalized:
                previous = normalized[-1]
                gap_seconds = normalized_start - float(previous["end_seconds"])
                if normalized_start < float(previous["end_seconds"]) or gap_seconds < merge_gap_seconds:
                    previous["start_seconds"] = min(float(previous["start_seconds"]), normalized_start)
                    previous["end_seconds"] = max(float(previous["end_seconds"]), normalized_end)
                    previous["rough_transcript"] = self._merge_transcript_text(
                        str(previous["rough_transcript"]),
                        str(candidate["rough_transcript"]),
                    )
                    previous["source_start_seconds"] = min(float(previous["source_start_seconds"]), raw_start)
                    previous["source_end_seconds"] = max(float(previous["source_end_seconds"]), raw_end)
                    previous["source_order_indices"] = [
                        *list(previous["source_order_indices"]),
                        int(chunk.order_index),
                    ]
                    previous["merged_input_count"] = int(previous["merged_input_count"]) + 1
                    previous["seed_metadata"] = self._merge_review_window_seed_metadata(
                        dict(previous["seed_metadata"]),
                        candidate["seed_metadata"],
                    )
                    previous["original_positions"] = [*list(previous["original_positions"]), original_position]
                    continue
            normalized.append(candidate)

        for index, window in enumerate(normalized):
            duration_seconds = float(window["end_seconds"]) - float(window["start_seconds"])
            if duration_seconds < minimum_window_duration_seconds:
                raise ValueError(
                    "Review window normalization produced a pathological tiny window; "
                    "increase padding or merge the upstream VAD segments first"
                )

            window["order_index"] = index
            window["window_metadata"] = self._build_review_window_generation_metadata(
                window,
                payload,
            )
            del window["seed_metadata"]
            del window["original_positions"]

        for previous, current in zip(normalized, normalized[1:]):
            if float(previous["end_seconds"]) > float(current["start_seconds"]):
                raise ValueError("Review window normalization produced overlapping windows")
        return normalized

    def _merge_review_window_seed_metadata(
        self,
        first: dict[str, Any],
        second: dict[str, Any],
    ) -> dict[str, Any]:
        merged: dict[str, Any] = dict(first)
        for key, value in second.items():
            if key not in merged:
                merged[key] = value
                continue
            if merged[key] == value:
                continue
            merged.pop(key, None)
        return merged

    def _build_review_window_generation_metadata(
        self,
        window: dict[str, Any],
        payload: SlicerHandoffRequest,
    ) -> dict[str, Any]:
        metadata = dict(window["seed_metadata"])
        metadata.update(
            {
                "generation_mode": str(window["generation_mode"]),
                "boundary_mode": str(window["boundary_mode"]),
                "pre_padding_ms": int(payload.pre_padding_ms),
                "post_padding_ms": int(payload.post_padding_ms),
                "merged_gap_threshold_ms": int(payload.merge_gap_threshold_ms),
                "minimum_window_duration_ms": int(payload.minimum_window_duration_ms),
                "source_start_seconds": round(float(window["source_start_seconds"]), 6),
                "source_end_seconds": round(float(window["source_end_seconds"]), 6),
                "was_merged": int(window["merged_input_count"]) > 1,
                "merged_input_count": int(window["merged_input_count"]),
                "coalesced_tiny_window": bool(window["coalesced_tiny_window"]),
                "source_order_indices": list(window["source_order_indices"]),
            }
        )
        if len(metadata["source_order_indices"]) == 1:
            metadata["source_order_index"] = metadata["source_order_indices"][0]
        return metadata

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

    def _review_window_tag_payloads(self, revision: ReviewWindowRevision) -> list[TagPayload]:
        return [
            TagPayload.model_validate(tag)
            for tag in sorted(revision.tags_payload or [], key=lambda item: str(item.get("name", "")).lower())
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

    def _append_review_window_revision(
        self,
        session: Session,
        window: ReviewWindow,
        *,
        transcript_text: str,
        status: ReviewStatus,
        tags: list[TagPayload],
        edl_operations: list[dict[str, Any]],
        active_variant_id: str | None,
        message: str | None = None,
        is_milestone: bool = False,
        created_at: datetime | None = None,
    ) -> ReviewWindowRevision:
        previous_revisions = session.exec(
            select(ReviewWindowRevision)
            .where(ReviewWindowRevision.review_window_id == window.id)
        ).all()
        parent_revision_id = None
        for revision in previous_revisions:
            if revision.is_active:
                parent_revision_id = revision.id
            revision.is_active = False
            session.add(revision)
        revision = ReviewWindowRevision(
            id=self._new_id("review-edit"),
            review_window_id=window.id,
            parent_revision_id=parent_revision_id,
            transcript_text=transcript_text,
            status=status,
            tags_payload=[tag.model_dump(mode="json") for tag in tags],
            edl_operations=list(edl_operations),
            active_variant_id_snapshot=active_variant_id,
            message=message,
            is_milestone=is_milestone,
            is_active=True,
            created_at=created_at or utc_now(),
        )
        session.add(revision)
        session.flush()
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

    def _clip_lab_transcript_from_review_window(
        self,
        window: ReviewWindow,
        active_revision: ReviewWindowRevision,
    ) -> ClipLabTranscriptView:
        modified_text = (
            active_revision.transcript_text
            if active_revision.transcript_text != window.rough_transcript
            else None
        )
        return ClipLabTranscriptView(
            id=active_revision.id,
            original_text=window.rough_transcript,
            modified_text=modified_text,
            is_modified=modified_text is not None,
            draft_text=window.asr_draft_transcript,
            draft_source="review_window_asr" if window.asr_draft_transcript else None,
            alignment_data=None,
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

    def _review_window_variant_view(
        self,
        variant: ReviewWindowVariant | None,
    ) -> ClipLabVariantView | None:
        if variant is None:
            return None
        return ClipLabVariantView(
            id=variant.id,
            is_original=variant.is_original,
            generator_model=variant.generator_model,
            sample_rate=variant.sample_rate,
            num_samples=variant.num_samples,
        )

    def _get_active_review_window_variant(
        self,
        session: Session,
        window: ReviewWindow,
        active_revision: ReviewWindowRevision,
    ) -> ReviewWindowVariant | None:
        active_variant_id = active_revision.active_variant_id_snapshot
        if active_variant_id is None:
            return None
        return session.get(ReviewWindowVariant, active_variant_id)

    def _review_window_transcript_source(
        self,
        window: ReviewWindow,
        active_revision: ReviewWindowRevision,
    ) -> str:
        metadata = dict(window.window_metadata or {})
        explicit_source = metadata.get("transcript_source")
        if isinstance(explicit_source, str) and explicit_source.strip():
            return explicit_source
        if active_revision.transcript_text != window.rough_transcript:
            return "manual"
        return "review_window_seed"

    def _review_window_asr_status(self, window: ReviewWindow) -> str:
        return "draft_available" if window.asr_draft_transcript else "not_started"

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

    def _review_window_view(
        self,
        window: ReviewWindow,
        session: Session | None = None,
    ) -> ReviewWindowView:
        if session is None:
            with self._session() as owned_session:
                session_window = self._get_review_window(owned_session, window.id)
                return self._review_window_view(session_window, owned_session)

        session_window = self._get_review_window(session, window.id)
        active_revision = session.exec(
            select(ReviewWindowRevision)
            .where(
                ReviewWindowRevision.review_window_id == window.id,
                ReviewWindowRevision.is_active.is_(True),
            )
            .order_by(ReviewWindowRevision.created_at.desc())
        ).first()
        tags = [
            self._tag_view(Tag(id=f"review-tag-{tag.name.lower()}", name=tag.name, color=tag.color))
            for tag in (self._review_window_tag_payloads(active_revision) if active_revision is not None else [])
        ]
        return ReviewWindowView(
            id=session_window.id,
            source_recording_id=session_window.source_recording_id,
            start_seconds=session_window.start_seconds,
            end_seconds=session_window.end_seconds,
            rough_transcript=session_window.rough_transcript,
            reviewed_transcript=active_revision.transcript_text if active_revision is not None else session_window.rough_transcript,
            asr_draft_transcript=session_window.asr_draft_transcript,
            transcript_source=self._review_window_transcript_source(session_window, active_revision) if active_revision is not None else "review_window_seed",
            last_asr_job_id=session_window.last_asr_job_id,
            last_asr_at=self._as_utc(session_window.last_asr_at) if session_window.last_asr_at is not None else None,
            asr_model_name=session_window.asr_model_name,
            asr_model_version=session_window.asr_model_version,
            asr_language=session_window.asr_language,
            review_status=active_revision.status if active_revision is not None else ReviewStatus.UNRESOLVED,
            tags=tags,
            order_index=session_window.order_index,
            window_metadata=session_window.window_metadata,
            can_undo=active_revision.parent_revision_id is not None if active_revision is not None else False,
            can_redo=self._get_review_window_redo_target(session, session_window, active_revision) is not None if active_revision is not None else False,
            created_at=self._as_utc(session_window.created_at),
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
            claimed_by=job.claimed_by,
            created_at=self._as_utc(job.created_at),
            started_at=self._as_utc(job.started_at) if job.started_at is not None else None,
            heartbeat_at=self._as_utc(job.heartbeat_at) if job.heartbeat_at is not None else None,
            completed_at=self._as_utc(job.completed_at) if job.completed_at is not None else None,
        )

    def _dataset_processing_run_view(self, run: DatasetProcessingRun) -> DatasetProcessingRunView:
        health_page_ready = bool(
            run.phase == "done"
            and (run.alignment_completed > 0 or run.alignment_failed > 0)
        )
        return DatasetProcessingRunView(
            id=run.id,
            source_recording_id=run.source_recording_id,
            status=run.status,
            phase=run.phase,
            total_review_windows=run.total_review_windows,
            asr_total=run.total_review_windows,
            alignment_total=run.alignment_total,
            asr_completed=run.asr_completed,
            asr_failed=run.asr_failed,
            alignment_completed=run.alignment_completed,
            alignment_failed=run.alignment_failed,
            current_message=run.current_message,
            started_at=self._as_utc(run.started_at),
            completed_at=self._as_utc(run.completed_at) if run.completed_at is not None else None,
            health_page_ready=health_page_ready,
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

    def _review_window_commit_view(self, commit: ReviewWindowRevision) -> ClipLabCommitView:
        return ClipLabCommitView(
            id=commit.id,
            parent_commit_id=commit.parent_revision_id,
            edl_operations=list(commit.edl_operations or []),
            transcript_text=commit.transcript_text,
            status=commit.status,
            tags=self._review_window_tag_payloads(commit),
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

    def _slice_clip_lab_capabilities(self) -> ClipLabCapabilitiesView:
        return ClipLabCapabilitiesView(
            can_edit_transcript=True,
            can_edit_tags=True,
            can_set_status=True,
            can_save=True,
            can_split=True,
            can_merge=True,
            can_edit_waveform=True,
            can_run_processing=True,
            can_switch_variants=True,
            can_export=False,
            can_finalize=False,
        )

    def _review_window_clip_lab_capabilities(self) -> ClipLabCapabilitiesView:
        return ClipLabCapabilitiesView(
            can_edit_transcript=True,
            can_edit_tags=True,
            can_set_status=True,
            can_save=True,
            can_split=True,
            can_merge=True,
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

    def _review_window_audio_url(self, window: ReviewWindow, active_revision: ReviewWindowRevision) -> str:
        active_variant_id = active_revision.active_variant_id_snapshot or "source"
        return f"/media/review-windows/{window.id}.wav?rev={active_variant_id}:{active_revision.id}"

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

    def _to_clip_lab_item_from_review_window(
        self,
        window: ReviewWindow,
        recording: SourceRecording,
        session: Session,
    ) -> ClipLabItemView:
        metadata = dict(window.window_metadata or {})
        session_window = self._get_review_window(session, window.id)
        active_revision = self._get_active_review_window_revision(session, session_window, allow_transient=True)
        variants = self._get_review_window_variants(session, session_window.id)
        active_variant = self._get_active_review_window_variant(session, session_window, active_revision)
        commits = session.exec(
            select(ReviewWindowRevision)
            .where(ReviewWindowRevision.review_window_id == session_window.id)
            .order_by(ReviewWindowRevision.created_at)
        ).all()
        return ClipLabItemView(
            id=window.id,
            kind="review_window",
            source_recording_id=window.source_recording_id,
            source_recording=self._source_recording_view(recording),
            start_seconds=window.start_seconds,
            end_seconds=window.end_seconds,
            duration_seconds=self._review_window_duration(session_window, active_revision, active_variant),
            status=active_revision.status,
            created_at=self._as_utc(window.created_at),
            transcript=self._clip_lab_transcript_from_review_window(session_window, active_revision),
            tags=[self._tag_view(Tag(id=f"review-tag-{tag.name.lower()}", name=tag.name, color=tag.color)) for tag in self._review_window_tag_payloads(active_revision)],
            speaker_name=str(metadata.get("speaker_name")) if metadata.get("speaker_name") is not None else None,
            language=str(metadata.get("language")) if metadata.get("language") is not None else None,
            audio_url=self._review_window_audio_url(session_window, active_revision),
            item_metadata=metadata or None,
            transcript_source=self._review_window_transcript_source(session_window, active_revision),
            can_run_asr=True,
            asr_placeholder_message="ASR runs per ReviewWindow and stores a draft transcript candidate without overwriting reviewed text.",
            asr_draft_transcript=session_window.asr_draft_transcript,
            last_asr_job_id=session_window.last_asr_job_id,
            last_asr_at=self._as_utc(session_window.last_asr_at) if session_window.last_asr_at is not None else None,
            asr_model_name=session_window.asr_model_name,
            asr_model_version=session_window.asr_model_version,
            asr_language=session_window.asr_language,
            active_variant_generator_model=active_variant.generator_model if active_variant is not None else None,
            can_undo=active_revision.parent_revision_id is not None,
            can_redo=self._get_review_window_redo_target(session, session_window, active_revision) is not None,
            capabilities=self._review_window_clip_lab_capabilities(),
            variants=[self._review_window_variant_view(variant) for variant in variants],
            commits=[self._review_window_commit_view(commit) for commit in commits],
            active_variant=self._review_window_variant_view(active_variant),
            active_commit=self._review_window_commit_view(active_revision),
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

    def _review_window_duration(
        self,
        window: ReviewWindow,
        active_revision: ReviewWindowRevision,
        active_variant: ReviewWindowVariant | None,
    ) -> float:
        if active_variant is not None:
            base_duration = active_variant.num_samples / max(active_variant.sample_rate, 1)
        else:
            base_duration = max(window.end_seconds - window.start_seconds, 0.0)
        return round(self._apply_edl_to_duration(base_duration, list(active_revision.edl_operations or [])), 2)

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

    def _review_window_audio_cache_identifier(
        self,
        window: ReviewWindow,
        active_revision: ReviewWindowRevision,
    ) -> str:
        state_key = json.dumps(
            {
                "review_window_id": window.id,
                "active_variant_id": active_revision.active_variant_id_snapshot,
                "edl_operations": list(active_revision.edl_operations or []),
            },
            sort_keys=True,
        )
        fingerprint = hashlib.sha1(state_key.encode("utf-8")).hexdigest()[:12]
        return f"{window.id}-{fingerprint}"

    def _review_window_render_cache_path(
        self,
        window: ReviewWindow,
        active_revision: ReviewWindowRevision,
    ) -> Path:
        return self._managed_media_path(
            "review-window-renders",
            self._review_window_audio_cache_identifier(window, active_revision),
        )

    def _render_review_window_audio_bytes(
        self,
        session: Session,
        window: ReviewWindow,
    ) -> bytes:
        recording = self._get_source_recording(session, window.source_recording_id)
        active_revision = self._get_active_review_window_revision(session, window, allow_transient=True)
        active_variant = self._get_active_review_window_variant(session, window, active_revision)
        if active_variant is None:
            base_bytes = self._extract_source_window_wav_bytes(recording, window.start_seconds, window.end_seconds)
        else:
            variant_path = self._resolve_variant_media_path(Path(active_variant.file_path))
            base_bytes = variant_path.read_bytes()
        return self._apply_edl_to_wav_bytes(base_bytes, list(active_revision.edl_operations or []))

    def _materialize_review_window_render_path(
        self,
        session: Session,
        window: ReviewWindow,
    ) -> Path:
        active_revision = self._get_active_review_window_revision(session, window, allow_transient=True)
        target_path = self._review_window_render_cache_path(window, active_revision)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if not target_path.exists():
            target_path.write_bytes(self._render_review_window_audio_bytes(session, window))
        return target_path

    def _prune_derived_media_cache(self) -> int:
        with self._session() as session:
            slices = session.exec(select(Slice)).all()
            keep_cache_ids = {self._slice_audio_cache_identifier(slice_row) for slice_row in slices}
            review_windows = session.exec(select(ReviewWindow)).all()
            keep_review_window_render_ids: set[str] = set()
            for window in review_windows:
                active_revision = self._get_active_review_window_revision(session, window, allow_transient=True)
                keep_review_window_render_ids.add(self._review_window_audio_cache_identifier(window, active_revision))
            retained_review_window_variant_paths = set()
            for raw_path in session.exec(select(ReviewWindowVariant.file_path)).all():
                try:
                    retained_review_window_variant_paths.add(self._resolve_variant_media_path(Path(raw_path)))
                except (FileNotFoundError, ValueError):
                    continue
        deleted_count = 0
        slices_root = self.media_root / "slices"
        peaks_root = self.media_root / "peaks"
        review_window_renders_root = self.media_root / "review-window-renders"
        review_window_variants_root = self.media_root / "review-window-variants"
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
        for path in review_window_renders_root.glob("*.wav"):
            cache_id = path.stem
            if cache_id in keep_review_window_render_ids:
                continue
            path.unlink()
            deleted_count += 1
        for path in review_window_variants_root.glob("*.wav"):
            try:
                resolved_path = self._resolve_variant_media_path(path)
            except (FileNotFoundError, ValueError):
                if path.exists():
                    path.unlink()
                    deleted_count += 1
                continue
            if resolved_path in retained_review_window_variant_paths:
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

    def _shift_review_window_order_indices(
        self,
        session: Session,
        recording_id: str,
        from_index: int,
        amount: int,
        exclude_ids: set[str] | None = None,
    ) -> None:
        exclude_ids = exclude_ids or set()
        for window in self._list_recording_review_windows(session, recording_id):
            if window.id in exclude_ids:
                continue
            if window.order_index > from_index:
                window.order_index = window.order_index + amount
                session.add(window)

    def _dispatch_processing_job(self, job_id: str) -> dict[str, Any] | None:
        with self._session() as session:
            job = self._get_processing_job(session, job_id)
            job_kind = job.kind
        if job_kind == JobKind.REVIEW_WINDOW_ASR:
            return self._run_review_window_asr_job(job_id)
        if job_kind == JobKind.FORCED_ALIGN_AND_PACK:
            return self._run_forced_align_and_pack_job(job_id)
        raise ValueError(f"Unsupported processing job kind: {job_kind}")

    def _run_review_window_asr_job(self, job_id: str) -> dict[str, Any]:
        with self._session() as session:
            job = self._get_processing_job(session, job_id)
            if job.source_recording_id is None:
                raise ValueError("ReviewWindow ASR job is missing its source recording")
            recording = self._get_source_recording(session, job.source_recording_id)
            payload = dict(job.input_payload or {})
            if payload.get("target_kind") != "review_window":
                raise ValueError("ReviewWindow ASR job target_kind must be review_window")
            ordered_windows = self._load_selected_review_windows_for_recording(
                session,
                recording,
                payload.get("review_window_ids"),
                empty_selection_message="ReviewWindow ASR job requires at least one review window",
            )
            adapter_config = self._review_window_asr_backend_config(payload)
            language_hint = payload.get("language_hint")
            completed_at = utc_now()
            window_results: list[dict[str, Any]] = []
            backend_client = self._create_review_window_asr_backend_client(adapter_config)

            for window in ordered_windows:
                active_revision = self._get_active_review_window_revision(session, window, allow_transient=True)
                draft_result = self._run_review_window_asr_adapter(
                    window,
                    adapter_config=adapter_config,
                    backend_client=backend_client,
                    language_hint=str(language_hint) if language_hint is not None else None,
                )
                reviewed_transcript_protected = active_revision.transcript_text != window.rough_transcript
                window.asr_draft_transcript = draft_result["transcript_text"]
                window.last_asr_job_id = job.id
                window.last_asr_at = completed_at
                window.asr_model_name = draft_result["model_name"]
                window.asr_model_version = draft_result["model_version"]
                window.asr_language = draft_result["language"]
                session.add(window)
                window_results.append(
                    {
                        "review_window_id": window.id,
                        "transcript_text": draft_result["transcript_text"],
                        "stored_as": "review_window_asr_draft",
                        "reviewed_transcript_protected": reviewed_transcript_protected,
                        "backend": draft_result["backend"],
                        "model_name": draft_result["model_name"],
                        "model_version": draft_result["model_version"],
                        "language": draft_result["language"],
                        "segments": list(draft_result["segments"]),
                    }
                )

            session.commit()

        return {
            "target_kind": "review_window",
            "backend": adapter_config["backend"],
            "processed_review_window_count": len(window_results),
            "review_window_results": window_results,
            "stored_as": "review_window_asr_draft",
        }

    def _run_forced_align_and_pack_job(self, job_id: str) -> dict[str, Any]:
        with self._session() as session:
            job = self._get_processing_job(session, job_id)
            if job.source_recording_id is None:
                raise ValueError("Forced align and pack job is missing its source recording")
            recording = self._get_source_recording(session, job.source_recording_id)
            payload = job.input_payload or {}
            self._validate_forced_align_request(payload)
            requested_window_ids = payload.get("review_window_ids")
            windows = self._load_forced_align_review_windows(session, recording, requested_window_ids)
            master_transcript = " ".join(
                self._get_active_review_window_revision(session, window, allow_transient=True).transcript_text.strip()
                for window in windows
                if self._get_active_review_window_revision(session, window, allow_transient=True).transcript_text.strip()
            )
            explicit_transcript = str(payload.get("transcript_text", "")).strip()
            if explicit_transcript:
                master_transcript = explicit_transcript
            if not master_transcript:
                raise ValueError("Forced align and pack job requires transcript_text")

            absolute_start = windows[0].start_seconds
            absolute_end = windows[-1].end_seconds
            recording_id = recording.id
            batch_id = recording.batch_id
            selected_window_ids = [window.id for window in windows]

        audio_bytes = self._extract_source_window_wav_bytes(recording, absolute_start, absolute_end)
        audio_samples, sample_rate = self._wav_bytes_to_mono_samples(audio_bytes)
        if sample_rate <= 0:
            raise ValueError("Forced align and pack job produced invalid sample rate")

        alignment_units = self._run_forced_align_worker(audio_bytes, master_transcript)
        packed_slices = list(pack_aligned_words(alignment_units, audio_samples, sample_rate))

        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            existing_slices = self._get_batch_slices(session, batch_id)
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
                        "source": "torchaudio_forced_align_worker",
                        "relative_start_seconds": float(packed_slice["start_s"]),
                        "relative_end_seconds": float(packed_slice["end_s"]),
                    },
                )
                created_slice_ids.append(slice_row.id)
                next_order_index += 1

            session.commit()

        return {
            "created_slice_count": len(created_slice_ids),
            "created_slice_ids": created_slice_ids,
            "selected_review_window_ids": selected_window_ids,
            "selection_mode": "contiguous_review_windows_only",
            "packing_policy": {
                "minimum_duration_seconds": 6.0,
                "configured_minimum_duration_seconds": float(payload.get("minimum_duration_seconds", 6.0)),
            },
            "window_span": {
                "start_seconds": absolute_start,
                "end_seconds": absolute_end,
            },
        }

    def _run_review_window_asr_adapter(
        self,
        window: ReviewWindow,
        *,
        adapter_config: dict[str, str],
        backend_client: Any = None,
        language_hint: str | None,
    ) -> dict[str, Any]:
        backend = adapter_config["backend"]
        if backend == "stub":
            return self._run_review_window_asr_stub_adapter(
                window,
                model_name=adapter_config["model_name"],
                model_version=adapter_config["model_version"],
                language_hint=language_hint,
            )
        if backend == "faster_whisper":
            return self._run_review_window_asr_faster_whisper_adapter(
                window,
                model=backend_client,
                model_name=adapter_config["model_name"],
                model_version=adapter_config["model_version"],
                language_hint=language_hint,
            )
        raise ValueError(f"Unsupported ASR backend: {backend}")

    def _create_review_window_asr_backend_client(self, adapter_config: dict[str, str]) -> Any:
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

    def _review_window_asr_backend_config(self, payload: dict[str, Any]) -> dict[str, str]:
        # Dev/local smoke path:
        # ASR_BACKEND=faster_whisper ASR_MODEL_PATH=/abs/path/to/local/model
        # ASR_DEVICE=cpu|cuda ASR_COMPUTE_TYPE=int8|float16 uv run --directory backend python -m app.worker --once
        backend = str(os.getenv("ASR_BACKEND", "stub")).strip().lower() or "stub"
        requested_model_name = str(payload.get("model_name") or "").strip()
        requested_model_version = str(payload.get("model_version") or "").strip()
        if backend == "stub":
            return {
                "backend": "stub",
                "model_name": requested_model_name or "stub-review-window-asr",
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

    def _run_review_window_asr_stub_adapter(
        self,
        window: ReviewWindow,
        *,
        model_name: str,
        model_version: str,
        language_hint: str | None,
    ) -> dict[str, Any]:
        transcript_text = window.rough_transcript.strip()
        if not transcript_text:
            transcript_text = (
                f"stub asr review window {window.order_index} "
                f"{window.start_seconds:.2f}-{window.end_seconds:.2f}"
            )
        metadata = dict(window.window_metadata or {})
        language = language_hint or window.asr_language or str(metadata.get("language") or "")
        duration_seconds = max(window.end_seconds - window.start_seconds, 0.0)
        return {
            "backend": "stub",
            "transcript_text": transcript_text,
            "model_name": model_name,
            "model_version": model_version,
            "language": language,
            "segments": [
                {
                    "start": 0.0,
                    "end": round(duration_seconds, 6),
                    "text": transcript_text,
                }
            ],
        }

    def _run_review_window_asr_faster_whisper_adapter(
        self,
        window: ReviewWindow,
        *,
        model: Any,
        model_name: str,
        model_version: str,
        language_hint: str | None,
    ) -> dict[str, Any]:
        audio_path = self.get_review_window_media_path(window.id)
        segments_iter, info = model.transcribe(
            str(audio_path),
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

        worker_python, worker_script = self._resolve_forced_align_worker_paths()
        with tempfile.TemporaryDirectory(prefix="speechcraft-aligner-") as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            audio_path = temp_dir / "input.wav"
            output_path = temp_dir / "alignment.json"
            audio_path.write_bytes(audio_bytes)

            try:
                completed = subprocess.run(
                    [
                        str(worker_python),
                        str(worker_script),
                        "--audio",
                        str(audio_path),
                        "--text",
                        transcript_text,
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
