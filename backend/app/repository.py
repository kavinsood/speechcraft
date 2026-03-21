from __future__ import annotations

import audioop
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
    ImportBatch,
    ImportBatchCreate,
    JobStatus,
    MediaCleanupResult,
    ProjectSummary,
    ReferenceAsset,
    ReferenceAssetCreateFromSlice,
    ReferenceAssetCreateFromCandidate,
    ReferenceAssetDetail,
    ReferenceAssetStatus,
    ReferenceAssetSummary,
    ReferenceCandidateSummary,
    ReferenceRunCreate,
    ReferenceRunView,
    ReferencePickerRun,
    ReferenceSourceKind,
    ReferenceVariant,
    ReferenceVariantView,
    ReviewStatus,
    ReferenceRunStatus,
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

DATA_VERSION_EXTERNAL_VARIANT_REHOME = 1
DATA_VERSION_SLICE_REVISION_HISTORY = 2
DATA_VERSION_REFERENCE_PICKER_SCHEMA = 3
DATA_VERSION_REFERENCE_VARIANT_RELATIVE_PATHS = 4
LATEST_DATA_VERSION = DATA_VERSION_REFERENCE_VARIANT_RELATIVE_PATHS


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

    def list_source_recordings(self, project_id: str) -> list[SourceRecordingView]:
        with self._session() as session:
            self._get_batch(session, project_id)
            recordings = session.exec(
                select(SourceRecording)
                .where(SourceRecording.batch_id == project_id)
                .order_by(SourceRecording.parent_recording_id.is_not(None), SourceRecording.id)
            ).all()
            return [self._source_recording_view(recording) for recording in recordings]

    def list_reference_runs(self, project_id: str) -> list[ReferenceRunView]:
        with self._session() as session:
            self._get_batch(session, project_id)
            runs = session.exec(
                select(ReferencePickerRun)
                .where(ReferencePickerRun.project_id == project_id)
                .order_by(ReferencePickerRun.created_at.desc())
            ).all()
            return [self._reference_run_view(run) for run in runs]

    def get_reference_run(self, run_id: str) -> ReferenceRunView:
        with self._session() as session:
            run = session.get(ReferencePickerRun, run_id)
            if run is None:
                raise KeyError(run_id)
            return self._reference_run_view(run)

    def create_reference_run(self, project_id: str, payload: ReferenceRunCreate) -> ReferenceRunView:
        with self._session() as session:
            self._get_batch(session, project_id)
            recording_ids = self._normalize_reference_run_recording_ids(payload.recording_ids)
            if not recording_ids:
                raise ValueError("Reference runs require at least one source recording")
            recordings = [self._get_source_recording(session, recording_id) for recording_id in recording_ids]
            for recording in recordings:
                if recording.batch_id != project_id:
                    raise ValueError("Reference run recordings must belong to the selected project")

            run_id = self._new_id("reference-run")
            artifact_root = self._reference_run_artifact_root(run_id)
            artifact_root.mkdir(parents=True, exist_ok=True)
            config = {
                "recording_ids": recording_ids,
                "mode": self._normalize_reference_run_mode(payload.mode),
                "target_durations": self._normalize_reference_target_durations(
                    payload.target_durations,
                    payload.mode,
                ),
                "candidate_count_cap": max(8, min(int(payload.candidate_count_cap or 60), 200)),
                "overlap_stride_ratio": 0.5,
            }
            run = ReferencePickerRun(
                id=run_id,
                project_id=project_id,
                status=ReferenceRunStatus.QUEUED,
                mode=config["mode"],
                config=config,
                artifact_root=str(artifact_root),
                candidate_count=0,
            )
            session.add(run)
            session.commit()
            return self._reference_run_view(run)

    def start_reference_run_worker(self, run_id: str) -> None:
        worker = threading.Thread(
            target=self.process_reference_run,
            args=(run_id,),
            name=f"reference-run-{run_id}",
            daemon=True,
        )
        worker.start()

    def process_reference_run(self, run_id: str) -> ReferenceRunView:
        with self._session() as session:
            run = session.get(ReferencePickerRun, run_id)
            if run is None:
                raise KeyError(run_id)
            if run.status == ReferenceRunStatus.COMPLETED:
                return self._reference_run_view(run)
            if run.status == ReferenceRunStatus.RUNNING:
                run.error_message = None
            else:
                run.status = ReferenceRunStatus.RUNNING
                run.started_at = utc_now()
                run.error_message = None
            session.add(run)
            session.commit()
            project_id = run.project_id
            config = dict(run.config or {})
            artifact_root = Path(run.artifact_root)

        try:
            candidates = self._build_reference_run_candidates(run_id, project_id, config)
            artifact_root.mkdir(parents=True, exist_ok=True)
            (artifact_root / "config.json").write_text(json.dumps(config, indent=2, sort_keys=True))
            self._write_reference_candidates_artifact(artifact_root, candidates)
            manifest_payload = {
                "run_id": run_id,
                "project_id": project_id,
                "candidate_count": len(candidates),
                "generated_at": utc_now().isoformat(),
            }
            (artifact_root / "manifest.json").write_text(json.dumps(manifest_payload, indent=2, sort_keys=True))

            with self._session() as session:
                run = session.get(ReferencePickerRun, run_id)
                if run is None:
                    raise KeyError(run_id)
                run.status = ReferenceRunStatus.COMPLETED
                run.candidate_count = len(candidates)
                run.completed_at = utc_now()
                run.error_message = None
                session.add(run)
                session.commit()
                return self._reference_run_view(run)
        except Exception as exc:
            with self._session() as session:
                run = session.get(ReferencePickerRun, run_id)
                if run is None:
                    raise
                run.status = ReferenceRunStatus.FAILED
                run.error_message = str(exc)
                run.completed_at = utc_now()
                session.add(run)
                session.commit()
                return self._reference_run_view(run)

    def list_reference_run_candidates(
        self,
        run_id: str,
        offset: int = 0,
        limit: int = 50,
        query: str | None = None,
    ) -> list[ReferenceCandidateSummary]:
        with self._session() as session:
            run = session.get(ReferencePickerRun, run_id)
            if run is None:
                raise KeyError(run_id)
            if run.status != ReferenceRunStatus.COMPLETED:
                return []
        candidates = self._read_reference_run_candidates(Path(run.artifact_root))
        needle = (query or "").strip().lower()
        if needle:
            candidates = [
                candidate
                for candidate in candidates
                if needle in (candidate.transcript_text or "").lower()
                or needle in (candidate.speaker_name or "").lower()
                or needle in (candidate.language or "").lower()
            ]
        start = max(offset, 0)
        end = start + max(min(limit, 200), 1)
        return candidates[start:end]

    def get_reference_candidate_media_path(self, run_id: str, candidate_id: str) -> Path:
        with self._session() as session:
            run = session.get(ReferencePickerRun, run_id)
            if run is None:
                raise KeyError(run_id)
        artifact_root = Path(run.artifact_root)
        candidate = self._get_reference_candidate(artifact_root, candidate_id)
        return self._ensure_reference_candidate_preview(artifact_root, candidate)

    def get_project_slices(self, project_id: str) -> list[SliceSummary]:
        with self._session() as session:
            self._get_batch(session, project_id)
            return [self._to_slice_summary(slice_row) for slice_row in self._get_batch_slice_summaries(session, project_id)]

    def get_slice_detail(self, slice_id: str) -> SliceDetail:
        with self._session() as session:
            return self._get_slice_detail(session, slice_id)

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
            deleted_slice_ids: list[str] = []
            deleted_variant_ids: list[str] = []
            deleted_paths: list[str] = []

            superseded_slices = [
                slice_row
                for slice_row in self._get_all_batch_slices(session, project_id)
                if self._slice_metadata(slice_row).get("is_superseded", False)
            ]
            for slice_row in superseded_slices:
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
            protected_variant_ids = self._get_revision_referenced_variant_ids(
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
            skipped_reference_count=0,
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

    def get_reference_variant_media_path(self, variant_id: str) -> Path:
        with self._session() as session:
            variant = session.get(ReferenceVariant, variant_id)
            if variant is None:
                raise KeyError(variant_id)
            return self._resolve_reference_variant_media_path(variant.file_path)

    def list_reference_assets(self, project_id: str) -> list[ReferenceAssetSummary]:
        with self._session() as session:
            self._get_batch(session, project_id)
            assets = session.exec(
                select(ReferenceAsset)
                .where(ReferenceAsset.project_id == project_id)
                .order_by(ReferenceAsset.updated_at.desc(), ReferenceAsset.created_at.desc())
            ).all()
            return [self._to_reference_asset_summary(session, asset) for asset in assets]

    def get_reference_asset(self, asset_id: str) -> ReferenceAssetDetail:
        with self._session() as session:
            return self._to_reference_asset_detail(session, self._get_reference_asset_row(session, asset_id))

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
                    active_commit_id=None,
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
                session.flush()
                self._append_slice_revision(
                    session,
                    slice_row,
                    edl_operations=[],
                    message="Imported slice baseline",
                )
                created_ids.append(slice_row.id)
            session.commit()
            session.expire_all()
            details = [self._get_slice_detail(session, slice_id) for slice_id in created_ids]
        for created_id in created_ids:
            self._warm_slice_artifacts_for_id(created_id)
        return details

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

    def create_reference_asset_from_slice(
        self,
        payload: ReferenceAssetCreateFromSlice,
    ) -> ReferenceAssetDetail:
        with self._session() as session:
            slice_row = self._get_loaded_slice(session, payload.slice_id)
            source_variant_id = slice_row.active_variant_id
            if source_variant_id is None:
                raise ValueError("Slice has no active variant to save as a reference")

            source_variant = next(
                (variant for variant in slice_row.variants if variant.id == source_variant_id),
                None,
            )
            if source_variant is None:
                raise KeyError(source_variant_id)

            rendered_audio_bytes = self._render_slice_audio_bytes(session, slice_row)
            sample_rate, channels, num_samples = self._wav_metadata(rendered_audio_bytes)
            reference_variant_id = self._new_id("reference-variant")
            storage_key = self._reference_variant_storage_key(reference_variant_id)
            managed_path = self._managed_reference_variant_path(reference_variant_id)
            managed_path.parent.mkdir(parents=True, exist_ok=True)
            managed_path.write_bytes(rendered_audio_bytes)
            self._validate_audio_asset(managed_path, sample_rate, channels, num_samples)

            transcript_text = self._transcript_text(slice_row.transcript).strip() or None
            now = utc_now()
            asset = ReferenceAsset(
                id=self._new_id("reference"),
                project_id=slice_row.source_recording.batch_id,
                name=payload.name or self._default_reference_asset_name(slice_row),
                status=ReferenceAssetStatus.ACTIVE,
                transcript_text=transcript_text,
                speaker_name=self._speaker_name(slice_row),
                language=self._language(slice_row),
                mood_label=payload.mood_label,
                created_from_run_id=None,
                created_from_candidate_id=None,
                model_metadata={
                    "origin": "slice-variant",
                    "source_slice_id": slice_row.id,
                    "source_audio_variant_id": source_variant.id,
                    "source_edit_commit_id": slice_row.active_commit_id,
                    "source_original_start_seconds": self._slice_metadata(slice_row).get("original_start_time"),
                    "source_original_end_seconds": self._slice_metadata(slice_row).get("original_end_time"),
                },
                created_at=now,
                updated_at=now,
            )
            variant = ReferenceVariant(
                id=reference_variant_id,
                reference_asset_id=asset.id,
                source_kind=ReferenceSourceKind.SLICE_VARIANT,
                source_recording_id=slice_row.source_recording_id,
                source_slice_id=slice_row.id,
                source_audio_variant_id=source_variant.id,
                # Label-derived saves preserve lineage back to the upstream slice and variant,
                # but the copied media may already reflect EDL edits or processing, so exact
                # source span bounds are not authoritative here.
                source_start_seconds=None,
                source_end_seconds=None,
                file_path=storage_key,
                is_original=True,
                generator_model=source_variant.generator_model,
                sample_rate=sample_rate,
                num_samples=num_samples,
                model_metadata={
                    "origin": "label-rendered-slice",
                    "source_edit_commit_id": slice_row.active_commit_id,
                },
            )
            self._validate_reference_variant_provenance(session, asset, variant)
            session.add(asset)
            session.add(variant)
            session.flush()
            asset.active_variant_id = variant.id
            session.add(asset)
            self._validate_reference_asset_integrity(session, asset, [variant])
            session.commit()
            return self._to_reference_asset_detail(session, self._get_reference_asset_row(session, asset.id))

    def create_reference_asset_from_candidate(
        self,
        payload: ReferenceAssetCreateFromCandidate,
    ) -> ReferenceAssetDetail:
        with self._session() as session:
            run = session.get(ReferencePickerRun, payload.run_id)
            if run is None:
                raise KeyError(payload.run_id)
            if run.status != ReferenceRunStatus.COMPLETED:
                raise ValueError("Reference run must be completed before promotion")

            candidate = self._get_reference_candidate(Path(run.artifact_root), payload.candidate_id)
            if candidate.source_media_kind != ReferenceSourceKind.SOURCE_RECORDING:
                raise ValueError("Phase 1 only supports promotion from source-recording candidates")
            if candidate.source_recording_id is None:
                raise ValueError("Candidate is missing its source recording")

            source_recording = self._get_source_recording(session, candidate.source_recording_id)
            if source_recording.batch_id != run.project_id:
                raise ValueError("Candidate source recording does not belong to the run project")

            rendered_audio_bytes = self._crop_source_recording_audio_bytes(
                source_recording,
                candidate.source_start_seconds,
                candidate.source_end_seconds,
            )
            sample_rate, channels, num_samples = self._wav_metadata(rendered_audio_bytes)
            reference_variant_id = self._new_id("reference-variant")
            storage_key = self._reference_variant_storage_key(reference_variant_id)
            managed_path = self._managed_reference_variant_path(reference_variant_id)
            managed_path.parent.mkdir(parents=True, exist_ok=True)
            managed_path.write_bytes(rendered_audio_bytes)
            self._validate_audio_asset(managed_path, sample_rate, channels, num_samples)

            now = utc_now()
            asset = ReferenceAsset(
                id=self._new_id("reference"),
                project_id=run.project_id,
                name=payload.name or self._default_reference_asset_name_from_candidate(candidate),
                status=ReferenceAssetStatus.ACTIVE,
                transcript_text=(candidate.transcript_text or "").strip() or None,
                speaker_name=(candidate.speaker_name or "").strip() or None,
                language=(candidate.language or "").strip() or None,
                mood_label=payload.mood_label,
                created_from_run_id=run.id,
                created_from_candidate_id=candidate.candidate_id,
                model_metadata={
                    "origin": "reference-picker-candidate",
                    "source_media_kind": candidate.source_media_kind.value,
                    "default_scores": candidate.default_scores,
                    "risk_flags": candidate.risk_flags,
                },
                created_at=now,
                updated_at=now,
            )
            variant = ReferenceVariant(
                id=reference_variant_id,
                reference_asset_id=asset.id,
                source_kind=ReferenceSourceKind.SOURCE_RECORDING,
                source_recording_id=source_recording.id,
                source_start_seconds=candidate.source_start_seconds,
                source_end_seconds=candidate.source_end_seconds,
                file_path=storage_key,
                is_original=True,
                generator_model="reference-picker",
                sample_rate=sample_rate,
                num_samples=num_samples,
                model_metadata={
                    "run_id": run.id,
                    "candidate_id": candidate.candidate_id,
                },
            )
            self._validate_reference_variant_provenance(session, asset, variant)
            session.add(asset)
            session.add(variant)
            session.flush()
            asset.active_variant_id = variant.id
            session.add(asset)
            self._validate_reference_asset_integrity(session, asset, [variant])
            session.commit()
            return self._to_reference_asset_detail(session, self._get_reference_asset_row(session, asset.id))

    def _session(self) -> Session:
        return Session(self.engine, expire_on_commit=False)

    def _run_data_migrations(self) -> None:
        version = self._get_data_version()
        migrated_reference_picker_schema = False
        if version < DATA_VERSION_EXTERNAL_VARIANT_REHOME:
            self._migrate_external_variant_media()
            self._set_data_version(DATA_VERSION_EXTERNAL_VARIANT_REHOME)
            version = DATA_VERSION_EXTERNAL_VARIANT_REHOME
        if version < DATA_VERSION_SLICE_REVISION_HISTORY:
            self._migrate_legacy_slice_revision_history()
            self._set_data_version(DATA_VERSION_SLICE_REVISION_HISTORY)
            version = DATA_VERSION_SLICE_REVISION_HISTORY
        if version < DATA_VERSION_REFERENCE_PICKER_SCHEMA:
            self._migrate_reference_picker_schema()
            self._set_data_version(DATA_VERSION_REFERENCE_PICKER_SCHEMA)
            version = DATA_VERSION_REFERENCE_PICKER_SCHEMA
            migrated_reference_picker_schema = True
        if not migrated_reference_picker_schema and self._table_exists("referenceasset_legacy"):
            self._migrate_reference_picker_legacy_rows()
        if version < DATA_VERSION_REFERENCE_VARIANT_RELATIVE_PATHS:
            self._migrate_reference_variant_storage_keys()
            self._set_data_version(DATA_VERSION_REFERENCE_VARIANT_RELATIVE_PATHS)

    def _get_data_version(self) -> int:
        with self.engine.begin() as connection:
            raw = connection.exec_driver_sql("PRAGMA user_version").scalar()
        return int(raw or 0)

    def _set_data_version(self, version: int) -> None:
        with self.engine.begin() as connection:
            connection.exec_driver_sql(f"PRAGMA user_version = {version}")

    def _table_exists(self, table_name: str) -> bool:
        with self.engine.begin() as connection:
            return bool(
                connection.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
                    (table_name,),
                ).fetchone()
            )

    def _table_columns(self, connection: Any, table_name: str) -> set[str]:
        return {
            row[1]
            for row in connection.exec_driver_sql(f"PRAGMA table_info({table_name})").fetchall()
        }

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

    def _migrate_reference_picker_schema(self) -> None:
        with self.engine.begin() as connection:
            columns = self._table_columns(connection, "referenceasset")
            if columns and "project_id" not in columns:
                connection.exec_driver_sql("ALTER TABLE referenceasset RENAME TO referenceasset_legacy")

        SQLModel.metadata.create_all(self.engine)
        self._migrate_reference_picker_legacy_rows()

    def _migrate_reference_picker_legacy_rows(self) -> None:
        if not self._table_exists("referenceasset_legacy"):
            return
        unresolved_rows: list[dict[str, Any]] = []
        with self.engine.begin() as connection:
            legacy_rows = [
                dict(row._mapping)
                for row in connection.exec_driver_sql(
                    "SELECT id, name, audio_variant_id, created_at FROM referenceasset_legacy"
                ).fetchall()
            ]

        with self._session() as session:
            for row in legacy_rows:
                asset_id = str(row["id"])
                if session.get(ReferenceAsset, asset_id) is not None:
                    continue

                legacy_variant_id = str(row["audio_variant_id"])
                source_variant = session.get(AudioVariant, legacy_variant_id)
                if source_variant is None:
                    unresolved_rows.append(
                        {
                            "legacy_asset_id": asset_id,
                            "name": str(row["name"]),
                            "audio_variant_id": legacy_variant_id,
                            "reason": "missing_audio_variant",
                        }
                    )
                    continue

                try:
                    slice_row = self._get_loaded_slice(session, source_variant.slice_id)
                except KeyError:
                    unresolved_rows.append(
                        {
                            "legacy_asset_id": asset_id,
                            "name": str(row["name"]),
                            "audio_variant_id": legacy_variant_id,
                            "reason": "missing_slice",
                            "source_slice_id": source_variant.slice_id,
                        }
                    )
                    continue

                created_at = self._coerce_datetime(row["created_at"])
                now = created_at
                asset = ReferenceAsset(
                    id=asset_id,
                    project_id=slice_row.source_recording.batch_id,
                    name=str(row["name"]),
                    status=ReferenceAssetStatus.ACTIVE,
                    transcript_text=self._transcript_text(slice_row.transcript).strip() or None,
                    speaker_name=self._speaker_name(slice_row),
                    language=self._language(slice_row),
                    model_metadata={
                        "origin": "legacy-referenceasset",
                        "legacy_audio_variant_id": legacy_variant_id,
                    },
                    created_at=created_at,
                    updated_at=now,
                )
                session.add(asset)
                session.flush()

                try:
                    source_path = self._get_variant_audio_path(
                        source_variant,
                        slice_row.source_recording.num_channels if slice_row.source_recording is not None else None,
                    )
                except (FileNotFoundError, ValueError):
                    asset.status = ReferenceAssetStatus.ARCHIVED
                    asset.model_metadata = {
                        **(asset.model_metadata or {}),
                        "migration_warning": "source_variant_media_missing",
                    }
                    session.add(asset)
                    continue

                reference_variant_id = self._new_id("reference-variant")
                storage_key = self._reference_variant_storage_key(reference_variant_id)
                managed_path = self._managed_reference_variant_path(reference_variant_id)
                managed_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source_path, managed_path)
                channels, _sample_width, sample_rate, num_samples = self._read_pcm_wav_header(managed_path)
                self._validate_audio_asset(managed_path, sample_rate, channels, num_samples)
                variant = ReferenceVariant(
                    id=reference_variant_id,
                    reference_asset_id=asset.id,
                    source_kind=ReferenceSourceKind.SLICE_VARIANT,
                    source_recording_id=slice_row.source_recording_id,
                    source_slice_id=slice_row.id,
                    source_audio_variant_id=source_variant.id,
                    source_start_seconds=None,
                    source_end_seconds=None,
                    file_path=storage_key,
                    is_original=True,
                    generator_model=source_variant.generator_model,
                    sample_rate=sample_rate,
                    num_samples=num_samples,
                    created_at=created_at,
                    model_metadata={
                        "origin": "legacy-referenceasset",
                        "legacy_audio_variant_id": legacy_variant_id,
                    },
                )
                self._validate_reference_variant_provenance(session, asset, variant)
                session.add(variant)
                session.flush()
                asset.active_variant_id = variant.id
                session.add(asset)
                self._validate_reference_asset_integrity(session, asset, [variant])

            session.commit()

        report_path = self._write_reference_asset_migration_report(unresolved_rows) if unresolved_rows else None
        if report_path is None:
            with self.engine.begin() as connection:
                connection.exec_driver_sql("DROP TABLE IF EXISTS referenceasset_legacy")

    def _migrate_reference_variant_storage_keys(self) -> None:
        with self._session() as session:
            variants = session.exec(select(ReferenceVariant)).all()
            changed = False
            for variant in variants:
                normalized_key = self._normalize_reference_variant_storage_key(variant.file_path, variant.id)
                if normalized_key != variant.file_path:
                    variant.file_path = normalized_key
                    session.add(variant)
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

    def _coerce_datetime(self, value: datetime | str | None) -> datetime:
        if isinstance(value, datetime):
            return self._as_utc(value)
        if isinstance(value, str) and value.strip():
            try:
                return self._as_utc(datetime.fromisoformat(value))
            except ValueError:
                pass
        return utc_now()

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
            duration_seconds=round(recording.duration_s, 2),
        )

    def _reference_variant_view(self, variant: ReferenceVariant | None) -> ReferenceVariantView | None:
        if variant is None:
            return None
        return ReferenceVariantView(
            id=variant.id,
            reference_asset_id=variant.reference_asset_id,
            source_kind=variant.source_kind,
            source_recording_id=variant.source_recording_id,
            source_slice_id=variant.source_slice_id,
            source_audio_variant_id=variant.source_audio_variant_id,
            source_reference_variant_id=variant.source_reference_variant_id,
            source_start_seconds=variant.source_start_seconds,
            source_end_seconds=variant.source_end_seconds,
            is_original=variant.is_original,
            generator_model=variant.generator_model,
            sample_rate=variant.sample_rate,
            num_samples=variant.num_samples,
            deleted=variant.deleted,
            created_at=self._as_utc(variant.created_at),
        )

    def _get_reference_asset_row(self, session: Session, asset_id: str) -> ReferenceAsset:
        asset = session.get(ReferenceAsset, asset_id)
        if asset is None:
            raise KeyError(asset_id)
        return asset

    def _get_reference_variants(self, session: Session, asset_id: str) -> list[ReferenceVariant]:
        return session.exec(
            select(ReferenceVariant)
            .where(ReferenceVariant.reference_asset_id == asset_id)
            .order_by(ReferenceVariant.created_at)
        ).all()

    def _validate_reference_variant_provenance(
        self,
        session: Session,
        asset: ReferenceAsset,
        variant: ReferenceVariant,
    ) -> None:
        if variant.reference_asset_id != asset.id:
            raise ValueError("Reference variant does not belong to the specified asset")

        if variant.source_kind == ReferenceSourceKind.SOURCE_RECORDING:
            if variant.source_recording_id is None:
                raise ValueError("Source-recording variants require source_recording_id")
            if any(
                value is not None
                for value in [
                    variant.source_slice_id,
                    variant.source_audio_variant_id,
                    variant.source_reference_variant_id,
                ]
            ):
                raise ValueError("Source-recording variants must not mix other source kinds")
            recording = self._get_source_recording(session, variant.source_recording_id)
            if recording.batch_id != asset.project_id:
                raise ValueError("Reference variant source recording does not belong to the asset project")
        elif variant.source_kind == ReferenceSourceKind.SLICE_VARIANT:
            if variant.source_slice_id is None or variant.source_audio_variant_id is None:
                raise ValueError("Slice-derived reference variants require slice and audio variant ids")
            if variant.source_reference_variant_id is not None:
                raise ValueError("Slice-derived reference variants cannot also reference a reference variant")
            source_slice = self._get_loaded_slice(session, variant.source_slice_id)
            source_variant = session.get(AudioVariant, variant.source_audio_variant_id)
            if source_variant is None:
                raise KeyError(variant.source_audio_variant_id)
            if source_variant.slice_id != source_slice.id:
                raise ValueError("Source audio variant does not belong to the referenced slice")
            if source_slice.source_recording.batch_id != asset.project_id:
                raise ValueError("Reference variant slice does not belong to the asset project")
            if (
                variant.source_recording_id is not None
                and variant.source_recording_id != source_slice.source_recording_id
            ):
                raise ValueError("Reference variant source recording does not match the referenced slice")
        elif variant.source_kind == ReferenceSourceKind.REFERENCE_VARIANT:
            if variant.source_reference_variant_id is None:
                raise ValueError("Reference-derived variants require source_reference_variant_id")
            if any(
                value is not None
                for value in [
                    variant.source_recording_id,
                    variant.source_slice_id,
                    variant.source_audio_variant_id,
                ]
            ):
                raise ValueError("Reference-derived variants must not mix other source kinds")
            source_reference_variant = session.get(ReferenceVariant, variant.source_reference_variant_id)
            if source_reference_variant is None:
                raise KeyError(variant.source_reference_variant_id)
            parent_asset = session.get(ReferenceAsset, source_reference_variant.reference_asset_id)
            if parent_asset is None:
                raise KeyError(source_reference_variant.reference_asset_id)
            if parent_asset.project_id != asset.project_id:
                raise ValueError("Reference-derived variants must stay within the same project")
        else:
            raise ValueError(f"Unsupported reference source kind: {variant.source_kind}")

        if (variant.source_start_seconds is None) != (variant.source_end_seconds is None):
            raise ValueError("Reference source bounds must be stored as a complete pair or both null")
        if variant.source_start_seconds is not None and variant.source_end_seconds is not None:
            if not math.isfinite(variant.source_start_seconds) or not math.isfinite(variant.source_end_seconds):
                raise ValueError("Reference source bounds must be finite")
            if variant.source_end_seconds <= variant.source_start_seconds:
                raise ValueError("Reference source bounds must have positive duration")

    def _validate_reference_asset_integrity(
        self,
        session: Session,
        asset: ReferenceAsset,
        variants: list[ReferenceVariant] | None = None,
    ) -> None:
        if asset.active_variant_id is None:
            return
        if variants is None:
            variants = self._get_reference_variants(session, asset.id)
        if not any(variant.id == asset.active_variant_id for variant in variants):
            raise ValueError("Reference asset active variant must belong to the asset")

    def _to_reference_asset_summary(
        self,
        session: Session,
        asset: ReferenceAsset,
    ) -> ReferenceAssetSummary:
        variants = self._get_reference_variants(session, asset.id)
        self._validate_reference_asset_integrity(session, asset, variants)
        active_variant = next(
            (variant for variant in variants if variant.id == asset.active_variant_id),
            None,
        )
        return ReferenceAssetSummary(
            id=asset.id,
            project_id=asset.project_id,
            name=asset.name,
            status=asset.status,
            transcript_text=asset.transcript_text,
            speaker_name=asset.speaker_name,
            language=asset.language,
            mood_label=asset.mood_label,
            active_variant_id=asset.active_variant_id,
            created_from_run_id=asset.created_from_run_id,
            created_from_candidate_id=asset.created_from_candidate_id,
            source_slice_id=self._reference_asset_metadata(asset).get("source_slice_id"),
            source_audio_variant_id=self._reference_asset_metadata(asset).get("source_audio_variant_id"),
            source_edit_commit_id=self._reference_asset_metadata(asset).get("source_edit_commit_id"),
            created_at=self._as_utc(asset.created_at),
            updated_at=self._as_utc(asset.updated_at),
            active_variant=self._reference_variant_view(active_variant),
        )

    def _to_reference_asset_detail(
        self,
        session: Session,
        asset: ReferenceAsset,
    ) -> ReferenceAssetDetail:
        variants = self._get_reference_variants(session, asset.id)
        for variant in variants:
            self._validate_reference_variant_provenance(session, asset, variant)
        summary = self._to_reference_asset_summary(session, asset)
        return ReferenceAssetDetail(
            **summary.model_dump(mode="json"),
            notes=asset.notes,
            favorite_rank=asset.favorite_rank,
            model_metadata=asset.model_metadata,
            variants=[self._reference_variant_view(variant) for variant in variants if variant is not None],
        )

    def _reference_run_view(self, run: ReferencePickerRun) -> ReferenceRunView:
        return ReferenceRunView(
            id=run.id,
            project_id=run.project_id,
            status=run.status,
            mode=run.mode,
            config=run.config,
            candidate_count=run.candidate_count,
            error_message=run.error_message,
            created_at=self._as_utc(run.created_at),
            started_at=self._as_utc(run.started_at) if run.started_at is not None else None,
            completed_at=self._as_utc(run.completed_at) if run.completed_at is not None else None,
        )

    def _normalize_reference_run_recording_ids(self, recording_ids: list[str]) -> list[str]:
        normalized: list[str] = []
        for recording_id in recording_ids:
            cleaned = recording_id.strip()
            if cleaned and cleaned not in normalized:
                normalized.append(cleaned)
        return normalized

    def _normalize_reference_run_mode(self, raw_mode: str | None) -> str:
        normalized = (raw_mode or "both").strip().lower()
        if normalized not in {"zero_shot", "finetune", "both"}:
            return "both"
        return normalized

    def _normalize_reference_target_durations(
        self,
        raw_durations: list[float] | None,
        raw_mode: str | None,
    ) -> list[float]:
        if raw_durations:
            normalized = sorted(
                {
                    round(float(duration), 2)
                    for duration in raw_durations
                    if duration is not None and math.isfinite(float(duration)) and float(duration) >= 1.0
                }
            )
            if normalized:
                return normalized
        mode = self._normalize_reference_run_mode(raw_mode)
        if mode == "zero_shot":
            return [3.0, 4.5, 6.0]
        if mode == "finetune":
            return [2.5, 4.0, 5.0]
        return [3.0, 4.5, 6.0]

    def _reference_runs_root(self) -> Path:
        return (self.db_path.parent / "reference-picker" / "runs").resolve()

    def _reference_run_artifact_root(self, run_id: str) -> Path:
        safe_run_id = self._validate_managed_media_id(run_id)
        return (self._reference_runs_root() / safe_run_id).resolve()

    def _reference_run_candidates_path(self, artifact_root: Path) -> Path:
        return artifact_root / "candidates.jsonl"

    def _reference_candidate_preview_path(self, artifact_root: Path, candidate_id: str) -> Path:
        safe_candidate_id = self._validate_managed_media_id(candidate_id)
        return artifact_root / "preview-cache" / f"{safe_candidate_id}.wav"

    def _write_reference_candidates_artifact(
        self,
        artifact_root: Path,
        candidates: list[ReferenceCandidateSummary],
    ) -> None:
        path = self._reference_run_candidates_path(artifact_root)
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [candidate.model_dump_json() for candidate in candidates]
        path.write_text("\n".join(lines))

    def _read_reference_run_candidates(self, artifact_root: Path) -> list[ReferenceCandidateSummary]:
        path = self._reference_run_candidates_path(artifact_root)
        if not path.exists():
            return []
        candidates: list[ReferenceCandidateSummary] = []
        for raw_line in path.read_text().splitlines():
            line = raw_line.strip()
            if not line:
                continue
            candidates.append(ReferenceCandidateSummary.model_validate_json(line))
        return candidates

    def _get_reference_candidate(self, artifact_root: Path, candidate_id: str) -> ReferenceCandidateSummary:
        for candidate in self._read_reference_run_candidates(artifact_root):
            if candidate.candidate_id == candidate_id:
                return candidate
        raise KeyError(candidate_id)

    def _build_reference_run_candidates(
        self,
        run_id: str,
        project_id: str,
        config: dict[str, Any],
    ) -> list[ReferenceCandidateSummary]:
        recording_ids = self._normalize_reference_run_recording_ids(list(config.get("recording_ids") or []))
        durations = self._normalize_reference_target_durations(
            config.get("target_durations"),
            str(config.get("mode") or "both"),
        )
        candidate_count_cap = max(8, min(int(config.get("candidate_count_cap") or 60), 200))
        stride_ratio = float(config.get("overlap_stride_ratio") or 0.5)
        stride_ratio = max(0.2, min(stride_ratio, 0.9))

        with self._session() as session:
            recordings = [self._get_source_recording(session, recording_id) for recording_id in recording_ids]
            recording_slices: dict[str, list[Slice]] = {}
            for recording in recordings:
                if recording.batch_id != project_id:
                    raise ValueError("Reference run recordings must belong to the selected project")
                recording_slices[recording.id] = self._get_recording_slices(session, recording.id)

        raw_candidates: list[ReferenceCandidateSummary] = []
        for recording in recordings:
            total_duration = recording.duration_s
            overlapping_slices = recording_slices.get(recording.id, [])
            speech_regions = self._reference_candidate_regions(recording)
            for window_seconds in durations:
                for start_seconds, end_seconds in self._candidate_time_windows(
                    speech_regions,
                    window_seconds,
                    stride_ratio,
                ):
                    if end_seconds - start_seconds < 1.0:
                        continue
                    transcript_text, speaker_name, language = self._candidate_context_from_slices(
                        overlapping_slices,
                        start_seconds,
                        end_seconds,
                    )
                    risk_flags = self._reference_candidate_risk_flags(
                        total_duration,
                        start_seconds,
                        end_seconds,
                        transcript_text,
                    )
                    default_scores = self._reference_candidate_default_scores(
                        total_duration,
                        start_seconds,
                        end_seconds,
                        transcript_text,
                        risk_flags,
                    )
                    raw_candidates.append(
                        ReferenceCandidateSummary(
                            candidate_id=self._reference_candidate_id(
                                recording.id,
                                start_seconds,
                                end_seconds,
                                f"{window_seconds:.2f}",
                            ),
                            run_id=run_id,
                            source_media_kind=ReferenceSourceKind.SOURCE_RECORDING,
                            source_recording_id=recording.id,
                            source_variant_id=None,
                            source_start_seconds=round(start_seconds, 3),
                            source_end_seconds=round(end_seconds, 3),
                            duration_seconds=round(end_seconds - start_seconds, 3),
                            transcript_text=transcript_text,
                            speaker_name=speaker_name,
                            language=language,
                            risk_flags=risk_flags,
                            default_scores=default_scores,
                        )
                    )

        deduped = self._dedupe_reference_candidates(raw_candidates)
        ranked = sorted(
            deduped,
            key=lambda candidate: (
                -float(candidate.default_scores.get("both", 0.0)),
                candidate.source_recording_id or "",
                candidate.source_start_seconds,
            ),
        )[:candidate_count_cap]
        return ranked

    def _get_recording_slices(self, session: Session, recording_id: str) -> list[Slice]:
        slices = session.exec(
            select(Slice)
            .where(Slice.source_recording_id == recording_id)
            .options(
                selectinload(Slice.source_recording),
                selectinload(Slice.transcript),
                selectinload(Slice.tags),
                selectinload(Slice.active_variant),
                selectinload(Slice.active_commit),
            )
        ).all()
        return [
            slice_row
            for slice_row in slices
            if not self._slice_metadata(slice_row).get("is_superseded", False)
        ]

    def _reference_candidate_regions(self, recording: SourceRecording) -> list[tuple[float, float]]:
        source_path = self._get_source_recording_audio_path(recording)
        audio_bytes = source_path.read_bytes()
        detected = self._speech_regions_from_audio_bytes(audio_bytes)
        if detected:
            return detected
        duration_seconds = round(recording.duration_s, 3)
        if duration_seconds <= 0:
            return []
        return [(0.0, duration_seconds)]

    def _candidate_time_windows(
        self,
        regions: list[tuple[float, float]],
        window_seconds: float,
        stride_ratio: float,
    ) -> list[tuple[float, float]]:
        window_seconds = max(float(window_seconds), 0.1)
        windows: list[tuple[float, float]] = []
        for region_start, region_end in regions:
            region_duration = max(region_end - region_start, 0.0)
            if region_duration <= 0:
                continue
            if region_duration <= window_seconds:
                windows.append((round(region_start, 3), round(region_end, 3)))
                continue
            stride_seconds = max(window_seconds * stride_ratio, 0.25)
            start_seconds = region_start
            max_start = max(region_end - window_seconds, region_start)
            while start_seconds < max_start:
                end_seconds = min(start_seconds + window_seconds, region_end)
                windows.append((round(start_seconds, 3), round(end_seconds, 3)))
                start_seconds += stride_seconds
            windows.append((round(max_start, 3), round(region_end, 3)))
        unique: list[tuple[float, float]] = []
        seen: set[tuple[float, float]] = set()
        for window in windows:
            if window not in seen:
                seen.add(window)
                unique.append(window)
        return unique

    def _speech_regions_from_audio_bytes(self, audio_bytes: bytes) -> list[tuple[float, float]]:
        channels, sample_width, sample_rate, frame_count, raw = self._read_pcm_wav(audio_bytes)
        if frame_count <= 0:
            return []

        bytes_per_frame = max(channels * sample_width, 1)
        window_frames = max(int(sample_rate * 0.03), 1)
        hop_frames = max(int(sample_rate * 0.02), 1)
        rms_windows: list[tuple[float, float, float]] = []
        for start_frame in range(0, frame_count, hop_frames):
            end_frame = min(start_frame + window_frames, frame_count)
            chunk = raw[start_frame * bytes_per_frame : end_frame * bytes_per_frame]
            if not chunk:
                continue
            rms = audioop.rms(chunk, sample_width) / 32767.0
            rms_windows.append((start_frame / sample_rate, end_frame / sample_rate, rms))

        if not rms_windows:
            return []

        rms_values = sorted(window[2] for window in rms_windows)
        peak = rms_values[-1]
        if peak <= 0.001:
            return []

        noise_floor = rms_values[min(int(len(rms_values) * 0.2), len(rms_values) - 1)]
        median = rms_values[len(rms_values) // 2]
        threshold = max(noise_floor * 2.2, median * 1.35, peak * 0.18, 0.012)

        merged: list[tuple[float, float]] = []
        current_start: float | None = None
        current_end: float | None = None
        max_gap_seconds = 0.18
        for start_seconds, end_seconds, rms in rms_windows:
            if rms >= threshold:
                if current_start is None:
                    current_start = start_seconds
                    current_end = end_seconds
                elif start_seconds - (current_end or start_seconds) <= max_gap_seconds:
                    current_end = end_seconds
                else:
                    merged.append((current_start, current_end or end_seconds))
                    current_start = start_seconds
                    current_end = end_seconds
            elif current_start is not None and current_end is not None and start_seconds - current_end > max_gap_seconds:
                merged.append((current_start, current_end))
                current_start = None
                current_end = None
        if current_start is not None and current_end is not None:
            merged.append((current_start, current_end))

        padded: list[tuple[float, float]] = []
        total_duration = frame_count / sample_rate
        for start_seconds, end_seconds in merged:
            padded_start = max(start_seconds - 0.08, 0.0)
            padded_end = min(end_seconds + 0.12, total_duration)
            if padded_end - padded_start >= 0.35:
                padded.append((round(padded_start, 3), round(padded_end, 3)))

        if not padded:
            return []

        normalized: list[tuple[float, float]] = []
        for start_seconds, end_seconds in sorted(padded):
            if not normalized:
                normalized.append((start_seconds, end_seconds))
                continue
            previous_start, previous_end = normalized[-1]
            if start_seconds <= previous_end + 0.12:
                normalized[-1] = (previous_start, max(previous_end, end_seconds))
            else:
                normalized.append((start_seconds, end_seconds))
        return normalized

    def _candidate_context_from_slices(
        self,
        slices: list[Slice],
        start_seconds: float,
        end_seconds: float,
    ) -> tuple[str | None, str | None, str | None]:
        overlapping: list[tuple[float, Slice]] = []
        for slice_row in slices:
            metadata = self._slice_metadata(slice_row)
            slice_start = float(metadata.get("original_start_time", 0.0))
            slice_end = float(metadata.get("original_end_time", slice_start + self._slice_duration(slice_row)))
            overlap = min(end_seconds, slice_end) - max(start_seconds, slice_start)
            if overlap > 0:
                overlapping.append((overlap, slice_row))
        if not overlapping:
            return None, None, None
        overlapping.sort(key=lambda item: (-item[0], item[1].id))
        transcript_parts: list[str] = []
        for _overlap, slice_row in overlapping[:2]:
            text = self._transcript_text(slice_row.transcript).strip()
            if text and text not in transcript_parts:
                transcript_parts.append(text)
        primary = overlapping[0][1]
        transcript_text = " ".join(transcript_parts).strip() or None
        return transcript_text, self._speaker_name(primary), self._language(primary)

    def _reference_candidate_risk_flags(
        self,
        total_duration: float,
        start_seconds: float,
        end_seconds: float,
        transcript_text: str | None,
    ) -> list[str]:
        flags: list[str] = []
        if start_seconds <= 0.05:
            flags.append("starts_at_recording_edge")
        if end_seconds >= max(total_duration - 0.05, 0.0):
            flags.append("ends_at_recording_edge")
        if not transcript_text:
            flags.append("missing_transcript_context")
        return flags

    def _reference_candidate_default_scores(
        self,
        total_duration: float,
        start_seconds: float,
        end_seconds: float,
        transcript_text: str | None,
        risk_flags: list[str],
    ) -> dict[str, float]:
        duration = max(end_seconds - start_seconds, 0.001)
        center = start_seconds + duration / 2
        midpoint = total_duration / 2 if total_duration > 0 else center
        normalized_center_distance = abs(center - midpoint) / max(total_duration / 2, 1.0)
        transcript_bonus = 0.16 if transcript_text else 0.0
        edge_penalty = 0.06 * len([flag for flag in risk_flags if "edge" in flag])
        context_penalty = 0.08 if "missing_transcript_context" in risk_flags else 0.0
        duration_bias = 0.1 - abs(duration - 4.5) * 0.02
        base = 0.7 + transcript_bonus + duration_bias - (normalized_center_distance * 0.1) - edge_penalty - context_penalty
        zero_shot = round(base - 0.02 * duration, 4)
        finetune = round(base + 0.015 * duration, 4)
        both = round((zero_shot + finetune) / 2, 4)
        overall = round(both, 4)
        return {
            "overall": overall,
            "zero_shot": zero_shot,
            "finetune": finetune,
            "both": both,
        }

    def _reference_candidate_id(
        self,
        source_media_id: str,
        start_seconds: float,
        end_seconds: float,
        family_label: str,
    ) -> str:
        fingerprint = hashlib.sha1(
            f"{source_media_id}:{start_seconds:.3f}:{end_seconds:.3f}:{family_label}".encode("utf-8")
        ).hexdigest()[:16]
        return f"cand-{fingerprint}"

    def _dedupe_reference_candidates(
        self,
        candidates: list[ReferenceCandidateSummary],
    ) -> list[ReferenceCandidateSummary]:
        kept: list[ReferenceCandidateSummary] = []
        for candidate in sorted(
            candidates,
            key=lambda item: (
                -(item.default_scores.get("both", 0.0)),
                item.source_recording_id or "",
                item.source_start_seconds,
            ),
        ):
            should_skip = False
            for existing in kept:
                if candidate.source_recording_id != existing.source_recording_id:
                    continue
                if self._time_overlap_ratio(candidate, existing) >= 0.82:
                    should_skip = True
                    break
            if not should_skip:
                kept.append(candidate)
        return kept

    def _time_overlap_ratio(
        self,
        first: ReferenceCandidateSummary,
        second: ReferenceCandidateSummary,
    ) -> float:
        overlap = min(first.source_end_seconds, second.source_end_seconds) - max(
            first.source_start_seconds,
            second.source_start_seconds,
        )
        if overlap <= 0:
            return 0.0
        first_duration = max(first.duration_seconds, 0.001)
        second_duration = max(second.duration_seconds, 0.001)
        return overlap / min(first_duration, second_duration)

    def _ensure_reference_candidate_preview(
        self,
        artifact_root: Path,
        candidate: ReferenceCandidateSummary,
    ) -> Path:
        preview_path = self._reference_candidate_preview_path(artifact_root, candidate.candidate_id)
        if preview_path.exists():
            return preview_path
        if candidate.source_media_kind != ReferenceSourceKind.SOURCE_RECORDING or candidate.source_recording_id is None:
            raise ValueError("Phase 1 preview only supports source-recording candidates")
        with self._session() as session:
            recording = self._get_source_recording(session, candidate.source_recording_id)
        preview_bytes = self._crop_source_recording_audio_bytes(
            recording,
            candidate.source_start_seconds,
            candidate.source_end_seconds,
        )
        preview_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = preview_path.with_name(f"{preview_path.name}.tmp.{uuid4().hex}")
        try:
            temp_path.write_bytes(preview_bytes)
            os.replace(temp_path, preview_path)
        finally:
            if temp_path.exists():
                temp_path.unlink()
        return preview_path

    def _default_reference_asset_name_from_candidate(
        self,
        candidate: ReferenceCandidateSummary,
    ) -> str:
        transcript_text = (candidate.transcript_text or "").strip()
        if transcript_text:
            words = transcript_text.split()
            preview = " ".join(words[:6]).strip()
            if len(words) > 6:
                preview = f"{preview}..."
            return preview
        speaker = (candidate.speaker_name or "reference").strip() or "reference"
        return f"{speaker} {candidate.source_start_seconds:.2f}-{candidate.source_end_seconds:.2f}s"

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

    def _get_source_recording_audio_path(self, recording: SourceRecording) -> Path:
        path = Path(recording.file_path).expanduser().resolve(strict=False)
        self._validate_audio_asset(path, recording.sample_rate, recording.num_channels, recording.num_samples)
        return path

    def _crop_source_recording_audio_bytes(
        self,
        recording: SourceRecording,
        start_seconds: float,
        end_seconds: float,
    ) -> bytes:
        duration_seconds = recording.duration_s
        if not math.isfinite(start_seconds) or not math.isfinite(end_seconds):
            raise ValueError("Candidate bounds must be finite")
        start_seconds = max(float(start_seconds), 0.0)
        end_seconds = min(float(end_seconds), duration_seconds)
        if end_seconds <= start_seconds:
            raise ValueError("Candidate bounds must have positive duration")
        source_path = self._get_source_recording_audio_path(recording)
        return self._apply_edl_to_wav_bytes(
            source_path.read_bytes(),
            [
                {
                    "op": "crop",
                    "range": {
                        "start_seconds": start_seconds,
                        "end_seconds": end_seconds,
                    },
                }
            ],
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

    def _reference_variant_storage_key(self, variant_id: str) -> str:
        safe_identifier = self._validate_managed_media_id(variant_id)
        return f"reference-variants/{safe_identifier}.wav"

    def _managed_reference_variant_path(self, variant_id: str) -> Path:
        return (self.media_root / self._reference_variant_storage_key(variant_id)).resolve()

    def _reference_asset_metadata(self, asset: ReferenceAsset) -> dict[str, Any]:
        return dict(asset.model_metadata or {})

    def _normalize_reference_variant_storage_key(self, raw_value: str, variant_id: str) -> str:
        raw_path = Path(raw_value)
        if raw_path.is_absolute():
            resolved = raw_path.expanduser().resolve(strict=False)
            media_root = self.media_root.resolve()
            if resolved.is_relative_to(media_root):
                return resolved.relative_to(media_root).as_posix()
            return raw_value
        if raw_value.startswith("reference-variants/"):
            return raw_value
        return self._reference_variant_storage_key(variant_id)

    def _resolve_reference_variant_media_path(self, raw_value: str) -> Path:
        raw_path = Path(raw_value)
        if raw_path.is_absolute():
            return self._resolve_variant_media_path(raw_path)
        resolved = (self.media_root / raw_path).resolve(strict=False)
        media_root = self.media_root.resolve()
        if not resolved.is_relative_to(media_root):
            raise ValueError(f"Managed reference media path escapes media root: {resolved}")
        if not resolved.exists():
            raise FileNotFoundError(resolved)
        return resolved

    def _reference_asset_migration_reports_root(self) -> Path:
        return (self.db_path.parent / "migration-reports").resolve()

    def _write_reference_asset_migration_report(self, issues: list[dict[str, Any]]) -> Path | None:
        if not issues:
            return None
        reports_root = self._reference_asset_migration_reports_root()
        reports_root.mkdir(parents=True, exist_ok=True)
        report_path = reports_root / f"referenceasset-migration-{utc_now().strftime('%Y%m%dT%H%M%S%fZ')}.json"
        payload = {
            "generated_at": utc_now().isoformat(),
            "issue_count": len(issues),
            "issues": issues,
        }
        report_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
        return report_path

    def _default_reference_asset_name(self, slice_row: Slice) -> str:
        transcript_text = self._transcript_text(slice_row.transcript).strip()
        if transcript_text:
            words = transcript_text.split()
            preview = " ".join(words[:6]).strip()
            if len(words) > 6:
                preview = f"{preview}..."
            return preview
        speaker = self._speaker_name(slice_row)
        start = float(self._slice_metadata(slice_row).get("original_start_time", 0.0))
        end = float(self._slice_metadata(slice_row).get("original_end_time", self._slice_duration(slice_row)))
        return f"{speaker} {start:.2f}-{end:.2f}s"

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

_repository_instance: SQLiteRepository | None = None


def get_repository() -> SQLiteRepository:
    global _repository_instance
    if _repository_instance is None:
        _repository_instance = SQLiteRepository()
    return _repository_instance


class _RepositoryProxy:
    def __getattr__(self, name: str) -> Any:
        return getattr(get_repository(), name)


repository = _RepositoryProxy()
