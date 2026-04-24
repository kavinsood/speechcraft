from __future__ import annotations

import hashlib
import io
import json
import math
import os
import re
import shutil
import threading
import subprocess
import tempfile
import wave
from dataclasses import dataclass, field, fields as dataclass_fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
from sqlalchemy import func
from sqlalchemy import insert
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
    ProjectPreparationRequest,
    ProjectPreparationRun,
    ProjectRecordingJobsRun,
    ProjectSlicerRunDeleteResult,
    ProjectSlicerRunRequest,
    ProjectSlicerRunView,
    QCBucket,
    QCRun,
    QCRunCreateRequest,
    QCRunView,
    ProjectSummary,
    ReferenceAsset,
    ReferenceAssetCreateFromSlice,
    ReferenceAssetCreateFromCandidate,
    ReferenceAssetDetail,
    ReferenceEmbeddingEvaluationRequest,
    ReferenceEmbeddingEvaluationProbeResult,
    ReferenceEmbeddingEvaluationResponse,
    ReferenceEmbeddingStatus,
    ReferenceAssetStatus,
    ReferenceAssetSummary,
    ReferenceCandidateRerankResult,
    ReferenceCandidateSummary,
    ReferenceRunCreate,
    ReferenceRunRerankRequest,
    ReferenceRunRerankResponse,
    ReferenceRunView,
    ReferencePickerRun,
    ReferenceSourceKind,
    ReferenceVariant,
    ReferenceVariantView,
    ReviewStatus,
    ReferenceRunStatus,
    SliceRevision,
    SliceSaveRequest,
    SliceQCResult,
    SliceQCResultView,
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
DATA_VERSION_REFERENCE_PICKER_SCHEMA = 3
DATA_VERSION_REFERENCE_VARIANT_RELATIVE_PATHS = 4
DATA_VERSION_LEGACY_SOURCE_RECORDING_AUDIO_BACKFILL = 5
LATEST_DATA_VERSION = DATA_VERSION_LEGACY_SOURCE_RECORDING_AUDIO_BACKFILL
REFERENCE_RUN_EMBEDDING_ARTIFACT_SCHEMA_VERSION = 2
REFERENCE_ASSET_EMBEDDING_ARTIFACT_SCHEMA_VERSION = 1
LEGACY_SEED_SOURCE_RECORDING_PROCESSING_RECIPE = "legacy_seed_clip_source"


class SliceSaveValidationError(ValueError):
    """Raised when a slice save request is syntactically valid but semantically invalid."""


def _pcm16le_as_float64(raw: bytes) -> np.ndarray:
    if not raw:
        return np.zeros(0, dtype=np.float64)
    return np.frombuffer(raw, dtype="<i2").astype(np.float64) / 32767.0


def _pcm16le_rms(raw: bytes) -> float:
    samples = _pcm16le_as_float64(raw)
    if len(samples) == 0:
        return 0.0
    return float(np.sqrt(np.mean(samples**2)))


def _pcm16le_tomono(raw: bytes, channels: int) -> bytes:
    if channels <= 1 or not raw:
        return raw
    samples = np.frombuffer(raw, dtype="<i2")
    if len(samples) == 0:
        return raw
    frame_count = len(samples) // channels
    if frame_count <= 0:
        return b""
    frames = samples[: frame_count * channels].reshape(frame_count, channels).astype(np.float64)
    mono = np.rint(frames.mean(axis=1)).clip(-32768, 32767).astype("<i2")
    return mono.tobytes()


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
        self._migrate_importbatch_schema()
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

    def list_project_preparation_jobs(self, project_id: str) -> list[ProcessingJobView]:
        with self._session() as session:
            self._get_batch(session, project_id)
            jobs = session.exec(
                select(ProcessingJob)
                .where(ProcessingJob.kind == JobKind.PREPROCESS)
                .order_by(ProcessingJob.created_at.desc(), ProcessingJob.id.desc())
            ).all()
            return [
                self._processing_job_view(job)
                for job in jobs
                if dict(job.input_payload or {}).get("project_id") == project_id
            ]

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
            embeddings = self._build_reference_run_candidate_embeddings(candidates)
            embedding_dimension = len(embeddings[0]) if embeddings else 17
            embedding_space = self._current_reference_embedding_space_descriptor(embedding_dimension)
            candidates = [
                candidate.model_copy(update={"embedding_space_id": embedding_space["id"]})
                for candidate in candidates
            ]
            artifact_root.mkdir(parents=True, exist_ok=True)
            config["embedding_space_id"] = embedding_space["id"]
            config["embedding_provider"] = embedding_space["name"]
            (artifact_root / "config.json").write_text(json.dumps(config, indent=2, sort_keys=True))
            self._write_reference_candidates_artifact(artifact_root, candidates)
            self._write_reference_run_embeddings_artifact(artifact_root, candidates, embeddings, embedding_space)
            manifest_payload = {
                "run_id": run_id,
                "project_id": project_id,
                "candidate_count": len(candidates),
                "embedding_count": len(embeddings),
                "embedding_dimension": embedding_dimension,
                "embedding_artifact_schema_version": REFERENCE_RUN_EMBEDDING_ARTIFACT_SCHEMA_VERSION,
                "embedding_space_id": embedding_space["id"],
                "embedding_extractor": embedding_space["name"],
                "embedding_extractor_version": embedding_space["version"],
                "generated_at": utc_now().isoformat(),
            }
            (artifact_root / "manifest.json").write_text(json.dumps(manifest_payload, indent=2, sort_keys=True))

            with self._session() as session:
                run = session.get(ReferencePickerRun, run_id)
                if run is None:
                    raise KeyError(run_id)
                run.status = ReferenceRunStatus.COMPLETED
                run.config = config
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

    def rerank_reference_run_candidates(
        self,
        run_id: str,
        payload: ReferenceRunRerankRequest,
    ) -> ReferenceRunRerankResponse:
        with self._session() as session:
            run = session.get(ReferencePickerRun, run_id)
            if run is None:
                raise KeyError(run_id)
            if run.status != ReferenceRunStatus.COMPLETED:
                raise ValueError("Reference run must be completed before reranking")
            artifact_root = Path(run.artifact_root)
            mode = self._normalize_reference_run_mode(payload.mode or run.mode)
            candidates = self._read_reference_run_candidates(artifact_root)
            loaded_artifact = self._load_reference_run_embeddings_artifact(artifact_root)
            embedding_space_id = loaded_artifact["space_id"]
            embeddings_by_candidate_id = loaded_artifact["vectors_by_candidate_id"]
        if not candidates:
            return ReferenceRunRerankResponse(
                run_id=run_id,
                mode=mode,
                embedding_space_id=embedding_space_id,
                positive_candidate_ids=[],
                negative_candidate_ids=[],
                positive_reference_asset_ids=[],
                negative_reference_asset_ids=[],
                candidates=[],
            )
        if len(embeddings_by_candidate_id) < len(candidates):
            raise ValueError("Reference run is missing candidate embeddings")

        candidate_by_id = {candidate.candidate_id: candidate for candidate in candidates}
        positive_ids = self._normalize_reference_anchor_ids(payload.positive_candidate_ids, candidate_by_id)
        negative_ids = self._normalize_reference_anchor_ids(payload.negative_candidate_ids, candidate_by_id)
        positive_asset_ids = self._normalize_reference_asset_anchor_ids(payload.positive_reference_asset_ids)
        negative_asset_ids = self._normalize_reference_asset_anchor_ids(payload.negative_reference_asset_ids)
        if overlap := sorted(set(positive_ids) & set(negative_ids)):
            raise ValueError(f"Candidate anchors cannot be both positive and negative: {', '.join(overlap)}")
        if overlap := sorted(set(positive_asset_ids) & set(negative_asset_ids)):
            raise ValueError(f"Reference anchors cannot be both positive and negative: {', '.join(overlap)}")

        normalized_vectors: dict[str, list[float]] = {}
        for candidate in candidates:
            raw_vector = embeddings_by_candidate_id.get(candidate.candidate_id)
            if raw_vector is None:
                raise ValueError(f"Reference run is missing an embedding for candidate {candidate.candidate_id}")
            normalized_vectors[candidate.candidate_id] = self._normalize_embedding_vector(raw_vector)

            asset_vectors = self._load_reference_asset_anchor_vectors(
                session,
                run.project_id,
                embedding_space_id,
                positive_asset_ids + negative_asset_ids,
            )

        positive_mean = self._mean_embedding_vector(
            [normalized_vectors[candidate_id] for candidate_id in positive_ids]
            + [asset_vectors[asset_id] for asset_id in positive_asset_ids]
        )
        negative_mean = self._mean_embedding_vector(
            [normalized_vectors[candidate_id] for candidate_id in negative_ids]
            + [asset_vectors[asset_id] for asset_id in negative_asset_ids]
        )

        reranked: list[ReferenceCandidateRerankResult] = []
        for candidate in candidates:
            vector = normalized_vectors[candidate.candidate_id]
            intent_score = 0.0
            if positive_mean is not None:
                intent_score += self._cosine_similarity(vector, positive_mean)
            if negative_mean is not None:
                intent_score -= self._cosine_similarity(vector, negative_mean)
            base_score = float(
                candidate.default_scores.get(mode, candidate.default_scores.get("both", candidate.default_scores.get("overall", 0.0)))
            )
            rerank_score = base_score + intent_score
            reranked.append(
                ReferenceCandidateRerankResult(
                    **candidate.model_dump(mode="python"),
                    mode=mode,
                    base_score=round(base_score, 6),
                    intent_score=round(intent_score, 6),
                    rerank_score=round(rerank_score, 6),
                )
            )

        reranked.sort(
            key=lambda candidate: (
                -candidate.rerank_score,
                -candidate.base_score,
                candidate.source_recording_id or "",
                candidate.source_start_seconds,
            )
        )
        return ReferenceRunRerankResponse(
            run_id=run_id,
            mode=mode,
            embedding_space_id=embedding_space_id,
            positive_candidate_ids=positive_ids,
            negative_candidate_ids=negative_ids,
            positive_reference_asset_ids=positive_asset_ids,
            negative_reference_asset_ids=negative_asset_ids,
            candidates=reranked,
        )

    def evaluate_reference_run_embeddings(
        self,
        run_id: str,
        payload: ReferenceEmbeddingEvaluationRequest,
    ) -> ReferenceEmbeddingEvaluationResponse:
        with self._session() as session:
            run = session.get(ReferencePickerRun, run_id)
            if run is None:
                raise KeyError(run_id)
            if run.status != ReferenceRunStatus.COMPLETED:
                raise ValueError("Reference run must be completed before evaluation")
            artifact_root = Path(run.artifact_root)
            candidates = self._read_reference_run_candidates(artifact_root)
            loaded_artifact = self._load_reference_run_embeddings_artifact(artifact_root)

        candidate_by_id = {candidate.candidate_id: candidate for candidate in candidates}
        normalized_vectors = {
            candidate_id: self._normalize_embedding_vector(vector)
            for candidate_id, vector in loaded_artifact["vectors_by_candidate_id"].items()
        }
        results: list[ReferenceEmbeddingEvaluationProbeResult] = []
        for probe in payload.probes:
            if probe.anchor_candidate_id not in candidate_by_id:
                raise ValueError(f"Evaluation anchor does not belong to the run: {probe.anchor_candidate_id}")
            top_k = max(1, min(int(probe.top_k or 5), 25))
            anchor_vector = normalized_vectors.get(probe.anchor_candidate_id)
            if anchor_vector is None:
                raise ValueError(f"Reference run is missing an embedding for candidate {probe.anchor_candidate_id}")
            ranked_neighbors = sorted(
                (
                    (
                        other_candidate_id,
                        self._cosine_similarity(anchor_vector, other_vector),
                    )
                    for other_candidate_id, other_vector in normalized_vectors.items()
                    if other_candidate_id != probe.anchor_candidate_id
                ),
                key=lambda item: (-item[1], item[0]),
            )
            retrieved = [candidate_id for candidate_id, _score in ranked_neighbors[:top_k]]
            expected = [
                candidate_id
                for candidate_id in probe.expected_neighbor_candidate_ids
                if candidate_id in candidate_by_id and candidate_id != probe.anchor_candidate_id
            ]
            matched = [candidate_id for candidate_id in retrieved if candidate_id in expected]
            recall = len(matched) / len(expected) if expected else 0.0
            results.append(
                ReferenceEmbeddingEvaluationProbeResult(
                    anchor_candidate_id=probe.anchor_candidate_id,
                    top_k=top_k,
                    retrieved_neighbor_candidate_ids=retrieved,
                    matched_neighbor_candidate_ids=matched,
                    recall_at_k=round(recall, 6),
                )
            )

        average_recall = sum(result.recall_at_k for result in results) / len(results) if results else 0.0
        return ReferenceEmbeddingEvaluationResponse(
            run_id=run_id,
            embedding_space_id=loaded_artifact["space_id"],
            probe_count=len(results),
            average_recall_at_k=round(average_recall, 6),
            probes=results,
        )

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

    def run_project_preparation(
        self,
        project_id: str,
        payload: ProjectPreparationRequest,
    ) -> ProjectPreparationRun:
        normalized_payload = self._normalize_project_preparation_payload(payload)
        with self._session() as session:
            self._get_batch(session, project_id)
            job = ProcessingJob(
                id=self._new_id("job"),
                kind=JobKind.PREPROCESS,
                status=JobStatus.PENDING,
                source_recording_id=None,
                input_payload={
                    "project_id": project_id,
                    **normalized_payload,
                },
            )
            session.add(job)
            session.commit()
            session.refresh(job)
            return ProjectPreparationRun(job=self._processing_job_view(job), created_recordings=[])

    def enqueue_project_transcription(
        self,
        project_id: str,
        payload: SourceTranscriptionRequest,
    ) -> ProjectRecordingJobsRun:
        normalized_payload = self._normalize_source_transcription_payload(payload)
        with self._session() as session:
            batch = self._get_batch(session, project_id)
            prepared_output_group_id = batch.active_prepared_output_group_id
            if not prepared_output_group_id:
                raise ValueError("Run preparation before launching ASR")
            recordings = self._prepared_recordings_for_group(session, project_id, prepared_output_group_id)
            if not recordings:
                raise ValueError("Active prepared output group has no prepared recordings")
            jobs: list[ProcessingJob] = []
            for recording in recordings:
                self._mark_slicer_runs_stale_for_recording(
                    session,
                    recording.id,
                    "ASR was re-run for this prepared recording.",
                )
                job = ProcessingJob(
                    id=self._new_id("job"),
                    kind=JobKind.SOURCE_TRANSCRIPTION,
                    status=JobStatus.PENDING,
                    source_recording_id=recording.id,
                    input_payload=normalized_payload,
                )
                session.add(job)
                jobs.append(job)
            session.commit()
            for job in jobs:
                session.refresh(job)
            return ProjectRecordingJobsRun(
                project_id=project_id,
                prepared_output_group_id=prepared_output_group_id,
                jobs=[self._processing_job_view(job) for job in jobs],
            )

    def enqueue_project_alignment(
        self,
        project_id: str,
        payload: SourceAlignmentRequest,
    ) -> ProjectRecordingJobsRun:
        normalized_payload = self._normalize_source_alignment_payload(payload)
        with self._session() as session:
            batch = self._get_batch(session, project_id)
            prepared_output_group_id = batch.active_prepared_output_group_id
            if not prepared_output_group_id:
                raise ValueError("Run preparation before launching alignment")
            recordings = self._prepared_recordings_for_group(session, project_id, prepared_output_group_id)
            if not recordings:
                raise ValueError("Active prepared output group has no prepared recordings")
            active_transcription = session.exec(
                select(ProcessingJob)
                .where(
                    ProcessingJob.kind == JobKind.SOURCE_TRANSCRIPTION,
                    ProcessingJob.source_recording_id.in_([recording.id for recording in recordings]),
                    ProcessingJob.status.in_([JobStatus.PENDING, JobStatus.RUNNING]),
                )
            ).first()
            if active_transcription is not None:
                raise ValueError("Wait for ASR to complete before launching alignment")
            not_transcribed = [
                recording.id
                for recording in recordings
                if recording.source_artifact is None
                or recording.source_artifact.transcript_status not in {"ok", "patched"}
            ]
            if not_transcribed:
                raise ValueError("Run ASR before launching alignment")
            jobs: list[ProcessingJob] = []
            for recording in recordings:
                self._mark_slicer_runs_stale_for_recording(
                    session,
                    recording.id,
                    "Alignment was re-run for this prepared recording.",
                )
                job = ProcessingJob(
                    id=self._new_id("job"),
                    kind=JobKind.SOURCE_ALIGNMENT,
                    status=JobStatus.PENDING,
                    source_recording_id=recording.id,
                    input_payload=normalized_payload,
                )
                session.add(job)
                jobs.append(job)
            session.commit()
            for job in jobs:
                session.refresh(job)
            return ProjectRecordingJobsRun(
                project_id=project_id,
                prepared_output_group_id=prepared_output_group_id,
                jobs=[self._processing_job_view(job) for job in jobs],
            )

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
        normalized_payload = self._normalize_source_transcription_payload(payload)
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            self._mark_slicer_runs_stale_for_recording(
                session,
                recording.id,
                "ASR was re-run for this recording.",
            )
            job = ProcessingJob(
                id=self._new_id("job"),
                kind=JobKind.SOURCE_TRANSCRIPTION,
                status=JobStatus.PENDING,
                source_recording_id=recording.id,
                input_payload=normalized_payload,
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
        normalized_payload = self._normalize_source_alignment_payload(payload)
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            self._mark_slicer_runs_stale_for_recording(
                session,
                recording.id,
                "Alignment was re-run for this recording.",
            )
            job = ProcessingJob(
                id=self._new_id("job"),
                kind=JobKind.SOURCE_ALIGNMENT,
                status=JobStatus.PENDING,
                source_recording_id=recording.id,
                input_payload=normalized_payload,
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

    def list_project_slicer_runs(self, project_id: str) -> list[ProjectSlicerRunView]:
        with self._session() as session:
            self._get_batch(session, project_id)
            jobs = session.exec(
                select(ProcessingJob)
                .where(ProcessingJob.kind == JobKind.SOURCE_SLICING)
                .order_by(ProcessingJob.created_at.desc(), ProcessingJob.id.desc())
            ).all()
            project_jobs = [
                job for job in jobs if dict(job.input_payload or {}).get("project_id") == project_id
            ]
            return self._project_slicer_run_views(project_id, project_jobs)

    def create_project_slicer_run(
        self,
        project_id: str,
        payload: ProjectSlicerRunRequest,
    ) -> ProjectSlicerRunView:
        slicer_run_id = self._new_id("slicer-run")
        with self._session() as session:
            batch = self._get_batch(session, project_id)
            prepared_output_group_id = batch.active_prepared_output_group_id
            if not prepared_output_group_id:
                raise ValueError("Run preparation before launching the slicer")
            prepared_recordings = self._prepared_recordings_for_group(
                session,
                project_id,
                prepared_output_group_id,
            )
            if not prepared_recordings:
                raise ValueError("Active prepared output group has no prepared recordings")
            config_overrides = self._project_slicer_config_overrides(payload)
            unaligned_recordings = [
                recording.id
                for recording in prepared_recordings
                if recording.source_artifact is None
                or recording.source_artifact.alignment_status != "ok"
                or not recording.source_artifact.alignment_json_path
            ]
            if unaligned_recordings:
                raise ValueError("Run ASR and alignment on the prepared output before launching the slicer")

            jobs: list[ProcessingJob] = []
            for recording in prepared_recordings:
                input_payload = SourceSlicingRequest(
                    replace_unlocked_slices=payload.replace_unlocked_slices,
                    preserve_locked_slices=payload.preserve_locked_slices,
                    config_overrides=config_overrides,
                ).model_dump(mode="json")
                input_payload.update(
                    {
                        "project_id": project_id,
                        "slicer_run_id": slicer_run_id,
                        "prepared_output_group_id": prepared_output_group_id,
                        "ui_config": payload.model_dump(mode="json"),
                    }
                )
                job = ProcessingJob(
                    id=self._new_id("job"),
                    kind=JobKind.SOURCE_SLICING,
                    status=JobStatus.PENDING,
                    source_recording_id=recording.id,
                    input_payload=input_payload,
                )
                session.add(job)
                jobs.append(job)
            session.commit()
            for job in jobs:
                session.refresh(job)
            return self._project_slicer_run_view(project_id, jobs)

    def delete_project_slicer_run(self, project_id: str, slicer_run_id: str) -> ProjectSlicerRunDeleteResult:
        with self._session() as session:
            self._get_batch(session, project_id)
            jobs = self._slicer_run_jobs(session, project_id, slicer_run_id)
            if not jobs:
                raise KeyError(slicer_run_id)
            if any(job.status in {JobStatus.PENDING, JobStatus.RUNNING} for job in jobs):
                raise ValueError("Cannot delete a slicer run while jobs are pending or running")

            job_ids = [job.id for job in jobs]
            deleted_paths: list[str] = []
            deleted_slice_ids: list[str] = []
            deleted_variant_ids: list[str] = []

            qc_runs = session.exec(
                select(QCRun).where(QCRun.project_id == project_id, QCRun.slicer_run_id == slicer_run_id)
            ).all()
            qc_run_ids = [run.id for run in qc_runs]
            deleted_qc_result_count = 0
            if qc_run_ids:
                qc_result_ids = session.exec(
                    select(SliceQCResult.id).where(SliceQCResult.qc_run_id.in_(qc_run_ids))
                ).all()
                deleted_qc_result_count = len(qc_result_ids)
                session.exec(delete(SliceQCResult).where(SliceQCResult.qc_run_id.in_(qc_run_ids)))
                session.exec(delete(QCRun).where(QCRun.id.in_(qc_run_ids)))

            candidate_slices = self._get_all_batch_slices(session, project_id)
            generated_slices = [
                slice_row
                for slice_row in candidate_slices
                if self._slice_metadata(slice_row).get("slicer_run_id") == slicer_run_id
            ]
            deleted_slice_ids = [slice_row.id for slice_row in generated_slices]
            deleted_variant_ids = [
                variant.id for slice_row in generated_slices for variant in slice_row.variants
            ]
            deleted_paths = [
                variant.file_path for slice_row in generated_slices for variant in slice_row.variants
            ]
            self._bulk_delete_slice_rows(session, generated_slices)

            deleted_slice_id_set = set(deleted_slice_ids)
            restored_slice_count = 0
            for slice_row in candidate_slices:
                if slice_row.id in deleted_slice_id_set:
                    continue
                metadata = self._slice_metadata(slice_row)
                if metadata.get("superseded_by_job_id") not in job_ids:
                    continue
                metadata["is_superseded"] = False
                metadata.pop("superseded_by_job_id", None)
                metadata["restored_after_slicer_run_delete"] = slicer_run_id
                metadata["updated_at"] = utc_now().isoformat()
                slice_row.model_metadata = metadata
                session.add(slice_row)
                restored_slice_count += 1

            session.exec(delete(ProcessingJob).where(ProcessingJob.id.in_(job_ids)))
            session.commit()

        deleted_file_count = self._delete_unreferenced_media_files(deleted_paths)
        return ProjectSlicerRunDeleteResult(
            project_id=project_id,
            slicer_run_id=slicer_run_id,
            deleted_job_count=len(job_ids),
            deleted_qc_run_count=len(qc_run_ids),
            deleted_qc_result_count=deleted_qc_result_count,
            deleted_slice_count=len(deleted_slice_ids),
            deleted_variant_count=len(deleted_variant_ids),
            deleted_file_count=deleted_file_count,
            restored_slice_count=restored_slice_count,
            deleted_slice_ids=deleted_slice_ids,
            deleted_variant_ids=deleted_variant_ids,
        )

    def list_project_qc_runs(
        self,
        project_id: str,
        slicer_run_id: str | None = None,
    ) -> list[QCRunView]:
        with self._session() as session:
            self._get_batch(session, project_id)
            statement = select(QCRun).where(QCRun.project_id == project_id).order_by(QCRun.created_at.desc())
            if slicer_run_id:
                statement = statement.where(QCRun.slicer_run_id == slicer_run_id)
            runs = session.exec(statement).all()
            return [self._qc_run_view(session, self._refresh_qc_run_stale_state(session, run)) for run in runs]

    def get_qc_run(self, qc_run_id: str) -> QCRunView:
        with self._session() as session:
            run = session.get(QCRun, qc_run_id)
            if run is None:
                raise KeyError(qc_run_id)
            return self._qc_run_view(session, self._refresh_qc_run_stale_state(session, run), include_results=True)

    def create_qc_run(self, project_id: str, payload: QCRunCreateRequest) -> QCRunView:
        keep_threshold = max(0.0, min(1.0, float(payload.keep_threshold)))
        reject_threshold = max(0.0, min(1.0, float(payload.reject_threshold)))
        if reject_threshold > keep_threshold:
            raise ValueError("reject_threshold cannot exceed keep_threshold")
        with self._session() as session:
            self._get_batch(session, project_id)
            slicer_jobs = self._slicer_run_jobs(session, project_id, payload.slicer_run_id)
            if not slicer_jobs:
                raise ValueError("Slicer run not found")
            if any(job.status != JobStatus.COMPLETED for job in slicer_jobs):
                raise ValueError("QC requires a completed slicer run")
            slices = self._qc_scope_slices_for_slicer_run(session, project_id, payload.slicer_run_id)
            if not slices:
                raise ValueError("Slicer run has no active slices to QC")
            snapshot = self._qc_snapshot_for_slices(slices)
            now = utc_now()
            run = QCRun(
                id=self._new_id("qc-run"),
                project_id=project_id,
                slicer_run_id=payload.slicer_run_id,
                status=JobStatus.COMPLETED,
                threshold_config={
                    "keep_threshold": keep_threshold,
                    "reject_threshold": reject_threshold,
                    "preset": payload.preset,
                },
                slice_population_hash=snapshot["slice_population_hash"],
                transcript_basis_hash=snapshot["transcript_basis_hash"],
                audio_basis_hash=snapshot["audio_basis_hash"],
                is_stale=False,
                completed_at=now,
            )
            session.add(run)
            session.flush()
            qc_result_rows: list[dict[str, Any]] = []
            for slice_row in slices:
                result_payload = self._score_slice_for_qc(
                    slice_row,
                    keep_threshold=keep_threshold,
                    reject_threshold=reject_threshold,
                )
                qc_result_rows.append(
                    {
                        "id": self._new_id("qc-result"),
                        "qc_run_id": run.id,
                        "slice_id": slice_row.id,
                        "aggregate_score": result_payload["aggregate_score"],
                        "bucket": result_payload["bucket"],
                        "raw_metrics": result_payload["raw_metrics"],
                        "reason_codes": result_payload["reason_codes"],
                        "human_review_status": slice_row.status,
                        "is_locked": bool(slice_row.is_locked),
                        "created_at": now,
                    }
                )
            if qc_result_rows:
                session.execute(insert(SliceQCResult), qc_result_rows)
            session.commit()
            session.refresh(run)
            return self._qc_run_view(session, run, include_results=True)

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
            deleted_slice_ids: list[str] = []
            deleted_variant_ids: list[str] = []
            deleted_paths: list[str] = []

            superseded_slices = [
                slice_row
                for slice_row in self._get_all_batch_slices(session, project_id)
                if self._slice_metadata(slice_row).get("is_superseded", False)
            ]
            deleted_slice_ids.extend(slice_row.id for slice_row in superseded_slices)
            deleted_variant_ids.extend(
                variant.id for slice_row in superseded_slices for variant in slice_row.variants
            )
            deleted_paths.extend(
                variant.file_path for slice_row in superseded_slices for variant in slice_row.variants
            )
            self._bulk_delete_slice_rows(session, superseded_slices)

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

    def _bulk_delete_slice_rows(self, session: Session, slices: list[Slice]) -> None:
        slice_ids = [slice_row.id for slice_row in slices]
        if not slice_ids:
            return
        variant_ids = [variant.id for slice_row in slices for variant in slice_row.variants]
        commit_ids = [commit.id for slice_row in slices for commit in slice_row.commits]
        session.exec(delete(SliceTagLink).where(SliceTagLink.slice_id.in_(slice_ids)))
        if variant_ids:
            session.exec(delete(AudioVariant).where(AudioVariant.id.in_(variant_ids)))
        if commit_ids:
            session.exec(delete(EditCommit).where(EditCommit.id.in_(commit_ids)))
        session.exec(delete(Transcript).where(Transcript.slice_id.in_(slice_ids)))
        session.exec(delete(Slice).where(Slice.id.in_(slice_ids)))

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
        normalized_name = payload.name.strip()
        if not normalized_name:
            raise ValueError("Project name is required")
        with self._session() as session:
            if session.get(ImportBatch, payload.id) is not None:
                raise ValueError("Project id already exists")
            existing_name = session.exec(
                select(ImportBatch).where(func.lower(ImportBatch.name) == normalized_name.lower())
            ).first()
            if existing_name is not None:
                raise ValueError("Project name already exists")
            batch = ImportBatch(id=payload.id, name=normalized_name)
            session.add(batch)
            session.commit()
            session.refresh(batch)
            return self._project_summary(session, batch)

    def delete_project(self, project_id: str) -> dict[str, int | str]:
        with self._session() as session:
            self._get_batch(session, project_id)
            recordings = session.exec(
                select(SourceRecording)
                .where(SourceRecording.batch_id == project_id)
                .options(
                    selectinload(SourceRecording.slices).selectinload(Slice.variants),
                    selectinload(SourceRecording.slices).selectinload(Slice.commits),
                )
            ).all()
            recording_ids = [recording.id for recording in recordings]
            slice_rows = [slice_row for recording in recordings for slice_row in recording.slices]
            slice_ids = [slice_row.id for slice_row in slice_rows]
            variant_paths = [
                variant.file_path for slice_row in slice_rows for variant in slice_row.variants
            ]
            source_paths = [recording.file_path for recording in recordings]
            qc_run_ids = session.exec(select(QCRun.id).where(QCRun.project_id == project_id)).all()

            if qc_run_ids:
                session.exec(delete(SliceQCResult).where(SliceQCResult.qc_run_id.in_(qc_run_ids)))
                session.exec(delete(QCRun).where(QCRun.id.in_(qc_run_ids)))
            self._bulk_delete_slice_rows(session, slice_rows)
            if recording_ids:
                session.exec(delete(SourceRecordingArtifact).where(SourceRecordingArtifact.source_recording_id.in_(recording_ids)))
                session.exec(delete(ProcessingJob).where(ProcessingJob.source_recording_id.in_(recording_ids)))
                session.exec(delete(SourceRecording).where(SourceRecording.id.in_(recording_ids)))
            session.exec(delete(ExportRun).where(ExportRun.batch_id == project_id))
            session.exec(delete(ImportBatch).where(ImportBatch.id == project_id))
            session.commit()

        deleted_file_count = self._delete_managed_media_paths(source_paths + variant_paths)
        return {"project_id": project_id, "deleted_file_count": deleted_file_count}

    def new_source_recording_id(self) -> str:
        return self._new_id("source")

    def managed_source_recording_path(self, recording_id: str) -> Path:
        return self._managed_media_path("sources", recording_id)

    def read_pcm_wav_header(self, path: Path) -> tuple[int, int, int, int]:
        return self._read_pcm_wav_header(path)

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

    def _normalize_project_preparation_payload(
        self,
        payload: ProjectPreparationRequest,
    ) -> dict[str, Any]:
        target_sample_rate = payload.target_sample_rate
        if target_sample_rate is not None:
            target_sample_rate = int(target_sample_rate)
            if target_sample_rate <= 0:
                raise ValueError("target_sample_rate must be positive")
        channel_mode = payload.channel_mode
        if channel_mode not in {"original", "mono", "left", "right"}:
            raise ValueError("Unsupported channel_mode")
        return {
            "target_sample_rate": target_sample_rate,
            "channel_mode": channel_mode,
        }

    def _normalize_source_transcription_payload(
        self,
        payload: SourceTranscriptionRequest,
    ) -> dict[str, Any]:
        batch_size = int(payload.batch_size)
        if batch_size <= 0:
            raise ValueError("ASR batch_size must be positive")
        initial_prompt = payload.initial_prompt.strip() if payload.initial_prompt else None
        return {
            **payload.model_dump(mode="json"),
            "model_size": payload.model_size,
            "batch_size": batch_size,
            "initial_prompt": initial_prompt,
            "language_hint": "en",
        }

    def _normalize_source_alignment_payload(
        self,
        payload: SourceAlignmentRequest,
    ) -> dict[str, Any]:
        batch_size = int(payload.batch_size)
        if batch_size <= 0:
            raise ValueError("Alignment batch_size must be positive")
        return {
            **payload.model_dump(mode="json"),
            "acoustic_model": payload.acoustic_model.strip() or "Wav2Vec2-Large-Robust-960h",
            "text_normalization_strategy": payload.text_normalization_strategy,
            "batch_size": batch_size,
        }

    def _project_slicer_config_overrides(
        self,
        payload: ProjectSlicerRunRequest,
    ) -> dict[str, Any]:
        target_duration = max(1.0, float(payload.target_clip_length))
        max_duration = max(target_duration, float(payload.max_clip_length))
        sensitivity = max(0.0, min(1.0, float(payload.segmentation_sensitivity)))
        overrides = {
            "target_duration": target_duration,
            "max_duration": max_duration,
            "soft_max": max(target_duration, min(max_duration, max_duration * 0.75)),
            "min_boundary_acoustic_score": round(0.55 - (sensitivity * 0.3), 3),
            "min_gap_for_boundary": round(0.18 - (sensitivity * 0.08), 3),
            "preferred_gap_for_boundary": round(0.34 - (sensitivity * 0.14), 3),
        }
        if payload.advanced_config_overrides:
            allowed_keys = {field.name for field in dataclass_fields(SlicerConfig)}
            normalized_overrides: dict[str, float] = {}
            for key, value in payload.advanced_config_overrides.items():
                if key not in allowed_keys:
                    raise ValueError(f"Unsupported slicer config override: {key}")
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                    raise ValueError(f"Slicer config override must be a finite number: {key}")
                if float(value) < 0:
                    raise ValueError(f"Slicer config override cannot be negative: {key}")
                normalized_overrides[key] = float(value)
            overrides.update(normalized_overrides)
        config = SlicerConfig(**overrides)
        if config.min_duration <= 0 or config.target_duration <= 0 or config.max_duration <= 0:
            raise ValueError("Slicer durations must be positive")
        if config.max_duration < config.min_duration:
            raise ValueError("Slicer max_duration cannot be less than min_duration")
        if not (config.min_duration <= config.target_duration <= config.max_duration):
            raise ValueError("Slicer target_duration must fall between min_duration and max_duration")
        return overrides

    def _preparation_recipe_payload(self, recording: SourceRecording) -> dict[str, Any] | None:
        if not recording.processing_recipe:
            return None
        try:
            payload = json.loads(recording.processing_recipe)
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict) or payload.get("type") != "overview_preparation":
            return None
        return payload

    def _prepared_recordings_for_group(
        self,
        session: Session,
        project_id: str,
        prepared_output_group_id: str,
    ) -> list[SourceRecording]:
        recordings = session.exec(
            select(SourceRecording)
            .where(
                SourceRecording.batch_id == project_id,
                SourceRecording.parent_recording_id.is_not(None),
            )
            .options(selectinload(SourceRecording.source_artifact))
            .order_by(SourceRecording.id.asc())
        ).all()
        matching_recordings: list[SourceRecording] = []
        for recording in recordings:
            recipe = self._preparation_recipe_payload(recording)
            if recipe is not None and recipe.get("output_group_id") == prepared_output_group_id:
                matching_recordings.append(recording)
        return matching_recordings

    def _project_slicer_run_views(
        self,
        project_id: str,
        jobs: list[ProcessingJob],
    ) -> list[ProjectSlicerRunView]:
        groups: dict[str, list[ProcessingJob]] = {}
        for job in jobs:
            payload = dict(job.input_payload or {})
            run_id = str(payload.get("slicer_run_id") or job.id)
            groups.setdefault(run_id, []).append(job)
        return [
            self._project_slicer_run_view(project_id, group_jobs)
            for _run_id, group_jobs in sorted(
                groups.items(),
                key=lambda item: max(self._as_utc(job.created_at) for job in item[1]),
                reverse=True,
            )
        ]

    def _project_slicer_run_view(
        self,
        project_id: str,
        jobs: list[ProcessingJob],
    ) -> ProjectSlicerRunView:
        if not jobs:
            raise ValueError("Slicer run requires at least one job")
        jobs = sorted(jobs, key=lambda job: (self._as_utc(job.created_at), job.id))
        first_payload = dict(jobs[0].input_payload or {})
        run_id = str(first_payload.get("slicer_run_id") or jobs[0].id)
        statuses = [job.status for job in jobs]
        if any(status == JobStatus.RUNNING for status in statuses):
            status = JobStatus.RUNNING
        elif any(status == JobStatus.PENDING for status in statuses):
            status = JobStatus.PENDING
        elif any(status == JobStatus.FAILED for status in statuses):
            status = JobStatus.FAILED
        else:
            status = JobStatus.COMPLETED
        created_at = min(self._as_utc(job.created_at) for job in jobs)
        started_values = [self._as_utc(job.started_at) for job in jobs if job.started_at is not None]
        completed_values = [self._as_utc(job.completed_at) for job in jobs if job.completed_at is not None]
        job_views = [self._processing_job_view(job) for job in jobs]
        stale_reasons = [
            str((job.output_payload or job.input_payload or {}).get("stale_reason") or "")
            for job in jobs
            if dict(job.output_payload or job.input_payload or {}).get("is_stale")
        ]
        stale_reasons = [reason for reason in stale_reasons if reason]
        is_stale = any(dict(job.output_payload or job.input_payload or {}).get("is_stale") for job in jobs)
        failure_warnings = [
            str(job.error_message)
            for job in jobs
            if job.status == JobStatus.FAILED and job.error_message
        ]
        return ProjectSlicerRunView(
            id=run_id,
            project_id=project_id,
            prepared_output_group_id=str(first_payload.get("prepared_output_group_id") or ""),
            status=status,
            created_at=created_at,
            started_at=min(started_values) if started_values else None,
            completed_at=max(completed_values) if len(completed_values) == len(jobs) else None,
            recording_ids=[str(job.source_recording_id) for job in jobs if job.source_recording_id],
            jobs=job_views,
            config=dict(first_payload.get("ui_config") or {}),
            summary=self._project_slicer_run_summary(jobs),
            warnings=[*failure_warnings, *stale_reasons],
            is_stale=is_stale,
            stale_reason=stale_reasons[0] if stale_reasons else None,
        )

    def _project_slicer_run_summary(self, jobs: list[ProcessingJob]) -> dict[str, Any]:
        completed_payloads = [
            dict(job.output_payload or {}) for job in jobs if job.status == JobStatus.COMPLETED
        ]
        created_slice_count = sum(int(payload.get("created_slice_count") or 0) for payload in completed_payloads)
        preserved_locked_count = sum(
            int(payload.get("preserved_locked_slice_count") or 0) for payload in completed_payloads
        )
        dropped_overlap_count = sum(
            int(payload.get("dropped_overlap_count") or 0) for payload in completed_payloads
        )
        stats = [dict(payload.get("slicer_stats") or {}) for payload in completed_payloads]
        total_sliced_duration = sum(float(item.get("total_clip_s") or 0.0) for item in stats)
        duration_counts = sum(int(item.get("total_clips") or 0) for item in stats)
        min_values = [float(item.get("min_duration_s") or 0.0) for item in stats if item.get("total_clips")]
        max_values = [float(item.get("max_duration_s") or 0.0) for item in stats if item.get("total_clips")]
        avg_duration = total_sliced_duration / duration_counts if duration_counts else 0.0
        return {
            "slices_created": created_slice_count,
            "total_sliced_duration": round(total_sliced_duration, 2),
            "average_slice_length": round(avg_duration, 2),
            "minimum_slice_length": round(min(min_values), 2) if min_values else 0.0,
            "maximum_slice_length": round(max(max_values), 2) if max_values else 0.0,
            "preserved_locked_slice_count": preserved_locked_count,
            "dropped_overlap_count": dropped_overlap_count,
            "completed_recording_count": len(completed_payloads),
            "failed_recording_count": sum(1 for job in jobs if job.status == JobStatus.FAILED),
            "pending_recording_count": sum(1 for job in jobs if job.status == JobStatus.PENDING),
            "running_recording_count": sum(1 for job in jobs if job.status == JobStatus.RUNNING),
            "downstream_qc_data_available": created_slice_count > 0,
        }

    def _slicer_run_jobs(self, session: Session, project_id: str, slicer_run_id: str) -> list[ProcessingJob]:
        jobs = session.exec(
            select(ProcessingJob)
            .where(ProcessingJob.kind == JobKind.SOURCE_SLICING)
            .order_by(ProcessingJob.created_at.asc(), ProcessingJob.id.asc())
        ).all()
        return [
            job
            for job in jobs
            if dict(job.input_payload or {}).get("project_id") == project_id
            and dict(job.input_payload or {}).get("slicer_run_id") == slicer_run_id
        ]

    def _mark_slicer_runs_stale_for_recording(
        self,
        session: Session,
        recording_id: str,
        reason: str,
    ) -> None:
        jobs = session.exec(
            select(ProcessingJob).where(
                ProcessingJob.kind == JobKind.SOURCE_SLICING,
                ProcessingJob.source_recording_id == recording_id,
            )
        ).all()
        if not jobs:
            return
        now = utc_now().isoformat()
        for job in jobs:
            input_payload = dict(job.input_payload or {})
            input_payload["is_stale"] = True
            input_payload["stale_reason"] = reason
            input_payload["stale_at"] = now
            job.input_payload = input_payload
            output_payload = dict(job.output_payload or {})
            output_payload["is_stale"] = True
            output_payload["stale_reason"] = reason
            output_payload["stale_at"] = now
            job.output_payload = output_payload
            session.add(job)

    def _sort_qc_slices_by_source_order(self, slices: list[Slice]) -> list[Slice]:
        return sorted(
            slices,
            key=lambda slice_row: (
                str(slice_row.source_recording_id),
                int(self._slice_metadata(slice_row).get("order_index", 0)),
                self._as_utc(slice_row.created_at),
                slice_row.id,
            ),
        )

    def _qc_scope_slices_for_slicer_run(
        self,
        session: Session,
        project_id: str,
        slicer_run_id: str,
    ) -> list[Slice]:
        slicer_jobs = self._slicer_run_jobs(session, project_id, slicer_run_id)
        recording_ids = [str(job.source_recording_id) for job in slicer_jobs if job.source_recording_id]
        if not recording_ids:
            return []
        slices = session.exec(
            select(Slice)
            .where(Slice.source_recording_id.in_(recording_ids))
            .options(
                selectinload(Slice.transcript),
                selectinload(Slice.variants),
                selectinload(Slice.active_variant),
                selectinload(Slice.commits),
            )
            .order_by(Slice.source_recording_id.asc(), Slice.created_at.asc(), Slice.id.asc())
        ).all()
        active_slices = []
        for slice_row in slices:
            metadata = self._slice_metadata(slice_row)
            if metadata.get("is_superseded", False):
                continue
            if metadata.get("slicer_run_id") != slicer_run_id:
                continue
            active_slices.append(slice_row)
        return self._sort_qc_slices_by_source_order(active_slices)

    def _qc_snapshot_for_slices(self, slices: list[Slice]) -> dict[str, str]:
        population_parts: list[str] = []
        transcript_parts: list[str] = []
        audio_parts: list[str] = []
        for slice_row in self._sort_qc_slices_by_source_order(slices):
            transcript_text = self._transcript_text(slice_row.transcript)
            active_variant_id = slice_row.active_variant_id or ""
            active_commit_id = slice_row.active_commit_id or ""
            metadata = self._slice_metadata(slice_row)
            population_parts.append(f"{slice_row.id}:{slice_row.source_recording_id}")
            transcript_parts.append(f"{slice_row.id}:{transcript_text}:{active_commit_id}")
            audio_parts.append(
                f"{slice_row.id}:{active_variant_id}:{active_commit_id}:"
                f"{metadata.get('training_start')}:{metadata.get('training_end')}"
            )
        return {
            "slice_population_hash": hashlib.sha1("|".join(population_parts).encode("utf-8")).hexdigest(),
            "transcript_basis_hash": hashlib.sha1("|".join(transcript_parts).encode("utf-8")).hexdigest(),
            "audio_basis_hash": hashlib.sha1("|".join(audio_parts).encode("utf-8")).hexdigest(),
        }

    def _refresh_qc_run_stale_state(self, session: Session, run: QCRun) -> QCRun:
        slices = self._qc_scope_slices_for_slicer_run(session, run.project_id, run.slicer_run_id)
        if not slices:
            run.is_stale = True
            run.stale_reason = "slicer_run_has_no_active_slices"
            session.add(run)
            session.commit()
            session.refresh(run)
            return run
        snapshot = self._qc_snapshot_for_slices(slices)
        stale_reasons = [
            key
            for key in ("slice_population_hash", "transcript_basis_hash", "audio_basis_hash")
            if getattr(run, key) != snapshot[key]
        ]
        run.is_stale = bool(stale_reasons)
        run.stale_reason = ",".join(stale_reasons) if stale_reasons else None
        session.add(run)
        session.commit()
        session.refresh(run)
        return run

    def _score_slice_for_qc(
        self,
        slice_row: Slice,
        *,
        keep_threshold: float,
        reject_threshold: float,
    ) -> dict[str, Any]:
        metadata = self._slice_metadata(slice_row)
        duration = self._slice_duration(slice_row)
        transcript_text = self._transcript_text(slice_row.transcript).strip()
        word_count = len(transcript_text.split()) if transcript_text else 0
        avg_alignment_confidence = float(metadata.get("avg_alignment_confidence") or 0.0)
        edge_start_energy = float(metadata.get("edge_start_energy") or 0.0)
        edge_end_energy = float(metadata.get("edge_end_energy") or 0.0)
        flag_reasons = list(metadata.get("flag_reasons") or [])
        reason_codes: list[str] = []

        if duration <= 0 or slice_row.active_variant_id is None:
            reason_codes.append("broken_audio")
        if duration < 1.0 or word_count == 0:
            reason_codes.append("near_silence_unusable_clip")
        if not transcript_text:
            reason_codes.append("transcript_mismatch")
        if edge_start_energy > 0.8 or edge_end_energy > 0.8:
            reason_codes.append("severe_clipping_corruption")
        if any("overlap" in str(reason).lower() for reason in flag_reasons):
            reason_codes.append("overlap_second_speaker")

        duration_score = max(0.0, 1.0 - abs(duration - 7.0) / 10.0)
        confidence_score = max(0.0, min(1.0, avg_alignment_confidence or 0.75))
        transcript_score = min(1.0, word_count / max(duration * 2.2, 1.0)) if duration > 0 else 0.0
        edge_penalty = min(0.35, (edge_start_energy + edge_end_energy) * 0.12)
        flag_penalty = min(0.35, len(flag_reasons) * 0.08)
        hard_gate_penalty = 0.55 if reason_codes else 0.0
        aggregate_score = max(
            0.0,
            min(
                1.0,
                (duration_score * 0.3)
                + (confidence_score * 0.3)
                + (transcript_score * 0.25)
                + 0.15
                - edge_penalty
                - flag_penalty
                - hard_gate_penalty,
            ),
        )
        aggregate_score = round(aggregate_score, 4)
        if reason_codes or aggregate_score < reject_threshold:
            bucket = QCBucket.AUTO_REJECTED
        elif aggregate_score >= keep_threshold:
            bucket = QCBucket.AUTO_KEPT
        else:
            bucket = QCBucket.NEEDS_REVIEW
        return {
            "aggregate_score": aggregate_score,
            "bucket": bucket,
            "reason_codes": sorted(set(reason_codes + [str(reason) for reason in flag_reasons])),
            "raw_metrics": {
                "duration_seconds": duration,
                "word_count": word_count,
                "avg_alignment_confidence": round(avg_alignment_confidence, 4),
                "edge_start_energy": round(edge_start_energy, 6),
                "edge_end_energy": round(edge_end_energy, 6),
                "duration_score": round(duration_score, 4),
                "confidence_score": round(confidence_score, 4),
                "transcript_score": round(transcript_score, 4),
                "edge_penalty": round(edge_penalty, 4),
                "flag_penalty": round(flag_penalty, 4),
                "hard_gate_penalty": hard_gate_penalty,
            },
        }

    def _qc_result_source_key(self, result: SliceQCResult, slice_row: Slice | None) -> tuple[str, int, float, str]:
        if slice_row is None:
            return ("", 0, 0.0, result.slice_id)
        metadata = self._slice_metadata(slice_row)
        return (
            slice_row.source_recording_id,
            int(metadata.get("order_index", 0)),
            float(metadata.get("training_start", metadata.get("original_start_time", 0.0)) or 0.0),
            slice_row.id,
        )

    def _qc_result_view(self, result: SliceQCResult, slice_row: Slice | None = None) -> SliceQCResultView:
        metadata = self._slice_metadata(slice_row) if slice_row else {}
        return SliceQCResultView(
            id=result.id,
            qc_run_id=result.qc_run_id,
            slice_id=result.slice_id,
            source_recording_id=slice_row.source_recording_id if slice_row else None,
            source_order_index=int(metadata.get("order_index", 0)) if slice_row else None,
            source_start_seconds=(
                float(metadata.get("training_start", metadata.get("original_start_time", 0.0)) or 0.0)
                if slice_row
                else None
            ),
            source_end_seconds=(
                float(metadata.get("training_end", metadata.get("original_end_time", 0.0)) or 0.0)
                if slice_row
                else None
            ),
            aggregate_score=result.aggregate_score,
            bucket=result.bucket,
            raw_metrics=result.raw_metrics,
            reason_codes=result.reason_codes,
            human_review_status=result.human_review_status,
            is_locked=result.is_locked,
            created_at=self._as_utc(result.created_at),
        )

    def _qc_run_view(
        self,
        session: Session,
        run: QCRun,
        *,
        include_results: bool = False,
    ) -> QCRunView:
        results = session.exec(
            select(SliceQCResult)
            .where(SliceQCResult.qc_run_id == run.id)
            .order_by(SliceQCResult.created_at.asc(), SliceQCResult.slice_id.asc())
        ).all()
        slice_ids = [result.slice_id for result in results]
        slice_map = {
            slice_row.id: slice_row
            for slice_row in session.exec(select(Slice).where(Slice.id.in_(slice_ids))).all()
        } if slice_ids else {}
        bucket_counts: dict[str, int] = {}
        for result in results:
            bucket_counts[result.bucket.value] = bucket_counts.get(result.bucket.value, 0) + 1
        source_ordered_results = sorted(
            results,
            key=lambda result: self._qc_result_source_key(result, slice_map.get(result.slice_id)),
        )
        return QCRunView(
            id=run.id,
            project_id=run.project_id,
            slicer_run_id=run.slicer_run_id,
            status=run.status,
            threshold_config=run.threshold_config,
            slice_population_hash=run.slice_population_hash,
            transcript_basis_hash=run.transcript_basis_hash,
            audio_basis_hash=run.audio_basis_hash,
            is_stale=run.is_stale,
            stale_reason=run.stale_reason,
            error_message=run.error_message,
            created_at=self._as_utc(run.created_at),
            completed_at=self._as_utc(run.completed_at) if run.completed_at else None,
            result_count=len(results),
            bucket_counts=bucket_counts,
            results=[
                self._qc_result_view(result, slice_map.get(result.slice_id))
                for result in source_ordered_results
            ] if include_results else [],
        )

    def _run_project_preparation_job(self, job_id: str) -> dict[str, Any]:
        with self._session() as session:
            job = self._get_processing_job(session, job_id)
            payload = dict(job.input_payload or {})
            project_id = str(payload.get("project_id") or "")
            if not project_id:
                raise ValueError("Preparation job is missing project_id")
            settings = self._normalize_project_preparation_payload(
                ProjectPreparationRequest(
                    target_sample_rate=payload.get("target_sample_rate"),
                    channel_mode=payload.get("channel_mode") or "original",
                )
            )
        return self._materialize_project_preparation(project_id, settings, job_id=job_id)

    def _materialize_project_preparation(
        self,
        project_id: str,
        settings: dict[str, Any],
        *,
        job_id: str,
    ) -> dict[str, Any]:
        output_group_id = self._new_id("prep")
        logs: list[str] = []
        created_recording_ids: list[str] = []
        written_paths: list[Path] = []
        try:
            with self._session() as session:
                self._get_batch(session, project_id)
                source_recordings = session.exec(
                    select(SourceRecording)
                    .where(
                        SourceRecording.batch_id == project_id,
                        SourceRecording.parent_recording_id.is_(None),
                    )
                    .order_by(SourceRecording.id.asc())
                ).all()
            if not source_recordings:
                raise ValueError("Project has no raw source recordings to prepare")

            logs.append(f"Preparing {len(source_recordings)} raw recording(s)")
            for index, source_recording in enumerate(source_recordings, start=1):
                source_path = self._get_source_recording_audio_path(source_recording)
                derivative_id = self._new_id("prepared")
                target_path = self._managed_media_path("prepared", derivative_id)
                target_path.parent.mkdir(parents=True, exist_ok=True)
                written_paths.append(target_path)
                sample_rate, channels, num_samples = self._write_prepared_wav(
                    source_path,
                    target_path,
                    source_channels=source_recording.num_channels,
                    target_sample_rate=settings["target_sample_rate"],
                    channel_mode=settings["channel_mode"],
                )
                processing_recipe = json.dumps(
                    {
                        "type": "overview_preparation",
                        "version": 1,
                        "job_id": job_id,
                        "output_group_id": output_group_id,
                        "source_recording_id": source_recording.id,
                        "settings": settings,
                    },
                    sort_keys=True,
                )
                derivative = self.create_preprocessed_recording(
                    source_recording.id,
                    RecordingDerivativeCreate(
                        id=derivative_id,
                        file_path=str(target_path),
                        sample_rate=sample_rate,
                        num_channels=channels,
                        num_samples=num_samples,
                        processing_recipe=processing_recipe,
                    ),
                )
                created_recording_ids.append(derivative.id)
                self._copy_prepared_recording_artifact(
                    source_recording.id,
                    derivative.id,
                    job_id=job_id,
                )
                logs.append(
                    f"[{index}/{len(source_recordings)}] Created prepared derivative {derivative.id} "
                    f"from {source_recording.id}"
                )
            with self._session() as session:
                batch = self._get_batch(session, project_id)
                batch.active_prepared_output_group_id = output_group_id
                batch.active_preparation_job_id = job_id
                session.add(batch)
                session.commit()
        except Exception:
            self._delete_source_recordings(created_recording_ids)
            for path in written_paths:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
            raise

        return {
            "project_id": project_id,
            "output_group_id": output_group_id,
            "created_recording_ids": created_recording_ids,
            "settings": settings,
            "logs": logs,
        }

    def _copy_prepared_recording_artifact(
        self,
        source_recording_id: str,
        derivative_recording_id: str,
        *,
        job_id: str,
    ) -> None:
        with self._session() as session:
            source_recording = self._get_source_recording(session, source_recording_id)
            derivative_recording = self._get_source_recording(session, derivative_recording_id)
            source_artifact = self._ensure_source_recording_artifact(session, source_recording)
            derivative_artifact = self._ensure_source_recording_artifact(session, derivative_recording)
            derivative_artifact.transcript_text_path = source_artifact.transcript_text_path
            derivative_artifact.transcript_json_path = source_artifact.transcript_json_path
            derivative_artifact.alignment_json_path = source_artifact.alignment_json_path
            derivative_artifact.transcript_status = source_artifact.transcript_status
            derivative_artifact.alignment_status = source_artifact.alignment_status
            derivative_artifact.transcript_word_count = source_artifact.transcript_word_count
            derivative_artifact.alignment_word_count = source_artifact.alignment_word_count
            derivative_artifact.transcript_updated_at = source_artifact.transcript_updated_at
            derivative_artifact.aligned_at = source_artifact.aligned_at
            derivative_artifact.alignment_backend = source_artifact.alignment_backend
            derivative_artifact.artifact_metadata = {
                **dict(source_artifact.artifact_metadata or {}),
                "copied_from_source_recording_id": source_recording.id,
                "copied_by_preparation_job_id": job_id,
            }
            session.add(derivative_artifact)
            session.commit()

    def _delete_source_recordings(self, recording_ids: list[str]) -> None:
        if not recording_ids:
            return
        with self._session() as session:
            recordings = session.exec(
                select(SourceRecording).where(SourceRecording.id.in_(recording_ids))
            ).all()
            for recording in recordings:
                session.delete(recording)
            session.commit()

    def _write_prepared_wav(
        self,
        source_path: Path,
        target_path: Path,
        *,
        source_channels: int,
        target_sample_rate: int | None,
        channel_mode: str,
    ) -> tuple[int, int, int]:
        if channel_mode == "right" and source_channels < 2:
            raise ValueError("Right channel selection requires a stereo source recording")
        if source_channels <= 0:
            raise ValueError("Source recording has an invalid channel count")

        filters: list[str] = []
        command = [
            "ffmpeg",
            "-hide_banner",
            "-nostdin",
            "-y",
            "-i",
            str(source_path),
            "-vn",
            "-map_metadata",
            "-1",
        ]
        if channel_mode == "mono":
            command.extend(["-ac", "1"])
        elif channel_mode == "left":
            filters.append("pan=mono|c0=c0")
        elif channel_mode == "right":
            filters.append("pan=mono|c0=c1")
        if target_sample_rate is not None:
            filters.append(f"aresample={int(target_sample_rate)}:resampler=soxr")
        if filters:
            command.extend(["-af", ",".join(filters)])
        command.extend(["-acodec", "pcm_s16le", str(target_path)])

        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            raise ValueError(f"ffmpeg preparation failed for {source_path.name}: {stderr or result.returncode}")

        channels, sample_width, sample_rate, frames = self._read_pcm_wav_header(target_path)
        if sample_width != 2:
            raise ValueError("Prepared WAV must be 16-bit PCM")
        self._validate_audio_asset(target_path, sample_rate, channels, frames)
        return sample_rate, channels, frames

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
            self._cache_reference_asset_embedding(session, asset, variant, audio_bytes=rendered_audio_bytes)
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

            source_start_seconds, source_end_seconds = self._resolve_reference_candidate_promotion_bounds(
                candidate,
                source_recording,
                payload.source_start_seconds,
                payload.source_end_seconds,
            )

            rendered_audio_bytes = self._crop_source_recording_audio_bytes(
                source_recording,
                source_start_seconds,
                source_end_seconds,
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
                    "candidate_source_start_seconds": candidate.source_start_seconds,
                    "candidate_source_end_seconds": candidate.source_end_seconds,
                    "default_scores": candidate.default_scores,
                    "risk_flags": candidate.risk_flags,
                    "trim_applied": not (
                        math.isclose(source_start_seconds, candidate.source_start_seconds, abs_tol=1e-6)
                        and math.isclose(source_end_seconds, candidate.source_end_seconds, abs_tol=1e-6)
                    ),
                },
                created_at=now,
                updated_at=now,
            )
            variant = ReferenceVariant(
                id=reference_variant_id,
                reference_asset_id=asset.id,
                source_kind=ReferenceSourceKind.SOURCE_RECORDING,
                source_recording_id=source_recording.id,
                source_start_seconds=source_start_seconds,
                source_end_seconds=source_end_seconds,
                file_path=storage_key,
                is_original=True,
                generator_model="reference-picker",
                sample_rate=sample_rate,
                num_samples=num_samples,
                model_metadata={
                    "run_id": run.id,
                    "candidate_id": candidate.candidate_id,
                    "candidate_source_start_seconds": candidate.source_start_seconds,
                    "candidate_source_end_seconds": candidate.source_end_seconds,
                },
            )
            self._validate_reference_variant_provenance(session, asset, variant)
            session.add(asset)
            session.add(variant)
            session.flush()
            asset.active_variant_id = variant.id
            session.add(asset)
            self._cache_reference_asset_embedding(session, asset, variant, audio_bytes=rendered_audio_bytes)
            self._validate_reference_asset_integrity(session, asset, [variant])
            session.commit()
            return self._to_reference_asset_detail(session, self._get_reference_asset_row(session, asset.id))

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
            version = DATA_VERSION_REFERENCE_VARIANT_RELATIVE_PATHS
        if version < DATA_VERSION_LEGACY_SOURCE_RECORDING_AUDIO_BACKFILL:
            self._migrate_legacy_seed_source_recording_media()
            self._set_data_version(DATA_VERSION_LEGACY_SOURCE_RECORDING_AUDIO_BACKFILL)

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

    def _migrate_importbatch_schema(self) -> None:
        with self.engine.begin() as connection:
            columns = self._table_columns(connection, "importbatch")
            if not columns:
                return
            if "active_prepared_output_group_id" not in columns:
                connection.exec_driver_sql("ALTER TABLE importbatch ADD COLUMN active_prepared_output_group_id TEXT")
            if "active_preparation_job_id" not in columns:
                connection.exec_driver_sql("ALTER TABLE importbatch ADD COLUMN active_preparation_job_id TEXT")

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
        source_audio_overrides = self._legacy_seed_source_audio_overrides(clips_by_project)

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
                source_override = source_audio_overrides.get(source_key)
                if source_override is not None:
                    sample_rate, channels, num_samples = self._materialize_legacy_seed_source_recording_audio(
                        source_path,
                        source_override,
                    )
                    processing_recipe = LEGACY_SEED_SOURCE_RECORDING_PROCESSING_RECIPE
                else:
                    if not source_path.exists():
                        source_path.write_bytes(
                            self._render_synthetic_wave_bytes(
                                clip_payload["sample_rate"],
                                clip_payload["channels"],
                                max(source_duration + 1.0, 1.0),
                                recording_id,
                            )
                        )
                    sample_rate = clip_payload["sample_rate"]
                    channels = clip_payload["channels"]
                    num_samples = max(int((source_duration + 1.0) * clip_payload["sample_rate"]), 1)
                    processing_recipe = None
                source_recording = SourceRecording(
                    id=recording_id,
                    batch_id=batch.id,
                    file_path=str(source_path),
                    sample_rate=sample_rate,
                    num_channels=channels,
                    num_samples=num_samples,
                    processing_recipe=processing_recipe,
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
            active_prepared_output_group_id=batch.active_prepared_output_group_id,
            active_preparation_job_id=batch.active_preparation_job_id,
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
            duration_seconds=round(recording.duration_s, 2),
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
        embedding_state = self._effective_reference_asset_embedding_state(asset)
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
            embedding_status=ReferenceEmbeddingStatus(embedding_state["status"]),
            embedding_space_id=embedding_state.get("space_id"),
            embedding_variant_id=embedding_state.get("variant_id"),
            embedding_updated_at=self._coerce_optional_datetime(embedding_state.get("updated_at")),
            embedding_error_message=embedding_state.get("error_message"),
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
            embedding_space_id=(run.config or {}).get("embedding_space_id"),
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

    def _reference_run_embeddings_path(self, artifact_root: Path) -> Path:
        return artifact_root / "embeddings.json"

    def _reference_asset_embeddings_root(self) -> Path:
        return (self.db_path.parent / "reference-picker" / "asset-embeddings").resolve()

    def _reference_asset_embedding_path(self, asset_id: str) -> Path:
        safe_asset_id = self._validate_managed_media_id(asset_id)
        return self._reference_asset_embeddings_root() / f"{safe_asset_id}.json"

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

    def _write_reference_run_embeddings_artifact(
        self,
        artifact_root: Path,
        candidates: list[ReferenceCandidateSummary],
        embeddings: list[list[float]],
        embedding_space: dict[str, Any],
    ) -> None:
        path = self._reference_run_embeddings_path(artifact_root)
        path.parent.mkdir(parents=True, exist_ok=True)
        dimension = len(embeddings[0]) if embeddings else 0
        payload = {
            "artifact_schema_version": REFERENCE_RUN_EMBEDDING_ARTIFACT_SCHEMA_VERSION,
            "space": {
                **embedding_space,
                "dimension": dimension,
            },
            "entries": [
                {
                    "candidate_id": candidate.candidate_id,
                    "vector": embedding,
                }
                for candidate, embedding in zip(candidates, embeddings, strict=True)
            ],
        }
        self._write_json_atomic(path, payload)

    def _load_reference_run_embeddings_artifact(self, artifact_root: Path) -> dict[str, Any]:
        path = self._reference_run_embeddings_path(artifact_root)
        if not path.exists():
            return {
                "schema_version": REFERENCE_RUN_EMBEDDING_ARTIFACT_SCHEMA_VERSION,
                "space_id": self._current_reference_embedding_space_id(),
                "vectors_by_candidate_id": {},
            }
        payload = json.loads(path.read_text())
        schema_version = int(payload.get("artifact_schema_version") or 0)
        raw_entries = payload.get("entries")
        if raw_entries is None:
            # Back-compat for earlier local artifacts before the keyed format.
            raw_vectors = payload.get("vectors") or []
            return {
                "schema_version": 1,
                "space_id": "legacy:indexed-acoustic-signature-v1",
                "vectors_by_candidate_id": {
                    str(index): [float(value) for value in list(raw_vector or [])]
                    for index, raw_vector in enumerate(raw_vectors)
                    if raw_vector
                },
            }

        vectors_by_candidate_id: dict[str, list[float]] = {}
        space = dict(payload.get("space") or {})
        space_id = str(space.get("id") or "").strip()
        if not space_id:
            raise ValueError("Embedding artifact is missing space identity")
        expected_dimension = int(space.get("dimension", 0) or 0)
        for raw_entry in list(raw_entries):
            candidate_id = str(raw_entry.get("candidate_id") or "").strip()
            if not candidate_id:
                raise ValueError("Embedding artifact entry is missing candidate_id")
            if candidate_id in vectors_by_candidate_id:
                raise ValueError(f"Embedding artifact contains duplicate candidate_id {candidate_id}")
            vector = [float(value) for value in list(raw_entry.get("vector") or [])]
            if not vector:
                raise ValueError(f"Embedding artifact entry is missing vector data for {candidate_id}")
            if expected_dimension and len(vector) != expected_dimension:
                raise ValueError(f"Embedding artifact vector dimension mismatch for {candidate_id}")
            vectors_by_candidate_id[candidate_id] = vector
        return {
            "schema_version": schema_version or REFERENCE_RUN_EMBEDDING_ARTIFACT_SCHEMA_VERSION,
            "space_id": space_id,
            "vectors_by_candidate_id": vectors_by_candidate_id,
        }

    def _get_reference_candidate(self, artifact_root: Path, candidate_id: str) -> ReferenceCandidateSummary:
        for candidate in self._read_reference_run_candidates(artifact_root):
            if candidate.candidate_id == candidate_id:
                return candidate
        raise KeyError(candidate_id)

    def _build_reference_run_candidate_embeddings(
        self,
        candidates: list[ReferenceCandidateSummary],
    ) -> list[list[float]]:
        if not candidates:
            return []

        recording_ids = sorted(
            {
                candidate.source_recording_id
                for candidate in candidates
                if candidate.source_media_kind == ReferenceSourceKind.SOURCE_RECORDING and candidate.source_recording_id
            }
        )
        with self._session() as session:
            recordings = {
                recording_id: self._get_source_recording(session, recording_id)
                for recording_id in recording_ids
            }

        embeddings: list[list[float]] = []
        for candidate in candidates:
            if candidate.source_media_kind != ReferenceSourceKind.SOURCE_RECORDING or candidate.source_recording_id is None:
                raise ValueError("Phase 1C only supports source-recording candidate embeddings")
            recording = recordings[candidate.source_recording_id]
            audio_bytes = self._crop_source_recording_audio_bytes(
                recording,
                candidate.source_start_seconds,
                candidate.source_end_seconds,
            )
            embeddings.append(self._embed_audio_bytes_for_reference_space(audio_bytes))
        return embeddings

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
        return [
            candidate.model_copy(update={"embedding_index": index})
            for index, candidate in enumerate(ranked)
        ]

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
            rms = _pcm16le_rms(chunk)
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

    def _reference_candidate_embedding_from_audio_bytes(self, audio_bytes: bytes) -> list[float]:
        channels, sample_width, sample_rate, frame_count, raw = self._read_pcm_wav(audio_bytes)
        if frame_count <= 0:
            return [0.0] * 17

        mono_raw = raw
        if channels == 2:
            mono_raw = _pcm16le_tomono(raw, channels)
        elif channels > 2:
            frame_values: list[int] = []
            bytes_per_frame = channels * sample_width
            for offset in range(0, len(raw), bytes_per_frame):
                frame_total = 0
                for channel_index in range(channels):
                    start = offset + (channel_index * sample_width)
                    frame_total += int.from_bytes(raw[start : start + sample_width], "little", signed=True)
                frame_values.append(int(frame_total / channels))
            mono_raw = b"".join(value.to_bytes(sample_width, "little", signed=True) for value in frame_values)

        sample_count = len(mono_raw) // sample_width
        if sample_count <= 0:
            return [0.0] * 17

        samples = [
            int.from_bytes(mono_raw[index : index + sample_width], "little", signed=True) / 32767.0
            for index in range(0, len(mono_raw), sample_width)
        ]
        stride = max(sample_count // 6000, 1)
        reduced = samples[::stride]
        if not reduced:
            return [0.0] * 17

        abs_sum = sum(abs(sample) for sample in reduced)
        square_sum = sum(sample * sample for sample in reduced)
        delta_sum = sum(abs(current - previous) for previous, current in zip(reduced, reduced[1:]))
        peak = max(abs(sample) for sample in reduced)
        zero_crossings = sum(
            1
            for previous, current in zip(reduced, reduced[1:])
            if (previous < 0 <= current) or (previous > 0 >= current)
        )
        length = len(reduced)
        rms = math.sqrt(square_sum / max(length, 1))
        mean_abs = abs_sum / max(length, 1)
        mean_delta = delta_sum / max(length - 1, 1)
        zcr = zero_crossings / max(length - 1, 1)

        segment_features: list[float] = []
        segment_count = 6
        for segment_index in range(segment_count):
            start = int(segment_index * length / segment_count)
            end = int((segment_index + 1) * length / segment_count)
            segment = reduced[start:end] or reduced[start : start + 1]
            if not segment:
                segment_features.extend([0.0, 0.0])
                continue
            segment_square_sum = sum(sample * sample for sample in segment)
            segment_rms = math.sqrt(segment_square_sum / len(segment))
            segment_zero_crossings = sum(
                1
                for previous, current in zip(segment, segment[1:])
                if (previous < 0 <= current) or (previous > 0 >= current)
            )
            segment_zcr = segment_zero_crossings / max(len(segment) - 1, 1)
            segment_features.extend([segment_rms, segment_zcr])

        vector = [
            rms,
            mean_abs,
            mean_delta,
            zcr,
            peak,
        ] + segment_features
        return self._normalize_embedding_vector(vector)

    def _normalize_reference_anchor_ids(
        self,
        raw_ids: list[str],
        candidate_by_id: dict[str, ReferenceCandidateSummary],
    ) -> list[str]:
        normalized: list[str] = []
        for candidate_id in raw_ids:
            cleaned = candidate_id.strip()
            if not cleaned:
                continue
            if cleaned not in candidate_by_id:
                raise ValueError(f"Candidate anchor does not belong to the run: {cleaned}")
            if cleaned not in normalized:
                normalized.append(cleaned)
        return normalized

    def _normalize_reference_asset_anchor_ids(self, raw_ids: list[str]) -> list[str]:
        normalized: list[str] = []
        for asset_id in raw_ids:
            cleaned = asset_id.strip()
            if cleaned and cleaned not in normalized:
                normalized.append(cleaned)
        return normalized

    def _normalize_embedding_vector(self, vector: list[float]) -> list[float]:
        norm = math.sqrt(sum(value * value for value in vector))
        if norm <= 1e-12:
            return [0.0 for _ in vector]
        return [float(value / norm) for value in vector]

    def _current_reference_embedding_space_id(self) -> str:
        return "acoustic_signature_v1:normalized_16bit_pcm_wav:v1"

    def _current_reference_embedding_space_descriptor(self, dimension: int) -> dict[str, Any]:
        return {
            "id": self._current_reference_embedding_space_id(),
            "name": "acoustic_signature_v1",
            "family": "acoustic_signature",
            "version": 1,
            "domain": "deterministic_acoustic_feature_vector",
            "normalized": True,
            "source_format": "normalized_16bit_pcm_wav",
            "preprocessing": {
                "channel_render_policy": "mono_average",
                "downsample_policy": "sample_stride_cap_6000",
                "windowing_policy": "global_plus_six_segments",
            },
            "features": [
                "global_rms",
                "global_mean_abs",
                "global_mean_delta",
                "global_zero_crossing_rate",
                "global_peak",
                "segment_rms_and_zero_crossing_rate_x6",
            ],
            "dimension": dimension,
        }

    def _embed_audio_bytes_for_reference_space(self, audio_bytes: bytes) -> list[float]:
        return self._reference_candidate_embedding_from_audio_bytes(audio_bytes)

    def _reference_asset_embedding_metadata(self, asset: ReferenceAsset) -> dict[str, Any]:
        metadata = self._reference_asset_metadata(asset)
        return dict(metadata.get("embedding_cache") or {})

    def _effective_reference_asset_embedding_state(self, asset: ReferenceAsset) -> dict[str, Any]:
        metadata = self._reference_asset_embedding_metadata(asset)
        status = str(metadata.get("status") or ReferenceEmbeddingStatus.MISSING.value)
        variant_id = metadata.get("variant_id")
        if (
            asset.active_variant_id
            and variant_id
            and asset.active_variant_id != variant_id
            and status == ReferenceEmbeddingStatus.READY.value
        ):
            status = ReferenceEmbeddingStatus.STALE.value
        return {
            "status": status,
            "space_id": metadata.get("space_id"),
            "variant_id": variant_id,
            "updated_at": metadata.get("updated_at"),
            "error_message": metadata.get("error_message"),
        }

    def _set_reference_asset_embedding_state(
        self,
        asset: ReferenceAsset,
        *,
        status: ReferenceEmbeddingStatus,
        variant_id: str | None,
        space_id: str | None,
        updated_at: datetime,
        error_message: str | None = None,
    ) -> None:
        metadata = self._reference_asset_metadata(asset)
        metadata["embedding_cache"] = {
            "status": status.value,
            "variant_id": variant_id,
            "space_id": space_id,
            "updated_at": updated_at.isoformat(),
            "error_message": error_message,
            "artifact_schema_version": REFERENCE_ASSET_EMBEDDING_ARTIFACT_SCHEMA_VERSION,
        }
        asset.model_metadata = metadata
        asset.updated_at = updated_at

    def _load_reference_asset_anchor_vectors(
        self,
        session: Session,
        project_id: str,
        expected_space_id: str,
        asset_ids: list[str],
    ) -> dict[str, list[float]]:
        if not asset_ids:
            return {}

        requires_commit = False
        vectors_by_asset_id: dict[str, list[float]] = {}
        for asset_id in asset_ids:
            asset = self._get_reference_asset_row(session, asset_id)
            if asset.project_id != project_id:
                raise ValueError(f"Reference anchor does not belong to the run project: {asset_id}")
            active_variant = self._get_active_reference_variant(session, asset)
            state = self._effective_reference_asset_embedding_state(asset)
            if state["status"] in {
                ReferenceEmbeddingStatus.MISSING.value,
                ReferenceEmbeddingStatus.STALE.value,
                ReferenceEmbeddingStatus.FAILED.value,
            }:
                if expected_space_id != self._current_reference_embedding_space_id():
                    raise ValueError(
                        f"Reference anchor embedding space is incompatible with the run: {asset_id}"
                    )
                self._cache_reference_asset_embedding(session, asset, active_variant)
                requires_commit = True
                state = self._effective_reference_asset_embedding_state(asset)
            if state["status"] != ReferenceEmbeddingStatus.READY.value:
                raise ValueError(f"Reference anchor embedding is not ready: {asset_id}")
            if state["space_id"] != expected_space_id:
                raise ValueError(f"Reference anchor embedding space is incompatible with the run: {asset_id}")
            artifact = self._read_reference_asset_embedding_artifact(asset.id)
            if artifact["space_id"] != expected_space_id:
                raise ValueError(f"Reference anchor embedding space is incompatible with the run: {asset_id}")
            vectors_by_asset_id[asset_id] = self._normalize_embedding_vector(artifact["vector"])
        if requires_commit:
            session.commit()
        return vectors_by_asset_id

    def _get_active_reference_variant(self, session: Session, asset: ReferenceAsset) -> ReferenceVariant:
        if asset.active_variant_id is None:
            raise ValueError(f"Reference asset {asset.id} has no active variant")
        active_variant = session.get(ReferenceVariant, asset.active_variant_id)
        if active_variant is None or active_variant.reference_asset_id != asset.id:
            raise ValueError(f"Reference asset {asset.id} active variant is invalid")
        return active_variant

    def _cache_reference_asset_embedding(
        self,
        session: Session,
        asset: ReferenceAsset,
        active_variant: ReferenceVariant,
        audio_bytes: bytes | None = None,
    ) -> None:
        timestamp = utc_now()
        self._set_reference_asset_embedding_state(
            asset,
            status=ReferenceEmbeddingStatus.PENDING,
            variant_id=active_variant.id,
            space_id=self._current_reference_embedding_space_id(),
            updated_at=timestamp,
        )
        session.add(asset)
        try:
            if audio_bytes is None:
                audio_bytes = self._resolve_reference_variant_media_path(active_variant.file_path).read_bytes()
            vector = self._embed_audio_bytes_for_reference_space(audio_bytes)
            self._write_reference_asset_embedding_artifact(asset.id, active_variant.id, vector)
            self._set_reference_asset_embedding_state(
                asset,
                status=ReferenceEmbeddingStatus.READY,
                variant_id=active_variant.id,
                space_id=self._current_reference_embedding_space_id(),
                updated_at=timestamp,
            )
        except Exception as exc:
            self._set_reference_asset_embedding_state(
                asset,
                status=ReferenceEmbeddingStatus.FAILED,
                variant_id=active_variant.id,
                space_id=self._current_reference_embedding_space_id(),
                updated_at=timestamp,
                error_message=str(exc),
            )
        session.add(asset)

    def _write_reference_asset_embedding_artifact(
        self,
        asset_id: str,
        variant_id: str,
        vector: list[float],
    ) -> None:
        path = self._reference_asset_embedding_path(asset_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "artifact_schema_version": REFERENCE_ASSET_EMBEDDING_ARTIFACT_SCHEMA_VERSION,
            "space": self._current_reference_embedding_space_descriptor(len(vector)),
            "asset_id": asset_id,
            "variant_id": variant_id,
            "vector": vector,
        }
        self._write_json_atomic(path, payload)

    def _read_reference_asset_embedding_artifact(self, asset_id: str) -> dict[str, Any]:
        path = self._reference_asset_embedding_path(asset_id)
        if not path.exists():
            raise ValueError(f"Reference asset embedding artifact is missing for {asset_id}")
        payload = json.loads(path.read_text())
        vector = [float(value) for value in list(payload.get("vector") or [])]
        if not vector:
            raise ValueError(f"Reference asset embedding artifact is missing vector data for {asset_id}")
        space = dict(payload.get("space") or {})
        space_id = str(space.get("id") or "").strip()
        if not space_id:
            raise ValueError(f"Reference asset embedding artifact is missing space identity for {asset_id}")
        dimension = int(space.get("dimension", 0) or 0)
        if dimension and len(vector) != dimension:
            raise ValueError(f"Reference asset embedding vector dimension mismatch for {asset_id}")
        return {
            "space_id": space_id,
            "variant_id": str(payload.get("variant_id") or "").strip() or None,
            "vector": vector,
        }

    def _write_json_atomic(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f"{path.name}.tmp.{uuid4().hex}")
        try:
            temp_path.write_text(json.dumps(payload))
            os.replace(temp_path, path)
        finally:
            if temp_path.exists():
                temp_path.unlink()

    def _coerce_optional_datetime(self, raw_value: Any) -> datetime | None:
        if raw_value in {None, ""}:
            return None
        return self._as_utc(self._coerce_datetime(raw_value))

    def _mean_embedding_vector(self, vectors: list[list[float]]) -> list[float] | None:
        if not vectors:
            return None
        dimension = len(vectors[0])
        if dimension == 0:
            return None
        total = [0.0] * dimension
        for vector in vectors:
            if len(vector) != dimension:
                raise ValueError("Embedding dimensions do not match")
            for index, value in enumerate(vector):
                total[index] += value
        return self._normalize_embedding_vector([value / len(vectors) for value in total])

    def _cosine_similarity(self, first: list[float], second: list[float] | None) -> float:
        if second is None or not first or not second:
            return 0.0
        if len(first) != len(second):
            raise ValueError("Embedding dimensions do not match")
        return float(sum(left * right for left, right in zip(first, second)))

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

    def _resolve_reference_candidate_promotion_bounds(
        self,
        candidate: ReferenceCandidateSummary,
        recording: SourceRecording,
        requested_start_seconds: float | None,
        requested_end_seconds: float | None,
    ) -> tuple[float, float]:
        candidate_start = float(candidate.source_start_seconds)
        candidate_end = float(candidate.source_end_seconds)
        if requested_start_seconds is None and requested_end_seconds is None:
            return candidate_start, candidate_end
        if requested_start_seconds is None or requested_end_seconds is None:
            raise ValueError(
                "Trim-aware candidate promotion requires both source_start_seconds and source_end_seconds"
            )

        start_seconds = float(requested_start_seconds)
        end_seconds = float(requested_end_seconds)
        if not math.isfinite(start_seconds) or not math.isfinite(end_seconds):
            raise ValueError("Trim bounds must be finite")
        if end_seconds <= start_seconds:
            raise ValueError("Trim bounds must have positive duration")
        epsilon = 1e-6
        if start_seconds < 0:
            if math.isclose(start_seconds, 0.0, abs_tol=epsilon):
                start_seconds = 0.0
            else:
                raise ValueError("Trim bounds must stay inside the source recording duration")
        if end_seconds > recording.duration_s:
            if math.isclose(end_seconds, recording.duration_s, abs_tol=epsilon):
                end_seconds = recording.duration_s
            else:
                raise ValueError("Trim bounds must stay inside the source recording duration")
        if start_seconds > recording.duration_s or end_seconds < 0:
            raise ValueError("Trim bounds must stay inside the source recording duration")
        if start_seconds < candidate_start:
            if math.isclose(start_seconds, candidate_start, abs_tol=epsilon):
                start_seconds = candidate_start
            else:
                raise ValueError("Trim bounds must stay inside the candidate's canonical bounds")
        if end_seconds > candidate_end:
            if math.isclose(end_seconds, candidate_end, abs_tol=epsilon):
                end_seconds = candidate_end
            else:
                raise ValueError("Trim bounds must stay inside the candidate's canonical bounds")
        return start_seconds, end_seconds

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

    def _legacy_seed_source_audio_overrides(
        self,
        clips_by_project: dict[str, list[dict[str, Any]]],
    ) -> dict[tuple[str, str], Path]:
        overrides: dict[tuple[str, str], Path] = {}
        for project_id, clips in clips_by_project.items():
            counts: dict[str, int] = {}
            for clip in clips:
                source_file_id = str(clip.get("source_file_id") or "")
                if not source_file_id:
                    continue
                counts[source_file_id] = counts.get(source_file_id, 0) + 1
            for clip in clips:
                source_file_id = str(clip.get("source_file_id") or "")
                if not source_file_id or counts.get(source_file_id) != 1:
                    continue
                raw_audio_path = clip.get("audio_path")
                if not raw_audio_path:
                    continue
                raw_path = Path(str(raw_audio_path)).expanduser().resolve(strict=False)
                if raw_path.exists():
                    overrides[(project_id, source_file_id)] = raw_path
        return overrides

    def _materialize_legacy_seed_source_recording_audio(
        self,
        target_path: Path,
        source_audio_path: Path,
    ) -> tuple[int, int, int]:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        resolved_source_path = source_audio_path.expanduser().resolve(strict=False)
        if not resolved_source_path.exists():
            raise FileNotFoundError(resolved_source_path)
        shutil.copyfile(resolved_source_path, target_path)
        channels, _sample_width, sample_rate, num_samples = self._read_pcm_wav_header(target_path)
        return sample_rate, channels, num_samples

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

    def _migrate_legacy_seed_source_recording_media(self) -> None:
        if not self.legacy_seed_path.exists():
            return
        legacy = json.loads(self.legacy_seed_path.read_text())
        source_audio_overrides = self._legacy_seed_source_audio_overrides(legacy.get("clips_by_project", {}))
        with self._session() as session:
            recordings = session.exec(
                select(SourceRecording).options(
                    selectinload(SourceRecording.slices).selectinload(Slice.variants),
                    selectinload(SourceRecording.slices).selectinload(Slice.active_variant),
                )
            ).all()
            updated = False
            for recording in recordings:
                if len(recording.slices) != 1:
                    continue
                slice_row = recording.slices[0]
                metadata = slice_row.model_metadata or {}
                source_file_id = str(metadata.get("source_file_id") or "").strip()
                if not source_file_id:
                    continue
                source_audio_path = source_audio_overrides.get((recording.batch_id, source_file_id))
                if source_audio_path is None:
                    original_variant = next((variant for variant in slice_row.variants if variant.is_original), None)
                    fallback_variant = original_variant or slice_row.active_variant
                    if fallback_variant is None:
                        continue
                    expected_channels = recording.num_channels if recording.num_channels > 0 else None
                    try:
                        source_audio_path = self._get_variant_audio_path(fallback_variant, expected_channels)
                    except (FileNotFoundError, ValueError):
                        continue
                target_path = self._managed_media_path("sources", recording.id)
                sample_rate, channels, num_samples = self._materialize_legacy_seed_source_recording_audio(
                    target_path,
                    source_audio_path,
                )
                if (
                    recording.file_path != str(target_path)
                    or recording.sample_rate != sample_rate
                    or recording.num_channels != channels
                    or recording.num_samples != num_samples
                    or recording.processing_recipe != LEGACY_SEED_SOURCE_RECORDING_PROCESSING_RECIPE
                ):
                    recording.file_path = str(target_path)
                    recording.sample_rate = sample_rate
                    recording.num_channels = channels
                    recording.num_samples = num_samples
                    recording.processing_recipe = LEGACY_SEED_SOURCE_RECORDING_PROCESSING_RECIPE
                    session.add(recording)
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

    def _delete_managed_media_paths(self, paths: list[str]) -> int:
        media_root = self.media_root.resolve()
        deleted_count = 0
        for raw_path in sorted(set(paths)):
            try:
                path = Path(raw_path).expanduser().resolve(strict=False)
            except (OSError, RuntimeError):
                continue
            if not path.is_relative_to(media_root) or not path.exists() or not path.is_file():
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
        if job_kind == JobKind.PREPROCESS:
            return self._run_project_preparation_job(job_id)
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
            backend_client = self._create_source_asr_backend_client(adapter_config)
            draft_result = self._run_source_recording_asr_adapter(
                recording,
                adapter_config=adapter_config,
                backend_client=backend_client,
                language_hint=str(language_hint) if language_hint is not None else None,
            )
            completed_at = utc_now()

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
            metadata["transcription_backend"] = draft_result["backend"]
            metadata["transcription_model_name"] = draft_result["model_name"]
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
                or payload.get("acoustic_model")
                or artifact.alignment_backend
                or "torchaudio_forced_align_worker"
            ).strip() or "torchaudio_forced_align_worker"
            text_normalization_strategy = str(payload.get("text_normalization_strategy") or "loose")

            if transcript_json_payload is not None:
                alignment_units, summary = self._run_segmented_source_alignment(recording, transcript_json_payload)
                alignment_mode = "segmented_transcript_json"
            else:
                transcript_text = transcript_text_path.read_text(encoding="utf-8").strip()
                if not transcript_text:
                    raise ValueError("Source alignment transcript artifact is empty")
                if transcript_text.startswith("stub asr source recording "):
                    raise ValueError("Refusing to align stub ASR transcript. Run real ASR before alignment.")
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
                "text_normalization_strategy": text_normalization_strategy,
                "batch_size": int(payload.get("batch_size") or 8),
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
            metadata["text_normalization_strategy"] = text_normalization_strategy
            metadata["alignment_batch_size"] = int(payload.get("batch_size") or 8)
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
            "text_normalization_strategy": text_normalization_strategy,
            "batch_size": int(payload.get("batch_size") or 8),
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
            slicer_run_id = str(payload.get("slicer_run_id") or job.id)
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
                        slicer_run_id=slicer_run_id,
                        slicer_job_id=job.id,
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
        slicer_run_id: str | None = None,
        slicer_job_id: str | None = None,
    ) -> Slice:
        extra_metadata = {
            "generation_mode": "source_slicer_v2",
            "slicer_run_id": slicer_run_id,
            "slicer_job_id": slicer_job_id,
            "raw_start": float(slice_payload["raw_start"]),
            "raw_end": float(slice_payload["raw_end"]),
            "snapped_start": float(slice_payload["snapped_start"]),
            "snapped_end": float(slice_payload["snapped_end"]),
            "training_start": float(slice_payload["training_start"]),
            "training_end": float(slice_payload["training_end"]),
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
                batch_size=max(1, int(adapter_config.get("batch_size") or 8)),
                initial_prompt=adapter_config.get("initial_prompt") or None,
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
        # ASR_BACKEND=faster_whisper ASR_MODEL_PATH=/abs/path/to/local/model-or-model-name
        # ASR_DEVICE=cpu|cuda ASR_COMPUTE_TYPE=int8|float16 uv run --directory backend python -m app.worker --once
        backend = str(os.getenv("ASR_BACKEND", "faster_whisper")).strip().lower() or "faster_whisper"
        model_size = str(payload.get("model_size") or "turbo").strip()
        batch_size = max(1, int(payload.get("batch_size") or 8))
        initial_prompt = str(payload.get("initial_prompt") or "").strip()
        requested_model_name = str(payload.get("model_name") or "").strip()
        requested_model_version = str(payload.get("model_version") or "").strip()
        if backend == "stub":
            if os.getenv("SPEECHCRAFT_ALLOW_STUB_ASR") != "1":
                raise ValueError(
                    "Stub ASR is disabled. Set ASR_BACKEND=faster_whisper for real transcription "
                    "or SPEECHCRAFT_ALLOW_STUB_ASR=1 for tests only."
                )
            return {
                "backend": "stub",
                "model_size": model_size,
                "batch_size": str(batch_size),
                "initial_prompt": initial_prompt,
                "model_name": requested_model_name or f"stub-source-recording-asr-{model_size}",
                "model_version": requested_model_version or "stub-v1",
            }
        if backend == "faster_whisper":
            model_path = str(os.getenv("ASR_MODEL_PATH", "")).strip()
            model_aliases = {
                "turbo": "large-v3-turbo",
            }
            model_ref = model_path or model_aliases.get(model_size, model_size)
            model_path_obj = Path(model_ref)
            if model_path and not model_path_obj.exists():
                raise ValueError(f"ASR_MODEL_PATH does not exist: {model_path_obj}")
            return {
                "backend": "faster_whisper",
                "model_path": str(model_path_obj if model_path else model_ref),
                "model_size": model_size,
                "batch_size": str(batch_size),
                "initial_prompt": initial_prompt,
                "model_name": requested_model_name or model_size or model_path_obj.name,
                "model_version": requested_model_version or ("local" if model_path else "faster-whisper"),
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
        batch_size: int,
        initial_prompt: str | None,
        language_hint: str | None,
    ) -> dict[str, Any]:
        transcriber = model
        transcribe_kwargs: dict[str, Any] = {}
        if batch_size > 1:
            try:
                from faster_whisper import BatchedInferencePipeline

                transcriber = BatchedInferencePipeline(model=model)
                transcribe_kwargs["batch_size"] = batch_size
            except ImportError:
                transcriber = model

        segments_iter, info = transcriber.transcribe(
            str(Path(recording.file_path)),
            language=language_hint or None,
            condition_on_previous_text=False,
            initial_prompt=initial_prompt or None,
            **transcribe_kwargs,
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
