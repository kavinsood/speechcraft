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
from sqlalchemy import event
from sqlalchemy import update as sql_update
from sqlalchemy.orm import selectinload
from sqlmodel import Session, SQLModel, create_engine, delete, select

from .models import (
    ExportRun,
    ExportPreview,
    ImportBatch,
    ImportBatchCreate,
    JobKind,
    JobStatus,
    ProcessingJob,
    ProcessingJobView,
    ProjectPreparationRequest,
    ProjectPreparationRun,
    ProjectRecordingJobsRun,
    ProjectSummary,
    ReferenceAsset,
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
    SourceAlignmentRequest,
    SourceRecordingArtifact,
    SourceRecordingArtifactView,
    SourceRecordingQueueView,
    SourceTranscriptionRequest,
    SourceRecordingView,
    SourceRecording,
    SourceRecordingCreate,
    RecordingDerivativeCreate,
    utc_now,
)

DEFAULT_PROCESSING_JOB_STALE_AFTER_SECONDS = 60.0
DATA_VERSION_EXTERNAL_VARIANT_REHOME = 1
DATA_VERSION_REFERENCE_PICKER_SCHEMA = 3
DATA_VERSION_REFERENCE_VARIANT_RELATIVE_PATHS = 4
DATA_VERSION_LEGACY_SOURCE_RECORDING_AUDIO_BACKFILL = 5
LATEST_DATA_VERSION = DATA_VERSION_LEGACY_SOURCE_RECORDING_AUDIO_BACKFILL
REFERENCE_RUN_EMBEDDING_ARTIFACT_SCHEMA_VERSION = 2
REFERENCE_ASSET_EMBEDDING_ARTIFACT_SCHEMA_VERSION = 1
LEGACY_SEED_SOURCE_RECORDING_PROCESSING_RECIPE = "legacy_seed_clip_source"


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
            connect_args={"check_same_thread": False, "timeout": 30},
        )
        @event.listens_for(self.engine, "connect")
        def _configure_sqlite(dbapi_connection: Any, _connection_record: Any) -> None:
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA busy_timeout=30000")
                cursor.execute("PRAGMA synchronous=NORMAL")
            finally:
                cursor.close()
        SQLModel.metadata.create_all(self.engine)
        self._migrate_dataset_run_schema()
        self._migrate_enum_storage()
        self._migrate_importbatch_schema()
        self._migrate_processingjob_schema()
        self._purge_legacy_review_window_schema()
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
            return [self._source_recording_view(recording, self._ensure_source_recording_artifact(session, recording)) for recording in recordings]

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
            return [
                self._source_recording_queue_view(recording, slice_count=0)
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

    def enqueue_source_slicing(self, recording_id: str, payload: object) -> ProcessingJobView:
        raise ValueError("removed")


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

    def get_export_preview(self, project_id: str) -> ExportPreview:
        with self._session() as session:
            batch = self._get_batch(session, project_id)
            return ExportPreview(
                project_id=batch.id,
                manifest_path=f"exports/{batch.id}/dataset.list",
                accepted_slice_count=0,
                lines=[],
            )


    def export_project(self, project_id: str) -> ExportRun:
        with self._session() as session:
            self._get_batch(session, project_id)
            export_id = self._new_id("export")
            output_root = self.exports_root / project_id / export_id
            manifest_path = output_root / "dataset.list"
            export_run = ExportRun(
                id=export_id,
                batch_id=project_id,
                status=JobStatus.COMPLETED,
                output_root=str(output_root),
                manifest_path=str(manifest_path),
                accepted_clip_count=0,
                completed_at=utc_now(),
            )
            session.add(export_run)
            session.commit()
            return self._normalize_export_run(export_run)


    def get_source_recording_window_media_path(
        self,
        recording_id: str,
        start_seconds: float,
        end_seconds: float,
    ) -> Path:
        with self._session() as session:
            recording = self._get_source_recording(session, recording_id)
            return self._materialize_source_window_media_path(recording, start_seconds, end_seconds)

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
                select(SourceRecording).where(SourceRecording.batch_id == project_id)
            ).all()
            recording_ids = [recording.id for recording in recordings]
            source_paths = [recording.file_path for recording in recordings]
            if recording_ids:
                session.exec(delete(SourceRecordingArtifact).where(SourceRecordingArtifact.source_recording_id.in_(recording_ids)))
                session.exec(delete(ProcessingJob).where(ProcessingJob.source_recording_id.in_(recording_ids)))
                session.exec(delete(SourceRecording).where(SourceRecording.id.in_(recording_ids)))
            session.exec(delete(ExportRun).where(ExportRun.batch_id == project_id))
            session.exec(delete(ImportBatch).where(ImportBatch.id == project_id))
            session.commit()

        deleted_file_count = self._delete_managed_media_paths(source_paths)
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

    def _migrate_dataset_run_schema(self) -> None:
        """Keep existing dev databases compatible with the dataset-run artifact spine."""

        with self.engine.begin() as connection:
            processing_columns = self._table_columns(connection, "processingrun")
            if processing_columns:
                if "pipeline_version" not in processing_columns:
                    connection.exec_driver_sql(
                        "ALTER TABLE processingrun ADD COLUMN pipeline_version TEXT NOT NULL DEFAULT 'pretraining_rfc_v1'"
                    )
                if "artifact_root" not in processing_columns:
                    connection.exec_driver_sql("ALTER TABLE processingrun ADD COLUMN artifact_root TEXT")
                if "config_hash" not in processing_columns:
                    connection.exec_driver_sql("ALTER TABLE processingrun ADD COLUMN config_hash TEXT")
                if "input_summary" not in processing_columns:
                    connection.exec_driver_sql("ALTER TABLE processingrun ADD COLUMN input_summary JSON DEFAULT '{}'")
                if "output_summary" not in processing_columns:
                    connection.exec_driver_sql("ALTER TABLE processingrun ADD COLUMN output_summary JSON DEFAULT '{}'")
                if "reason_codes" not in processing_columns:
                    connection.exec_driver_sql("ALTER TABLE processingrun ADD COLUMN reason_codes JSON DEFAULT '[]'")
                if "started_at" not in processing_columns:
                    connection.exec_driver_sql("ALTER TABLE processingrun ADD COLUMN started_at TIMESTAMP")
                if "completed_at" not in processing_columns:
                    connection.exec_driver_sql("ALTER TABLE processingrun ADD COLUMN completed_at TIMESTAMP")

            artifact_columns = self._table_columns(connection, "runartifact")
            if artifact_columns:
                if "project_id" not in artifact_columns:
                    connection.exec_driver_sql("ALTER TABLE runartifact ADD COLUMN project_id TEXT")
                if "source_audio_id" not in artifact_columns:
                    connection.exec_driver_sql("ALTER TABLE runartifact ADD COLUMN source_audio_id TEXT")
                if "source_recording_id" not in artifact_columns:
                    connection.exec_driver_sql("ALTER TABLE runartifact ADD COLUMN source_recording_id TEXT")
                if "schema_version" not in artifact_columns:
                    connection.exec_driver_sql("ALTER TABLE runartifact ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1")
                if "byte_size" not in artifact_columns:
                    connection.exec_driver_sql("ALTER TABLE runartifact ADD COLUMN byte_size INTEGER")
                if "content_hash" not in artifact_columns:
                    connection.exec_driver_sql("ALTER TABLE runartifact ADD COLUMN content_hash TEXT")
                if "config_hash" not in artifact_columns:
                    connection.exec_driver_sql("ALTER TABLE runartifact ADD COLUMN config_hash TEXT")
                if "input_artifact_hashes" not in artifact_columns:
                    connection.exec_driver_sql("ALTER TABLE runartifact ADD COLUMN input_artifact_hashes JSON DEFAULT '{}'")
                if "backend" not in artifact_columns:
                    connection.exec_driver_sql("ALTER TABLE runartifact ADD COLUMN backend TEXT")
                if "backend_version" not in artifact_columns:
                    connection.exec_driver_sql("ALTER TABLE runartifact ADD COLUMN backend_version TEXT")
                if "summary" not in artifact_columns:
                    connection.exec_driver_sql("ALTER TABLE runartifact ADD COLUMN summary JSON DEFAULT '{}'")
                if "reason_codes" not in artifact_columns:
                    connection.exec_driver_sql("ALTER TABLE runartifact ADD COLUMN reason_codes JSON DEFAULT '[]'")
                if "created_at" not in artifact_columns:
                    connection.exec_driver_sql("ALTER TABLE runartifact ADD COLUMN created_at TIMESTAMP")

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
        for row in legacy_rows:
            unresolved_rows.append(
                {
                    "legacy_asset_id": str(row["id"]),
                    "name": str(row["name"]),
                    "audio_variant_id": str(row["audio_variant_id"]),
                    "reason": "legacy_slice_reference_removed",
                }
            )
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


    def _get_batch(self, session: Session, batch_id: str) -> ImportBatch:
        batch = session.get(ImportBatch, batch_id)
        if batch is None:
            raise KeyError(batch_id)
        return batch

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



    def _source_recording_view(
        self,
        recording: SourceRecording | None,
        artifact: SourceRecordingArtifact | None = None,
    ) -> SourceRecordingView:
        if recording is None:
            raise ValueError("Slice is missing its source recording")
        metadata = dict(artifact.artifact_metadata or {}) if artifact is not None else {}
        display_name = str(metadata.get("original_filename") or "").strip() or Path(recording.file_path).name or recording.id
        return SourceRecordingView(
            id=recording.id,
            batch_id=recording.batch_id,
            display_name=display_name,
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
            if job.kind in {JobKind.SOURCE_TRANSCRIPTION, JobKind.SOURCE_ALIGNMENT}
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
            if job.kind in {JobKind.SOURCE_TRANSCRIPTION, JobKind.SOURCE_ALIGNMENT}
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
        _ = slice_count
        if active_job is not None:
            if active_job.kind == JobKind.SOURCE_TRANSCRIPTION:
                return ("transcribing", "Transcribing audio...")
            if active_job.kind == JobKind.SOURCE_ALIGNMENT:
                return ("aligning", "Aligning transcript...")
        if latest_source_job is not None and latest_source_job.status == JobStatus.FAILED:
            return (
                "failed",
                latest_source_job.error_message
                or f"{latest_source_job.kind.value.replace('_', ' ').title()} failed.",
            )
        if artifact is not None and artifact.alignment_status == "stale":
            return ("alignment_stale", "Transcript changed. Re-run alignment.")
        if artifact is not None and artifact.alignment_status == "ok":
            return ("aligned", "Alignment is ready.")
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
                    variant.source_reference_variant_id,
                ]
            ):
                raise ValueError("Source-recording variants must not mix other source kinds")
            recording = self._get_source_recording(session, variant.source_recording_id)
            if recording.batch_id != asset.project_id:
                raise ValueError("Reference variant source recording does not belong to the asset project")
        elif variant.source_kind == ReferenceSourceKind.REFERENCE_VARIANT:
            if variant.source_reference_variant_id is None:
                raise ValueError("Reference-derived variants require source_reference_variant_id")
            if any(
                value is not None
                for value in [
                    variant.source_recording_id,
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
            for recording in recordings:
                if recording.batch_id != project_id:
                    raise ValueError("Reference run recordings must belong to the selected project")

        raw_candidates: list[ReferenceCandidateSummary] = []
        for recording in recordings:
            total_duration = recording.duration_s
            speech_regions = self._reference_candidate_regions(recording)
            for window_seconds in durations:
                for start_seconds, end_seconds in self._candidate_time_windows(
                    speech_regions,
                    window_seconds,
                    stride_ratio,
                ):
                    if end_seconds - start_seconds < 1.0:
                        continue
                    transcript_text, speaker_name, language = None, None, None
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
        return self._extract_source_window_wav_bytes(recording, start_seconds, end_seconds)

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
        deleted_count = 0
        for root_name in ("slices", "peaks", "variants", "review-window-renders", "review-window-variants", "review-windows"):
            root = self.media_root / root_name
            if not root.exists():
                continue
            for path in root.rglob("*"):
                if path.is_file():
                    path.unlink()
                    deleted_count += 1
        return deleted_count


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

    def _migrate_legacy_seed_source_recording_media(self) -> None:
        if not self.legacy_seed_path.exists():
            return
        legacy = json.loads(self.legacy_seed_path.read_text())
        source_audio_overrides = self._legacy_seed_source_audio_overrides(legacy.get("clips_by_project", {}))
        with self._session() as session:
            recordings = session.exec(select(SourceRecording)).all()
            updated = False
            for recording in recordings:
                if recording.processing_recipe == LEGACY_SEED_SOURCE_RECORDING_PROCESSING_RECIPE:
                    continue
                source_prefix = f"source-{recording.batch_id}-"
                source_file_id = recording.id[len(source_prefix) :] if recording.id.startswith(source_prefix) else ""
                source_audio_path = source_audio_overrides.get((recording.batch_id, source_file_id))
                if source_audio_path is None:
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
            for raw_path in session.exec(select(ReferenceVariant.file_path)).all():
                try:
                    retained_paths.add(self._resolve_reference_variant_media_path(raw_path))
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
