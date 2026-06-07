from __future__ import annotations

import os
import shutil
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from .dataset_worker_client import run_dataset_worker_preflight
from .dataset_runs import (
    create_dataset_run,
    get_candidate_review_media_path,
    get_dataset_run,
    get_dataset_run_log,
    get_dataset_speaker_results,
    get_dataset_slicer_results,
    get_speaker_sample_media_path,
    list_dataset_runs,
    refresh_dataset_run,
    resume_dataset_run_processing,
    rerun_dataset_slicer,
    save_dataset_speaker_selection,
    start_dataset_run,
)
from .models import (
    DatasetRunCreateRequest,
    DatasetRunResumeRequest,
    DatasetRunLogView,
    DatasetSpeakerResultsView,
    DatasetSpeakerSelectionUpdateRequest,
    DatasetSpeakerSelectionView,
    DatasetRunView,
    DatasetSlicerResultsView,
    DatasetSlicerRerunRequest,
    ExportPreview,
    ExportRun,
    ImportBatchCreate,
    ProcessingJobView,
    ProjectPreparationRequest,
    ProjectPreparationRun,
    ProjectRecordingJobsRun,
    ProjectSummary,
    RecordingDerivativeCreate,
    ReferenceAssetCreateFromCandidate,
    ReferenceAssetDetail,
    ReferenceAssetSummary,
    ReferenceEmbeddingEvaluationRequest,
    ReferenceEmbeddingEvaluationResponse,
    ReferenceCandidateSummary,
    ReferenceRunCreate,
    ReferenceRunRerankRequest,
    ReferenceRunRerankResponse,
    ReferenceRunView,
    SourceAlignmentRequest,
    SourceRecording,
    SourceRecordingArtifactView,
    SourceRecordingQueueView,
    SourceRecordingCreate,
    SourceRecordingView,
    SourceTranscriptionRequest,
)
from .repository import repository


ALLOWED_WAV_CONTENT_TYPES = {
    "",
    "application/octet-stream",
    "audio/vnd.wave",
    "audio/wav",
    "audio/wave",
    "audio/x-wav",
}


DEFAULT_ALLOWED_ORIGINS = (
    "http://127.0.0.1:4173",
    "http://127.0.0.1:5173",
    "http://localhost:4173",
    "http://localhost:5173",
)


def get_allowed_origins(raw_value: str | None = None) -> list[str]:
    env_value = raw_value if raw_value is not None else os.getenv("SPEECHCRAFT_ALLOWED_ORIGINS")
    if env_value is None or not env_value.strip():
        return list(DEFAULT_ALLOWED_ORIGINS)

    origins: list[str] = []
    for origin in env_value.split(","):
        normalized = origin.strip().rstrip("/")
        if not normalized:
            continue
        if normalized == "*":
            raise ValueError("SPEECHCRAFT_ALLOWED_ORIGINS cannot include '*' when credentials are enabled")
        if normalized not in origins:
            origins.append(normalized)

    return origins or list(DEFAULT_ALLOWED_ORIGINS)


app = FastAPI(
    title="Speechcraft API",
    version="0.3.0",
    description="SQLite-backed API for the Speechcraft labeling workstation",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/healthz")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/system/preflight")
def system_preflight(artifact_root: str | None = None) -> dict[str, object]:
    return run_dataset_worker_preflight(artifact_root=artifact_root)


@app.get("/api/projects/{project_id}/dataset-runs", response_model=list[DatasetRunView])
def list_project_dataset_runs(project_id: str) -> list[DatasetRunView]:
    try:
        return list_dataset_runs(repository, project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/projects/{project_id}/dataset-runs", response_model=DatasetRunView, status_code=201)
def create_project_dataset_run(project_id: str, payload: DatasetRunCreateRequest) -> DatasetRunView:
    try:
        return create_dataset_run(repository, project_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/dataset-runs/{run_id}", response_model=DatasetRunView)
def read_dataset_run(run_id: str) -> DatasetRunView:
    try:
        return get_dataset_run(repository, run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/dataset-runs/{run_id}/start", response_model=DatasetRunView, status_code=202)
def start_project_dataset_run(run_id: str) -> DatasetRunView:
    try:
        return start_dataset_run(repository, run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/dataset-runs/{run_id}/refresh", response_model=DatasetRunView)
def refresh_project_dataset_run(run_id: str) -> DatasetRunView:
    try:
        return refresh_dataset_run(repository, run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/dataset-runs/{run_id}/log", response_model=DatasetRunLogView)
def read_dataset_run_log(run_id: str) -> DatasetRunLogView:
    try:
        return get_dataset_run_log(repository, run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/dataset-runs/{run_id}/speakers", response_model=DatasetSpeakerResultsView)
def read_dataset_speakers(run_id: str) -> DatasetSpeakerResultsView:
    try:
        return get_dataset_speaker_results(repository, run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.put("/api/dataset-runs/{run_id}/speaker-selection", response_model=DatasetSpeakerSelectionView)
def update_dataset_speaker_selection(
    run_id: str,
    payload: DatasetSpeakerSelectionUpdateRequest,
) -> DatasetSpeakerSelectionView:
    try:
        return save_dataset_speaker_selection(repository, run_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/dataset-runs/{run_id}/resume-processing", response_model=DatasetRunView, status_code=202)
def resume_project_dataset_run(
    run_id: str,
    payload: DatasetRunResumeRequest,
) -> DatasetRunView:
    try:
        return resume_dataset_run_processing(repository, run_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/dataset-runs/{run_id}/slicer-rerun", response_model=DatasetRunView, status_code=202)
def rerun_project_dataset_slicer(run_id: str, payload: DatasetSlicerRerunRequest) -> DatasetRunView:
    try:
        return rerun_dataset_slicer(repository, run_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/api/dataset-runs/{run_id}/slicer-results", response_model=DatasetSlicerResultsView)
def read_dataset_slicer_results(run_id: str) -> DatasetSlicerResultsView:
    try:
        return get_dataset_slicer_results(repository, run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/media/dataset-runs/{run_id}/candidate-review/{clip_id}.wav")
def get_dataset_candidate_review_media(run_id: str, clip_id: str) -> FileResponse:
    try:
        path = get_candidate_review_media_path(repository, run_id, clip_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FileResponse(path=path, media_type="audio/wav")


@app.get("/media/dataset-runs/{run_id}/speaker-samples/{sample_id}.wav")
def get_dataset_speaker_sample_media(run_id: str, sample_id: str) -> FileResponse:
    try:
        path = get_speaker_sample_media_path(repository, run_id, sample_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FileResponse(path=path, media_type="audio/wav")


@app.get("/api/projects", response_model=list[ProjectSummary])
def list_projects() -> list[ProjectSummary]:
    return repository.list_projects()


@app.get("/api/projects/{project_id}", response_model=ProjectSummary)
def get_project(project_id: str) -> ProjectSummary:
    try:
        return repository.get_project(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.delete("/api/projects/{project_id}")
def delete_project(project_id: str) -> dict[str, int | str]:
    try:
        return repository.delete_project(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.get("/api/projects/{project_id}/source-recordings", response_model=list[SourceRecordingView])
def list_project_source_recordings(project_id: str) -> list[SourceRecordingView]:
    try:
        return repository.list_source_recordings(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.post("/api/projects/{project_id}/source-recordings/upload", response_model=SourceRecordingView)
async def upload_project_source_recording(
    project_id: str,
    file: UploadFile = File(...),
) -> SourceRecordingView:
    filename = Path(file.filename or "").name
    if not filename.lower().endswith(".wav"):
        raise HTTPException(status_code=400, detail="Only WAV files are supported right now")
    if (file.content_type or "") not in ALLOWED_WAV_CONTENT_TYPES:
        raise HTTPException(status_code=400, detail="Only WAV files are supported right now")

    try:
        repository.get_project(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc

    recording_id = repository.new_source_recording_id()
    target_path = repository.managed_source_recording_path(recording_id)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer, length=1024 * 1024)
        channels, _sample_width, sample_rate, frames = repository.read_pcm_wav_header(target_path)
        recording = repository.create_source_recording(
            SourceRecordingCreate(
                id=recording_id,
                batch_id=project_id,
                file_path=str(target_path),
                sample_rate=sample_rate,
                num_channels=channels,
                num_samples=frames,
            )
        )
        repository.set_source_recording_artifact_paths(
            recording_id,
            artifact_metadata={
                "original_filename": filename,
                "upload_content_type": file.content_type or "",
            },
        )
        return next(item for item in repository.list_source_recordings(project_id) if item.id == recording.id)
    except KeyError as exc:
        target_path.unlink(missing_ok=True)
        raise HTTPException(status_code=404, detail="Project not found") from exc
    except ValueError as exc:
        target_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        await file.close()


@app.get("/api/projects/{project_id}/recordings", response_model=list[SourceRecordingQueueView])
def list_project_recordings(project_id: str) -> list[SourceRecordingQueueView]:
    try:
        return repository.list_project_recordings(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.post("/api/projects/{project_id}/preparation", response_model=ProjectPreparationRun, status_code=202)
def run_project_preparation(
    project_id: str,
    payload: ProjectPreparationRequest,
) -> ProjectPreparationRun:
    try:
        return repository.run_project_preparation(project_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/projects/{project_id}/preparation-jobs", response_model=list[ProcessingJobView])
def list_project_preparation_jobs(project_id: str) -> list[ProcessingJobView]:
    try:
        return repository.list_project_preparation_jobs(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.post("/api/projects/{project_id}/transcription", response_model=ProjectRecordingJobsRun, status_code=202)
def enqueue_project_transcription(
    project_id: str,
    payload: SourceTranscriptionRequest,
) -> ProjectRecordingJobsRun:
    try:
        return repository.enqueue_project_transcription(project_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/projects/{project_id}/alignment", response_model=ProjectRecordingJobsRun, status_code=202)
def enqueue_project_alignment(
    project_id: str,
    payload: SourceAlignmentRequest,
) -> ProjectRecordingJobsRun:
    try:
        return repository.enqueue_project_alignment(project_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/recordings/{recording_id}/artifacts", response_model=SourceRecordingArtifactView)
def get_source_recording_artifact(recording_id: str) -> SourceRecordingArtifactView:
    try:
        return repository.get_source_recording_artifact(recording_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Source recording not found") from exc


@app.get("/api/projects/{project_id}/export-preview", response_model=ExportPreview)
def get_export_preview(project_id: str) -> ExportPreview:
    try:
        return repository.get_export_preview(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.get("/api/projects/{project_id}/exports", response_model=list[ExportRun])
def list_export_runs(project_id: str) -> list[ExportRun]:
    try:
        return repository.list_export_runs(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.post("/api/projects/{project_id}/export", response_model=ExportRun)
def export_project(project_id: str) -> ExportRun:
    try:
        return repository.export_project(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/recordings/{recording_id}/jobs/transcription", response_model=ProcessingJobView)
def enqueue_source_transcription(
    recording_id: str,
    payload: SourceTranscriptionRequest,
) -> ProcessingJobView:
    try:
        return repository.enqueue_source_transcription(recording_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Source recording not found") from exc


@app.post("/api/recordings/{recording_id}/jobs/alignment", response_model=ProcessingJobView)
def enqueue_source_alignment(
    recording_id: str,
    payload: SourceAlignmentRequest,
) -> ProcessingJobView:
    try:
        return repository.enqueue_source_alignment(recording_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Source recording not found") from exc


@app.get("/media/reference-variants/{variant_id}.wav")
def get_reference_variant_media(variant_id: str) -> FileResponse:
    try:
        path = repository.get_reference_variant_media_path(variant_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Reference variant not found") from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Audio file missing: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return FileResponse(path=path, media_type="audio/wav")


@app.get("/media/source-recordings/{recording_id}/window.wav")
def get_source_recording_window_media(
    recording_id: str,
    start_seconds: float,
    end_seconds: float,
) -> FileResponse:
    try:
        path = repository.get_source_recording_window_media_path(recording_id, start_seconds, end_seconds)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Source recording not found") from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Audio file missing: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return FileResponse(path=path, media_type="audio/wav")


@app.post("/api/import-batches", response_model=ProjectSummary)
def create_import_batch(payload: ImportBatchCreate) -> ProjectSummary:
    try:
        return repository.create_import_batch(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/source-recordings", response_model=SourceRecording)
def create_source_recording(payload: SourceRecordingCreate) -> SourceRecording:
    try:
        return repository.create_source_recording(payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/source-recordings/{recording_id}/preprocess", response_model=SourceRecording)
def create_preprocessed_recording(
    recording_id: str,
    payload: RecordingDerivativeCreate,
) -> SourceRecording:
    try:
        return repository.create_preprocessed_recording(recording_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Source recording not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/projects/{project_id}/reference-assets", response_model=list[ReferenceAssetSummary])
def list_project_reference_assets(project_id: str) -> list[ReferenceAssetSummary]:
    try:
        return repository.list_reference_assets(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=f"Reference asset integrity error: {exc}") from exc


@app.get("/api/reference-assets/{asset_id}", response_model=ReferenceAssetDetail)
def get_reference_asset(asset_id: str) -> ReferenceAssetDetail:
    try:
        return repository.get_reference_asset(asset_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Reference asset not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=f"Reference asset integrity error: {exc}") from exc


@app.get("/api/projects/{project_id}/reference-runs", response_model=list[ReferenceRunView])
def list_project_reference_runs(project_id: str) -> list[ReferenceRunView]:
    try:
        return repository.list_reference_runs(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.post("/api/projects/{project_id}/reference-runs", response_model=ReferenceRunView)
def create_reference_run(project_id: str, payload: ReferenceRunCreate) -> ReferenceRunView:
    try:
        run = repository.create_reference_run(project_id, payload)
        repository.start_reference_run_worker(run.id)
        return run
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Missing entity: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/reference-runs/{run_id}", response_model=ReferenceRunView)
def get_reference_run(run_id: str) -> ReferenceRunView:
    try:
        return repository.get_reference_run(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Reference run not found") from exc


@app.get("/api/reference-runs/{run_id}/candidates", response_model=list[ReferenceCandidateSummary])
def list_reference_run_candidates(
    run_id: str,
    offset: int = 0,
    limit: int = 50,
    query: str | None = None,
) -> list[ReferenceCandidateSummary]:
    try:
        return repository.list_reference_run_candidates(run_id, offset=offset, limit=limit, query=query)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Reference run not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/reference-runs/{run_id}/rerank", response_model=ReferenceRunRerankResponse)
def rerank_reference_run_candidates(
    run_id: str,
    payload: ReferenceRunRerankRequest,
) -> ReferenceRunRerankResponse:
    try:
        return repository.rerank_reference_run_candidates(run_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Missing entity: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/reference-runs/{run_id}/embedding-evaluation", response_model=ReferenceEmbeddingEvaluationResponse)
def evaluate_reference_run_embeddings(
    run_id: str,
    payload: ReferenceEmbeddingEvaluationRequest,
) -> ReferenceEmbeddingEvaluationResponse:
    try:
        return repository.evaluate_reference_run_embeddings(run_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Missing entity: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/reference-assets/from-candidate", response_model=ReferenceAssetDetail)
def create_reference_asset_from_candidate(
    payload: ReferenceAssetCreateFromCandidate,
) -> ReferenceAssetDetail:
    try:
        return repository.create_reference_asset_from_candidate(payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Missing entity: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/media/reference-candidates/{run_id}/{candidate_id}.wav")
def get_reference_candidate_media(run_id: str, candidate_id: str) -> FileResponse:
    try:
        path = repository.get_reference_candidate_media_path(run_id, candidate_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Missing entity: {exc}") from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Audio file missing: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return FileResponse(path=path, media_type="audio/wav")


@app.get("/api/source-recordings/{recording_id}/jobs", response_model=list[ProcessingJobView])
def list_source_recording_jobs(recording_id: str) -> list[ProcessingJobView]:
    try:
        return repository.list_source_recording_jobs(recording_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Source recording not found") from exc


@app.get("/api/jobs/{job_id}", response_model=ProcessingJobView)
def get_processing_job(job_id: str) -> ProcessingJobView:
    try:
        return repository.get_processing_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Processing job not found") from exc
