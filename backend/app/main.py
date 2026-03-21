from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from .models import (
    ActiveVariantUpdate,
    AudioVariantCreate,
    AudioVariantRunRequest,
    ExportPreview,
    ExportRun,
    ImportBatchCreate,
    MediaCleanupResult,
    ProjectSummary,
    RecordingDerivativeCreate,
    ReferenceAssetCreateFromCandidate,
    ReferenceAssetCreateFromSlice,
    ReferenceAssetDetail,
    ReferenceAssetSummary,
    ReferenceCandidateSummary,
    ReferenceRunCreate,
    ReferenceRunRerankRequest,
    ReferenceRunRerankResponse,
    ReferenceRunView,
    SliceSaveRequest,
    SliceDetail,
    SliceSummary,
    SliceEdlUpdate,
    SliceSplitRequest,
    SliceStatusUpdate,
    SliceTagUpdate,
    SliceTranscriptUpdate,
    SourceRecording,
    SourceRecordingCreate,
    SourceRecordingView,
    SlicerHandoffRequest,
    WaveformPeaks,
)
from .repository import SliceSaveValidationError, repository


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


@app.get("/api/projects", response_model=list[ProjectSummary])
def list_projects() -> list[ProjectSummary]:
    return repository.list_projects()


@app.get("/api/projects/{project_id}", response_model=ProjectSummary)
def get_project(project_id: str) -> ProjectSummary:
    try:
        return repository.get_project(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.get("/api/projects/{project_id}/source-recordings", response_model=list[SourceRecordingView])
def list_project_source_recordings(project_id: str) -> list[SourceRecordingView]:
    try:
        return repository.list_source_recordings(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.get("/api/projects/{project_id}/slices", response_model=list[SliceSummary])
def list_project_slices(project_id: str) -> list[SliceSummary]:
    try:
        return repository.get_project_slices(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.get("/api/slices/{slice_id}", response_model=SliceDetail)
def get_slice_detail(slice_id: str) -> SliceDetail:
    try:
        return repository.get_slice_detail(slice_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc


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


@app.post("/api/projects/{project_id}/media-cleanup", response_model=MediaCleanupResult)
def cleanup_project_media(project_id: str) -> MediaCleanupResult:
    try:
        return repository.cleanup_project_media(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.patch("/api/clips/{clip_id}/status", response_model=SliceDetail)
def update_slice_status(clip_id: str, payload: SliceStatusUpdate) -> SliceDetail:
    try:
        return repository.update_slice_status(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc


@app.patch("/api/clips/{clip_id}/transcript", response_model=SliceDetail)
def update_slice_transcript(clip_id: str, payload: SliceTranscriptUpdate) -> SliceDetail:
    try:
        return repository.update_slice_transcript(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc


@app.patch("/api/clips/{clip_id}/tags", response_model=SliceDetail)
def update_slice_tags(clip_id: str, payload: SliceTagUpdate) -> SliceDetail:
    try:
        return repository.update_slice_tags(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc


@app.post("/api/clips/{clip_id}/save", response_model=SliceDetail)
def save_slice_state(clip_id: str, payload: SliceSaveRequest) -> SliceDetail:
    try:
        return repository.save_slice_state(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc
    except SliceSaveValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/clips/{clip_id}/edl", response_model=SliceDetail)
def append_slice_edl_operation(clip_id: str, payload: SliceEdlUpdate) -> SliceDetail:
    try:
        return repository.append_edl_operation(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc


@app.post("/api/clips/{clip_id}/undo", response_model=SliceDetail)
def undo_slice(clip_id: str) -> SliceDetail:
    try:
        return repository.undo_slice(clip_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/clips/{clip_id}/redo", response_model=SliceDetail)
def redo_slice(clip_id: str) -> SliceDetail:
    try:
        return repository.redo_slice(clip_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/clips/{clip_id}/split", response_model=list[SliceSummary])
def split_slice(clip_id: str, payload: SliceSplitRequest) -> list[SliceSummary]:
    try:
        return repository.split_slice(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/clips/{clip_id}/merge-next", response_model=list[SliceSummary])
def merge_with_next_slice(clip_id: str) -> list[SliceSummary]:
    try:
        return repository.merge_with_next_slice(clip_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/clips/{clip_id}/waveform-peaks", response_model=WaveformPeaks)
def get_waveform_peaks(clip_id: str, bins: int = 120) -> WaveformPeaks:
    try:
        return repository.get_waveform_peaks(clip_id, bins)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.patch("/api/clips/{clip_id}/active-variant", response_model=SliceDetail)
def set_active_variant(clip_id: str, payload: ActiveVariantUpdate) -> SliceDetail:
    try:
        return repository.set_active_variant(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Audio variant not found") from exc


@app.post("/api/clips/{clip_id}/variants", response_model=SliceDetail)
def create_audio_variant(clip_id: str, payload: AudioVariantCreate) -> SliceDetail:
    try:
        return repository.create_audio_variant(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/clips/{clip_id}/variants/run", response_model=SliceDetail)
def run_audio_variant(clip_id: str, payload: AudioVariantRunRequest) -> SliceDetail:
    try:
        return repository.run_audio_variant(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/media/variants/{variant_id}.wav")
def get_variant_media(variant_id: str) -> FileResponse:
    try:
        path = repository.get_variant_media_path(variant_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Audio variant not found") from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Audio file missing: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return FileResponse(path=path, media_type="audio/wav")


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


@app.get("/media/slices/{slice_id}.wav")
def get_slice_media(slice_id: str) -> FileResponse:
    try:
        path = repository.get_slice_media_path(slice_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Audio file missing: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return FileResponse(path=path, media_type="audio/wav")


@app.post("/api/import-batches", response_model=ProjectSummary)
def create_import_batch(payload: ImportBatchCreate) -> ProjectSummary:
    return repository.create_import_batch(payload)


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


@app.post("/api/reference-assets/from-slice", response_model=ReferenceAssetDetail)
def create_reference_asset_from_slice(
    payload: ReferenceAssetCreateFromSlice,
) -> ReferenceAssetDetail:
    try:
        return repository.create_reference_asset_from_slice(payload)
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


@app.post("/api/source-recordings/{recording_id}/slice-handoff", response_model=list[SliceDetail])
def register_slicer_chunks(recording_id: str, payload: SlicerHandoffRequest) -> list[SliceDetail]:
    try:
        return repository.register_slicer_chunks(recording_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Source recording not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
