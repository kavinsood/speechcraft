from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from .models import (
    ActiveVariantUpdate,
    AudioVariantCreate,
    AudioVariantRunRequest,
    ClipLabItemView,
    DatasetProcessingRunRequest,
    DatasetProcessingRunView,
    ExportPreview,
    ExportRun,
    ForcedAlignAndPackRequest,
    ImportBatchCreate,
    MediaCleanupResult,
    ProcessingJobView,
    ProjectSummary,
    RecordingDerivativeCreate,
    ReferenceAsset,
    ReferenceAssetCreate,
    ReviewWindowAsrRequest,
    ReviewWindowView,
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


@app.get("/api/projects/{project_id}/slices", response_model=list[SliceSummary])
def list_project_slices(project_id: str) -> list[SliceSummary]:
    try:
        return repository.get_project_slices(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.get("/api/projects/{project_id}/review-windows", response_model=list[ReviewWindowView])
def list_project_review_windows(project_id: str) -> list[ReviewWindowView]:
    try:
        return repository.list_project_review_windows(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.get("/api/slices/{slice_id}", response_model=SliceDetail)
def get_slice_detail(slice_id: str) -> SliceDetail:
    try:
        return repository.get_slice_detail(slice_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc


@app.get("/api/clip-lab-items/{item_kind}/{item_id}", response_model=ClipLabItemView)
def get_clip_lab_item(item_kind: str, item_id: str) -> ClipLabItemView:
    try:
        return repository.get_clip_lab_item(item_kind, item_id)
    except KeyError as exc:
        detail = "Slice not found" if item_kind == "slice" else "Review window not found"
        raise HTTPException(status_code=404, detail=detail) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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


@app.get("/api/clip-lab-items/{item_kind}/{item_id}/waveform-peaks", response_model=WaveformPeaks)
def get_clip_lab_item_waveform_peaks(item_kind: str, item_id: str, bins: int = 120) -> WaveformPeaks:
    try:
        return repository.get_clip_lab_waveform_peaks(item_kind, item_id, bins)
    except KeyError as exc:
        detail = "Slice not found" if item_kind == "slice" else "Review window not found"
        raise HTTPException(status_code=404, detail=detail) from exc
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


@app.patch("/api/review-windows/{review_window_id}/status", response_model=ClipLabItemView)
def update_review_window_status(
    review_window_id: str,
    payload: SliceStatusUpdate,
) -> ClipLabItemView:
    try:
        return repository.update_review_window_status(review_window_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Review window not found") from exc


@app.patch("/api/review-windows/{review_window_id}/transcript", response_model=ClipLabItemView)
def update_review_window_transcript(
    review_window_id: str,
    payload: SliceTranscriptUpdate,
) -> ClipLabItemView:
    try:
        return repository.update_review_window_transcript(review_window_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Review window not found") from exc


@app.patch("/api/review-windows/{review_window_id}/tags", response_model=ClipLabItemView)
def update_review_window_tags(
    review_window_id: str,
    payload: SliceTagUpdate,
) -> ClipLabItemView:
    try:
        return repository.update_review_window_tags(review_window_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Review window not found") from exc


@app.post("/api/review-windows/{review_window_id}/save", response_model=ClipLabItemView)
def save_review_window_state(
    review_window_id: str,
    payload: SliceSaveRequest,
) -> ClipLabItemView:
    try:
        return repository.save_review_window_state(review_window_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Review window not found") from exc
    except SliceSaveValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/review-windows/{review_window_id}/edl", response_model=ClipLabItemView)
def append_review_window_edl_operation(
    review_window_id: str,
    payload: SliceEdlUpdate,
) -> ClipLabItemView:
    try:
        return repository.append_review_window_edl_operation(review_window_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Review window not found") from exc


@app.post("/api/review-windows/{review_window_id}/undo", response_model=ClipLabItemView)
def undo_review_window(review_window_id: str) -> ClipLabItemView:
    try:
        return repository.undo_review_window(review_window_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Review window not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/review-windows/{review_window_id}/redo", response_model=ClipLabItemView)
def redo_review_window(review_window_id: str) -> ClipLabItemView:
    try:
        return repository.redo_review_window(review_window_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Review window not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/review-windows/{review_window_id}/split", response_model=list[ReviewWindowView])
def split_review_window(
    review_window_id: str,
    payload: SliceSplitRequest,
) -> list[ReviewWindowView]:
    try:
        return repository.split_review_window(review_window_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Review window not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/review-windows/{review_window_id}/merge-next", response_model=list[ReviewWindowView])
def merge_with_next_review_window(review_window_id: str) -> list[ReviewWindowView]:
    try:
        return repository.merge_with_next_review_window(review_window_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Review window not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.patch("/api/review-windows/{review_window_id}/active-variant", response_model=ClipLabItemView)
def set_active_review_window_variant(
    review_window_id: str,
    payload: ActiveVariantUpdate,
) -> ClipLabItemView:
    try:
        return repository.set_active_review_window_variant(review_window_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Review window variant not found") from exc


@app.post("/api/review-windows/{review_window_id}/variants/run", response_model=ClipLabItemView)
def run_review_window_variant(
    review_window_id: str,
    payload: AudioVariantRunRequest,
) -> ClipLabItemView:
    try:
        return repository.run_review_window_variant(review_window_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Review window not found") from exc
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


@app.get("/media/review-windows/{review_window_id}.wav")
def get_review_window_media(review_window_id: str) -> FileResponse:
    try:
        path = repository.get_review_window_media_path(review_window_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Review window not found") from exc
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


@app.get("/api/source-recordings/{recording_id}/review-windows", response_model=list[ReviewWindowView])
def list_review_windows(recording_id: str) -> list[ReviewWindowView]:
    try:
        return repository.list_review_windows(recording_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Source recording not found") from exc


@app.post("/api/source-recordings/{recording_id}/slice-handoff", response_model=list[ReviewWindowView])
def register_slicer_chunks(recording_id: str, payload: SlicerHandoffRequest) -> list[ReviewWindowView]:
    try:
        return repository.register_slicer_chunks(recording_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Source recording not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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


@app.post(
    "/api/source-recordings/{recording_id}/dataset-processing",
    response_model=DatasetProcessingRunView,
    status_code=202,
)
def start_dataset_processing_run(
    recording_id: str,
    payload: DatasetProcessingRunRequest,
) -> DatasetProcessingRunView:
    try:
        return repository.start_dataset_processing_run(recording_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Source recording not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get(
    "/api/source-recordings/{recording_id}/processing-status",
    response_model=DatasetProcessingRunView,
)
def get_source_recording_processing_status(recording_id: str) -> DatasetProcessingRunView:
    try:
        return repository.get_source_recording_processing_status(recording_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Dataset processing run not found") from exc


@app.post(
    "/api/source-recordings/{recording_id}/review-window-asr",
    response_model=ProcessingJobView,
    status_code=202,
)
def enqueue_review_window_asr(
    recording_id: str,
    payload: ReviewWindowAsrRequest,
) -> ProcessingJobView:
    try:
        return repository.enqueue_review_window_asr(recording_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Source recording not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post(
    "/api/source-recordings/{recording_id}/forced-align-and-pack",
    response_model=ProcessingJobView,
    status_code=202,
)
def enqueue_forced_align_and_pack(
    recording_id: str,
    payload: ForcedAlignAndPackRequest,
) -> ProcessingJobView:
    try:
        return repository.enqueue_forced_align_and_pack(recording_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Source recording not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/reference-assets", response_model=ReferenceAsset)
def create_reference_asset(payload: ReferenceAssetCreate) -> ReferenceAsset:
    try:
        return repository.create_reference_asset(payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Audio variant not found") from exc
