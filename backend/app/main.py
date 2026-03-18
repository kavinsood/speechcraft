from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from .models import (
    ActiveVariantUpdate,
    AudioVariantCreate,
    AudioVariantRunRequest,
    ExportPreview,
    ExportRun,
    ImportBatch,
    ImportBatchCreate,
    MediaCleanupResult,
    RecordingDerivativeCreate,
    ReferenceAsset,
    ReferenceAssetCreate,
    SliceDetail,
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
from .repository import repository


app = FastAPI(
    title="Speechcraft API",
    version="0.3.0",
    description="SQLite-backed API for the Speechcraft labeling workstation",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/healthz")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/projects", response_model=list[ImportBatch])
def list_projects() -> list[ImportBatch]:
    return repository.list_projects()


@app.get("/api/projects/{project_id}", response_model=ImportBatch)
def get_project(project_id: str) -> ImportBatch:
    try:
        return repository.get_project(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.get("/api/projects/{project_id}/slices", response_model=list[SliceDetail])
def list_project_slices(project_id: str) -> list[SliceDetail]:
    try:
        return repository.get_project_slices(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


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


@app.post("/api/clips/{clip_id}/split", response_model=list[SliceDetail])
def split_slice(clip_id: str, payload: SliceSplitRequest) -> list[SliceDetail]:
    try:
        return repository.split_slice(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Slice not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/clips/{clip_id}/merge-next", response_model=list[SliceDetail])
def merge_with_next_slice(clip_id: str) -> list[SliceDetail]:
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


@app.post("/api/import-batches", response_model=ImportBatch)
def create_import_batch(payload: ImportBatchCreate) -> ImportBatch:
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


@app.post("/api/source-recordings/{recording_id}/slice-handoff", response_model=list[SliceDetail])
def register_slicer_chunks(recording_id: str, payload: SlicerHandoffRequest) -> list[SliceDetail]:
    try:
        return repository.register_slicer_chunks(recording_id, payload)
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
