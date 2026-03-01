from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

from .models import (
    Clip,
    ClipCommit,
    ClipCommitCreate,
    ClipEdlUpdate,
    ClipHistoryResult,
    ClipMutationResult,
    ClipSplitRequest,
    ClipStatusUpdate,
    ClipTagUpdate,
    ClipTranscriptUpdate,
    ExportRun,
    ExportPreview,
    Project,
    ProjectDetail,
    WaveformPeaks,
)
from .repository import repository


app = FastAPI(
    title="Speechcraft API",
    version="0.1.0",
    description="Phase 1 Clip Prep Workstation API",
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


@app.get("/api/projects", response_model=list[Project])
def list_projects() -> list[Project]:
    return repository.list_projects()


@app.get("/api/projects/{project_id}", response_model=ProjectDetail)
def get_project_detail(project_id: str) -> ProjectDetail:
    try:
        return repository.get_project_detail(project_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Project not found") from exc


@app.get("/api/projects/{project_id}/clips", response_model=list[Clip])
def list_project_clips(project_id: str) -> list[Clip]:
    try:
        return repository.get_project_clips(project_id)
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


@app.get("/api/clips/{clip_id}/commits", response_model=list[ClipCommit])
def list_clip_commits(clip_id: str) -> list[ClipCommit]:
    try:
        return repository.get_clip_commits(clip_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Clip not found") from exc


@app.patch("/api/clips/{clip_id}/status", response_model=Clip)
def update_clip_status(clip_id: str, payload: ClipStatusUpdate) -> Clip:
    try:
        return repository.update_clip_status(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Clip not found") from exc


@app.patch("/api/clips/{clip_id}/transcript", response_model=Clip)
def update_clip_transcript(clip_id: str, payload: ClipTranscriptUpdate) -> Clip:
    try:
        return repository.update_clip_transcript(clip_id, payload.text_current)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Clip not found") from exc


@app.patch("/api/clips/{clip_id}/tags", response_model=Clip)
def update_clip_tags(clip_id: str, payload: ClipTagUpdate) -> Clip:
    try:
        return repository.update_clip_tags(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Clip not found") from exc


@app.post("/api/clips/{clip_id}/edl", response_model=Clip)
def append_clip_edl_operation(clip_id: str, payload: ClipEdlUpdate) -> Clip:
    try:
        return repository.append_edl_operation(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Clip not found") from exc


@app.post("/api/clips/{clip_id}/commit", response_model=ClipCommit)
def commit_clip(clip_id: str, payload: ClipCommitCreate) -> ClipCommit:
    try:
        return repository.commit_clip(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Clip not found") from exc


@app.post("/api/clips/{clip_id}/undo", response_model=ClipHistoryResult)
def undo_clip(clip_id: str) -> ClipHistoryResult:
    try:
        return repository.undo_clip(clip_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Clip not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/clips/{clip_id}/redo", response_model=ClipHistoryResult)
def redo_clip(clip_id: str) -> ClipHistoryResult:
    try:
        return repository.redo_clip(clip_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Clip not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/clips/{clip_id}/split", response_model=ClipMutationResult)
def split_clip(clip_id: str, payload: ClipSplitRequest) -> ClipMutationResult:
    try:
        return repository.split_clip(clip_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Clip not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/clips/{clip_id}/merge-next", response_model=ClipMutationResult)
def merge_with_next_clip(clip_id: str) -> ClipMutationResult:
    try:
        return repository.merge_with_next_clip(clip_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Clip not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/clips/{clip_id}/waveform-peaks", response_model=WaveformPeaks)
def get_waveform_peaks(clip_id: str, bins: int = 120) -> WaveformPeaks:
    try:
        return repository.get_waveform_peaks(clip_id, bins)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Clip not found") from exc


@app.get("/api/clips/{clip_id}/audio")
def get_clip_audio(clip_id: str) -> Response:
    try:
        return Response(
            content=repository.get_clip_audio_bytes(clip_id),
            media_type="audio/wav",
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Clip not found") from exc
