# Current Repo State And Next Steps

## Purpose

This document describes the current shape of the Speechcraft repository after the native SQLite/media refactor.

Speechcraft is still focused on the Phase 1 clip-preparation workstation:

`smart segmentation + Whisper -> Clip Prep Workstation -> export -> fine-tuning`

The repo implements the review workstation side of that flow. It does not yet implement the upstream preprocessing stack or downstream training orchestration.

## What The Repo Contains Today

The current repository is a working Phase 1 application with persisted review state, managed media artifacts, and a browser-first editing UI.

### Backend

The backend lives in `backend/` and is a FastAPI app backed by SQLite/SQLModel.

Current backend capabilities:

- Project listing and project summaries, sorted by `updated_at`
- Lightweight queue loading through `GET /api/projects/{project_id}/slices`
- Full slice detail loading through `GET /api/slices/{slice_id}`
- Persisted review status, transcript, tag, variant, and EDL state
- Full-state slice revision history for undo/redo
- Saved milestone revisions through `POST /api/clips/{clip_id}/save`
- Slice split and merge
- Managed variant media serving at `GET /media/variants/{variant_id}.wav`
- Managed edited-slice media serving at `GET /media/slices/{slice_id}.wav`
- Cached slice render artifacts under `backend/data/media/slices/`
- Cached waveform peak artifacts under `backend/data/media/peaks/`
- Export preview and export runs
- Media cleanup for superseded slices, unused variants, and stale derived caches
- Legacy JSON import into SQLite on first startup when `backend/data/phase1-demo.json` exists

Current managed runtime data:

- SQLite database: `backend/data/project.db`
- Managed media root: `backend/data/media/`
- Export output: `backend/exports/<project_id>/<export_id>/`

Important implementation notes:

- The backend now persists to SQLite instead of rewriting one giant JSON blob.
- Media responses use `FileResponse`, so browser playback is range-friendly.
- Slice audio and waveform peaks are cached by audio state, not by every metadata revision.
- Source audio rendering is still demo-grade for built-in seed data; real FFmpeg/source-backed render jobs are still future work.

### Frontend

The frontend lives in `frontend/` and is a React + Vite app.

Current frontend capabilities:

- Review queue with search, tag filters, and hide-resolved behavior
- Lightweight queue payloads plus detail fetch for the active slice
- Slice editor with transcript draft editing and tag draft editing
- Immediate EDL actions for delete-range and insert-silence
- Split and merge actions
- Undo/redo over full persisted slice revisions
- Saved milestones for transcript/tag/status snapshots
- Variant switching and clip-lab model runs
- Backend-authoritative duration and backend-generated waveform peaks
- Edited-slice playback from the slice media route
- Export preview and export history
- Dev-only destructive tooling page at `/backend-test`
- Dev-only cleanup action with confirmation

## Decisions Reflected In The Current Code

The current codebase assumes:

- The logical unit of work is the slice
- Source recordings are immutable
- Physical audio variants are immutable managed files
- Slice edits are represented as EDL operations plus metadata state
- Undo/redo walks full-state slice revisions, not just audio math
- Milestones are user-facing saved revisions, not a separate shadow model
- Export uses the latest persisted accepted slice state
- Queue loading and detail loading are separate API concerns

## What Is Still Demo-Grade

The repository is usable, but a few parts are still intentionally not production-complete:

- Built-in demo data still uses deterministic synthetic audio
- Rendering is still request-path work, not FFmpeg jobs with retries/progress
- Peak generation is cached, but there is still no background worker/orchestrator
- Queue rendering is still non-virtualized on the frontend
- Timeline tick rendering is still duration-driven rather than viewport-driven
- Keyboard-first review ergonomics are still incomplete

## Recommended Next Build Order

If continuing from the current repo, the highest-value next steps are:

1. Replace synthetic render paths with source-backed FFmpeg rendering.
2. Add a real background job layer for slice render, peak generation, and export work.
3. Virtualize the queue and reduce duration-linear DOM work in the editor timeline.
4. Expand keyboard-first review workflows and shortcut discoverability.
5. Build the upstream ingest/preprocess path that feeds this workstation.

## Current Limits

This repository is best understood as:

- a real persisted Phase 1 workstation
- a solid architectural foundation
- not yet a full production audio processing pipeline

It now has the correct storage, media-serving, and workflow shape for Phase 1, but it still needs source-backed rendering and background job infrastructure before it should be treated like a heavy-duty production system.
