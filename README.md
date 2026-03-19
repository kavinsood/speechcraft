# Speechcraft

Speechcraft is a browser-first workstation for building clean, reviewable speech datasets for voice model fine-tuning.

The current implementation is focused on Phase 1 of the product:

- load a clip review project
- inspect candidate clips
- review and edit transcripts
- track clip status and tags
- prepare accepted clips for export
- export accepted committed clips as rendered audio plus a `.list`

## Project Layout

- `backend/`: FastAPI API for the Phase 1 clip review domain
- `frontend/`: React + Vite UI for the Clip Prep Workstation
- `docs/`: product, architecture, and implementation notes

## Core Docs

- `docs/Audio-Labeling-WebUI-Design-2026-03-01.md`: original design conversation that set the project direction
- `docs/Phase_1_Clip_Prep_Workstation_Contract.md`: the Phase 1 product boundary
- `docs/Phase_1_Workflow_and_State_Model.md`: lifecycle and state transitions
- `docs/Phase_1_Data_Model.md`: logical data model
- `docs/Current_Repo_State_and_Next_Steps.md`: where the repo stands now, what was completed, and what comes next
- `INSTALL.md`: deterministic setup and bootstrap instructions for humans and agents

## Current Status

This repository currently contains a working Phase 1 scaffold for the Clip Prep Workstation.

What is already implemented:

- SQLite-backed backend persistence
- clip review queue and project stats
- transcript editing
- tag editing and filtering
- clip status changes
- per-clip EDL operations
- undo / redo
- split / merge
- commit history
- waveform display
- clip audio preview
- export preview
- export runs
- request-level backend integration tests
- non-destructive backend smoke script

What is still demo-grade:

- waveform and audio are generated deterministically for now
- clips are not yet rebuilt from real source audio via FFmpeg
- upstream preprocessing (denoise, segmentation, ASR) is not in this repo yet

## Quick Start

The best setup path is in `INSTALL.md`.

The easiest repo-level workflow is now:

```bash
make setup
make check
```

Then, in two terminals:

```bash
make dev-backend
```

```bash
make dev-frontend
```

If you just want the shortest working commands:

### Backend

```bash
cd backend
UV_CACHE_DIR=/tmp/uv-cache uv sync
uv run uvicorn app.main:app --reload
```

### Frontend

```bash
cd frontend
npm install
npm run dev
```

## URLs

- frontend: `http://127.0.0.1:5173`
- backend: `http://127.0.0.1:8000`
- backend docs: `http://127.0.0.1:8000/docs`

## Local State

When the backend starts, it maintains local runtime state here:

- `backend/data/project.db`
- `backend/data/media/`
- `backend/exports/`

These files are expected and are part of the current demo workflow.

## Verification

Use these quick checks after setup:

```bash
make check
make smoke-backend
```

You can also run the smoke script directly against a running backend:

```bash
cd backend
uv run python scripts/smoke_backend.py --base-url http://127.0.0.1:8000
```

## Environment Notes

- The frontend talks to `http://127.0.0.1:8000` by default.
- Override the API target with `VITE_API_BASE_URL` if needed.
- Override backend CORS allowlists with `SPEECHCRAFT_ALLOWED_ORIGINS` as a comma-separated origin list.
- Tests can isolate backend runtime paths with `SPEECHCRAFT_DB_PATH`, `SPEECHCRAFT_LEGACY_SEED_PATH`, `SPEECHCRAFT_MEDIA_ROOT`, and `SPEECHCRAFT_EXPORTS_ROOT`.
- `uv` is the preferred backend workflow.
- `npm` is the preferred frontend workflow.
- `bun` can work for the frontend, but `npm` is the default repo path.
- `make help` prints the root-level helper commands.
