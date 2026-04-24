# Install And Bootstrap

This guide is for humans and coding agents setting up Speechcraft locally.

## Prerequisites

Preferred tools:

- Python `3.11+`
- Node.js `20+`
- npm `10+`
- `uv`
- `make`
- `ffmpeg` available on `PATH` for audio preparation/export paths

Optional:

- NVIDIA GPU drivers/CUDA for faster ASR
- `sqlite3` CLI for inspecting local workstation state

## One-Time Setup

From the repo root:

```bash
make setup
```

That runs:

- backend dependency installation through `uv sync`
- frontend dependency installation through `npm install`

You can also run them separately:

```bash
make setup-backend
make setup-frontend
```

## Run The App

Open two terminals from the repo root.

### Terminal 1: Backend API + Worker

```bash
make dev-backend
```

This starts both:

- FastAPI API
- `ProcessingJob` worker

The worker must be running for preparation, ASR, alignment, slicer jobs, and other queued work.

Default backend URL:

- `http://127.0.0.1:8010`
- `http://127.0.0.1:8010/docs`

### Terminal 2: Frontend

```bash
make dev-frontend
```

Default frontend URL:

- `http://127.0.0.1:5173`

The Makefile points the frontend at `http://127.0.0.1:8010` by default.

## Run API And Worker Separately

For debugging, use separate terminals:

```bash
make dev-api
```

```bash
make dev-worker
```

## Ports And Overrides

Makefile defaults:

- `BACKEND_HOST=127.0.0.1`
- `BACKEND_PORT=8010`
- `FRONTEND_API_BASE_URL=http://127.0.0.1:8010`

Examples:

```bash
BACKEND_PORT=8000 make dev-backend
```

```bash
FRONTEND_API_BASE_URL=http://127.0.0.1:8000 make dev-frontend
```

The frontend can also be configured directly with:

```bash
VITE_API_BASE_URL=http://127.0.0.1:8010 npm run dev
```

## ASR Configuration

Speechcraft uses `faster-whisper` for real ASR by default.

CPU default:

```bash
make dev-backend
```

CUDA example:

```bash
ASR_DEVICE=cuda ASR_COMPUTE_TYPE=float16 make dev-backend
```

Useful ASR environment variables:

- `ASR_BACKEND=faster_whisper`
- `ASR_DEVICE=cpu` or `cuda`
- `ASR_COMPUTE_TYPE=int8`, `float16`, or another faster-whisper-supported compute type
- `ASR_MODEL_PATH=/absolute/path/to/model-or-model-name`

The UI option `turbo` maps to faster-whisper model `large-v3-turbo`.

First ASR run may download the selected model into the local model/cache path used by faster-whisper/Hugging Face tooling.

The stub ASR backend is blocked unless this is explicitly set:

```bash
SPEECHCRAFT_ALLOW_STUB_ASR=1
```

Use that only for tests or controlled smoke work. It writes placeholder transcripts and is not valid for real slicing.

## Verification

Run all normal checks:

```bash
make check
```

Backend only:

```bash
make check-backend
```

Frontend only:

```bash
make check-frontend
```

Focused commands:

```bash
python3 -m compileall backend/app
```

```bash
cd backend
uv run python -m unittest discover -s tests -p 'test_*.py'
```

```bash
cd frontend
npm run build
```

Smoke test against a running backend:

```bash
make smoke-backend
```

If your backend is not on the default port:

```bash
SMOKE_BACKEND_BASE_URL=http://127.0.0.1:8000 make smoke-backend
```

## Local Runtime State

Expected local files/directories:

- `backend/.venv/`
- `frontend/node_modules/`
- `frontend/dist/`
- `backend/data/project.db`
- `backend/data/media/`
- `backend/exports/`

The database and media/export folders are workstation runtime state. They may contain large generated audio artifacts.

## Common Workflow Check

After starting backend and frontend:

1. Open `http://127.0.0.1:5173`.
2. Create a project in Ingest.
3. Select one or more `.wav` files.
4. Create project and import.
5. Open Overview and run preparation if needed.
6. Run ASR.
7. Run alignment after ASR completes.
8. Open Slicer and create a slicer run.
9. Open QC and run QC for the selected slicer run.
10. Open Lab from QC or directly for manual review.

## Troubleshooting

### Frontend cannot reach backend

Check that:

- `make dev-backend` is running
- frontend API base is `http://127.0.0.1:8010` unless you changed `BACKEND_PORT`
- browser console is not showing CORS/API-base mismatch errors

### Jobs stay queued

The worker is not running.

Use:

```bash
make dev-backend
```

or run the worker separately:

```bash
make dev-worker
```

### ASR completes instantly

That is suspicious for real audio.

Check that you are not intentionally running the stub backend:

```bash
echo "$ASR_BACKEND"
echo "$SPEECHCRAFT_ALLOW_STUB_ASR"
```

For real ASR, use the default faster-whisper backend and restart `make dev-backend` after changing environment variables.

### Preparation or export cannot process audio

Confirm `ffmpeg` is installed and available:

```bash
ffmpeg -version
```

### `uv` cache permission issues

The Makefile defaults to:

```bash
UV_CACHE_DIR=/tmp/uv-cache
```

If running raw `uv` commands in restricted environments, set the same variable explicitly.
