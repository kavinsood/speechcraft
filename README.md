# Speechcraft

Speechcraft is a browser-first workstation for turning raw speech recordings into clean, reviewable, training-ready voice datasets.

The current app is no longer just the original clip-review scaffold. It now has a full staged workflow:

```text
Ingest -> Overview -> Slicer -> QC -> Lab -> Export
```

Reference selection remains available as a separate workstation route.

## Current Workflow

### Ingest

Create a project, select one or more `.wav` files with the native browser file picker, stage the file list, and upload the recordings into the project.

Uploads are streamed to the backend and imported as raw `SourceRecording` rows. Raw imported files are treated as immutable source material.

### Overview

Overview is the source/preparation page.

It shows recording count, total duration, sample rates, channel counts, raw-vs-derived state, and technical warnings. It also owns dataset preparation and source-level speech metadata:

- preparation by downsampling, mono/downmix, or channel selection
- project-level ASR over the active prepared output group
- project-level alignment after ASR completes
- batch job activity for prep, ASR, and alignment

Preparation creates derived recordings with lineage instead of mutating raw imports.

### Slicer

Slicer is run-centric.

It launches slicer runs over the active prepared output group and requires prepared recordings to have alignment artifacts before slicing. Every slicer execution creates a distinct run; prior runs are preserved. Runs can be deleted to reclaim generated slices, jobs, QC runs, and media.

### QC

QC is machine triage for one slicer run.

It creates persisted QC runs, stores one result per slice, stores raw metrics and reason codes, separates machine buckets from human review state, detects stale QC, and hands filter/sort/threshold context into Lab.

QC buckets:

- Auto-kept
- Needs review
- Auto-rejected

### Lab

Lab is the human review and override surface.

It can consume QC-origin queue context, but human review state remains authoritative. Machine QC shapes the queue and displays metadata; it does not replace human decisions.

### Export

Export is present in navigation as the downstream handoff stage. The backend has export preview/run endpoints, but the current frontend page is still mostly a shell.

## Project Layout

- `backend/`: FastAPI API, SQLite persistence, media management, processing worker, slicing/QC/export logic
- `frontend/`: React + Vite UI for the staged workstation
- `docs/`: product, workflow, and implementation notes

## Core Docs

- `docs/speechcraft_overview.md`: product overview and stage responsibilities
- `docs/dataset_overview.md`: Ingest/Overview/preparation/ASR/alignment behavior
- `docs/slicer_working_in_depth.md`: slicer architecture and run behavior
- `docs/qc_page.md`: QC data model, scoring, UI, stale-state, and Lab handoff
- `docs/clip_lab.md`: Lab review and human override behavior
- `docs/Prep_Slicer_QC_spec.md`: current sprint contract for prep, slicer, QC, and Lab
- `docs/Reference_Picker_Spec.md`: reference picker/workstation design notes
- `INSTALL.md`: setup, runtime, and verification commands

## Quick Start

The easiest repo-level workflow is:

```bash
make setup
make check
```

Then run the app in two terminals:

```bash
make dev-backend
```

```bash
make dev-frontend
```

`make dev-backend` starts both the FastAPI API and the processing worker. The worker is required for preparation, ASR, alignment, and slicer jobs.

## URLs

By default the Makefile uses:

- frontend: `http://127.0.0.1:5173`
- backend: `http://127.0.0.1:8010`
- backend docs: `http://127.0.0.1:8010/docs`

The frontend API target can be overridden with `VITE_API_BASE_URL`.

## Local State

The backend stores local runtime state here:

- `backend/data/project.db`
- `backend/data/media/`
- `backend/exports/`

These are local workstation/runtime files, not source code.

## ASR Runtime Notes

The backend uses `faster-whisper` for real ASR by default.

Useful environment variables:

- `ASR_BACKEND=faster_whisper`
- `ASR_DEVICE=cpu` or `ASR_DEVICE=cuda`
- `ASR_COMPUTE_TYPE=int8` for CPU or `float16` for CUDA
- `ASR_MODEL_PATH=/path/to/local/model-or-model-name` when using a local model

The UI model option `turbo` maps to `large-v3-turbo`.

The stub ASR backend is disabled unless `SPEECHCRAFT_ALLOW_STUB_ASR=1` is set. That path is for tests only.

## Verification

Use:

```bash
make check
```

Useful focused checks:

```bash
python3 -m compileall backend/app
cd backend && uv run python -m unittest discover -s tests -p 'test_*.py'
cd frontend && npm run build
```

## Current Status

Implemented:

- project creation and `.wav` ingest
- streamed recording upload
- raw and derived `SourceRecording` model
- preparation jobs with derived-output lineage
- project-level ASR and alignment jobs
- active prepared output group for downstream slicing
- slicer run creation, history, stale state, and deletion
- QC run persistence, per-slice results, reason codes, raw metrics, and stale detection
- QC page summary, thresholds, histogram, source-order timeline, preview table, and Lab handoff
- Lab QC-origin filtering/sorting plus live human review override
- export preview and export run backend endpoints
- reusable job activity panel
- backend API and repository test coverage for the main pipeline contracts

Still intentionally rough:

- QC scoring is heuristic, not a trained model
- ASR/alignment batch progress is functional but not a full production job dashboard
- Export UI is still a shell around backend export capabilities
- advanced audio-quality metrics such as SNR, LUFS, clipping percentage, VAD speech ratio, and diarization are not implemented yet
- Reference remains a separate workstation outside the main sprint path
