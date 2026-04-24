# Speechcraft Overview

## What Speechcraft Is

Speechcraft is a browser-first workstation for turning raw speech recordings into training-ready voice datasets.

It is built for a practical workflow:

```text
Ingest -> Overview -> Slicer -> QC -> Lab -> Export
```

The app is not a generic audio editor. It is a staged dataset-preparation system where each page owns a different kind of truth.

## Current User

Speechcraft is for people preparing speech data for voice cloning, TTS fine-tuning, or related speech-model workflows.

Primary users include:

- ML engineers building voice datasets
- voice-cloning operators
- researchers preparing controlled speech corpora
- users who want a fast machine-triaged path
- users who need a careful human-review path

## Core Product Rule

The pipeline keeps machine state, derived data, run outputs, and human decisions separate.

That means:

- raw imports are not mutated
- preparation creates derived recordings
- ASR and alignment are source-recording artifacts
- slicer executions create distinct slicer runs
- QC executions create distinct QC runs
- machine QC does not overwrite human review
- Lab is where human truth is changed

## Stage Responsibilities

### Ingest

Ingest creates projects and imports raw `.wav` files.

The current UI provides:

- project-name input
- native multi-file picker
- staged file list
- remove/clear actions
- serial upload progress
- project cleanup on import failure

The backend streams uploaded files to disk and validates WAV headers before creating raw `SourceRecording` rows.

### Overview

Overview is recording-centric.

It owns:

- raw and derived recording inventory
- dataset summary cards
- sample-rate/channel-count warnings
- preparation controls
- active prepared output status
- project-level ASR launch
- project-level alignment launch
- prep/ASR/alignment job activity

Preparation creates derived `SourceRecording` rows with lineage. It does not mutate raw files.

ASR and alignment run over the active prepared output group. Alignment is blocked until ASR has completed.

### Slicer

Slicer is run-centric.

It consumes the active prepared output group from Overview and requires aligned prepared recordings. The old “ASR metadata requested” slicer option was removed because ASR/alignment are upstream preparation responsibilities.

Each slicer execution creates a distinct run. Prior runs remain available, can be selected, and can be deleted to reclaim generated slices/media/QC data.

Rerunning ASR or alignment marks downstream slicer runs stale.

### QC

QC is machine triage for one slicer run.

It creates persisted QC runs and per-slice QC results. Each result stores:

- aggregate score
- machine bucket
- raw metrics
- reason codes
- human review snapshot

QC uses machine buckets:

- Auto-kept
- Needs review
- Auto-rejected

QC can become stale if slice population, transcript basis, or audio basis changes.

### Lab

Lab is slice-centric human review.

It can open directly in source order or consume QC handoff context from the QC page. QC context can shape the queue through bucket filters, sort order, and thresholds, but it does not become human truth.

Human actions in Lab remain authoritative.

### Export

Export is the downstream handoff stage.

The backend has export preview/run endpoints. The current frontend Export page is still mostly a stage shell and boundary marker rather than a full export workstation.

### Reference

Reference remains available as a separate workstation route. It is not part of the main Ingest -> Export sprint path, but it was preserved rather than mapped onto Lab.

## Shared State And Navigation

Project selection is shared across stages.

Run selection is URL-backed where it matters:

- `/slicer?project=...&run=...`
- `/qc?project=...&run=...&qc=...`
- `/lab?project=...&run=...&qc=...&bucket=...&sort=...&keep=...&reject=...`

Changing project clears downstream run/QC/Lab handoff state. Changing slicer run clears selected QC run and Lab handoff.

## Job Model

Long-running work is represented as `ProcessingJob` rows and surfaced through reusable job activity panels.

Current job-backed operations include:

- preparation
- source transcription
- source alignment
- slicer runs

`make dev-backend` starts both the API and worker. If the worker is not running, jobs can remain queued.

## Current QC Scoring Status

QC is currently deterministic heuristic triage, not a trained ML quality model.

It uses signals such as:

- duration
- word count / transcript density
- alignment confidence
- edge/boundary energy
- slicer flag reasons
- hard-gate reason codes

This is intentionally transparent and auditable. It is a foundation for future ML-quality metrics, not the final scoring model.

## Product Principles

### Originals Are Immutable

Raw imported WAV files remain source truth.

### Derived Data Is Explicit

Prepared recordings are derived outputs with parent/recipe lineage.

### Runs Are First-Class

Slicer and QC runs are not silently overwritten.

### Human Decision Wins

Machine QC can rank, bucket, and filter. Lab owns final review state.

### Stale State Is Surfaced

When upstream inputs change, downstream slicer/QC state is marked stale instead of silently reused.

### Boring Native UX Where Possible

Ingest uses the native browser file picker. The app avoids custom file-manager behavior for the core import path.

## Current Assumptions

Current implementation assumptions:

- `.wav` import only
- English ASR/alignment path
- single-speaker-oriented workflow
- prepared output is the normal slicer input
- ASR uses faster-whisper by default
- alignment is required before slicing
- QC is advisory machine triage
- Lab is final human review

Still out of scope:

- diarization-first multi-speaker workflow
- transcript import as a primary ingest path
- multilingual corpus management
- learned QC model
- advanced waveform analytics such as full SNR/LUFS/clipping percent/VAD ratio
- graph-heavy QC interaction

## One Sentence

Speechcraft is a staged speech-dataset workstation that imports WAVs, prepares derived recordings, generates ASR/alignment, creates slicer runs, performs persisted machine QC, and hands slices to Lab for human review and export.
