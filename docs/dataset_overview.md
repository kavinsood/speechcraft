# Ingest And Overview

## Purpose

Ingest and Overview are the source-recording side of Speechcraft.

They answer:

1. What project is being created?
2. Which raw `.wav` files were imported?
3. What technical shape does the dataset have?
4. Which prepared output should downstream stages use?
5. Do prepared recordings have ASR and alignment artifacts?

Overview is not a QC page and not a slice-review page.

## Ingest

The current Ingest page is the front door for new projects.

It provides:

- project-name input
- native `.wav` file picker
- staged file list
- per-file remove action
- clear-all action
- serial upload progress
- clear validation and submit errors

Project creation and import are presented as one explicit user action: **Create project and import**.

The backend currently uses separate project creation and per-file upload endpoints, but the frontend treats the flow as one operation and deletes the project on import failure when cleanup is possible.

## Upload Rules

Current import behavior:

- `.wav` only
- files are passed as `File` objects into `FormData`
- frontend does not read audio into memory with `FileReader`
- uploads are serial for predictable cleanup behavior
- backend streams `UploadFile` to disk
- backend validates WAV headers before creating `SourceRecording`
- failed imports clean up written files where possible

Raw imported recordings become immutable source material.

## Overview Unit Of Truth

Overview owns recording-level state:

- raw imported recordings
- derived prepared recordings
- active prepared output group
- source-level technical warnings
- preparation settings
- ASR settings
- alignment settings
- prep/ASR/alignment job status

Overview does not own:

- slicer runs
- QC runs
- slice acceptance/rejection
- Lab human review

## Dataset Summary

Overview shows:

- total duration
- recording count
- sample-rate set
- channel-count set
- raw recording count
- prepared recording count
- active prepared output status

Warnings are intentionally basic and deterministic:

- no recordings imported
- mixed sample rates
- mixed channel counts
- stale or missing prepared output for current settings
- missing ASR
- missing alignment

Overview should not invent a global “dataset quality score.”

## Preparation

Preparation creates derived `SourceRecording` rows from raw recordings.

Current controls:

- target sample rate
- channel mode
- channel selection

Current behavior:

- creates a `PREPROCESS` `ProcessingJob`
- worker materializes derived WAVs
- derived rows point back to raw parents
- recipe metadata records settings, job id, source id, and output group
- project stores the active prepared output group
- source files are not mutated

Preparation uses backend media management and cleanup rules so failed runs do not intentionally leave unmanaged database truth.

## Active Prepared Output

The active prepared output group is the downstream input scope.

Slicer runs use this active group rather than guessing from “latest-looking” recordings. Re-running preparation creates another derived group and can change the active prepared group.

This rule prevents downstream pages from mixing raw recordings, old prepared outputs, and new prepared outputs.

## ASR

ASR is an Overview preparation action.

It runs over every recording in the active prepared output group.

Current ASR controls:

- model size: `base`, `small`, `medium`, `large-v3`, `turbo`
- batch size
- optional initial prompt

Current defaults:

- model size: `turbo`
- batch size: `8`
- language: English

The backend uses faster-whisper by default. `turbo` maps to `large-v3-turbo`.

The stub backend is disabled unless `SPEECHCRAFT_ALLOW_STUB_ASR=1` is explicitly set for tests.

ASR writes source-recording transcript artifacts:

- transcript text
- transcript JSON
- model/backend metadata
- word count

## Alignment

Alignment is also an Overview preparation action.

It runs over the active prepared output group after ASR has completed.

Current alignment controls:

- acoustic model
- text normalization strategy
- batch size

Current behavior:

- alignment is blocked while ASR jobs are pending/running
- alignment is blocked if prepared recordings lack transcripts
- alignment writes source-recording alignment artifacts
- alignment refuses known stub ASR transcript text

The default alignment backend is the current torchaudio forced-align worker path.

## Downstream Invalidations

Rerunning ASR or alignment marks downstream slicer runs stale for affected recordings.

That is required because changing transcript or word timing invalidates slice boundaries and QC assumptions.

## Job Activity

Overview uses a reusable job activity panel and a batch-aware source job surface.

It can show:

- preparation jobs
- ASR batch progress
- alignment batch progress
- queued/running/completed/failed states
- logs or terminal-style messages

If jobs stay queued, the processing worker is probably not running.

Use:

```bash
make dev-backend
```

or:

```bash
make dev-worker
```

## What Happens Next

Once a project has:

- imported raw WAVs
- a prepared output group
- ASR artifacts
- alignment artifacts

the next stage is Slicer.

Slicer refuses to launch on prepared recordings that do not have alignment artifacts.

## One Sentence

Ingest creates projects and imports raw WAV files; Overview turns those raw recordings into explicit prepared outputs, generates ASR/alignment artifacts, and exposes the source-level readiness state required before slicing.
