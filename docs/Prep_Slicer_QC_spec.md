# Speechcraft Current Pipeline Contract

This document replaces the early sprint plan for Overview/Prep, Slicer, QC, and Lab. It describes the current implemented contract after the pipeline phases.

## 1. Current Product Flow

```text
Ingest -> Overview -> Slicer -> QC -> Lab -> Export
```

Reference remains available as a separate route/workstation and is not part of the main sprint path.

## 2. Page Ownership

Each page owns a different unit of truth.

| Page | Owns | Does Not Own |
| --- | --- | --- |
| Ingest | project creation, raw WAV import staging/upload | preparation, slicing, QC, review |
| Overview | source recordings, preparation, active prepared output, ASR, alignment | slice QC, human review |
| Slicer | slicer run launch/history/summary/deletion | ASR/alignment, QC scoring, Lab review |
| QC | QC runs/results for one slicer run | live human review truth |
| Lab | live slice review/edit state | prep, slicer run creation, QC run creation |
| Export | downstream dataset output | upstream run generation |

## 3. Global Rules

### Raw Imports Are Immutable

Imported WAV files are source truth. Preparation creates derived recordings and does not mutate raw imports.

### Prepared Output Is Explicit

Preparation creates a derived output group. The project stores the active prepared output group, and Slicer consumes that scope.

### ASR And Alignment Belong To Overview

ASR and alignment are source-level preparation jobs over the active prepared output group.

Slicer does not request or fake ASR metadata.

### Slicer And QC Are Run-Based

Every slicer execution creates a distinct slicer run.

Every QC execution creates a distinct QC run tied to one slicer run.

### Human Review Wins

QC is advisory machine triage. Lab owns live human review state.

### Stale State Must Be Visible

Changing upstream preparation/ASR/alignment or slice/audio/transcript basis can stale downstream slicer/QC state.

### Long Jobs Need A Visible Activity Surface

Preparation, ASR, alignment, slicing, and QC-style work must show status through job activity UI. The worker must be running for queued jobs to progress.

## 4. Ingest Contract

Current Ingest behavior:

- project name required
- native browser file picker
- `.wav` only
- multiple files
- staged selected-file list
- per-file remove
- clear all
- create project and import as one explicit submit action
- serial upload queue
- per-file progress
- clear submit error
- cleanup project on failed import where possible

Implementation rules:

- do not read audio into browser memory with `FileReader`
- pass `File` objects directly into `FormData`
- backend streams `UploadFile` to disk
- backend validates WAV headers before creating `SourceRecording`

## 5. Overview Contract

Overview is recording-centric.

It displays:

- total duration
- recording count
- raw/derived counts
- sample rates
- channel counts
- active prepared output status
- ASR completion count
- alignment completion count

Warnings include:

- empty import
- mixed sample rates
- mixed channel counts
- missing/stale prepared output
- missing ASR
- missing alignment

Overview does not display slice-level quality graphs or human review decisions.

## 6. Preparation Contract

Preparation controls:

- target sample rate
- channel mode
- channel selection

Preparation behavior:

- creates a `PREPROCESS` job
- worker materializes derived WAV files
- creates derived `SourceRecording` rows
- stores parent recording id
- stores processing recipe metadata
- updates active prepared output group
- keeps raw recordings untouched

The prepared output group becomes the input scope for Slicer.

## 7. ASR Contract

Project ASR endpoint:

- `POST /api/projects/{project_id}/transcription`

Scope:

- active prepared output group only

UI controls:

- model size: `base`, `small`, `medium`, `large-v3`, `turbo`
- batch size
- initial prompt

Defaults:

- `turbo`
- batch size `8`
- English

Backend:

- default backend is `faster_whisper`
- `turbo` maps to `large-v3-turbo`
- stub ASR requires `SPEECHCRAFT_ALLOW_STUB_ASR=1` and is test-only

Output:

- transcript text artifact
- transcript JSON artifact
- word count
- model/backend metadata

Rerunning ASR marks downstream slicer runs stale.

## 8. Alignment Contract

Project alignment endpoint:

- `POST /api/projects/{project_id}/alignment`

Scope:

- active prepared output group only

UI controls:

- acoustic model
- text normalization strategy
- batch size

Rules:

- alignment is blocked until ASR exists
- alignment is blocked while ASR jobs are pending/running
- alignment refuses known stub ASR transcript text
- rerunning alignment marks downstream slicer runs stale

Output:

- source-recording alignment artifact
- alignment summary/metadata

## 9. Slicer Contract

Slicer input:

- active prepared output group
- ASR transcript artifacts
- alignment artifacts

Slicer launch is blocked when prepared recordings are not aligned.

Visible controls:

- target clip length
- maximum clip length
- segmentation sensitivity

Advanced:

- JSON override object for power users
- frontend validates that overrides are a JSON object
- backend validates normalized config

Run behavior:

- creates a distinct slicer run
- queues per-recording `SOURCE_SLICING` jobs
- materializes real `Slice` rows and audio variants
- stores source-order/provenance metadata
- shows run history and summary
- stale runs are visible but constrained for QC handoff

Run deletion:

- deletes generated slices and related media/data
- deletes grouped slicer jobs
- deletes downstream QC runs/results
- returns cleanup counts

## 10. QC Data Contract

QC run endpoint:

- `GET /api/projects/{project_id}/qc-runs?slicer_run_id=...`
- `POST /api/projects/{project_id}/qc-runs`
- `GET /api/qc-runs/{qc_run_id}`

`QCRun` stores:

- project id
- slicer run id
- status
- threshold config
- slice population hash
- transcript basis hash
- audio basis hash
- stale state
- timestamps

`SliceQCResult` stores:

- QC run id
- slice id
- aggregate score
- machine bucket
- raw metrics
- reason codes
- human review status snapshot
- lock snapshot

Machine QC state is separate from human review state.

## 11. Current QC Scoring

Current raw metrics:

- `duration_seconds`
- `word_count`
- `avg_alignment_confidence`
- `edge_start_energy`
- `edge_end_energy`
- `duration_score`
- `confidence_score`
- `transcript_score`
- `edge_penalty`
- `flag_penalty`
- `hard_gate_penalty`

Current reason codes:

- `broken_audio`
- `near_silence_unusable_clip`
- `transcript_mismatch`
- `severe_clipping_corruption`
- `overlap_second_speaker`

Current scoring formula:

```text
score =
  duration_score * 0.30
+ confidence_score * 0.30
+ transcript_score * 0.25
+ 0.15
- edge_penalty
- flag_penalty
- hard_gate_penalty
```

This is heuristic triage, not a learned ML quality model.

## 12. QC Page Contract

QC page shows:

- run history for selected slicer run
- Run QC action
- keep threshold
- reject threshold
- preset
- visible yield by count
- visible yield by duration
- machine bucket counts
- review snapshot count
- stale state
- score histogram
- source-order timeline strip
- preview table
- advanced metrics toggle
- bucket filter
- source-order / score sorting
- Lab handoff

Persisted machine bucket and visible threshold bucket are separate.

## 13. Stale QC Contract

QC becomes stale when current basis hashes differ from stored hashes:

- slice population
- transcript basis
- audio basis

Stale QC remains viewable. The user is warned to rerun QC before relying on threshold-driven decisions.

## 14. Lab Handoff Contract

QC transfers to Lab:

- slicer run id
- QC run id
- bucket filter
- sort mode
- keep threshold
- reject threshold
- preset

Lab validates that the QC run matches the current project and slicer run. If the handoff is stale/mismatched/missing, Lab falls back to source-order review with a notice.

Lab shows QC context as advisory metadata and keeps live human review status authoritative.

## 15. URL And Selection State

Run/QC/Lab navigation state is URL-backed where needed:

- `run`
- `qc`
- `bucket`
- `sort`
- `keep`
- `reject`
- `preset`

Project changes clear downstream selection. Slicer run changes clear QC selection and Lab handoff.

## 16. Export Contract

Export is the downstream handoff page.

Current state:

- backend export preview/run endpoints exist
- frontend Export page is still mostly a shell
- final selection policy is not fully productized in the Export UI

It should consume final slice state, not raw QC buckets as if they were human approval.

## 17. Current Non-Goals

Not currently implemented:

- diarization-first multi-speaker workflow
- transcript import as the main path
- learned QC model
- SNR/LUFS/clipping-percent/VAD-ratio QC metrics
- multilingual/code-switching support
- complex graph brushing/cross-filtering
- recommendation engine
- custom file browser

## 18. Review Checklist

When reviewing future changes, check:

- raw source files are not mutated
- prepared output group is explicit
- Slicer does not own ASR/alignment
- Slicer runs are distinct and deletable
- QC runs are tied to one slicer run
- machine buckets are separate from human state
- stale state is surfaced
- Lab handoff carries complete filter/sort/threshold context
- Lab human actions remain authoritative
- long-running work is queued and visible
