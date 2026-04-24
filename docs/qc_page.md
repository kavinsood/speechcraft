# QC Page

## Purpose

QC is the post-slice machine-triage stage.

It exists to answer:

- how good does the current slicer run look?
- which slices are likely usable without manual review?
- which slices should a human inspect first?
- what yield do current thresholds imply?

QC is not human approval. It is advisory machine state.

## Unit Of Truth

QC is tied to exactly one slicer run.

Current API surface:

- `GET /api/projects/{project_id}/qc-runs?slicer_run_id=...`
- `POST /api/projects/{project_id}/qc-runs`
- `GET /api/qc-runs/{qc_run_id}`

Each `QCRun` stores:

- project id
- slicer run id
- status
- threshold config
- slice population hash
- transcript basis hash
- audio basis hash
- stale state
- completion time

Each `SliceQCResult` stores:

- QC run id
- slice id
- aggregate score
- machine bucket
- raw metrics
- reason codes
- human review status snapshot
- lock snapshot

## Machine State vs Human State

The most important rule:

**Machine QC does not equal human approval.**

QC stores machine triage. Lab stores live human decisions.

A slice can be:

- machine-kept but human-rejected
- machine-rejected but human-accepted
- machine-needs-review and later accepted/rejected/quarantined

Human review wins.

## QC Buckets

Current buckets:

- `auto_kept`
- `needs_review`
- `auto_rejected`

UI labels:

- Auto-kept
- Needs review
- Auto-rejected

## Current Scoring Implementation

QC scoring is currently deterministic heuristic triage, not a trained ML model.

For each slice, backend scoring computes:

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

The current aggregate score is:

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

The result is clipped into `[0, 1]` and rounded.

This formula is meant to be transparent and easy to replace. It should not be presented as a final learned quality model.

## Reason Codes

Current hard/explicit reason codes include:

- `broken_audio`
- `near_silence_unusable_clip`
- `transcript_mismatch`
- `severe_clipping_corruption`
- `overlap_second_speaker`

Some of these are currently inferred from available slicer/alignment metadata. For example, the clipping/corruption reason is presently based on edge-energy style boundary signals, not a full waveform clipping-percent detector.

## Bucket Assignment

QC run creation stores a machine bucket using the run thresholds:

```text
if reason_codes exist or score < reject_threshold:
    auto_rejected
elif score >= keep_threshold:
    auto_kept
else:
    needs_review
```

Default thresholds:

- keep threshold: `0.72`
- reject threshold: `0.35`
- preset: `balanced`

## Visible Buckets

The QC page lets the user move threshold sliders.

Changing sliders updates the **visible bucket** and visible yield locally. It does not rewrite the persisted machine bucket stored in `SliceQCResult`.

This gives the user threshold exploration without pretending backend truth changed.

## UI Behavior

The current QC page provides:

- QC run history for the selected slicer run
- Run QC action
- keep/reject threshold sliders
- preset selector
- stale QC warning
- summary cards
- visible count yield
- visible duration yield
- machine bucket counts
- review snapshot count
- score histogram
- source-order timeline strip
- preview table
- advanced metric visibility
- bucket filter
- source-order / QC-score sorting
- handoff into Lab

The preview table shows:

- slice id
- aggregate score
- visible bucket
- persisted machine bucket
- review snapshot
- reason codes
- optional advanced metrics

## Timeline Strip

The timeline strip is source-order based.

It sorts results by:

- source recording id
- source order index
- source start time
- slice id

Segment width is roughly duration-weighted, and segment color comes from the current visible bucket.

This is a macro view, not a detailed waveform editor.

## Stale QC

QC can become stale when its original basis no longer matches current data.

The backend stores hashes for:

- slice population
- transcript basis
- audio basis

When those hashes change, the QC run is marked stale and records a stale reason.

Stale QC can still be viewed for audit/history, but the UI warns the user to rerun QC before relying on threshold-driven decisions for fresh Lab work.

## Lab Handoff

Opening Lab from QC transfers:

- slicer run id
- QC run id
- bucket filter
- sort mode
- keep threshold
- reject threshold
- preset

Lab then reconstructs visible buckets from the transferred threshold context and shapes the queue accordingly.

If QC handoff is stale or mismatched, Lab degrades to normal source-order review instead of silently using the wrong machine context.

## What QC Does Not Do Yet

Not implemented yet:

- learned quality model
- SNR calculation
- LUFS/loudness quality metric
- full clipping-percent detector
- VAD speech ratio
- diarization-backed speaker purity
- MOS/PESQ/STOI style prediction
- waveform-level noise classifier
- complex graph brushing/cross-filtering

These are future ML/audio-quality improvements, not current behavior.

## One Sentence

The QC page creates persisted machine-triage runs for one slicer run, scores slices with transparent heuristic metrics, exposes threshold-driven yield and bucket views, detects stale QC state, and hands advisory context into Lab while preserving human review as final truth.
