# Slicer: Current Technical Contract

## Purpose

The Slicer stage turns prepared, aligned source recordings into candidate training slices.

It is not:

- a waveform editor
- an ASR launcher
- an alignment launcher
- a manual review surface

Slicer is a run launcher, run selector, run summary, and transition point into QC.

## Input Scope

Slicer runs over the project’s active prepared output group.

Required upstream state:

- prepared recordings exist
- each prepared recording has an ASR transcript artifact
- each prepared recording has an alignment artifact

ASR and alignment belong to Overview. The old slicer-side “ASR metadata requested” control was removed because slicer cannot correctly invent missing upstream speech metadata.

If prepared recordings are not aligned, slicer launch is blocked.

## Unit Of Truth

The slicer is run-centric.

Every slicer execution creates a distinct slicer run. Prior runs are preserved, selectable, and auditable.

The run identity is carried through app state and URL parameters so QC and Lab know exactly which slice population they are operating on.

## Backend Shape

Project-level slicer runs are represented by grouped `SOURCE_SLICING` jobs.

Important API endpoints:

- `GET /api/projects/{project_id}/slicer-runs`
- `POST /api/projects/{project_id}/slicer-runs`
- `DELETE /api/projects/{project_id}/slicer-runs/{slicer_run_id}`

The create endpoint queues work and returns a run view. The worker materializes slices and updates job output.

## Run Settings

The visible Slicer UI exposes a restrained top-level set:

- target clip length
- maximum clip length
- segmentation sensitivity

Advanced JSON overrides exist for power users. They are validated as an object before submission.

Backend config normalization keeps the main controls sane:

- durations must be positive
- max duration cannot be lower than min duration
- target duration must fall between min and max
- sensitivity is clamped into a valid range

## Segmentation Sensitivity

The segmentation slider changes how willing the slicer is to accept weaker boundaries.

Higher sensitivity means:

- lower minimum acoustic-boundary score
- more willingness to split at smaller/weaker gaps
- usually more slices
- greater risk of questionable boundaries

Lower sensitivity means:

- stricter boundary acceptance
- fewer splits
- longer clips on average
- lower risk of cut artifacts

It is not a quality score and does not change ASR/alignment.

## Algorithm Summary

The slicer is alignment-guided and acoustic-aware.

It uses:

- source-level aligned words
- word start/end timing
- confidence when available
- punctuation/pause structure
- duration constraints
- acoustic boundary refinement
- edge energy checks
- configured min/target/max duration limits

The algorithm tries to create clips that are:

- not too short
- not too long
- semantically coherent
- safe at boundaries
- linked back to source timing
- reviewable directly in Lab

## Slice Materialization

The slicer creates real `Slice` rows and active audio variants.

Slice metadata records provenance such as:

- slicer run id
- source recording id
- source start/end timing
- order index
- boundary/flag metadata
- alignment-derived information

These slices are the objects reviewed in Lab and scored in QC.

## Run Summary

Completed slicer runs expose summary information such as:

- created slice count
- total sliced duration
- average slice length
- min/max slice duration
- warnings/failure messages
- stale state
- downstream QC availability

The UI shows run history and makes the active run identity obvious.

## Stale State

Slicer runs become stale when upstream truth changes.

Examples:

- ASR rerun for prepared recordings
- alignment rerun for prepared recordings
- prepared output changed

Stale slicer runs remain visible for audit/review, but QC handoff is constrained so the user does not accidentally treat stale slice populations as fresh truth.

## Deleting Runs

Slicer runs can be deleted.

Deleting a run removes generated run data such as:

- generated slices
- related audio variants/transcripts/commits where applicable
- grouped slicer jobs
- downstream QC runs and QC results

This exists because run-based slicing can otherwise accumulate a lot of media.

## Relationship To QC

QC runs on one selected completed slicer run.

Slicer does not run QC automatically. It transitions the user into QC with the selected run identity.

## Relationship To Lab

Lab reviews the slices created by slicer runs.

Human-reviewed or locked slice state is protected conceptually from machine-only rerun behavior. The slicer preserves the architectural rule that human review is not casually destroyed by later automation.

## One Sentence

Slicer consumes aligned prepared recordings, creates explicit slicer runs with real slice rows and media, preserves run history, marks stale downstream state when upstream metadata changes, and hands one completed run into QC.
