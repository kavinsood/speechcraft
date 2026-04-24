# Lab

## Purpose

Lab is the manual slice review and editing stage.

It is where a human reviews actual slicer-produced slices, edits transcripts, changes status, applies tags, and overrides machine QC when needed.

Lab is not:

- an ingest page
- a preparation page
- a slicer launcher
- a QC run creator
- an export page

Lab owns slice-level human truth.

## Unit Of Truth

Lab is slice-centric.

It owns:

- live human review status
- slice transcript edits
- slice tags
- save/commit history
- EDL operations
- undo/redo
- split/merge actions
- active variant selection
- slice-level processing where supported

It does not own:

- raw recording import
- preparation settings
- ASR/alignment jobs
- slicer run creation
- QC run creation
- QC thresholding as backend truth

## Human Review State

Current human states:

- unresolved
- accepted
- rejected
- quarantined

These are not QC buckets.

QC may say Auto-kept, Needs review, or Auto-rejected. Lab decides the live human review state.

## QC Handoff

Lab can open in two modes.

### Direct Lab Review

Without QC handoff, Lab shows the normal source-order review queue.

This remains the fallback path and keeps Lab useful without QC.

### QC-Driven Review

When opened from QC, Lab receives:

- slicer run id
- QC run id
- bucket filter
- sort mode
- keep threshold
- reject threshold
- preset

Lab loads the QC run, validates that it matches the current project and selected slicer run, reconstructs visible buckets, filters the queue, and sorts it using the transferred context.

If the QC run is stale, missing, or mismatched, Lab shows a notice and falls back to source-order review.

## Current QC Metadata In Lab

When QC context is available, Lab can show:

- visible QC bucket under transferred thresholds
- persisted machine bucket
- aggregate QC score
- reason codes
- review snapshot captured when QC was created
- current live human status

The UI wording intentionally distinguishes snapshot state from live state.

## Human Override Rule

Human action wins.

Examples:

- a machine-kept slice can be rejected by a human
- a machine-rejected slice can be accepted by a human
- a Needs review slice can be resolved by a human
- transcript edits can supersede the original machine transcript for review/export state

QC remains advisory metadata after the human acts.

## Queue Behavior

Direct Lab queue:

- source-order fallback
- normal project slices
- human review filters and editing tools remain available

QC-driven queue:

- includes only slices from the QC result set after bucket filtering
- preserves QC-derived order efficiently
- supports source-order, QC-score ascending, and QC-score descending modes
- keeps current human status visible

## Main Lab Capabilities

Current Lab/Clip Lab capabilities include:

- play slice audio
- inspect waveform peaks
- edit transcript text
- change review status
- edit tags
- save current slice state
- append EDL operations
- undo/redo
- split clip
- merge with next clip
- switch active audio variant
- run slice-level variant/model operations where supported
- save a slice as a reference asset

## Relationship To Export

Export reads final review/export state from the project data.

QC does not export by itself. QC can shape what a user reviews in Lab, but export should respect the authoritative slice state.

## What Lab Is Not Responsible For

Lab should not grow into:

- dataset preparation
- ASR/alignment control
- slicer rerun control
- QC threshold dashboard
- global dataset analytics
- recommendation engine

Those belong to Overview, Slicer, or QC.

## One Sentence

Lab is the slice-level human review surface where QC can shape the queue, but live human review status and edits remain the authoritative dataset truth.
