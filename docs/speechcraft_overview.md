# Speechcraft

## What Speechcraft is

Speechcraft is a workstation for turning raw speech recordings into training-ready datasets for voice models.

It helps a user go from a folder of `.wav` files to a clean set of slices that can be reviewed, filtered, and exported for downstream TTS or voice-cloning workflows.

At a high level, Speechcraft is not trying to be a generic audio editor or a toy demo UI. It is built around one practical problem:

**taking messy raw speech audio and turning it into something a model can actually train on well.**

## Who it is for

Speechcraft is for people who need to prepare speech datasets without building and maintaining a pile of fragile scripts.

That includes:
- individual hackers and researchers
- ML engineers building voice datasets
- teams preparing internal speech corpora
- users who want a fast automatic path
- users who want a careful manual review path

It is designed for both:
- the person who wants something usable quickly
- the person who is willing to review and curate for better quality

## The core idea

Speechcraft is organized as a pipeline with clear stages.

For this sprint, the intended flow is:

**Ingest -> Overview -> Prep -> Slicer -> QC -> Lab -> Export**

Each stage owns a different kind of truth.
That is important.

- **Overview** works with imported raw recordings and preparation settings.
- **Slicer** creates slice runs from prepared recordings.
- **QC** analyzes one slicer run and classifies slices for fast triage.
- **Lab** is where a human manually reviews and edits slices.

Speechcraft deliberately keeps these stages separate instead of blurring everything into one screen with hidden state.

## What makes Speechcraft different

### 1. It treats dataset preparation as a real product

A lot of speech workflows are still built from scattered scripts, broken dependencies, one-off notebooks, and random open-source tools that only work if the moon phase is correct.

Speechcraft is trying to replace that with a coherent workstation.

### 2. It is slice-first where it matters

The user reviews the actual slices that matter downstream, not fake intermediary objects that never reach training.

That means when a human opens Lab, they are looking at the units the model will actually train on.

### 3. It supports both automation and human override

Speechcraft can automatically score and triage slices, but machine output is not final truth.

A human can always review, restore, reject, or override what the machine did.

### 4. It keeps the workflow explicit

Raw recordings are immutable.
Preparation creates derived outputs.
Slicer and QC are run-based.
Human-reviewed material is protected.
Long-running jobs must visibly report what they are doing.

Nothing important should happen silently.

## What the stages do

## Ingest

The user imports raw `.wav` recordings.

For this sprint, `.wav` import is the only supported path.

## Overview

Overview is the post-ingest source-level page.

Its job is to answer:
- what raw audio was imported
- what technical properties it has
- whether recordings are mixed in sample rate or channels
- whether preparation is needed before slicing

It is not a slice-quality page.

## Prep

Preparation creates a derived dataset copy from the raw recordings.

For this sprint, preparation focuses on dataset-level source operations such as:
- downsampling
- mono/downmix
- channel selection

Preparation does not mutate original imports in place.

## Slicer

The Slicer stage creates candidate slices from prepared recordings.

It is not a waveform editor.
It is a run launcher and run summary surface.

Each slicer execution creates a new slicer run.
Prior runs remain distinct.
Reviewed or locked material is preserved by the existing protection logic when rerunning.

## QC

QC is the post-slice triage stage.

Its job is to:
- analyze slices from one slicer run
- classify them into machine buckets
- support the no-review fast path
- give the user a macro view of the dataset before entering manual review

For this sprint, the UI-facing buckets are:
- **Auto-kept**
- **Needs review**
- **Auto-rejected**

QC is machine triage, not human approval.

## Lab

Lab is the manual review and editing surface.

This is where a human can:
- inspect slices
- review transcripts
- correct mistakes
- accept or reject slices
- override QC decisions

Lab owns slice-level human review.

## Export

Export is the handoff stage where the reviewed or automatically selected dataset can be emitted for downstream use.

## Fast path and review path

Speechcraft supports two main user modes.

### Fast path

The user imports audio, prepares if needed, runs slicing, runs QC, adjusts thresholding, and exports using the machine-selected result set.

This is for the user who wants speed and is willing to trust machine triage.

### Review path

The user goes through the same earlier stages, then opens Lab and manually reviews slices.

This is the safer path and is recommended when quality matters more than speed.

## Product principles

Speechcraft is built around a few strong rules.

### Originals are immutable
Raw imported recordings are never mutated in place.

### Preparation creates derived data
Preparation generates a new derived dataset copy using explicit settings.

### Runs are first-class
Slicer and QC are run-based. Old runs are not silently replaced.

### Human decision is king
Machine QC may classify and rank. Human review may override it.

### Reviewed material is protected
Reviewed and locked slices are preserved and kept distinct from machine-only results.

### Advanced controls stay hidden by default
The default UX is for the lazy user. Expert controls exist, but are collapsed.

### Long-running jobs must be visibly alive
Preparation, slicing, QC, and similar steps must show clear activity, logs, and completion state.

## What Speechcraft is not

Speechcraft is not:
- a generic DAW
- a music editor
- a diarization-first multi-speaker suite in this sprint
- a multilingual corpus manager in this sprint
- a recommendation engine that tells the user what dataset choices to make
- a giant graph playground with complex interactions

It is a focused speech dataset preparation workstation.

## Current sprint assumptions

For this sprint, Speechcraft assumes:
- `.wav` import only
- English only
- single-speaker workflows
- a no-review fast path exists
- a manual review path exists in Lab
- advanced controls are available for expert users

Explicitly out of scope for this sprint:
- transcript import
- diarization workflows
- multi-speaker workflows
- multilingual/code-switching support
- region-level exclusion tools
- automatic recommendation logic
- full cross-stage reversible navigation

## In one sentence

Speechcraft is a browser-first workstation for turning raw speech recordings into training-ready voice datasets through explicit preparation, repeatable slicer and QC runs, and optional human review in a slice-level lab.

