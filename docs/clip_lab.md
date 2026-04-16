# Lab

## What Lab is

**Lab** is the manual slice review and editing stage in Speechcraft.

It is the place where a human looks at the actual slices produced by the slicer, decides what should be kept or rejected, corrects mistakes, and overrides machine QC when necessary.

Lab is not a source-recording page.
It is not a slicer run launcher.
It is not the QC page.

Lab is the human-in-the-loop surface for **slice-level truth**.

## What unit of truth Lab owns

For this sprint, Lab is **slice-centric**.

That means it owns:
- slice-level manual review
- slice transcript editing
- slice status changes
- slice tags
- slice-level non-destructive edit history
- slice-level variant switching and clip processing where already supported

Lab does **not** own:
- raw imported recordings
- source-level preparation
- slicer run creation
- QC run creation
- dataset-wide thresholding

Those belong to other pages.

## Why Lab exists

Machine triage can rank and classify slices, but it cannot be trusted as final authority.

Lab exists because:
- some machine-kept slices will still be bad
- some machine-rejected slices will still be recoverable
- transcripts sometimes need correction
- slice boundaries sometimes need human judgment
- users need a place to make explicit final decisions

Speechcraft treats human review as the final authority.

## Relationship to QC

QC and Lab are separate.

QC is machine triage.
Lab is human decision.

QC may send context into Lab, but QC does not replace Lab.

When a user opens Lab from QC, Lab should inherit useful machine context such as:
- selected QC bucket filters
- selected sort order
- current threshold context

But once the user is in Lab, they are making human decisions on slices.

Human decision overrides QC.

## What the user does in Lab

Lab supports the manual review workflow.

In this stage, the user can:
- inspect slices one by one
- play slice audio
- inspect waveform and timing
- read and edit transcript text
- change tags
- set review state
- accept, reject, or quarantine slices
- review machine-flagged slices first
- inspect machine-kept or machine-rejected slices if desired
- use existing edit and variant tools where already supported

This is the slice-level truth-editing surface.

## Human review state

Lab is where human review state is created and changed.

For this sprint, the human review states are:
- unresolved
- accepted
- rejected
- quarantined

These are human states.
They are not the same as QC buckets.

A slice may be:
- machine-kept but later human-rejected
- machine-rejected but later human-accepted
- machine-flagged and then resolved by a human

That distinction must remain clear.

## Relationship to slicer output

Lab works on the real slices created by the slicer.

That is a major product rule.

Users are not reviewing fake intermediary windows or temporary page objects.
They are reviewing the same slices that downstream training and export care about.

This keeps manual review grounded in the real dataset.

## Relationship to reruns and locked material

Because Lab is where human decisions happen, its output must be protected.

When the slicer is rerun later:
- reviewed and locked slices must remain protected
- manually reviewed work must not be casually destroyed
- new candidate slices may be generated around existing protected material

This makes Lab safe to use even before the user is fully done iterating on slicer settings.

## How users arrive in Lab

There are two main routes into Lab.

## 1. Direct manual review path

The user chooses to review slices manually after slicing.

They open Lab and inspect slices in source order or whatever the current default view is.

## 2. QC-driven review path

The user opens QC first, runs machine triage, then sends slices into Lab.

In this case, Lab should open with transferred context, such as:
- a QC bucket filter like **Needs review**
- a sort order based on QC ranking
- the current threshold context that produced the selected set

This makes Lab a better triage tool without turning it into a QC page.

## Slice visibility and filtering

Lab should support filtering and sorting over slices.

At minimum, Lab needs to remain useful for:
- source-order review
- QC-guided review
- finding obviously flagged slices quickly
- finding manually unresolved slices quickly

Useful slice views include:
- all slices
- unresolved only
- accepted only
- rejected only
- quarantined only
- QC Auto-kept
- QC Needs review
- QC Auto-rejected

QC-related state should be visible, but it must not replace the human review state.

## Tags in Lab

Lab already supports tags, and that remains useful.

For this sprint, tags can help with:
- user categorization
- slice organization
- surfacing QC-related context

However, tags should not be treated as the only backend truth for QC or review behavior.

QC-related state must remain conceptually first-class even if some of it is surfaced through existing tag displays.

## Transcript editing

Transcript editing is part of Lab.

This is where a user can inspect a slice’s transcript and correct it if needed.

That matters because some slices will be machine-triable but still contain transcript mistakes that only a human can confirm.

Lab should treat transcript editing as part of slice review, not as a separate detached workflow.

## Existing edit and processing capabilities

Lab inherits the existing slice-oriented editing surface.

That means it is already the place where slice-level tooling lives, including existing support for:
- waveform inspection
- playback
- transcript editing
- tag editing
- status management
- revision history
- non-destructive audio editing where already supported
- variant switching where already supported
- processing/model runs where already supported

For this sprint, these remain Lab capabilities rather than moving to earlier pages.

## What Lab should show clearly

When a user opens a slice in Lab, they should be able to see:
- the slice itself
- its transcript state
- its human review state
- relevant tags
- relevant QC context if present
- whether the slice was machine-kept, machine-flagged, or machine-rejected

But the page must not visually imply that machine QC is the final verdict.

## What Lab is not responsible for

Lab is not responsible for:
- preparation settings
- source-level dataset standardization
- slicer launch or rerun launch
- QC thresholding
- dataset-wide yield analysis
- model recommendation logic
- full dataset analytics

Those belong to Overview, Slicer, or QC.

Lab stays focused on human review of slices.

## What makes Lab different from QC

The difference is simple:

### QC asks:
- what does the machine think about this slicer run?
- how much can we keep without manual review?
- what should probably be reviewed first?

### Lab asks:
- what does the human decide about this slice?
- should this slice stay in the dataset?
- does the transcript need correction?
- does the machine need to be overridden here?

That distinction should stay obvious in both UX and state handling.

## What makes Lab different from Overview and Slicer

Overview is about imported recordings and preparation.

Slicer is about creating candidate slices from prepared recordings.

Lab is about reviewing those slices after they exist.

It should not drift backward into source-recording management or forward into export analytics.

## Why Lab matters

Without Lab, the system would only support two bad extremes:
- fully blind trust in machine output
- trying to reason about everything at the dataset level without touching actual slices

Lab is where the user can finally inspect the concrete units the model will train on and make final human decisions.

It is the part of Speechcraft that turns machine triage into an actual reviewed dataset.

## In one sentence

Lab is Speechcraft’s slice-level manual review surface, where humans inspect, edit, classify, and override machine-triaged slices to produce the final reviewed dataset.

