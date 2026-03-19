# Phase 1 Workflow And State Model

## Purpose

This document describes how the Phase 1 review workstation behaves after the native slice/revision refactor.

Phase 1 is the manual QA boundary between upstream segmentation/transcription and downstream fine-tuning.

## End-To-End Workflow Context

The full preprocessing path is:

`source ingest -> enhancement jobs -> smart segmentation -> Whisper transcription -> Clip Prep Workstation -> export -> fine-tuning`

Phase 1 begins only after:

- upstream processing has created candidate slice boundaries
- upstream ASR has produced initial transcript text

Phase 1 ends when:

- accepted slices are exported as rendered audio plus a `.list`

## Practical User Flow

The user flow in Phase 1 is:

1. Open a project with candidate slices.
2. Work through the unresolved review queue.
3. Correct transcript and audio issues.
4. Adjust slice audio with EDL operations when needed.
5. Save meaningful milestones.
6. Mark slices as accepted, rejected, or quarantined.
7. Export accepted slices.

## Object Tiers

There are four key object tiers in the current implementation:

1. `SourceRecording`
2. `AudioVariant`
3. `Slice`
4. `SliceRevision`

Interpretation:

- `SourceRecording` is the immutable provenance anchor.
- `AudioVariant` is the immutable physical file chosen as the slice's active base audio.
- `Slice` is the logical review object shown in the queue.
- `SliceRevision` is the persisted full-state history chain used for undo/redo and milestones.

## Review Status State Machine

Every slice enters Phase 1 in:

- `unresolved`

From there, it may move through:

- `accepted`
- `rejected`
- `quarantined`

Allowed transitions are intentionally forgiving:

- `unresolved -> accepted`
- `unresolved -> rejected`
- `unresolved -> quarantined`
- `accepted -> unresolved`
- `rejected -> unresolved`
- `quarantined -> unresolved`

The key rule is simple:

- `accepted` means exportable if transcript text is present
- everything else is non-exportable by default

## Local Drafts Versus Persisted Revisions

There are two editing layers in the current UI:

- local draft state for transcript/tag edits before the user saves
- persisted slice revisions for saved metadata, EDL edits, variant changes, and milestones

Interpretation:

- Transcript and tag text can exist as unsaved local drafts in the editor.
- EDL operations are persisted immediately as new slice revisions.
- Status changes are persisted immediately as new slice revisions.
- `Save Slice` writes a full-state milestone revision with the current transcript, tags, and status.

## Undo / Redo Contract

Undo/redo operates on persisted slice revisions.

That means undo/redo must restore:

- EDL operations
- transcript text
- tags
- review status
- active variant selection

Undo/redo is not allowed to be waveform-only math while leaving human metadata behind.

## Milestone Contract

Milestones are not a separate snapshot system anymore.

Instead:

- every persisted edit creates a `SliceRevision`
- meaningful user saves are marked with `is_milestone = true`

This keeps history linear and makes milestone inspection consistent with undo/redo.

## Split Behavior

When a slice is split:

- the original slice becomes superseded
- two new child slices are created
- each child inherits provenance from the original source recording
- each child gets its own transcript text
- each child gets its own persisted revision baseline
- each child starts as `unresolved`

The split itself is represented by new child slice state, not by mutating the original slice in place.

## Merge Behavior

When slices are merged:

- the original slices become superseded
- a new merged slice is created
- the merged slice gets a new physical merged variant
- the merged slice gets a baseline revision
- the merged slice starts as `unresolved`

The user must still validate the merged result after the merge.

## Queue And Detail Loading

The current frontend/API split is:

- queue endpoint: lightweight slice summaries for browsing and filtering
- detail endpoint: heavy active-slice data including revisions and variants

This is important because the queue should not fetch full history and variant trees for every slice.

## Playback And Waveform Contract

The editor must satisfy:

- what the user sees in the waveform
- what the user hears during playback
- what the backend reports as `duration_seconds`

all refer to the same edited slice state.

Current implementation:

- playback uses `/media/slices/{slice_id}.wav`
- slice media is cached by audio state
- waveform peaks are cached by audio state and bin count

## Export Eligibility Rules

A slice is export-eligible when:

- status is `accepted`
- transcript text is non-empty after trimming
- persisted slice state is internally consistent

A slice is not export-eligible when:

- status is `unresolved`
- status is `rejected`
- status is `quarantined`
- transcript text is blank

Unsaved local transcript/tag drafts are not part of export until the user saves them.

## Failure And Recovery Rules

Phase 1 must support recovery from interruptions.

Minimum guarantees:

- persisted slice revisions survive restart
- undo/redo remains available after reload
- accepted/rejected/quarantined state persists
- stale slice render and peak caches can be pruned safely
- export failures do not corrupt prior successful exports

## One-Line Summary

Phase 1 is a slice-first review workflow where playback, waveform peaks, undo/redo, milestones, and export all derive from the same persisted slice revision model.
