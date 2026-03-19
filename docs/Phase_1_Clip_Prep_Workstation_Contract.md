# Phase 1: Clip Prep Workstation Contract

## Purpose

This document defines the product contract for Phase 1 of Speechcraft.

Phase 1 is the dedicated manual review and editing phase that begins after segmentation and upstream transcription have already produced candidate slices. Its purpose is to convert machine-generated candidates into accepted, training-ready data for downstream fine-tuning.

The workflow boundary is:

`source ingest -> enhancement jobs -> smart segmentation -> Whisper transcription -> Clip Prep Workstation -> export -> fine-tuning`

Phase 1 covers only the `Clip Prep Workstation` portion of that flow.

## Product Boundary

Phase 1 is responsible for:

- Loading projects that already contain candidate slices and initial transcripts
- Providing a slice-by-slice editing workspace
- Allowing manual transcript review and correction
- Allowing slice-level audio edits through per-slice EDL
- Allowing split and merge operations
- Allowing status changes and tag assignment
- Preserving provenance and revision history
- Exporting accepted slices as rendered audio plus a SoVITS-compatible `.list`

Phase 1 is not responsible for:

- Source-file ingest UX
- Denoise, dereverb, deecho, or other preprocessing job execution
- Automatic segmentation
- Whisper/ASR execution
- Training orchestration
- Inference or serving

## Unit Of Work

The primary unit of work is the slice.

Product copy may still say "clip" in some places, but the runtime contract is slice-based.

Each slice is:

- Derived from a source recording
- Bound to an original source-relative time range
- Edited non-destructively through slice-level EDL
- Reviewed independently
- Exported independently

Required provenance fields:

- `source_file_id`
- `original_start_time`
- `original_end_time`
- EDL operations

## Allowed User Actions

Within Phase 1, the user must be able to:

- Open one slice at a time in the editor
- Play, pause, seek, zoom, and inspect waveform state
- Inspect duration, sample rate, channels, speaker, and language
- Select waveform regions
- Delete a selected region
- Insert silence
- Split a slice
- Merge adjacent slices
- Edit transcript text
- Edit tags
- Change review status
- Undo and redo persisted edits
- Save meaningful milestones
- Export accepted slices

## Editor Rules

Core rules:

- Editing is per-slice only
- Source recordings are immutable
- Physical variants are immutable
- Audio edits are represented as EDL operations
- Undo/redo restores full persisted slice state
- Milestones are saved revisions in that same history chain
- Playback, waveform, and duration must reflect the same edited slice state
- Export uses the latest persisted accepted slice state, not unsaved local drafts

The workstation is the main product surface for Phase 1, not a helper dialog.

## Review Status Contract

Every slice must have a review status.

Required statuses:

- `unresolved`
- `accepted`
- `rejected`
- `quarantined`

Semantics:

- `unresolved`: still needs a human decision or additional work
- `accepted`: approved for export
- `rejected`: excluded from export
- `quarantined`: blocked for follow-up or QA

## History Contract

The product must preserve meaningful user history.

Required guarantees:

- Transcript edits are undoable
- Tag edits are undoable
- Status changes are undoable
- Active variant changes are undoable
- EDL edits are undoable
- Users can save milestone revisions with a message

An audio editor where only waveform math is undoable does not satisfy the Phase 1 contract.

## Tags Contract

Tags are lightweight user-defined labels for filtering and QA.

Examples:

- `noisy`
- `bad_transcript`
- `clipped_end`
- `breath`
- `emotion`
- `recheck`

Tags do not determine export by themselves. Status determines export behavior.

## Export Contract

Required export outputs:

- Rendered audio files for accepted slices
- A SoVITS-compatible `.list` manifest

The `.list` format remains:

`wav_path|speaker_name|language|transcription_text`

Only accepted slices with non-empty transcript text are exported by default.

Rejected, unresolved, and quarantined slices are excluded by default.

## Runtime Media Contract

The backend must provide browser-friendly managed media routes:

- `GET /media/variants/{variant_id}.wav`
- `GET /media/slices/{slice_id}.wav`

The slice media route is the editor playback contract.

Managed runtime artifacts live under:

- `backend/data/media/variants/`
- `backend/data/media/slices/`
- `backend/data/media/peaks/`

## Completion Criteria

Phase 1 is complete for a project when:

- every slice has reached a resolved operator decision
- all training-intended slices are marked `accepted`
- accepted slices have final persisted transcript text
- accepted slices have the intended persisted audio state
- export succeeds and produces rendered clips plus a `.list`

## One-Line Summary

Phase 1 is the dedicated manual slice review and editing phase that turns machine-generated candidates into trusted, exportable training data using a full-state slice revision model.
