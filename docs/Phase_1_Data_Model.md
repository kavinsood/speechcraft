# Phase 1 Data Model

## Purpose

This document defines the logical Phase 1 data model for Speechcraft as it exists after the native SQLite refactor.

The goal is to support a browser-first review workstation that preserves provenance, keeps audio assets managed on disk, and makes both undo/redo and export reproducible.

## Design Principles

The Phase 1 data model must satisfy these rules:

- Original source recordings are immutable.
- Physical slice variants are immutable files.
- The logical unit of work is the slice.
- Every slice preserves provenance back to a source recording and source-relative time bounds.
- Audio edits are represented non-destructively as per-slice EDL operations.
- Undo/redo restores full slice state, not just waveform math.
- Saved milestones are part of the same revision chain as regular slice edits.
- Export is reproducible from the latest persisted accepted slice state.

## Core Entities

Phase 1 needs these core entities:

- `ImportBatch`
- `SourceRecording`
- `AudioVariant`
- `Slice`
- `SliceRevision`
- `Transcript`
- `Tag`
- `SliceTag`
- `ExportRun`

## ImportBatch

`ImportBatch` is the project-level container for review work.

Suggested fields:

- `id`
- `name`
- `created_at`
- derived `updated_at`
- derived `export_status`

Responsibilities:

- Own source recordings, slices, and export runs
- Define the review workspace boundary
- Provide project-level recency and export status

## SourceRecording

`SourceRecording` represents an immutable long-form audio source.

Suggested fields:

- `id`
- `batch_id`
- `parent_recording_id`
- `file_path`
- `sample_rate`
- `num_channels`
- `num_samples`
- `processing_recipe`

Rules:

- Never modified in place by the Phase 1 editor
- Acts as the provenance anchor for downstream slices
- May point at a managed source asset or a stable external path

## AudioVariant

`AudioVariant` represents an immutable physical slice-level WAV file.

Suggested fields:

- `id`
- `slice_id`
- `file_path`
- `is_original`
- `generator_model`
- `sample_rate`
- `num_samples`

Rules:

- Variants are physical files, not transient buffers
- The active variant determines the base audio used for playback and export
- Variant files should live under the managed media root when the app owns them

## Slice

`Slice` is the primary review entity in Phase 1.

Suggested fields:

- `id`
- `source_recording_id`
- `active_variant_id`
- `active_revision_id`
- `status`
- `model_metadata`
- `created_at`

Required provenance fields stored in slice metadata:

- `source_file_id`
- `original_start_time`
- `original_end_time`
- `speaker_name`
- `language`
- `order_index`
- `is_superseded`

Suggested review statuses:

- `unresolved`
- `accepted`
- `rejected`
- `quarantined`

Interpretation:

- `unresolved`: still needs a human decision or additional work
- `accepted`: approved for export
- `rejected`: excluded from export
- `quarantined`: blocked for QA or follow-up

## SliceRevision

`SliceRevision` stores immutable full-state slice snapshots.

Suggested fields:

- `id`
- `slice_id`
- `parent_revision_id`
- `edl_operations`
- `transcript_text`
- `status`
- `tags_payload`
- `active_variant_id_snapshot`
- `message`
- `is_milestone`
- `created_at`

Rules:

- Revisions are append-only
- Undo/redo moves the active slice pointer through these revisions
- Audio edits and metadata edits both produce revisions
- Milestones are revisions with extra user-facing intent, not a separate shadow system

## Transcript

`Transcript` stores source and edited text for a slice.

Suggested fields:

- `id`
- `slice_id`
- `original_text`
- `modified_text`
- `is_modified`
- `alignment_data`

Rules:

- `original_text` preserves the upstream transcript
- `modified_text` may be blank and still be intentional valid data
- Alignment metadata is detail-only; queue payloads should not overfetch it

## Tag And SliceTag

`Tag` defines a reusable user-visible label.

Suggested fields:

- `id`
- `name`
- `color`

`SliceTag` is the many-to-many join between slices and tags.

Rules:

- Tags support filtering and QA
- Tags do not determine export by themselves
- Tags must be captured in slice revisions so undo/redo can restore them

## ExportRun

`ExportRun` captures project-level export attempts.

Suggested fields:

- `id`
- `batch_id`
- `status`
- `output_root`
- `manifest_path`
- `accepted_clip_count`
- `failed_clip_count`
- `created_at`
- `completed_at`

Rules:

- Export uses accepted persisted slice state
- Exported transcript text comes from the current persisted transcript value
- Export failures must not corrupt prior runs

## Managed Artifact Layout

The runtime-managed media root is:

- `backend/data/media/`

Expected artifact categories:

- `sources/`
- `variants/`
- `slices/`
- `peaks/`

Rules:

- Variant, slice-render, and peak caches are managed artifacts
- Slice render and peak cache keys should be derived from audio state, not every metadata-only revision
- Cache cleanup must prune stale slice and peak artifacts

## One-Line Summary

Phase 1 uses immutable source recordings and immutable variants underneath a full-state slice revision model, so playback, undo/redo, and export all operate from the same persisted slice state.
