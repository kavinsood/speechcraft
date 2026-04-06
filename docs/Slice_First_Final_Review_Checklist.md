# Slice-First Final Review Checklist

This document is the shortest path for reviewing the slice-first refactor.

Read these first:
- [RFC_Slice_First_Refactor.md](/home/aaravthegreat/Projects/speechcraft/docs/RFC_Slice_First_Refactor.md)
- [Slice_First_Backend_UI_Boundaries.md](/home/aaravthegreat/Projects/speechcraft/docs/Slice_First_Backend_UI_Boundaries.md)
- [slice-first-final-review.patch](/home/aaravthegreat/Projects/speechcraft/docs/slice-first-final-review.patch)

## What Changed

The old public architecture:
- `source recording -> review windows -> review-window ASR / forced-align-and-pack -> slices`

The new public architecture:
- `source recording -> source transcript/alignment artifacts -> direct slicing -> slices`

The important consequences:
- Clip Lab is slice-only.
- Recording-level jobs are the only processing model for transcription, alignment, and slicing.
- `training_start` / `training_end` remain canonical export truth.
- Review-window public routes are gone.
- The old review-window database schema is purged lazily by an explicit startup migration.

## Files To Review

Primary backend:
- [models.py](/home/aaravthegreat/Projects/speechcraft/backend/app/models.py)
- [repository.py](/home/aaravthegreat/Projects/speechcraft/backend/app/repository.py)
- [main.py](/home/aaravthegreat/Projects/speechcraft/backend/app/main.py)
- [slicer_algo.py](/home/aaravthegreat/Projects/speechcraft/backend/app/slicer_algo.py)

Primary frontend:
- [api.ts](/home/aaravthegreat/Projects/speechcraft/frontend/src/api.ts)
- [types.ts](/home/aaravthegreat/Projects/speechcraft/frontend/src/types.ts)
- [LabelPage.tsx](/home/aaravthegreat/Projects/speechcraft/frontend/src/pages/LabelPage.tsx)
- [ClipQueuePane.tsx](/home/aaravthegreat/Projects/speechcraft/frontend/src/workspace/ClipQueuePane.tsx)
- [EditorPane.tsx](/home/aaravthegreat/Projects/speechcraft/frontend/src/workspace/EditorPane.tsx)

Primary tests:
- [test_repository_media.py](/home/aaravthegreat/Projects/speechcraft/backend/tests/test_repository_media.py)
- [test_media_requests.py](/home/aaravthegreat/Projects/speechcraft/backend/tests/test_media_requests.py)
- [test_slicer_algo.py](/home/aaravthegreat/Projects/speechcraft/backend/tests/test_slicer_algo.py)

Deleted legacy pieces:
- [slicer_core.py](/home/aaravthegreat/Projects/speechcraft/backend/app/slicer_core.py)
- [test_slicer_core.py](/home/aaravthegreat/Projects/speechcraft/backend/tests/test_slicer_core.py)

## Main Reviewer Questions

### 1. Is the public contract fully slice-first now?
Check that:
- the frontend does not request review windows anymore
- Clip Lab item loading is slice-only
- waveform and media loading are slice-only in the active UI path

Expected answer:
- yes

### 2. Does source truth live on the recording side?
Check that:
- transcript/alignment artifacts are recording-level
- slicing consumes recording-level artifacts
- slices are no longer the storage location for source alignment truth

Expected answer:
- yes

### 3. Is reslicing safe for human-reviewed work?
Check that:
- slices can be locked
- human transcript/state edits lock the slice
- reslice preserves locked slices and drops overlapping fresh generated slices
- no “gap-fill around locked slices” logic exists

Expected answer:
- yes

### 4. Is export truth still exact?
Check that:
- export is based on slice truth only
- `training_*` bounds remain canonical for export
- padded/review-safe bounds are not used in export rendering

Expected answer:
- yes

### 5. Is the old architecture actually removed, not just hidden?
Check that:
- public review-window routes are deleted
- legacy review-window models are deleted
- old review-window job kinds are deleted
- the old punctuation-led slicer is deleted

Expected answer:
- yes, except for the intentional legacy schema cleanup code

## Intentional Legacy Remnant

The only allowed remaining `review_window` references are in the explicit startup migration path in:
- [repository.py](/home/aaravthegreat/Projects/speechcraft/backend/app/repository.py)

Why they remain:
- they are deleting old SQLite tables/columns safely
- they are not part of the live product path

If a reviewer finds `review_window` references outside legacy schema purge code, that is likely a bug or incomplete cleanup.

## Verification Commands

Backend compile:
```bash
python -m py_compile backend/app/main.py backend/app/models.py backend/app/repository.py backend/tests/test_repository_media.py backend/tests/test_media_requests.py
```

Backend tests:
```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend python -m unittest tests.test_repository_media tests.test_slicer_algo
```

Frontend build:
```bash
npm run build
```

## What Was Explicitly Deferred

These are not bugs in this PR unless they regress current behavior:
- client-side source-buffer offset playback for slice review
- reintroducing split/merge
- auto-pick / health-score export policy
- deeper alignment-confidence-aware collar logic

## Known Review Frame

This refactor should be reviewed as:
- a contract collapse from dual-kind Clip Lab to slice-only
- a data-model shift from review-window intermediates to recording-level artifacts
- a removal of obsolete routing and schema

It should not be reviewed as:
- a redesign of the current slicer algorithm
- a UI redesign
- an export-policy overhaul
