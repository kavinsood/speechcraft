# Agent Audio Data Setup

This repo now stores managed runtime media under `backend/data/media/`, not `backend/media/`.

## Canonical Runtime Layout

The managed media root is:

- `backend/data/media/`

Current subdirectories:

- `backend/data/media/sources/`
- `backend/data/media/variants/`
- `backend/data/media/slices/`
- `backend/data/media/peaks/`

What each directory means:

- `sources/`: managed source recordings created by demo seeding or import flows
- `variants/`: immutable physical slice variants
- `slices/`: cached rendered slice audio for the active slice state
- `peaks/`: cached waveform peak JSON keyed by slice audio state and bin count

## Persistence Location

Project and slice state now lives in:

- `backend/data/project.db`

Legacy JSON at `backend/data/phase1-demo.json` is only used as a one-time seed/import source when the SQLite database does not exist yet.

## Media Path Rules

- Do not point runtime media into `/tmp`.
- Do not rely on client-visible absolute server paths.
- Managed variant, slice, and peak artifacts should stay under `backend/data/media/`.
- Source recordings should use stable local paths. Managed source assets should prefer `backend/data/media/sources/`.

## Verification Checklist

- `backend/data/project.db` exists after backend startup.
- `backend/data/media/` exists with the expected subdirectories.
- `GET /media/variants/{variant_id}.wav` serves managed variant media.
- `GET /media/slices/{slice_id}.wav` serves edited slice media.
- `GET /api/clips/{clip_id}/waveform-peaks` returns cached or generated peaks for the current slice audio state.
- No managed media path points into `/tmp`.

## Recovery Notes

If you are restoring a machine state or import dataset:

1. Put stable source recordings in a non-temporary location.
2. Prefer copying repo-owned runtime media into `backend/data/media/`.
3. Start the backend so SQLite can seed or migrate state.
4. Verify that edited slice playback works through `/media/slices/{slice_id}.wav`.
5. Verify that waveform peaks appear and that `backend/data/media/peaks/` populates.

## Operational Notes For Agents

- Treat `backend/data/media/` as managed runtime data, not source-controlled product code.
- Avoid changing managed file paths by hand unless you are intentionally migrating data.
- If imported source media is missing, the correct behavior is media-offline or validation failure, not silent synthetic substitution.
