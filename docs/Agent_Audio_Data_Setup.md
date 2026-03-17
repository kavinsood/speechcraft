# Agent Audio Data Setup

This repo expects real imported project audio to live in a stable repo-owned location, not in `/tmp`.

## Canonical Location

Put restored source media under:

- `backend/media/`

Keep the recovered archive directory structure intact underneath that folder. The current working layout is:

- `backend/media/Charactors/EmmaWatson/voxCPM/raw/...`
- `backend/media/Charactors/HeyBillieRae/voxCPM/audio/...`

Do not flatten or rename these subtrees unless you also rewrite every affected `audio_path` in project state.

## Source Archive

The known-good archive we used was:

- `~/Downloads/speechcraft-final-machine-state.tar.gz`

That archive contains the real media needed by imported projects that otherwise render as offline.

## Extraction Rule

Extract the archive contents into a stable location inside the repo, then copy or move the media subtree into:

- `backend/media/`

Do not extract to:

- `/tmp/...`
- a user-specific home directory path that is not part of the repo

Those locations make the recovered project state brittle and machine-specific.

## State That Must Match The Media

Imported clips are resolved through `audio_path` values stored in:

- `backend/data/phase1-demo.json`

For imported projects, those paths must point at actual files under this repo, for example:

- `/home/kavin/github/speechcraft/backend/media/Charactors/EmmaWatson/voxCPM/raw/output_004.wav_0000000000_0000173120.wav`
- `/home/kavin/github/speechcraft/backend/media/Charactors/HeyBillieRae/voxCPM/audio/billie_000001.wav`

If the files are present but `audio_path` still points somewhere else, the UI will show media-offline behavior.

## Important Distinction

Not every clip in `backend/data/phase1-demo.json` should have a real source file.

- Imported clips should have a real `audio_path`.
- Built-in synthetic demo clips may still have `audio_path: null`.

That is expected. Do not try to backfill synthetic demo clips with archive media.

## Recommended Recovery Procedure

1. Ensure `backend/media/` exists.
2. Restore the media subtree from `~/Downloads/speechcraft-final-machine-state.tar.gz` into `backend/media/`.
3. Preserve the `Charactors/...` structure exactly as recovered.
4. Rewrite imported clip `audio_path` entries in `backend/data/phase1-demo.json` so they point to the local repo path under `backend/media/`.
5. Restart the backend.
6. Open the `Label` step and verify imported clips render real waveforms instead of an offline state.

## Verification Checklist

- `backend/media/` exists and contains the recovered audio files.
- Imported clip `audio_path` values resolve to real files on disk.
- The backend can serve `/api/clips/{clip_id}/audio` for imported clips.
- The frontend waveform loads for imported clips.
- No restored paths point into `/tmp`.

## Operational Notes For Agents

- Treat `backend/media/` as repo-owned runtime data.
- Prefer stable absolute paths rooted in the current repo checkout when rewriting `audio_path`.
- If a clip is imported and its file is missing, the correct behavior is media offline, not synthetic fallback.
