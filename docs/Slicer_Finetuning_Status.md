## Scope

This document describes the current state of the fine-tuning slicer prototype in:

- [slicer_algo.py](/home/aaravthegreat/Projects/speechcraft/backend/app/slicer_algo.py)
- [evaluate_slicer_algo.py](/home/aaravthegreat/Projects/speechcraft/backend/scripts/evaluate_slicer_algo.py)
- [import_provisional_slicer_batch.py](/home/aaravthegreat/Projects/speechcraft/backend/scripts/import_provisional_slicer_batch.py)

This is the experimental path used to test the slicer on local datasets before folding the logic into the main repository flow.

## Current Objective

The slicer is for fine-tuning, not reference selection.

Current priority order:

1. do not cut words
2. do not cut breaths if avoidable
3. transcript must match kept audio
4. duration distribution should stay reasonable
5. punctuation is secondary and should be pause-faithful, not boundary-driving

This means the slicer is no longer trying to be a sentence chunker. It is a boundary-safety and transcript-integrity pass.

## Current Implementation Phases

### Phase 1

Phase 1 replaced the older punctuation-led boundary logic with acoustic-safe inter-word gaps.

Implemented:

- safe-gap boundary discovery instead of punctuation ranking
- leading/trailing no-cut collars around aligned words
- no forced cuts through active speech
- long/no-safe-boundary clips are flagged instead of being cut badly

Result:

- removed the worst half-word amputations
- made the slicer conservative enough to listen to real outputs

### Phase 2

Phase 2 tightened acoustic edge quality.

Implemented:

- acoustic scoring for candidate gaps
- valley snapping across the full safe gap window
- rejection of acoustically noisy gap candidates
- edge-energy flagging for suspicious starts/ends

Result:

- fewer ugly cuts even when words remained intelligible before
- clips that still sounded rough began to correlate with QC flags

### Phase 3

Phase 3 separated training truth from review audition and added transcript/QC cleanup.

Implemented:

- canonical `training_start` / `training_end`
- separate `padded_*` review bounds
- pause-faithful punctuation normalization from aligned pauses
- breath-side boundary choice for detected breath gaps
- QC flags for edge energy and long edge silence

Result:

- cleaner export contract
- better basis for future policy gating and auto-triage

## Current Output Contract

Each slice now has multiple boundary concepts:

- `raw_start` / `raw_end`
  - aligned word span
- `snapped_start` / `snapped_end`
  - acoustically refined boundary
- `training_start` / `training_end`
  - canonical non-overlapping export truth

Optional review context should now be derived dynamically around `training_*`, not persisted as slicer truth.

Important rule:

- training/export should use `training_*`
- Clip Lab default playback should use exact `training_*`
- any extra context playback must be explicit and derived dynamically

Do not treat review bounds as training truth.

## Current Test Method

The slicer has been tested in two ways:

- unit tests in [test_slicer_algo.py](/home/aaravthegreat/Projects/speechcraft/backend/tests/test_slicer_algo.py)
- real-dataset evaluation on local long-form recordings using:
  - ASR + alignment on full WAVs
  - slicer evaluation via [evaluate_slicer_algo.py](/home/aaravthegreat/Projects/speechcraft/backend/scripts/evaluate_slicer_algo.py)
  - temporary import into Clip Lab for ear review

Current temp review path:

- project id: `emma-fullwav-slicer-temp`
- dataset under test: local long-form validation recordings

Latest broad result:

- vast majority of clips are acceptable by ear
- remaining issues are a small number of edge spikes / carry-in / carry-out artifacts

## Remaining Problems

Current real residual problem class:

- slight spike at the end of a small number of clips
- slight spike at the beginning of a small number of clips
- occasional tiny non-transcript carry-over adjacent to a boundary

These are not whole-algorithm failures.
They are edge-polish failures on top of an otherwise acceptable slicer.

The right next fix is likely a narrow post-pass:

- only for flagged high-edge-energy clips
- move the bad edge inward by a small amount
- do not cross aligned word boundaries
- do not aggressively rewrite breath-preserving edges

## Notes For Code Reviewer

The current algorithm is intentionally acoustic-first.

Reviewer should keep these design assumptions in mind:

- punctuation should not be the primary cut signal
- review-window boundaries are not final slice boundaries
- no-cut word collars are deliberate and should not be relaxed casually
- forcing a cut through speech to hit a duration target is considered worse than returning a longer flagged clip
- review-safe import behavior is a listening aid, not training truth

### What Was Tried And Did Not Work

These attempts were tried during testing and should not be reintroduced casually:

- punctuation-led boundary selection
  - produced too many half-cut words and bad linguistic-first boundaries

- importing `padded_*` bounds into Clip Lab for review
  - sounded like clipped next/previous words because overlapped context was being auditioned as literal clip truth

- importing strictly `raw_*`-anchored review bounds
  - made many clips sound hard-cut even when the slicer had chosen a better acoustic snap point

- aggressively clamping review bounds to adjacent raw-word boundaries
  - reduced tail junk in some cases but introduced more audible hard cuts overall

- capped non-breath drift compromise for review bounds
  - was better than the hard clamp but still worse by ear than the earlier snapped-centered review import

### Review-Tooling Issues That Were Not Core Slicer Bugs

During testing, a few issues came from the review tooling rather than the slicer itself:

- stale waveform peak caches caused fake half-waveform rendering in Clip Lab
- peak-driven waveform rendering sometimes diverged from the actual audio bytes

Those issues were in the audition path, not the slice boundary algorithm itself.

## Reviewer Summary

Current status is:

- the slicer is good enough to review real clips in Clip Lab
- most clips are acceptable now
- the remaining work should be surgical edge polishing, not another full algorithm rewrite

If reviewing code changes, bias toward preserving:

- acoustic-safe boundaries
- training/review bound separation
- conservative no-force-cut behavior

and treat any change that improves a few flagged clips at the cost of destabilizing the majority as suspect.
