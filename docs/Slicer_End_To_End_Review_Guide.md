## Purpose

This document describes the current experimental fine-tuning slicer path end to end, starting from one raw audio file and ending at provisional slices inside Clip Lab.

It is written for human review.
The goal is not only to explain the intended flow, but also to expose every place where edge cases can enter:

- bad source audio
- bad transcript text
- bad alignment
- bad boundary choice
- bad review-only bound extension
- bad import/export semantics

This document reflects the current implementation in:

- [slicer_algo.py](/home/aaravthegreat/Projects/speechcraft/backend/app/slicer_algo.py)
- [evaluate_slicer_algo.py](/home/aaravthegreat/Projects/speechcraft/backend/scripts/evaluate_slicer_algo.py)
- [import_provisional_slicer_batch.py](/home/aaravthegreat/Projects/speechcraft/backend/scripts/import_provisional_slicer_batch.py)
- [align_existing_segments.py](/home/aaravthegreat/Projects/speechcraft/backend/scripts/align_existing_segments.py)
- [run_folder_asr_align.py](/home/aaravthegreat/Projects/speechcraft/backend/scripts/run_folder_asr_align.py)

This is the temporary local validation path used before the slicer is folded into the main backend processing flow.

## Scope

This is the current path for testing one file:

1. Start with one raw long-form audio file.
2. Obtain a transcript.
3. Obtain word timings by forced alignment.
4. Run the standalone slicer.
5. Write a slicer JSON manifest.
6. Import provisional slices into Clip Lab.
7. Listen by ear and look for edge cases.

This is not yet the final product pipeline.
It is the current evaluation path for validating the slicer behavior.

## High-Level Mental Model

The slicer is for fine-tuning.
Its priorities are:

1. do not cut words
2. do not cut breaths if avoidable
3. keep transcript aligned to kept audio
4. keep durations reasonable
5. treat punctuation as secondary metadata, not a primary cut rule

The algorithm is therefore acoustic-first and transcript-safe, not sentence-first.

## End-To-End Pipeline

### Step 0: Input File

Input:

- one long audio file, usually WAV
- optionally one existing transcript or transcript JSON with timed ASR segments

The audio used by the slicer path must be decodable and, for the current worker aligner, effectively usable as 16-bit PCM WAV.

Files involved:

- raw source recording
- transcript `.txt` or transcript `.json`

Human review questions:

- Is the file single-speaker?
- Is there music, TV, crosstalk, room noise, or source-separation residue?
- Does the file sound naturally recorded or heavily processed?
- Does the file have clipped starts/ends, desync, or long silent regions?

Likely edge cases introduced here:

- multi-speaker audio
- loud background music
- demucs/source-separation artifacts
- clipped source audio
- stereo phase weirdness
- float WAV or unsupported format in the aligner path

### Step 1: Transcript Acquisition

There are two current transcript-entry routes:

- `run_folder_asr_align.py`
  - runs ASR on whole WAV files
- pre-existing transcript JSON
  - used with `align_existing_segments.py`

If a transcript JSON already exists and contains timed ASR segments, the current preferred path is:

1. reuse that transcript
2. do not rerun ASR
3. run forced alignment over each existing segment
4. merge aligned word timings back into one absolute timeline

Human review questions:

- Does the transcript actually match the spoken words?
- Are numbers, names, slang, or repeated fillers transcribed plausibly?
- Are there segments where the text is clearly paraphrased or wrong?

Likely edge cases introduced here:

- ASR text mismatch
- missing words
- hallucinated words
- wrong normalization for numbers/symbols
- punctuation that does not reflect pauses

Important note:

- Wrong ASR can poison alignment.
- Wrong alignment can poison slicing.
- Small residual spikes at slice edges can come from bad local transcript alignment even if the rest of the file is good.

### Step 2: Forced Alignment

Current worker:

- [run_aligner.py](/home/aaravthegreat/Projects/speechcraft/workers/aligner/run_aligner.py)

Current forced-alignment behavior:

1. load torchaudio MMS forced-alignment bundle
2. load and normalize transcript text
3. normalize special tokens
4. convert numbers to words in a simple way
5. remove unsupported characters
6. align characters/tokens against model emissions
7. merge token spans into word spans
8. restore skipped words if necessary

There are two current ways this worker is used:

- whole-file transcript -> whole-file alignment
- segment transcript JSON -> per-segment alignment -> merged global alignment

For segment-based alignment:

- [align_existing_segments.py](/home/aaravthegreat/Projects/speechcraft/backend/scripts/align_existing_segments.py)
  - reads transcript JSON `segments`
  - slices the WAV into segment chunks
  - aligns each segment chunk separately
  - offsets the resulting word times back into full-file time
  - merges all word timings into one alignment JSON

Outputs:

- `*.alignment.json`
- optional `_summary.json`

Human review questions:

- Did all segments process?
- Does the alignment span roughly cover the real speaking duration?
- Do a few spot-checked words visually and audibly land where expected?

Likely edge cases introduced here:

- worker model missing or download blocked
- unsupported transcript tokens
- weird numeric normalization
- slight word-boundary drift
- alignment that is globally okay but locally late/early
- fallback/interpolated words

Why this step matters so much:

- the slicer trusts word boundaries
- if alignment is wrong, the slicer can choose a “safe” boundary that still leaks a consonant or cuts a release

### Step 3: Audio Loading For Slicer Evaluation

Current standalone eval entrypoint:

- [evaluate_slicer_algo.py](/home/aaravthegreat/Projects/speechcraft/backend/scripts/evaluate_slicer_algo.py)

What it does:

1. load one WAV
2. downmix to mono if needed
3. read the alignment JSON
4. call `plan_slices(...)`
5. print aggregate stats
6. optionally write a full slicer result JSON

Important constraint:

- this eval script currently expects 16-bit PCM WAV input

Human review questions:

- Was the actual source file used, or a converted working copy?
- If converted, did conversion change timing, sample rate, or audibility?

Likely edge cases introduced here:

- PCM conversion mismatch
- wrong file path pairings
- stale alignment JSON for the wrong audio file

### Step 4: Alignment Parsing In `slicer_algo.py`

Entry:

- `plan_slices(...)`

First real algorithm step:

- `parse_alignment(raw)`

What it does:

1. read each aligned word
2. clamp any backward-overlapping word starts forward
3. drop zero-length or negative-duration words
4. keep the remaining aligned words in time order

Result:

- a normalized list of `AlignedWord`

Human review questions:

- Are there warnings about overlapping or zero-duration words?
- Are there suspicious dropped words?

Likely edge cases introduced here:

- malformed alignment payload
- overlapping word timings
- zero-duration tokens
- confidence fields missing or fake

Important limitation:

- current confidence values are often not trustworthy enough to drive fine-grained policy yet

### Step 5: RMS Frame Computation

Function:

- `_compute_rms_frames(...)`

What it does:

1. normalize the audio array to mono-like shape
2. chop it into fixed RMS windows
3. compute RMS energy per frame
4. compute frame-center timestamps

Current default:

- `10ms` RMS frames

Why this exists:

- the slicer is acoustic-first
- it needs a simple energy map to detect quieter valleys between words

Human review questions:

- Does the file have stable energy behavior, or lots of music/phasing that makes RMS misleading?

Likely edge cases introduced here:

- music residue that looks like speech
- source-separated phase smear
- breath/noise that creates fake valleys or fake transients

### Step 6: Candidate Boundary Discovery

Function:

- `find_candidate_boundaries(...)`

This is where the slicer proposes legal cut locations.

What it does for each pair of adjacent words:

1. compute a protected no-cut gap using `_safe_gap_window(...)`
2. enforce no-cut collars:
   - trailing guard after the left word
   - leading guard before the right word
3. measure the remaining safe gap
4. skip the pair if the safe gap is too small
5. choose a provisional cut in the safe gap
6. score the cut acoustically
7. reject it if the acoustic score is too poor
8. keep it as a `BoundaryCandidate` if it passes

Key idea:

- a gap is only usable if it is both word-safe and acoustically quiet enough

Current important config values:

- `min_gap_for_boundary`
- `preferred_gap_for_boundary`
- `leading_word_guard_ms`
- `trailing_word_guard_ms`
- `min_boundary_acoustic_score`

Human review questions:

- Are candidates being generated only in real between-word gaps?
- On bad files, are good gaps being missed because guards are too strict?
- On noisy files, are fake low-energy regions still passing?

Likely edge cases introduced here:

- narrow gaps that contain no truly safe boundary
- plosive release or fricative tail inside a gap
- musical valleys that are not real non-speech
- slight alignment drift making a true safe gap look unsafe or vice versa

### Step 7: Acoustic Snap Inside The Gap

Function:

- `_snap_boundary_in_gap(...)`

What it does:

1. restrict search to the candidate gap
2. compute the lowest RMS inside that gap
3. build a tolerance around that minimum
4. select a low-energy point close to the gap midpoint

What this means in plain language:

- do not cut at the edge of silence
- cut at a quiet valley inside the safe region

Why midpoint matters:

- if a gap has several similarly quiet points, the algorithm biases toward the center instead of hugging one word too closely

Human review questions:

- Are cuts landing in obviously quiet spots?
- Are they hugging one side of the gap too much?
- On noisy files, are the “quietest” points still too hot to sound clean?

Likely edge cases introduced here:

- quietest point still contains a transient
- no true silence, only “least bad” audio
- source-separation artifacts making valleys deceptive

### Step 8: Boundary Acoustic Scoring

Function:

- `_boundary_acoustic_score(...)`

What it does:

1. estimate local valley energy around the cut
2. estimate speech-reference energy from the nearby word edges
3. compare valley energy against speech energy
4. produce a normalized acoustic score

Interpretation:

- high score means the cut sits in a relatively quiet dip
- low score means the “gap” still looks too much like active speech

Human review questions:

- Are bad sounding edges correlated with low acoustic score or flags?
- On difficult files, does this reject too many candidates or too few?

Likely edge cases introduced here:

- valley appears quiet relative to speech but still sounds bad
- breaths behave differently from silence
- low-energy background music fools the ratio

### Step 9: Greedy Grouping Into Slice Specs

Functions:

- `greedy_group(...)`
- `_find_slice_end(...)`
- `_build_spec(...)`

This is where the algorithm turns many possible boundaries into actual provisional clip spans.

What it does:

1. start from the first word
2. scan forward through available boundary candidates
3. ignore cuts that would make the slice too short
4. keep the best boundary that stays under `max_duration`
5. if none exist under `max_duration`, take the first one over
6. if no candidate exists at all, end at the recording end
7. create a `SliceSpec` for that word range
8. repeat from the next word after the chosen boundary

Important policy:

- no forced cuts through active speech
- if the file does not offer a good safe gap, the slicer prefers a longer clip over a bad cut

`_build_spec(...)` also:

- stores `raw_start` and `raw_end`
- stores transcript and transcript_original
- computes average confidence
- applies initial flags like:
  - `long_*`
  - `no_safe_boundary_*`
  - `low_confidence_*`

Human review questions:

- Are long clips happening because the source truly lacks safe gaps?
- Are bad clips coming from grouping or from later refinement?

Likely edge cases introduced here:

- a clip gets long because no safe cut exists
- local duration optimum is not the same as perceptual optimum
- confidence-based flags are currently limited by weak confidence data

### Step 10: Merge Short Clips

Functions:

- `_merge_short_clips(...)`
- `_merge_two(...)`

Purpose:

- avoid tiny unusable clips

What it does:

1. find any slice shorter than `min_duration`
2. merge it with the previous or next neighbor
3. choose the merge that lands closer to target duration
4. rebuild transcript, word count, and duration
5. carry flags forward

Current limitation:

- this policy is duration-first, not boundary-weakness-first

Human review questions:

- Are tiny clips being merged into a more natural neighbor?
- Did merging keep a worse boundary alive just because duration looked prettier?

Likely edge cases introduced here:

- awkward phrase structure after merge
- wrong neighbor chosen in ambiguous cases

### Step 11: Acoustic Refinement

Function:

- `apply_acoustic_refinement(...)`

This is where the grouped clips get their real boundary variants.

What it does:

1. reset all specs to raw defaults
2. keep first clip start and last clip end anchored to raw recording edges
3. for each adjacent pair of clips:
   - compute the shared gap region
   - detect breath-like gap as a hint
   - snap the shared boundary again inside the legal gap
   - assign that shared timestamp as:
     - left `snapped_end`
     - right `snapped_start`
4. for each final spec:
   - set `training_start = snapped_start`
   - set `training_end = snapped_end`
   - build review-only `padded_start` and `padded_end`
   - compute edge energies
   - compute leading/trailing silence
   - compute speech ratio
   - add QC flags if needed

Critical design rule:

- `training_*` is canonical export truth
- `padded_*` is review-only context

This separation is one of the most important current policies.

Human review questions:

- Do the `training_*` bounds sound clean and word-safe?
- Do the `padded_*` bounds help listening without reintroducing junk?
- Are flagged clips actually the suspicious ones by ear?

Likely edge cases introduced here:

- padded review tails stepping into a later transient
- breath hints preserving too much context
- snapped boundary landing on a hard non-zero waveform edge

### Step 12: Breath Hinting

Function:

- `_detect_breath(...)`

Current role:

- hint-level only
- influences edge metadata and review padding behavior
- does not strongly control canonical training boundary choice anymore

What it does:

1. only examine gaps within a breath-like duration range
2. compute RMS for the gap
3. reject if too quiet or too loud
4. inspect frame-level RMS variance
5. classify the gap as breath-like if it looks plausible

Human review questions:

- Is the detector mostly finding natural inhale/exhale-like gaps?
- Is it confusing fricatives, noise, or separator residue for breath?

Likely edge cases introduced here:

- false breath on noise
- false breath on fricatives
- false breath on source-separation residue

Important note:

- this is intentionally heuristic and weak
- it should be treated as a hint, not truth

### Step 13: Transcript Normalization From Pauses

Function:

- `_render_pause_faithful_transcript(...)`

What it does:

1. render the slice transcript from the aligned word list
2. look at pause duration between adjacent words
3. remove pause punctuation if the pause is tiny
4. insert commas if the pause is large enough and punctuation is absent

Important policy:

- this is a derived normalization layer
- it is not transcript truth
- it should not overwrite reviewed source truth in the future main pipeline

Human review questions:

- Does punctuation roughly reflect real pauses?
- Are commas appearing in natural places?
- Are tiny no-pause commas being removed as intended?

Likely edge cases introduced here:

- ASR punctuation that was already wrong
- alignment pause errors causing punctuation changes

### Step 14: QC Flags

Flags are added in multiple places.
Current categories include:

- long clip
- no safe boundary
- low confidence
- high start edge energy
- high end edge energy
- leading silence too long
- trailing silence too long
- low speech ratio

Human review questions:

- Are flagged clips actually suspicious by ear?
- Are obviously bad clips getting flagged?
- Are too many good clips being flagged on noisy-but-usable files?

Likely edge cases introduced here:

- false positives on noisy but usable data
- false negatives when a spike is perceptual but not very energetic

### Step 15: Building Slice Entries

Function:

- `build_slice_entries(...)`

What it writes for each slice:

- transcript
- transcript_original
- `raw_start` / `raw_end`
- `snapped_start` / `snapped_end`
- `training_start` / `training_end`
- `padded_start` / `padded_end`
- relative word offsets from `training_start`
- overlap metrics
- boundary type
- gap size
- flags
- breath hints
- edge energies

Important review semantic:

- relative word offsets are now anchored to `training_start`
- not `padded_start`

This prevents review-only context from acting like canonical training truth.

Human review questions:

- Are exported word offsets consistent with the actual training clip?
- Do overlap metrics match what the UI is auditioning versus what training would export?

### Step 16: Aggregate Slice Stats

Function:

- `build_slice_stats(...)`

This computes run-level stats like:

- total clips
- duration distribution buckets
- flagged count
- forced cut count
- coverage
- training overlap
- review overlap
- average alignment confidence

Human review questions:

- Do the duration distributions look sane for fine-tuning?
- Is flagged rate suspiciously high?
- Is review overlap much larger than training overlap, as expected?

### Step 17: Writing The Slicer Manifest

Current output:

- one JSON file per source file

Example:

- `backend/exports/.../<stem>.json`

This file is the handoff between the pure slicer path and temp Clip Lab import.

Human review questions:

- Does the manifest look consistent with the audio?
- Are there slices with obviously absurd durations or timestamps?

### Step 18: Temp Import Into Clip Lab

Script:

- [import_provisional_slicer_batch.py](/home/aaravthegreat/Projects/speechcraft/backend/scripts/import_provisional_slicer_batch.py)

What it does:

1. create or replace a temp import batch
2. register each source WAV as a `SourceRecording`
3. read the slicer JSON
4. choose import bounds according to `--bounds-mode`
5. create normal slice rows in the app DB
6. attach slicer metadata to each imported slice

Current bounds modes:

- `raw`
- `snapped`
- `padded`
- `review_safe`

Current intended mode for listening:

- `review_safe`

Important distinction:

- `review_safe` is a listening layer
- it is not training truth

### Step 19: Review-Safe Bound Resolution

Function:

- `resolve_review_safe_bounds(...)`

What it does:

1. start from `snapped_start` / `snapped_end`
2. add modest review context
3. optionally give more context around breath-marked edges
4. respect neighboring slice boundaries and word-edge margins
5. if review extension enters a hot transient:
   - trim back using `_adjust_review_edge(...)`
   - seek a quieter local zero-crossing

This behavior exists because pure snapped/training bounds can sound too hard-cut for human review, while blind padding can reintroduce junk that training would not actually use.

This was a major source of confusion during testing.

Important tested result:

- several audible “spikes” were caused by review-safe extension stepping beyond a clean training boundary
- those were fixed in the review layer without changing canonical training boundaries

Human review questions:

- Does the review clip sound natural without lying about what the training clip really is?
- Are review-only spikes or carry-over being mistaken for slicer-core failures?

Likely edge cases introduced here:

- review tail catches the onset of the next word
- review head catches the tail of the previous word
- zero-crossing trim still sounds perceptually rough

### Step 20: Human Ear Review

This is the most important manual validation stage.

What the reviewer should listen for:

- half-cut first word
- half-cut last word
- click/spike at the start
- click/spike at the end
- tiny non-transcript carry-in or carry-out
- clipped breath
- preserved breath with extra junk after it
- transcript mismatch
- weird overlong clip with no obvious reason
- music/noise residue being treated as part of speech

What the reviewer should compare:

- what the clip sounds like
- what the transcript says
- whether the issue appears in training truth or only review-safe audition

## File Variants And Their Meaning

### Source Audio

The raw full recording.
This is the original truth for timing.

### Alignment JSON

Word-level timing over the source timeline.
This is the slicer’s primary structural input.

### Slicer Manifest JSON

The slicer’s output contract.
This includes all boundary variants and QC flags.

### Imported Clip Lab Slice

A temp review object created from the slicer manifest.
This is a listening and triage surface, not yet the final integrated backend contract.

## Canonical Truth vs Review Convenience

This distinction must stay explicit:

- `training_start` / `training_end`
  - canonical export truth
- `padded_start` / `padded_end`
  - review context
- `review_safe`
  - audition policy layered on top of slicer output

If a clip sounds bad in `review_safe`, it does not automatically mean the training boundary is bad.

Several real spike cases were caused by the review layer, not by `training_*`.

## Known Failure Sources By Layer

### Source Layer

- noisy recording
- music
- demucs residue
- crosstalk
- clipped original mic signal

### Transcript Layer

- ASR mismatch
- bad normalization
- names/numbers wrong
- punctuation wrong

### Alignment Layer

- slightly early or late word edges
- fallback/interpolated words
- locally wrong but globally okay alignment

### Candidate Boundary Layer

- fake safe gaps
- safe gap too small
- acoustic valley misleading

### Grouping Layer

- longer-than-desired clip because no safe gap exists
- suboptimal short-clip merge

### Refinement Layer

- snapped cut still lands on a perceptually sharp transient
- breath hint causes too much review context

### Review Import Layer

- review extension into a transient
- review extension into the next word onset
- waveform display/cache bugs making audio look broken when it is not

## Human Review Checklist

For one file under test, review at least:

- first 10 clips
- last 10 clips
- several flagged clips
- several unflagged clips
- one short clip
- one long clip
- one clip with breath at start
- one clip with breath at end
- one clip adjacent to obvious music/noise

For each bad clip, classify the failure:

- transcript wrong
- alignment wrong
- boundary wrong
- review-only bound wrong
- waveform/UI bug
- source audio itself bad

That classification is more useful than just “clip sounds bad.”

## Reviewer Questions To Answer

When reviewing a dataset, try to answer these:

1. Are most bad clips true training-boundary failures or only review-audition failures?
2. Are flags well correlated with audible problems?
3. Are noisy datasets failing because of alignment or because of the boundary policy itself?
4. Do breaths sound naturally preserved or sloppily carried over?
5. Are there recurring issues on source-separated audio that do not appear on clean mic audio?
6. Are long clips happening for understandable reasons?
7. Is the transcript generally accurate enough that alignment can be trusted?

## Current Practical Interpretation

Based on current testing:

- clean mic-heavy recordings perform well
- the slicer core is now broadly acceptable for human review
- many remaining “spike” issues turned out to be review-layer issues, not training-boundary issues
- noisier and more processed audio should still be treated with more suspicion because alignment can drift and RMS valleys can become less trustworthy

## Recommended Use Of This Doc

Use this document when:

- reviewing slicer code
- reviewing a dataset by ear
- diagnosing a bad clip
- deciding whether a failure belongs to:
  - transcript acquisition
  - alignment
  - slicer core
  - review import behavior

If a reviewer reports an issue, the ideal bug report should include:

- source file
- slice index
- transcript text
- whether the problem is at start or end
- whether the problem exists in `training_*` or only `review_safe`
- whether the clip was flagged
- whether the source audio is clean, noisy, or source-separated
