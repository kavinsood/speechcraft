## Context

Related implementation notes live in [Slicer_Finetuning_Status.md](/home/aaravthegreat/Projects/speechcraft/docs/Slicer_Finetuning_Status.md).

Speechcraft is a browser-first speech dataset workstation for preparing training data for speech-model fine-tuning. It is not meant to be a generic TTS platform. Its core job is helping an operator turn messy long-form source recordings into clean, inspectable, reproducible training slices.

The important product idea is that there are two different layers of objects:

- `source recordings`
  - real WAV files on disk
  - immutable
  - the ground-truth timeline
- `review windows`
  - temporary human-review spans over a source recording
  - not real exported clips
  - used for ASR, transcript correction, alignment, and marking unusable spans
- `final slices`
  - actual train/export units
  - created only after review/alignment/selection is finished
  - should not inherit review-window boundaries blindly

The system needs to support two user styles:

- `curated mode`
  - user manually reviews and corrects a lot
  - highest dataset quality
- `quick fine-tune mode`
  - user wants a usable dataset with minimal manual work
  - system auto-screens windows using alignment and quality metrics
  - user controls how strict acceptance should be

## Why This Pipeline Exists

Long recordings are too messy to send directly into TTS training. They contain bad ASR, laughter, breaths, noise, crosstalk, awkward sentence boundaries, and unusable spans. The pipeline exists to:

- break long recordings into manageable review spans
- get draft text
- align text back to audio
- let users reject or exclude bad parts
- automatically score window quality
- assemble final training slices from only the usable source spans

The core design rule is: review windows are for humans and triage, final slices are for training.

## Full Pipeline

1. Import source recordings.
   - User adds raw WAV recordings to the project.
   - These source recordings are immutable and remain the canonical audio timeline.

2. Generate review windows.
   - The system creates coarse review spans over each source recording.
   - These are conservative windows for ASR and human review.
   - They are not final training clips.
   - Their purpose is fault isolation and manageable editing.

3. Run ASR per review window.
   - Each review window gets a draft transcript.
   - This localizes transcription errors to one window instead of poisoning an entire long recording.
   - Raw ASR output is preserved as draft text, not treated as final truth.

4. Run forced alignment per review window.
   - The aligner tries to map the current transcript text onto the audio and produce word timings.
   - This is done per review window so failures stay localized.
   - Alignment must be rerunnable at any time.
   - Alignment output should include a quality signal, not just timings.

5. Compute review-window health metrics.
   - After ASR and alignment, the system computes health/quality data for each review window.
   - Example metrics:
     - alignment completeness
     - interpolation/skipped-word ratio
     - alignment confidence
     - transcript weirdness
     - speech ratio
     - silence ratio
     - clipping/noise proxies
     - ignored-span ratio

6. Apply an acceptance policy.
   - User chooses a strictness preset or custom thresholds.
   - Example modes:
     - `Draft`
     - `Balanced`
     - `Curated`
   - The system then classifies each window as:
     - `auto_accept_candidate`
     - `needs_review`
     - `auto_reject`

7. Human review happens only where needed.
   - In curated mode, most windows are reviewed manually.
   - In quick mode, only borderline or suspicious windows are reviewed.
   - Review happens in Clip Lab review-window mode.

8. In review-window mode, user can edit transcript.
   - User corrects the transcript if ASR is wrong.
   - This makes previous alignment stale.
   - Old alignment is retained for audit/debug, but marked unusable until rerun.

9. In review-window mode, user can exclude bad spans.
   - User can select part of the window and exclude it.
   - This is not destructive audio editing.
   - It creates source-relative ignore/exclusion timestamps.
   - Whole-window reject is just an exclusion over the entire window span.
   - This handles laughter, coughs, crosstalk, noise bursts, and other unusable sections.

10. Rerun alignment after transcript or exclusion edits.
   - If transcript changes or ignored spans change, alignment is marked stale.
   - User can rerun alignment manually.
   - Alignment should run against the kept audio, not the excluded parts.
   - Returned word timings must still be mapped back into source-relative timestamps.

11. Produce reviewed usable spans at source level.
   - After review/triage, each source recording now has:
     - accepted windows
     - rejected windows
     - ignored subranges
     - reviewed transcripts
     - alignment data
   - The system then treats this as a source-level timeline with usable and unusable intervals.

12. Stitch review-window results into one source-relative aligned representation.
   - The system combines window-local reviewed alignment into a source-level representation.
   - Window boundaries are no longer important once review is done.
   - The important outputs are:
     - source-relative word timings
     - reviewed transcript truth
     - exclusion barriers

13. Run final slicer on usable source spans.
   - The final slicer operates on the source-level reviewed/aligned timeline.
   - It does not trust review-window boundaries as final slice boundaries.
   - It uses alignment-aware and acoustic-safe logic to choose better cuts.
   - Ignored spans are treated as hard barriers.
   - The slicer must never pack across excluded spans.
   - The current implementation direction is:
     - acoustic-safe gap candidates first
     - no-cut collars around aligned words
     - no forced cuts through active speech
     - canonical non-overlapping training bounds
     - separate review-safe audition bounds
     - post-slice QC flags for edge energy and suspicious silence

14. Create final training slices.
   - These are actual export/training units.
   - They are derived from accepted source spans plus slicer logic.
   - They should have:
     - source-relative provenance
     - transcript text
     - alignment data
     - inspectable boundaries
     - repeatable generation policy

15. Export or hand off to inference/training.
   - Export uses approved backend truth only.
   - Not temporary frontend state.
   - The result is a reproducible dataset for fine-tuning.

## Clip Lab Modes

Clip Lab should open in two modes.

- `clip mode`
  - used for real final clips/slices
  - waveform edits are true derived-audio edits
  - split/merge/edit waveform all make sense

- `review window mode`
  - used during review and triage
  - split/merge are disabled
  - transcript edits are allowed
  - manual alignment rerun is allowed
  - delete selection means `exclude source span`
  - reject means `exclude whole window span`
  - alignment score/status is shown
  - alignment can be stale, fresh, missing, or failed

The waveform UI can be shared between modes, but the semantics of the actions must differ.

## Why Review Windows and Final Slices Must Stay Separate

If review windows are treated as final slices, several problems appear:

- bad window boundaries become training boundaries
- partial rejection breaks transcript continuity
- excluded laughter/noise shifts timing if treated as destructive delete
- alignment and slicing become tied to UI spans instead of source truth

The correct model is:

- windows are for review
- source timeline is for truth
- final slices are for training

## Auto-Accept / Low-Touch Mode

To support users who do not want to manually label everything, the system should support a policy-driven path:

- ASR all windows
- align all windows
- score all windows
- accept only the windows that meet the chosen quality threshold
- optionally send borderline windows to review
- reject obvious garbage automatically

This creates a fast but lower-trust dataset. The user can choose how strict this should be.

This should not rely on a single alignment score. It should use multiple simple metrics, because a window can align well and still be bad training data.

## Key Design Rules

- Source recordings are immutable.
- Review windows are temporary review objects, not final clips.
- Raw ASR transcript is preserved as draft, not truth.
- Reviewed transcript is truth.
- Alignment target may be normalized internally, but must not overwrite transcript truth.
- Excluded spans are stored as source-relative ignore ranges.
- Alignment becomes stale whenever transcript or ignored spans change.
- Final slicer uses source-level reviewed alignment plus exclusion barriers.
- Final export uses approved backend truth only.
- Training/export bounds and review/audition bounds are not the same object.
- Review-bound tuning is allowed to change for Clip Lab listening, but must not silently redefine training truth.

## What This Pipeline Is For

This pipeline is for turning messy long-form speech recordings into trainable, high-quality, provenance-aware slices for speech-model fine-tuning, while supporting both:

- careful manual curation
- faster low-touch dataset creation with tunable strictness

## Current Slicer Status

The current pure slicer test path is being validated on long-form local recordings before backend merge. The broad result so far is:

- much better than the earlier punctuation-led slicer
- most clips are now acceptable by ear on local long-form validation data
- the remaining problems are narrow edge-polish issues, not catastrophic boundary failures

The current remaining issue class is:

- a small number of clips still have a slight spike or tiny carry-in/carry-out at the start or end
- these are mostly edge-polish problems on otherwise usable clips
- they should be handled as a targeted post-pass, not by destabilizing the main grouping logic again

See [Slicer_Finetuning_Status.md](/home/aaravthegreat/Projects/speechcraft/docs/Slicer_Finetuning_Status.md) for the current implementation phases, validation notes, and reviewer guidance.
