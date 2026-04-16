# Slicer: How It Works Technically

## Purpose

The **Slicer** stage is the part of Speechcraft that turns one prepared source recording into a set of candidate training clips.

It is not a waveform editor.
It is not a manual labeling surface.
It is not a one-off script that mutates audio in place.

For this sprint, the Slicer page is a **run launcher and run summary surface** for a source-recording-driven slicing pipeline.

Its job is to:
- consume prepared recordings
- use source-level transcript and alignment artifacts
- create a new slicer run
- materialize candidate `Slice` rows and their audio
- preserve reviewed or locked material when rerun

## The unit of truth

The slicer is built on three core ideas.

### 1. Source recording is the timing anchor

A `SourceRecording` is the canonical long-form audio object.

It owns the timing space that slicing operates inside.
All slice boundaries are ultimately expressed relative to the source recording.

### 2. Transcript and alignment live at the recording level

The transcript and alignment used for slicing belong to the `SourceRecordingArtifact`, not to a review-window object.

This is a major architectural rule.
It means slicing can be rerun from reusable source-level artifacts instead of rebuilding truth from intermediate UI objects.

### 3. Slices are the reviewable output

The slicer creates real `Slice` rows directly.

Humans review the actual slices the model will train on.
They do not review temporary review-window abstractions.

## High-level flow

The intended technical flow for the slicer is:

1. start from a prepared `SourceRecording`
2. ensure a source transcript artifact exists
3. ensure a source alignment artifact exists
4. run the slicer algorithm over source-level alignment
5. create candidate boundaries
6. refine those boundaries acoustically
7. materialize slices directly from source truth
8. preserve reviewed/locked slices if rerunning

That is the core of the current slice-first path.

## Why the slicer is built this way

The old product path treated review windows as a major part of the data flow.
That made sense when slicing quality was weak and the UI had to compensate.

The newer slicer path assumes something different:

- source recording is the timing anchor
- source transcript and source alignment belong to the recording
- the slicer runs directly on source alignment
- the slicer creates `Slice` rows directly
- Clip Lab reviews slices only

This is the current architectural direction because the slicer is now good enough that the slice itself is the thing worth reviewing.

## Inputs

For this sprint, the slicer should run over **prepared recordings** coming out of Overview / Prep.

That means the slicer input is not an ambiguous bag of raw and half-prepared files.
It runs over a clear prepared dataset state.

The main technical inputs are:
- prepared source audio
- source transcript artifact
- source alignment artifact
- slicer configuration
- rerun policy regarding locked/reviewed slices

## Source transcript and alignment

The slicer is alignment-driven.

That means it does not blindly cut on energy drops or on simple punctuation rules alone.
Instead, it assumes that source-level transcript and alignment already exist and uses them as the semantic backbone for boundary placement.

The source transcript tells the system what was said.
The source alignment tells the system where those words sit in time.

That gives the slicer a much stronger signal than old silence-threshold approaches.

## The current slicing philosophy

The current slicer is best understood as **acoustic-first, alignment-guided, and safety-biased**.

That means:
- transcript and alignment define the word-level timing structure
- acoustic evidence helps decide where cuts are actually safe
- duration targets matter, but not at the cost of clipping speech badly
- punctuation is useful, but no longer the fake boss of the system
- breaths and natural pauses should be preserved when possible

The slicer is trying to create clips that are:
- trainable
- semantically coherent
- acoustically safe at boundaries
- not stupidly short or long
- not obviously mutilated by the cut itself

## Current configuration surface

The current slicer algorithm exposes a real parameter set.

These include ideas like:
- target duration
- minimum duration
- maximum duration
- soft maximum duration
- minimum and preferred gap for a boundary
- snap collar around proposed cuts
- RMS frame size
- leading and trailing word guards
- boundary context window
- acoustic boundary weighting
- minimum acoustic boundary score
- edge window settings
- edge energy ratio threshold
- max leading and trailing silence
- minimum speech ratio
- punctuation/pause timing thresholds
- breath-related duration and energy thresholds
- long-clip flag threshold

These are real engineering controls, but they are not meant to all be first-class top-level UI knobs.

## Core algorithm shape

At a high level, the slicer operates like this.

### 1. Read aligned words

The algorithm works on aligned word objects that include:
- the word text
- start time
- end time
- confidence

It can also inspect trailing punctuation from the token text.

This lets it reason about both timing and textual context.

### 2. Build candidate boundaries

The slicer looks for places where a cut might be valid.

A candidate boundary is not just “there is a pause here.”
It is a structured object containing:
- which word it sits after
- the candidate timestamp
- gap duration
- boundary type
- strength
- safe start and safe end window
- acoustic score
- valley energy

That means the slicer is not simply chopping at hard thresholds.
It is collecting candidate cut locations and evaluating them.

### 3. Use duration targets without letting them dominate

The algorithm tries to stay near a target clip length, but it also respects:
- minimum duration
- soft maximum
- hard maximum

This is important.
The slicer is not trying to make every clip exactly one fixed length.
It is trying to make clips land in a reasonable band without making stupid cuts.

### 4. Respect safe gaps and word guards

Cuts should not land too close to active speech.
So the slicer keeps guard regions around word edges.

It also distinguishes between:
- a weak gap
- a preferred gap
- a truly safe boundary region

That means not every pause is treated as equally trustworthy.

### 5. Refine boundaries acoustically

Even when alignment suggests a cut area, the slicer still uses an acoustic layer.

The algorithm examines a local collar around a candidate boundary and prefers cuts that land in a low-energy valley rather than at unstable edges.

This is important for avoiding:
- clipped phoneme tails
- sharp transients at the cut point
- unnatural boundary clicks
- cutting too close to inhalations or fragile non-speech transitions

The slicer is trying to cut **inside** a stable region, not on the sharp edge of a speech transition.

### 6. Preserve breaths and natural pauses when possible

Breaths are treated as part of realism, not as garbage by default.

That means the slicer avoids being too aggressive with micro-pauses and breath-adjacent regions.
It uses padding, guard windows, and breath-related heuristics to reduce the chance of accidentally guillotining natural breathing and phrase rhythm.

This does not mean it perfectly models every breath.
It means the policy is intentionally conservative so breaths are not casually destroyed.

### 7. Compute review/export-relevant boundaries

The slicer distinguishes between training truth and review convenience.

The canonical training bounds are the ones that matter for export.
Review-safe or audition-safe extensions are conceptually different.

A clip that sounds bad in a review-safe extension is not automatically evidence that the canonical training boundary is bad.

That distinction matters when debugging slicer issues.

## What the slicer is optimizing for

The slicer is trying to satisfy several constraints at once:

- do not cut words in half
- do not cut too close to unstable speech transitions
- preserve breaths and natural flow when possible
- keep clips in a useful training duration range
- avoid pathological silence-heavy clips
- keep source-relative provenance intact
- produce slices that a human can review directly

This is why the algorithm is not a trivial silence threshold tool.

## What creates a new slicer run

Every slicer execution creates a new slicer run.

That means:
- runs are first-class
- prior runs remain available
- runs are not silently overwritten
- rerunning slicing is a normal operation

The slicer page should behave like a run launcher and run summary page, not like a mutable hidden state machine.

## Rerunning the slicer

Rerunning the slicer is a supported workflow.

The normal use case is:
- run slicer once with default or current settings
- inspect the resulting slices
- decide whether the segmentation is good enough
- rerun with adjusted settings if the dataset looks systematically wrong

The important rule is that rerun must not casually destroy human-reviewed material.

## Locked/reviewed slice preservation

Speechcraft already has a preservation policy for reviewed or locked slices.

When the slicer is rerun:
- new candidate slices are created
- reviewed/locked material remains protected
- heavily overlapping new candidates are handled by the existing overlap-preservation logic
- manually reviewed material is not casually destroyed

This is one of the most important correctness rules in the slicer system.

Without it, rerunning the slicer would make human review unsafe and would destroy trust in the product.

## What the slicer page should expose

Because defaults reportedly work most of the time, the slicer UI should not immediately dump every parameter on the user.

For this sprint, the top-level visible controls should stay restrained.

Directly visible controls should focus on a small core such as:
- target clip length
- maximum clip length
- one general segmentation sensitivity control
- optional ASR on/off, if that applies in the current backend path

Everything else can exist in a collapsed Advanced section.

That keeps the main UX usable while still allowing experts to tweak the real algorithm when needed.

## What happens after a slicer run finishes

After slicing completes, the system should show a slicer run summary.

At minimum, that summary should include:
- slices created
- total sliced duration
- average slice length
- minimum slice length
- maximum slice length
- skipped or failed region counts, when available
- whether downstream QC-relevant data is available
- warnings about suspicious segmentation, when available

This makes the slicer page more than a launch button. It becomes a proper run summary surface.

## Job visibility is mandatory

Slicing is a long-running backend operation.

That means the page must show:
- that slicing is running
- the current run state
- logs or terminal-style messages
- completion state
- failure state

A spinner alone is not enough.
A toast alone is not enough.
The user needs a visible activity surface so the app does not feel frozen or haunted.

## Relationship to QC

After the slicer completes, the next intended stage is **QC**.

QC is not automatically run in this sprint.
The user manually goes to QC and starts it for the current slicer run.

That keeps slicing and QC cleanly separated:
- slicer creates candidate slices
- QC classifies and triages those slices
- Lab performs human review on slices

## What the slicer is not responsible for

The slicer is not responsible for:
- manual clip editing
- transcript import workflows
- diarization in this sprint
- multilingual handling in this sprint
- final human approval
- QC bucket decisions
- recommendation logic telling the user what threshold to choose

It is responsible for one thing:

**creating technically reasonable candidate slices from prepared recordings using source-level transcript and alignment truth.**

## How to think about slicer failures

When a bad clip appears, it is important not to blame the slicer blindly.

A bad result can come from multiple layers:
- source audio itself is bad
- transcript is wrong
- alignment is wrong
- boundary choice is wrong
- review-safe bounds are making a good training cut sound bad
- UI/waveform import layer is misleading

That is why slicer review has to be done carefully.
A bad audible clip is not always proof that the training boundary itself is bad.

## In one sentence

The Speechcraft slicer is a run-based, source-recording-driven, alignment-guided, acoustically refined clip generator that creates real reviewable slices directly from source truth while protecting human-reviewed material on reruns.

