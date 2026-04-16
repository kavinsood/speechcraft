# QC Page

## What the QC page is

The **QC** page is the post-slice triage page.

Its job is to sit between slicing and manual review, give the user a macro view of the current slicer run, and support the fast automatic path for users who do not want to review everything manually.

For this sprint, the QC page has three priorities, in this order:

1. **macro triage before labeling**
2. **automatic pruning for the fast path**
3. **dataset analytics for the current slicer run**

That priority order matters.

The QC page is not a replacement for human review.
It is a machine triage and filtering surface.

## What unit of truth it owns

The QC page is tied to **one slicer run**.

That means:
- QC is run-based
- QC results are scoped to one slicer run
- QC results are not mixed across slicer runs
- rerunning the slicer creates a different slice population and therefore a different QC basis

For this sprint, QC should not pretend to be global dataset truth across unrelated slicer runs.

## What the QC page does for users

The QC page supports two broad user workflows.

## 1. Fast path workflow

This is the no-review path.

The user:
- imports `.wav` files
- checks Overview
- runs preparation if needed
- runs the slicer
- opens QC
- runs QC
- adjusts thresholding
- sees how much of the dataset will be kept or rejected
- exports using the machine-selected result set

This is the “no-brains” path.
The user is allowed to make threshold decisions based on the visible yield and QC summary.
The app does not need to nanny them with a recommendation engine in this sprint.

## 2. Review path workflow

This is the human-in-the-loop path.

The user:
- runs slicing
- opens QC
- runs QC
- uses QC to see which slices are likely good, suspicious, or likely bad
- sends relevant slices into Lab
- manually reviews and overrides machine decisions where needed

In this path, QC is a triage and prioritization stage, not the final authority.

## Human vs machine rule

The most important rule on the QC page is this:

**Machine QC is not human approval.**

The page must make this obvious.

Machine QC can:
- classify
- rank
- filter
- estimate yield
- suggest what to review first

But a human may always override it later in Lab.

Human decision is king.

## QC buckets

For this sprint, the QC page must show three machine triage buckets:

- **Auto-kept**
- **Needs review**
- **Auto-rejected**

These are machine buckets only.
They are not the same thing as human acceptance or rejection.

## How QC is triggered

QC is manually triggered.

The page may initially show no QC results until the user clicks a button to start QC for the current slicer run.

This is intentional.
QC is a distinct run, not a hidden side effect.

## What QC runs on

For this sprint, QC operates on:
- the current unreviewed slices in the slicer run

Reviewed slices may still appear on the QC page, but they must be clearly distinguished visually.

Reviewed or locked slices should not be casually modified by QC logic.
They may be visible for context, but the UI must keep machine-only triage and human-reviewed material conceptually separate.

## What metrics QC uses

QC uses a research-backed composite approach.
It does not reduce the dataset to a single magic number with no explanation.

There are two layers:

## 1. Hard failure classes

These are the strongest machine failure classes and should be treated as hard-gate conditions.

For this sprint, the agreed hard failure classes are:
- transcript mismatch
- overlap / second speaker
- broken audio
- near-silence / unusable clip
- severe clipping / corruption

These are the classes that can justify strong machine rejection without pretending the system is omniscient.

## 2. Soft-score dimensions

Beyond hard failures, QC computes a composite quality score using multiple dimensions.

These dimensions should support ranking and thresholding, not replace reason codes.

The intended soft dimensions include:
- transcript integrity
- speaker purity
- boundary integrity
- nonverbal cleanliness
- silence / pacing quality
- acoustic cleanliness

The exact implementation may evolve, but the page should present them as a structured multi-signal system rather than a bag of nonsense heuristics.

## Aggregate QC score

There is one visible aggregate QC score.

This score exists for:
- ranking
- thresholding
- estimating yield
- sorting preview results

Important rule:

**The aggregate score is not the only truth.**

Reason codes and raw metrics still matter.

The visible score should help users make fast decisions, but the page must not hide why a slice ended up where it did.

## Raw metrics and advanced mode

Raw metric values should be available in **Advanced** mode.

Normal users should not be dumped into metric soup by default.

So the page should work like this:
- normal mode: summary, score, buckets, reasons, threshold/yield
- advanced mode: raw metric values and deeper QC details

## Thresholding and user control

The QC page must support threshold-based filtering.

For this sprint, thresholding should include:
- threshold sliders
- presets
- visible yield impact for the current threshold

At minimum, the page must show the user what the current threshold would retain or reject in terms of dataset yield.

This is especially important for the fast path.

## Important thresholding rule

For this sprint:
- thresholding is **score-based**
- percentile-only pruning flows like “drop worst 20%” are deferred

Do not add percentile-only shortcuts yet.

## What graphs the QC page shows

The QC page should include a small, clear set of analysis surfaces.

### 1. Summary cards

These give the user the macro result immediately.

The page should show:
- total slices in scope
- Auto-kept count
- Needs review count
- Auto-rejected count
- retained duration / yield under the current threshold
- maybe reviewed slice count if they are visible in context

### 2. Histogram / distribution view

This is the main thresholding graph.

Its purpose is to show:
- how QC scores are distributed
- where the current threshold sits
- how much data falls into each region

This supports the fast path directly because the user can understand how aggressive or lenient the current threshold is.

### 3. Timeline strip

This gives the user a source-order or time-order macro view of the slicer run.

Its purpose is to show:
- where suspicious or weak slices cluster
- whether badness is scattered or concentrated
- how the current slicer run behaves across the dataset

For this sprint, the timeline strip is a view/analysis surface only.
It is not a complex graph interaction system.

### 4. Preview table

The preview table is mandatory.

It should show slices and their QC-related information so the user can inspect machine triage before going into Lab.

This is important for both:
- the fast path user who wants confidence before exporting
- the review path user who wants to understand what will be sent to Lab

## Graph interaction policy

For this sprint, graph interaction should stay simple.

The graphs are analysis surfaces.
They are not full interactive selection tools.

That means:
- no complex brushing/linking behavior
- no advanced graph-driven selection logic
- no big cross-filtering engine

Keep it simple.

## Preview table behavior

The preview table should present slices and their QC-related state for the current slicer run.

Useful columns include:
- slice identifier
- aggregate QC score
- machine bucket
- primary reason code
- reviewed/locked state indicator if applicable

The point of the preview table is to let the user inspect the machine triage outcome before entering Lab or exporting.

## Reviewed slice visibility

Reviewed slices may appear on the QC page, but they must be visually distinguished.

Machine-triaged and human-reviewed material are not the same thing.

A different color treatment or other clear visual distinction is enough for this sprint.

The goal is to keep the page honest:
- machine QC is machine state
- reviewed slices are human state

## QC actions

For this sprint, the QC page should support these actions:
- run QC
- adjust threshold
- view retained/rejected yield
- send relevant slices to Lab with transferred filters/sort
- rerun QC for the same slicer run

QC is not destructive deletion.
It is classification and filtering state.

## Reset and rerun behavior

Keep this simple.

For this sprint, the page should support:
- reset to the QC run’s current threshold/default state
- rerun QC

Do not build a giant undo system.

## QC backend truth

QC needs persisted backend truth.
It cannot live as tags in memory or frontend-only state.

At minimum, QC needs:

### A. QC Run

A first-class object storing:
- QC run id
- slicer run id
- created time
- status
- threshold/preset configuration used

### B. Per-slice QC result

A first-class per-slice result storing:
- slice id
- QC run id
- aggregate score
- raw metric values
- bucket
- reason codes
- reviewed/locked visibility context if needed

Without persisted QC run data:
- the page is not reproducible
- thresholds are not auditable
- results become frontend-only lies
- Lab handoff becomes vague garbage

So QC state must exist as real backend truth.

## Stale QC state

QC can become stale.

For this sprint, QC should be marked stale if:
- the slicer run changes
- the slice population changes
- slices are regenerated for that run
- a slice’s active audio basis changes in a way that invalidates QC assumptions
- transcript changes invalidate QC-relevant scoring inputs

UI behavior for stale QC should stay simple:
- show a stale badge/state
- allow viewing old QC results
- require rerun before applying fresh QC threshold decisions

That is cheap, simple, and sane.

## Handoff from QC to Lab

QC must transfer machine-triaged context into Lab.

When the user opens Lab from QC, the system should transfer:
- selected QC bucket filters
- selected sort order
- current threshold context

This lets QC genuinely shape what the user sees next.

Absent QC-driven filters/sort, Lab retains source order as default.

## QC state visibility in Lab

QC-related tags or state should be visible in Lab through the existing slice tag/state display.

That lets the human understand:
- what the machine thought
- which bucket a slice came from
- what kind of problem was suspected

But again, human review can override machine classification.

Lab must allow:
- restoring machine-rejected slices
- reviewing machine-kept slices
- overriding QC classification through human action

Human decision overrides QC.

## What the QC page is not

The QC page is not:
- a transcript import workflow
- a diarization workflow
- a multilingual analysis system
- a recommendation engine telling the user what to do
- a giant graph playground
- a huge undo system
- a cross-stage navigation controller

Its purpose is focused and practical:

**post-slice machine triage, threshold control, dataset yield visibility, and handoff into Lab.**

## Why the QC page exists

Without the QC page, the user has only two bad choices:
- trust everything blindly
- manually inspect everything blindly

QC gives the user a macro-level decision surface before manual review.
It lets them:
- prune obviously bad material fast
- see the likely yield of threshold choices
- identify the slices most worth human attention
- move into Lab with machine context attached

## In one sentence

The QC page is the run-based post-slice triage surface where Speechcraft scores one slicer run, classifies slices into Auto-kept / Needs review / Auto-rejected, shows yield and quality distribution, and hands machine-triaged context into Lab while keeping human review as the final authority.

