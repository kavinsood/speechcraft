Speechcraft Sprint Spec: Overview / Prep, Slicer, QC
1. Scope and current product flow
Current happy path

Ingest -> Overview -> Prep -> Slicer -> QC -> Lab -> Export

This is the intended flow for this sprint.

Navigation

Full backward/forward navigation between stages is not required in this sprint.
The system may allow limited movement between pages, but “jump anywhere at any time with perfect consistency” is explicitly deferred.

Page ownership

Each page owns a different unit of truth:

Overview owns imported raw recordings and preparation configuration
Slicer owns slicer runs over prepared recordings
QC owns QC runs and QC results for one slicer run
Lab owns slice-level manual review/editing

Do not blur these boundaries.

2. Global product assumptions for this sprint
In scope
.wav import only
English only
single-speaker assumption
no-review fast path
advanced controls for expert users
manual review path in Lab
Out of scope for this sprint
transcript import
diarization
multi-speaker workflows
multilingual/code-switching
dataset-size recommendations
region-level exclusion tools
recommendation engine telling users what to do
complex graph interactions
full cross-stage reversible navigation
3. Cross-page system rules
3.1 Originals are immutable

Imported raw audio is never mutated in place.

3.2 Preparation creates derived data

Preparation actions create a new derived dataset copy on disk using the chosen prep settings.

3.3 Runs are first-class

Slicer and QC are both run-based.
No hidden in-place replacement of previous outputs.

3.4 Human decision is king

Machine QC may classify, rank, and filter.
Human review may always override it.

3.5 Reviewed/locked material is protected

Reviewed and locked slices are preserved and visually separated from machine-QC-only material.

3.6 Advanced controls are hidden by default

Default UX is for the lazy user.
Power user detail is available but collapsed.

3.7 Long-running jobs must be visibly alive

For preparation, slicing, QC, and similar operations, the frontend must show:

that a job is running
what type of job is running
current status/state
log output or streamed terminal-style messages
success/failure completion state
a clear message when the job finishes

This is mandatory. Silent background jobs are garbage UX.

4. Overview page spec
4.1 Purpose

The Overview page is the post-ingest source-level page.

Its job is to answer:

what raw audio was imported
what basic technical properties the dataset has
what preparation settings will be applied
whether the dataset is ready to move into slicing

It is not a slice-quality page.

4.2 Unit of truth

The Overview page is recording-centric.

It operates on imported raw recordings, not slices.

4.3 Mandatory displayed stats

For this sprint, Overview must show:

total duration
number of recordings
sample rate(s)
channel count(s)

That is the required baseline.

4.4 Warnings/checks

For this sprint, Overview should support only simple technical warnings, not fake intelligence.

Initial warning set
mixed sample rates across imported recordings
mixed channel counts across imported recordings
no recordings imported
preparation settings changed but no prepared derivative generated yet

Do not invent a nonsense “dataset quality score” here.

4.5 Preparation tools on Overview

Preparation tools live on the Overview page.

In-scope preparation actions
downsampling
mono/downmix
channel selection
Deferred preparation action
loudness normalization

Loudness normalization is intentionally deferred until the standard and policy are decided.

4.6 Preparation behavior

Preparation is explicit and user-triggered.

Required interaction

User:

chooses prep settings
clicks a button to run preparation
system creates a new prepared dataset copy on disk
Important rule

Preparation must not silently mutate current source files or sneakily modify prior prepared outputs.

4.7 Preparation output

Preparation produces a derived output dataset suitable for downstream slicing.

This prepared output becomes the input scope for slicer runs.

4.8 Overview layout requirements

The page should include:

A. Dataset summary section
total duration
recording count
sample rates
channel counts
B. Preparation controls section
target sample rate
mono/downmix options
channel selection options
run prep button
C. Warning/status section
mixed sample rate warning
mixed channel count warning
stale prep warning
prep completed state
D. Job activity section

A visible job panel for preparation jobs showing:

current state
streamed logs or terminal-style messages
completion/failure message
4.9 Overview non-goals

Overview should not show:

slice-level quality analysis
QC graphs
clip acceptance/rejection logic
transcript quality scoring
speaker analytics

That belongs later or elsewhere.

5. Preparation job UX / terminal log panel

This is shared behavior across Overview, Slicer, and QC.

5.1 Required job feedback UI

Any long-running backend operation must expose a visible frontend activity surface.

Minimum requirements
job type label
running / completed / failed state
start time
simple progress state when available
log output or terminal-like streamed text
clear completion message
clear failure message
5.2 Jobs requiring this treatment

At minimum:

preparation
slicing
QC

Potentially later:

export
model processing jobs
alignment-related jobs
5.3 UX rule

The user must never wonder whether the app is frozen or whether a job is doing anything.

That means:

spinner alone is not enough
toast alone is not enough
silent async polling is not enough

You need a persistent visible job panel.

6. Slicer page spec
6.1 Purpose

The Slicer page exists to create candidate slices from prepared recordings.

It is a run launcher and run summary page, not a manual waveform editing tool.

6.2 Unit of truth

The Slicer page is run-centric.

Each execution creates a distinct slicer run.

6.3 Slicer input

Slicer runs over prepared recordings from Overview.

It should not run over ambiguous or half-applied source state.

6.4 Slicer execution model

Each slicer execution creates a new slicer run.

Rules
prior slicer runs remain available
slicer runs are not silently overwritten
a user may rerun slicing with new settings
reviewed/locked slices remain protected by the existing overlap-preservation mechanism
6.5 Relationship to existing reviewed/locked slices

When a new slicer run is generated:

new slices are created
slices overlapping heavily with locked/accepted material are handled by the existing preservation logic
locked/reviewed material is not casually destroyed

This behavior should be preserved as-is for this sprint.

6.6 Visible slicer controls

Because defaults reportedly work most of the time, the default UI should be restrained.

Directly visible controls

For this sprint, top-level visible controls should be limited to a small core set such as:

target clip length
maximum clip length
one general segmentation sensitivity control
optional ASR on/off, if applicable in your current backend flow

Do not dump every backend parameter in the main view.

6.7 Advanced section

All other slicer parameters may exist under a collapsed Advanced section.

This section is hidden by default.

6.8 Presets

Presets are optional in this sprint.

Because current defaults already work well in most observed cases, presets are not required to ship this phase.

6.9 Slicer summary after run

After a slicer run completes, the page should show:

slices created
total sliced duration
average slice length
minimum slice length
maximum slice length
skipped/failed regions or counts, when available
whether downstream ASR/QC-relevant data is available
warnings about suspicious segmentation, if available
6.10 Job activity section

The Slicer page must include the same visible job/log panel behavior as Overview.

When slicing is running, the user must see:

that slicing is running
current run state
logs/messages
completion/failure state
6.11 Transition to QC

After slicing completes, the next intended step is QC.

QC is not automatically run.
The user manually triggers it from the QC page.

7. QC page spec
7.1 Purpose

The QC page is the post-slice triage page.

Its job is, in this order:

macro triage before labeling
automatic pruning for the fast path
dataset analytics for the current slicer run

That priority order matters.

7.2 Unit of truth

QC is tied to one slicer run.

Do not mix QC across multiple slicer runs in this sprint.

7.3 Trigger model

QC is manually triggered.

The page may initially show no QC results until the user clicks a button to start QC.

7.4 QC scope

For this sprint, QC operates on:

current unreviewed slices in the slicer run
reviewed slices may still be shown, but clearly distinguished visually

Reviewed/locked slices should not be casually modified by QC logic.

7.5 Machine vs human separation

QC results and human review state must remain conceptually separate.

Required rule

Machine QC does not equal human approval.

The UI must make this obvious.

7.6 Mandatory QC outcome buckets

Use these UI-facing names:

Auto-kept
Needs review
Auto-rejected

These are the machine triage buckets.

7.7 QC score

There will be one visible aggregate QC score for ranking.

Important rule

This score is for ranking and thresholding.
It is not the only truth.

Reason codes and raw metrics still matter.

7.8 Raw metrics

Raw metric values should be available in Advanced mode.

Normal users should not be dumped into metric soup by default.

7.9 QC metrics policy

The exact scoring implementation should follow the research-backed composite approach rather than ad hoc nonsense.

Hard-gate classes

Use the final agreed hard failure classes:

transcript mismatch
overlap / second speaker
broken audio
near-silence / unusable clip
severe clipping / corruption
Soft-score dimensions

Use the research-recommended multi-signal scoring approach and compute one aggregate score for ranking.

Do not reduce the page to a single opaque magic number with no reason breakdown.

7.10 Thresholding

The QC page must support:

threshold sliders
presets
visible dataset yield impact for the current threshold
Yield display

At minimum, show the user what the current threshold would retain/reject in terms of dataset yield.

7.11 No “worst 20%” shortcut for now

For this sprint, thresholding is score-based.
Do not add percentile-only pruning flows yet.

7.12 QC page visuals required at launch

Mandatory:

summary cards
histogram/distribution view
threshold control
preview table
timeline strip

Optional later:

complex graph interactions
advanced cross-selection behavior
7.13 Graph interaction behavior

For this sprint, graph selection does nothing special.

The graphs are view/analysis surfaces, not full interactive selection tools yet.

Good. Keep it simple.

7.14 Preview table

The QC page includes a preview table showing slices and their QC-related information.

This supports the fast path and manual inspection before entering Lab.

7.15 Reviewed slice visibility

Reviewed slices may appear on the QC page, but must be visually distinguished from unreviewed/machine-triaged slices.

A different color treatment is acceptable.

7.16 QC actions

The page should support these actions:

run QC
adjust threshold
view retained/rejected yield
send relevant slices to Lab with transferred filters/sort
rerun QC for the same slicer run
7.17 Recovery / reset behavior

Keep this simple.

For this sprint:

allow reset to the QC run’s current threshold/default state
allow rerun QC
do not build a huge undo system

QC is classification/filter state, not destructive deletion.

8. QC data/state contract

You said “whatever seems best.” Fine. This is what seems best.

8.1 First-class backend objects required

QC needs persisted backend truth.

At minimum:

A. QC Run

Stores:

qc run id
slicer run id
created time
status
threshold/preset configuration used
B. Per-slice QC Result

Stores per slice:

slice id
qc run id
aggregate score
raw metric values
bucket
reason codes
reviewed/locked visibility context if needed

This is the minimum sane substrate.

8.2 Why this must exist

Without stored QC run data:

the QC page is not reproducible
thresholds are not auditable
results drift into frontend-only lies
Lab handoff becomes vague garbage

So no, this should not be “just tags in memory.”

8.3 Relationship to human review

Human review remains separate.

Required rule

A human review action may override machine QC outcome.

Final authority

Human action is king.

9. QC stale-state policy

You did not answer this, so here is the minimal correct policy.

9.1 QC becomes stale when

A QC run should be marked stale if:

the slicer run changes
the slice population changes
slices are regenerated for that run
a slice’s active audio basis changes in a way that invalidates QC assumptions
transcript changes invalidate QC-relevant scoring inputs
9.2 UI behavior for stale QC

For this sprint:

show a stale badge/state
allow viewing old QC results
require rerun before applying fresh QC threshold decisions

Cheap, simple, sane.

10. Lab handoff from QC
10.1 Purpose

QC must hand off machine-triaged context into Lab.

10.2 Automatically transferred state

When opening Lab from QC, transfer:

selected QC bucket filters
selected sort order
current threshold context
10.3 Default sort behavior

Absent QC-driven sorting/filtering, Lab retains source order as default.

10.4 Tags and slice metadata

QC-related tags/state should be visible in Lab through existing slice tag/state display.

10.5 Human override in Lab

Lab must allow:

restoring machine-rejected slices
reviewing machine-kept slices
overriding QC classification through human action

Human decision overrides QC.

11. Fast-path user flow

This is the no-brains path.

11.1 Minimal fast-path flow
ingest wav(s)
inspect Overview briefly
confirm sample rate/channel setup
run preparation if needed
run slicer with defaults
open QC
run QC
adjust threshold
export using auto-selected result set

That is the dumb-user path.

11.2 No recommendation engine yet

The app does not yet need to tell the user whether their dataset is “good enough” or what model size requirements imply.

That is explicitly deferred.

11.3 No aggressive guardrails yet

If users make bad threshold choices, the system does not yet need to nanny them beyond showing yield.

That is also deferred.

12. Logging / activity UX spec

This needs to be explicit because otherwise the agent will half-ass it.

12.1 Shared component

There should be a reusable job activity panel component used across pages.

12.2 Behavior

For any active long-running job, show:

job name/type
current state
running indicator
log stream / terminal-style output
success message on completion
failure message on error
12.3 Placement

Each relevant page should surface its own active job panel in a clearly visible area.

No hidden modal nonsense.

12.4 History

Nice-to-have, but not required in this sprint:

prior job log history per page

Current requirement is only live visibility plus completion/failure messaging.

13. Explicit non-goals for this sprint

Do not let the agent invent these:

transcript import workflow
diarization workflows
speaker selection flows
region-level timeline mass actions
complex graph brushing/linking behavior
automatic dataset recommendations
loudness normalization implementation
multilingual handling
small-dataset warnings
percentile pruning workflow
full backward/forward cross-stage navigation support
giant undo system for QC
14. Final naming for UI

Use these page names:

Overview
Slicer
QC
Lab

Keep it simple. Don’t get cute.

Use these QC bucket labels in UI:

Auto-kept
Needs review
Auto-rejected

Internal enum names can be stricter if needed.